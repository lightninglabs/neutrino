// Package feedb implements persistent storage for per-block fee-rate samples
// observed by the chain service. The store is intentionally narrow: one fixed
// row per fetched block, indexed by height for cheap range scans and ordered
// pruning.
//
// The samples are consumed by the feeest package, which builds a fee-rate
// estimator over a rolling window of recent observations.
package feedb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcwallet/walletdb"
)

var (
	// feeBucket is the top-level bucket for the fee sample store.
	feeBucket = []byte("fee-store")

	// samplesBucket holds per-block fee samples, keyed by big-endian
	// height followed by block hash. Big-endian height makes ordered
	// iteration (forward = oldest first) and range pruning trivial via a
	// cursor.
	samplesBucket = []byte("samples")

	// metaBucket holds a small set of scalar values: schema version and
	// the height of the most recent sample observed.
	metaBucket = []byte("meta")

	// versionKey records the schema version of the on-disk layout.
	versionKey = []byte("version")

	// tipKey records the highest height with a sample present.
	tipKey = []byte("tip")
)

// ErrSampleNotFound is returned when a sample is requested for a height that
// has no stored observation.
var ErrSampleNotFound = errors.New("fee sample not found")

// schemaVersion is the current on-disk schema version. Bumped only on
// incompatible layout changes; new fields appended via the encoding version
// byte do not require a bump.
const schemaVersion uint32 = 1

// SampleFlag is a bitset describing properties of a stored sample.
type SampleFlag uint8

const (
	// FlagEmpty marks blocks containing only the coinbase transaction
	// (no fee-paying txs). These contribute no useful signal and are
	// dropped by the estimator.
	FlagEmpty SampleFlag = 1 << iota

	// FlagSpam marks blocks whose total fees are dominated by a single
	// transaction (defined as one tx accounting for >50% of the block's
	// fees). The estimator down-weights or recomputes these.
	FlagSpam
)

// FeeSample is one observation derived from a fully fetched block. The block
// average fee rate is implicitly TotalFees * 1000 / TotalWeight (sat/kW).
type FeeSample struct {
	// Height is the block height the sample came from.
	Height uint32

	// BlockHash identifies the block; required to detect reorgs against a
	// stored sample.
	BlockHash chainhash.Hash

	// Timestamp is the block header timestamp (Unix seconds).
	Timestamp int64

	// TotalFees is the sum of fees paid by all transactions in the block,
	// derived as coinbase output minus subsidy.
	TotalFees uint64

	// TotalWeight is the sum of weight units across all transactions in
	// the block (including the coinbase).
	TotalWeight uint64

	// Flags captures qualitative properties of the sample.
	Flags SampleFlag
}

// FeeRatePerKW returns the block-average fee rate in satoshis per kilo-weight.
// Returns 0 if TotalWeight is zero (which should not happen for a valid block).
func (s *FeeSample) FeeRatePerKW() uint64 {
	if s.TotalWeight == 0 {
		return 0
	}
	return s.TotalFees * 1000 / s.TotalWeight
}

// FeeSampleStore is the interface exposed to consumers of the package. The
// estimator depends on this interface, allowing in-memory mocks for tests.
type FeeSampleStore interface {
	// PutSample writes a sample to disk and updates the recorded tip if
	// the new sample's height exceeds it.
	PutSample(*FeeSample) error

	// FetchSample retrieves a sample by height. Returns ErrSampleNotFound
	// if no sample exists at that height.
	FetchSample(height uint32) (*FeeSample, error)

	// FetchTipN returns up to n most recent samples in descending height
	// order. Fewer samples may be returned if the store has fewer rows.
	FetchTipN(n int) ([]*FeeSample, error)

	// FetchRange returns all samples with heights in [min, max] inclusive,
	// in ascending height order.
	FetchRange(min, max uint32) ([]*FeeSample, error)

	// Tip returns the highest height with a stored sample. Returns 0 and
	// no error when the store is empty.
	Tip() (uint32, error)

	// PurgeBefore deletes all samples with height < cutoff. Used for the
	// retention-window GC.
	PurgeBefore(cutoff uint32) error

	// PurgeFrom deletes all samples with height >= cutoff. Used on reorg
	// to drop observations from the orphaned chain.
	PurgeFrom(cutoff uint32) error
}

// FeeStore is the concrete walletdb-backed implementation of FeeSampleStore.
type FeeStore struct {
	db walletdb.DB
}

// Compile-time check.
var _ FeeSampleStore = (*FeeStore)(nil)

// New initialises a fee sample store using the provided database. The buckets
// are created on first use; subsequent calls are idempotent.
func New(db walletdb.DB) (*FeeStore, error) {
	err := walletdb.Update(db, func(tx walletdb.ReadWriteTx) error {
		root, err := tx.CreateTopLevelBucket(feeBucket)
		if err != nil && err != walletdb.ErrBucketExists {
			return err
		}
		if root == nil {
			root = tx.ReadWriteBucket(feeBucket)
		}

		if _, err := root.CreateBucketIfNotExists(samplesBucket); err != nil {
			return err
		}
		meta, err := root.CreateBucketIfNotExists(metaBucket)
		if err != nil {
			return err
		}

		// Stamp the schema version on first creation. We do not attempt
		// migrations here; future schema changes should add their own
		// migration step keyed off this value.
		if meta.Get(versionKey) == nil {
			var buf [4]byte
			binary.BigEndian.PutUint32(buf[:], schemaVersion)
			if err := meta.Put(versionKey, buf[:]); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil && err != walletdb.ErrBucketExists {
		return nil, err
	}

	return &FeeStore{db: db}, nil
}

// sampleKey builds the bbolt key for a sample: 4-byte big-endian height
// followed by the 32-byte block hash. Big-endian height keeps cursor order
// aligned with chain order, which makes range scans and prune operations
// simple sequential walks.
func sampleKey(height uint32, hash *chainhash.Hash) []byte {
	var key [4 + chainhash.HashSize]byte
	binary.BigEndian.PutUint32(key[0:4], height)
	copy(key[4:], hash[:])
	return key[:]
}

// heightFromKey extracts the height prefix from a key. Used during cursor
// iteration where we walk only by height range.
func heightFromKey(key []byte) uint32 {
	return binary.BigEndian.Uint32(key[0:4])
}

// encoding constants. The first byte is an encoding version that allows
// appending fields without bumping the on-disk schema version.
const (
	encodingV1   uint8 = 1
	encodingSize       = 1 + 32 + 8 + 8 + 8 + 1 // 58 bytes
)

// encodeSample writes a sample to a fixed-size byte slice for storage.
func encodeSample(s *FeeSample) []byte {
	var buf [encodingSize]byte
	buf[0] = encodingV1
	copy(buf[1:33], s.BlockHash[:])
	binary.BigEndian.PutUint64(buf[33:41], uint64(s.Timestamp))
	binary.BigEndian.PutUint64(buf[41:49], s.TotalFees)
	binary.BigEndian.PutUint64(buf[49:57], s.TotalWeight)
	buf[57] = uint8(s.Flags)
	return buf[:]
}

// decodeSample parses a stored value back into a FeeSample. The height is not
// stored in the value because it is encoded in the key; the caller passes it
// back in.
func decodeSample(height uint32, raw []byte) (*FeeSample, error) {
	if len(raw) < 1 {
		return nil, fmt.Errorf("empty sample value")
	}
	if raw[0] != encodingV1 {
		return nil, fmt.Errorf("unsupported sample encoding %d", raw[0])
	}
	if len(raw) != encodingSize {
		return nil, fmt.Errorf("sample value size %d != %d",
			len(raw), encodingSize)
	}

	s := &FeeSample{Height: height}
	copy(s.BlockHash[:], raw[1:33])
	s.Timestamp = int64(binary.BigEndian.Uint64(raw[33:41]))
	s.TotalFees = binary.BigEndian.Uint64(raw[41:49])
	s.TotalWeight = binary.BigEndian.Uint64(raw[49:57])
	s.Flags = SampleFlag(raw[57])
	return s, nil
}

// PutSample writes a sample to disk and updates the recorded tip if needed.
func (f *FeeStore) PutSample(s *FeeSample) error {
	if s == nil {
		return errors.New("nil sample")
	}

	return walletdb.Update(f.db, func(tx walletdb.ReadWriteTx) error {
		root := tx.ReadWriteBucket(feeBucket)
		samples := root.NestedReadWriteBucket(samplesBucket)
		meta := root.NestedReadWriteBucket(metaBucket)

		key := sampleKey(s.Height, &s.BlockHash)
		if err := samples.Put(key, encodeSample(s)); err != nil {
			return err
		}

		// Update tip if this sample advances it. We compare against
		// the stored tip rather than the most-recently-written sample
		// because PutSample may be called for a backfill at a lower
		// height (e.g., a block fetched out of chronological order).
		if cur := readTip(meta); s.Height > cur {
			if err := writeTip(meta, s.Height); err != nil {
				return err
			}
		}

		log.Tracef("Stored fee sample h=%d hash=%s fees=%d weight=%d",
			s.Height, s.BlockHash, s.TotalFees, s.TotalWeight)

		return nil
	})
}

// FetchSample looks up a sample by height. There may be at most one sample
// per height under normal operation; if multiple are present (transient state
// after a reorg before PurgeFrom runs) the lexicographically first hash wins.
func (f *FeeStore) FetchSample(height uint32) (*FeeSample, error) {
	var sample *FeeSample
	err := walletdb.View(f.db, func(tx walletdb.ReadTx) error {
		samples := tx.ReadBucket(feeBucket).
			NestedReadBucket(samplesBucket)

		c := samples.ReadCursor()
		var prefix [4]byte
		binary.BigEndian.PutUint32(prefix[:], height)

		k, v := c.Seek(prefix[:])
		if k == nil || !bytes.HasPrefix(k, prefix[:]) {
			return ErrSampleNotFound
		}

		s, err := decodeSample(heightFromKey(k), v)
		if err != nil {
			return err
		}
		sample = s
		return nil
	})
	if err != nil {
		return nil, err
	}
	return sample, nil
}

// FetchTipN returns up to n samples in descending height order, walking the
// cursor backwards from the end of the samples bucket.
func (f *FeeStore) FetchTipN(n int) ([]*FeeSample, error) {
	if n <= 0 {
		return nil, nil
	}

	out := make([]*FeeSample, 0, n)
	err := walletdb.View(f.db, func(tx walletdb.ReadTx) error {
		samples := tx.ReadBucket(feeBucket).
			NestedReadBucket(samplesBucket)

		c := samples.ReadCursor()
		for k, v := c.Last(); k != nil && len(out) < n; k, v = c.Prev() {
			s, err := decodeSample(heightFromKey(k), v)
			if err != nil {
				return err
			}
			out = append(out, s)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// FetchRange returns samples in [min, max] inclusive, ascending by height.
func (f *FeeStore) FetchRange(min, max uint32) ([]*FeeSample, error) {
	if min > max {
		return nil, fmt.Errorf("invalid range [%d, %d]", min, max)
	}

	var out []*FeeSample
	err := walletdb.View(f.db, func(tx walletdb.ReadTx) error {
		samples := tx.ReadBucket(feeBucket).
			NestedReadBucket(samplesBucket)

		var minKey [4]byte
		binary.BigEndian.PutUint32(minKey[:], min)

		c := samples.ReadCursor()
		for k, v := c.Seek(minKey[:]); k != nil; k, v = c.Next() {
			h := heightFromKey(k)
			if h > max {
				break
			}
			s, err := decodeSample(h, v)
			if err != nil {
				return err
			}
			out = append(out, s)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Tip returns the height of the highest stored sample, or 0 if empty.
func (f *FeeStore) Tip() (uint32, error) {
	var tip uint32
	err := walletdb.View(f.db, func(tx walletdb.ReadTx) error {
		meta := tx.ReadBucket(feeBucket).NestedReadBucket(metaBucket)
		tip = readTip(meta)
		return nil
	})
	return tip, err
}

// PurgeBefore deletes samples with height < cutoff. Implemented as a single
// cursor walk under one read-write transaction.
func (f *FeeStore) PurgeBefore(cutoff uint32) error {
	return f.purge(func(h uint32) bool {
		return h < cutoff
	})
}

// PurgeFrom deletes samples with height >= cutoff. Used to drop the orphaned
// suffix on a reorg.
func (f *FeeStore) PurgeFrom(cutoff uint32) error {
	if err := f.purge(func(h uint32) bool {
		return h >= cutoff
	}); err != nil {
		return err
	}

	// On reorg-style purge we may have evicted the tip; recompute it from
	// the remaining keys. PurgeBefore does not need this because it only
	// deletes from the bottom.
	return walletdb.Update(f.db, func(tx walletdb.ReadWriteTx) error {
		root := tx.ReadWriteBucket(feeBucket)
		samples := root.NestedReadBucket(samplesBucket)
		meta := root.NestedReadWriteBucket(metaBucket)

		c := samples.ReadCursor()
		k, _ := c.Last()
		if k == nil {
			return writeTip(meta, 0)
		}
		return writeTip(meta, heightFromKey(k))
	})
}

// purge removes every sample whose height satisfies pred. We can't delete
// while iterating on the same cursor, so we collect keys first.
func (f *FeeStore) purge(pred func(uint32) bool) error {
	return walletdb.Update(f.db, func(tx walletdb.ReadWriteTx) error {
		samples := tx.ReadWriteBucket(feeBucket).
			NestedReadWriteBucket(samplesBucket)

		var toDelete [][]byte
		c := samples.ReadCursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			if !pred(heightFromKey(k)) {
				continue
			}
			cp := make([]byte, len(k))
			copy(cp, k)
			toDelete = append(toDelete, cp)
		}

		for _, k := range toDelete {
			if err := samples.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
}

// readTip parses the tip key out of the meta bucket. Returns 0 when absent.
func readTip(meta walletdb.ReadBucket) uint32 {
	v := meta.Get(tipKey)
	if len(v) != 4 {
		return 0
	}
	return binary.BigEndian.Uint32(v)
}

// writeTip stamps a new tip height into the meta bucket.
func writeTip(meta walletdb.ReadWriteBucket, h uint32) error {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], h)
	return meta.Put(tipKey, buf[:])
}
