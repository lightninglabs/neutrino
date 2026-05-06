package feeest

import (
	"errors"
	"sync"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/lightninglabs/neutrino/feedb"
)

// DefaultRingSize is the default capacity of the in-memory ring used by the
// estimator (~1 day at 10-min blocks). The on-disk store keeps a wider window
// for cold-start recovery and longer-target estimates.
const DefaultRingSize = 144

// DefaultRetentionBlocks is how many blocks of samples are kept on disk. ~7
// days mainnet, matching Bitcoin Core's longest-horizon estimator window.
const DefaultRetentionBlocks uint32 = 1008

// SpamFractionThreshold is the threshold above which a single transaction is
// considered to dominate a block's fees, marking the sample as "spam". Set
// to 0.5: any block where one tx contributes >50% of total fees is flagged.
const SpamFractionThreshold = 0.5

// Sampler observes fully-fetched blocks and produces FeeSamples that flow
// into both an in-memory ring and the on-disk feedb store. Observe is safe
// for concurrent use; it is the chokepoint called from ChainService.GetBlock.
type Sampler struct {
	store     feedb.FeeSampleStore
	ring      *ring
	params    *chaincfg.Params
	retention uint32

	// gcMu serialises the retention-window GC walk; we don't want
	// concurrent Observe calls each spawning their own purge.
	gcMu sync.Mutex
}

// SamplerConfig configures a new Sampler.
type SamplerConfig struct {
	// Store is the durable backing store for samples. Required.
	Store feedb.FeeSampleStore

	// Params is the active chain parameters; needed to compute the block
	// subsidy (and therefore total block fees from the coinbase).
	Params *chaincfg.Params

	// RingSize is the capacity of the in-memory ring buffer. Zero falls
	// back to DefaultRingSize.
	RingSize int

	// Retention is the on-disk retention window in blocks. Zero falls
	// back to DefaultRetentionBlocks.
	Retention uint32
}

// NewSampler builds a Sampler and warm-loads the in-memory ring from the
// store. A failure to warm-load is logged but not fatal; the ring will refill
// from new observations.
func NewSampler(cfg SamplerConfig) (*Sampler, error) {
	if cfg.Store == nil {
		return nil, errors.New("feeest: nil store")
	}
	if cfg.Params == nil {
		return nil, errors.New("feeest: nil chain params")
	}

	ringSize := cfg.RingSize
	if ringSize <= 0 {
		ringSize = DefaultRingSize
	}
	retention := cfg.Retention
	if retention == 0 {
		retention = DefaultRetentionBlocks
	}

	s := &Sampler{
		store:     cfg.Store,
		ring:      newRing(ringSize),
		params:    cfg.Params,
		retention: retention,
	}

	// Warm-load the ring from disk. Samples are returned newest-first, so
	// reverse them when adding to preserve chronological order.
	persisted, err := cfg.Store.FetchTipN(ringSize)
	if err != nil {
		log.Warnf("Could not warm-load fee samples: %v", err)
	} else {
		for i := len(persisted) - 1; i >= 0; i-- {
			s.ring.add(*persisted[i])
		}
		log.Debugf("Warm-loaded %d fee samples", len(persisted))
	}

	return s, nil
}

// Observe processes a block, computes a FeeSample and persists it. The block
// must have been sanity-checked by the caller; observation does not validate
// the block. height is the block's height in the active chain.
//
// Errors are logged and swallowed: fee sampling is best-effort and must not
// disrupt the caller.
func (s *Sampler) Observe(block *btcutil.Block, height uint32) {
	sample, err := computeSample(block, height, s.params)
	if err != nil {
		log.Debugf("Skipping fee sample at h=%d: %v", height, err)
		return
	}

	s.ring.add(sample)

	if err := s.store.PutSample(&sample); err != nil {
		log.Warnf("Persisting fee sample h=%d: %v", height, err)
		return
	}

	// Trigger a GC walk if this sample advanced the tip past the
	// retention horizon. Cheap when there is nothing to delete.
	if height > s.retention {
		go s.gcTo(height - s.retention)
	}
}

// gcTo deletes samples older than cutoff. Runs in its own goroutine; the
// gcMu serialises so concurrent Observes don't pile up purges.
func (s *Sampler) gcTo(cutoff uint32) {
	s.gcMu.Lock()
	defer s.gcMu.Unlock()
	if err := s.store.PurgeBefore(cutoff); err != nil {
		log.Warnf("Pruning fee samples before h=%d: %v", cutoff, err)
	}
}

// Snapshot returns a chronological copy of the in-memory ring. Used by the
// estimator on every query.
func (s *Sampler) Snapshot() []feedb.FeeSample {
	return s.ring.snapshot()
}

// computeSample derives a FeeSample from a block. It computes total fees as
// (coinbase output value) - (block subsidy), and total weight via
// blockchain.GetBlockWeight. Both values are derivable from the block alone,
// without prevout knowledge.
func computeSample(block *btcutil.Block, height uint32,
	params *chaincfg.Params) (feedb.FeeSample, error) {

	if block == nil {
		return feedb.FeeSample{}, errors.New("nil block")
	}
	msg := block.MsgBlock()
	if len(msg.Transactions) == 0 {
		return feedb.FeeSample{}, errors.New("block has no transactions")
	}

	coinbase := msg.Transactions[0]
	var coinbaseValue int64
	for _, out := range coinbase.TxOut {
		coinbaseValue += out.Value
	}

	subsidy := blockchain.CalcBlockSubsidy(int32(height), params)

	// A coinbase paying less than the subsidy is invalid, but some
	// regtest blocks paying exactly the subsidy carry zero fees. Both
	// degenerate cases produce a zero-fee, "empty" sample that the
	// estimator drops at aggregation time.
	var totalFees uint64
	if coinbaseValue > subsidy {
		totalFees = uint64(coinbaseValue - subsidy)
	}

	totalWeight := uint64(blockchain.GetBlockWeight(block))
	if totalWeight == 0 {
		return feedb.FeeSample{}, errors.New("block weight is zero")
	}

	var flags feedb.SampleFlag
	if totalFees == 0 {
		flags |= feedb.FlagEmpty
	}
	// FlagSpam (single tx dominating block fees) requires per-tx fee
	// data, which needs prevout lookups we do not implement yet. The
	// flag stays in the schema so the estimator already knows to treat
	// it as a down-weight signal once a UTXO cache lands.

	return feedb.FeeSample{
		Height:      height,
		BlockHash:   *block.Hash(),
		Timestamp:   msg.Header.Timestamp.Unix(),
		TotalFees:   totalFees,
		TotalWeight: totalWeight,
		Flags:       flags,
	}, nil
}
