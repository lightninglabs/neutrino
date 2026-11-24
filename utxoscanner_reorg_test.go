package neutrino

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/stretchr/testify/require"
)

// swapChain is an in-memory chain whose block at a height can be replaced
// while the scanner has a fetch for it in flight.
type swapChain struct {
	mu        sync.Mutex
	tipHeight int64
	canonical map[int64]chainhash.Hash
	blocks    map[chainhash.Hash]*wire.MsgBlock

	// onFetch runs inside GetBlock before it answers.
	onFetch func(hash chainhash.Hash) error
}

// newSwapChain returns an empty chain with its tip at tipHeight.
func newSwapChain(tipHeight int64) *swapChain {
	return &swapChain{
		tipHeight: tipHeight,
		canonical: make(map[int64]chainhash.Hash),
		blocks:    make(map[chainhash.Hash]*wire.MsgBlock),
	}
}

// add makes blk the active block at height and returns its hash.
func (c *swapChain) add(height int64, blk *wire.MsgBlock) chainhash.Hash {
	c.mu.Lock()
	defer c.mu.Unlock()

	hash := blk.BlockHash()
	c.blocks[hash] = blk
	c.canonical[height] = hash

	return hash
}

// config returns a scanner config over the chain.
func (c *swapChain) config() *UtxoScannerConfig {
	return &UtxoScannerConfig{
		BestSnapshot: func() (*headerfs.BlockStamp, error) {
			c.mu.Lock()
			defer c.mu.Unlock()

			return &headerfs.BlockStamp{
				Hash:   c.canonical[c.tipHeight],
				Height: int32(c.tipHeight),
			}, nil
		},
		GetBlockHash: func(height int64) (*chainhash.Hash, error) {
			c.mu.Lock()
			defer c.mu.Unlock()

			hash, ok := c.canonical[height]
			if !ok {
				return nil, fmt.Errorf("no block at %d", height)
			}

			return &hash, nil
		},
		BlockFilterMatches: func(*rescanOptions,
			*chainhash.Hash) (bool, error) {

			return true, nil
		},
		GetBlock: func(hash chainhash.Hash,
			_ ...QueryOption) (*btcutil.Block, error) {

			if c.onFetch != nil {
				if err := c.onFetch(hash); err != nil {
					return nil, err
				}
			}

			c.mu.Lock()
			defer c.mu.Unlock()

			blk, ok := c.blocks[hash]
			if !ok {
				return nil, fmt.Errorf("unknown block %v", hash)
			}

			return btcutil.NewBlock(blk), nil
		},
	}
}

// runScan starts a scanner over cfg, asks it about outpoint created at
// birthHeight, and returns the result.
func runScan(t *testing.T, cfg *UtxoScannerConfig, outpoint wire.OutPoint,
	birthHeight uint32) (*SpendReport, error) {

	t.Helper()

	scanner := NewUtxoScanner(cfg)
	require.NoError(t, scanner.Start())
	t.Cleanup(func() { require.NoError(t, scanner.Stop()) })

	req, err := scanner.Enqueue(&InputWithScript{
		OutPoint: outpoint,
		PkScript: []byte{0x51},
	}, birthHeight, nil)
	require.NoError(t, err)

	cancel := make(chan struct{})
	timer := time.AfterFunc(10*time.Second, func() { close(cancel) })
	defer timer.Stop()

	report, err := req.Result(cancel)
	require.NotErrorIs(t, err, ErrGetUtxoCancelled,
		"the scanner never answered")

	return report, err
}

// TestUtxoScannerSkipsBlockReplacedInFlight checks that a block replaced at
// its height while the scanner fetched it isn't processed: a spend that only
// exists on the abandoned block must not be reported.
func TestUtxoScannerSkipsBlockReplacedInFlight(t *testing.T) {
	t.Parallel()

	created := wire.NewMsgTx(2)
	created.AddTxOut(wire.NewTxOut(5000, []byte{0x51}))
	outpoint := wire.OutPoint{Hash: created.TxHash()}

	spend := wire.NewMsgTx(2)
	spend.AddTxIn(&wire.TxIn{PreviousOutPoint: outpoint})
	spend.AddTxOut(wire.NewTxOut(4000, []byte{0x51}))

	chain := newSwapChain(2)
	birth := &wire.MsgBlock{
		Header:       wire.BlockHeader{Nonce: 1},
		Transactions: []*wire.MsgTx{created},
	}
	birthHash := chain.add(1, birth)
	orphan := chain.add(2, &wire.MsgBlock{
		Header: wire.BlockHeader{
			PrevBlock: birthHash, Nonce: 2,
		},
		Transactions: []*wire.MsgTx{spend},
	})
	replacement := &wire.MsgBlock{
		Header: wire.BlockHeader{
			PrevBlock: birthHash, Nonce: 3,
		},
		Transactions: []*wire.MsgTx{wire.NewMsgTx(2)},
	}

	// The replacement lands while the orphan's fetch is in flight.
	var once sync.Once
	chain.onFetch = func(hash chainhash.Hash) error {
		if hash == orphan {
			once.Do(func() { chain.add(2, replacement) })
		}

		return nil
	}

	report, err := runScan(t, chain.config(), outpoint, 1)
	require.NoError(t, err)
	require.NotNil(t, report)
	require.Nil(t, report.SpendingTx, "reported a spend from a block "+
		"that is no longer active")
}

// TestUtxoScannerFailsRequestOnFetchError checks that a request whose birth
// block can't be fetched gets the error, rather than never being answered.
func TestUtxoScannerFailsRequestOnFetchError(t *testing.T) {
	t.Parallel()

	created := wire.NewMsgTx(2)
	created.AddTxOut(wire.NewTxOut(5000, []byte{0x51}))
	outpoint := wire.OutPoint{Hash: created.TxHash()}

	chain := newSwapChain(1)
	chain.add(1, &wire.MsgBlock{
		Header:       wire.BlockHeader{Nonce: 1},
		Transactions: []*wire.MsgTx{created},
	})
	errFetch := errors.New("fetch failed")
	chain.onFetch = func(chainhash.Hash) error { return errFetch }

	_, err := runScan(t, chain.config(), outpoint, 1)
	require.ErrorIs(t, err, errFetch)
}

// TestUtxoScannerRechecksSkippedBlock checks that a block skipped because its
// filter didn't match is checked again against the active chain: if a block
// that spends the outpoint replaced it in the meantime, the spend must be
// found.
func TestUtxoScannerRechecksSkippedBlock(t *testing.T) {
	t.Parallel()

	created := wire.NewMsgTx(2)
	created.AddTxOut(wire.NewTxOut(5000, []byte{0x51}))
	outpoint := wire.OutPoint{Hash: created.TxHash()}

	spend := wire.NewMsgTx(2)
	spend.AddTxIn(&wire.TxIn{PreviousOutPoint: outpoint})
	spend.AddTxOut(wire.NewTxOut(4000, []byte{0x51}))

	chain := newSwapChain(2)
	birthHash := chain.add(1, &wire.MsgBlock{
		Header:       wire.BlockHeader{Nonce: 1},
		Transactions: []*wire.MsgTx{created},
	})
	stale := chain.add(2, &wire.MsgBlock{
		Header:       wire.BlockHeader{PrevBlock: birthHash, Nonce: 2},
		Transactions: []*wire.MsgTx{wire.NewMsgTx(2)},
	})
	replacement := &wire.MsgBlock{
		Header:       wire.BlockHeader{PrevBlock: birthHash, Nonce: 3},
		Transactions: []*wire.MsgTx{spend},
	}
	replacementHash := replacement.BlockHash()

	// The stale block's filter doesn't match, and the replacement lands
	// while that filter is checked. The replacement's filter matches.
	cfg := chain.config()
	var once sync.Once
	cfg.BlockFilterMatches = func(_ *rescanOptions,
		hash *chainhash.Hash) (bool, error) {

		switch *hash {
		case stale:
			once.Do(func() { chain.add(2, replacement) })
			return false, nil
		case replacementHash:
			return true, nil
		}

		return true, nil
	}

	report, err := runScan(t, cfg, outpoint, 1)
	require.NoError(t, err)
	require.NotNil(t, report)
	require.NotNil(t, report.SpendingTx, "missed the spend in the block "+
		"that replaced a skipped one")
	require.Equal(t, spend.TxHash(), report.SpendingTx.TxHash())
}

// TestUtxoScannerStopCancelsFetch checks that stopping the scanner cancels a
// block fetch in flight, rather than waiting for the query to time out, and
// that the request is told the scanner is shutting down.
func TestUtxoScannerStopCancelsFetch(t *testing.T) {
	t.Parallel()

	created := wire.NewMsgTx(2)
	created.AddTxOut(wire.NewTxOut(5000, []byte{0x51}))
	outpoint := wire.OutPoint{Hash: created.TxHash()}

	chain := newSwapChain(1)
	chain.add(1, &wire.MsgBlock{
		Header:       wire.BlockHeader{Nonce: 1},
		Transactions: []*wire.MsgTx{created},
	})

	// The fetch blocks until its query is cancelled.
	cfg := chain.config()
	fetching := make(chan struct{})
	cfg.GetBlock = func(_ chainhash.Hash,
		opts ...QueryOption) (*btcutil.Block, error) {

		qo := defaultQueryOptions()
		qo.applyQueryOptions(opts...)
		close(fetching)

		select {
		case <-qo.cancelChan:
			return nil, errors.New("query cancelled")
		case <-time.After(time.Minute):
			return nil, errors.New("query timed out")
		}
	}

	scanner := NewUtxoScanner(cfg)
	require.NoError(t, scanner.Start())
	req, err := scanner.Enqueue(&InputWithScript{
		OutPoint: outpoint,
		PkScript: []byte{0x51},
	}, 1, nil)
	require.NoError(t, err)
	<-fetching

	stopped := make(chan error, 1)
	go func() { stopped <- scanner.Stop() }()
	select {
	case err := <-stopped:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Stop waited for the fetch in flight")
	}

	_, err = req.Result(nil)
	require.ErrorIs(t, err, ErrShuttingDown)
}
