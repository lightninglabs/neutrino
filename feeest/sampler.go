package feeest

import (
	"errors"
	"math"
	"sync"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
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
// into both an in-memory window and the on-disk feedb store. Observe is safe
// for concurrent use; it is the chokepoint called from ChainService.GetBlock.
//
// The background retention GC worker is spawned by Start and shut down by
// Stop; both are idempotent, and Stop blocks until the worker has fully
// exited so the backing store can be closed safely afterwards.
type Sampler struct {
	store      feedb.FeeSampleStore
	window     *sampleWindow
	params     *chaincfg.Params
	retention  uint32
	bestHeight func() (uint32, error)

	// gcTrigger is a size-1 channel that coalesces GC requests. The
	// background gcWorker drains it. Non-blocking sends skip scheduling
	// when a purge is already pending, avoiding goroutine pile-up when
	// bulk-fetching blocks that are all above the retention horizon.
	gcTrigger chan uint32

	// quit is closed by Stop() to shut down the background GC worker.
	quit chan struct{}

	startOnce sync.Once
	stopOnce  sync.Once
	wg        sync.WaitGroup
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

	// BestHeight, when non-nil, returns the height of the best known
	// block header. Observe uses it to skip historical blocks that sit
	// below the retention horizon entirely: such samples could never
	// enter the estimator window with a competitive timestamp and their
	// rows would be deleted by the very next GC pass, so computing and
	// persisting them during a deep rescan is pure write-then-delete
	// churn. Optional; nil disables the early-out.
	BestHeight func() (uint32, error)
}

// NewSampler builds a Sampler and warm-loads the in-memory ring from the
// store. A failure to warm-load is logged but not fatal; the ring will refill
// from new observations. Callers must call Start() before observations begin
// flowing (it launches the retention GC worker) and Stop() when done.
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
		store:      cfg.Store,
		window:     newSampleWindow(ringSize),
		params:     cfg.Params,
		retention:  retention,
		bestHeight: cfg.BestHeight,
		gcTrigger:  make(chan uint32, 1),
		quit:       make(chan struct{}),
	}

	// Warm-load the window from disk. The window orders by height itself,
	// so insertion order does not matter.
	persisted, err := cfg.Store.FetchTipN(ringSize)
	if err != nil {
		log.Warnf("Could not warm-load fee samples: %v", err)
	} else {
		for _, sample := range persisted {
			s.window.add(*sample)
		}
		log.Debugf("Warm-loaded %d fee samples", len(persisted))
	}

	return s, nil
}

// Start launches the background retention GC worker. Spawning it here rather
// than in the constructor means an error return from a partially-constructed
// chain service cannot leak the goroutine. Start is idempotent.
func (s *Sampler) Start() {
	s.startOnce.Do(func() {
		s.wg.Add(1)
		go s.gcWorker()
	})
}

// Stop shuts down the background GC worker and blocks until it has fully
// exited, so callers can safely close the backing store afterwards. Stop is
// idempotent and safe to call whether or not Start ever ran.
func (s *Sampler) Stop() {
	s.stopOnce.Do(func() {
		close(s.quit)
	})
	s.wg.Wait()
}

// gcWorker is the single background goroutine that executes retention-window
// purges. It reads cutoff heights from gcTrigger, which is a buffered
// channel of size 1. Sends from Observe are non-blocking, so only one
// purge is ever pending at a time regardless of how many blocks land.
func (s *Sampler) gcWorker() {
	defer s.wg.Done()

	for {
		select {
		case cutoff := <-s.gcTrigger:
			if err := s.store.PurgeBefore(cutoff); err != nil {
				log.Warnf("Pruning fee samples before h=%d: %v",
					cutoff, err)
			}
		case <-s.quit:
			return
		}
	}
}

// Observe processes a block, computes a FeeSample and persists it. The block
// must have been sanity-checked by the caller; observation does not validate
// the block. height is the block's height in the active chain.
//
// Observe is idempotent: if the window already contains a sample for the
// block's hash, the call is a no-op. This prevents concurrent GetBlock calls
// for the same block from double-counting the sample in the estimator window.
//
// Errors are logged and swallowed: fee sampling is best-effort and must not
// disrupt the caller.
func (s *Sampler) Observe(block *btcutil.Block, height uint32) {
	// Skip blocks that already sit below the retention horizon: a deep
	// rescan would otherwise compute and persist thousands of samples
	// whose rows the next GC pass deletes, and whose timestamps are far
	// too old to carry weight in the estimator window anyway.
	if s.bestHeight != nil {
		if tip, err := s.bestHeight(); err == nil &&
			height+s.retention < tip {

			return
		}
	}

	sample, err := computeSample(block, height, s.params)
	if err != nil {
		log.Debugf("Skipping fee sample at h=%d: %v", height, err)
		return
	}

	// add returns false when the window already holds a sample for this
	// block hash (idempotence under concurrent fetches), or when the
	// sample is older than the full window's horizon (e.g. a historical
	// block pulled in by a rescan). In the latter case we still persist:
	// the disk store keeps the wider retention window.
	if !s.window.add(sample) {
		log.Tracef("Fee sample not admitted to window h=%d hash=%s",
			height, sample.BlockHash)
	}

	log.Debugf("Fee sample h=%d hash=%s rate=%d sat/kW fees=%d weight=%d "+
		"knownMin=%d knownCnt=%d flags=%d", height, sample.BlockHash,
		sample.FeeRatePerKW(), sample.TotalFees, sample.TotalWeight,
		sample.MinKnownTxRate, sample.KnownTxCount, sample.Flags)

	if err := s.store.PutSample(&sample); err != nil {
		log.Warnf("Persisting fee sample h=%d: %v", height, err)
		return
	}

	// Schedule a GC pass if this sample advanced the tip past the
	// retention horizon. A non-blocking send coalesces concurrent
	// triggers into a single pending purge.
	if height > s.retention {
		select {
		case s.gcTrigger <- height - s.retention:
		default:
		}
	}
}

// Snapshot returns a height-ordered copy of the in-memory window. Used by
// the estimator on every query.
func (s *Sampler) Snapshot() []feedb.FeeSample {
	return s.window.snapshot()
}

// PruneFrom drops all samples at or above the given height from both the
// in-memory window and the durable store. It is called from the block
// manager's reorg path so observations from an orphaned chain never feed an
// estimate. The disk purge is synchronous; reorgs are rare and shallow, so
// the extra latency on the reorg path is preferable to serving stale samples
// while an async purge is pending.
func (s *Sampler) PruneFrom(height uint32) {
	s.window.prune(func(sample feedb.FeeSample) bool {
		return sample.Height >= height
	})
	if err := s.store.PurgeFrom(height); err != nil {
		log.Warnf("Purging fee samples from h=%d: %v", height, err)
	}
}

// computeSample derives a FeeSample from a block. It computes total fees as
// (coinbase output value) - (block subsidy), and total weight via
// blockchain.GetBlockWeight. Both values are derivable from the block alone,
// without prevout knowledge.
//
// It additionally extracts exact per-transaction fee rates for transactions
// whose inputs are all outputs created earlier in the same block (CPFP chains
// and other intra-block spends). Those are the only transactions whose fees a
// light client can compute without a UTXO set, and the minimum such rate is a
// tighter upper bound on the block's marginal entry rate than the block-wide
// average.
func computeSample(block *btcutil.Block, height uint32,
	params *chaincfg.Params) (feedb.FeeSample, error) {

	if block == nil {
		return feedb.FeeSample{}, errors.New("nil block")
	}
	// CalcBlockSubsidy takes int32. Bitcoin's practical halving schedule
	// puts current heights well inside int32 range, but guard against
	// the theoretical overflow at height > 2^31-1.
	if height > math.MaxInt32 {
		return feedb.FeeSample{}, errors.New("block height overflows int32")
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

	// Miners may claim less than the full coinbase reward. When that
	// happens coinbaseValue <= subsidy, which yields zero fees. That
	// observation is valid (zero-fee block) and carries the FlagEmpty
	// flag so the estimator ignores it for signal purposes.
	var totalFees uint64
	if coinbaseValue > subsidy {
		totalFees = uint64(coinbaseValue - subsidy)
	}

	totalWeight := uint64(blockchain.GetBlockWeight(block))
	if totalWeight == 0 {
		return feedb.FeeSample{}, errors.New("block weight is zero")
	}

	txs := block.Transactions()
	coinbaseWeight := uint64(blockchain.GetTransactionWeight(txs[0]))

	minKnownRate, knownCount, maxKnownFee := knownTxRates(txs)

	var flags feedb.SampleFlag
	if totalFees == 0 {
		flags |= feedb.FlagEmpty
	}
	// If a single transaction whose fee we could compute exactly
	// contributes the majority of the block's fees, the block-average
	// rate is dominated by that one payer and is a poor congestion
	// signal. Flag it so consumers can prefer the per-tx bound.
	if totalFees > 0 && float64(maxKnownFee) >
		SpamFractionThreshold*float64(totalFees) {

		flags |= feedb.FlagSpam
	}

	return feedb.FeeSample{
		Height:         height,
		BlockHash:      *block.Hash(),
		Timestamp:      msg.Header.Timestamp.Unix(),
		TotalFees:      totalFees,
		TotalWeight:    totalWeight,
		CoinbaseWeight: coinbaseWeight,
		MinKnownTxRate: minKnownRate,
		KnownTxCount:   knownCount,
		Flags:          flags,
	}, nil
}

// knownTxRates scans a block's transactions for those whose prevouts are all
// created within the same block, computes their exact fees and fee rates, and
// returns the minimum rate (sat/kW), the number of such transactions, and the
// largest single fee among them. Returns (0, 0, 0) when no transaction
// qualifies.
func knownTxRates(txs []*btcutil.Tx) (uint64, uint16, uint64) {
	// Index the block's own outputs so we can resolve intra-block spends.
	outputs := make(map[chainhash.Hash]*wire.MsgTx, len(txs))
	for _, tx := range txs {
		outputs[*tx.Hash()] = tx.MsgTx()
	}

	var (
		minRate     uint64
		count       uint16
		maxKnownFee uint64
	)
	for _, tx := range txs[1:] {
		msgTx := tx.MsgTx()

		// Sum input values; bail on the first prevout we cannot
		// resolve within this block.
		var inValue int64
		known := true
		for _, in := range msgTx.TxIn {
			prev, ok := outputs[in.PreviousOutPoint.Hash]
			if !ok {
				known = false
				break
			}
			idx := in.PreviousOutPoint.Index
			if idx >= uint32(len(prev.TxOut)) {
				known = false
				break
			}
			inValue += prev.TxOut[idx].Value
		}
		if !known {
			continue
		}

		var outValue int64
		for _, out := range msgTx.TxOut {
			outValue += out.Value
		}

		fee := inValue - outValue
		weight := blockchain.GetTransactionWeight(tx)
		if fee < 0 || weight <= 0 {
			continue
		}

		rate := uint64(fee) * 1000 / uint64(weight)
		if count == 0 || rate < minRate {
			minRate = rate
		}
		if uint64(fee) > maxKnownFee {
			maxKnownFee = uint64(fee)
		}
		if count < math.MaxUint16 {
			count++
		}
	}

	return minRate, count, maxKnownFee
}
