package neutrino

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/btcutil/v2/gcs"
	"github.com/btcsuite/btcd/btcutil/v2/gcs/builder"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/blockdn"
	"github.com/lightninglabs/neutrino/cache"
	"github.com/lightninglabs/neutrino/filterdb"
)

func (s *ChainService) startBlockDN(ctx context.Context) error {
	s.blockSubscriptionMgr.Start()
	if err := s.utxoScanner.Start(); err != nil {
		return err
	}
	if s.persistToDisk {
		s.filterBatchWriter.Start()
	}
	s.blockDN.start(ctx)
	return s.broadcaster.Start()
}

func (s *ChainService) stopBlockDN() error {
	// Cancel downloads before waiting for scanners or rebroadcast
	// callbacks.
	close(s.quit)
	s.blockDN.stop()
	var stopErr error
	if atomic.LoadInt32(&s.started) != 0 {
		s.broadcaster.Stop()
		stopErr = s.utxoScanner.Stop()
		s.blockSubscriptionMgr.Stop()
		if s.persistToDisk {
			s.filterBatchWriter.Stop()
		}
	}
	if s.sqlBackend != nil {
		stopErr = errors.Join(stopErr, s.sqlBackend.Close())
	}
	return stopErr
}

func (b *blockDNSync) queryContext(
	options []QueryOption,
) (context.Context, context.CancelFunc, *queryOptions, error) {

	qo := defaultQueryOptions()
	qo.applyQueryOptions(options...)
	b.mu.RLock()
	parent := b.ctx
	b.mu.RUnlock()
	if parent == nil {
		return nil, nil, nil, fmt.Errorf(
			"block-dn service has not started",
		)
	}
	timeout := qo.timeout
	if timeout <= 0 {
		timeout = 45 * time.Second
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	go func() {
		select {
		case <-qo.cancelChan:
			cancel()
		case <-b.s.quit:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel, qo, nil
}

func (b *blockDNSync) cachedFilter(hash chainhash.Hash) (*gcs.Filter, error) {
	f, err := b.s.getFilterFromCache(&hash, filterdb.RegularFilter)
	if err == nil && f != nil {
		return f, nil
	}
	if err != nil && err != cache.ErrElementNotFound {
		return nil, err
	}
	f, err = b.s.FilterDB.FetchFilter(&hash, filterdb.RegularFilter)
	if err == filterdb.ErrFilterNotFound {
		return nil, nil
	}
	return f, err
}

func (b *blockDNSync) getFilter(
	hash chainhash.Hash,
	filterType wire.FilterType,
	options ...QueryOption,
) (*gcs.Filter, error) {

	if filterType != wire.GCSFilterRegular {
		return nil, fmt.Errorf("unknown filter type %v", filterType)
	}
	if filter, err := b.cachedFilter(hash); filter != nil || err != nil {
		return filter, err
	}
	ctx, cancel, qo, err := b.queryContext(options)
	if err != nil {
		return nil, err
	}
	defer cancel()
	select {
	case b.filterSem <- struct{}{}:
		defer func() { <-b.filterSem }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if filter, err := b.cachedFilter(hash); filter != nil || err != nil {
		return filter, err
	}
	b.mu.RLock()
	_, height, err := b.s.BlockHeaders.FetchHeader(&hash)
	var tipHash chainhash.Hash
	var tipHeight uint32
	if err == nil {
		_, tipHeight, err = b.s.RegFilterHeaders.ChainTip()
	}
	if err == nil {
		var tip *wire.BlockHeader
		tip, err = b.s.BlockHeaders.FetchHeaderByHeight(tipHeight)
		if err == nil {
			tipHash = tip.BlockHash()
		}
	}
	b.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	client := b.client.AtTip(tipHeight, tipHash)
	start := height
	var raw [][]byte
	if qo.optimisticBatch != noBatch &&
		(qo.maxBatchSize <= 0 ||
			qo.maxBatchSize >= int64(blockdn.FiltersPerFile)) {

		start = height / blockdn.FiltersPerFile * blockdn.FiltersPerFile
		raw, err = client.Filters(ctx, start)
	} else {
		var data []byte
		data, err = client.Filter(ctx, height)
		raw = [][]byte{data}
	}
	if err != nil {
		return nil, err
	}
	// Check the whole response before adding any filter to the cache. A
	// reorg or stale CDN response therefore cannot poison subsequent scans.
	b.mu.RLock()
	defer b.mu.RUnlock()
	active, err := b.s.BlockHeaders.FetchHeaderByHeight(height)
	if err != nil || active.BlockHash() != hash {
		return nil, fmt.Errorf("chain changed during filter fetch")
	}
	_, tip, err := b.s.RegFilterHeaders.ChainTip()
	if err != nil {
		return nil, err
	}
	var data []*filterdb.FilterData
	var result *gcs.Filter
	for i, bytes := range raw {
		h := start + uint32(i)
		if h == 0 || h > tip {
			continue
		}
		filter, err := gcs.FromNBytes(
			builder.DefaultP,
			builder.DefaultM,
			bytes,
		)
		if err != nil {
			return nil, err
		}
		previous, err := b.s.RegFilterHeaders.FetchHeaderByHeight(h - 1)
		if err != nil {
			return nil, err
		}
		expected, err := b.s.RegFilterHeaders.FetchHeaderByHeight(h)
		if err != nil {
			return nil, err
		}
		commitment, err := builder.MakeHeaderForFilter(
			filter,
			*previous,
		)
		if err != nil {
			return nil, err
		}
		if commitment != *expected {
			return nil, fmt.Errorf(
				"filter commitment mismatch at height %d",
				h,
			)
		}
		header, err := b.s.BlockHeaders.FetchHeaderByHeight(h)
		if err != nil {
			return nil, err
		}
		blockHash := header.BlockHash()
		data = append(
			data,
			&filterdb.FilterData{
				BlockHash: &blockHash,
				Filter:    filter,
				Type:      filterdb.RegularFilter,
			},
		)
		if h == height {
			result = filter
		}
	}
	if result == nil {
		return nil, ErrFilterFetchFailed
	}
	for _, d := range data {
		_, err := b.s.putFilterToCache(d.BlockHash, d.Type, d.Filter)
		if err != nil {
			log.Warnf("Unable to cache HTTP filter %s: %v",
				d.BlockHash, err)
		}
	}
	if b.s.persistToDisk {
		if err := b.s.FilterDB.PutFilters(data...); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (b *blockDNSync) getBlock(
	hash chainhash.Hash,
	options ...QueryOption,
) (*btcutil.Block, error) {

	ctx, cancel, _, err := b.queryContext(options)
	if err != nil {
		return nil, err
	}
	defer cancel()
	b.mu.RLock()
	_, height, err := b.s.BlockHeaders.FetchHeader(&hash)
	b.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	inv := *wire.NewInvVect(wire.InvTypeWitnessBlock, &hash)
	if cached, err := b.s.BlockCache.Get(inv); err == nil && cached != nil {
		return cached.Block, nil
	}
	msg, err := b.client.Block(ctx, hash)
	if err != nil {
		return nil, err
	}
	block := btcutil.NewBlock(msg)
	block.SetHeight(int32(height))
	err = blockchain.CheckBlockSanity(
		block, b.s.chainParams.PowLimit, b.s.timeSource,
	)
	if err != nil {
		return nil, err
	}
	if err := blockchain.ValidateWitnessCommitment(block); err != nil {
		return nil, err
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	header, err := b.s.BlockHeaders.FetchHeaderByHeight(height)
	if err != nil || header.BlockHash() != hash {
		return nil, fmt.Errorf("chain changed during block fetch")
	}
	_, err = b.s.BlockCache.Put(inv, &CacheableBlock{Block: block})
	if err != nil {
		log.Warnf("Unable to cache HTTP block %s: %v", hash, err)
	}
	return block, nil
}
