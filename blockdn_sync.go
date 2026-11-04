package neutrino

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"net/http"
	"sync"
	"time"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/btcutil/v2/gcs"
	"github.com/btcsuite/btcd/btcutil/v2/gcs/builder"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/blockdn"
	"github.com/lightninglabs/neutrino/blockntfns"
	"github.com/lightninglabs/neutrino/chainsync"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/lightninglabs/neutrino/headerlist"
	"github.com/lightninglabs/neutrino/headersync"
)

// ErrBlockDNReadOnly indicates that HTTP mode has no transaction broadcaster.
var ErrBlockDNReadOnly = errors.New(
	"block-dn mode has no transaction broadcaster",
)

// ErrP2PDisabled indicates that a peer operation was requested in HTTP mode.
var ErrP2PDisabled = errors.New("Bitcoin P2P is disabled in block-dn mode")

// BlockDNConfig selects HTTP-only chain synchronization. The server is trusted
// for freshness and filter-header commitments between hardcoded checkpoints.
// Bitcoin headers, filter contents and downloaded blocks are still validated.
// No Bitcoin P2P connections or DNS seeds are used in this mode.
type BlockDNConfig struct {
	BaseURL    string
	HTTPClient *http.Client

	// WitnessURLs require other block-dn servers to agree on the selected
	// block hash and filter header. Use independently operated servers.
	// A lagging or disagreeing witness pauses sync.
	WitnessURLs []string

	// PollInterval defaults to 15 seconds.
	PollInterval time.Duration
	// MaxReorgDepth bounds rollback depth and replacement length.
	// Deeper reorganizations fail without changing the local chain.
	// Defaults to 10000. Forward initial sync is not subject to this bound.
	MaxReorgDepth uint32
	// Broadcast optionally submits transactions over a caller-provided HTTP
	// service. It must honor ctx cancellation and map already-known errors
	// to pushtx.BroadcastError. Neutrino retains its rebroadcast loop.
	Broadcast func(context.Context, *wire.MsgTx) error
}

// BlockDNSyncStatus is a snapshot of HTTP synchronization progress. Current
// means caught up to a recent observation of the configured server, not an
// independently authenticated view of the Bitcoin network.
type BlockDNSyncStatus struct {
	Height       uint32
	TargetHeight uint32
	Hash         chainhash.Hash
	Current      bool

	// LastSuccess is the start of the last successfully imported server
	// observation. A long import does not extend its freshness.
	LastSuccess time.Time
	LastError   error
}

type blockDNSync struct {
	s             *ChainService
	cfg           BlockDNConfig
	client        *blockdn.Client
	witnesses     []*blockdn.Client
	mu            sync.RWMutex
	status        BlockDNSyncStatus
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	notifications chan blockntfns.BlockNtfn
	filterSem     chan struct{}

	// Publication progress is independent of durable store tips: a store
	// operation can commit successfully and still return an error.
	publishedHeight   uint32
	publishedHash     chainhash.Hash
	pendingDisconnect *blockntfns.Disconnected
}

func newBlockDNSync(s *ChainService, cfg *BlockDNConfig) (*blockDNSync, error) {
	c := *cfg
	if c.PollInterval == 0 {
		c.PollInterval = 15 * time.Second
	}
	if c.PollInterval < 0 {
		return nil, errors.New(
			"block-dn poll interval must be positive",
		)
	}
	if c.MaxReorgDepth == 0 {
		c.MaxReorgDepth = 10000
	}
	if c.MaxReorgDepth > math.MaxInt32 {
		return nil, errors.New(
			"block-dn reorg bound exceeds maximum height",
		)
	}
	client, err := blockdn.NewClient(c.BaseURL, c.HTTPClient)
	if err != nil {
		return nil, err
	}
	if s != nil {
		client = client.ForNetwork(*s.chainParams.GenesisHash)
	}
	var witnesses []*blockdn.Client
	for _, url := range c.WitnessURLs {
		witness, err := blockdn.NewClient(url, c.HTTPClient)
		if err != nil {
			return nil, err
		}
		if s != nil {
			witness = witness.ForNetwork(*s.chainParams.GenesisHash)
		}
		witnesses = append(witnesses, witness)
	}
	b := &blockDNSync{
		s: s, cfg: c, client: client, witnesses: witnesses,
		filterSem:     make(chan struct{}, 1),
		notifications: make(chan blockntfns.BlockNtfn),
	}
	if s != nil {
		tip, err := s.BestBlock()
		if err != nil {
			return nil, err
		}
		b.publishedHeight = uint32(tip.Height)
		b.publishedHash = tip.Hash
		b.status.Height, b.status.Hash = uint32(tip.Height), tip.Hash
	}
	return b, nil
}

// BlockDNStatus returns HTTP sync progress, including the last polling error.
func (s *ChainService) BlockDNStatus() (BlockDNSyncStatus, error) {
	if s.blockDN == nil {
		return BlockDNSyncStatus{}, errors.New(
			"block-dn mode is not enabled",
		)
	}
	b := s.blockDN
	b.mu.RLock()
	defer b.mu.RUnlock()
	status := b.status
	status.Current = status.Current && b.ctx != nil && b.ctx.Err() == nil &&
		time.Since(status.LastSuccess) < 3*b.cfg.PollInterval
	return status, nil
}

func (b *blockDNSync) start(ctx context.Context) {
	b.mu.Lock()
	b.ctx, b.cancel = context.WithCancel(ctx)
	b.mu.Unlock()
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		for {
			err := b.poll(b.ctx)
			if err != nil && b.ctx.Err() == nil {
				log.Warnf("block-dn sync: %v", err)
			}
			timer := time.NewTimer(b.cfg.PollInterval)
			select {
			case <-b.ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()
}

// poll records freshness from the observation's start, rather than from the
// end of a potentially long history import or notification replay.
func (b *blockDNSync) poll(ctx context.Context) error {
	observed := time.Now()
	err := b.syncOnce(ctx)
	b.mu.Lock()
	b.status.LastError = err
	b.status.Current = err == nil
	if err == nil {
		b.status.LastSuccess = observed
	}
	b.mu.Unlock()
	return err
}

func (b *blockDNSync) stop() {
	b.mu.Lock()
	if b.cancel != nil {
		b.cancel()
	}
	b.mu.Unlock()
	b.wg.Wait()
}

// headerFiles retains one pair of files per poll, bounding memory during IBD.
type headerFiles struct {
	client       *blockdn.Client
	start        uint32
	headers      []wire.BlockHeader
	filters      []chainhash.Hash
	entries      map[uint32]httpChainEntry
	pinned       *httpChainEntry
	pinnedHeight uint32
}

type httpChainEntry struct {
	header wire.BlockHeader
	filter chainhash.Hash
}

func (f *headerFiles) at(
	ctx context.Context,
	height uint32,
) (wire.BlockHeader, chainhash.Hash, error) {

	if entry, ok := f.entries[height]; ok {
		return entry.header, entry.filter, nil
	}
	start := height / blockdn.HeadersPerFile * blockdn.HeadersPerFile
	if f.headers == nil || f.start != start {
		headers, err := f.client.Headers(ctx, start)
		if err != nil {
			return wire.BlockHeader{}, chainhash.Hash{}, err
		}
		filters, err := f.client.FilterHeaders(ctx, start)
		if err != nil {
			return wire.BlockHeader{}, chainhash.Hash{}, err
		}
		f.start, f.headers, f.filters = start, headers, filters
	}
	i := height - start
	if i >= uint32(len(f.headers)) || i >= uint32(len(f.filters)) {
		return wire.BlockHeader{}, chainhash.Hash{}, fmt.Errorf(
			"block-dn files do not cover height %d",
			height,
		)
	}
	if f.pinned != nil && height == f.pinnedHeight &&
		(f.headers[i].BlockHash() != f.pinned.header.BlockHash() ||
			f.filters[i] != f.pinned.filter) {

		return wire.BlockHeader{}, chainhash.Hash{}, errors.New(
			"block-dn snapshot changed during sync",
		)
	}
	return f.headers[i], f.filters[i], nil
}

func (b *blockDNSync) syncOnce(ctx context.Context) error {
	if err := b.reconcileNotifications(ctx); err != nil {
		return err
	}
	status, err := b.client.Status(ctx)
	if err != nil {
		return err
	}
	if status.GenesisHash != *b.s.chainParams.GenesisHash {
		return fmt.Errorf(
			"block-dn network mismatch: server genesis %s, "+
				"expected %s",
			status.GenesisHash,
			b.s.chainParams.GenesisHash,
		)
	}
	if status.Height > math.MaxInt32 ||
		status.FilterHeight > status.Height {

		return errors.New("invalid block-dn tip heights")
	}
	target := min(status.Height, status.FilterHeight)
	b.mu.Lock()
	b.status.TargetHeight = target
	b.status.Current = false
	b.mu.Unlock()
	local, localHeight, err := b.s.BlockHeaders.ChainTip()
	if err != nil {
		return err
	}
	localFilter, _, err := b.s.RegFilterHeaders.ChainTip()
	if err != nil {
		return err
	}
	b.mu.Lock()
	b.status.Height, b.status.Hash = localHeight, local.BlockHash()
	b.mu.Unlock()
	if target == localHeight && status.Height == target &&
		local.BlockHash() == status.Hash &&
		*localFilter == status.FilterHeader {

		return b.checkWitnesses(ctx, target, status.Hash, *localFilter)
	}
	tipClient := b.client.AtTip(status.Height, status.Hash)
	files := &headerFiles{client: tipClient}
	// At the live tip, fetch just new headers and filters. Filter hashes
	// derive the new commitments, which must end at the status commitment.
	// A fork falls back to the bounded replacement path below.
	if status.Height == target && target > localHeight &&
		target-localHeight <= 128 {

		entries := make(map[uint32]httpChainEntry)
		hash := status.Hash
		for height := target; height > localHeight; height-- {
			h, err := b.client.Header(ctx, hash)
			if err != nil {
				return err
			}
			entries[height] = httpChainEntry{header: *h}
			hash = h.PrevBlock
		}
		if hash == local.BlockHash() {
			previous := *localFilter
			for h := localHeight + 1; h <= target; h++ {
				raw, err := tipClient.Filter(ctx, h)
				if err != nil {
					return err
				}
				filter, err := gcs.FromNBytes(
					builder.DefaultP,
					builder.DefaultM,
					raw,
				)
				if err != nil {
					return err
				}
				fh, err := builder.MakeHeaderForFilter(
					filter,
					previous,
				)
				if err != nil {
					return err
				}
				entry := entries[h]
				entry.filter = fh
				entries[h] = entry
				previous = fh
			}
			if previous != status.FilterHeader {
				return errors.New(
					"live filter commitment mismatch",
				)
			}
			entries[localHeight] = httpChainEntry{
				header: *local,
				filter: *localFilter,
			}
			files.entries = entries
		}
	}
	remoteTip, filterTip, err := files.at(ctx, target)
	if err != nil {
		return err
	}
	if target == status.Height && remoteTip.BlockHash() != status.Hash {
		return errors.New("block-dn status/header snapshot changed")
	}
	if target == status.FilterHeight && filterTip != status.FilterHeader {
		return errors.New(
			"block-dn status/filter-header snapshot changed",
		)
	}
	err = b.checkWitnesses(ctx, target, remoteTip.BlockHash(), filterTip)
	if err != nil {
		return err
	}
	files.pinned, files.pinnedHeight = &httpChainEntry{
		header: remoteTip,
		filter: filterTip,
	}, target
	ancestor := min(localHeight, target)
	for {
		remote, remoteFilter, err := files.at(ctx, ancestor)
		if err != nil {
			return err
		}
		stored, err := b.s.BlockHeaders.FetchHeaderByHeight(ancestor)
		if err != nil {
			return err
		}
		if remote.BlockHash() == stored.BlockHash() {
			store := b.s.RegFilterHeaders
			storedFilter, err := store.FetchHeaderByHeight(
				ancestor,
			)
			if err != nil {
				return err
			}
			if remoteFilter != *storedFilter {
				return fmt.Errorf(
					"filter commitment mismatch at "+
						"common ancestor %d",
					ancestor,
				)
			}
			break
		}
		if ancestor == 0 ||
			localHeight-ancestor >= b.cfg.MaxReorgDepth {

			return errors.New(
				"fork exceeds limit or has wrong genesis",
			)
		}
		ancestor--
	}
	if ancestor < localHeight {
		if localHeight-ancestor > b.cfg.MaxReorgDepth ||
			target-ancestor > b.cfg.MaxReorgDepth {

			return errors.New(
				"replacement branch exceeds reorg limit",
			)
		}
		blocks, filters, err := b.validateRange(
			ctx,
			files,
			ancestor+1,
			target,
		)
		if err != nil {
			return err
		}
		newWork, oldWork := new(big.Int), new(big.Int)
		for _, h := range blocks {
			newWork.Add(newWork, blockchain.CalcWork(h.Bits))
		}
		for height := ancestor + 1; height <= localHeight; height++ {
			h, err := b.s.BlockHeaders.FetchHeaderByHeight(height)
			if err != nil {
				return err
			}
			oldWork.Add(oldWork, blockchain.CalcWork(h.Bits))
		}
		if newWork.Cmp(oldWork) <= 0 {
			return errors.New(
				"replacement chain has no more work",
			)
		}
		if err := b.rollback(ctx, ancestor); err != nil {
			return err
		}
		if err := b.commit(ctx, blocks, filters); err != nil {
			return err
		}
	} else {
		for start := localHeight + 1; start <= target; {
			end := min(start+1999, target)
			blocks, filters, err := b.validateRange(
				ctx, files, start, end,
			)
			if err != nil {
				return err
			}
			if err := b.commit(ctx, blocks, filters); err != nil {
				return err
			}
			start = end + 1
		}
	}
	if status.FilterHeight < status.Height {
		return errors.New(
			"block-dn filter headers are behind block headers",
		)
	}
	return nil
}

// recoverTips repairs an interrupted paired-store mutation. The caller holds
// mu; publication reconciliation retains any removed published header.
func (b *blockDNSync) recoverTips() error {
	_, height, err := b.s.BlockHeaders.ChainTip()
	if err != nil {
		return err
	}
	_, filterHeight, err := b.s.RegFilterHeaders.ChainTip()
	if err != nil {
		return err
	}
	if filterHeight > height {
		return errors.New("filter-header tip exceeds block-header tip")
	}
	if height > filterHeight {
		_, err := b.s.BlockHeaders.RollbackBlockHeaders(
			height - filterHeight,
		)
		return err
	}
	return nil
}

// reconcileNotifications publishes every durable transition exactly once to
// the notification channel. Only the sync loop calls this method. Cursor
// updates happen after the channel accepts an event, and rollback data stays
// available even if an ambiguous commit has already deleted the old header.
func (b *blockDNSync) reconcileNotifications(ctx context.Context) error {
	b.mu.Lock()
	err := b.recoverTips()
	if err != nil {
		b.mu.Unlock()
		return err
	}
	tip, height, err := b.s.BlockHeaders.ChainTip()
	if err != nil {
		b.mu.Unlock()
		return err
	}
	b.status.Height, b.status.Hash = height, tip.BlockHash()
	b.mu.Unlock()
	// The sync loop is the only store writer, so the tip stays stable while
	// publication catches up. Avoid rereading it for every IBD event.
	for {
		b.mu.Lock()
		next, err := b.nextNotification(tip, height)
		b.mu.Unlock()
		if err != nil || next == nil {
			return err
		}
		if err := b.notify(ctx, next); err != nil {
			return err
		}
		b.mu.Lock()
		nextTip := next.ChainTip()
		b.publishedHash = nextTip.BlockHash()
		b.publishedHeight = next.Height()
		if _, ok := next.(*blockntfns.Disconnected); ok {
			b.publishedHeight--
			b.pendingDisconnect = nil
		}
		b.mu.Unlock()
	}
}

// nextNotification reads durable state without changing publication progress.
// The caller holds mu and is the only writer of stores and publication state.
func (b *blockDNSync) nextNotification(
	tip *wire.BlockHeader, height uint32,
) (blockntfns.BlockNtfn, error) {

	if pending := b.pendingDisconnect; pending != nil {
		pendingHeader := pending.Header()
		if height < pending.Height() {
			if b.publishedHeight != pending.Height() ||
				b.publishedHash != pendingHeader.BlockHash() {

				return nil, errors.New(
					"unexpected publication cursor " +
						"during rollback",
				)
			}
			return pending, nil
		}
		// The failed rollback did not remove this block. Discard its
		// tentative event only after reading back the original header.
		header, err := b.s.BlockHeaders.FetchHeaderByHeight(
			pending.Height(),
		)
		if err != nil {
			return nil, err
		}
		if header.BlockHash() != pendingHeader.BlockHash() {
			return nil, errors.New(
				"chain changed during pending rollback",
			)
		}
		b.pendingDisconnect = nil
	}
	if b.publishedHeight == height && b.publishedHash == tip.BlockHash() {
		return nil, nil
	}
	if b.publishedHeight >= height {
		return nil, errors.New(
			"durable chain diverged from publication cursor",
		)
	}
	nextHeight := b.publishedHeight + 1
	header, err := b.s.BlockHeaders.FetchHeaderByHeight(nextHeight)
	if err != nil {
		return nil, err
	}
	if header.PrevBlock != b.publishedHash {
		return nil, errors.New(
			"durable header does not extend publication cursor",
		)
	}
	return blockntfns.NewBlockConnected(*header, nextHeight), nil
}

func (b *blockDNSync) validateRange(
	ctx context.Context,
	files *headerFiles,
	start, end uint32,
) ([]headerfs.BlockHeader, []headerfs.FilterHeader, error) {

	if start > end {
		return nil, nil, nil
	}
	parent, err := b.s.BlockHeaders.FetchHeaderByHeight(start - 1)
	if err != nil {
		return nil, nil, err
	}
	chain := headerlist.NewBoundedMemoryChain(end - start + 2)
	chain.PushBack(
		headerlist.Node{Height: int32(start - 1), Header: *parent},
	)
	retarget, err := chainsync.NewRetarget(b.s.chainParams)
	if err != nil {
		return nil, nil, err
	}
	validator := headersync.HeaderValidator{
		Params:              &b.s.chainParams,
		BlocksPerRetarget:   retarget.BlocksPerRetarget,
		MinRetargetTimespan: retarget.MinRetargetTimespan,
		MaxRetargetTimespan: retarget.MaxRetargetTimespan,
		TimeSource:          b.s.timeSource,
		Store:               b.s.BlockHeaders,
	}
	var blocks []headerfs.BlockHeader
	var filters []headerfs.FilterHeader
	for height := start; height <= end; height++ {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		h, fh, err := files.at(ctx, height)
		if err != nil {
			return nil, nil, err
		}
		if h.PrevBlock != parent.BlockHash() {
			return nil, nil, fmt.Errorf(
				"broken block-dn header chain at %d",
				height,
			)
		}
		err = validator.Check(
			&h, int32(height-1), parent, chain,
		)
		if err != nil {
			return nil, nil, fmt.Errorf(
				"header %d: %w",
				height,
				err,
			)
		}
		hash := h.BlockHash()
		for _, checkpoint := range b.s.chainParams.Checkpoints {
			if checkpoint.Height == int32(height) &&
				*checkpoint.Hash != hash {

				return nil, nil, fmt.Errorf(
					"block checkpoint mismatch at %d",
					height,
				)
			}
		}
		err = chainsync.ValidateCFHeader(
			b.s.chainParams, wire.GCSFilterRegular, height, &fh,
		)
		if err != nil {
			return nil, nil, err
		}
		blocks = append(
			blocks,
			headerfs.BlockHeader{BlockHeader: &h, Height: height},
		)
		filters = append(
			filters,
			headerfs.FilterHeader{
				HeaderHash: hash,
				FilterHash: fh,
				Height:     height,
			},
		)
		chain.PushBack(
			headerlist.Node{Height: int32(height), Header: h},
		)
		parent = &h
	}
	return blocks, filters, nil
}

func (b *blockDNSync) commit(
	ctx context.Context,
	blocks []headerfs.BlockHeader,
	filters []headerfs.FilterHeader,
) error {

	if len(blocks) == 0 {
		return nil
	}
	b.mu.Lock()
	err := b.s.BlockHeaders.WriteHeaders(blocks...)
	if err == nil {
		err = b.s.RegFilterHeaders.WriteHeaders(filters...)
	}
	b.mu.Unlock()
	return errors.Join(err, b.reconcileNotifications(ctx))
}

func (b *blockDNSync) rollback(ctx context.Context, ancestor uint32) error {
	for {
		if err := b.reconcileNotifications(ctx); err != nil {
			return err
		}
		b.mu.Lock()
		header, height, err := b.s.BlockHeaders.ChainTip()
		if err != nil || height <= ancestor {
			b.mu.Unlock()
			return err
		}
		parent, err := b.s.BlockHeaders.FetchHeaderByHeight(height - 1)
		if err == nil {
			b.pendingDisconnect = blockntfns.NewBlockDisconnected(
				*header, height, *parent,
			)
			hash := parent.BlockHash()
			_, err = b.s.RegFilterHeaders.RollbackLastBlock(&hash)
		}
		if err == nil {
			_, err = b.s.BlockHeaders.RollbackLastBlock()
		}
		b.mu.Unlock()
		err = errors.Join(err, b.reconcileNotifications(ctx))
		if err != nil {
			return err
		}
	}
}

func (b *blockDNSync) notify(
	ctx context.Context,
	n blockntfns.BlockNtfn,
) error {

	select {
	case b.notifications <- n:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *blockDNSync) Notifications() <-chan blockntfns.BlockNtfn {
	return b.notifications
}

func (b *blockDNSync) NotificationsSinceHeight(
	height uint32,
) ([]blockntfns.BlockNtfn, uint32, error) {

	b.mu.RLock()
	defer b.mu.RUnlock()
	_, tip, err := b.s.RegFilterHeaders.ChainTip()
	if err != nil {
		return nil, 0, err
	}
	if height > tip {
		return nil, 0, fmt.Errorf(
			"requested height %d exceeds tip %d",
			height,
			tip,
		)
	}
	var notifications []blockntfns.BlockNtfn
	if height != 0 {
		for i := height + 1; i <= tip; i++ {
			h, err := b.s.BlockHeaders.FetchHeaderByHeight(i)
			if err != nil {
				return nil, 0, err
			}
			notifications = append(
				notifications,
				blockntfns.NewBlockConnected(*h, i),
			)
		}
	}
	return notifications, tip, nil
}

// checkWitnesses compares commitments at the same height, tolerating a witness
// that is ahead but never silently accepting one that is behind.
func (b *blockDNSync) checkWitnesses(ctx context.Context, height uint32,
	hash, filter chainhash.Hash) error {

	for i, witness := range b.witnesses {
		status, err := witness.Status(ctx)
		if err != nil {
			return fmt.Errorf("witness %d: %w", i, err)
		}
		if status.GenesisHash != *b.s.chainParams.GenesisHash {
			return fmt.Errorf("witness %d: network mismatch", i)
		}
		if status.Height < height || status.FilterHeight < height {
			return fmt.Errorf(
				"witness %d: behind height %d",
				i,
				height,
			)
		}
		var remoteHash, remoteFilter chainhash.Hash
		if status.Height == height && status.FilterHeight == height {
			remoteHash = status.Hash
			remoteFilter = status.FilterHeader
		} else {
			tipClient := witness.AtTip(status.Height, status.Hash)
			files := &headerFiles{client: tipClient}
			header, fh, err := files.at(ctx, height)
			if err != nil {
				return fmt.Errorf("witness %d: %w", i, err)
			}
			remoteHash, remoteFilter = header.BlockHash(), fh
		}
		if remoteHash != hash || remoteFilter != filter {
			return fmt.Errorf(
				"witness %d: commitment mismatch at %d",
				i,
				height,
			)
		}
	}
	return nil
}
