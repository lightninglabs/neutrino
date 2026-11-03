package neutrino

import (
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/chainstore"
	"github.com/lightninglabs/neutrino/headerfs"
)

// chainStoreHost lets the chain store actor publish what became durable. The
// store decides what happened, and these methods make it visible to the
// handlers and to block notification subscribers. It runs on the actor's
// goroutine, and the handlers read the tips it publishes through the block
// manager's tip feed.
type chainStoreHost struct {
	manager *blockManager
}

// A compile time check that chainStoreHost implements chainstore.Host.
var _ chainstore.Host = (*chainStoreHost)(nil)

// BlockHeaders returns the block header store.
func (h *chainStoreHost) BlockHeaders() headerfs.BlockHeaderStore {
	return h.manager.cfg.BlockHeaders
}

// FilterHeaders returns the regular filter header store.
func (h *chainStoreHost) FilterHeaders() headerfs.FilterHeaderStore {
	return h.manager.cfg.RegFilterHeaders
}

// PublishBlockTip makes a durable header visible, without touching the
// checkpoint and validation state that the block handler owns.
func (h *chainStoreHost) PublishBlockTip(height uint32,
	header wire.BlockHeader) {

	h.manager.tipFeed.SetBlockTip(
		height, header.BlockHash(), header.Timestamp,
	)
}

// PublishedFilterHeight returns the height of the visible filter tip.
func (h *chainStoreHost) PublishedFilterHeight() uint32 {
	return h.manager.tipFeed.Snapshot().FilterHeight
}

// PublishFilterTip makes the block at height the visible filter tip, and wakes
// anything waiting on it.
func (h *chainStoreHost) PublishFilterTip(height uint32, hash chainhash.Hash) {
	h.manager.tipFeed.SetFilterTip(height, hash)
}

// PublishFilterHeaders makes the last of headers, which start at startHeight,
// the visible filter tip, then sends a connected notification for each one.
func (h *chainStoreHost) PublishFilterHeaders(headers []wire.BlockHeader,
	startHeight uint32) {

	if len(headers) == 0 {
		return
	}

	// We move the tip first. This is for subsystems that only need to know
	// the height changed, as opposed to the notifications below, which
	// report each new header.
	lastHeight := startHeight + uint32(len(headers)) - 1
	h.PublishFilterTip(lastHeight, headers[len(headers)-1].BlockHash())

	for i, header := range headers {
		height := startHeight + uint32(i)
		h.manager.fltrHeaderProgessLogger.LogBlockHeight(
			header.Timestamp, int32(height),
		)

		h.manager.onBlockConnected(header, height)
	}
}

// RewindFilterTip lowers the visible filter tip only if a rollback crossed it.
// Durable filter headers that were never published must stay unpublished.
func (h *chainStoreHost) RewindFilterTip(height uint32, hash chainhash.Hash) {
	h.manager.tipFeed.RewindFilterTip(height, hash)
}

// BlockConnected queues a connected notification.
func (h *chainStoreHost) BlockConnected(header wire.BlockHeader,
	height uint32) {

	h.manager.onBlockConnected(header, height)
}

// BlockDisconnected queues a disconnected notification.
func (h *chainStoreHost) BlockDisconnected(header wire.BlockHeader,
	height uint32, newTip wire.BlockHeader) {

	h.manager.onBlockDisconnected(header, height, newTip)
}
