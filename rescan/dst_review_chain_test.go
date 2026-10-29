package rescan

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/btcutil/v2/gcs"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/blockntfns"
	"github.com/lightninglabs/neutrino/headerfs"
)

// This file holds the scripted fixture behind the regression tests in
// dst_review_test.go. The swarm harness in dst_test.go uses the fixture in
// dst_world_test.go instead.

// dstChain injects faults at the ChainSource boundary used by the actor.
type dstChain struct {
	*mockChainWithTxs
	muFault                       sync.Mutex
	filterFailures, blockFailures int
	filterEntered, filterRelease  chan struct{}
	subscribeSuccesses            int
	onSubscribe                   func()

	onFilter          func(chainhash.Hash) error
	onHeader          func(chainhash.Hash) error
	headerFailures    map[chainhash.Hash]int
	headerFailureHits int
}

func (c *dstChain) GetBlockHeader(hash *chainhash.Hash) (
	*wire.BlockHeader, uint32, error) {

	c.muFault.Lock()
	hook := c.onHeader
	c.muFault.Unlock()
	if hook != nil {
		if err := hook(*hash); err != nil {
			return nil, 0, err
		}
	}
	c.muFault.Lock()
	if c.headerFailures[*hash] != 0 {
		if c.headerFailures[*hash] > 0 {
			c.headerFailures[*hash]--
		}
		c.headerFailureHits++
		c.muFault.Unlock()
		return nil, 0, headerfs.ErrHashNotFound
	}
	c.muFault.Unlock()
	return c.mockChainSource.GetBlockHeader(hash)
}

func (c *dstChain) GetCFilter(ctx context.Context,
	hash chainhash.Hash, batch bool) (*gcs.Filter, error) {

	c.muFault.Lock()
	hook := c.onFilter
	c.muFault.Unlock()
	if hook != nil {
		if err := hook(hash); err != nil {
			return nil, err
		}
	}
	c.muFault.Lock()
	entered, release := c.filterEntered, c.filterRelease
	c.filterEntered, c.filterRelease = nil, nil
	c.muFault.Unlock()
	if entered != nil {
		close(entered)
		<-release
	}
	c.muFault.Lock()
	if c.filterFailures > 0 {
		c.filterFailures--
		c.muFault.Unlock()
		return nil, errors.New("dst filter outage")
	}
	c.muFault.Unlock()
	return c.mockChainSource.GetCFilter(ctx, hash, batch)
}
func (c *dstChain) GetBlock(ctx context.Context,
	hash chainhash.Hash) (*btcutil.Block, error) {

	c.muFault.Lock()
	if c.blockFailures > 0 {
		c.blockFailures--
		c.muFault.Unlock()
		return nil, errors.New("dst block outage")
	}
	c.muFault.Unlock()
	return c.mockChainSource.GetBlock(ctx, hash)
}
func (c *dstChain) Subscribe(height uint32) (*blockntfns.Subscription, error) {
	c.muFault.Lock()
	c.subscribeSuccesses++
	hook := c.onSubscribe
	c.onSubscribe = nil
	c.muFault.Unlock()
	if hook != nil {
		hook()
	}
	return c.mockChainSource.Subscribe(height)
}

type dstEvent struct {
	kind   string
	height int32
	hash   chainhash.Hash
}
type dstOracle struct {
	tip    []chainhash.Hash
	events []dstEvent
	err    string
}

func (o *dstOracle) connect(height int32, h *wire.BlockHeader) {
	hash := h.BlockHash()
	if int(height) != len(o.tip) || h.PrevBlock != o.tip[len(o.tip)-1] {
		o.err = fmt.Sprintf("connect %d %v does not follow tip %d %v",
			height, hash, len(o.tip)-1, o.tip[len(o.tip)-1])
		return
	}
	o.tip = append(o.tip, hash)
	o.events = append(o.events, dstEvent{"connect", height, hash})
}
func (o *dstOracle) disconnect(height int32, h *wire.BlockHeader) {
	hash := h.BlockHash()
	if int(height) != len(o.tip)-1 || hash != o.tip[len(o.tip)-1] {
		o.err = fmt.Sprintf("disconnect %d %v does not match tip %d %v",
			height, hash, len(o.tip)-1, o.tip[len(o.tip)-1])
		return
	}
	o.tip = o.tip[:len(o.tip)-1]
	o.events = append(o.events, dstEvent{"disconnect", height, hash})
}
