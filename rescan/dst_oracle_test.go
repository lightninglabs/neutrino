package rescan

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
)

// The names of the oracle's invariants. Each violation is reported under
// one of these, so a failure names the property that broke.
const (
	invContinuity     = "callbackContinuity"
	invQuiescentAtTip = "quiescentAtTip"
)

// dstCallback is one recorded callback. The double run compares the whole
// list.
type dstCallback struct {
	kind   string
	height int32
	hash   chainhash.Hash
	txs    int
}

// dstTruth is the DST oracle. It records every callback and checks that
// connects and disconnects form one continuous chain.
//
// The callbacks arrive on the actor goroutine and the op checks on the
// harness goroutine, so every method takes mu.
type dstTruth struct {
	mu sync.Mutex

	fx *dstFixture

	// stack is the chain the callbacks have delivered, from the start
	// block up.
	stack []*dstBlock

	events   []dstCallback
	payments []chainhash.Hash
	errors   int

	// violations holds the first violation of each invariant.
	violations map[string]string
}

// newDSTTruth returns an oracle for a scan that starts at start.
func newDSTTruth(fx *dstFixture, start *dstBlock) *dstTruth {
	return &dstTruth{
		fx:         fx,
		stack:      []*dstBlock{start},
		violations: make(map[string]string),
	}
}

// fail records the first violation of an invariant. The caller holds mu.
func (o *dstTruth) fail(name, format string, args ...any) {
	if _, ok := o.violations[name]; ok {
		return
	}
	o.violations[name] = fmt.Sprintf(
		"%s at callback %d: %s", name, len(o.events),
		fmt.Sprintf(format, args...),
	)
}

// err returns every recorded violation in name order, or "".
func (o *dstTruth) err() string {
	o.mu.Lock()
	defer o.mu.Unlock()

	names := make([]string, 0, len(o.violations))
	for name := range o.violations {
		names = append(names, name)
	}
	sort.Strings(names)
	msgs := make([]string, 0, len(names))
	for _, name := range names {
		msgs = append(msgs, o.violations[name])
	}
	return strings.Join(msgs, "; ")
}

// top returns the delivered tip. The caller holds mu.
func (o *dstTruth) top() *dstBlock {
	return o.stack[len(o.stack)-1]
}

// tip returns the delivered tip.
func (o *dstTruth) tip() *dstBlock {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.top()
}

// identify returns the fixture block a callback header names, and its hash.
// An unknown header is hashed instead. The caller holds mu.
func (o *dstTruth) identify(h *wire.BlockHeader) (*dstBlock, chainhash.Hash) {
	if b, ok := o.fx.byHeaderOf(h); ok {
		return b, b.hash
	}
	return nil, h.BlockHash()
}

// record appends a callback to the event log. The caller holds mu.
func (o *dstTruth) record(kind string, height int32, hash chainhash.Hash,
	txs int) {

	o.events = append(o.events, dstCallback{kind, height, hash, txs})
}

// filteredConnected records one OnFilteredBlockConnected callback.
func (o *dstTruth) filteredConnected(height int32, header *wire.BlockHeader,
	txs []*btcutil.Tx) {

	o.mu.Lock()
	defer o.mu.Unlock()

	_, hash := o.identify(header)
	for _, tx := range txs {
		o.payments = append(o.payments, *tx.Hash())
	}
	o.record("filtered", height, hash, len(txs))
}

// blockConnected checks one OnBlockConnected callback.
func (o *dstTruth) blockConnected(height int32, header *wire.BlockHeader) {
	o.mu.Lock()
	defer o.mu.Unlock()

	b, hash := o.identify(header)
	o.record("connect", height, hash, 0)

	top := o.top()
	if height != int32(top.height)+1 || header.PrevBlock != top.hash {
		o.fail(invContinuity, "connect %d %v does not extend tip %d "+
			"%v", height, hash, top.height, top.hash)
		return
	}
	if b == nil {
		o.fail(invContinuity, "connect of unknown block %v", hash)
		return
	}
	o.stack = append(o.stack, b)
}

// filteredDisconnected records one OnFilteredBlockDisconnected callback.
func (o *dstTruth) filteredDisconnected(height int32,
	header *wire.BlockHeader) {

	o.mu.Lock()
	defer o.mu.Unlock()

	_, hash := o.identify(header)
	o.record("unfiltered", height, hash, 0)
}

// blockDisconnected checks one OnBlockDisconnected callback.
func (o *dstTruth) blockDisconnected(height int32, header *wire.BlockHeader) {
	o.mu.Lock()
	defer o.mu.Unlock()

	_, hash := o.identify(header)
	o.record("disconnect", height, hash, 0)

	top := o.top()
	if len(o.stack) == 1 || height != int32(top.height) ||
		hash != top.hash {

		o.fail(invContinuity, "disconnect %d %v does not match tip "+
			"%d %v", height, hash, top.height, top.hash)
		return
	}
	o.stack = o.stack[:len(o.stack)-1]
}

// progress records one OnRescanProgress callback.
func (o *dstTruth) progress(height int32) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.record("progress", height, chainhash.Hash{}, 0)
}

// finished records one OnRescanFinished callback.
func (o *dstTruth) finished(height int32) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.record("finished", height, chainhash.Hash{}, 0)
}

// onError counts one OnError callback.
func (o *dstTruth) onError(error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.errors++
}

// errorCount returns the number of OnError callbacks so far.
func (o *dstTruth) errorCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.errors
}

// checkOp runs the state checks that hold between ops: the delivered chain
// is a chain of known blocks.
func (o *dstTruth) checkOp() {
	o.mu.Lock()
	defer o.mu.Unlock()

	for i := 1; i < len(o.stack); i++ {
		if o.stack[i].parent != o.stack[i-1] {
			o.fail(invContinuity, "delivered chain broken at %d",
				o.stack[i].height)
		}
	}
}

// checkQuiescent runs the checks that hold once the run has healed: the
// delivered chain is the active chain.
func (o *dstTruth) checkQuiescent(w *dstWorld) {
	o.mu.Lock()
	defer o.mu.Unlock()

	for _, b := range o.stack {
		if !w.onActive(b) {
			o.fail(invQuiescentAtTip, "delivered block %d %v is "+
				"not active", b.height, b.hash)
			break
		}
	}
	if tip := w.tip(); o.top() != tip {
		o.fail(invQuiescentAtTip, "delivered tip %d, active tip %d",
			o.top().height, tip.height)
	}
}

// callbacks returns the callback set that feeds the oracle.
func (o *dstTruth) callbacks() Callbacks {
	return Callbacks{
		OnFilteredBlockConnected:    o.filteredConnected,
		OnBlockConnected:            o.blockConnectedAt,
		OnFilteredBlockDisconnected: o.filteredDisconnected,
		OnBlockDisconnected:         o.blockDisconnected,
		OnRescanProgress:            o.progress,
		OnRescanFinished:            o.finished,
	}
}

// blockConnectedAt adapts blockConnected to the callback signature.
func (o *dstTruth) blockConnectedAt(height int32, header *wire.BlockHeader,
	_ time.Time) {

	o.blockConnected(height, header)
}
