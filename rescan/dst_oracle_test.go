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
	invMissedTx       = "noMissedRelevantTx"
	invDuplicate      = "noDuplicateFiltered"
	invPairing        = "filteredPairing"
	invProgress       = "progressMonotone"
	invRewind         = "rewindRedelivers"
	invFinished       = "finishedOnce"
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

// dstRewind is a rewind request the oracle holds production to.
type dstRewind struct {
	target int32

	// allowance is how many connects the harness knows were already in
	// flight when it sent the request.
	allowance int
	connects  int
}

// dstTruth is the ground-truth oracle. It is an independent statement of
// what the callbacks must show given the honest chain and the watch set's
// history, not another implementation of the rescan states.
//
// The callbacks arrive on the actor goroutine and the op checks on the
// harness goroutine, so every method takes mu.
type dstTruth struct {
	mu sync.Mutex

	fx        *dstFixture
	startTime time.Time

	// stack is the chain the callbacks have delivered, from the start
	// block up.
	stack []*dstBlock

	// scripts is every watched address script and the callback index at
	// which it became watched. outpoints is the same for watched
	// outpoints, including outputs discovered by delivered payments.
	scripts   map[string]int
	outpoints map[wire.OutPoint]int

	// live is every block with a filtered connect not yet followed by a
	// filtered disconnect.
	live map[chainhash.Hash]bool

	// pair is the filtered callback still waiting for its plain twin.
	pair *dstCallback

	// lastProgress is the last progress height since the delivered chain
	// last dropped below it, or -1.
	lastProgress int32

	// finishAllowed is set while a finished callback may fire, and
	// needFinish while one must fire before the run is quiescent.
	finishAllowed bool
	needFinish    bool

	rewinds []*dstRewind

	events   []dstCallback
	payments []chainhash.Hash
	errors   int

	// violations holds the first violation of each invariant.
	violations map[string]string
}

// newDSTTruth returns an oracle for a scan that starts at start with the
// given address scripts watched.
func newDSTTruth(fx *dstFixture, start *dstBlock, startTime time.Time,
	scripts ...[]byte) *dstTruth {

	o := &dstTruth{
		fx:            fx,
		startTime:     startTime,
		stack:         []*dstBlock{start},
		scripts:       make(map[string]int),
		outpoints:     make(map[wire.OutPoint]int),
		live:          make(map[chainhash.Hash]bool),
		lastProgress:  -1,
		finishAllowed: true,
		needFinish:    true,
		violations:    make(map[string]string),
	}
	for _, script := range scripts {
		o.scripts[string(script)] = 0
	}
	return o
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

// watchScript starts watching an address script from the next callback on.
func (o *dstTruth) watchScript(script []byte) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if _, ok := o.scripts[string(script)]; !ok {
		o.scripts[string(script)] = len(o.events)
	}
}

// relevant returns the transactions of b that the watch set as of now makes
// relevant, and adds the outputs those transactions pay to the watch set.
// Transactions are taken in block order, so a spend of an output created
// earlier in the same block counts. The caller holds mu.
func (o *dstTruth) relevant(b *dstBlock) []chainhash.Hash {
	// The fixture keeps every transaction above the birthday, so a block
	// at or before it has nothing to report whether or not it is scanned.
	if !b.header.Timestamp.After(o.startTime) {
		if len(b.msg.Transactions) != 0 {
			panic(fmt.Sprintf("fixture block %d before birthday "+
				"has transactions", b.height))
		}
		return nil
	}

	var hashes []chainhash.Hash
	for _, tx := range b.msg.Transactions {
		hash := tx.TxHash()
		hit := false
		for _, in := range tx.TxIn {
			if _, ok := o.outpoints[in.PreviousOutPoint]; ok {
				hit = true
			}
		}
		for i, out := range tx.TxOut {
			if _, ok := o.scripts[string(out.PkScript)]; !ok {
				continue
			}
			hit = true
			op := wire.OutPoint{Hash: hash, Index: uint32(i)}
			if _, ok := o.outpoints[op]; !ok {
				o.outpoints[op] = len(o.events)
			}
		}
		if hit {
			hashes = append(hashes, hash)
		}
	}
	return hashes
}

// filteredConnected checks one OnFilteredBlockConnected callback.
func (o *dstTruth) filteredConnected(height int32, header *wire.BlockHeader,
	txs []*btcutil.Tx) {

	o.mu.Lock()
	defer o.mu.Unlock()

	b, hash := o.identify(header)
	for _, tx := range txs {
		o.payments = append(o.payments, *tx.Hash())
	}
	o.record("filtered", height, hash, len(txs))

	if o.pair != nil {
		o.fail(invPairing, "filtered connect %d after unpaired %s %d",
			height, o.pair.kind, o.pair.height)
	}
	o.pair = &dstCallback{kind: "filtered", height: height, hash: hash}

	if o.live[hash] {
		o.fail(invDuplicate, "block %d %v reported twice without a "+
			"disconnect", height, hash)
	}
	o.live[hash] = true

	for _, r := range o.rewinds {
		r.connects++
		if r.connects > r.allowance {
			o.fail(invRewind, "connect %d before the rewind to %d "+
				"walked back from %d", height, r.target,
				o.top().height)
		}
	}

	if b == nil || int32(b.height) != height {
		o.fail(invMissedTx, "unknown block %d %v", height, hash)
		return
	}
	want := o.relevant(b)
	got := make(map[chainhash.Hash]bool, len(txs))
	for _, tx := range txs {
		if got[*tx.Hash()] {
			o.fail(invMissedTx, "block %d reported tx %v twice",
				height, *tx.Hash())
		}
		got[*tx.Hash()] = true
	}
	for _, tx := range want {
		if !got[tx] {
			o.fail(invMissedTx, "block %d missed relevant tx %v",
				height, tx)
		}
		delete(got, tx)
	}
	for tx := range got {
		o.fail(invMissedTx, "block %d reported irrelevant tx %v",
			height, tx)
	}
}

// blockConnected checks one OnBlockConnected callback.
func (o *dstTruth) blockConnected(height int32, header *wire.BlockHeader) {
	o.mu.Lock()
	defer o.mu.Unlock()

	b, hash := o.identify(header)
	o.record("connect", height, hash, 0)

	if o.pair == nil || o.pair.kind != "filtered" ||
		o.pair.height != height || o.pair.hash != hash {

		o.fail(invPairing, "connect %d %v without its filtered "+
			"connect", height, hash)
	}
	o.pair = nil

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

// filteredDisconnected checks one OnFilteredBlockDisconnected callback.
func (o *dstTruth) filteredDisconnected(height int32,
	header *wire.BlockHeader) {

	o.mu.Lock()
	defer o.mu.Unlock()

	_, hash := o.identify(header)
	o.record("unfiltered", height, hash, 0)

	if o.pair != nil {
		o.fail(invPairing, "filtered disconnect %d after unpaired "+
			"%s %d", height, o.pair.kind, o.pair.height)
	}
	o.pair = &dstCallback{kind: "unfiltered", height: height, hash: hash}
	delete(o.live, hash)
}

// blockDisconnected checks one OnBlockDisconnected callback.
func (o *dstTruth) blockDisconnected(height int32, header *wire.BlockHeader) {
	o.mu.Lock()
	defer o.mu.Unlock()

	_, hash := o.identify(header)
	o.record("disconnect", height, hash, 0)

	if o.pair == nil || o.pair.kind != "unfiltered" ||
		o.pair.height != height || o.pair.hash != hash {

		o.fail(invPairing, "disconnect %d %v without its filtered "+
			"disconnect", height, hash)
	}
	o.pair = nil

	top := o.top()
	if len(o.stack) == 1 || height != int32(top.height) ||
		hash != top.hash {

		o.fail(invContinuity, "disconnect %d %v does not match tip "+
			"%d %v", height, hash, top.height, top.hash)
		return
	}
	o.stack = o.stack[:len(o.stack)-1]
	newTop := int32(o.top().height)

	if newTop < o.lastProgress {
		o.lastProgress = -1
	}
	o.finishAllowed = true

	kept := o.rewinds[:0]
	for _, r := range o.rewinds {
		if newTop <= r.target {
			// The rewind walked back far enough, so the scan
			// must report finished again on its way back up.
			o.needFinish = true
			continue
		}
		kept = append(kept, r)
	}
	o.rewinds = kept
}

// progress checks one OnRescanProgress callback.
func (o *dstTruth) progress(height int32) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.record("progress", height, chainhash.Hash{}, 0)

	if o.pair != nil {
		o.fail(invPairing, "progress %d inside a callback pair",
			height)
	}
	if top := int32(o.top().height); height != top {
		o.fail(invProgress, "progress %d but delivered tip is %d",
			height, top)
	}
	if height <= o.lastProgress {
		o.fail(invProgress, "progress %d after %d with no rewind "+
			"below it", height, o.lastProgress)
	}
	o.lastProgress = height
}

// finished checks one OnRescanFinished callback.
func (o *dstTruth) finished(height int32) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.record("finished", height, chainhash.Hash{}, 0)

	if o.pair != nil {
		o.fail(invPairing, "finished %d inside a callback pair",
			height)
	}
	if top := int32(o.top().height); height != top {
		o.fail(invFinished, "finished %d but delivered tip is %d",
			height, top)
	}
	if !o.finishAllowed {
		o.fail(invFinished, "finished %d again with no disconnect "+
			"since the last one", height)
	}
	o.finishAllowed = false
	o.needFinish = false
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

// beginRewind arms rewindRedelivers for a rewind to target sent while
// allowance connects were already in flight. A target at or above the
// delivered tip asks for no walk back.
func (o *dstTruth) beginRewind(target uint32, allowance int) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if int32(target) >= int32(o.top().height) {
		return
	}
	o.rewinds = append(o.rewinds, &dstRewind{
		target:    int32(target),
		allowance: allowance,
	})
}

// checkOp runs the state check that holds between ops: no callback pair is
// left open.
func (o *dstTruth) checkOp() {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.pair != nil {
		o.fail(invPairing, "%s %d unpaired at an op boundary",
			o.pair.kind, o.pair.height)
	}
}

// checkFinished fails finishedOnce if the scan has not reported finished
// since it started or since its last rewind walked back. The caller checks
// it only once the scan is current, which every walk back passes through
// Syncing to reach.
func (o *dstTruth) checkFinished() {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.needFinish {
		o.fail(invFinished, "current at %d with no finished "+
			"callback since the last rewind", o.top().height)
	}
}

// checkQuiescent runs the checks that hold once the run has healed: the
// delivered tip is the active tip, so by continuity the delivered chain is
// the active chain, every rewind walked back, and the scan reported
// finished after the last one.
func (o *dstTruth) checkQuiescent(w *dstWorld) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if tip := w.tip(); o.top() != tip {
		o.fail(invQuiescentAtTip, "delivered tip %d, active tip %d",
			o.top().height, tip.height)
	}
	for _, r := range o.rewinds {
		o.fail(invRewind, "rewind to %d never walked back from %d",
			r.target, o.top().height)
	}
	if o.needFinish {
		o.fail(invFinished, "no finished callback after the last "+
			"rewind")
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
