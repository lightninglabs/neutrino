package rescan

import (
	"context"
	"testing"

	"github.com/btcsuite/btcd/address/v2"
	"pgregory.net/rapid"
)

// fsmModel drives the rescan states directly, without the actor, and feeds
// every outbox event to the same ground-truth oracle as the DST. Rapid
// chooses the order of chain events, notifications, continuations, rewinds
// and watch updates. A retry re-applies the failed event as is, without the
// actor's retry admission checks, so the model reaches orders the actor
// would filter out but cannot see bugs in the actor itself.
type fsmModel struct {
	w     *dstWorld
	env   *Environment
	truth *dstTruth
	state RescanState

	// next is set while a ProcessNextBlockEvent continuation is owed.
	next bool

	// queue holds chain notifications in the order the subscription
	// delivers them. retries holds events whose processing failed, which
	// the actor would retry after a delay.
	queue   []RescanEvent
	retries []RescanEvent

	// added is the number of fixture addresses watched so far.
	added dstAddr
}

// newFSMModel returns a model that starts at genesis with dstAddrA watched.
func newFSMModel(t *rapid.T) *fsmModel {
	w := newDSTWorld(uint32(rapid.IntRange(0, 12).Draw(t, "blocks")))
	start := w.fx.genesis
	m := &fsmModel{
		w: w,
		env: &Environment{
			Chain: &dstSource{w: w},
		},
		truth: newDSTTruth(
			w, start, w.fx.genesis.header.Timestamp,
			w.fx.scripts[dstAddrA],
		),
		next:  true,
		added: dstAddrB,
	}
	watch, err := (&WatchState{}).ApplyUpdate(&AddWatchAddrsEvent{
		Addrs: []address.Address{w.fx.addrs[dstAddrA]},
	})
	if err != nil {
		t.Fatalf("initial watch: %v", err)
	}
	m.state = &StateInitializing{
		Cursor: Cursor{
			CurStamp:  start.stamp(),
			CurHeader: start.header,
			Watch:     watch,
		},
	}
	return m
}

// apply processes one event and dispatches its outbox the way the actor
// does. It reports whether the state accepted the event.
func (m *fsmModel) apply(event RescanEvent) bool {
	transition, err := m.state.ProcessEvent(
		context.Background(), event, m.env,
	)
	if err != nil {
		return false
	}
	m.state = transition.NextState
	for _, out := range outboxOf(transition) {
		switch e := out.(type) {
		case FilteredBlockOutbox:
			m.truth.filteredConnected(
				e.Height, e.Header, e.RelevantTxs,
			)

		case BlockConnectedOutbox:
			m.truth.blockConnected(&e.Hash, e.Height)

		case BlockDisconnectedOutbox:
			// Report the two halves in the order the actor's
			// dispatch does.
			if e.Rewind {
				m.truth.blockDisconnected(&e.Hash, e.Height)
				m.truth.filteredDisconnected(e.Height, e.Header)
				break
			}
			m.truth.filteredDisconnected(e.Height, e.Header)
			m.truth.blockDisconnected(&e.Hash, e.Height)

		case RescanProgressOutbox:
			m.truth.progress(e.Height)

		case RescanFinishedOutbox:
			m.truth.finished(e.Height)

		case ContinueOutbox:
			m.next = true
		}
	}
	return true
}

// deliver processes the next queued notification. A failed one goes to the
// retry list, as the actor schedules a retry and moves on.
func (m *fsmModel) deliver() {
	if len(m.queue) == 0 {
		return
	}
	event := m.queue[0]
	m.queue = m.queue[1:]
	if !m.apply(event) {
		m.retries = append(m.retries, event)
	}
}

// retry reprocesses the oldest failed event.
func (m *fsmModel) retry() {
	if len(m.retries) == 0 {
		return
	}
	event := m.retries[0]
	m.retries = m.retries[1:]
	if !m.apply(event) {
		m.retries = append(m.retries, event)
	}
}

// proceed runs an owed continuation. A failed one goes to the retry list,
// as the actor retries it after a delay while its mailbox moves on.
func (m *fsmModel) proceed() {
	if !m.next {
		return
	}
	m.next = false
	event := ProcessNextBlockEvent{}
	if !m.apply(event) {
		m.retries = append(m.retries, event)
	}
}

// connect queues the notification for a new active block.
func (m *fsmModel) connect(b *dstBlock) {
	m.queue = append(m.queue, BlockConnectedEvent{
		Header: b.header, Height: b.height,
	})
}

// extend adds a block to the chain and queues its notification.
func (m *fsmModel) extend(*rapid.T) {
	m.connect(m.w.extend())
}

// reorg replaces the top blocks of the chain and queues the disconnects and
// connects in the order the block manager sends them.
func (m *fsmModel) reorg(t *rapid.T) {
	tip := m.w.tip()
	if tip.height < 2 {
		return
	}
	depth := rapid.Uint32Range(1, min(3, tip.height-1)).Draw(t, "depth")
	longer := rapid.IntRange(0, 1).Draw(t, "longer")
	old, branch := m.w.reorg(tip.height-depth, int(depth)+longer)
	for i := len(old) - 1; i >= 0; i-- {
		b := old[i]
		m.queue = append(m.queue, BlockDisconnectedEvent{
			Header: b.header, Height: b.height,
			ChainTip: b.parent.header,
		})
	}
	for _, b := range branch {
		m.connect(b)
	}
}

// rewindTarget draws a rewind target at or below the delivered tip.
func (m *fsmModel) rewindTarget(t *rapid.T) uint32 {
	top := m.truth.tip().height
	return rapid.Uint32Range(0, top).Draw(t, "target")
}

// rewind asks for a rewind with no watch change.
func (m *fsmModel) rewind(t *rapid.T) {
	target := m.rewindTarget(t)
	m.truth.beginRewind(target, 0)
	if !m.apply(AddWatchAddrsEvent{RewindTo: &target}) {
		t.Fatalf("rewind to %d rejected", target)
	}
}

// addAddr watches the next fixture address, optionally with a rewind so
// its past payments are found.
func (m *fsmModel) addAddr(t *rapid.T) {
	if m.added >= dstAddrOther {
		return
	}
	addr := m.added
	event := AddWatchAddrsEvent{
		Addrs: []address.Address{m.w.fx.addrs[addr]},
	}
	if rapid.Bool().Draw(t, "rewind") {
		target := m.rewindTarget(t)
		event.RewindTo = &target
		m.truth.beginRewind(target, 0)
	}
	if !m.apply(event) {
		t.Fatalf("watch update rejected")
	}
	m.truth.watchScript(m.w.fx.scripts[addr])
	m.added++
}

// check runs the op-boundary invariants after every action.
func (m *fsmModel) check(t *rapid.T) {
	m.truth.checkOp()
	if err := m.truth.err(); err != "" {
		t.Fatalf("%s", err)
	}
}

// settle runs every owed continuation, notification and retry until none
// is left, then checks that the scan is current at the tip.
func (m *fsmModel) settle(t *rapid.T) {
	for i := 0; i < 1000; i++ {
		switch {
		case m.next:
			m.proceed()
		case len(m.queue) > 0:
			m.deliver()
		case len(m.retries) > 0:
			m.retry()
		default:
			m.check(t)
			cur, ok := m.state.(*StateCurrent)
			tip := m.w.tip()
			if !ok || cur.CurStamp.Hash != tip.hash {
				t.Fatalf("%s: state %v, chain tip %d",
					invQuiescentAtTip, m.state, tip.height)
			}
			m.truth.checkQuiescent(m.w)
			m.check(t)
			return
		}
		m.check(t)
	}
	t.Fatalf("%s: no quiescence after 1000 steps, state %v, %d "+
		"retries", invQuiescentAtTip, m.state, len(m.retries))
}

// TestPropertyRescanStateMachine drives the rescan states with rapid
// actions and checks the DST's ground-truth oracle after every one: every
// relevant transaction of every delivered block is reported, no block is
// reported twice, callbacks pair and chain up, progress and finished
// callbacks are consistent, every rewind walks back, and the scan ends
// current at the tip. It replaces TestPropertySyncNeverMissesTx, which
// passed even when every relevant transaction was dropped.
func TestPropertyRescanStateMachine(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		m := newFSMModel(t)
		t.Repeat(map[string]func(*rapid.T){
			"extend":  m.extend,
			"reorg":   m.reorg,
			"deliver": func(*rapid.T) { m.deliver() },
			"retry":   func(*rapid.T) { m.retry() },
			"proceed": func(*rapid.T) { m.proceed() },
			"rewind":  m.rewind,
			"addAddr": m.addAddr,
			"":        m.check,
		})
		m.settle(t)
	})
}
