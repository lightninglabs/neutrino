package neutrino

import (
	"bytes"
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/banman"
	"github.com/lightninglabs/neutrino/headersync"
	"github.com/lightninglabs/neutrino/headersync/querydispatch"
	"github.com/lightninglabs/neutrino/querysync"
	"github.com/stretchr/testify/require"
)

const (
	// dstPeerSlots is the number of simulated peer connections.
	dstPeerSlots = 3

	// dstMaxSteps bounds the scripted fault phase of one run.
	dstMaxSteps = 32

	// dstHealRounds bounds the recovery phase of one run.
	dstHealRounds = 64

	// dstRangeCap is the scaled-down getheaders cap used by discovery
	// modes, so a 12-header chain needs several spans. Simulated peers
	// honour the same cap, as real peers honour the 2000-header wire cap.
	dstRangeCap = 4

	// dstRequestTimeout is the base header request timeout.
	dstRequestTimeout = time.Second

	// dstStall is long enough for every outstanding request to expire.
	dstStall = 2 * time.Second

	// dstWireCap is the getheaders reply cap of checkpoint modes.
	dstWireCap = wire.MaxBlockHeadersPerMsg
)

// dstMode selects how a run plans and discovers header work.
type dstMode uint8

const (
	// dstModeCheckpointAll pre-plans every checkpoint-backed range, as
	// the original transport harness did.
	dstModeCheckpointAll dstMode = iota

	// dstModeCheckpoint starts from production startSync with a
	// checkpoint every two heights.
	dstModeCheckpoint

	// dstModeAnchor has a single checkpoint at dstChainHeight, so
	// production must discover intermediate anchors toward it.
	dstModeAnchor

	// dstModeFrontier1 has no checkpoints and uses the production anchor
	// policy: one observation confirms an anchor, fanout four.
	dstModeFrontier1

	// dstModeFrontier2 has no checkpoints and requires two matching
	// observations per anchor.
	dstModeFrontier2

	// dstModeCount is the number of modes chosen by the mode byte's
	// remainder.
	dstModeCount

	// dstModeSerial runs on the SimNet network ID, where production keeps
	// the serial header path, with a checkpoint every two heights. It is
	// chosen by a mode byte ending in 0xf, so adding it did not remap the
	// modes of existing seeds.
	dstModeSerial = dstModeCount
)

// String returns a short mode name.
func (m dstMode) String() string {
	return [...]string{
		"checkpoint_all", "checkpoint", "anchor", "frontier1",
		"frontier2", "serial",
	}[m]
}

// discovery reports whether the mode reaches anchor discovery.
func (m dstMode) discovery() bool {
	return m >= dstModeAnchor && m != dstModeSerial
}

// dstFault is one swarm fault class. A seed enables a subset of classes so
// different runs stress different combinations instead of averaging every
// fault into a uniform background rate.
type dstFault uint16

const (
	dstFaultDuplicate dstFault = 1 << iota
	dstFaultStall
	dstFaultChurn
	dstFaultBlock
	dstFaultStore
	dstFaultAsk
	dstFaultByzantine
	dstFaultRestart
	dstFaultShutdown
	dstFaultRTT
	dstFaultReorg
)

// dstConfig is the decoded swarm configuration of one seed.
type dstConfig struct {
	mode   dstMode
	faults dstFault
	// order seeds the delivery order of same-instant callbacks.
	order uint64
}

// decodeDSTScript splits a seed into its swarm configuration and op bytes.
// The first three bytes choose the mode and fault set; an empty or short
// seed enables every fault class in the default mode.
func decodeDSTScript(script []byte) (dstConfig, []byte) {
	cfg := dstConfig{faults: ^dstFault(0)}
	if len(script) >= 3 {
		cfg.mode = dstMode(script[0] % byte(dstModeCount))
		if script[0]%16 == 0xf {
			cfg.mode = dstModeSerial
		}
		cfg.faults = dstFault(binary.LittleEndian.Uint16(script[1:3]))
		script = script[3:]
	}
	digest := sha256.Sum256(script)
	cfg.order = binary.LittleEndian.Uint64(digest[:8])
	return cfg, script[:min(len(script), dstMaxSteps)]
}

// dstHeaderRequest is one getheaders request waiting in a peer's outbound
// queue.
type dstHeaderRequest struct {
	peer  *ServerPeer
	start uint32
	stop  uint32
	// stopKnown is false for discovery requests with a zero stop hash.
	stopKnown bool
}

// dstHeaderTransport models each connection's outbound getheaders queue. A peer
// answers its requests in order. The transport never schedules work or
// synthesizes timeout events. Production request timers and
// message handlers decide when a stalled request loses its lease.
type dstHeaderTransport struct {
	// chain is the chain simulated peers serve; index i is height i.
	chain []*wire.BlockHeader

	queued  []dstHeaderRequest
	blocked map[string]bool
	cap     uint32
	best    uint32
	sent    int
}

// SendGetHeaders queues a request in the peer's single outbound slot.
func (d *dstHeaderTransport) SendGetHeaders(peer *ServerPeer,
	locator blockchain.BlockLocator, stop *chainhash.Hash) error {

	if d.blocked[peer.Addr()] {
		return errors.New("simulated outbound queue blocked")
	}
	if len(locator) == 0 || stop == nil {
		return errors.New("missing header request boundaries")
	}

	// A real peer answers from the first locator hash it knows on its own
	// (honest) chain.
	fixture := dstChains()
	start, ok := uint32(0), false
	for _, hash := range locator {
		height, known := fixture.heights[*hash]
		if known && height <= d.best &&
			d.chain[height].BlockHash() == *hash {

			start, ok = height, true
			break
		}
	}
	if !ok {
		return errors.New("unknown header request locator")
	}
	end, stopKnown := min(d.best, start+d.cap), false
	if *stop != zeroHash {
		height, known := fixture.heights[*stop]
		// Like btcd, a stop hash at or behind the locator is never
		// reached, so the reply runs to the cap.
		if known && height <= d.best && height > start &&
			d.chain[height].BlockHash() == *stop {

			end, stopKnown = min(height, start+d.cap), true
		}
	}
	d.queued = append(d.queued, dstHeaderRequest{
		peer: peer, start: start, stop: end, stopKnown: stopKnown,
	})
	d.sent++
	return nil
}

// take removes and returns the oldest request queued for peer.
func (d *dstHeaderTransport) take(peer *ServerPeer) (dstHeaderRequest, bool) {
	for i, request := range d.queued {
		if request.peer == peer {
			d.queued = slices.Delete(d.queued, i, i+1)
			return request, true
		}
	}
	return dstHeaderRequest{}, false
}

// queuedFor reports whether peer has an unanswered request.
func (d *dstHeaderTransport) queuedFor(peer *ServerPeer) bool {
	for _, request := range d.queued {
		if request.peer == peer {
			return true
		}
	}
	return false
}

// dstRun is one deterministic header-sync simulation over the production
// blockmanager, headersync runtime, and querysync dispatch actors.
type dstRun struct {
	t   *testing.T
	cfg dstConfig
	rng *rand.Rand

	bm        *blockManager
	store     *dstHeaderStore
	filters   *memFilterHeaderStore
	transport *dstHeaderTransport
	recorder  *headersync.EventRecorder
	peers     *list.List

	current    [dstPeerSlots]*ServerPeer
	generation [dstPeerSlots]int
	last       [dstPeerSlots]*headersMsg

	// faulted records peers that misbehaved or stalled. Only these may be
	// quarantined or disconnected.
	faulted map[string]string

	// punished records quarantines, bans and disconnects by peer.
	punished map[string]string

	// bans records every BanPeer call as "addr:reason".
	bans []string

	// askFaults holds one-shot actor ask faults at blockmanager call
	// sites.
	askFaults map[headerSyncAskOp]headerSyncAskFault

	// controllerFaults holds one-shot faults for asks the headersync
	// controller makes itself.
	controllerFaults map[headersync.AskOp]headersync.AskFault

	// anchorLiar is set once a peer served a PoW-valid but dishonest
	// chain, which can win anchor discovery.
	anchorLiar bool

	// conflictPunishments counts honest peers punished for reporting an
	// anchor that conflicted with a liar's (known policy gap F12).
	conflictPunishments int

	// anchorConflicts records peers that sent an honest reply ending at a
	// height where a liar's anchor was already known.
	anchorConflicts map[string]bool

	// ntfns records every block notification in delivery order.
	ntfns   []string
	ntfnMtx sync.Mutex

	// poisoned is set when a run ended stuck behind a liar's confirmed
	// anchor (known gap F13) instead of reaching the honest tip.
	poisoned bool

	// restarts counts block manager rebuilds over the same stores.
	restarts int

	// stopped is set once the run shut the block manager down.
	stopped bool

	// target is the honest best height the run must reach.
	target uint32

	inv      *dstInvariants
	steps    []string
	timeouts int
}

// dstSnapshot is the comparable outcome of one run. Two runs of one seed
// must produce identical snapshots.
type dstSnapshot struct {
	Events   []headersync.Event
	Steps    []string
	Writes   []uint32
	Ranges   []headersync.HeaderRange
	Peers    []headersync.PeerSnapshot
	Bans     []string
	Timeouts int
	Sent     int
	Restarts int
	Ntfns    []string

	ConflictPunishments int
	Poisoned            bool
}

// setupDSTBlockManager builds a block manager over in-memory header stores.
// It skips the bbolt setup that dominated the cost of each DST execution.
func setupDSTBlockManager(t *testing.T) (*blockManager,
	*memBlockHeaderStore) {

	t.Helper()
	store := newMemBlockHeaderStore(&chaincfg.SimNetParams)
	filters, err := newMemFilterHeaderStore(&chaincfg.SimNetParams)
	require.NoError(t, err)
	bm, err := newBlockManager(&blockManagerCfg{
		ChainParams:      chaincfg.SimNetParams,
		BlockHeaders:     store,
		RegFilterHeaders: filters,
		QueryDispatcher:  &mockDispatcher{},
		TimeSource:       blockchain.NewMedianTime(),
		BanPeer: func(string, banman.Reason) error {
			return nil
		},
	})
	require.NoError(t, err)
	return bm, store
}

// newDSTRun builds a block manager over in-memory stores and connects the
// initial peers. It must be called inside a synctest bubble.
func newDSTRun(t *testing.T, cfg dstConfig) *dstRun {
	t.Helper()
	enableDSTLog()

	d := &dstRun{
		t:   t,
		cfg: cfg,
		rng: rand.New(rand.NewPCG(cfg.order, uint64(cfg.mode))),
		store: &dstHeaderStore{
			BlockHeaderStore: newMemBlockHeaderStore(
				&chaincfg.SimNetParams,
			),
		},
		peers:           list.New(),
		faulted:         make(map[string]string),
		anchorConflicts: make(map[string]bool),
		punished:        make(map[string]string),
		askFaults:       make(map[headerSyncAskOp]headerSyncAskFault),
		controllerFaults: make(
			map[headersync.AskOp]headersync.AskFault,
		),
		target: dstChainHeight,
	}
	filters, err := newMemFilterHeaderStore(&chaincfg.SimNetParams)
	require.NoError(t, err)
	d.filters = filters
	d.inv = newDSTInvariants(d)
	d.build()

	// Stop inside the bubble even when an invariant fails, so a failure
	// reports itself instead of a goroutine deadlock.
	t.Cleanup(func() {
		if !d.stopped {
			d.stopped = true
			require.NoError(t, d.bm.Stop())
		}
	})

	// The checkpoint-all mode pre-plans every range before peers connect,
	// so production planning finds the spans already covered.
	if cfg.mode == dstModeCheckpointAll {
		manager := d.bm.headerSync.Manager()
		plan, err := manager.PlanConfirmedAnchorRanges(
			t.Context(), 1, 0, dstChainHeight,
		)
		require.NoError(t, err)
		require.Len(t, plan.Ranges, dstChainHeight/2)
		d.bm.headerSync.Session().SetNextRangeID(plan.NextID)
	}
	for slot := range d.current {
		d.connect(slot)
	}
	if cfg.mode == dstModeCheckpointAll {
		d.bm.dispatchHeaderSyncWork(d.peers)
	}
	d.pump()
	d.check("start")
	return d
}

// params returns the chain parameters for the run's mode. SimNet proof of
// work stays cheap while the TestNet3 network ID enables the production
// actor path instead of the serial SimNet compatibility path.
func (d *dstRun) params() chaincfg.Params {
	params := chaincfg.SimNetParams
	params.Net = chaincfg.TestNet3Params.Net
	params.Checkpoints = nil
	honest := dstChains().honest
	switch d.cfg.mode {
	case dstModeSerial:
		params.Net = chaincfg.SimNetParams.Net
		fallthrough

	case dstModeCheckpointAll, dstModeCheckpoint:
		for height := 2; height <= dstChainHeight; height += 2 {
			hash := honest[height].BlockHash()
			params.Checkpoints = append(params.Checkpoints,
				chaincfg.Checkpoint{
					Height: int32(height), Hash: &hash,
				},
			)
		}
	case dstModeAnchor:
		hash := honest[dstChainHeight].BlockHash()
		params.Checkpoints = []chaincfg.Checkpoint{{
			Height: dstChainHeight, Hash: &hash,
		}}
	}
	return params
}

// headerConfig returns the headersync policy for the run's mode.
func (d *dstRun) headerConfig() headersync.Config {
	cfg := headersync.ProductionConfig()
	if d.cfg.mode.discovery() {
		cfg.MaxRangeHeaders = dstRangeCap
	}
	if d.cfg.mode == dstModeFrontier2 {
		cfg.AnchorConfirmationsRequired = 2
		cfg.AnchorRequestFanout = 2
	}
	return cfg
}

// build creates a block manager over the run's durable stores. A restart
// calls it again with the same stores.
func (d *dstRun) build() {
	t := d.t
	genesis := chaincfg.SimNetParams.GenesisBlock.Header
	bm, err := newBlockManager(&blockManagerCfg{
		ChainParams:      d.params(),
		BlockHeaders:     d.store,
		RegFilterHeaders: d.filters,
		QueryDispatcher:  &mockDispatcher{},
		TimeSource: dstMedianTime{
			MedianTimeSource: blockchain.NewMedianTime(),
			offset: time.Until(
				genesis.Timestamp.Add(time.Hour),
			),
		},
		HeaderSyncTimeout: dstRequestTimeout,
		BanPeer: func(addr string, reason banman.Reason) error {
			d.bans = append(d.bans, fmt.Sprintf("%s:%v", addr,
				reason))
			d.punished[addr] = "ban"
			return nil
		},
		QuarantinePeer: func(addr string, _ time.Duration) {
			d.punished[addr] = "quarantine"
		},
		headerSyncAskFault: d.askFault,
	})
	require.NoError(t, err)

	// Replace the default runtime so discovery modes can use a scaled cap
	// and the two-observation policy. The dispatch mirror is the same
	// querysync actor production builds.
	_, tip, err := d.store.ChainTip()
	require.NoError(t, err)
	tipHeader, err := d.store.FetchHeaderByHeight(tip)
	require.NoError(t, err)
	bm.headerSync.Stop()
	dispatchFSM := querysync.NewSchedulerFSM(querysync.DefaultConfig())
	dispatchActor := querysync.NewManagerActor(
		dispatchFSM, headerSyncActorMailboxSize,
	)
	bm.headerSync, _ = headersync.NewRuntime(
		headersync.ChainPoint{Height: tip, Hash: tipHeader.BlockHash()},
		bm.cfg.ChainParams.Checkpoints,
		headersync.RuntimeConfig{
			Config:        d.headerConfig(),
			MailboxSize:   headerSyncActorMailboxSize,
			Dispatch:      querydispatch.New(dispatchActor),
			DispatchActor: dispatchActor,
			AskFault:      d.controllerFault,
		},
	)
	d.recorder = &headersync.EventRecorder{}
	bm.headerSync.Manager().SetEventSink(d.recorder)
	bm.headerSync.Start(t.Context())

	if d.transport == nil {
		d.transport = &dstHeaderTransport{
			chain:   dstChains().honest,
			blocked: make(map[string]bool),
			cap:     dstWireCap,
			best:    dstChainHeight,
		}
		if d.cfg.mode.discovery() {
			d.transport.cap = dstRangeCap
		}
	}
	bm.cfg.HeaderTransport = d.transport
	d.bm = bm
	d.inv.reset()

	// Stand in for the notification subscription manager, which drains
	// the unbuffered notification channel in production. Record every
	// notification so ordering can be inspected.
	go func(bm *blockManager) {
		for {
			select {
			case ntfn := <-bm.blockNtfnChan:
				d.ntfnMtx.Lock()
				d.ntfns = append(d.ntfns, fmt.Sprint(ntfn))
				d.ntfnMtx.Unlock()
			case <-bm.quit:
				return
			}
		}
	}(bm)
}

// controllerFault consumes a one-shot controller ask fault.
func (d *dstRun) controllerFault(
	op headersync.AskOp) headersync.AskFault {

	fault, ok := d.controllerFaults[op]
	if !ok {
		return headersync.AskOK
	}
	delete(d.controllerFaults, op)
	return fault
}

// askFault consumes a one-shot blockmanager ask fault armed by the script.
func (d *dstRun) askFault(op headerSyncAskOp) headerSyncAskFault {
	fault, ok := d.askFaults[op]
	if !ok {
		return headerSyncAskOK
	}
	delete(d.askFaults, op)
	return fault
}

// slotOf returns the slot holding peer, or -1.
func (d *dstRun) slotOf(peer *ServerPeer) int {
	for slot, current := range d.current {
		if current == peer && peer != nil {
			return slot
		}
	}
	return -1
}

// listed reports whether peer is still a sync candidate.
func (d *dstRun) listed(peer *ServerPeer) bool {
	for element := d.peers.Front(); element != nil; {
		if element.Value == peer {
			return true
		}
		element = element.Next()
	}
	return false
}

// connect replaces the peer in slot with a fresh connection through the
// production new-peer handler.
func (d *dstRun) connect(slot int) {
	if d.current[slot] != nil {
		d.disconnect(slot)
	}
	d.generation[slot]++
	peer := newTestServerPeer(d.t,
		fmt.Sprintf("127.0.0.%d:%d", slot+1,
			8332+d.generation[slot]), int32(d.transport.best),
	)
	d.current[slot] = peer
	d.last[slot] = nil
	d.handle(&newPeerMsg{peer: peer})
}

// disconnect drops the connection in slot through the production done-peer
// handler, as the server does when a socket closes.
func (d *dstRun) disconnect(slot int) {
	peer := d.current[slot]
	if peer == nil {
		return
	}
	d.current[slot] = nil
	d.transport.take(peer)
	delete(d.transport.blocked, peer.Addr())
	d.handle(&donePeerMsg{peer: peer})
}

// handle delivers one message through the serialized block handler entry
// point and checks every invariant afterwards.
func (d *dstRun) handle(message any) {
	d.t.Helper()
	if timeout, ok := message.(*headerSyncTimeoutMsg); ok {
		d.timeouts++
		// An expired request means the peer stalled; production may
		// quarantine it.
		d.fault(timeout.peer, "timeout")
	}
	d.bm.handlePeerMessage(d.peers, message)
	synctest.Wait()
	d.reapRemoved()
	d.check(fmt.Sprintf("message:%T", message))
}

// reapRemoved disconnects peers the block manager dropped from its candidate
// list, as the server would after peer.Disconnect.
func (d *dstRun) reapRemoved() {
	for slot, peer := range d.current {
		if peer == nil || (d.listed(peer) && !dstDisconnected(peer)) {
			continue
		}

		// Dropping a peer from the candidate list is not punishment;
		// disconnecting it is. Either way the slot is gone.
		_, punished := d.punished[peer.Addr()]
		if !punished && dstDisconnected(peer) {
			d.punished[peer.Addr()] = "disconnect"
		}
		d.current[slot] = nil
		d.transport.take(peer)
		d.bm.handlePeerMessage(d.peers, &donePeerMsg{peer: peer})
		synctest.Wait()
	}
}

// dstDisconnected reports whether production called Disconnect on peer. The
// simulated peer has no socket, so the btcd disconnect flag is the only trace.
func dstDisconnected(peer *ServerPeer) bool {
	return reflect.ValueOf(peer.Peer).Elem().
		FieldByName("disconnect").Int() != 0
}

// fault marks peer as misbehaving or slow.
func (d *dstRun) fault(peer *ServerPeer, reason string) {
	if peer == nil {
		return
	}
	d.faultAddr(peer.Addr(), reason)
}

// faultAddr marks the peer with addr as misbehaving or slow.
func (d *dstRun) faultAddr(addr, reason string) {
	if _, ok := d.faulted[addr]; !ok {
		d.faulted[addr] = reason
	}
}

// messageKey orders block-handler callbacks canonically before the seeded
// shuffle, so delivery order depends only on the seed.
func messageKey(message any) string {
	switch msg := message.(type) {
	case *headerSyncTimeoutMsg:
		return fmt.Sprintf("timeout:%020d", msg.requestID)
	case *headerSyncCommitRetryMsg:
		return fmt.Sprintf("retry:%020d", msg.generation)
	case *headerSyncCooldownMsg:
		return fmt.Sprintf("cooldown:%020d", msg.generation)
	case *headerSyncBootstrapMsg:
		return "bootstrap"
	case *headersMsg:
		return "headers:" + msg.peer.Addr()
	case *donePeerMsg:
		return "done:" + msg.peer.Addr()
	case *newPeerMsg:
		return "new:" + msg.peer.Addr()
	default:
		return fmt.Sprintf("%T", message)
	}
}

// pump delivers every callback that is ready at the current virtual instant.
// synctest makes the set deterministic but not its order, so the batch is
// sorted and then shuffled by the seed.
func (d *dstRun) pump() {
	d.t.Helper()
	for !d.stopped {
		synctest.Wait()
		var messages []any
	drain:
		for {
			select {
			case message := <-d.bm.peerChan:
				messages = append(messages, message)
			default:
				break drain
			}
		}
		if len(messages) == 0 {
			return
		}
		sort.SliceStable(messages, func(i, j int) bool {
			return messageKey(messages[i]) <
				messageKey(messages[j])
		})
		d.rng.Shuffle(len(messages), func(i, j int) {
			messages[i], messages[j] = messages[j], messages[i]
		})
		for _, message := range messages {
			d.handle(message)
		}
	}
}

// dstReplyKind selects how a peer answers its queued request.
type dstReplyKind uint8

const (
	dstReplyHonest dstReplyKind = iota
	dstReplyCorrupt
	dstReplyPartial
	dstReplyEmpty
	dstReplyOverlong
	dstReplyReorder
	dstReplyFork
	dstReplyBadTime
	dstReplyBadBits
	dstReplyGap
	dstReplyUnsolicited
	dstReplyKinds
)

// String returns a short reply kind name.
func (k dstReplyKind) String() string {
	return [...]string{
		"honest", "corrupt", "partial", "empty", "overlong",
		"reorder", "fork", "bad_time", "bad_bits", "gap",
		"unsolicited",
	}[k]
}

// reply answers the request queued for slot. Every kind except honest marks
// the peer faulty, since production may then punish it.
func (d *dstRun) reply(slot int, kind dstReplyKind) bool {
	d.t.Helper()
	peer := d.current[slot]
	if peer == nil || d.transport.blocked[peer.Addr()] {
		return false
	}

	// An unsolicited reply answers some other peer's request without
	// consuming this peer's own.
	if kind == dstReplyUnsolicited {
		for _, request := range d.transport.queued {
			if request.peer == peer {
				continue
			}
			d.fault(peer, kind.String())
			d.deliver(&headersMsg{
				peer: peer, headers: d.answer(request,
					dstReplyHonest),
			})
			return true
		}
		return false
	}

	request, ok := d.transport.take(peer)
	if !ok {
		return false
	}
	if kind != dstReplyHonest {
		d.fault(peer, kind.String())
	}
	// Any reply whose last header is not honest can win discovery with a
	// boundary of its own.
	switch kind {
	case dstReplyFork, dstReplyBadTime, dstReplyBadBits,
		dstReplyCorrupt:

		d.anchorLiar = true

	case dstReplyHonest:
		// Remember an honest reply that conflicts with a liar's
		// anchor already known at its final height: production
		// quarantines its sender (known gap F12).
		if d.conflictsWithAnchor(peer, request.start, request.stop) {
			d.anchorConflicts[peer.Addr()] = true
		}
	}
	log.Debugf("dst reply slot=%d %d..%d %s", slot, request.start,
		request.stop, kind)
	d.steps = append(d.steps, fmt.Sprintf("reply slot=%d %d..%d %s",
		slot, request.start, request.stop, kind))
	d.last[slot] = &headersMsg{peer: peer, headers: d.answer(request, kind)}
	d.deliver(d.last[slot])
	return true
}

// answer builds the headers message for request.
func (d *dstRun) answer(request dstHeaderRequest,
	kind dstReplyKind) *wire.MsgHeaders {

	fixture := dstChains()
	chain := d.transport.chain
	headers := chain[request.start+1 : request.stop+1]
	if request.start < dstChainHeight {
		count := int(request.stop - request.start)
		switch kind {
		case dstReplyFork:
			headers = fixture.fork[request.start][:count]
		case dstReplyBadTime:
			headers = fixture.badTime[request.start][:count]
		case dstReplyBadBits:
			headers = fixture.badBits[request.start][:count]
		}
	}

	msg := wire.NewMsgHeaders()
	for _, header := range headers {
		copyHeader := *header
		require.NoError(d.t, msg.AddBlockHeader(&copyHeader))
	}
	n := len(msg.Headers)
	switch kind {
	case dstReplyCorrupt:
		if n > 0 {
			msg.Headers[0].Bits = 0
		}
	case dstReplyPartial:
		if n > 0 {
			msg.Headers = msg.Headers[:n-1]
		}
	case dstReplyEmpty:
		msg.Headers = nil
	case dstReplyOverlong:
		if int(request.stop)+1 < len(chain) {
			extra := *chain[request.stop+1]
			msg.Headers = append(msg.Headers, &extra)
		}
	case dstReplyReorder:
		if n > 1 {
			msg.Headers[0], msg.Headers[1] =
				msg.Headers[1], msg.Headers[0]
		}
	case dstReplyGap:
		if n > 2 {
			msg.Headers = slices.Delete(msg.Headers, 1, 2)
		}
	}
	return msg
}

// deliver encodes and decodes a reply, then queues it through QueueHeaders
// like the peer read loop. Encoding prevents duplicates from sharing mutable
// header objects with earlier callbacks. TCP framing is not simulated.
func (d *dstRun) deliver(message *headersMsg) {
	d.t.Helper()
	var buf bytes.Buffer
	require.NoError(d.t, message.headers.BtcEncode(
		&buf, wire.ProtocolVersion, wire.BaseEncoding,
	))
	decoded := wire.NewMsgHeaders()
	require.NoError(d.t, decoded.BtcDecode(
		&buf, wire.ProtocolVersion, wire.BaseEncoding,
	))
	d.bm.QueueHeaders(decoded, message.peer)
	d.pump()
}

// enabled reports whether the seed enabled a fault class.
func (d *dstRun) enabled(fault dstFault) bool {
	return d.cfg.faults&fault != 0
}

// step decodes and applies one scripted op. An op whose fault class is
// disabled degrades to an honest reply, so every seed still makes progress.
func (d *dstRun) step(value byte) {
	d.t.Helper()
	op, arg := value%16, int(value/16)
	slot := arg % dstPeerSlots
	sub := arg / dstPeerSlots
	label := fmt.Sprintf("op=%d arg=%d", op, arg)

	honest := func() {
		d.reply(slot, dstReplyHonest)
	}
	switch {
	case op <= 2:
		d.reply(int(op), dstReplyHonest)

	case op == 3 && d.enabled(dstFaultDuplicate):
		// TCP does not duplicate messages, so a repeated reply is the
		// peer's doing. Once its request is gone, the legacy path may
		// reject the repeat as unconnected headers.
		if d.last[slot] != nil {
			d.fault(d.last[slot].peer, "duplicate")
			d.deliver(d.last[slot])
		}

	case op == 4:
		if d.enabled(dstFaultStall) && sub%2 == 1 {
			// Keep one peer responsive while the others stall.
			for d.reply(slot, dstReplyHonest) {
			}
			for _, peer := range d.current {
				if peer != nil && d.transport.queuedFor(peer) {
					d.fault(peer, "stall")
				}
			}
			time.Sleep(dstStall)
		} else {
			time.Sleep(time.Duration(arg+1) * time.Millisecond)
		}

	case op == 5 && d.enabled(dstFaultChurn):
		if peer := d.current[slot]; peer != nil {
			d.fault(peer, "churn")
			d.disconnect(slot)
		}

	case op == 6 && d.enabled(dstFaultChurn):
		if d.current[slot] == nil {
			d.connect(slot)
		}

	case op == 7 && d.enabled(dstFaultBlock):
		if peer := d.current[slot]; peer != nil {
			id := peer.Addr()
			d.transport.blocked[id] = !d.transport.blocked[id]
			d.fault(peer, "blocked")
		}

	case op == 8 && d.enabled(dstFaultStore):
		mode := dstStoreMode(1 + arg%3)
		if d.store.mode == mode {
			mode = dstStoreOK
		}
		d.store.mode = mode
		label += " store=" + mode.String()

	case op == 9 && d.enabled(dstFaultAsk):
		// Faults land at a blockmanager call site or at an ask the
		// controller makes itself, before or after it applies.
		ops := int(headerSyncAskOps) + int(headersync.AskOps)
		target, after := arg%ops, (arg/ops)%2 == 1
		if target < int(headerSyncAskOps) {
			fault := headerSyncAskFailBefore
			if after {
				fault = headerSyncAskFailAfter
			}
			d.askFaults[headerSyncAskOp(target)] = fault
			break
		}
		fault := headersync.AskFailBefore
		if after {
			fault = headersync.AskFailAfter
		}
		op := headersync.AskOp(target - int(headerSyncAskOps))
		d.controllerFaults[op] = fault

	case (op == 10 || op == 11) && d.enabled(dstFaultByzantine):
		kind := 1 + dstReplyKind(
			(int(op-10)*6+sub)%int(dstReplyKinds-1),
		)

		// Without a checkpoint above it, an equal-work fork is a
		// valid chain and may legitimately win. On mainnet an
		// attacker cannot mine one, so discovery modes model liars
		// whose chains fail contextual checks instead.
		if kind == dstReplyFork && d.cfg.mode.discovery() {
			kind = dstReplyBadTime
		}
		label += " reply=" + kind.String()
		d.reply(slot, kind)

	case op == 12 && d.enabled(dstFaultRestart):
		d.restart()

	case op == 13 && d.enabled(dstFaultShutdown):
		d.shutdown()
		return

	case op == 14 && d.enabled(dstFaultRTT):
		if peer := d.current[slot]; peer != nil {
			rtt := time.Duration(sub+1) * 150 * time.Millisecond
			_ = d.bm.headerSync.UpdatePeerRTT(
				d.t.Context(), peer.Addr(), rtt,
			)
		}

	case op == 15:
		for slot := range d.current {
			d.reply(slot, dstReplyHonest)
		}

	default:
		honest()
	}
	d.pump()
	d.check("step " + label)
}

// restart stops the block manager and rebuilds it over the same durable
// stores, as a process restart would. Every connection drops.
func (d *dstRun) restart() {
	d.t.Helper()
	require.NoError(d.t, d.bm.Stop())
	synctest.Wait()
	for len(d.bm.peerChan) > 0 {
		<-d.bm.peerChan
	}
	d.restarts++
	d.transport.queued = nil
	clear(d.transport.blocked)
	d.peers = list.New()
	old := d.current
	d.current = [dstPeerSlots]*ServerPeer{}
	d.build()
	for slot, peer := range old {
		if peer != nil {
			d.connect(slot)
		}
	}
	d.pump()
}

// shutdown stops the block manager at an arbitrary point and checks that no
// request or retry callback survives it.
func (d *dstRun) shutdown() {
	d.t.Helper()
	require.NoError(d.t, d.bm.Stop())
	d.stopped = true
	synctest.Wait()
	pending := len(d.bm.peerChan)
	time.Sleep(3 * dstStall)
	synctest.Wait()
	require.Len(d.t, d.bm.peerChan, pending,
		"request/retry callback survived shutdown")
	require.Zero(d.t, d.bm.headerSync.Session().ActiveRangeCount())
	require.Nil(d.t, d.bm.headerCommitRetryTimer)
	d.bm.QueueHeaders(wire.NewMsgHeaders(), d.current[0])
	require.Len(d.t, d.bm.peerChan, pending)
}

// heal restores storage and gives every slot a responsive, honest peer. It
// changes only the environment, never scheduler state: replies and real
// deadlines must finish the work without directly invoking commit.
func (d *dstRun) heal() {
	d.t.Helper()
	d.store.mode = dstStoreOK
	clear(d.askFaults)
	clear(d.controllerFaults)
	clear(d.transport.blocked)
	// Only empty slots get a fresh peer. Replacing a faulted peer that is
	// still connected would run the done-peer path, which resets the
	// header state and restarts sync, and would hide a stall the faulted
	// peer left behind.
	for slot, peer := range d.current {
		if peer == nil {
			d.connect(slot)
		}
	}
	d.pump()
	idle := 0
	for rounds := 0; rounds < dstHealRounds; rounds++ {
		if d.quiescent() == nil {
			break
		}

		// Production may still drop peers that the scripted phase
		// faulted. Keep every slot filled so liveness never depends on
		// fewer honest peers than the confirmation policy needs.
		for slot, peer := range d.current {
			if peer == nil {
				d.connect(slot)
			}
		}
		progressed := false
		for slot := range d.current {
			if d.reply(slot, dstReplyHonest) {
				progressed = true
			}
		}
		// Let virtual time move every round, so production timers
		// such as the commit retry still fire while replies keep
		// arriving. Half a request timeout lets peers answer a request
		// sent this round before its deadline.
		// When nothing moves for a few rounds, wait out a peer
		// cooldown, which production recovers with its own wakeup.
		switch {
		case progressed:
			idle = 0
			time.Sleep(dstRequestTimeout / 2)
		case idle < 3:
			idle++
			time.Sleep(dstStall)
		default:
			idle = 0
			time.Sleep(defaultHeaderPeerCooldown)
		}
		d.pump()
	}
	d.check("healed")
}

// tipPhase follows the chain past the sync target, then announces a heavier
// branch from below the old tip. It drives the serial path's extension and
// reorg handling, rollBackToHeight, and the reorg's block notifications.
func (d *dstRun) tipPhase() {
	d.t.Helper()
	fixture := dstChains()
	announce := func(chain []*wire.BlockHeader, from int) {
		best := uint32(len(chain) - 1)
		d.transport.chain = chain
		d.transport.best = best
		d.target = best
		for _, peer := range d.current {
			if peer != nil {
				peer.UpdateLastBlockHeight(int32(best))
			}
		}
		msg := wire.NewMsgHeaders()
		for _, header := range chain[from:] {
			copyHeader := *header
			require.NoError(d.t, msg.AddBlockHeader(&copyHeader))
		}
		// Announce from the sync peer, or any connected peer.
		peer := d.bm.SyncPeer()
		if peer == nil || d.slotOf(peer) < 0 {
			for slot := range d.current {
				if d.current[slot] == nil {
					d.connect(slot)
				}
			}
			peer = d.current[0]
		}
		d.deliver(&headersMsg{peer: peer, headers: msg})
		d.heal()
	}

	// Extend the honest chain by announcing the new blocks.
	announce(fixture.honest, dstChainHeight+1)
	d.check("tip_extended")

	// A branch forking below the last checkpoint must be refused; above
	// it, the heavier branch must win.
	if d.cfg.mode.discovery() && d.cfg.mode != dstModeAnchor {
		d.inv.reorg = true
		announce(fixture.reorg, dstReorgFork+1)
		d.check("reorged")
	}
}

// check runs every per-event invariant.
func (d *dstRun) check(event string) {
	d.t.Helper()
	d.inv.check(event)
	d.steps = append(d.steps, fmt.Sprintf("%s:height=%d", event,
		d.inv.lastTip))
}

// snapshot returns the comparable outcome of the run, including the final
// range and peer tables so a divergence that changes no event is still seen.
func (d *dstRun) snapshot() dstSnapshot {
	ctx := d.t.Context()
	snapshot := dstSnapshot{
		Events: d.recorder.Events(), Steps: d.steps,
		Writes: d.store.writes, Bans: d.bans,
		Timeouts: d.timeouts, Sent: d.transport.sent,
		Restarts: d.restarts,
		Ntfns:    d.ntfns,

		ConflictPunishments: d.conflictPunishments,
		Poisoned:            d.poisoned,
	}
	if d.stopped {
		return snapshot
	}
	manager := d.bm.headerSync.Manager()
	nextID := d.rangeIDLimit()
	for id := headersync.RangeID(1); id < nextID; id++ {
		rng, ok, err := manager.SnapshotRange(ctx, id)
		require.NoError(d.t, err)
		if ok {
			snapshot.Ranges = append(snapshot.Ranges, rng)
		}
	}
	for _, peer := range d.current {
		if peer == nil {
			continue
		}
		state, ok, err := manager.SnapshotPeer(ctx, peer.Addr())
		require.NoError(d.t, err)
		if ok {
			snapshot.Peers = append(snapshot.Peers, state)
		}
	}
	return snapshot
}

// describe renders the run for a failure message.
func (d *dstRun) describe() string {
	var out strings.Builder
	fmt.Fprintf(&out, "mode=%s faults=%#x\n", d.cfg.mode, d.cfg.faults)
	if !d.stopped {
		manager := d.bm.headerSync.Manager()
		nextID := d.rangeIDLimit()
		for id := headersync.RangeID(1); id < nextID; id++ {
			rng, _, _ := manager.SnapshotRange(d.t.Context(), id)
			fmt.Fprintf(&out, "range=%+v\n", rng)
		}
	}
	fmt.Fprintf(&out, "faulted=%v punished=%v\n", d.faulted, d.punished)
	if !d.stopped {
		var queued []string
		for _, request := range d.transport.queued {
			queued = append(queued, fmt.Sprintf("%s:%d..%d",
				request.peer.Addr(), request.start,
				request.stop))
		}
		syncPeer := "none"
		if peer := d.bm.SyncPeer(); peer != nil {
			syncPeer = peer.Addr()
		}
		fmt.Fprintf(&out, "sync_peer=%s serial_timer=%v queued=%v\n",
			syncPeer, d.bm.headerSyncTimer != nil, queued)
	}
	fmt.Fprintf(&out, "steps=%v\n", d.steps)
	return out.String()
}

// runHeaderTransportDST runs one seed to completion: the scripted fault phase,
// then healing, then the liveness and quiescence checks.
func runHeaderTransportDST(t *testing.T, script []byte) dstSnapshot {
	t.Helper()
	var result dstSnapshot
	synctest.Test(t, func(t *testing.T) {
		cfg, ops := decodeDSTScript(script)
		d := newDSTRun(t, cfg)
		for _, value := range ops {
			if d.stopped {
				break
			}
			d.step(value)
		}
		if !d.stopped {
			d.heal()
			if d.enabled(dstFaultReorg) {
				d.tipPhase()
			}
			err := d.quiescent()
			if err != nil && d.poisonedAnchor() {
				// Known gap F13: a liar's confirmed but
				// never-validated anchor blocks its span.
				d.poisoned = true
				err = nil
			}
			require.NoError(t, err,
				"recovery did not complete; seed=%x\n%s",
				script, d.describe())
		}
		result = d.snapshot()
	})
	return result
}

// dstCorpus is the fixed seed corpus. Each entry is checked for determinism
// by running it twice.
var dstCorpus = [][]byte{
	{}, {0, 0, 0, 20, 10, 0}, {0, 0, 0, 0, 1}, {0, 0, 0, 4 + 16},
	{0, 255, 255, 133}, {0, 255, 255, 5, 6}, {0, 255, 255, 7, 4 + 16},
	{0, 255, 255, 10}, {0, 255, 255, 11},
	{0, 255, 255, 8, 20, 10, 0, 8},
	{0, 255, 255, 20, 21, 4 + 16, 5, 6},
	{1, 255, 255, 0, 1, 2, 15},
	{2, 255, 255, 0, 1, 2, 15},
	{3, 255, 255, 0, 1, 2, 15},
	{4, 255, 255, 0, 1, 2, 15},
	{3, 255, 255, 10 + 5*16, 0, 1, 2},
	{4, 255, 255, 10 + 5*16, 0, 1, 2},
	{2, 255, 255, 11, 10 + 6*16, 0, 1},
	{1, 255, 255, 8 + 16, 15, 15},
	{0, 255, 255, 8 + 32, 15, 15},
	{1, 255, 255, 9, 15, 15}, {1, 255, 255, 9 + 16*4, 15},
	{1, 255, 255, 12, 0, 15}, {3, 255, 255, 0, 12, 15},
	{1, 255, 255, 0, 13}, {3, 255, 255, 14, 14 + 16, 15},
}

// dstRegressionSeeds are swarm seeds that found bugs, in hex. Each one is
// replayed by TestHeaderTransportDST so the fix stays fixed.
var dstRegressionSeeds = []string{
	// Ambiguous commit stall, B2 ban, torn write in frontier discovery.
	"01ffff180f0f", "00ffff280f0f", "03ffff000c0f",
	// F12 sticky rejection and F11 discovery livelock.
	"3141300b3030", "f477f208201c96081121373b6cafd5cc2f",
	"5479161c102b44973f909820a050fd3e8a5e6804bcf7de019326",
	// Staged leaks after serial writes and below-tip staging.
	"40732f9e5a32e68ede3bca6f1c12489bf5098721c492ae923a196503",
	"ef395d762a43ac360572c13572e5441c614e0973842cdf34b27ccb7ac94e08",
	// Overlap abort in anchor discovery.
	"2f65cc0de558c6aa",
	// Serial path cache and tip consistency.
	"4a1de7e96a0717c39ab32fbf0d1277596fb8b7a39d", "c4eb4a04faf7f3c5",
	"0e1b043680dbd3cd07e0df85e4b36d6ac7f867a544f1d718fd25941b",
	// F1 asks that apply then fail, and the new-peer dispatch stall.
	"7a6fa024c529a8e9abc0eb2e03b0b23f8c233fae580d45",
	"ffbaceae8c60fb040cf599bcb6a96b9296187a1b84081bf190f7120a25c4d67ab2ca",
	"4b68e926ec48d9f1cfc8d123e73b08af8001434a9e289865",
	"66fa4297a69e52879dae65edd0af69832a418111e2d2e4a19d547675c48ccf4dbf",
	"3c7b937a39a6f46f3021287c317877d3",
	// Reorg notification deadlock, FSM left behind after a failed serial
	// write, retry after a failed serial write, and the stale next
	// checkpoint that let a fork past three checkpoints.
	"d77bb606ee817e1a5b31f65e4e6905ccf1855ce8db5a79de89bd96fa12e126530d45",
	"195120c67404a29dc585cb8637de9b98c333e983a88d92704c",
	"ded4487caf9a7882f5425aeef6378aab1779baf98a6629f1bde352",
	"6f53a47a3d22a22f18b5abbffa735e7e8c81bf47ada842d000dc95854477",
	// A late discovery reply matched to a range from the same locator,
	// and heal phases that lost peers mid-recovery.
	"c549a8254d53ba71b6c1416f29523562a5",
	"277d75d4a62964a270d68e72b3131be1f056dbf0",
	"5efcd649b1dba97bb29b86658bf2a2bad9c4a9900330885243bf",
	"2c32dc3e81d1e62029fc492b6b9029a02395adcd82d151cc",
	// A corrupt discovery reply winning an anchor, a lagging peer asked
	// for blocks it never had, and work queued above every peer's tip.
	"81cbc61289d3bc6f773c1341504a585a1a1d6e1055bcad",
	"3ff9cd239f8f0c0caa711003bea0fe34e8ce7bd3aa3919ced732cf38cef0",
	"17d1c2321aed3719ba5dda26f1eb1020cf00ffa933c8e3f193c7ba",
	// A blocked peer whose cooldown expired with nothing to wake it.
	"594a287c17b7ef352630f8133cea52e3f222a8451e7a1c1575c1b2",
	"7f594edd1aa359a3df8e520794782a7397b87325138fd79aeef7cdc3f5384d",
	// A reply judged against a range the serial writer already committed.
	"1b66426e6b5a8ed82ea0095936fa89bca0430d",
	// Serial mode: a follow-up request to a non-sync peer cancelling the
	// sync peer's deadline, and a failed follow-up send to the sync peer.
	"8fc173f7bba5c713c3f4ceaafb8a912e6a8fc5d48377eff027b7",
	"cfc8b87fa5b27e053a45c56ba6de82f71fa3bbd6",
	// Serial mode: a sync peer answering empty kept the role with no
	// request and no deadline.
	"ff6240a6106ae838014c714ad54cff39b6ba0afdbaf2842424993232d5",
}

// dstSeeds returns the fixed corpus plus every regression seed.
func dstSeeds(t testing.TB) [][]byte {
	seeds := append([][]byte(nil), dstCorpus...)
	for _, encoded := range dstRegressionSeeds {
		seed, err := hex.DecodeString(encoded)
		require.NoError(t, err)
		seeds = append(seeds, seed)
	}
	return seeds
}

func TestHeaderTransportDST(t *testing.T) {
	for _, seed := range dstSeeds(t) {
		t.Run(fmt.Sprintf("%x", seed), func(t *testing.T) {
			first := runHeaderTransportDST(t, seed)
			require.Equal(t, first, runHeaderTransportDST(t, seed),
				"same seed produced a different trace")
		})
	}
}

func FuzzHeaderTransportDST(f *testing.F) {
	for _, seed := range dstSeeds(f) {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, seed []byte) {
		first := runHeaderTransportDST(t, seed)

		// Determinism is verified on the fixed corpus. Re-run a cheap
		// sample of generated inputs too, keyed by content so a saved
		// input replays the same way.
		if sha256.Sum256(seed)[0]%16 == 0 {
			require.Equal(t, first, runHeaderTransportDST(t, seed))
		}
	})
}

// dstSwarmSeed derives the i-th swarm seed of a campaign.
func dstSwarmSeed(rng *rand.Rand) []byte {
	seed := make([]byte, 3+rng.IntN(dstMaxSteps))
	for i := range seed {
		seed[i] = byte(rng.Uint32())
	}
	return seed
}

// TestHeaderTransportDSTSwarm runs a fixed, reproducible swarm campaign. Each
// seed draws its own mode and fault set. Set the count with -dst.swarm.
func TestHeaderTransportDSTSwarm(t *testing.T) {
	rng := rand.New(rand.NewPCG(*dstSwarmSeedFlag, 0))
	for i := 0; i < *dstSwarmRuns; i++ {
		seed := dstSwarmSeed(rng)
		ok := t.Run(fmt.Sprintf("%d_%x", i, seed), func(t *testing.T) {
			runHeaderTransportDST(t, seed)
		})
		if !ok {
			t.Fatalf("swarm seed %d failed after %d runs: %x", i,
				i+1, seed)
		}
	}
}

func TestHeaderTransportCheckpointRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := newDSTRun(t, dstConfig{mode: dstModeCheckpoint})
		d.store.mode = dstStoreDown
		for slot := range d.current {
			d.reply(slot, dstReplyHonest)
		}
		d.pump()
		require.Zero(t, d.inv.lastTip)
		require.NotNil(t, d.bm.headerCommitRetryTimer)
		sent := d.transport.sent
		d.store.mode = dstStoreOK
		time.Sleep(time.Second)
		d.pump()
		require.EqualValues(t, 2, d.inv.lastTip)
		require.Greater(t, d.transport.sent, sent,
			"retry must dispatch the next checkpoint span")
	})
}

func TestHeaderTransportDuplicatePreservesRequest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := newDSTRun(t, dstConfig{mode: dstModeCheckpointAll})
		require.True(t, d.reply(0, dstReplyHonest))
		manager := d.bm.headerSync.Manager()
		peerID := d.current[0].Addr()
		before, ok, err := manager.SnapshotPeer(t.Context(), peerID)
		require.NoError(t, err)
		require.True(t, ok)
		require.NotEqual(t, headersync.NoRange, before.ActiveRange)
		events := d.recorder.Events()
		d.deliver(d.last[0])
		after, ok, err := manager.SnapshotPeer(t.Context(), peerID)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, before, after,
			"duplicate changed ownership or peer quality")
		require.Equal(t, events, d.recorder.Events())
		require.True(t, d.bm.headerSync.Session().RangeActive(peerID))
	})
}

func TestHeaderTransportFailedCompatibilityWrite(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := newDSTRun(t, dstConfig{mode: dstModeCheckpointAll})
		// Remove request matching to exercise the compatibility writer.
		// The original failing seed entered this path with a duplicate.
		d.inv.skipLeases = true
		d.bm.headerSync.Session().StopAll()
		d.store.mode = dstStoreDown
		message := wire.NewMsgHeaders()
		for _, header := range dstChains().honest[1:3] {
			require.NoError(t, message.AddBlockHeader(header))
		}
		d.deliver(&headersMsg{peer: d.current[0], headers: message})
		require.Zero(t, d.inv.lastTip)
		require.Empty(t, d.store.writes)
		require.Zero(t, d.bm.headerList.Back().Height)
	})
}

func TestHeaderTransportDSTShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := newDSTRun(t, dstConfig{mode: dstModeCheckpointAll})
		require.NotZero(t, d.bm.headerSync.Session().ActiveRangeCount())
		d.bm.scheduleHeaderCommitRetry()
		d.shutdown()
	})
}

func TestHeaderCommitRetryGeneration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := newDSTRun(t, dstConfig{mode: dstModeCheckpointAll})
		d.bm.scheduleHeaderCommitRetry()
		first := d.bm.headerCommitRetryTimer
		generation := d.bm.headerCommitRetryID
		d.bm.scheduleHeaderCommitRetry()
		require.Same(t, first, d.bm.headerCommitRetryTimer)
		d.bm.cancelHeaderCommitRetry()
		d.bm.scheduleHeaderCommitRetry()
		replacement := d.bm.headerCommitRetryTimer
		d.bm.handlePeerMessage(d.peers, &headerSyncCommitRetryMsg{
			generation: generation,
		})
		require.Same(t, replacement, d.bm.headerCommitRetryTimer,
			"obsolete wakeup consumed a replacement timer")
	})
}

// TestHeaderCooldownWakeup checks that a blocked header peer is recovered when
// its cooldown expires, with no other event. Recovery used to happen only
// while dispatching, so a sync waiting on that peer, such as for a second
// anchor observation, never dispatched again.
func TestHeaderCooldownWakeup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Finish sync first, so no request can time out meanwhile.
		d := newDSTRun(t, dstConfig{mode: dstModeFrontier2})
		d.heal()
		require.NoError(t, d.quiescent())
		peer := d.current[1]
		d.fault(peer, "blocked")
		d.bm.updateHeaderSyncPeerState(peer, headersync.PeerBlocked)
		require.NotNil(t, d.bm.headerCooldownTimer)

		time.Sleep(defaultHeaderPeerCooldown)
		d.pump()
		snapshot, ok, err := d.bm.headerSync.Manager().SnapshotPeer(
			t.Context(), peer.Addr(),
		)
		require.NoError(t, err)
		require.True(t, ok)
		require.NotEqual(t, headersync.PeerBlocked, snapshot.State,
			"cooldown expired without recovering the peer")
	})
}

// TestHeaderOrphanedRangeReleased checks that a peer recovering from its
// cooldown gives back an active range it holds with no request behind it, as
// a RequestRange ask that applied but reported an error leaves. With no other
// peer to steal it, the lease used to stay active with nothing running it;
// only a serial batch that happened to cover the span retired it.
func TestHeaderOrphanedRangeReleased(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := newDSTRun(t, dstConfig{mode: dstModeCheckpointAll})
		d.disconnect(1)
		d.disconnect(2)
		d.askFaults[headerSyncAskRequestRange] = headerSyncAskFailAfter
		require.True(t, d.reply(0, dstReplyHonest))
		require.Empty(t, d.askFaults, "fault never fired")

		// Expire the peer's cooldown and run the recovery pass at once,
		// before any timeout or serial batch can retire the lease.
		peerID := d.current[0].Addr()
		session := d.bm.headerSync.Session()
		session.TrackPeerCooldown(
			peerID, headersync.PeerBlocked,
			time.Now().Add(-time.Second), 0,
		)
		d.bm.recoverHeaderSyncPeers(d.peers)
		synctest.Wait()

		manager := d.bm.headerSync.Manager()
		peer, ok, err := manager.SnapshotPeer(t.Context(), peerID)
		require.NoError(t, err)
		require.True(t, ok)

		// The released span must be startable again by the same
		// peer: a peer actor left busy would never take new work.
		d.bm.dispatchHeaderSyncWork(d.peers)
		synctest.Wait()
		require.True(t, session.RangeActive(peerID),
			"recovered peer never started new work")

		nextID := session.NextRangeID()
		for id := headersync.RangeID(1); id < nextID; id++ {
			rng, ok, err := manager.SnapshotRange(t.Context(), id)
			require.NoError(t, err)
			if !ok || rng.State != headersync.RangeActive ||
				rng.OwnerPeer != peerID {

				continue
			}
			require.True(t, session.RangeActive(peerID) ||
				peer.State == headersync.PeerBlocked,
				"range %d active on a ready peer with no "+
					"request", id)
		}
	})
}

// flakyTipStore fails one ChainTip call and counts calls.
type flakyTipStore struct {
	*memBlockHeaderStore
	failAt int
	calls  int
}

// ChainTip fails the failAt-th call.
func (s *flakyTipStore) ChainTip() (*wire.BlockHeader, uint32, error) {
	s.calls++
	if s.calls == s.failAt {
		return nil, 0, errDSTWrite
	}
	return s.memBlockHeaderStore.ChainTip()
}

// TestCFHandlerRetriesTipError checks that a failed block header tip read
// does not end filter header sync. cfHandler used to log it as critical and
// return, which stopped filter sync until restart; with the SQL backends that
// read can fail transiently.
func TestCFHandlerRetriesTipError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bm, store := setupDSTBlockManager(t)
		genesis := chaincfg.SimNetParams.GenesisBlock.Header
		bm.cfg.TimeSource = dstMedianTime{
			MedianTimeSource: bm.cfg.TimeSource,
			offset:           time.Until(genesis.Timestamp),
		}
		// The first read is the synced check in the wait loop; fail
		// the second, cfHandler's own tip read.
		flaky := &flakyTipStore{memBlockHeaderStore: store, failAt: 2}
		bm.cfg.BlockHeaders = flaky

		done := make(chan struct{})
		go func() {
			defer close(done)
			bm.cfHandler()
		}()
		synctest.Wait()
		require.GreaterOrEqual(t, flaky.calls, 2, "tip was never read")
		calls := flaky.calls

		select {
		case <-done:
			t.Fatal("cfHandler exited after a tip read error")
		default:
		}
		time.Sleep(retryTimeout)
		synctest.Wait()
		require.Greater(t, flaky.calls, calls,
			"cfHandler never read the tip again")

		close(bm.quit)
		bm.newHeadersSignal.Broadcast()
		bm.newFilterHeadersSignal.Broadcast()
		<-done
	})
}

// TestHeaderSyncOpCtxCancelledOnStop checks that shutdown cancels headersync
// actor asks. The op context used to derive from context.Background, so Stop
// could wait out the full deadline for every ask still pending.
func TestHeaderSyncOpCtxCancelledOnStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := newDSTRun(t, dstConfig{mode: dstModeCheckpointAll})
		ctx, cancel := d.bm.headerSyncOpCtx()
		defer cancel()
		require.NoError(t, ctx.Err())

		d.stopped = true
		require.NoError(t, d.bm.Stop())
		require.ErrorIs(t, ctx.Err(), context.Canceled)
	})
}

// TestHeaderTransportAmbiguousCommit is the regression for an ambiguous header
// commit. A write that lands but reports an error, a write that lands in part,
// or a CommitReadyRanges ask that fails after a successful write all used to
// leave memory behind disk, after which the commit retry never connected and
// header sync stalled until restart.
func TestHeaderTransportAmbiguousCommit(t *testing.T) {
	cases := []struct {
		name  string
		store dstStoreMode
		ask   headerSyncAskFault
	}{
		{name: "ack_lost", store: dstStoreAckLost},
		{name: "torn", store: dstStoreTorn},
		{name: "commit_ask_after_write", ask: headerSyncAskFailAfter},
		{name: "commit_ask_before", ask: headerSyncAskFailBefore},
	}
	modes := []dstMode{
		dstModeCheckpointAll, dstModeCheckpoint, dstModeFrontier1,
	}
	for _, tc := range cases {
		for _, mode := range modes {
			name := tc.name + "/" + mode.String()
			t.Run(name, func(t *testing.T) {
				runAmbiguousCommit(t, mode, tc.store, tc.ask)
			})
		}
	}
}

// runAmbiguousCommit arms one ambiguous fault for the first commit, heals,
// and requires the run to reach the honest tip with nothing left over.
func runAmbiguousCommit(t *testing.T, mode dstMode, store dstStoreMode,
	ask headerSyncAskFault) {

	synctest.Test(t, func(t *testing.T) {
		d := newDSTRun(t, dstConfig{mode: mode})
		d.store.mode = store
		if ask != headerSyncAskOK {
			d.askFaults[headerSyncAskCommitReady] = ask
		}

		// Let the first replies hit the fault before healing clears
		// it. The bootstrap window may hold discovery back, so give it
		// time to expire.
		for i := 0; i < 2; i++ {
			for slot := range d.current {
				d.reply(slot, dstReplyHonest)
			}
			time.Sleep(headerSyncBootstrapMaxWait)
			d.pump()
		}
		// Each case's fault must actually have fired: a store fault
		// reports one ambiguous write, an ask fault is consumed.
		if store != dstStoreOK {
			require.Equal(t, 1, d.store.ambiguous,
				"store fault never fired")
		}
		require.Empty(t, d.askFaults, "ask fault never fired")
		d.heal()
		require.NoError(t, d.quiescent(), d.describe())
	})
}
