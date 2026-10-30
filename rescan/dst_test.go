package rescan

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/btcsuite/btcd/address/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btclog"
	"github.com/lightninglabs/neutrino/blockntfns"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/stretchr/testify/require"
)

var (
	// rescanSwarmRuns is the number of seeds TestRescanDSTSwarm runs.
	rescanSwarmRuns = flag.Int("rescan.swarm", 64,
		"number of swarm seeds in TestRescanDSTSwarm")

	// rescanSwarmSeed seeds the swarm campaign's seed generator.
	rescanSwarmSeed = flag.Uint64("rescan.seed", 1,
		"seed of the rescan swarm campaign generator")

	// rescanReplay is a hex seed for TestRescanDSTReplay.
	rescanReplay = flag.String("rescan.replay", "",
		"hex seed replayed by TestRescanDSTReplay")

	// rescanLog routes rescan and block notification logs to stderr.
	rescanLog = flag.Bool("rescan.log", false,
		"log rescan output while replaying a seed")
)

const (
	// dstMaxOps is the most ops a seed runs after its three header bytes.
	dstMaxOps = 64

	// dstInitialBlocks is the number of blocks above the start block when
	// the run begins.
	dstInitialBlocks = 12

	// dstModeCount is the number of modes the mode byte's remainder picks.
	dstModeCount = 16
)

// dstFault is one swarm fault class. A seed enables a subset of classes, so
// different runs stress different combinations instead of averaging every
// fault into one background rate. Each class reproduces a fault the scripted
// harness already injected; later steps add new classes in the free bits.
type dstFault uint16

const (
	// dstFaultFilter fails the first filter fetch and lets each new
	// block op fail the next few fetches.
	dstFaultFilter dstFault = 1 << iota

	// dstFaultBlock fails the first block fetch.
	dstFaultBlock

	// dstFaultSubscribe fails the first Subscribe.
	dstFaultSubscribe

	// dstFaultHandoff replaces the tip inside the first successful
	// Subscribe, before it registers, so the subscription never sees
	// the reorg's notifications. The actor subscribes before it scans,
	// so today this only reorgs the chain ahead of the scan. It reaches
	// the handoff once a later fault class closes the subscription
	// mid-run.
	dstFaultHandoff

	// dstFaultHistReorg reorgs the chain from three blocks above the
	// start while the historical scan fetches the next block's filter.
	dstFaultHistReorg

	// dstFaultDeepReorg is the same reorg from one block above the start,
	// below the scanned tip.
	dstFaultDeepReorg

	// dstFaultRewindReorg reorgs the chain while the scripted rewind
	// walks back through it.
	dstFaultRewindReorg

	// dstFaultLiveRetry fails the filter fetch of the live block that is
	// about to be replaced, so a retry is pending across the reorg.
	dstFaultLiveRetry

	// dstFaultGate holds a live filter fetch while a rewind waits in
	// the mailbox.
	dstFaultGate
)

// dstConfig is the decoded swarm configuration of one seed.
type dstConfig struct {
	mode   uint8
	faults dstFault
}

// batchSize returns the production batch size the mode selects.
func (c dstConfig) batchSize() int {
	return [...]int{1, 2, 3, DefaultBatchSize}[c.mode&3]
}

// birthday reports whether the mode sets a wallet birthday inside the
// plain prefix, so the first blocks are not scanned.
func (c dstConfig) birthday() bool {
	return c.mode&4 != 0
}

// base returns the start height the mode selects. The high base crosses
// ProgressInterval during the initial scan.
func (c dstConfig) base() uint32 {
	if c.mode&8 != 0 {
		return dstBaseHigh
	}
	return 0
}

// String returns a short description of the configuration.
func (c dstConfig) String() string {
	return fmt.Sprintf("mode=%d batch=%d birthday=%v base=%d faults=%#x",
		c.mode, c.batchSize(), c.birthday(), c.base(), uint16(c.faults))
}

// decodeRescanDST splits a seed into its configuration and op bytes. Byte 0
// is the mode, bytes 1 and 2 are the little-endian fault mask, and the rest
// are ops, truncated to dstMaxOps. A seed shorter than the header runs every
// fault class in mode 0, with its bytes as ops.
func decodeRescanDST(seed []byte) (dstConfig, []byte) {
	cfg := dstConfig{faults: ^dstFault(0)}
	if len(seed) >= 3 {
		cfg.mode = seed[0] % dstModeCount
		cfg.faults = dstFault(binary.LittleEndian.Uint16(seed[1:3]))
		seed = seed[3:]
	}
	return cfg, seed[:min(len(seed), dstMaxOps)]
}

// dstSnapshot is everything the double run compares.
type dstSnapshot struct {
	events   []dstCallback
	payments []chainhash.Hash
	errors   int
	watch    []string
	state    string
}

// dstRun is one DST run: the production actor over the simulated world,
// the fault layer and the oracle.
type dstRun struct {
	t    *testing.T
	seed []byte
	cfg  dstConfig
	ops  []byte

	w     *dstWorld
	mgr   *blockntfns.SubscriptionManager
	src   *dstSource
	truth *dstTruth
	actor *RescanActor
	start *dstBlock
}

// newDSTRun builds the world, the fault layer and the actor for one seed.
// It must run inside a synctest bubble.
func newDSTRun(t *testing.T, seed []byte) *dstRun {
	cfg, ops := decodeRescanDST(seed)
	base := cfg.base()
	w := newDSTWorld(base + dstInitialBlocks)
	start := w.fx.main[base]

	var startTime time.Time
	if cfg.birthday() {
		startTime = w.fx.main[base+dstPlainPrefix].header.Timestamp
	}

	mgr := blockntfns.NewSubscriptionManager(w)
	mgr.Start()
	d := &dstRun{
		t:     t,
		seed:  seed,
		cfg:   cfg,
		ops:   ops,
		w:     w,
		mgr:   mgr,
		src:   &dstSource{w: w, mgr: mgr},
		start: start,
		truth: newDSTTruth(
			w.fx, start, startTime, w.fx.scripts[dstAddrA],
		),
	}
	d.armStartFaults()

	d.actor = NewRescanActor(ActorConfig{
		Chain:       d.src,
		StartStamp:  start.stamp(),
		StartHeader: start.header,
		StartTime:   startTime,
		WatchAddrs:  []address.Address{w.fx.addrs[dstAddrA]},
		BatchSize:   cfg.batchSize(),
		OnError:     d.truth.onError,
		Callbacks:   d.truth.callbacks(),
	})
	return d
}

// enabled reports whether the seed enabled a fault class.
func (d *dstRun) enabled(fault dstFault) bool {
	return d.cfg.faults&fault != 0
}

// armStartFaults arms the faults that hit the initial sync.
func (d *dstRun) armStartFaults() {
	s := d.src
	if d.enabled(dstFaultFilter) {
		s.filterFail = 1
	}
	if d.enabled(dstFaultBlock) {
		s.blockFail = 1
	}
	if d.enabled(dstFaultSubscribe) {
		s.subscribeFail = 1
	}
	if d.enabled(dstFaultHandoff) {
		s.onSubscribe = func() {
			tip := d.w.tip()
			old, branch := d.w.reorg(tip.height-1, 1)
			d.w.announceReorg(old, branch)
		}
	}
	if d.enabled(dstFaultHistReorg | dstFaultDeepReorg) {
		fork := d.start.height + 3
		if d.enabled(dstFaultDeepReorg) {
			fork = d.start.height + 1
		}
		target := d.start.height + 4
		s.onFilter = func(b *dstBlock) error {
			if b.height != target || !d.w.onActive(b) {
				return nil
			}
			s.mu.Lock()
			s.onFilter = nil
			s.mu.Unlock()

			n := int(d.w.tip().height-fork) + 1
			old, branch := d.w.reorg(fork, n)
			d.announceWhileBusy(old, branch)
			return headerfs.ErrHashNotFound
		}
	}
}

// announceWhileBusy announces a reorg from inside a fault hook, on the
// actor goroutine. It then sleeps for a nanosecond of virtual time, which
// ends only once every other goroutine is blocked. By then the subscription
// bridge has queued the whole burst in the mailbox, so the actor's own
// continuations land behind it rather than racing it.
func (d *dstRun) announceWhileBusy(old, branch []*dstBlock) {
	d.w.announceReorg(old, branch)
	time.Sleep(time.Nanosecond)
}

// settle lets every runnable goroutine block, then separates the next
// stimulus's timer deadlines from this one's.
func (d *dstRun) settle() {
	synctest.Wait()
	time.Sleep(time.Nanosecond)
	synctest.Wait()
}

// sleepRetries advances virtual time by n retry intervals.
func (d *dstRun) sleepRetries(n int) {
	for i := 0; i < n; i++ {
		time.Sleep(filterRetryInterval)
		synctest.Wait()
	}
}

// check runs the op-boundary invariants and fails on any violation so far.
func (d *dstRun) check(label string) {
	d.t.Helper()
	d.truth.checkOp()
	if err := d.truth.err(); err != "" {
		d.t.Fatalf("%s: %s\n%v, seed %x", label, err, d.cfg, d.seed)
	}
}

// current returns the actor's state, which must be StateCurrent.
func (d *dstRun) current(label string) *StateCurrent {
	d.t.Helper()
	state, err := d.actor.CurrentState()
	require.NoError(d.t, err, "%s, seed %x", label, d.seed)
	cur, ok := state.(*StateCurrent)
	require.True(d.t, ok, "%s: state %v, %v, seed %x", label, state,
		d.cfg, d.seed)
	return cur
}

// requireAtTip checks the quiescentAtTip invariant at a phase boundary: the
// actor is current at the active tip and the callbacks delivered it. It
// also checks finishedOnce: a scan that got there has reported finished
// since its last rewind.
func (d *dstRun) requireAtTip(label string) {
	d.t.Helper()
	d.truth.checkFinished()
	d.check(label)
	state, err := d.actor.CurrentState()
	require.NoError(d.t, err, "%s, seed %x", label, d.seed)
	tip := d.w.tip()
	cur, ok := state.(*StateCurrent)
	switch {
	case !ok:
		d.t.Fatalf("%s: %s: state %v, chain tip %d\n%v, seed %x",
			label, invQuiescentAtTip, state, tip.height, d.cfg,
			d.seed)

	case cur.CurStamp.Hash != tip.hash:
		d.t.Fatalf("%s: %s: scan tip %d, chain tip %d\n%v, seed %x",
			label, invQuiescentAtTip, cur.CurStamp.Height,
			tip.height, d.cfg, d.seed)

	case d.truth.tip() != tip:
		d.t.Fatalf("%s: %s: callback tip %d, chain tip %d\n%v, "+
			"seed %x", label, invQuiescentAtTip,
			d.truth.tip().height, tip.height, d.cfg, d.seed)
	}
}

// rewind asks the actor to rewind to target. allowance is the number of
// connects already in flight that may still be delivered first.
func (d *dstRun) rewind(target uint32, allowance int) error {
	d.truth.beginRewind(target, allowance)
	return d.actor.AddWatchAddrs(nil, nil, &target)
}

// initialSync starts the actor and lets the historical scan reach the tip.
func (d *dstRun) initialSync() {
	d.actor.Start()
	synctest.Wait()
	d.sleepRetries(6)

	d.src.mu.Lock()
	subscriptions := d.src.subscriptions
	d.src.mu.Unlock()
	require.Positive(d.t, subscriptions,
		"subscription did not recover, seed %x", d.seed)
	if d.enabled(dstFaultHistReorg | dstFaultDeepReorg) {
		d.src.mu.Lock()
		fired := d.src.onFilter == nil
		d.src.mu.Unlock()
		require.True(d.t, fired, "missing historical reorg")
	}
	d.requireAtTip("initial sync")
}

// rewindPhase rewinds to two blocks above the start, optionally reorging the
// chain under the walk back.
func (d *dstRun) rewindPhase() {
	fired := true
	if d.enabled(dstFaultRewindReorg) {
		fired = false
		fork := d.start.height + 3
		victim, _ := d.w.at(fork + 1)
		d.src.mu.Lock()
		d.src.onHeader = func(hash chainhash.Hash) error {
			if hash != victim.hash {
				return nil
			}
			d.src.mu.Lock()
			d.src.onHeader = nil
			d.src.mu.Unlock()
			fired = true

			n := int(d.w.tip().height - fork)
			old, branch := d.w.reorg(fork, n)
			d.announceWhileBusy(old, branch)
			return nil
		}
		d.src.mu.Unlock()
	}

	require.NoError(d.t, d.rewind(d.start.height+2, 0))
	synctest.Wait()
	d.sleepRetries(4)
	require.True(d.t, fired, "missing rewind reorg")
	d.requireAtTip("rewind")
}

// liveReorg replaces a live block, possibly with a retry pending for it.
func (d *dstRun) liveReorg() {
	base := d.w.tip()
	old := d.w.extend()
	if d.enabled(dstFaultLiveRetry) {
		d.src.mu.Lock()
		d.src.filterFail = 1
		d.src.mu.Unlock()
	}
	d.w.announceConnected(old)
	synctest.Wait()

	_, branch := d.w.reorg(base.height, 1)
	d.w.announceReorg([]*dstBlock{old}, nil)
	synctest.Wait()
	d.w.announceConnected(branch[0])
	synctest.Wait()

	errors := d.truth.errorCount()
	d.sleepRetries(3)
	require.Equal(d.t, errors, d.truth.errorCount(),
		"obsolete retry after recovery, seed %x", d.seed)
	d.requireAtTip("live reorg")
}

// newBlock extends the chain by one block and announces it. With the filter
// class on, the next arg%3 filter fetches fail. With the gate class on and
// bit 4 of arg set, the block's filter fetch is held while a rewind waits in
// the mailbox.
func (d *dstRun) newBlock(arg byte, faulty bool) {
	gate := false
	if faulty && d.enabled(dstFaultGate) && arg&16 != 0 {
		// Only a current scan fetches the new block's filter at once.
		state, err := d.actor.CurrentState()
		require.NoError(d.t, err)
		cur, ok := state.(*StateCurrent)
		gate = ok && cur.CurStamp.Hash == d.w.tip().hash
	}

	var entered, release chan struct{}
	d.src.mu.Lock()
	if gate {
		entered = make(chan struct{})
		release = make(chan struct{})
		d.src.gateEntered, d.src.gateRelease = entered, release
	}
	if faulty && d.enabled(dstFaultFilter) {
		d.src.filterFail = int(arg % 3)
	}

	// A held fetch that is about to fail delivers no connect, so the
	// rewind gets no allowance for it.
	allowance := 1
	if d.src.filterFail > 0 {
		allowance = 0
	}
	d.src.mu.Unlock()

	d.w.announceConnected(d.w.extend())
	if !gate {
		return
	}

	<-entered
	target := d.start.height + 2
	done := make(chan error, 1)
	d.truth.beginRewind(target, allowance)
	go func() {
		done <- d.actor.AddWatchAddrs(nil, nil, &target)
	}()
	synctest.Wait()
	close(release)
	require.NoError(d.t, <-done)
}

// step decodes and applies one op. The low three bits pick the op and the
// rest are its argument. An op whose fault class is off performs the honest
// action, so every seed keeps making progress. Ops 4 to 7 are honest
// actions reserved for the fault classes of later steps.
func (d *dstRun) step(i int, value byte) {
	op, arg := value%8, value/8
	switch op {
	case 0, 4, 6:
		d.newBlock(arg, op == 0)

	case 1, 5:
		time.Sleep(filterRetryInterval)

	case 2:
		// Rewind even when a retry is outstanding.
		span := d.w.tip().height - d.start.height + 1
		target := d.start.height + uint32(arg)%span
		require.NoError(d.t, d.rewind(target, 0))

	case 3:
		require.NoError(d.t, d.actor.AddWatchAddrs(nil, nil, nil))

	case 7:
		_, err := d.actor.CurrentState()
		require.NoError(d.t, err)
	}
	d.settle()
	d.check(fmt.Sprintf("op %d (%#x)", i, value))
}

// heal turns every fault off, lets the retries run, and checks that the run
// is quiescent at the tip.
func (d *dstRun) heal() {
	d.src.clearFaults()
	d.sleepRetries(20)
	d.requireAtTip("heal")
	d.truth.checkQuiescent(d.w)
	d.check("quiescent")
}

// snapshot returns what the double run compares.
func (d *dstRun) snapshot() dstSnapshot {
	cur := d.current("snapshot")
	var watch []string
	for _, addr := range cur.Watch.Addrs {
		watch = append(watch, "addr "+addr.String())
	}
	for _, in := range cur.Watch.Inputs {
		watch = append(watch, "input "+in.OutPoint.String())
	}
	for _, script := range cur.Watch.List {
		watch = append(watch, "script "+hex.EncodeToString(script))
	}

	d.truth.mu.Lock()
	defer d.truth.mu.Unlock()
	return dstSnapshot{
		events:   append([]dstCallback(nil), d.truth.events...),
		payments: append([]chainhash.Hash(nil), d.truth.payments...),
		errors:   d.truth.errors,
		watch:    watch,
		state: fmt.Sprintf("%v %d %v", cur, cur.CurStamp.Height,
			cur.CurStamp.Hash),
	}
}

// shutdown stops the actor and the subscription manager.
func (d *dstRun) shutdown() {
	d.actor.Stop()
	d.actor.WaitForShutdown()
	d.mgr.Stop()
}

// runRescanDST runs one seed and returns its snapshot.
func runRescanDST(t *testing.T, seed []byte) dstSnapshot {
	t.Helper()
	var snap dstSnapshot
	synctest.Test(t, func(t *testing.T) {
		d := newDSTRun(t, seed)
		defer d.shutdown()

		d.initialSync()
		d.rewindPhase()
		d.liveReorg()
		for i, value := range d.ops {
			d.step(i, value)
		}
		d.heal()
		snap = d.snapshot()
	})
	return snap
}

// dstCorpus is the fixed corpus. It covers every mode bit and every fault
// class alone, a few combinations, and short seeds that turn every class
// on.
var dstCorpus = [][]byte{
	{}, {0}, {0x90, 0x01}, {0x12, 0x00},
	{0, 0, 0},
	{1, 1, 0, 0x00, 0x08, 0x10, 0x01, 0x01},
	{2, 2, 0, 0x12, 0x00},
	{3, 4, 0, 0x00, 0x01},
	{4, 8, 0, 0x00, 0x02},
	{5, 0x10, 0, 0x00, 0x02, 0x00},
	{6, 0x20, 0, 0x00, 0x01},
	{7, 0x40, 0, 0x12, 0x00},
	{8, 0x80, 0, 0x00, 0x01, 0x01},
	{9, 0x00, 1, 0x80, 0x88, 0x01, 0x12},
	{10, 0x01, 1, 0x90, 0x00, 0x01, 0x12, 0x10, 0x01},
	{11, 0xff, 1, 0x10, 0x00, 0x01, 0x01, 0x12, 0x03, 0x07},
	{12, 0x50, 0, 0x22, 0x00, 0x00, 0x01},
	{15, 0xff, 0xff, 0x00, 0x08, 0x10, 0x02, 0x03, 0x01, 0x01, 0x01},
}

// dstRegressionSeeds are seeds that found bugs, in hex. TestRescanDST
// double-runs each one so the fix stays fixed.
var dstRegressionSeeds = []string{
	// The two inputs the scripted harness saved under
	// testdata/fuzz/FuzzRescanDST, re-encoded with their old flags as
	// mode and fault mask. 9cc2bc806a385370 interleaved a retry deadline
	// with a rewind, and b15b16f1ef330f91 combined a failed first
	// subscription, a failed block fetch, a historical reorg and a rewind
	// reorg.
	"020101900001121001010101",
	"00560012",
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

// TestRescanDST runs the corpus and the regression seeds twice each and
// requires identical snapshots.
func TestRescanDST(t *testing.T) {
	for _, seed := range dstSeeds(t) {
		t.Run(fmt.Sprintf("%x", seed), func(t *testing.T) {
			first := runRescanDST(t, seed)
			require.Equal(t, first, runRescanDST(t, seed),
				"same seed produced a different trace")
		})
	}
}

// FuzzRescanDST runs the same runner under coverage-guided fuzzing.
func FuzzRescanDST(f *testing.F) {
	for _, seed := range dstSeeds(f) {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, seed []byte) {
		first := runRescanDST(t, seed)

		// Determinism is verified on the fixed corpus. Re-run a cheap
		// sample of generated inputs too, keyed by content so a saved
		// input replays the same way.
		if sha256.Sum256(seed)[0]%16 == 0 {
			require.Equal(t, first, runRescanDST(t, seed))
		}
	})
}

// dstSwarmSeed derives the next swarm seed of a campaign.
func dstSwarmSeed(rng *rand.Rand) []byte {
	seed := make([]byte, 3+rng.IntN(dstMaxOps+1))
	for i := range seed {
		seed[i] = byte(rng.Uint32())
	}
	return seed
}

// dstSwarmBatch is the number of swarm seeds that run in parallel before
// the campaign looks for a failure.
const dstSwarmBatch = 256

// TestRescanDSTSwarm runs a fixed, reproducible swarm campaign. Each seed
// draws its own mode and fault set. Set the count with -rescan.swarm, the
// generator seed with -rescan.seed and the worker count with -parallel.
//
// Seeds run in parallel batches. A failure reports the lowest failing index
// of its batch, which does not depend on scheduling, so "failed after N
// runs" is reproducible.
func TestRescanDSTSwarm(t *testing.T) {
	rng := rand.New(rand.NewPCG(*rescanSwarmSeed, 0))
	began := time.Now()
	for first := 0; first < *rescanSwarmRuns; first += dstSwarmBatch {
		n := min(dstSwarmBatch, *rescanSwarmRuns-first)
		seeds := make([][]byte, n)
		for j := range seeds {
			seeds[j] = dstSwarmSeed(rng)
		}

		var (
			mu     sync.Mutex
			failed = -1
		)
		t.Run(fmt.Sprintf("batch_%d", first), func(t *testing.T) {
			for j, seed := range seeds {
				i := first + j
				name := fmt.Sprintf("%d_%x", i, seed)
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					defer func() {
						if !t.Failed() {
							return
						}
						mu.Lock()
						if failed < 0 || i < failed {
							failed = i
						}
						mu.Unlock()
					}()
					runRescanDST(t, seed)
				})
			}
		})
		if failed >= 0 {
			t.Fatalf("swarm seed %d failed after %d runs: %x",
				failed, failed+1, seeds[failed-first])
		}
	}
	elapsed := time.Since(began)
	t.Logf("%d runs in %v, %.0f runs/s", *rescanSwarmRuns, elapsed,
		float64(*rescanSwarmRuns)/elapsed.Seconds())
}

// TestRescanDSTReplay replays one seed given with -rescan.replay, for
// example one printed by a failing swarm campaign. Add -rescan.log for
// logs.
func TestRescanDSTReplay(t *testing.T) {
	if *rescanReplay == "" {
		t.Skip("no -rescan.replay seed")
	}
	seed, err := hex.DecodeString(*rescanReplay)
	require.NoError(t, err)
	if *rescanLog {
		backend := btclog.NewBackend(os.Stderr)
		logger := backend.Logger("RSCN")
		logger.SetLevel(btclog.LevelTrace)
		UseLogger(logger)
		ntfnLogger := backend.Logger("NTFN")
		ntfnLogger.SetLevel(btclog.LevelTrace)
		blockntfns.UseLogger(ntfnLogger)
		defer DisableLog()
		defer blockntfns.DisableLog()
	}
	cfg, ops := decodeRescanDST(seed)
	t.Logf("%v, %d ops", cfg, len(ops))
	runRescanDST(t, seed)
}
