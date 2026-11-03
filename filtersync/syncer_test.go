package filtersync

import (
	"errors"
	"testing"
	"time"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/chainstore"
	"github.com/stretchr/testify/require"
)

const (
	// testRetryDelay is the retry delay the test Syncers use.
	testRetryDelay = 3 * time.Second

	// interval is the checkpoint interval, short for the tables below.
	interval uint32 = wire.CFCheckptInterval
)

var (
	// errTest is the error the failing rounds report.
	errTest = errors.New("test error")

	// cp1, cp2 and cp3 are the checkpoints the test peers serve.
	cp1 = hashOf(1)
	cp2 = hashOf(2)
	cp3 = hashOf(3)

	// tipHash is the block header tip of the test passes.
	tipHash = *hashOf(100)

	// cpHash is the hash of the block checkpoint above the tip.
	cpHash = hashOf(200)
)

// peerLists is a shorter name for the checkpoints each peer served.
type peerLists = map[string][]*chainhash.Hash

// hashes is a shorter name for a checkpoint list.
type hashes = []*chainhash.Hash

// hashOf returns a hash whose first byte is b.
func hashOf(b byte) *chainhash.Hash {
	var h chainhash.Hash
	h[0] = b

	return &h
}

// step is one event fed to the Syncer, with the effects and the phase it
// must produce.
type step struct {
	// name says what the step exercises.
	name string

	// event is the event to feed.
	event Event

	// effects are the expected effects, in order.
	effects []Effect

	// phase is the expected phase after the event.
	phase Phase
}

// runSteps feeds each step to the Syncer and checks its result. It also
// checks the batch invariant the run loop relies on: a non-empty batch ends
// with an effect that always yields an event, which is any effect but
// PollQuit.
func runSteps(t *testing.T, s *Syncer, steps []step) {
	t.Helper()

	for _, st := range steps {
		effects := s.Handle(st.event)
		require.Equal(t, st.effects, effects, st.name)
		require.Equal(t, st.phase, s.Phase(), st.name)

		if len(effects) > 0 {
			require.NotEqual(
				t, PollQuit{}, effects[len(effects)-1], st.name,
			)
		}
	}
}

// newTestSyncer returns a Syncer with the given block checkpoints.
func newTestSyncer(checkpoints ...chaincfg.Checkpoint) *Syncer {
	return NewSyncer(Config{
		BlockCheckpoints: checkpoints,
		RetryDelay:       testRetryDelay,
	})
}

// tipRead returns a successful tip read at the given height.
func tipRead(height uint32) BlockTipRead {
	return BlockTipRead{
		Hash:   tipHash,
		Height: height,
		Tips:   chainstore.Tips{BlockHeight: height},
	}
}

// startedAtTip returns the steps that take a fresh Syncer through Start, the
// lag wait and a tip read at the given height, ending in the first checkpoint
// fetch.
func startedAtTip(height uint32) []step {
	return []step{
		{
			name:    "start waits for the lag",
			event:   Start{Tips: chainstore.Tips{FilterHeight: 7}},
			effects: []Effect{WaitForTips{WaitLagOrSynced}},
			phase:   PhaseWaitingForHeaders,
		},
		{
			name:    "lag reached reads the tip",
			event:   TipsChanged{},
			effects: []Effect{ReadBlockTip{}},
			phase:   PhaseReadingTip,
		},
		{
			name:  "tip read fetches checkpoints up to it",
			event: tipRead(height),
			effects: []Effect{
				PollQuit{},
				FetchCheckpoints{
					FilterType: wire.GCSFilterRegular,
					StopHash:   tipHash,
				},
			},
			phase: PhaseFetchingCheckpoints,
		},
	}
}

// TestSyncerStartAndTipRead covers the lag wait, the tip read retry and the
// two ways the tip read continues: with a checkpoint fetch when the chain
// spans an interval, and straight to the checkpointed fetch when it does not.
func TestSyncerStartAndTipRead(t *testing.T) {
	t.Parallel()

	t.Run("tip read error retries the wait", func(t *testing.T) {
		t.Parallel()

		runSteps(t, newTestSyncer(), []step{
			{
				name:    "start",
				event:   Start{},
				effects: []Effect{WaitForTips{WaitLagOrSynced}},
				phase:   PhaseWaitingForHeaders,
			},
			{
				name:    "lag reached",
				event:   TipsChanged{},
				effects: []Effect{ReadBlockTip{}},
				phase:   PhaseReadingTip,
			},
			{
				name:    "read fails",
				event:   BlockTipRead{Err: errTest},
				effects: []Effect{RetryAfter{testRetryDelay}},
				phase:   PhaseRetrying,
			},
			{
				name:    "retry starts a new pass",
				event:   RetryElapsed{},
				effects: []Effect{WaitForTips{WaitLagOrSynced}},
				phase:   PhaseWaitingForHeaders,
			},
		})
	})

	t.Run("short chain skips the checkpoints", func(t *testing.T) {
		t.Parallel()

		runSteps(t, newTestSyncer(), []step{
			{
				name:    "start",
				event:   Start{},
				effects: []Effect{WaitForTips{WaitLagOrSynced}},
				phase:   PhaseWaitingForHeaders,
			},
			{
				name:    "lag reached",
				event:   TipsChanged{},
				effects: []Effect{ReadBlockTip{}},
				phase:   PhaseReadingTip,
			},
			{
				name:  "tip below one interval",
				event: tipRead(interval - 1),
				effects: []Effect{FetchCheckpointed{
					FilterType: wire.GCSFilterRegular,
				}},
				phase: PhaseFetchingCheckpointed,
			},
		})
	})

	t.Run("block checkpoint above the tip", func(t *testing.T) {
		t.Parallel()

		s := newTestSyncer(
			chaincfg.Checkpoint{Height: 10, Hash: hashOf(10)},
			chaincfg.Checkpoint{
				Height: 5 * int32(interval), Hash: cpHash,
			},
		)
		steps := startedAtTip(interval)
		steps[2].name = "checkpoints are fetched up to the " +
			"block checkpoint"
		steps[2].effects = []Effect{
			PollQuit{},
			FetchCheckpoints{
				FilterType: wire.GCSFilterRegular,
				StopHash:   *cpHash,
			},
		}
		runSteps(t, s, steps)
	})
}

// TestSyncerCheckpointLoop covers the checkpoint fetch retry, the cap at the
// tip, the unresolved conflict retry, and the reuse of fetched checkpoints
// on a later pass.
func TestSyncerCheckpointLoop(t *testing.T) {
	t.Parallel()

	height := 2*interval + 5
	fetch := FetchCheckpoints{
		FilterType: wire.GCSFilterRegular,
		StopHash:   tipHash,
	}

	t.Run("no checkpoints retries the fetch", func(t *testing.T) {
		t.Parallel()

		steps := append(startedAtTip(height), []step{
			{
				name:    "no peer answered",
				event:   CheckpointsFetched{},
				effects: []Effect{RetryAfter{testRetryDelay}},
				phase:   PhaseRetrying,
			},
			{
				name:    "retry fetches again",
				event:   RetryElapsed{},
				effects: []Effect{PollQuit{}, fetch},
				phase:   PhaseFetchingCheckpoints,
			},
		}...)
		runSteps(t, newTestSyncer(), steps)
	})

	t.Run("checkpoints are capped at the tip", func(t *testing.T) {
		t.Parallel()

		steps := append(startedAtTip(height), []step{
			{
				name: "three checkpoints, two below the tip",
				event: CheckpointsFetched{
					ByPeer: peerLists{
						"a": {cp1, cp2, cp3},
						"b": {cp1, cp2},
						"c": {cp1},
					},
				},
				effects: []Effect{ResolveConflict{
					FilterType: wire.GCSFilterRegular,
					Checkpoints: peerLists{
						"a": {cp1, cp2},
						"b": {cp1, cp2},
						"c": {cp1},
					},
				}},
				phase: PhaseResolvingConflict,
			},
			{
				name: "agreement fetches the headers",
				event: ConflictResolved{
					Good: hashes{cp1, cp2},
				},
				effects: []Effect{FetchCheckpointed{
					FilterType:  wire.GCSFilterRegular,
					Checkpoints: hashes{cp1, cp2},
				}},
				phase: PhaseFetchingCheckpointed,
			},
		}...)
		runSteps(t, newTestSyncer(), steps)
	})

	t.Run("unresolved conflict retries", func(t *testing.T) {
		t.Parallel()

		byPeer := peerLists{
			"a": {cp1, cp2, cp3},
			"b": {cp1, cp2, cp3},
		}
		resolve := ResolveConflict{
			FilterType: wire.GCSFilterRegular,
			Checkpoints: peerLists{
				"a": {cp1, cp2},
				"b": {cp1, cp2},
			},
		}
		steps := append(startedAtTip(height), []step{
			{
				name:    "checkpoints cover the tip",
				event:   CheckpointsFetched{ByPeer: byPeer},
				effects: []Effect{resolve},
				phase:   PhaseResolvingConflict,
			},
			{
				name:    "resolution fails",
				event:   ConflictResolved{Err: errTest},
				effects: []Effect{RetryAfter{testRetryDelay}},
				phase:   PhaseRetrying,
			},
			{
				name:    "retry resolves the same checkpoints",
				event:   RetryElapsed{},
				effects: []Effect{PollQuit{}, resolve},
				phase:   PhaseResolvingConflict,
			},
			{
				name:    "no agreement, no error",
				event:   ConflictResolved{},
				effects: []Effect{RetryAfter{testRetryDelay}},
				phase:   PhaseRetrying,
			},
			{
				name:    "retry resolves again",
				event:   RetryElapsed{},
				effects: []Effect{PollQuit{}, resolve},
				phase:   PhaseResolvingConflict,
			},
		}...)
		runSteps(t, newTestSyncer(), steps)
	})

	t.Run("short checkpoints are fetched again", func(t *testing.T) {
		t.Parallel()

		// A peer whose list ends below the tip makes the minimum
		// checkpoint height fall below the tip, so the checkpoints are
		// fetched again after the retry.
		steps := append(startedAtTip(height), []step{
			{
				name: "shortest list ends below the tip",
				event: CheckpointsFetched{
					ByPeer: peerLists{
						"a": {cp1, cp2, cp3},
						"b": {cp1},
					},
				},
				effects: []Effect{ResolveConflict{
					FilterType: wire.GCSFilterRegular,
					Checkpoints: peerLists{
						"a": {cp1, cp2},
						"b": {cp1},
					},
				}},
				phase: PhaseResolvingConflict,
			},
			{
				name:    "no agreement",
				event:   ConflictResolved{},
				effects: []Effect{RetryAfter{testRetryDelay}},
				phase:   PhaseRetrying,
			},
			{
				name:    "retry fetches again",
				event:   RetryElapsed{},
				effects: []Effect{PollQuit{}, fetch},
				phase:   PhaseFetchingCheckpoints,
			},
		}...)
		runSteps(t, newTestSyncer(), steps)
	})

	t.Run("next pass reuses the checkpoints", func(t *testing.T) {
		t.Parallel()

		byPeer := peerLists{
			"a": {cp1, cp2, cp3},
		}
		resolveAll := ResolveConflict{
			FilterType:  wire.GCSFilterRegular,
			Checkpoints: byPeer,
		}
		steps := append(startedAtTip(height), []step{
			{
				name:  "checkpoints fetched",
				event: CheckpointsFetched{ByPeer: byPeer},
				effects: []Effect{ResolveConflict{
					FilterType: wire.GCSFilterRegular,
					Checkpoints: peerLists{
						"a": {cp1, cp2},
					},
				}},
				phase: PhaseResolvingConflict,
			},
			{
				name: "agreement",
				event: ConflictResolved{
					Good: hashes{cp1, cp2},
				},
				effects: []Effect{FetchCheckpointed{
					FilterType:  wire.GCSFilterRegular,
					Checkpoints: hashes{cp1, cp2},
				}},
				phase: PhaseFetchingCheckpointed,
			},
			{
				name:    "headers not synced starts a new pass",
				event:   CheckpointedFetched{},
				effects: []Effect{WaitForTips{WaitLagOrSynced}},
				phase:   PhaseWaitingForHeaders,
			},
			{
				name:    "lag reached",
				event:   TipsChanged{},
				effects: []Effect{ReadBlockTip{}},
				phase:   PhaseReadingTip,
			},
			{
				name:    "tip within the fetched checkpoints",
				event:   tipRead(3 * interval),
				effects: []Effect{PollQuit{}, resolveAll},
				phase:   PhaseResolvingConflict,
			},
			{
				name:    "lag reached",
				event:   TipsChanged{},
				effects: []Effect{ReadBlockTip{}},
				phase:   PhaseReadingTip,
			},
		}...)

		// The last step feeds an event the phase does not expect, so
		// it is dropped with no effects and the phase is unchanged.
		steps[len(steps)-1].name = "unexpected event is dropped"
		steps[len(steps)-1].effects = nil
		steps[len(steps)-1].phase = PhaseResolvingConflict
		runSteps(t, newTestSyncer(), steps)
	})
}

// TestSyncerCheckpointedFetched covers each outcome of the checkpointed
// fetch: the error retry, the loop back while headers are not synced, the
// loop back while the filter tip still lags, and the move to the tip.
func TestSyncerCheckpointedFetched(t *testing.T) {
	t.Parallel()

	fetched := func(s *Syncer) {
		runSteps(t, s, append(startedAtTip(interval), []step{
			{
				name: "checkpoints fetched",
				event: CheckpointsFetched{
					ByPeer: peerLists{
						"a": {cp1},
					},
				},
				effects: []Effect{ResolveConflict{
					FilterType: wire.GCSFilterRegular,
					Checkpoints: peerLists{
						"a": {cp1},
					},
				}},
				phase: PhaseResolvingConflict,
			},
			{
				name: "agreement",
				event: ConflictResolved{
					Good: hashes{cp1},
				},
				effects: []Effect{FetchCheckpointed{
					FilterType:  wire.GCSFilterRegular,
					Checkpoints: hashes{cp1},
				}},
				phase: PhaseFetchingCheckpointed,
			},
		}...))
	}

	t.Run("error retries the wait", func(t *testing.T) {
		t.Parallel()

		s := newTestSyncer()
		fetched(s)
		runSteps(t, s, []step{
			{
				name:    "fetch fails",
				event:   CheckpointedFetched{Err: errTest},
				effects: []Effect{RetryAfter{testRetryDelay}},
				phase:   PhaseRetrying,
			},
			{
				name:    "retry starts a new pass",
				event:   RetryElapsed{},
				effects: []Effect{WaitForTips{WaitLagOrSynced}},
				phase:   PhaseWaitingForHeaders,
			},
		})
	})

	t.Run("not synced loops back", func(t *testing.T) {
		t.Parallel()

		s := newTestSyncer()
		fetched(s)
		runSteps(t, s, []step{{
			name: "headers not synced",
			event: CheckpointedFetched{
				HeadersSynced: false,
				Tips: chainstore.Tips{
					BlockHeight:  interval,
					FilterHeight: interval,
				},
			},
			effects: []Effect{WaitForTips{WaitLagOrSynced}},
			phase:   PhaseWaitingForHeaders,
		}})
		require.Nil(t, s.goodCheckpoints)
	})

	t.Run("synced but lagging loops back", func(t *testing.T) {
		t.Parallel()

		s := newTestSyncer()
		fetched(s)
		runSteps(t, s, []step{{
			name: "filter tip lags by an interval",
			event: CheckpointedFetched{
				HeadersSynced: true,
				Tips: chainstore.Tips{
					BlockHeight:  2 * interval,
					FilterHeight: interval,
				},
			},
			effects: []Effect{WaitForTips{WaitLagOrSynced}},
			phase:   PhaseWaitingForHeaders,
		}})
	})

	t.Run("synced and caught up waits at the tip", func(t *testing.T) {
		t.Parallel()

		s := newTestSyncer()
		fetched(s)
		runSteps(t, s, []step{{
			name: "filter tip lags by less than an interval",
			event: CheckpointedFetched{
				HeadersSynced: true,
				Tips: chainstore.Tips{
					BlockHeight:  2*interval - 1,
					FilterHeight: interval,
				},
			},
			effects: []Effect{WaitForTips{WaitTipMismatch}},
			phase:   PhaseWaitingAtTip,
		}})
	})
}

// TestSyncerAtTip covers the uncheckpointed path: a tip change fetches the
// new headers, a failure retries, and both go back to waiting for the next
// hash mismatch.
func TestSyncerAtTip(t *testing.T) {
	t.Parallel()

	s := newTestSyncer()
	runSteps(t, s, startedAtTip(interval - 1)[:2])
	runSteps(t, s, []step{
		{
			name:  "short chain fetches checkpointed headers",
			event: tipRead(interval - 1),
			effects: []Effect{FetchCheckpointed{
				FilterType: wire.GCSFilterRegular,
			}},
			phase: PhaseFetchingCheckpointed,
		},
		{
			name: "caught up",
			event: CheckpointedFetched{
				HeadersSynced: true,
				Tips:          chainstore.Tips{BlockHeight: 5},
			},
			effects: []Effect{WaitForTips{WaitTipMismatch}},
			phase:   PhaseWaitingAtTip,
		},
		{
			name:  "hash mismatch fetches the new headers",
			event: TipsChanged{},
			effects: []Effect{FetchUncheckpointed{
				FilterType: wire.GCSFilterRegular,
			}},
			phase: PhaseFetchingUncheckpointed,
		},
		{
			name:  "fetched, wait for the next mismatch",
			event: UncheckpointedFetched{},
			effects: []Effect{
				PollQuit{}, WaitForTips{WaitTipMismatch},
			},
			phase: PhaseWaitingAtTip,
		},
		{
			name:  "hash mismatch again",
			event: TipsChanged{},
			effects: []Effect{FetchUncheckpointed{
				FilterType: wire.GCSFilterRegular,
			}},
			phase: PhaseFetchingUncheckpointed,
		},
		{
			name:    "fetch fails",
			event:   UncheckpointedFetched{Err: errTest},
			effects: []Effect{RetryAfter{testRetryDelay}},
			phase:   PhaseRetrying,
		},
		{
			name:  "retry waits for the next mismatch",
			event: RetryElapsed{},
			effects: []Effect{
				PollQuit{}, WaitForTips{WaitTipMismatch},
			},
			phase: PhaseWaitingAtTip,
		},
	})
}

// TestSyncerQuit checks that Quit stops the Syncer in every phase, with no
// effects, and that nothing leaves the stopped phase.
func TestSyncerQuit(t *testing.T) {
	t.Parallel()

	reach := map[Phase]func(s *Syncer){
		PhaseIdle: func(*Syncer) {},
		PhaseWaitingForHeaders: func(s *Syncer) {
			s.Handle(Start{})
		},
		PhaseReadingTip: func(s *Syncer) {
			s.Handle(Start{})
			s.Handle(TipsChanged{})
		},
		PhaseFetchingCheckpoints: func(s *Syncer) {
			s.Handle(Start{})
			s.Handle(TipsChanged{})
			s.Handle(tipRead(interval))
		},
		PhaseResolvingConflict: func(s *Syncer) {
			s.Handle(Start{})
			s.Handle(TipsChanged{})
			s.Handle(tipRead(interval))
			s.Handle(CheckpointsFetched{
				ByPeer: peerLists{
					"a": {cp1},
				},
			})
		},
		PhaseFetchingCheckpointed: func(s *Syncer) {
			s.Handle(Start{})
			s.Handle(TipsChanged{})
			s.Handle(tipRead(interval - 1))
		},
		PhaseWaitingAtTip: func(s *Syncer) {
			s.Handle(Start{})
			s.Handle(TipsChanged{})
			s.Handle(tipRead(interval - 1))
			s.Handle(CheckpointedFetched{HeadersSynced: true})
		},
		PhaseFetchingUncheckpointed: func(s *Syncer) {
			s.Handle(Start{})
			s.Handle(TipsChanged{})
			s.Handle(tipRead(interval - 1))
			s.Handle(CheckpointedFetched{HeadersSynced: true})
			s.Handle(TipsChanged{})
		},
		PhaseRetrying: func(s *Syncer) {
			s.Handle(Start{})
			s.Handle(TipsChanged{})
			s.Handle(BlockTipRead{Err: errTest})
		},
	}

	for phase, setup := range reach {
		s := newTestSyncer()
		setup(s)
		require.Equal(t, phase, s.Phase())

		require.Nil(t, s.Handle(Quit{}), phase.String())
		require.Equal(t, PhaseStopped, s.Phase(), phase.String())

		// Nothing leaves the stopped phase.
		require.Nil(t, s.Handle(TipsChanged{}), phase.String())
		require.Nil(t, s.Handle(RetryElapsed{}), phase.String())
		require.Equal(t, PhaseStopped, s.Phase(), phase.String())
	}
}
