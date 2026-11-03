package filtersync

import (
	"time"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/chainstore"
)

// Phase is where the Syncer is in filter header sync. Every phase but Idle
// and Stopped waits for exactly one event.
type Phase uint8

const (
	// PhaseIdle is the phase before Start.
	PhaseIdle Phase = iota

	// PhaseWaitingForHeaders waits for the filter header tip to lag the
	// block header tip by a checkpoint interval, or for the block header
	// sync to be current.
	PhaseWaitingForHeaders

	// PhaseReadingTip waits for the block header tip read.
	PhaseReadingTip

	// PhaseFetchingCheckpoints waits for the peers' checkpoints.
	PhaseFetchingCheckpoints

	// PhaseResolvingConflict waits for the checkpoint resolution.
	PhaseResolvingConflict

	// PhaseFetchingCheckpointed waits for the checkpointed fetch.
	PhaseFetchingCheckpointed

	// PhaseWaitingAtTip waits for the filter header tip hash to differ
	// from the block header tip hash.
	PhaseWaitingAtTip

	// PhaseFetchingUncheckpointed waits for the uncheckpointed fetch.
	PhaseFetchingUncheckpointed

	// PhaseRetrying waits for the retry delay to pass.
	PhaseRetrying

	// PhaseStopped is the phase after Quit. No event leaves it.
	PhaseStopped
)

// String returns the name of the phase.
func (p Phase) String() string {
	switch p {
	case PhaseIdle:
		return "idle"
	case PhaseWaitingForHeaders:
		return "waiting_for_headers"
	case PhaseReadingTip:
		return "reading_tip"
	case PhaseFetchingCheckpoints:
		return "fetching_checkpoints"
	case PhaseResolvingConflict:
		return "resolving_conflict"
	case PhaseFetchingCheckpointed:
		return "fetching_checkpointed"
	case PhaseWaitingAtTip:
		return "waiting_at_tip"
	case PhaseFetchingUncheckpointed:
		return "fetching_uncheckpointed"
	case PhaseRetrying:
		return "retrying"
	case PhaseStopped:
		return "stopped"
	default:
		return "unknown"
	}
}

// resumePoint is where the Syncer continues after a retry delay. Each one is
// the target of a retry in the loop the Syncer replaced.
type resumePoint uint8

const (
	// resumeWaitForHeaders starts a new pass from the wait for headers.
	resumeWaitForHeaders resumePoint = iota

	// resumeCheckpointLoop runs the next step of the checkpoint loop.
	resumeCheckpointLoop

	// resumeWaitAtTip goes back to waiting at the tip.
	resumeWaitAtTip
)

// Config holds what the Syncer needs to know about the chain and the block
// manager's policy. It is fixed for the life of the Syncer.
type Config struct {
	// BlockCheckpoints are the block checkpoints of the active chain, in
	// height order. The Syncer asks for filter checkpoints up to the last
	// one when it is past the block header tip, so the checkpoints do not
	// have to be fetched again each time the block header chain advances.
	BlockCheckpoints []chaincfg.Checkpoint

	// RetryDelay is how long the Syncer waits after a failed round before
	// it tries again.
	RetryDelay time.Duration
}

// Syncer decides what regular filter header sync does next. It owns the
// progress of the sync: the checkpoints each peer served and the height they
// were fetched for, the checkpoints the peers and the store agree on, the
// block header tip the current pass syncs up to, and the phase. It does no
// I/O, reads no clock and starts no goroutine: Handle takes one event and
// returns the effects the run loop performs in return.
//
// A Syncer has no lock. The run loop is its only caller, and it calls Handle
// from one goroutine.
type Syncer struct {
	// cfg is the fixed configuration.
	cfg Config

	// fType is the filter type being synced. Only regular filters are
	// defined.
	fType wire.FilterType

	// lastCp is the latest block checkpoint of the active chain, or the
	// zero checkpoint when the chain has none.
	lastCp chaincfg.Checkpoint

	// phase is the current phase.
	phase Phase

	// resume is where the Syncer continues after the retry delay. It is
	// only meaningful in PhaseRetrying.
	resume resumePoint

	// allCheckpoints maps each peer to the checkpoints it served the last
	// time they were fetched. It persists across passes, so a pass whose
	// block header tip is still covered by the fetched checkpoints does
	// not fetch them again.
	allCheckpoints map[string][]*chainhash.Hash

	// goodCheckpoints are the checkpoints the peers and the store agreed
	// on in the current pass. They are reset at the start of each pass.
	goodCheckpoints []*chainhash.Hash

	// lastHash is the hash of the block header tip the current pass
	// syncs up to.
	lastHash chainhash.Hash

	// lastHeight is the height of the block header tip the current pass
	// syncs up to.
	lastHeight uint32
}

// NewSyncer creates a Syncer in PhaseIdle.
func NewSyncer(cfg Config) *Syncer {
	s := &Syncer{
		cfg:   cfg,
		fType: wire.GCSFilterRegular,
	}

	// Set the variable to the latest block checkpoint if we have any for
	// this chain. Otherwise this block checkpoint will just stay at height
	// 0, which will prompt us to look at the block headers to fetch
	// checkpoints below.
	if len(cfg.BlockCheckpoints) > 0 {
		s.lastCp = cfg.BlockCheckpoints[len(cfg.BlockCheckpoints)-1]
	}

	return s
}

// Phase returns the current phase.
func (s *Syncer) Phase() Phase {
	return s.phase
}

// Handle applies one event and returns the effects the run loop performs in
// return. An empty result means the Syncer has nothing left to do, which is
// only the case after Quit.
//
// Each phase expects one kind of event, and the run loop feeds exactly that
// one, so an event that does not fit the phase cannot happen. If one does, it
// is logged and dropped, and the empty result ends the run loop.
func (s *Syncer) Handle(event Event) []Effect {
	if _, ok := event.(Quit); ok {
		s.phase = PhaseStopped
		return nil
	}

	switch s.phase {
	case PhaseIdle:
		if e, ok := event.(Start); ok {
			return s.waitForHeaders(e.Tips)
		}

	case PhaseWaitingForHeaders:
		if _, ok := event.(TipsChanged); ok {
			s.phase = PhaseReadingTip
			return []Effect{ReadBlockTip{}}
		}

	case PhaseReadingTip:
		if e, ok := event.(BlockTipRead); ok {
			return s.handleBlockTipRead(e)
		}

	case PhaseFetchingCheckpoints:
		if e, ok := event.(CheckpointsFetched); ok {
			return s.handleCheckpointsFetched(e)
		}

	case PhaseResolvingConflict:
		if e, ok := event.(ConflictResolved); ok {
			return s.handleConflictResolved(e)
		}

	case PhaseFetchingCheckpointed:
		if e, ok := event.(CheckpointedFetched); ok {
			return s.handleCheckpointedFetched(e)
		}

	case PhaseWaitingAtTip:
		if _, ok := event.(TipsChanged); ok {
			s.phase = PhaseFetchingUncheckpointed
			return []Effect{
				FetchUncheckpointed{FilterType: s.fType},
			}
		}

	case PhaseFetchingUncheckpointed:
		if e, ok := event.(UncheckpointedFetched); ok {
			return s.handleUncheckpointedFetched(e)
		}

	case PhaseRetrying:
		if e, ok := event.(RetryElapsed); ok {
			return s.handleRetryElapsed(e)
		}

	case PhaseStopped:
	}

	log.Errorf("Filter sync dropped %T in phase %v", event, s.phase)

	return nil
}

// waitForHeaders starts a new pass. We'll wait until the main header sync is
// either finished or the filter headers are lagging at least a checkpoint
// interval behind the block headers, before we actually start to sync the set
// of cfheaders.
func (s *Syncer) waitForHeaders(tips chainstore.Tips) []Effect {
	s.goodCheckpoints = nil
	s.phase = PhaseWaitingForHeaders

	log.Infof("Waiting for more block headers, then will start "+
		"cfheaders sync from height %v...", tips.FilterHeight)

	return []Effect{WaitForTips{Condition: WaitLagOrSynced}}
}

// retryAfter sleeps for the retry delay, then continues at the given point.
func (s *Syncer) retryAfter(resume resumePoint) []Effect {
	s.phase = PhaseRetrying
	s.resume = resume

	return []Effect{RetryAfter{Delay: s.cfg.RetryDelay}}
}

// handleBlockTipRead records the block header tip the pass syncs up to and
// starts the checkpoint loop. A store read can fail transiently with the SQL
// backends. Retry rather than end filter header sync until restart.
func (s *Syncer) handleBlockTipRead(e BlockTipRead) []Effect {
	if e.Err != nil {
		log.Errorf("Unable to read block header tip for cfheaders "+
			"sync, retrying: %v", e.Err)

		return s.retryAfter(resumeWaitForHeaders)
	}

	s.lastHash = e.Hash
	s.lastHeight = e.Height

	log.Infof("Starting cfheaders sync from (block_height=%v, "+
		"block_hash=%v) to (block_height=%v, block_hash=%v)",
		e.Tips.FilterHeight, e.Tips.FilterHash, e.Height, e.Hash)

	log.Infof("Starting cfheaders sync for filter_type=%v", s.fType)

	return s.stepCheckpointLoop()
}

// stepCheckpointLoop runs one step of the loop that finds the agreed
// checkpoints. If we have less than a full checkpoint's worth of blocks, such
// as on simnet, we don't really need to request checkpoints as we'll get 0
// from all peers. We can go on and just request the cfheaders.
func (s *Syncer) stepCheckpointLoop() []Effect {
	if len(s.goodCheckpoints) != 0 ||
		s.lastHeight < wire.CFCheckptInterval {

		return s.fetchCheckpointed()
	}

	// If the height now exceeds the height at which we fetched the
	// checkpoints last time, we must query our peers again.
	if MinCheckpointHeight(s.allCheckpoints) < s.lastHeight {
		// Start by getting the filter checkpoints up to the height of
		// our block header chain. If we have a chain checkpoint that is
		// past this height, we use that instead. We do this so we don't
		// have to fetch all filter checkpoints each time our block
		// header chain advances.
		// TODO(halseth): fetch filter checkpoints up to the best block
		// of the connected peers.
		bestHeight := s.lastHeight
		bestHash := s.lastHash
		if bestHeight < uint32(s.lastCp.Height) {
			bestHeight = uint32(s.lastCp.Height)
			bestHash = *s.lastCp.Hash
		}

		log.Debugf("Getting filter checkpoints up to height=%v, "+
			"hash=%v", bestHeight, bestHash)

		s.phase = PhaseFetchingCheckpoints

		return []Effect{
			PollQuit{},
			FetchCheckpoints{
				FilterType: s.fType,
				StopHash:   bestHash,
				Height:     bestHeight,
			},
		}
	}

	return append([]Effect{PollQuit{}}, s.resolveConflict()...)
}

// handleCheckpointsFetched records the checkpoints the peers served and
// resolves them, or retries the fetch when no peer answered.
func (s *Syncer) handleCheckpointsFetched(e CheckpointsFetched) []Effect {
	s.allCheckpoints = e.ByPeer
	if len(s.allCheckpoints) == 0 {
		log.Warnf("Unable to fetch set of candidate checkpoints, " +
			"trying again...")

		return s.retryAfter(resumeCheckpointLoop)
	}

	return s.resolveConflict()
}

// resolveConflict caps the received checkpoints at the current height, as we
// can only verify checkpoints up to the height we have block headers for,
// and asks the run loop to detect which checkpoint list is correct.
func (s *Syncer) resolveConflict() []Effect {
	checkpoints := make(map[string][]*chainhash.Hash)
	for p, cps := range s.allCheckpoints {
		for i, cp := range cps {
			height := uint32(i+1) * wire.CFCheckptInterval
			if height > s.lastHeight {
				break
			}
			checkpoints[p] = append(checkpoints[p], cp)
		}
	}

	s.phase = PhaseResolvingConflict

	return []Effect{ResolveConflict{
		FilterType:  s.fType,
		Checkpoints: checkpoints,
	}}
}

// handleConflictResolved records the agreed checkpoints and fetches the
// headers up to them. If no list was agreed on, we will cycle again.
func (s *Syncer) handleConflictResolved(e ConflictResolved) []Effect {
	if e.Err != nil {
		log.Warnf("got error attempting to determine correct "+
			"cfheader checkpoints: %v, trying again", e.Err)
	}

	s.goodCheckpoints = e.Good
	if len(s.goodCheckpoints) == 0 {
		return s.retryAfter(resumeCheckpointLoop)
	}

	return s.stepCheckpointLoop()
}

// fetchCheckpointed gets all the headers up to the last known good
// checkpoint.
func (s *Syncer) fetchCheckpointed() []Effect {
	s.phase = PhaseFetchingCheckpointed

	return []Effect{FetchCheckpointed{
		FilterType:  s.fType,
		Checkpoints: s.goodCheckpoints,
	}}
}

// handleCheckpointedFetched decides whether the pass is done. A store failure
// is retried from the top rather than crashing the process.
func (s *Syncer) handleCheckpointedFetched(e CheckpointedFetched) []Effect {
	if e.Err != nil {
		log.Errorf("Unable to fetch checkpointed cfheaders, "+
			"retrying: %v", e.Err)

		return s.retryAfter(resumeWaitForHeaders)
	}

	// Now we check the headers again. If the block headers are not yet
	// current, then we go back to the loop waiting for them to finish.
	if !e.HeadersSynced {
		return s.waitForHeaders(e.Tips)
	}

	// If block headers are current, but the filter header tip is still
	// lagging more than a checkpoint interval behind the block header tip,
	// we also go back to the loop to utilize the faster check pointed
	// fetching.
	if e.Tips.FilterHeight+wire.CFCheckptInterval <= e.Tips.BlockHeight {
		return s.waitForHeaders(e.Tips)
	}

	log.Infof("Fully caught up with cfheaders at height "+
		"%v, waiting at tip for new blocks", s.lastHeight)

	return s.waitAtTip()
}

// waitAtTip waits for a signal that more blocks have been connected. If this
// happens then we'll do another round to fetch the new set of filter headers.
func (s *Syncer) waitAtTip() []Effect {
	s.phase = PhaseWaitingAtTip

	return []Effect{WaitForTips{Condition: WaitTipMismatch}}
}

// handleUncheckpointedFetched goes back to waiting at the tip, after the
// retry delay if the fetch failed.
func (s *Syncer) handleUncheckpointedFetched(
	e UncheckpointedFetched) []Effect {

	if e.Err != nil {
		log.Debugf("couldn't get uncheckpointed headers for "+
			"%v: %v", s.fType, e.Err)

		return s.retryAfter(resumeWaitAtTip)
	}

	return append([]Effect{PollQuit{}}, s.waitAtTip()...)
}

// handleRetryElapsed continues where the retry left off.
func (s *Syncer) handleRetryElapsed(e RetryElapsed) []Effect {
	switch s.resume {
	case resumeWaitForHeaders:
		return s.waitForHeaders(e.Tips)

	case resumeCheckpointLoop:
		return s.stepCheckpointLoop()

	default:
		return append([]Effect{PollQuit{}}, s.waitAtTip()...)
	}
}
