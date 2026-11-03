package headersync

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/btcsuite/btcd/chaincfg/v2"
)

// ActorLifecycle is the minimal lifecycle surface needed for an auxiliary
// actor that the runtime owns. This keeps querysync wiring outside this
// package while still letting headersync control startup and shutdown.
type ActorLifecycle interface {
	Start(context.Context)
	Stop()
}

// RuntimeConfig contains the production actor/runtime dependencies.
type RuntimeConfig struct {
	Config Config

	Dispatch      DispatchController
	DispatchActor ActorLifecycle
}

// Runtime owns the headersync actor graph and production request bookkeeping.
// Blockmanager remains responsible for peer I/O and header storage, while this
// type owns the manager actor, peer actors, controller, driver, and session
// state. The session is private to the package: the driver is its only user
// outside of it.
type Runtime struct {
	cfg Config

	dispatchActor ActorLifecycle
	manager       *ManagerActor
	session       *Session
	controller    *Controller
	driver        *Driver

	peers map[string]*PeerActor

	mu sync.Mutex
}

// NewRuntime creates a headersync actor runtime at the current durable chain
// tip and seeds all known chain checkpoints as trusted anchors.
func NewRuntime(tip ChainPoint, checkpoints []chaincfg.Checkpoint,
	cfg RuntimeConfig) (*Runtime, int) {

	headerCfg := cfg.Config
	if headerCfg.MaxRangeHeaders == 0 {
		headerCfg = ProductionConfig()
	}
	headerCfg = headerCfg.normalize()

	fsm := NewHeaderSyncFSM(tip.Height, tip.Hash, headerCfg)
	fsm.SeedCommittedAnchor()
	seeded := fsm.AddCheckpointAnchors(checkpoints)

	manager := NewManagerActor(fsm)
	if cfg.Dispatch != nil {
		manager.SetDispatchController(cfg.Dispatch)
	}

	session := NewSession()

	runtime := &Runtime{
		cfg:           headerCfg,
		dispatchActor: cfg.DispatchActor,
		manager:       manager,
		session:       session,
		controller: NewController(
			manager, session, headerCfg,
		),
		peers: make(map[string]*PeerActor),
	}
	runtime.controller.peerLookup = runtime.Peer
	runtime.driver = newDriver(runtime, session)
	return runtime, seeded
}

// Start starts the actor graph.
func (r *Runtime) Start(ctx context.Context) {
	if r == nil {
		return
	}
	if r.dispatchActor != nil {
		r.dispatchActor.Start(ctx)
	}
	if r.manager != nil {
		r.manager.Start(ctx)
	}
}

// Stop stops all actors and request timers owned by the runtime.
func (r *Runtime) Stop() {
	if r == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.session != nil {
		r.session.StopAll()
	}
	for peerID, peer := range r.peers {
		peer.Stop()
		delete(r.peers, peerID)
	}
	if r.manager != nil {
		r.manager.Stop()
	}
	if r.dispatchActor != nil {
		r.dispatchActor.Stop()
	}
}

// Driver returns the header-sync driver.
func (r *Runtime) Driver() *Driver {
	if r == nil {
		return nil
	}

	return r.driver
}

// Peer returns a peer actor by ID.
func (r *Runtime) Peer(id string) (*PeerActor, bool) {
	if r == nil {
		return nil, false
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	peer, ok := r.peers[id]
	return peer, ok
}

// RegisterPeer creates and starts a peer actor if it doesn't already exist.
func (r *Runtime) RegisterPeer(ctx context.Context,
	peer PeerSnapshot) (*PeerActor, bool, error) {

	if r == nil || r.manager == nil {
		return nil, false, ErrActorStopped
	}

	r.mu.Lock()
	if existing, ok := r.peers[peer.ID]; ok {
		r.mu.Unlock()
		return existing, false, nil
	}
	r.mu.Unlock()

	actor := NewPeerActor(peer.ID, peer.Rank, r.manager)
	if err := actor.Start(ctx); err != nil {
		return nil, false, err
	}

	if peer.RTT > 0 {
		if err := actor.Send(
			ctx,
			PeerRTTEvent{RTT: peer.RTT},
		); err != nil {
			actor.Stop()
			return nil, false, err
		}
	}
	if peer.State != PeerReady {
		if err := actor.Send(
			ctx,
			peerStateEvent(peer.State),
		); err != nil {
			actor.Stop()
			return nil, false, err
		}
	}

	r.mu.Lock()
	if existing, ok := r.peers[peer.ID]; ok {
		r.mu.Unlock()
		actor.Stop()
		return existing, false, nil
	}
	r.peers[peer.ID] = actor
	r.mu.Unlock()

	return actor, true, nil
}

// UnregisterPeer stops a peer actor and removes it from the manager.
func (r *Runtime) UnregisterPeer(ctx context.Context, id string) error {
	if r == nil || r.manager == nil {
		return nil
	}

	r.mu.Lock()
	peer, ok := r.peers[id]
	if ok {
		delete(r.peers, id)
	}
	r.mu.Unlock()

	if ok {
		peer.Stop()
	}

	return r.manager.RemovePeer(ctx, id)
}

// UpdatePeerRTT updates a peer actor when present, falling back to the manager
// snapshot when only manager state exists.
func (r *Runtime) UpdatePeerRTT(ctx context.Context, id string,
	rtt time.Duration) error {

	if r == nil || r.manager == nil {
		return nil
	}
	if rtt <= 0 {
		return nil
	}

	if peer, ok := r.Peer(id); ok {
		return peer.Send(ctx, PeerRTTEvent{RTT: rtt})
	}

	return r.manager.UpdatePeerRTT(ctx, id, rtt)
}

// UpdatePeerState updates a peer actor when present, falling back to the
// manager snapshot when only manager state exists.
func (r *Runtime) UpdatePeerState(ctx context.Context, id string,
	state PeerState) error {

	if r == nil || r.manager == nil {
		return nil
	}

	if peer, ok := r.Peer(id); ok {
		return peer.Send(ctx, peerStateEvent(state))
	}

	return r.manager.UpdatePeerState(ctx, id, state)
}

func peerStateEvent(state PeerState) PeerMsg {
	switch state {
	case PeerReady:
		return PeerReadyEvent{}

	case PeerBlocked:
		return PeerBlockedEvent{}

	case PeerQuarantined:
		return PeerQuarantinedEvent{}

	case PeerDown:
		return PeerDownEvent{}

	default:
		panic(fmt.Sprintf("unknown header peer state %s", state))
	}
}
