package headersync

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/btcsuite/btcd/chaincfg/v2"
)

// RuntimeConfig contains the production runtime dependencies.
type RuntimeConfig struct {
	Config Config

	Dispatch DispatchController

	// Tip configures the validation cache the runtime's driver owns. The
	// block header handler reads and writes the cache only through the
	// driver.
	Tip TipConfig
}

// Runtime owns the headersync manager, its peers and production request
// bookkeeping.
// Blockmanager remains responsible for peer I/O and header storage, while this
// type owns the manager, peers, controller, driver, and session
// state. The session is private to the package: the driver is its only user
// outside of it.
type Runtime struct {
	cfg Config

	manager    *Manager
	session    *Session
	controller *Controller
	driver     *Driver

	peers map[string]*Peer

	mu sync.Mutex
}

// NewRuntime creates a headersync runtime at the current durable chain
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

	manager := NewManager(fsm)
	if cfg.Dispatch != nil {
		manager.SetDispatchController(cfg.Dispatch)
	}

	session := NewSession()

	runtime := &Runtime{
		cfg:     headerCfg,
		manager: manager,
		session: session,
		controller: NewController(
			manager, session, headerCfg,
		),
		peers: make(map[string]*Peer),
	}
	runtime.controller.peerLookup = runtime.Peer
	runtime.driver = newDriver(runtime, session, NewTipState(cfg.Tip))
	return runtime, seeded
}

// Start starts the manager and peers.
func (r *Runtime) Start(ctx context.Context) {
	if r == nil {
		return
	}
	if r.manager != nil {
		r.manager.Start(ctx)
	}
}

// Stop stops the manager, the peers and the request
// timers owned by the runtime.
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
}

// Driver returns the header-sync driver.
func (r *Runtime) Driver() *Driver {
	if r == nil {
		return nil
	}

	return r.driver
}

// Peer returns a peer by ID.
func (r *Runtime) Peer(id string) (*Peer, bool) {
	if r == nil {
		return nil, false
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	peer, ok := r.peers[id]
	return peer, ok
}

// RegisterPeer creates and starts a peer if it doesn't already exist.
func (r *Runtime) RegisterPeer(ctx context.Context,
	peer PeerSnapshot) (*Peer, bool, error) {

	if r == nil || r.manager == nil {
		return nil, false, ErrActorStopped
	}

	r.mu.Lock()
	if existing, ok := r.peers[peer.ID]; ok {
		r.mu.Unlock()
		return existing, false, nil
	}
	r.mu.Unlock()

	actor := NewPeer(peer.ID, peer.Rank, r.manager)
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

// UnregisterPeer stops a peer and removes it from the manager.
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

// UpdatePeerRTT updates a peer when present, falling back to the manager
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

// UpdatePeerState updates a peer when present, falling back to the
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
