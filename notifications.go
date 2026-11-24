// Copyright (c) 2013-2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// NOTE: THIS API IS UNSTABLE RIGHT NOW.

package neutrino

import (
	"errors"
	"net"
	"time"

	"github.com/btcsuite/btcd/addrmgr"
	"github.com/btcsuite/btcd/connmgr"
	querysyncwm "github.com/lightninglabs/neutrino/querysync/workmanager"
)

type getConnCountMsg struct {
	reply chan int32
}

type subConnPeersReply struct {
	peerChan   chan querysyncwm.Peer
	cancelChan chan struct{}
}
type subConnPeersMsg struct {
	reply chan subConnPeersReply
}

type quarantinePeerMsg struct {
	addr  string
	until time.Time
}

type getPeersMsg struct {
	reply chan []*ServerPeer
}

type getOutboundGroup struct {
	key   string
	reply chan int
}

type getAddedNodesMsg struct {
	reply chan []*ServerPeer
}

type disconnectNodeMsg struct {
	cmp   func(*ServerPeer) bool
	reply chan error
}

type connectNodeMsg struct {
	addr      net.Addr
	permanent bool
	reply     chan error
}

// removeNodeMsg removes a permanent peer, given by its key or, if cmp is
// set, by the peer cmp matches.
type removeNodeMsg struct {
	key   peerKey
	cmp   func(*ServerPeer) bool
	reply chan error
}

// peerConnectedMsg hands a new outbound connection to the peer handler.
type peerConnectedMsg struct {
	req  *connmgr.ConnReq
	conn net.Conn
}

type forAllPeersMsg struct {
	closure func(*ServerPeer)
}

// TODO: General - abstract out more of blockmanager into queries. It'll make
// this way more maintainable and usable.

// handleQuery is the central handler for all queries and commands from other
// goroutines related to peer state.
func (s *ChainService) handleQuery(state *peerState, querymsg interface{}) {
	switch msg := querymsg.(type) {
	case getConnCountMsg:
		nconnected := int32(0)
		state.forAllPeers(func(sp *ServerPeer) {
			if sp.Connected() {
				nconnected++
			}
		})
		msg.reply <- nconnected

	case getPeersMsg:
		peers := make([]*ServerPeer, 0, state.Count())
		now := time.Now()
		state.forAllPeers(func(sp *ServerPeer) {
			if !sp.Connected() {
				return
			}
			if s.peerQuarantined(sp.Addr(), now) {
				return
			}
			peers = append(peers, sp)
		})
		msg.reply <- peers

	// Subscription for connected peers requested.
	case subConnPeersMsg:
		// Create a channel and fill it with the current set of
		// connected peers.
		cancelChan := make(chan struct{})
		peerChan := make(chan querysyncwm.Peer, state.Count())
		now := time.Now()
		state.forAllPeers(func(sp *ServerPeer) {
			if !sp.Connected() {
				return
			}
			if s.peerQuarantined(sp.Addr(), now) {
				return
			}
			peerChan <- sp
		})

		s.peerSubscribers = append(s.peerSubscribers, &peerSubscription{
			peers:  peerChan,
			cancel: cancelChan,
		})
		msg.reply <- subConnPeersReply{
			peerChan:   peerChan,
			cancelChan: cancelChan,
		}

	case quarantinePeerMsg:
		s.trackPeerQuarantine(msg.addr, msg.until)
		state.forAllPeers(func(sp *ServerPeer) {
			if sp.Addr() == msg.addr {
				sp.Disconnect()
			}
		})

	case connectNodeMsg:
		msg.reply <- s.handleConnectNode(state, msg)

	case removeNodeMsg:
		key := msg.key
		if msg.cmp != nil {
			for k, sp := range state.peers {
				if msg.cmp(sp) {
					key = k
					break
				}
			}
		}

		if s.removePermanentPeer(state, key) {
			msg.reply <- nil
		} else {
			msg.reply <- errors.New("peer not found")
		}

	case peerConnectedMsg:
		s.handlePeerConnected(state, msg.req, msg.conn)

	case getOutboundGroup:
		count, ok := state.outboundGroups[msg.key]
		if ok {
			msg.reply <- count
		} else {
			msg.reply <- 0
		}

	// Request a list of the persistent (added) peers.
	case getAddedNodesMsg:
		// Respond with a slice of the relevant peers.
		peers := make([]*ServerPeer, 0, len(state.persistentPeers))
		for _, sp := range state.persistentPeers {
			peers = append(peers, sp)
		}
		msg.reply <- peers

	case disconnectNodeMsg:
		// Check outbound peers.
		found := disconnectPeer(state.outboundPeers, msg.cmp, func(sp *ServerPeer) {
			// Keep group counts ok since we remove from
			// the list now.
			state.outboundGroups[addrmgr.GroupKey(sp.NA())]--
		})
		if found {
			// If there are multiple outbound connections to the same
			// ip:port, continue disconnecting them all until no such
			// peers are found.
			for found {
				found = disconnectPeer(state.outboundPeers, msg.cmp, func(sp *ServerPeer) {
					state.outboundGroups[addrmgr.GroupKey(sp.NA())]--
				})
			}
			msg.reply <- nil
			return
		}

		msg.reply <- errors.New("peer not found")

	case forAllPeersMsg:
		// TODO: Remove this when it's unnecessary due to wider use of
		// the work manager.
		// Run the closure on all peers in the passed state.
		state.forAllPeers(msg.closure)
		// Even though this is a query, there's no reply channel as the
		// forAllPeers method doesn't return anything. An error might be
		// useful in the future.
	}
}

// handleConnectNode starts a connection to a resolved address. A permanent
// one is recorded in the state, which keeps one permanent request per
// address. It is invoked from the peerHandler goroutine.
func (s *ChainService) handleConnectNode(state *peerState,
	msg connectNodeMsg) error {

	// Limit max number of total peers.
	if state.Count() >= MaxPeers {
		return errors.New("max peers reached")
	}

	key := keyOf(msg.addr)
	if _, ok := state.permanent[key]; ok {
		return errors.New("peer exists as a permanent peer")
	}

	if !msg.permanent {
		if _, ok := state.peers[key]; ok {
			return errors.New("peer already connected")
		}

		// TODO: if too many, nuke a non-perm peer.
		go s.connManager.Connect(&connmgr.ConnReq{Addr: msg.addr})

		return nil
	}

	pr := newPermanentReq(msg.addr)
	state.permanent[key] = pr
	go s.connManager.Connect(pr.req)

	return nil
}

// removePermanentPeer removes the permanent peer with the given key, ending
// its request and disconnecting it, wherever it is in its life: connected, in
// its handshake, dialing or waiting to retry. It returns false if there is no
// permanent peer with the key. It is invoked from the peerHandler goroutine.
func (s *ChainService) removePermanentPeer(state *peerState,
	key peerKey) bool {

	pr, ok := state.permanent[key]
	if !ok {
		return false
	}
	delete(state.permanent, key)

	// The flag is set before we read the request's ID, so a request the
	// connection manager hasn't given an ID yet sees the flag when it's
	// dialed, and is ended then.
	pr.removed.Store(true)
	if id := pr.req.ID(); id != 0 {
		s.connManager.Remove(id)
	}

	// The peer stays in state.peers until it's done, so no other peer
	// can take its address while it shuts down.
	if sp, ok := state.peers[key]; ok {
		dropPeer(state, sp)
		sp.Disconnect()
	}

	return true
}

// ConnectedCount returns the number of currently connected peers.
func (s *ChainService) ConnectedCount() int32 {
	replyChan := make(chan int32)

	select {
	case s.query <- getConnCountMsg{reply: replyChan}:
		return <-replyChan
	case <-s.quit:
		return 0
	}
}

// ConnectedPeers is a function that returns a channel where all connected
// peers will be sent. It is assumed that all current peers will be sent
// imemdiately, and new peers as they connect.
func (s *ChainService) ConnectedPeers() (<-chan querysyncwm.Peer, func(),
	error) {

	replyChan := make(chan subConnPeersReply, 1)

	select {
	case s.query <- subConnPeersMsg{
		reply: replyChan,
	}:
	case <-s.quit:
		return nil, nil, ErrShuttingDown
	}

	select {
	case reply := <-replyChan:
		return reply.peerChan, func() {
			close(reply.cancelChan)
		}, nil

	case <-s.quit:
		return nil, nil, ErrShuttingDown
	}
}

// OutboundGroupCount returns the number of peers connected to the given
// outbound group key.
func (s *ChainService) OutboundGroupCount(key string) int {
	replyChan := make(chan int)

	select {
	case s.query <- getOutboundGroup{key: key, reply: replyChan}:
		return <-replyChan
	case <-s.quit:
		return 0
	}
}

// AddedNodeInfo returns an array of btcjson.GetAddedNodeInfoResult structures
// describing the persistent (added) nodes.
func (s *ChainService) AddedNodeInfo() []*ServerPeer {
	replyChan := make(chan []*ServerPeer)

	select {
	case s.query <- getAddedNodesMsg{reply: replyChan}:
		return <-replyChan
	case <-s.quit:
		return nil
	}
}

// Peers returns an array of all connected peers.
func (s *ChainService) Peers() []*ServerPeer {
	replyChan := make(chan []*ServerPeer)

	select {
	case s.query <- getPeersMsg{reply: replyChan}:
		return <-replyChan
	case <-s.quit:
		return nil
	}
}

// eligiblePeers returns the connected peers that are currently eligible to
// receive queries, as a snapshot suitable for the work manager's broadcasts.
func (s *ChainService) eligiblePeers() []querysyncwm.Peer {
	serverPeers := s.Peers()

	peers := make([]querysyncwm.Peer, len(serverPeers))
	for i, sp := range serverPeers {
		peers[i] = sp
	}

	return peers
}

// DisconnectNodeByAddr disconnects a peer by target address. Both outbound and
// inbound nodes will be searched for the target node. An error message will
// be returned if the peer was not found.
func (s *ChainService) DisconnectNodeByAddr(addr string) error {
	replyChan := make(chan error)

	select {
	case s.query <- disconnectNodeMsg{
		cmp:   func(sp *ServerPeer) bool { return sp.Addr() == addr },
		reply: replyChan,
	}:
		return <-replyChan
	case <-s.quit:
		return nil
	}
}

// DisconnectNodeByID disconnects a peer by target node id. Both outbound and
// inbound nodes will be searched for the target node. An error message will be
// returned if the peer was not found.
func (s *ChainService) DisconnectNodeByID(id int32) error {
	replyChan := make(chan error)

	select {
	case s.query <- disconnectNodeMsg{
		cmp:   func(sp *ServerPeer) bool { return sp.ID() == id },
		reply: replyChan,
	}:
		return <-replyChan
	case <-s.quit:
		return nil
	}
}

// RemoveNodeByAddr removes a peer from the list of persistent peers if
// present, whether it's connected or not, and disconnects it. The address is
// resolved the way ConnectNode resolves it, so a peer can be removed by the
// same string it was added with. An error will be returned if the peer was
// not found.
func (s *ChainService) RemoveNodeByAddr(addr string) error {
	netAddr, err := s.addrStringToNetAddr(addr)
	if err != nil {
		return err
	}

	return s.removeNode(removeNodeMsg{key: keyOf(netAddr)})
}

// RemoveNodeByID removes a peer by node ID from the list of persistent peers
// if present. An error will be returned if the peer was not found.
func (s *ChainService) RemoveNodeByID(id int32) error {
	return s.removeNode(removeNodeMsg{
		cmp: func(sp *ServerPeer) bool { return sp.ID() == id },
	})
}

// removeNode hands a removal to the peer handler and returns its reply.
func (s *ChainService) removeNode(msg removeNodeMsg) error {
	msg.reply = make(chan error, 1)

	select {
	case s.query <- msg:
	case <-s.quit:
		return nil
	}

	select {
	case err := <-msg.reply:
		return err
	case <-s.quit:
		return nil
	}
}

// ConnectNode adds `addr' as a new outbound peer. If permanent is true then the
// peer will be persistent and reconnect if the connection is lost.
// It is an error to call this with an already existing peer.
func (s *ChainService) ConnectNode(addr string, permanent bool) error {
	netAddr, err := s.addrStringToNetAddr(addr)
	if err != nil {
		return err
	}

	return s.connectNode(netAddr, permanent)
}

// connectNode hands a connection to a resolved address to the peer handler
// and returns its reply.
func (s *ChainService) connectNode(addr net.Addr, permanent bool) error {
	reply := make(chan error, 1)

	select {
	case s.query <- connectNodeMsg{
		addr:      addr,
		permanent: permanent,
		reply:     reply,
	}:
	case <-s.quit:
		return nil
	}

	select {
	case err := <-reply:
		return err
	case <-s.quit:
		return nil
	}
}

// ForAllPeers runs a closure over all peers (outbound and persistent) to which
// the ChainService is connected. Nothing is returned because the peerState's
// ForAllPeers method doesn't return anything as the closure passed to it
// doesn't return anything.
func (s *ChainService) ForAllPeers(closure func(sp *ServerPeer)) {
	select {
	case s.query <- forAllPeersMsg{closure: closure}:
	case <-s.quit:
	}
}
