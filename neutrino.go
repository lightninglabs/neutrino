// NOTE: THIS API IS UNSTABLE RIGHT NOW.
// TODO: Add functional options to ChainService instantiation.

package neutrino

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/btcsuite/btcd/addrmgr"
	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/connmgr"
	"github.com/btcsuite/btcd/peer"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/btcsuite/btcwallet/walletdb"
	"github.com/lightninglabs/neutrino/banman"
	"github.com/lightninglabs/neutrino/blockntfns"
	"github.com/lightninglabs/neutrino/cache/lru"
	"github.com/lightninglabs/neutrino/chainimport"
	"github.com/lightninglabs/neutrino/chainsync"
	"github.com/lightninglabs/neutrino/chanutils"
	"github.com/lightninglabs/neutrino/filterdb"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/lightninglabs/neutrino/pushtx"
	"github.com/lightninglabs/neutrino/querysync"
	querysyncwm "github.com/lightninglabs/neutrino/querysync/workmanager"
	"github.com/lightninglabs/neutrino/sqldb"
)

// These are exported variables so they can be changed by users.
//
// TODO: Export functional options for these as much as possible so they can be
// changed call-to-call.
var (
	// ConnectionRetryInterval is the base amount of time to wait in
	// between retries when connecting to persistent peers.  It is adjusted
	// by the number of retries such that there is a retry backoff.
	ConnectionRetryInterval = time.Second * 5

	// UserAgentName is the user agent name and is used to help identify
	// ourselves to other bitcoin peers.
	UserAgentName = "neutrino"

	// UserAgentVersion is the user agent version and is used to help
	// identify ourselves to other bitcoin peers.
	UserAgentVersion = "0.17.1"

	// Services describes the services that are supported by the server.
	Services = wire.SFNodeWitness | wire.SFNodeCF

	// RequiredServices describes the services that are required to be
	// supported by outbound peers.
	RequiredServices = wire.SFNodeNetwork | wire.SFNodeWitness | wire.SFNodeCF

	// BanThreshold is the maximum ban score before a peer is banned.
	BanThreshold = uint32(100)

	// BanDuration is the duration of a ban.
	BanDuration = time.Hour * 24

	// unresponsivePeerQuarantine is how long a peer that repeatedly failed
	// to answer queries is excluded from query dispatch. It matches the
	// work manager's local quarantine for such peers.
	unresponsivePeerQuarantine = 10 * time.Minute

	// TargetOutbound is the number of outbound peers to target.
	TargetOutbound = 8

	// MaxPeers is the maximum number of connections the client maintains.
	MaxPeers = 125

	// DisableDNSSeed disables getting initial addresses for Bitcoin nodes
	// from DNS.
	DisableDNSSeed = false

	// DefaultFilterCacheSize is the size (in bytes) of filters neutrino
	// will keep in memory if no size is specified in the neutrino.Config.
	// Since we utilize the cache during batch filter fetching, it is
	// beneficial if it is able to keep a whole batch. The current batch
	// size is 1000, so we default to 30 MB, which can fit about 1450 to
	// 2300 mainnet filters.
	DefaultFilterCacheSize uint64 = 3120 * 10 * 1000

	// DefaultBlockCacheSize is the size (in bytes) of blocks neutrino will
	// keep in memory if no size is specified in the neutrino.Config.
	DefaultBlockCacheSize uint64 = 4096 * 10 * 1000 // 40 MB
)

// isDevNetwork indicates if the chain is a private development network, namely
// simnet or regtest/regnet.
func isDevNetwork(net wire.BitcoinNet) bool {
	return net == chaincfg.SimNetParams.Net ||
		net == chaincfg.RegressionNetParams.Net
}

// updatePeerHeightsMsg is a message sent from the blockmanager to the server
// after a new block has been accepted. The purpose of the message is to update
// the heights of peers that were known to announce the block before we
// connected it to the main chain or recognized it as an orphan. With these
// updates, peer heights will be kept up to date, allowing for fresh data when
// selecting sync peer candidacy.
type updatePeerHeightsMsg struct {
	newHash    *chainhash.Hash
	newHeight  int32
	originPeer *ServerPeer
}

// peerState maintains state of inbound, persistent, outbound peers as well
// as banned peers and outbound groups.
type peerState struct {
	outboundPeers   map[int32]*ServerPeer
	persistentPeers map[int32]*ServerPeer

	// peers holds every peer we have a connection to, from the moment
	// the connection is made until the peer is done, so it includes
	// peers that are still in their handshake. There is at most one
	// peer per address.
	peers map[peerKey]*ServerPeer

	// permanent holds the request of every permanent peer, whether it's
	// connected, dialing, or waiting to retry. It is the one record of
	// which addresses we keep a connection to, so adding, finding and
	// removing a permanent peer all go through it.
	permanent map[peerKey]*permanentReq
}

// newPeerState returns an empty peerState.
func newPeerState() *peerState {
	return &peerState{
		persistentPeers: make(map[int32]*ServerPeer),
		outboundPeers:   make(map[int32]*ServerPeer),
		peers:           make(map[peerKey]*ServerPeer),
		permanent:       make(map[peerKey]*permanentReq),
	}
}

// peerKey identifies a peer by the resolved address we dial, an ip:port or
// an onion host:port. A hostname and its IP, or an address with and without
// the default port, give the same key, so requests and peers are matched by
// it rather than by the string a caller passed in.
type peerKey string

// keyOf returns the key of a resolved address, as returned by
// addrStringToNetAddr.
func keyOf(addr net.Addr) peerKey {
	return peerKey(addr.String())
}

// key returns the peer's key. All of our peers are outbound, and an outbound
// peer's Addr is the string of the address we dialed.
func (sp *ServerPeer) key() peerKey {
	return peerKey(sp.Addr())
}

// errPeerRemoved is returned by the dial function for the request of a
// permanent peer that has been removed.
var errPeerRemoved = errors.New("permanent peer removed")

// errPeerBanned and errPeerQuarantined are returned by the dial function for
// a peer that is banned or quarantined.
var (
	errPeerBanned      = errors.New("peer is banned")
	errPeerQuarantined = errors.New("peer is quarantined")
	errGroupTaken      = errors.New("outbound group already taken")
)

// permanentReq is the connection request of a permanent peer. It is also the
// request's address: the connection manager redials a permanent request on
// its own after every failure, passing its address to the dial function each
// time, which lets the dial function see that a request has been removed and
// end it rather than dial.
type permanentReq struct {
	// Addr is the resolved address of the peer.
	net.Addr

	// req is the request we give the connection manager. Its Addr is
	// this permanentReq.
	req *connmgr.ConnReq

	// removed is set once the peer is removed, before the request is
	// ended.
	removed atomic.Bool
}

// newPermanentReq returns a permanent request for the resolved address.
func newPermanentReq(addr net.Addr) *permanentReq {
	pr := &permanentReq{Addr: addr}
	pr.req = &connmgr.ConnReq{Addr: pr, Permanent: true}

	return pr
}

// Count returns the count of all known peers.
func (ps *peerState) Count() int {
	return len(ps.outboundPeers) + len(ps.persistentPeers)
}

// forAllOutboundPeers is a helper function that runs closure on all outbound
// peers known to peerState.
func (ps *peerState) forAllOutboundPeers(closure func(sp *ServerPeer)) {
	for _, e := range ps.outboundPeers {
		closure(e)
	}
	for _, e := range ps.persistentPeers {
		closure(e)
	}
}

// forAllPeers is a helper function that runs closure on all peers known to
// peerState.
func (ps *peerState) forAllPeers(closure func(sp *ServerPeer)) {
	ps.forAllOutboundPeers(closure)
}

// msgSubscription sends all messages from a peer over a channel, allowing
// pluggable filtering of the messages.
type msgSubscription struct {
	msgChan  chan<- wire.Message
	quitChan <-chan struct{}
}

// ServerPeer extends the peer to maintain state shared by the server and the
// blockmanager.
type ServerPeer struct {
	// The following variables must only be used atomically
	feeFilter int64

	*peer.Peer

	connReq        *connmgr.ConnReq
	server         *ChainService
	persistent     bool
	knownAddresses *lru.Cache[string, *cachedAddr]
	quit           chan struct{}

	// The following map of subcribers is used to subscribe to messages
	// from the peer. This allows broadcast to multiple subscribers at
	// once, allowing for multiple queries to be going to multiple peers at
	// any one time. The mutex is for subscribe/unsubscribe functionality.
	// Each message is sent to every subscriber in turn from the peer's
	// read loop, which blocks until the subscriber takes it, cancels, or
	// the peer or the service goes away. A subscriber that falls behind
	// therefore slows only this peer's reads, rather than leaving a
	// goroutine and a message behind for every message the peer sends.
	recvSubscribers map[msgSubscription]struct{}
	mtxSubscribers  sync.RWMutex
}

// NewServerPeer returns a new ServerPeer instance. The peer needs to be set by
// the caller.
func NewServerPeer(s *ChainService, isPersistent bool) *ServerPeer {
	return &ServerPeer{
		server:          s,
		persistent:      isPersistent,
		knownAddresses:  lru.NewCache[string, *cachedAddr](5000),
		quit:            make(chan struct{}),
		recvSubscribers: make(map[msgSubscription]struct{}),
	}
}

// newestBlock returns the current best block hash and height using the format
// required by the configuration for the peer package.
func (sp *ServerPeer) newestBlock() (*chainhash.Hash, int32, error) {
	bestHeader, bestHeight, err := sp.server.BlockHeaders.ChainTip()
	if err != nil {
		return nil, 0, err
	}
	bestHash := bestHeader.BlockHash()
	return &bestHash, int32(bestHeight), nil
}

// addKnownAddresses adds the given addresses to the set of known addresses to
// the peer to prevent sending duplicate addresses.
func (sp *ServerPeer) addKnownAddresses(addresses []*wire.NetAddressV2) {
	for _, na := range addresses {
		_, err := sp.knownAddresses.Put(
			addrmgr.NetAddressKey(na), &cachedAddr{},
		)
		if err != nil {
			log.Debugf("Could not store known addresses: %v", err)
		}
	}
}

// OnVerAck is invoked when a peer receives a verack bitcoin message and is used
// to kick start communication with them.
func (sp *ServerPeer) OnVerAck(_ *peer.Peer, msg *wire.MsgVerAck) {
	sp.server.AddPeer(sp)
}

// OnVersion is invoked when a peer receives a version bitcoin message
// and is used to negotiate the protocol version details as well as kick start
// the communications.
func (sp *ServerPeer) OnVersion(_ *peer.Peer, msg *wire.MsgVersion) *wire.MsgReject {
	// Add the remote peer time as a sample for creating an offset against
	// the local clock to keep the network time in sync.
	sp.server.timeSource.AddTimeSample(sp.Addr(), msg.Timestamp)

	// Check to see if the peer supports the latest protocol version and
	// service bits required to service us. If not, then we'll disconnect
	// so we can find compatible peers.
	//
	// Missing service bits are a property of this one endpoint, not
	// misbehaviour, so we don't ban: a ban covers every port at the
	// address, and would also cut off a compatible node next to this one
	// (two bitcoinds on one host, say) for the whole ban duration.
	// Recording the services in the address manager is enough to keep
	// discovery from picking this peer again, as it only picks addresses
	// that advertise the services we need.
	peerServices := sp.Services()
	compatible := peerServices&wire.SFNodeWitness == wire.SFNodeWitness &&
		peerServices&wire.SFNodeCF == wire.SFNodeCF

	// Update the address manager with the advertised services for outbound
	// connections in case they have changed. This is not done for inbound
	// connections to help prevent malicious behavior and is skipped when
	// running on the simulation test network since it is only intended to
	// connect to specified peers and actively avoids advertising and
	// connecting to discovered peers.
	if !sp.Inbound() {
		sp.server.addrManager.SetServices(sp.NA(), msg.Services)
	}

	if !compatible {
		// A peer the user configured is retried, so say why it keeps
		// being dropped where they'll see it.
		logf := log.Debugf
		if sp.persistent {
			logf = log.Warnf
		}
		logf("Disconnecting peer %v, services %v lack witness or "+
			"compact filter support", sp.Addr(), peerServices)
		sp.Disconnect()
	}

	return nil
}

// OnInv is invoked when a peer receives an inv bitcoin message and is
// used to examine the inventory being advertised by the remote peer and react
// accordingly.  We pass the message down to blockmanager which will call
// QueueMessage with any appropriate responses.
func (sp *ServerPeer) OnInv(p *peer.Peer, msg *wire.MsgInv) {
	log.Tracef("Got inv with %d items from %s", len(msg.InvList), p.Addr())
	newInv := wire.NewMsgInvSizeHint(uint(len(msg.InvList)))
	for _, invVect := range msg.InvList {
		if invVect.Type == wire.InvTypeTx {
			log.Tracef("Ignoring tx %s in inv from %v -- "+
				"SPV mode", invVect.Hash, sp)
			if sp.ProtocolVersion() >= wire.BIP0037Version {
				log.Infof("Peer %v is announcing "+
					"transactions -- disconnecting", sp)
				sp.Disconnect()
				return
			}
			continue
		}
		err := newInv.AddInvVect(invVect)
		if err != nil {
			log.Errorf("Failed to add inventory vector: %s", err)
			break
		}
	}

	if len(newInv.InvList) > 0 {
		sp.server.blockManager.QueueInv(newInv, sp)
	}
}

// OnHeaders is invoked when a peer receives a headers bitcoin
// message.  The message is passed down to the block manager.
func (sp *ServerPeer) OnHeaders(p *peer.Peer, msg *wire.MsgHeaders) {
	log.Tracef("Got headers with %d items from %s", len(msg.Headers),
		p.Addr())
	sp.server.blockManager.QueueHeaders(msg, sp)
}

// OnFeeFilter is invoked when a peer receives a feefilter bitcoin message and
// is used by remote peers to request that no transactions which have a fee rate
// lower than provided value are inventoried to them.  The peer will be
// disconnected if an invalid fee filter value is provided.
func (sp *ServerPeer) OnFeeFilter(_ *peer.Peer, msg *wire.MsgFeeFilter) {
	// Check that the passed minimum fee is a valid amount.
	if msg.MinFee < 0 || msg.MinFee > btcutil.MaxSatoshi {
		log.Debugf("Peer %v sent an invalid feefilter '%v' -- "+
			"disconnecting", sp, btcutil.Amount(msg.MinFee))
		sp.Disconnect()
		return
	}

	atomic.StoreInt64(&sp.feeFilter, msg.MinFee)
}

// OnReject is invoked when a peer receives a reject bitcoin message and is
// used to notify the server about a rejected transaction.
func (sp *ServerPeer) OnReject(_ *peer.Peer, msg *wire.MsgReject) {
	// TODO(roaseef): log?
}

// OnAddr is invoked when a peer receives an addr bitcoin message and is
// used to notify the server about advertised addresses.
func (sp *ServerPeer) OnAddr(_ *peer.Peer, msg *wire.MsgAddr) {
	// Ignore addresses when running on a private development network.  This
	// helps prevent the network from becoming another public test network
	// since it will not be able to learn about other peers that have not
	// specifically been provided.
	if isDevNetwork(sp.server.chainParams.Net) {
		return
	}

	// Ignore old style addresses which don't include a timestamp.
	if sp.ProtocolVersion() < wire.NetAddressTimeVersion {
		return
	}

	// A message that has no addresses is invalid.
	if len(msg.AddrList) == 0 {
		log.Errorf("Command [%s] from %s does not contain any "+
			"addresses", msg.Command(), sp.Addr())
		sp.Disconnect()
		return
	}

	addrs := make([]*wire.NetAddressV2, 0, len(msg.AddrList))
	for _, na := range msg.AddrList {
		// Don't add more address if we're disconnecting.
		if !sp.Connected() {
			return
		}

		// Skip any that don't advertise our required services.
		if na.Services&RequiredServices != RequiredServices {
			continue
		}

		// Set the timestamp to 5 days ago if it's more than 10 minutes
		// in the future so this address is one of the first to be
		// removed when space is needed.
		now := time.Now()
		if na.Timestamp.After(now.Add(time.Minute * 10)) {
			na.Timestamp = now.Add(-1 * time.Hour * 24 * 5)
		}

		// Convert the wire.NetAddress to wire.NetAddressV2 since that
		// is what is used by the addrmgr.
		currentNa := wire.NetAddressV2FromBytes(
			na.Timestamp, na.Services, na.IP, na.Port,
		)
		addrs = append(addrs, currentNa)
	}

	// Ignore any addr messages if none of them contained our required
	// services.
	if len(addrs) == 0 {
		return
	}

	// Add addresses to the set of known addresses for this peer.
	sp.addKnownAddresses(addrs)

	// Add addresses to server address manager.  The address manager handles
	// the details of things such as preventing duplicate addresses, max
	// addresses, and last seen updates.
	// XXX bitcoind gives a 2 hour time penalty here, do we want to do the
	// same?
	sp.server.addrManager.AddAddresses(addrs, sp.NA())
}

// OnAddrV2 is called when a peer receives an AddrV2 message from its peer.
func (sp *ServerPeer) OnAddrV2(_ *peer.Peer, msg *wire.MsgAddrV2) {
	// Ignore addresses when running on a private development network for
	// the same reason that OnAddr does.
	if isDevNetwork(sp.server.chainParams.Net) {
		return
	}

	// An empty AddrV2 message is invalid.
	if len(msg.AddrList) == 0 {
		log.Errorf("Command [%s] from %s does not contain any "+
			"addresses", msg.Command(), sp.Addr())
		sp.Disconnect()
		return
	}

	addrs := make([]*wire.NetAddressV2, 0, len(msg.AddrList))
	for _, na := range msg.AddrList {
		// Don't add more addresses if we're disconnecting.
		if !sp.Connected() {
			return
		}

		// Skip any that don't advertise our required services.
		if na.Services&RequiredServices != RequiredServices {
			continue
		}

		// Set the timestamp to 5 days ago if it's more than 10 minutes
		// in the future so this address is one of the first to be
		// removed when space is needed.
		now := time.Now()
		if na.Timestamp.After(now.Add(time.Minute * 10)) {
			na.Timestamp = now.Add(-1 * time.Hour * 24 * 5)
		}
		addrs = append(addrs, na)
	}

	// Ignore addrv2 message if no addresses contained our required
	// services.
	if len(addrs) == 0 {
		return
	}

	// Add the addresses to the set of known addresses for this peer.
	sp.addKnownAddresses(addrs)

	// Add addresses to the address manager.
	sp.server.addrManager.AddAddresses(addrs, sp.NA())
}

// OnRead is invoked when a peer receives a message and it is used to update
// the bytes received by the server.
func (sp *ServerPeer) OnRead(_ *peer.Peer, bytesRead int, msg wire.Message,
	err error) {

	sp.server.AddBytesReceived(uint64(bytesRead))

	// Send the message to each subscriber in turn, in the order the peer
	// sent them. We copy the set first so the lock isn't held while we
	// wait on a subscriber, and leave removing cancelled subscriptions to
	// their cancel closure, which holds the write lock.
	sp.mtxSubscribers.RLock()
	subscribers := make([]msgSubscription, 0, len(sp.recvSubscribers))
	for subscription := range sp.recvSubscribers {
		subscribers = append(subscribers, subscription)
	}
	sp.mtxSubscribers.RUnlock()

	for _, subscription := range subscribers {
		select {
		case subscription.msgChan <- msg:
		case <-subscription.quitChan:
		case <-sp.quit:
			return
		case <-sp.server.quit:
			return
		}
	}
}

// A compile-time check to ensure that ServerPeer implements the
// querysyncwm.Peer interface.
var _ querysyncwm.Peer = (*ServerPeer)(nil)

// SubscribeRecvMsg adds a OnRead subscription to the peer. All bitcoin
// messages received from this peer will be sent on the returned channel. A
// closure is also returned, that should be called to cancel the subscription.
//
// NOTE: Part of the querysyncwm.Peer interface.
func (sp *ServerPeer) SubscribeRecvMsg() (<-chan wire.Message, func()) {
	// The channel is unbuffered: OnRead waits for the subscriber to take
	// each message, which pushes back on the peer instead of queueing.
	msgChan := make(chan wire.Message)
	quitChan := make(chan struct{})

	sub := msgSubscription{
		msgChan:  msgChan,
		quitChan: quitChan,
	}

	sp.mtxSubscribers.Lock()
	defer sp.mtxSubscribers.Unlock()
	sp.recvSubscribers[sub] = struct{}{}

	return msgChan, func() {
		close(quitChan)

		sp.mtxSubscribers.Lock()
		delete(sp.recvSubscribers, sub)
		sp.mtxSubscribers.Unlock()
	}
}

// OnDisconnect returns a channel that will be closed when this peer is
// disconnected.
//
// NOTE: Part of the querysyncwm.Peer interface.
func (sp *ServerPeer) OnDisconnect() <-chan struct{} {
	return sp.quit
}

// OnWrite is invoked when a peer sends a message and it is used to update
// the bytes sent by the server.
func (sp *ServerPeer) OnWrite(_ *peer.Peer, bytesWritten int, msg wire.Message, err error) {
	sp.server.AddBytesSent(uint64(bytesWritten))
}

// Config is a struct detailing the configuration of the chain service.
type Config struct {
	// DataDir is the directory that neutrino will store all header
	// information within.
	DataDir string

	// Database is an *open* database instance that we'll use to storm
	// indexes of the chain. At least one of Database and SQLConfig must be
	// set. When only Database is set, it backs all persistent stores.
	// When SQLConfig is set too, the SQL backend is used for storage and
	// Database is treated as the legacy walletdb to import filters and
	// bans from (the flat header files are read from DataDir). The caller
	// keeps ownership of Database and may close it once NewChainService
	// returns.
	Database walletdb.DB

	// SQLConfig, when non-nil, switches all persistent stores
	// (block headers, filter headers, filter DB, ban store) to a
	// SQLite or PostgreSQL backend. If Database is also set, it is the
	// source of the legacy import, see Database.
	SQLConfig *sqldb.Config

	// LegacyImport optionally overrides where the legacy walletdb +
	// flat-file state is imported from when the SQL backend is first
	// started. When nil, the flat files are detected in DataDir
	// (block_headers.bin, reg_filter_headers.bin) and the walletdb is
	// Database, or DataDir/neutrino.db if Database is nil, and everything
	// found is imported automatically; a fresh install has none. The
	// import runs
	// exactly once, recorded in the neutrino_migrations table at version
	// 2, and is disabled by SQLConfig.SkipLegacyMigration. Ignored when
	// SQLConfig is nil.
	LegacyImport *sqldb.LegacyDataSource

	// ChainParams is the chain that we're running on. Retarget parameters
	// must be valid whole-second values, as checked by
	// chainsync.NewRetarget.
	ChainParams chaincfg.Params

	// ConnectPeers is a slice of hosts that should be connected to on
	// startup, and be established as persistent peers.
	//
	// NOTE: If specified, we'll *only* connect to this set of peers and
	// won't attempt to automatically seek outbound peers.
	ConnectPeers []string

	// AddPeers is a slice of hosts that should be connected to on startup,
	// and be maintained as persistent peers.
	AddPeers []string

	// Dialer is an optional function closure that will be used to
	// establish outbound TCP connections. If specified, then the
	// connection manager will use this in place of net.Dial for all
	// outbound connection attempts.
	Dialer func(addr net.Addr) (net.Conn, error)

	// NameResolver is an optional function closure that will be used to
	// lookup the IP of any host. If specified, then the address manager,
	// along with regular outbound connection attempts will use this
	// instead.
	NameResolver func(host string) ([]net.IP, error)

	// FilterCacheSize indicates the size (in bytes) of filters the cache will
	// hold in memory at most.
	FilterCacheSize uint64

	// BlockCache is an LRU block cache. If none is provided then the a new
	// one will be instantiated.
	BlockCache *lru.Cache[wire.InvVect, *CacheableBlock]

	// BlockCacheSize indicates the size (in bytes) of blocks the block
	// cache will hold in memory at most. If a BlockCache is provided then
	// BlockCacheSize is ignored.
	BlockCacheSize uint64

	// persistToDisk indicates whether the filter should also be written
	// to disk in addition to the memory cache. For "normal" wallets, they'll
	// almost never need to re-match a filter once it's been fetched unless
	// they're doing something like a key import.
	PersistToDisk bool

	// HeadersImport contains configuration options for importing block and
	// filter headers from external sources.
	HeadersImport *HeadersImportConfig

	// AssertFilterHeader is an optional field that allows the creator of
	// the ChainService to ensure that if any chain data exists, it's
	// compliant with the expected filter header state. If neutrino starts
	// up and this filter header state has diverged, then it'll remove the
	// current on disk filter headers to sync them anew.
	AssertFilterHeader *headerfs.FilterHeader

	// BroadcastTimeout is the amount of time we'll wait before giving up on
	// a transaction broadcast attempt. Broadcasting transactions consists
	// of three steps:
	//
	// 1. Neutrino sends an inv for the transaction.
	// 2. The recipient node determines if the inv is known, and if it's
	//    not, replies with a getdata message.
	// 3. Neutrino sends the raw transaction.
	BroadcastTimeout time.Duration

	// QuerySchedulerTrace records model-owned scheduler actor events from
	// the production query sync path. The trace can be replayed against the
	// querysync model to validate dispatch, retry, cooldown, and work
	// stealing invariants.
	QuerySchedulerTrace *querysync.TraceRecorder

	// QuerySchedulerEvents receives structured production query scheduler
	// telemetry for live sync diagnostics and validation summaries.
	QuerySchedulerEvents querysync.SchedulerEventSink
}

// HeadersImportConfig contains configuration options for importing headers
// from external sources.
type HeadersImportConfig struct {
	// BlockHeadersSource specifies where to obtain block headers from.
	// This could be a file path, URL, or other source identifier.
	BlockHeadersSource string

	// FilterHeadersSource specifies the filter-header source. The import
	// format doesn't contain compact filter hashes, so entries between
	// hardcoded checkpoints trust the configured source. Compact filters
	// fetched later are checked against these imported commitments.
	FilterHeadersSource string

	// WriteBatchSizePerRegion defines the number of headers to write in a
	// single batch per processing region during import. The import process
	// divides header ranges into distinct regions
	// (overlap, divergence, and new headers), each with different
	// processing requirements. Each region is processed in batches of this
	// size to optimize database performance while maintaining data
	// integrity boundaries. Larger values improve speed but increase memory
	// usage.
	//
	// Default value: 16,384 (2^14) entries.
	// This results in 16,384 block headers and 16,384 filter headers per
	// batch. The total upper bound size per batch is:
	//   - Block headers: (16,384 * 80 bytes) / (2^10) = 1.28 MB
	//   - Filter headers: (16,384 * 32 bytes) / (2^10) = 0.5 MB
	// Peak memory usage observed during benchmarking was ≈ 66 MB.
	// Actual memory usage can vary depending on factors such as database
	// state, Go garbage collector semantics and activity.
	WriteBatchSizePerRegion int

	// ValidationFlags specifies the behavior flags used during header
	// validation. It defaults to BFNone.
	ValidationFlags blockchain.BehaviorFlags
}

// peerSubscription holds a peer subscription which we'll notify about any
// connected peers.
type peerSubscription struct {
	peers  chan<- querysyncwm.Peer
	cancel <-chan struct{}
}

// ChainService is instantiated with functional options.
type ChainService struct { // nolint:maligned
	// The following variables must only be used atomically.
	// Putting the uint64s first makes them 64-bit aligned for 32-bit systems.
	bytesReceived uint64 // Total bytes received from all peers since start.
	bytesSent     uint64 // Total bytes sent by all peers since start.
	started       int32
	shutdown      int32

	FilterDB         filterdb.FilterDatabase
	BlockHeaders     headerfs.BlockHeaderStore
	RegFilterHeaders headerfs.FilterHeaderStore
	persistToDisk    bool

	headersImport *HeadersImportConfig

	FilterCache *lru.Cache[FilterCacheKey, *CacheableFilter]
	BlockCache  *lru.Cache[wire.InvVect, *CacheableBlock]

	chainParams          chaincfg.Params
	addrManager          *addrmgr.AddrManager
	connManager          *connmgr.ConnManager
	blockManager         *blockManager
	blockSubscriptionMgr *blockntfns.SubscriptionManager
	newPeers             chan *ServerPeer
	donePeers            chan *ServerPeer
	query                chan interface{}
	firstPeerConnect     chan struct{}
	peerHeightsUpdate    chan updatePeerHeightsMsg
	wg                   sync.WaitGroup
	quit                 chan struct{}
	timeSource           blockchain.MedianTimeSource
	services             wire.ServiceFlag
	utxoScanner          *UtxoScanner
	broadcaster          *pushtx.Broadcaster
	banStore             banman.Store
	sqlBackend           *sqldb.Backend
	workManager          querysyncwm.WorkManager
	filterBatchWriter    *chanutils.BatchWriter[*filterdb.FilterData]

	// quarantine holds the peers excluded from queries for a while. The
	// peer handler writes it, and the dial function reads it as well.
	quarantine peerQuarantine

	// peerSubscribers is a slice of active peer subscriptions, that we
	// will notify each time a new peer is connected.
	peerSubscribers []*peerSubscription

	// TODO: Add a map for more granular exclusion?
	mtxCFilter     sync.Mutex
	filterQuerySem chan struct{}

	userAgentName    string
	userAgentVersion string

	nameResolver func(string) ([]net.IP, error)
	dialer       func(net.Addr) (net.Conn, error)

	// conns holds every connection we have dialed until it's closed, so
	// that Stop can close those the connection manager drops.
	conns connSet

	// connectOnly is set when the service only connects to the peers in
	// Config.ConnectPeers, with no address discovery.
	connectOnly bool

	broadcastTimeout time.Duration
}

// NewChainService returns a new chain service configured to connect to the
// bitcoin network type specified by chainParams.  Use start to begin syncing
// with peers.
func NewChainService(cfg Config) (*ChainService, error) {
	if _, err := chainsync.NewRetarget(cfg.ChainParams); err != nil {
		return nil, fmt.Errorf("unable to create chain service: %w",
			err)
	}

	// Validate that the caller provided at least one of the two supported
	// persistent backends. This is checked before any other side effect
	// so misconfigurations fail fast.
	if cfg.Database == nil && cfg.SQLConfig == nil {
		return nil, errors.New("neutrino: one of cfg.Database " +
			"or cfg.SQLConfig must be set")
	}

	// Use the default broadcast timeout if one isn't provided.
	if cfg.BroadcastTimeout == 0 {
		cfg.BroadcastTimeout = pushtx.DefaultBroadcastTimeout
	}

	// First, we'll sort out the methods that we'll use to established
	// outbound TCP connections, as well as perform any DNS queries.
	//
	// If the dialler was specified, then we'll use that in place of the
	// default net.Dial function.
	var (
		nameResolver func(string) ([]net.IP, error)
		dialer       func(net.Addr) (net.Conn, error)
	)
	if cfg.Dialer != nil {
		dialer = cfg.Dialer
	} else {
		dialer = func(addr net.Addr) (net.Conn, error) {
			return net.Dial(addr.Network(), addr.String())
		}
	}

	// Similarly, if the user specified as function to use for name
	// resolution, then we'll use that everywhere as well.
	if cfg.NameResolver != nil {
		nameResolver = cfg.NameResolver
	} else {
		nameResolver = net.LookupIP
	}

	// When creating the addr manager, we'll check to see if the user has
	// provided their own resolution function. If so, then we'll use that
	// instead as this may be proxying requests over an anonymizing
	// network.
	amgr := addrmgr.New(cfg.DataDir, nameResolver)

	s := ChainService{
		chainParams:       cfg.ChainParams,
		addrManager:       amgr,
		newPeers:          make(chan *ServerPeer, MaxPeers),
		donePeers:         make(chan *ServerPeer, MaxPeers),
		query:             make(chan interface{}),
		quit:              make(chan struct{}),
		firstPeerConnect:  make(chan struct{}),
		peerHeightsUpdate: make(chan updatePeerHeightsMsg),
		timeSource:        blockchain.NewMedianTime(),
		services:          Services,
		userAgentName:     UserAgentName,
		userAgentVersion:  UserAgentVersion,
		nameResolver:      nameResolver,
		dialer:            dialer,
		connectOnly:       len(cfg.ConnectPeers) > 0,
		persistToDisk:     cfg.PersistToDisk,
		broadcastTimeout:  cfg.BroadcastTimeout,
		headersImport:     cfg.HeadersImport,
	}
	s.workManager = querysyncwm.NewWorkManager(&querysyncwm.Config{
		ConnectedPeers:     s.ConnectedPeers,
		Peers:              s.eligiblePeers,
		OnPeerUnresponsive: s.onPeerUnresponsive,
		Ranking:            querysyncwm.NewPeerRanking(),
		SchedulerTrace:     cfg.QuerySchedulerTrace,
		SchedulerEvents:    cfg.QuerySchedulerEvents,
	})

	var err error

	// If the caller opted into the SQL backend, open it now (running the
	// schema migrations and any registered legacy import) and inject the
	// resulting per-domain TransactionExecutors into each store below.
	if cfg.SQLConfig != nil {
		// A walletdb handed in next to the SQL config is the legacy
		// state to import from.
		legacy := cfg.LegacyImport
		if cfg.Database != nil && (legacy == nil || legacy.DB == nil) {
			var src sqldb.LegacyDataSource
			if legacy != nil {
				src = *legacy
			}
			src.DB = cfg.Database
			legacy = &src
		}

		s.sqlBackend, err = sqldb.NewBackendWithLegacy(
			cfg.DataDir, cfg.SQLConfig, &cfg.ChainParams, legacy,
		)
		if err != nil {
			return nil, fmt.Errorf("unable to open SQL "+
				"backend: %w", err)
		}
	}

	if s.sqlBackend != nil {
		s.FilterDB, err = filterdb.NewSQLFilterStore(
			context.Background(), s.sqlBackend.FilterTxer,
			cfg.ChainParams,
		)
	} else {
		s.FilterDB, err = filterdb.New(cfg.Database, cfg.ChainParams)
	}
	if err != nil {
		return nil, err
	}

	if s.persistToDisk {
		cfg := &chanutils.BatchWriterConfig[*filterdb.FilterData]{
			QueueBufferSize:        chanutils.DefaultQueueSize,
			MaxBatch:               10,
			DBWritesTickerDuration: time.Millisecond * 500,
			PutItems:               s.FilterDB.PutFilters,
		}

		batchWriter := chanutils.NewBatchWriter[*filterdb.FilterData](
			cfg,
		)

		s.filterBatchWriter = batchWriter
	}

	filterCacheSize := DefaultFilterCacheSize
	if cfg.FilterCacheSize != 0 {
		filterCacheSize = cfg.FilterCacheSize
	}
	s.FilterCache = lru.NewCache[FilterCacheKey, *CacheableFilter](
		filterCacheSize,
	)

	if cfg.BlockCache != nil {
		s.BlockCache = cfg.BlockCache
	} else {
		blockCacheSize := DefaultBlockCacheSize
		if cfg.BlockCacheSize != 0 {
			blockCacheSize = cfg.BlockCacheSize
		}
		s.BlockCache = lru.NewCache[wire.InvVect, *CacheableBlock](
			blockCacheSize,
		)
	}

	if s.sqlBackend != nil {
		s.BlockHeaders, err = headerfs.NewSQLBlockHeaderStore(
			context.Background(), s.sqlBackend.HeaderTxer,
			&cfg.ChainParams,
		)
	} else {
		s.BlockHeaders, err = headerfs.NewBlockHeaderStore(
			cfg.DataDir, cfg.Database, &cfg.ChainParams,
		)
	}
	if err != nil {
		return nil, err
	}

	if s.sqlBackend != nil {
		s.RegFilterHeaders, err = headerfs.NewSQLFilterHeaderStore(
			context.Background(), s.sqlBackend.HeaderTxer,
			headerfs.RegularFilter, &cfg.ChainParams,
			cfg.AssertFilterHeader,
		)
	} else {
		s.RegFilterHeaders, err = headerfs.NewFilterHeaderStore(
			cfg.DataDir, cfg.Database, headerfs.RegularFilter,
			&cfg.ChainParams, cfg.AssertFilterHeader,
		)
	}
	if err != nil {
		return nil, err
	}

	err = rollBackZeroedFilterTip(s.BlockHeaders, s.RegFilterHeaders)
	if err != nil {
		return nil, fmt.Errorf("unable to repair filter header "+
			"tip: %w", err)
	}

	bm, err := newBlockManager(&blockManagerCfg{
		ChainParams:      s.chainParams,
		BlockHeaders:     s.BlockHeaders,
		RegFilterHeaders: s.RegFilterHeaders,
		TimeSource:       s.timeSource,
		QueryDispatcher:  s.workManager,
		BanPeer:          s.BanPeer,
		DisconnectPeer:   s.DisconnectPeer,
		QuarantinePeer:   s.QuarantinePeer,
		GetBlock:         s.GetBlock,
		firstPeerSignal:  s.firstPeerConnect,
		Broadcaster:      s.workManager,
	})
	if err != nil {
		return nil, err
	}
	s.blockManager = bm
	s.blockSubscriptionMgr = blockntfns.NewSubscriptionManager(s.blockManager)

	// Only setup a function to return new addresses to connect to when not
	// running in connect-only mode.  Private development networks are always in
	// connect-only mode since it is only intended to connect to specified peers
	// and actively avoid advertising and connecting to discovered peers in
	// order to prevent it from becoming a public test network.
	var newAddressFunc func() (net.Addr, error)
	if !isDevNetwork(s.chainParams.Net) {
		newAddressFunc = func() (net.Addr, error) {
			// Gather our set of currently connected peers to avoid
			// connecting to them again.
			connectedPeers := make(map[string]struct{})
			for _, peer := range s.Peers() {
				peerAddr := addrmgr.NetAddressKey(peer.NA())
				connectedPeers[peerAddr] = struct{}{}
			}

			for tries := 0; tries < 100; tries++ {
				select {
				case <-s.quit:
					return nil, ErrShuttingDown
				default:
				}

				addr := s.addrManager.GetAddress()
				if addr == nil {
					break
				}

				// Ignore peers that we've already banned.
				addrString := addrmgr.NetAddressKey(addr.NetAddress())
				if s.IsBanned(addrString) {
					log.Debugf("Ignoring banned peer: %v", addrString)
					continue
				}

				// Skip any addresses that correspond to our set
				// of currently connected peers.
				if _, ok := connectedPeers[addrString]; ok {
					continue
				}

				// The peer behind this address should support
				// all of our required services.
				if addr.Services()&RequiredServices != RequiredServices {
					continue
				}

				// Address will not be invalid, local or unroutable
				// because addrmanager rejects those on addition.
				// Just check that we don't already have an address
				// in the same group so that we are not connecting
				// to the same network segment at the expense of
				// others.
				key := addrmgr.GroupKey(addr.NetAddress())
				if s.OutboundGroupCount(key) != 0 {
					continue
				}

				// only allow recent nodes (10mins) after we failed 30
				// times
				if tries < 30 && time.Since(addr.LastAttempt()) < 10*time.Minute {
					continue
				}

				// allow nondefault ports after 50 failed tries.
				if tries < 50 && fmt.Sprintf("%d", addr.NetAddress().Port) !=
					s.chainParams.DefaultPort {

					continue
				}

				// Mark an attempt for the valid address.
				s.addrManager.Attempt(addr.NetAddress())
				netAddr, err := s.addrStringToNetAddr(
					addrString,
				)
				if err != nil {
					return nil, err
				}

				return &discoveredAddr{Addr: netAddr}, nil
			}

			return nil, errors.New("no valid connect address")
		}
	}

	cmgrCfg := &connmgr.Config{
		RetryDuration:  ConnectionRetryInterval,
		TargetOutbound: uint32(TargetOutbound),
		OnConnection:   s.outboundPeerConnected,
		Dial:           s.dialPeer(dialUntilQuit(dialer, s.quit)),
	}
	if len(cfg.ConnectPeers) == 0 {
		cmgrCfg.GetNewAddress = newAddressFunc
	}

	// Create a connection manager.
	if MaxPeers < TargetOutbound {
		TargetOutbound = MaxPeers
	}
	cmgr, err := connmgr.New(cmgrCfg)
	if err != nil {
		return nil, err
	}
	s.connManager = cmgr

	s.utxoScanner = NewUtxoScanner(&UtxoScannerConfig{
		BestSnapshot: s.BestBlock,
		GetBlockHash: s.GetBlockHash,
		GetBlock:     s.GetBlock,
		BlockFilterMatches: func(ro *rescanOptions,
			blockHash *chainhash.Hash) (bool, error) {

			matches, _, err := blockFilterMatches(
				&RescanChainSource{&s}, ro, blockHash,
			)

			return matches, err
		},
	})

	s.broadcaster = pushtx.NewBroadcaster(&pushtx.Config{
		Broadcast: func(tx *wire.MsgTx) error {
			return s.sendTransaction(tx)
		},
		SubscribeBlocks: func() (*blockntfns.Subscription, error) {
			return s.blockSubscriptionMgr.NewSubscription(0)
		},
		RebroadcastInterval: pushtx.DefaultRebroadcastInterval,
	})

	if s.sqlBackend != nil {
		s.banStore, err = banman.NewSQLStore(s.sqlBackend.BanTxer)
	} else {
		s.banStore, err = banman.NewStore(cfg.Database)
	}
	if err != nil {
		return nil, fmt.Errorf("unable to initialize ban store: %v", err)
	}

	// Start up persistent peers.
	permanentPeers := cfg.ConnectPeers
	if len(permanentPeers) == 0 {
		permanentPeers = cfg.AddPeers
	}

	for _, addr := range permanentPeers {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()

			// Since netwok access might not be established yet, we
			// loop until we are able to look up the permanent
			// peer.
			var tcpAddr net.Addr
			for {
				// The lookup goes through the caller's
				// resolver, which can be slow (Tor, say),
				// and this goroutine is one Stop waits on.
				var err error
				tcpAddr, err = untilQuit(
					s.quit, func() (net.Addr, error) {
						return s.addrStringToNetAddr(
							addr,
						)
					}, nil,
				)
				if errors.Is(err, ErrShuttingDown) {
					return
				}
				if err != nil {
					log.Warnf("Unable to look up "+
						"configured peer %v, "+
						"retrying in %v: %v", addr,
						ConnectionRetryInterval, err)

					select {
					// Try again in 5 seconds.
					case <-time.After(ConnectionRetryInterval):
					case <-s.quit:
						return
					}
					continue
				}

				break
			}

			// The only error is a peer given twice, which is
			// already being connected to.
			err := s.connectNode(tcpAddr, true)
			if err != nil {
				log.Warnf("Configured peer %v (%v) not added "+
					"again: %v", addr, tcpAddr, err)
			}
		}()
	}

	return &s, nil
}

// BestBlock retrieves the most recent block's height and hash where we
// have both the header and filter header ready.
func (s *ChainService) BestBlock() (*headerfs.BlockStamp, error) {
	bestHeader, bestHeight, err := s.BlockHeaders.ChainTip()
	if err != nil {
		return nil, err
	}

	_, filterHeight, err := s.RegFilterHeaders.ChainTip()
	if err != nil {
		return nil, err
	}

	// Filter headers might lag behind block headers, so we can fetch a
	// previous block header if the filter headers are not caught up.
	if filterHeight < bestHeight {
		bestHeight = filterHeight
		bestHeader, err = s.BlockHeaders.FetchHeaderByHeight(
			bestHeight,
		)
		if err != nil {
			return nil, err
		}
	}

	return &headerfs.BlockStamp{
		Height:    int32(bestHeight),
		Hash:      bestHeader.BlockHash(),
		Timestamp: bestHeader.Timestamp,
	}, nil
}

// GetBlockHash returns the block hash at the given height.
func (s *ChainService) GetBlockHash(height int64) (*chainhash.Hash, error) {
	header, err := s.BlockHeaders.FetchHeaderByHeight(uint32(height))
	if err != nil {
		return nil, err
	}
	hash := header.BlockHash()
	return &hash, err
}

// GetBlockHeader returns the block header for the given block hash, or an
// error if the hash doesn't exist or is unknown.
func (s *ChainService) GetBlockHeader(
	blockHash *chainhash.Hash) (*wire.BlockHeader, error) {

	header, _, err := s.BlockHeaders.FetchHeader(blockHash)
	return header, err
}

// GetBlockHeight gets the height of a block by its hash. An error is returned
// if the given block hash is unknown.
func (s *ChainService) GetBlockHeight(hash *chainhash.Hash) (int32, error) {
	_, height, err := s.BlockHeaders.FetchHeader(hash)
	if err != nil {
		return 0, err
	}
	return int32(height), nil
}

// BanPeer disconnects and bans a peer due to a specific reason for a duration
// of BanDuration.
func (s *ChainService) BanPeer(addr string, reason banman.Reason) error {
	log.Warnf("Banning peer %v: duration=%v, reason=%v", addr, BanDuration,
		reason)

	// We'll want to disconnect the peer after we return regardless of
	// whether we ban the peer or not. We do this to prevent a possible race
	// condition where we end up reconnecting with the peer slightly
	// before the ban succeeds.
	defer func() {
		// We do so in a goroutine to prevent blocking if the server is
		// handling a query or a new/stale peer.
		go func() {
			if sp := s.PeerByAddr(addr); sp != nil {
				sp.Disconnect()
			}
		}()
	}()

	key, err := banman.ParseAddr(addr)
	if err != nil {
		return fmt.Errorf("unable to parse peer address for peer %v: "+
			"%v", addr, err)
	}
	return s.banStore.BanKey(key, reason, BanDuration)
}

// onPeerUnresponsive is invoked by the query work manager once a peer has
// repeatedly failed to answer queries. The peer is quarantined and
// disconnected, but its IP address is deliberately not banned: slow or
// overloaded peers are common, and an address ban would also catch unrelated
// peers behind the same NAT or on localhost.
func (s *ChainService) onPeerUnresponsive(peer querysyncwm.Peer) {
	s.QuarantinePeer(peer.Addr(), unresponsivePeerQuarantine)
}

// DisconnectPeer disconnects a peer without recording a ban.
func (s *ChainService) DisconnectPeer(addr string) error {
	if sp := s.PeerByAddr(addr); sp != nil {
		sp.Disconnect()
	}
	return nil
}

// QuarantinePeer temporarily excludes one exact peer address from peer
// subscriptions used by query dispatch. Unlike BanPeer, this does not ban the
// peer's IP address, so multiple localhost or NAT-shared peers are not grouped
// together.
func (s *ChainService) QuarantinePeer(addr string, duration time.Duration) {
	until := time.Now().Add(duration)
	log.Warnf("Quarantining peer %v until %s", addr,
		until.Format(time.RFC3339))

	// Hand off without blocking the caller. The block handler calls this,
	// and peerHandler, the only reader of s.query, can itself be blocked
	// handing a new peer to the block handler's full peerChan: a
	// synchronous send deadlocks both until shutdown. Quarantines keep the
	// latest expiry, so their arrival order does not matter.
	//
	// Callers (the block and work managers) stop before Stop waits on
	// s.wg, so the Add below is ordered before that Wait.
	msg := quarantinePeerMsg{addr: addr, until: until}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		select {
		case s.query <- msg:
		case <-s.quit:
		}
	}()
}

func (s *ChainService) trackPeerQuarantine(addr string, until time.Time) {
	s.quarantine.add(addr, until)
}

func (s *ChainService) peerQuarantined(addr string, now time.Time) bool {
	return s.quarantine.active(addr, now)
}

// peerQuarantine records which peer addresses are quarantined, and until
// when. It is safe for concurrent use.
type peerQuarantine struct {
	mtx   sync.Mutex
	until map[string]time.Time
}

// add quarantines addr until the given time, unless it's quarantined for
// longer already.
func (q *peerQuarantine) add(addr string, until time.Time) {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	if q.until == nil {
		q.until = make(map[string]time.Time)
	}

	if current, ok := q.until[addr]; ok && current.After(until) {
		return
	}

	q.until[addr] = until
}

// active returns whether addr is quarantined at the given time, and forgets
// a quarantine that has expired.
func (q *peerQuarantine) active(addr string, now time.Time) bool {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	until, ok := q.until[addr]
	if !ok {
		return false
	}

	if now.Before(until) {
		return true
	}

	delete(q.until, addr)
	return false
}

// UnbanPeer connects and unbans a previously banned peer.
func (s *ChainService) UnbanPeer(addr string, parmanent bool) error {
	log.Infof("UnBanning peer %v", addr)

	key, err := banman.ParseAddr(addr)
	if err != nil {
		return fmt.Errorf("unable to parse peer address for peer %v: "+
			"%v", addr, err)
	}

	err = s.banStore.UnbanKey(key)
	if err != nil {
		return fmt.Errorf("unable to unban peer: %v", err)
	}

	return s.ConnectNode(addr, parmanent)
}

// IsBanned returns true if the peer is banned, and false otherwise.
func (s *ChainService) IsBanned(addr string) bool {
	key, err := banman.ParseAddr(addr)
	if err != nil {
		log.Errorf("Unable to parse peer address for peer %v: %v", addr,
			err)
		return false
	}
	banStatus, err := s.banStore.StatusKey(key)
	if err != nil {
		log.Errorf("Unable to determine ban status for peer %v: %v",
			addr, err)
		return false
	}

	// Log how much time left the peer will remain banned for, if any.
	if time.Now().Before(banStatus.Expiration) {
		log.Debugf("Peer %v is banned for another %v", addr,
			time.Until(banStatus.Expiration))
	}

	return banStatus.Banned
}

// AddPeer adds a new peer that has already been connected to the server.
func (s *ChainService) AddPeer(sp *ServerPeer) {
	select {
	case s.newPeers <- sp:
	case <-s.quit:
		return
	}
}

// AddBytesSent adds the passed number of bytes to the total bytes sent counter
// for the server.  It is safe for concurrent access.
func (s *ChainService) AddBytesSent(bytesSent uint64) {
	atomic.AddUint64(&s.bytesSent, bytesSent)
}

// AddBytesReceived adds the passed number of bytes to the total bytes received
// counter for the server.  It is safe for concurrent access.
func (s *ChainService) AddBytesReceived(bytesReceived uint64) {
	atomic.AddUint64(&s.bytesReceived, bytesReceived)
}

// NetTotals returns the sum of all bytes received and sent across the network
// for all peers.  It is safe for concurrent access.
func (s *ChainService) NetTotals() (uint64, uint64) {
	return atomic.LoadUint64(&s.bytesReceived),
		atomic.LoadUint64(&s.bytesSent)
}

// peerHandler is used to handle peer operations such as adding and removing
// peers to and from the server, banning peers, and broadcasting messages to
// peers.  It must be run in a goroutine.
func (s *ChainService) peerHandler() {
	state := newPeerState()

	// With a fixed set of peers there's no discovery, so nothing would
	// use the seeded addresses. We skip the lookups too, as btcd does
	// with --connect, so a connect-only node doesn't reveal itself to the
	// DNS seeders or their resolvers.
	if !DisableDNSSeed && !s.connectOnly {
		// Add peers discovered through DNS to the address manager.
		connmgr.SeedFromDNS(&s.chainParams, RequiredServices,
			s.nameResolver, func(addrs []*wire.NetAddressV2) {
				var validAddrs []*wire.NetAddressV2
				for _, addr := range addrs {
					addr.Services = RequiredServices

					validAddrs = append(validAddrs, addr)
				}

				if len(validAddrs) == 0 {
					return
				}

				// Bitcoind uses a lookup of the dns seeder
				// here. This is rather strange since the
				// values looked up by the DNS seed lookups
				// will vary quite a lot.  to replicate this
				// behaviour we put all addresses as having
				// come from the first one.
				s.addrManager.AddAddresses(
					validAddrs, validAddrs[0],
				)
			})
	}

out:
	for {
		select {
		// New peers connected to the server.
		case p := <-s.newPeers:
			s.handleAddPeerMsg(state, p)

		// Disconnected peers.
		case p := <-s.donePeers:
			s.handleDonePeerMsg(state, p)

		// Block accepted in mainchain or orphan, update peer height.
		case umsg := <-s.peerHeightsUpdate:
			s.handleUpdatePeerHeights(state, umsg)

		case qmsg := <-s.query:
			s.handleQuery(state, qmsg)

		case <-s.quit:
			// Disconnect all peers on server shutdown, those
			// still in their handshake as well, so none of their
			// connections outlive the service.
			for _, sp := range state.peers {
				log.Tracef("Shutdown peer %s", sp)
				sp.Disconnect()
			}
			break out
		}
	}

	// Drain channels before exiting so nothing is left waiting around
	// to send.
cleanup:
	for {
		select {
		case <-s.newPeers:
		case <-s.donePeers:
		case <-s.peerHeightsUpdate:
		case <-s.query:
		default:
			break cleanup
		}
	}
	s.wg.Done()
	log.Tracef("Peer handler done")
}

// addrStringToNetAddr takes an address in the form of 'host:port' or 'host'
// and returns a net.Addr which maps to the original address with any host
// names resolved to IP addresses and a default port added, if not specified,
// from the ChainService's network parameters.
func (s *ChainService) addrStringToNetAddr(addr string) (net.Addr, error) {
	host, strPort, err := net.SplitHostPort(addr)
	if err != nil {
		switch err.(type) {
		case *net.AddrError:
			host = addr
			strPort = s.ChainParams().DefaultPort
		default:
			return nil, err
		}
	}

	// Tor addresses cannot be resolved to an IP, so just return onionAddr
	// instead. Like a resolved IP address, it always has a port, the
	// default one if none was given, and onion names are case
	// insensitive, so one onion peer gives one address string.
	host = strings.ToLower(host)
	if strings.HasSuffix(host, ".onion") {
		return &onionAddr{addr: net.JoinHostPort(host, strPort)}, nil
	}

	// Attempt to look up an IP address associated with the parsed host.
	// An IP address is its own, which saves a lookup through a resolver
	// that may be slow.
	ips := []net.IP{net.ParseIP(host)}
	if ips[0] == nil {
		ips, err = s.nameResolver(host)
		if err != nil {
			return nil, err
		}
	}

	if len(ips) == 0 {
		return nil, fmt.Errorf("no addresses found for %s", host)
	}

	port, err := strconv.Atoi(strPort)
	if err != nil {
		return nil, err
	}

	return &net.TCPAddr{
		IP:   ips[0],
		Port: port,
	}, nil
}

// handleUpdatePeerHeight updates the heights of all peers who were known to
// announce a block we recently accepted.
func (s *ChainService) handleUpdatePeerHeights(state *peerState, umsg updatePeerHeightsMsg) {
	state.forAllPeers(func(sp *ServerPeer) {
		// The origin peer should already have the updated height.
		if sp == umsg.originPeer {
			return
		}

		// This is a pointer to the underlying memory which doesn't
		// change.
		latestBlkHash := sp.LastAnnouncedBlock()

		// Skip this peer if it hasn't recently announced any new blocks.
		if latestBlkHash == nil {
			return
		}

		// If the peer has recently announced a block, and this block
		// matches our newly accepted block, then update their block
		// height.
		if *latestBlkHash == *umsg.newHash {
			sp.UpdateLastBlockHeight(umsg.newHeight)
			sp.UpdateLastAnnouncedBlock(nil)
		}
	})
}

// handleAddPeerMsg deals with adding new peers.  It is invoked from the
// peerHandler goroutine.
func (s *ChainService) handleAddPeerMsg(state *peerState, sp *ServerPeer) bool {
	if sp == nil || !sp.Connected() {
		return false
	}

	// Ignore new peers if we're shutting down.
	if atomic.LoadInt32(&s.shutdown) != 0 {
		log.Infof("New peer %s ignored - server is shutting down", sp)
		sp.Disconnect()
		return false
	}

	// Disconnect banned peers.
	if s.IsBanned(sp.Addr()) {
		sp.Disconnect()
		return false
	}
	if s.peerQuarantined(sp.Addr(), time.Now()) {
		sp.Disconnect()
		return false
	}

	// TODO: Check for max peers from a single IP.

	// Limit max number of total peers. A permanent peer is one we were
	// asked to keep, so it's never refused: were it refused, its request
	// would retry, and be refused again, for as long as we're full.
	// Instead it takes the place of a one-off peer, or, with only
	// permanent peers left, goes above MaxPeers, the way Bitcoin Core
	// keeps separate slots for the nodes it's told to add. Neither can
	// loop: an evicted one-off peer isn't replaced while we're full, and
	// a permanent peer evicts no other permanent peer.
	if state.Count() >= MaxPeers {
		if !sp.persistent {
			log.Infof("Max peers reached [%d] - disconnecting "+
				"peer %s", MaxPeers, sp)
			sp.Disconnect()
			return false
		}

		if !evictOneOffPeer(state) {
			log.Infof("Max peers reached [%d] - keeping "+
				"permanent peer %s above the limit",
				MaxPeers, sp)
		}
	}

	// Add the new peer and start it.
	log.Debugf("New peer %s", sp)
	if sp.persistent {
		state.persistentPeers[sp.ID()] = sp
	} else {
		state.outboundPeers[sp.ID()] = sp
	}

	// Close firstPeerConnect channel so blockManager will be notified.
	if s.firstPeerConnect != nil {
		close(s.firstPeerConnect)
		s.firstPeerConnect = nil
	}

	// Update the address' last seen time if the peer has acknowledged our
	// version and has sent us its version as well.
	if sp.VerAckReceived() && sp.VersionKnown() && sp.NA() != nil {
		s.addrManager.Connected(sp.NA())
	}

	// Signal the block manager this peer is a new sync candidate.
	s.blockManager.NewPeer(sp)

	// Update the address manager and request known addresses from the
	// remote peer for outbound connections. This is skipped when running on
	// a development network since it is only intended to connect to
	// specified peers and actively avoids advertising and connecting to
	// discovered peers.
	if !isDevNetwork(s.chainParams.Net) {
		// Request known addresses if the server address manager needs
		// more and the peer has a protocol version new enough to
		// include a timestamp with addresses.
		hasTimestamp := sp.ProtocolVersion() >= wire.NetAddressTimeVersion
		if s.addrManager.NeedMoreAddresses() && hasTimestamp {
			sp.QueueMessage(wire.NewMsgGetAddr(), nil)
		}

		// Add the address to the addr manager anew, and also mark it as
		// a good address.
		s.addrManager.AddAddresses([]*wire.NetAddressV2{sp.NA()}, sp.NA())
		s.addrManager.Good(sp.NA())
	}

	// We'll go through each peer subscriber and notify it about the added
	// peer.
	n := 0
	for i, sub := range s.peerSubscribers {
		select {
		// Quickly check whether this subscription has been canceled.
		case <-sub.cancel:
			// Avoid GC leak.
			s.peerSubscribers[i] = nil
			continue
		default:
		}

		// Keep non-canceled subscribers around.
		s.peerSubscribers[n] = sub
		n++

		// Send a notification in a goroutine to avoid blocking the
		// peerHandler.
		s.wg.Add(1)
		go s.notifyConnectedPeer(sub, sp)
	}

	// Re-align the slice to only active subscribers.
	s.peerSubscribers = s.peerSubscribers[:n]

	return true
}

// notifyConnectedPeer sends the given peer to the peerSubsription.
//
// NOTE: MUST be run as a goroutine.
func (s *ChainService) notifyConnectedPeer(
	sub *peerSubscription, sp *ServerPeer) {

	defer s.wg.Done()

	select {
	case sub.peers <- sp:
	case <-sub.cancel:
	case <-s.quit:
	}
}

// handleDonePeerMsg deals with peers that have signalled they are done.  It is
// invoked from the peerHandler goroutine.
func (s *ChainService) handleDonePeerMsg(state *peerState, sp *ServerPeer) {
	key := sp.key()
	delete(state.peers, key)
	dropPeer(state, sp)

	// The connection manager reconnects a permanent peer on its own.
	pr, permanent := state.permanent[key]
	if permanent && pr.req == sp.connReq {
		s.connManager.Disconnect(sp.connReq.ID())
		return
	}

	// A one-off peer's request ends with it. A removed permanent peer
	// had its request ended on removal.
	if !sp.persistent {
		s.connManager.Remove(sp.connReq.ID())
	}

	// A permanent request held back while this peer had its address,
	// because this was a one-off peer made permanent or a removed
	// permanent peer added again, starts now. Its request can't have
	// started before, since we admit one peer per address, and refuse
	// one-off connections to a permanent peer's address.
	if permanent {
		go s.connManager.Connect(pr.req)
		return
	}

	// Only request a new connection if the peer being disconnected is not
	// persistent and we still need more peer connections.
	if !sp.persistent && state.Count() < MaxPeers {
		go s.connManager.NewConnReq()
	}
}

// evictOneOffPeer disconnects a one-off outbound peer to make room for a
// permanent one. It returns false if there's none to evict.
func evictOneOffPeer(state *peerState) bool {
	for _, sp := range state.outboundPeers {
		// A one-off peer that has been made permanent is kept.
		if _, ok := state.permanent[sp.key()]; ok {
			continue
		}

		log.Infof("Disconnecting peer %s to make room for a "+
			"permanent peer", sp)
		dropPeer(state, sp)
		sp.Disconnect()

		return true
	}

	return false
}

// dropPeer takes the peer out of the added peers if it's among them, that is,
// if we received its verack.
func dropPeer(state *peerState, sp *ServerPeer) {
	list := state.outboundPeers
	if sp.persistent {
		list = state.persistentPeers
	}
	if _, ok := list[sp.ID()]; ok {
		delete(list, sp.ID())

		log.Debugf("Removed peer %s", sp)
	}
}

// SendTransaction broadcasts the transaction to all currently active peers so
// it can be propagated to other nodes and eventually mined. An error won't be
// returned if the transaction already exists within the mempool. Any
// transaction broadcast through this method will be rebroadcast upon every
// change of the tip of the chain.
func (s *ChainService) SendTransaction(tx *wire.MsgTx) error {
	// TODO(roasbeef): pipe through querying interface
	return s.broadcaster.Broadcast(tx)
}

// MarkAsConfirmed tells the broadcaster that a transaction has been confirmed
// and no longer needs to be rebroadcast.
func (s *ChainService) MarkAsConfirmed(txHash chainhash.Hash) {
	s.broadcaster.MarkAsConfirmed(txHash)
}

// NewPeerConfig returns the configuration for the given ServerPeer.
func NewPeerConfig(sp *ServerPeer) *peer.Config {
	return &peer.Config{
		Listeners: peer.MessageListeners{
			OnVersion:   sp.OnVersion,
			OnVerAck:    sp.OnVerAck,
			OnInv:       sp.OnInv,
			OnHeaders:   sp.OnHeaders,
			OnReject:    sp.OnReject,
			OnFeeFilter: sp.OnFeeFilter,
			OnAddr:      sp.OnAddr,
			OnAddrV2:    sp.OnAddrV2,
			OnRead:      sp.OnRead,
			OnWrite:     sp.OnWrite,
		},
		NewestBlock:      sp.newestBlock,
		HostToNetAddress: sp.server.addrManager.HostToNetAddress,
		UserAgentName:    sp.server.userAgentName,
		UserAgentVersion: sp.server.userAgentVersion,
		ChainParams:      &sp.server.chainParams,
		Services:         sp.server.services,
		ProtocolVersion:  wire.AddrV2Version,
		DisableRelayTx:   true,
	}
}

// outboundPeerConnected is invoked by the connection manager when a new
// outbound connection is established. It hands the connection to the peer
// handler, which starts a server peer on it in handlePeerConnected, since
// the peer handler holds the peers and the permanent requests that decide
// whether we keep the connection.
func (s *ChainService) outboundPeerConnected(c *connmgr.ConnReq, conn net.Conn) {
	select {
	case s.query <- peerConnectedMsg{req: c, conn: conn}:
	case <-s.quit:
		_ = conn.Close()
	}
}

// handlePeerConnected starts a peer on a new outbound connection, unless we
// have a peer at that address already or the request is no longer wanted.
// It is invoked from the peerHandler goroutine.
func (s *ChainService) handlePeerConnected(state *peerState,
	c *connmgr.ConnReq, conn net.Conn) {

	// In the event that we have to disconnect the peer, we'll choose the
	// appropriate method to do so based on whether the connection request
	// is for a persistent peer or not. Either way, the connection manager
	// closes the connection.
	disconnect := func() {
		if c.Permanent {
			s.connManager.Disconnect(c.ID())
			return
		}

		// Since we're completely removing the request for this peer,
		// we'll need to request a new one.
		s.connManager.Remove(c.ID())
		go s.connManager.NewConnReq()
	}

	key := keyOf(c.Addr)
	pr, permanent := state.permanent[key]
	switch {
	// The permanent peer was removed while this connection was being
	// made, and the removal ended its request.
	case c.Permanent && (!permanent || pr.req != c):
		_ = conn.Close()
		return

	// A permanent request owns its address, so a one-off connection to
	// it would be a second peer at the address.
	case !c.Permanent && permanent:
		disconnect()
		return
	}

	// If the peer is banned, then we'll disconnect them.
	peerAddr := c.Addr.String()
	if s.IsBanned(peerAddr) {
		disconnect()
		return
	}

	// If we're already connected to this peer, then we'll close out the new
	// connection and keep the old.
	if _, ok := state.peers[key]; ok {
		disconnect()
		return
	}

	sp := NewServerPeer(s, c.Permanent)
	p, err := peer.NewOutboundPeer(NewPeerConfig(sp), peerAddr)
	if err != nil {
		log.Debugf("Cannot create outbound peer %s: %s", c.Addr, err)
		disconnect()
		return
	}
	sp.Peer = p
	sp.connReq = c
	state.peers[key] = sp
	sp.AssociateConnection(conn)

	// Stop waits for the peer to be done. The peer handler runs under
	// s.wg, so this Add comes before Stop's Wait returns.
	s.wg.Add(1)
	go s.peerDoneHandler(sp)
}

// dialPeer wraps the dial function given to the connection manager. A
// permanent request may be removed before the connection manager has
// registered it, when ending it there has no effect, and the manager then
// dials it and keeps retrying. The manager has registered a request by the
// time it dials it, so we end a removed request here instead of dialing.
// Every connection we dial is also tracked in s.conns, until it's closed.
func (s *ChainService) dialPeer(
	dial func(net.Addr) (net.Conn, error)) func(net.Addr) (net.Conn,
	error) {

	return func(addr net.Addr) (net.Conn, error) {
		if pr, ok := addr.(*permanentReq); ok {
			if pr.removed.Load() {
				s.connManager.Remove(pr.req.ID())
				return nil, errPeerRemoved
			}
			addr = pr.Addr
		}

		// We'd only drop a connection to a banned or quarantined
		// peer once it's made, and a permanent request would then
		// retry it on the next retry interval, since the connection
		// manager resets a request's backoff when it connects. A
		// failed dial grows the backoff instead, so the request
		// waits out the ban or quarantine, and dials again after.
		peerAddr := addr.String()
		if s.IsBanned(peerAddr) {
			return nil, errPeerBanned
		}
		if s.quarantine.active(peerAddr, time.Now()) {
			return nil, errPeerQuarantined
		}

		// A peer we found ourselves isn't dialed while we have a
		// dial or a peer in its group. The address was picked for
		// an empty group, but another dial may have taken the group
		// since.
		discovered, only := addr.(*discoveredAddr)
		if only {
			addr = discovered.Addr
		}
		group := s.groupOf(addr)
		if !s.conns.startDial(group, only) {
			return nil, errGroupTaken
		}

		conn, err := dial(addr)

		return s.conns.track(group, conn, err)
	}
}

// connSet holds open connections, so they can all be closed at once. It
// also counts, for each outbound group, the dials in flight and the open
// connections they made, from the start of the dial until the connection is
// closed, so a dial and a peer in its handshake count as much as a
// connected peer.
type connSet struct {
	mtx    sync.Mutex
	conns  map[*setConn]struct{}
	groups map[string]int
	closed bool
}

// setConn is a connection in a connSet, which leaves the set when it's
// closed.
type setConn struct {
	net.Conn
	set   *connSet
	group string
}

// Close closes the connection and takes it out of its set.
func (c *setConn) Close() error {
	c.set.mtx.Lock()
	if _, ok := c.set.conns[c]; ok {
		delete(c.set.conns, c)
		c.set.groups[c.group]--
	}
	c.set.mtx.Unlock()

	return c.Conn.Close()
}

// startDial counts a dial to the group, unless only is set and the group
// has a dial or a connection already, in which case it returns false.
func (cs *connSet) startDial(group string, only bool) bool {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()

	if only && cs.groups[group] != 0 {
		return false
	}

	if cs.groups == nil {
		cs.groups = make(map[string]int)
	}
	cs.groups[group]++

	return true
}

// track adds the result of a dial to the group, counted by startDial, to
// the set. A failed dial is no longer counted, and a connection made once
// the set has been closed is closed right away.
func (cs *connSet) track(group string, conn net.Conn,
	err error) (net.Conn, error) {

	cs.mtx.Lock()
	defer cs.mtx.Unlock()

	if err == nil && cs.closed {
		_ = conn.Close()
		err = ErrShuttingDown
	}
	if err != nil {
		cs.groups[group]--
		return nil, err
	}

	if cs.conns == nil {
		cs.conns = make(map[*setConn]struct{})
	}
	c := &setConn{Conn: conn, set: cs, group: group}
	cs.conns[c] = struct{}{}

	return c, nil
}

// groupCount returns the number of dials and connections to the group.
func (cs *connSet) groupCount(group string) int {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()

	return cs.groups[group]
}

// closeAll closes every connection in the set, and any added later.
func (cs *connSet) closeAll() {
	cs.mtx.Lock()
	cs.closed = true
	conns := cs.conns
	cs.conns = nil
	cs.mtx.Unlock()

	for c := range conns {
		_ = c.Conn.Close()
	}
}

// groupOf returns the outbound group of a resolved address.
func (s *ChainService) groupOf(addr net.Addr) string {
	host, portStr, err := net.SplitHostPort(addr.String())
	if err != nil {
		return addr.String()
	}
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return addr.String()
	}

	na, err := s.addrManager.HostToNetAddress(host, uint16(port), 0)
	if err != nil {
		return addr.String()
	}

	return addrmgr.GroupKey(na)
}

// discoveredAddr is the address of a peer we found ourselves, rather than
// one we were asked to connect to. We keep to one of those per outbound
// group.
type discoveredAddr struct {
	net.Addr
}

// peerDoneHandler handles peer disconnects by notifying the server that it's
// done along with other performing other desirable cleanup.
func (s *ChainService) peerDoneHandler(sp *ServerPeer) {
	defer s.wg.Done()

	sp.WaitForDisconnect()

	select {
	case s.donePeers <- sp:
	case <-s.quit:
		return
	}

	// Only tell block manager we are gone if we ever told it we existed.
	if sp.VersionKnown() {
		s.blockManager.DonePeer(sp)
	}
	close(sp.quit)
}

// UpdatePeerHeights updates the heights of all peers who have announced the
// latest connected main chain block, or a recognized orphan. These height
// updates allow us to dynamically refresh peer heights, ensuring sync peer
// selection has access to the latest block heights for each peer.
func (s *ChainService) UpdatePeerHeights(latestBlkHash *chainhash.Hash,
	latestHeight int32, updateSource *ServerPeer) {

	select {
	case s.peerHeightsUpdate <- updatePeerHeightsMsg{
		newHash:    latestBlkHash,
		newHeight:  latestHeight,
		originPeer: updateSource,
	}:
	case <-s.quit:
		return
	}
}

// ChainParams returns a copy of the ChainService's chaincfg.Params.
func (s *ChainService) ChainParams() chaincfg.Params {
	return s.chainParams
}

// Start begins connecting to peers and syncing the blockchain.
func (s *ChainService) Start(ctx context.Context) error {
	// Already started?
	if atomic.AddInt32(&s.started, 1) != 1 {
		return nil
	}

	// Import headers if configured.
	//nolint:lll
	if s.headersImport != nil {
		options := chainimport.ImportOptions{
			BlockHeadersSource:      s.headersImport.BlockHeadersSource,
			FilterHeadersSource:     s.headersImport.FilterHeadersSource,
			TargetChainParams:       s.chainParams,
			TargetBlockHeaderStore:  s.BlockHeaders,
			TargetFilterHeaderStore: s.RegFilterHeaders,
			ValidationFlags:         s.headersImport.ValidationFlags,
			WriteBatchSizePerRegion: s.headersImport.WriteBatchSizePerRegion,
		}
		importer, err := chainimport.NewHeadersImport(&options)
		if err != nil {
			return err
		}
		if _, err := importer.Import(ctx); err != nil {
			return err
		}

		// The block manager was constructed before the import ran,
		// so its internal header tracking state (the validation cache,
		// tip feed, etc.) reflects the pre-import chain tips. Re-read
		// the now-updated stores so the block manager starts syncing
		// from the correct height rather than from genesis.
		if err := s.blockManager.ResetHeaderState(); err != nil {
			return fmt.Errorf("failed to reset block manager "+
				"state after headers import: %w", err)
		}
	}

	// Start the address manager and block manager, both of which are
	// needed by peers.
	s.addrManager.Start()
	s.blockManager.Start()
	s.blockSubscriptionMgr.Start()
	if err := s.workManager.Start(); err != nil {
		return fmt.Errorf("unable to start work manager: %v", err)
	}

	if err := s.utxoScanner.Start(); err != nil {
		return fmt.Errorf("unable to start utxo scanner: %v", err)
	}

	if err := s.broadcaster.Start(); err != nil {
		return fmt.Errorf("unable to start transaction broadcaster: %v",
			err)
	}

	if s.persistToDisk {
		s.filterBatchWriter.Start()
	}

	go s.connManager.Start()

	// Start the peer handler which in turn starts the address and block
	// managers.
	s.wg.Add(1)
	go s.peerHandler()

	return nil
}

// Stop gracefully shuts down the server by stopping and disconnecting all
// peers and the main listener.
func (s *ChainService) Stop() error {
	// Make sure this only happens once.
	if atomic.AddInt32(&s.shutdown, 1) != 1 {
		return nil
	}

	var returnErr error
	storeJoined := true
	s.connManager.Stop()
	s.broadcaster.Stop()
	if err := s.utxoScanner.Stop(); err != nil {
		log.Errorf("error stopping utxo scanner: %v", err)
		returnErr = err
	}
	if err := s.workManager.Stop(); err != nil {
		log.Errorf("error stopping work manager: %v", err)
		returnErr = err
	}
	s.blockSubscriptionMgr.Stop()
	if err := s.blockManager.Stop(); err != nil {
		storeJoined = false
		log.Errorf("error stopping block manager: %v", err)
		returnErr = err
	}
	if err := s.addrManager.Stop(); err != nil {
		log.Errorf("error stopping address manager: %v", err)
		returnErr = err
	}

	if s.persistToDisk {
		s.filterBatchWriter.Stop()
	}

	// Signal the remaining goroutines to quit.
	close(s.quit)
	s.wg.Wait()

	// The connection manager drops a connection whose dial finished
	// while it was stopping, without closing it, so we close any
	// connection still open now that every peer is done.
	s.conns.closeAll()

	// Release the SQL backend connection if one was opened. This must
	// happen after all background goroutines exit so no one is using
	// the backend mid-shutdown.
	if s.sqlBackend != nil && storeJoined {
		if err := s.sqlBackend.Close(); err != nil {
			log.Errorf("error closing sql backend: %v", err)
			if returnErr == nil {
				returnErr = err
			}
		}
	}

	return returnErr
}

// IsCurrent lets the caller know whether the chain service's block manager
// thinks its view of the network is current.
func (s *ChainService) IsCurrent() bool {
	return s.blockManager.IsFullySynced()
}

// PeerByAddr lets the caller look up a peer address in the service's peer
// table, if connected to that peer address. The address is matched the way
// ConnectNode resolves it.
func (s *ChainService) PeerByAddr(addr string) *ServerPeer {
	key := s.lookupKey(addr)
	for _, peer := range s.Peers() {
		if peer.key() == key {
			return peer
		}
	}
	return nil
}

// RescanChainSource is a wrapper type around the ChainService struct that will
// be used to satisfy the rescan.ChainSource interface.
type RescanChainSource struct {
	*ChainService
}

// A compile-time check to ensure that RescanChainSource implements the
// rescan.ChainSource interface.
var _ ChainSource = (*RescanChainSource)(nil)

// GetBlockHeaderByHeight returns the header of the block with the given height.
func (s *RescanChainSource) GetBlockHeaderByHeight(
	height uint32) (*wire.BlockHeader, error) {

	return s.BlockHeaders.FetchHeaderByHeight(height)
}

// GetBlockHeader returns the header of the block with the given hash.
func (s *RescanChainSource) GetBlockHeader(
	hash *chainhash.Hash) (*wire.BlockHeader, uint32, error) {

	return s.BlockHeaders.FetchHeader(hash)
}

// GetFilterHeaderByHeight returns the filter header of the block with the given
// height.
func (s *RescanChainSource) GetFilterHeaderByHeight(
	height uint32) (*chainhash.Hash, error) {

	return s.RegFilterHeaders.FetchHeaderByHeight(height)
}

// Subscribe returns a block subscription that delivers block notifications in
// order. The bestHeight parameter can be used to signal that a backlog of
// notifications should be delivered from this height. When providing a height
// of 0, a backlog will not be delivered.
func (s *RescanChainSource) Subscribe(
	bestHeight uint32) (*blockntfns.Subscription, error) {

	return s.blockSubscriptionMgr.NewSubscription(bestHeight)
}

// cachedAddr is an empty struct used to satisfy the cache.Value interface.
type cachedAddr struct{}

// Size returns the size of cachedAddr, which is 1.
func (c *cachedAddr) Size() (uint64, error) {
	return 1, nil
}

// onionAddr implements the net.Addr interface and represents a tor address.
// This code is identical to btcd's unexported onionAddr. It is used so that
// neutrino can connect to v2 addresses without relying on the OnionCat
// encoding. It also enables connecting to v3 addresses.
type onionAddr struct {
	addr string
}

// String returns the onion address.
func (o *onionAddr) String() string {
	return o.addr
}

// Network returns "onion".
func (o *onionAddr) Network() string {
	return "onion"
}

// Ensure onionAddr implements the net.Addr interface.
var _ net.Addr = (*onionAddr)(nil)

// dialUntilQuit wraps dial so that a pending dial returns as soon as quit is
// closed, rather than when the underlying dial gives up. The connection
// manager dials synchronously from goroutines that Stop waits on, and a dial
// through a proxy can take minutes to time out, so without this an
// unreachable peer would hold up shutdown for that long (lnd#7349).
//
// The underlying dial keeps running in the background until it returns, and
// any connection it makes after quit has closed is closed right away.
func dialUntilQuit(dial func(net.Addr) (net.Conn, error),
	quit <-chan struct{}) func(net.Addr) (net.Conn, error) {

	return func(addr net.Addr) (net.Conn, error) {
		return untilQuit(
			quit, func() (net.Conn, error) { return dial(addr) },
			func(conn net.Conn) {
				if conn != nil {
					_ = conn.Close()
				}
			},
		)
	}
}

// untilQuit runs call and returns its result, or ErrShuttingDown as soon as
// quit is closed if call is still running then. For calls that can't be
// cancelled (a dial, a name lookup through a proxy) but run in goroutines
// Stop waits on. call keeps running in the background until it returns, and
// a result it produces after quit has closed is passed to discard, if set.
func untilQuit[T any](quit <-chan struct{}, call func() (T, error),
	discard func(T)) (T, error) {

	type result struct {
		val T
		err error
	}

	// The channel is unbuffered, so once the caller has given up, the
	// call's goroutine can't hand its result over and discards it.
	resultChan := make(chan result)
	go func() {
		val, err := call()
		select {
		case resultChan <- result{val: val, err: err}:
		case <-quit:
			if err == nil && discard != nil {
				discard(val)
			}
		}
	}()

	select {
	case res := <-resultChan:
		return res.val, res.err

	case <-quit:
		var zero T
		return zero, ErrShuttingDown
	}
}

// corruptStoreHint says how to recover from header stores the start up checks
// find damaged beyond repair.
const corruptStoreHint = "the header stores were likely damaged by an " +
	"unclean shutdown, remove the chain data (block_headers.bin, " +
	"reg_filter_headers.bin and neutrino.db) to sync it again"

// rollBackZeroedFilterTip rolls the filter header store back past any
// all-zero headers at its tip. A filter header is a hash, so an all-zero one
// is never real: it's what a flat file holds when a power loss drops records
// the index already points at (neutrino#451), and it can be carried over into
// the SQL store by the legacy import. Left in place, the un-checkpointed
// cfheader sync checks every peer's PrevFilterHeader against it and bans them
// all. Rolling back just re-fetches the lost headers.
//
// The same power loss can zero the block header at the tip too. That one
// can't be rolled back the same way, as its parent hash is gone with it, so
// we refuse to start with an error that says how to recover rather than sync
// from a tip whose hash the index doesn't know.
func rollBackZeroedFilterTip(blocks headerfs.BlockHeaderStore,
	filters headerfs.FilterHeaderStore) error {

	blockTip, blockHeight, err := blocks.ChainTip()
	if err != nil {
		return err
	}
	if blockHeight > 0 {
		// The tip has to link to the header below it. A zeroed record
		// never does, and checking the link rather than the index
		// also catches one the legacy SQL import carried over, as the
		// import keys each row by the hash of whatever bytes it read.
		parent, err := blocks.FetchHeaderByHeight(blockHeight - 1)
		if err != nil {
			return err
		}
		if blockTip.PrevBlock != parent.BlockHash() {
			return fmt.Errorf("block header tip at height %d is "+
				"corrupt: %s", blockHeight, corruptStoreHint)
		}
	}

	var rolledBack uint32
	for {
		tip, height, err := filters.ChainTip()
		if err != nil {
			return err
		}
		if *tip != (chainhash.Hash{}) || height == 0 {
			break
		}
		if rolledBack == 0 {
			log.Infof("Filter header tip at height %d is zeroed, "+
				"rolling back to the last good header", height)
		}

		parent, err := blocks.FetchHeaderByHeight(height - 1)
		if err != nil {
			return err
		}
		parentHash := parent.BlockHash()

		// The same power loss can also zero the block headers below
		// the tip, in which case the parent read back isn't a real
		// block. Rolling back onto it would make the filter tip point
		// at a block the index doesn't know, and the store would then
		// fail to open on every start. Refuse instead, so the store is
		// left as it is.
		parentHeight, err := blocks.HeightFromHash(&parentHash)
		if err != nil || parentHeight != height-1 {
			return fmt.Errorf("block header at height %d is "+
				"corrupt, unable to roll back the zeroed "+
				"filter header at height %d: %s", height-1,
				height, corruptStoreHint)
		}

		_, err = filters.RollbackLastBlock(&parentHash)
		if err != nil {
			return err
		}
		rolledBack++
	}

	if rolledBack > 0 {
		log.Warnf("Rolled back %d zeroed filter headers at the tip of "+
			"the filter header store, they'll be fetched again",
			rolledBack)
	}

	return nil
}
