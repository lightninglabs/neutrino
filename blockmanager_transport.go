package neutrino

import (
	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chainhash/v2"
)

// headerTransport owns outbound getheaders delivery. Responses still enter
// through QueueHeaders, and request deadlines remain owned by blockmanager.
type headerTransport interface {
	SendGetHeaders(*ServerPeer, blockchain.BlockLocator,
		*chainhash.Hash) error
}

func (b *blockManager) sendHeaderRequest(peer *ServerPeer,
	locator blockchain.BlockLocator, stopHash *chainhash.Hash) error {

	if b.cfg.HeaderTransport != nil {
		return b.cfg.HeaderTransport.SendGetHeaders(
			peer, locator, stopHash,
		)
	}
	return peer.PushGetHeadersMsg(locator, stopHash)
}
