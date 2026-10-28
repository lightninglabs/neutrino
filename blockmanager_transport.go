package neutrino

import (
	"errors"

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

// headerSyncAskOp names a headersync actor ask whose outcome can be faulted by
// deterministic tests.
type headerSyncAskOp uint8

const (
	// headerSyncAskCommitReady is the post-write CommitReadyRanges ask.
	headerSyncAskCommitReady headerSyncAskOp = iota

	// headerSyncAskCompleteRange is the range completion ask made when a
	// range reply arrives.
	headerSyncAskCompleteRange

	// headerSyncAskFinishAnchor is the anchor discovery ask that may
	// stage a discovered range.
	headerSyncAskFinishAnchor

	// headerSyncAskRequestRange is the ask that starts a peer's next
	// range.
	headerSyncAskRequestRange

	// headerSyncAskOps is the number of faultable asks.
	headerSyncAskOps
)

// headerSyncAskFault selects how a faulted ask reports its outcome.
type headerSyncAskFault uint8

const (
	// headerSyncAskOK runs the ask normally.
	headerSyncAskOK headerSyncAskFault = iota

	// headerSyncAskFailBefore reports a caller deadline without sending
	// the ask, so the actor state is unchanged.
	headerSyncAskFailBefore

	// headerSyncAskFailAfter runs the ask and then reports a caller
	// deadline. An lnd actor applies a request on its own context, so a
	// caller can observe ctx.Done after the mutation already happened.
	headerSyncAskFailAfter
)

// errHeaderSyncAskFault is the error a faulted ask reports to its caller.
var errHeaderSyncAskFault = errors.New("simulated headersync ask deadline")

// headerSyncAsk runs one actor ask through the optional test fault seam.
// Production leaves the seam nil, so this only calls ask.
func (b *blockManager) headerSyncAsk(op headerSyncAskOp,
	ask func() error) error {

	fault := headerSyncAskOK
	if b.cfg.headerSyncAskFault != nil {
		fault = b.cfg.headerSyncAskFault(op)
	}
	if fault == headerSyncAskFailBefore {
		return errHeaderSyncAskFault
	}
	if err := ask(); err != nil {
		return err
	}
	if fault == headerSyncAskFailAfter {
		return errHeaderSyncAskFault
	}

	return nil
}
