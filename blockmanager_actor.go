package neutrino

// This file holds the block handler as an actor. Everything the block manager
// reacts to arrives as one message in a single mailbox: a message from a peer,
// a peer that went away, an expired deadline, a timer, and the result of a
// chain store mutation. The actor handles them one at a time, so the driver and
// the state the executor owns need no lock, and it runs each event to
// completion, including the follow-up events of its effects, before it reads
// the next message.

import (
	"container/list"
	"context"

	"github.com/lightninglabs/neutrino/chainstore"
	"github.com/lightningnetwork/lnd/actor"
	"github.com/lightningnetwork/lnd/fn/v2"
)

// blockHandlerMailbox is the capacity of the block handler's mailbox. It adds
// up the buffers of the channels the mailbox replaced: messages from peers,
// departed peers, timers and deadlines.
var blockHandlerMailbox = MaxPeers*10 + 1

// blockInput is the one message type of the block handler actor. Its payload is
// a peer message, a timer or deadline message, a donePeerMsg or a
// chainstore.Result.
type blockInput struct {
	actor.BaseMessage

	payload any
}

// MessageType returns the name of the message type.
func (blockInput) MessageType() string {
	return "neutrino.blockInput"
}

// blockHandler is the behavior of the block handler actor. It owns the list of
// candidate peers, which only Receive touches.
type blockHandler struct {
	bm    *blockManager
	peers *list.List
}

// Receive handles one input. A message from a peer or an expired deadline goes
// through the driver, which holds it back while a chain store mutation is in
// flight. The rest is bookkeeping that cannot wait: a peer's departure, a
// timer, and the result of the mutation itself.
func (h *blockHandler) Receive(_ context.Context,
	input blockInput) fn.Result[struct{}] {

	switch msg := input.payload.(type) {
	case *newPeerMsg, *invMsg, *headersMsg, *headerSyncTimeoutMsg:
		h.bm.dispatchInbound(h.peers, msg)

	case *donePeerMsg, *headerSyncBootstrapMsg, *headerSyncCooldownMsg,
		*headerSyncCommitRetryMsg:

		h.bm.handlePeerMessage(h.peers, msg)

	case chainstore.Result:
		h.bm.finishAsyncHeaderCommit(h.peers, msg)

	default:
		log.Warnf("Invalid message type in block handler: %T", msg)
	}

	return fn.Ok(struct{}{})
}

// newBlockHandler creates the block handler actor without starting it.
func (b *blockManager) newBlockHandler() error {
	handler, err := actor.NewActor(actor.ActorConfig[blockInput, struct{}]{
		ID:          "block-handler",
		Behavior:    &blockHandler{bm: b, peers: list.New()},
		MailboxSize: blockHandlerMailbox,
	})
	if err != nil {
		return err
	}

	b.handler = handler
	b.setDeliver(b.tellHandler)

	return nil
}

// tellHandler puts a message in the handler's mailbox. It waits for room, and
// gives up when the block manager shuts down. It must never be called from
// inside Receive: it carries no turn context, so on a full mailbox it would
// park the handler on itself.
func (b *blockManager) tellHandler(message any) {
	b.handler.Ref().Tell(b.quitCtx, blockInput{payload: message})
}

// startBlockHandler starts the block handler actor, and points deliver at its
// mailbox, so a test that collected messages by hand until now no longer does.
func (b *blockManager) startBlockHandler() {
	b.setDeliver(b.tellHandler)
	b.handler.Start()
}

// setDeliver sets where post sends the messages for the block handler.
func (b *blockManager) setDeliver(deliver func(message any)) {
	b.deliver.Store(&deliver)
}

// stopBlockHandler stops the block handler actor and waits for the message it
// is handling to finish.
func (b *blockManager) stopBlockHandler() {
	if b.handler == nil {
		return
	}

	// The actor's context ends with this call, and the message in flight
	// has no other wait than the one its own deadline bounds.
	_ = b.handler.StopAndWait(context.Background())
}

// post queues a message for the block handler.
func (b *blockManager) post(message any) {
	(*b.deliver.Load())(message)
}

// postChainResult queues the outcome of a chain store mutation.
func (b *blockManager) postChainResult(result chainstore.Result) {
	b.post(result)
}
