package neutrino

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/lightningnetwork/lnd/actor"
	"github.com/lightningnetwork/lnd/fn/v2"
)

// chainStoreMessage is a private command processed by the chain store actor.
type chainStoreMessage interface {
	actor.Message
	chainStoreCommand()
}

// chainStoreBlockCommit requests a block-header mutation. ExpectedParent
// identifies the committed tip against which the batch was prepared.
type chainStoreBlockCommit struct {
	actor.BaseMessage
	ExpectedParent headerfs.BlockStamp
	Headers        []headerfs.BlockHeader
}

func (chainStoreBlockCommit) MessageType() string { return "block_commit" }
func (chainStoreBlockCommit) chainStoreCommand()  {}

// chainStoreRollback requests a paired block and filter rollback.
type chainStoreRollback struct {
	actor.BaseMessage
	ExpectedTip headerfs.BlockStamp
	Height      uint32
}

func (chainStoreRollback) MessageType() string { return "rollback" }
func (chainStoreRollback) chainStoreCommand()  {}

// reorgMessageType names the reorg command.
const reorgMessageType = "reorg"

// chainStoreReorg applies a paired rollback and replacement block batch in
// one actor turn, preventing another filter commit between the phases.
type chainStoreReorg struct {
	actor.BaseMessage
	ExpectedTip headerfs.BlockStamp
	ForkHeight  uint32
	Headers     []headerfs.BlockHeader
}

func (chainStoreReorg) MessageType() string { return reorgMessageType }
func (chainStoreReorg) chainStoreCommand()  {}

// chainStoreFilterCommit requests a verified filter-header batch commit.
type chainStoreFilterCommit struct {
	actor.BaseMessage
	Messages []*wire.MsgCFHeaders
}

func (chainStoreFilterCommit) MessageType() string { return "filter_commit" }
func (chainStoreFilterCommit) chainStoreCommand()  {}

// chainStoreSnapshotRequest requests reconciliation and an immutable snapshot.
type chainStoreSnapshotRequest struct{ actor.BaseMessage }

func (chainStoreSnapshotRequest) MessageType() string { return "snapshot" }
func (chainStoreSnapshotRequest) chainStoreCommand()  {}

// chainStoreBarrier acknowledges all commands admitted before it.
type chainStoreBarrier struct{ actor.BaseMessage }

func (chainStoreBarrier) MessageType() string { return "barrier" }
func (chainStoreBarrier) chainStoreCommand()  {}

// chainStoreReply carries the durable mutation result or query snapshot.
type chainStoreReply struct {
	BlockTip      headerfs.BlockStamp
	BlockHeader   wire.BlockHeader
	BlockTipValid bool
	Filter        cfHeaderSnapshot
	Err           error
}

// chainStoreOperations defines the mutations and snapshots owned by the actor.
type chainStoreOperations interface {
	commitBlocks(chainStoreBlockCommit) (chainStoreReply, error)
	rollback(chainStoreRollback) (chainStoreReply, error)
	reorg(chainStoreReorg) (chainStoreReply, error)
	commitFilters(chainStoreFilterCommit) (chainStoreReply, error)
	snapshot() (chainStoreReply, error)
}

// chainStoreBehavior serializes every operation through one actor mailbox.
type chainStoreBehavior struct {
	ops chainStoreOperations
}

func (b *chainStoreBehavior) Receive(_ context.Context,
	message chainStoreMessage) fn.Result[chainStoreReply] {

	var reply chainStoreReply
	var err error
	switch m := message.(type) {
	case chainStoreBlockCommit:
		reply, err = b.ops.commitBlocks(m)
	case chainStoreRollback:
		reply, err = b.ops.rollback(m)
	case chainStoreReorg:
		reply, err = b.ops.reorg(m)
	case chainStoreFilterCommit:
		reply, err = b.ops.commitFilters(m)
	case chainStoreSnapshotRequest:
		reply, err = b.ops.snapshot()
	case chainStoreBarrier:
		return fn.Ok(chainStoreReply{})
	default:
		return fn.Err[chainStoreReply](
			errors.New("unknown chain store command"),
		)
	}
	// Preserve verified durable progress even when the mutation returns an
	// error, so the handler can reconcile without discarding the outcome.
	reply.Err = err
	return fn.Ok(reply)
}

// chainStoreClient tracks admissions so shutdown can drain accepted commands
// before the actor system is stopped and the underlying stores are closed.
type chainStoreClient struct {
	system *actor.ActorSystem
	ref    actor.ActorRef[chainStoreMessage, chainStoreReply]
	mu     sync.Mutex
	closed bool
	asks   sync.WaitGroup

	// rejected counts TryAsk submissions that the mailbox refused. The
	// mailbox capacity makes that impossible for the two clients, so a
	// nonzero count means a command, and the headers it carried, were
	// dropped.
	rejected atomic.Int64
}

// chainStoreMailboxCapacity is the mailbox size of the chain store actor. The
// actor has exactly two clients: blockHandler submits with TryAsk and
// cfHandler submits with Ask, and each has at most one command outstanding.
// At most two envelopes therefore exist at any time, so the mailbox never
// holds more than two and TryAsk can never see it full. This matters because
// blockHandler has already disarmed its header deadline when it submits, and
// a dropped command would lose the headers it carries.
const chainStoreMailboxCapacity = 2

// newChainStoreClient registers a private actor with a bounded mailbox.
func newChainStoreClient(ops chainStoreOperations) (*chainStoreClient, error) {
	system := actor.NewActorSystemWithConfig(actor.SystemConfig{
		MailboxCapacity: chainStoreMailboxCapacity,
	})
	key := actor.NewServiceKey[chainStoreMessage, chainStoreReply](
		"chain-store",
	)
	ref, err := key.Spawn(
		system, "chain-store", &chainStoreBehavior{ops: ops},
	)
	if err != nil {
		_ = system.Shutdown()
		return nil, err
	}
	return &chainStoreClient{system: system, ref: ref}, nil
}

// ask admits one command and waits for its response or the caller's context.
// A returned context error does not imply that an admitted command was undone.
func (c *chainStoreClient) ask(ctx context.Context,
	message chainStoreMessage) fn.Result[chainStoreReply] {

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return fn.Err[chainStoreReply](ErrShuttingDown)
	}
	c.asks.Add(1)
	c.mu.Unlock()
	defer c.asks.Done()

	return c.ref.Ask(ctx, message).Await(ctx)
}

// chainStoreResult carries an asynchronous actor reply to the block handler.
type chainStoreResult struct {
	reply chainStoreReply
	err   error

	// retry marks a timer wakeup that asks the handler to resubmit its
	// pending reconciliation snapshot. It carries no actor reply.
	retry bool
}

// tryAsk sends without parking the block handler on mailbox admission.
func (c *chainStoreClient) tryAsk(
	ctx context.Context,
	message chainStoreMessage,
	result chan<- chainStoreResult,
) {

	c.ref.TryAsk(ctx, message).
		OnComplete(ctx, func(r fn.Result[chainStoreReply]) {
			reply, err := r.Unpack()
			if errors.Is(err, actor.ErrMailboxFull) {
				c.rejected.Add(1)
			}
			select {
			case result <- chainStoreResult{reply: reply, err: err}:
			case <-ctx.Done():
			}
		})
}

// stop closes admission and waits for a FIFO barrier before stopping the
// actor. Callers must first unblock any notification sends and cancel asks.
func (c *chainStoreClient) stop() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	c.asks.Wait()
	result := c.ref.Ask(context.Background(), chainStoreBarrier{}).
		Await(context.Background())
	if result.IsErr() {
		return result.Err()
	}
	return c.system.ShutdownAndWait(context.Background())
}
