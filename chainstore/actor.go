package chainstore

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/lightningnetwork/lnd/actor"
	"github.com/lightningnetwork/lnd/fn/v2"
)

// ErrShuttingDown is returned for a command submitted after Client.Stop.
var ErrShuttingDown = errors.New("neutrino shutting down")

// MailboxCapacity is the mailbox size of the actor. There are exactly two
// clients: the block handler submits with TryAsk and the filter handler
// submits with Ask, and each has at most one command outstanding. The mailbox
// therefore never holds more than two, and TryAsk can never find it full. This
// matters because the block handler has already disarmed its header deadline
// when it submits, so a refused command would lose the headers it carries.
const MailboxCapacity = 2

// Operations is what the actor executes, one command at a time. Store is the
// production implementation.
type Operations interface {
	// CommitBlocks appends a block header batch.
	CommitBlocks(BlockCommit) (Reply, error)

	// Rollback removes block and filter headers down to a height.
	Rollback(Rollback) (Reply, error)

	// Reorg rolls back and appends a replacement branch.
	Reorg(Reorg) (Reply, error)

	// CommitFilters appends a filter header batch.
	CommitFilters(FilterCommit) (Reply, error)

	// Snapshot reconciles publication and reads both tips.
	Snapshot() (Reply, error)
}

// behavior adapts Operations to the actor's message interface.
type behavior struct {
	ops Operations
}

// Receive executes one command.
func (b *behavior) Receive(_ context.Context,
	message Message) fn.Result[Reply] {

	var (
		reply Reply
		err   error
	)
	switch m := message.(type) {
	case BlockCommit:
		reply, err = b.ops.CommitBlocks(m)

	case Rollback:
		reply, err = b.ops.Rollback(m)

	case Reorg:
		reply, err = b.ops.Reorg(m)

	case FilterCommit:
		reply, err = b.ops.CommitFilters(m)

	case SnapshotRequest:
		reply, err = b.ops.Snapshot()

	case barrier:
		return fn.Ok(Reply{})

	default:
		return fn.Err[Reply](errors.New("unknown chain store command"))
	}

	// A failed mutation can still have made durable progress, so the error
	// travels inside the reply, next to whatever tip we verified.
	reply.Err = err

	return fn.Ok(reply)
}

// Client submits commands to the chain store actor. It tracks admissions so
// that shutdown can drain accepted commands before the stores are closed.
type Client struct {
	system *actor.ActorSystem
	ref    actor.ActorRef[Message, Reply]

	// mu guards closed against the WaitGroup, so that no Ask can start
	// once Stop has begun waiting.
	mu     sync.Mutex
	closed bool
	asks   sync.WaitGroup

	// rejected counts TryAsk submissions that the mailbox refused. The
	// mailbox capacity makes that impossible for the two clients, so a
	// nonzero count means a command, and the headers it carried, were
	// dropped.
	rejected atomic.Int64
}

// NewClient starts a private actor that runs ops.
func NewClient(ops Operations) (*Client, error) {
	system := actor.NewActorSystemWithConfig(actor.SystemConfig{
		MailboxCapacity: MailboxCapacity,
	})
	key := actor.NewServiceKey[Message, Reply]("chain-store")
	ref, err := key.Spawn(system, "chain-store", &behavior{ops: ops})
	if err != nil {
		_ = system.Shutdown()

		return nil, err
	}

	return &Client{system: system, ref: ref}, nil
}

// Ask admits one command and waits for its reply or the caller's context. A
// context error doesn't mean an admitted command was undone: it may still run.
func (c *Client) Ask(ctx context.Context, message Message) fn.Result[Reply] {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()

		return fn.Err[Reply](ErrShuttingDown)
	}
	c.asks.Add(1)
	c.mu.Unlock()
	defer c.asks.Done()

	return c.ref.Ask(ctx, message).Await(ctx)
}

// TryAsk submits without parking the caller on mailbox admission, and delivers
// the outcome to result. The block handler uses this, since it must keep
// serving peers while a commit runs.
func (c *Client) TryAsk(ctx context.Context, message Message,
	result chan<- Result) {

	c.ref.TryAsk(ctx, message).OnComplete(ctx, func(r fn.Result[Reply]) {
		reply, err := r.Unpack()
		if errors.Is(err, actor.ErrMailboxFull) {
			c.rejected.Add(1)
		}

		select {
		case result <- Result{Reply: reply, Err: err}:
		case <-ctx.Done():
		}
	})
}

// Rejected returns how many TryAsk submissions the mailbox refused.
func (c *Client) Rejected() int64 {
	return c.rejected.Load()
}

// Stop closes admission, waits for a barrier to pass through the mailbox, and
// stops the actor. Callers must first unblock any notification sends and
// cancel outstanding asks, or the barrier never gets a turn.
func (c *Client) Stop() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()

	c.asks.Wait()

	result := c.ref.Ask(context.Background(), barrier{}).
		Await(context.Background())
	if result.IsErr() {
		return result.Err()
	}

	return c.system.ShutdownAndWait(context.Background())
}
