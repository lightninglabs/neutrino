package chainstore

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"

	"github.com/lightningnetwork/lnd/actor"
	"github.com/stretchr/testify/require"
)

// blockedChainStoreOps holds a mutation after it enters the actor.
type blockedChainStoreOps struct {
	entered chan struct{}
	release chan struct{}
}

func (s *blockedChainStoreOps) CommitBlocks(
	BlockCommit) (Reply, error) {

	close(s.entered)
	<-s.release
	return Reply{}, nil
}

func (*blockedChainStoreOps) Rollback(
	Rollback) (Reply, error) {

	return Reply{}, nil
}

func (*blockedChainStoreOps) Reorg(
	Reorg) (Reply, error) {

	return Reply{}, nil
}

func (*blockedChainStoreOps) CommitFilters(
	FilterCommit) (Reply, error) {

	return Reply{}, nil
}

func (*blockedChainStoreOps) Snapshot() (Reply, error) {
	return Reply{}, nil
}

// TestChainStoreShutdownWaitsForAdmittedMutation verifies that canceling an
// ask does not let shutdown return while the actor still mutates its store.
func TestChainStoreShutdownWaitsForAdmittedMutation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ops := &blockedChainStoreOps{
			entered: make(chan struct{}),
			release: make(chan struct{}),
		}
		client, err := NewClient(ops)
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(t.Context())
		askDone := make(chan error, 1)
		go func() {
			result := client.Ask(ctx, BlockCommit{})
			askDone <- result.Err()
		}()
		<-ops.entered
		cancel()
		require.ErrorIs(t, <-askDone, context.Canceled)
		stopDone := make(chan error, 1)
		go func() { stopDone <- client.Stop() }()
		synctest.Wait()
		select {
		case err := <-stopDone:
			t.Fatalf("shutdown returned before mutation: %v",
				err)
		default:
		}
		close(ops.release)
		require.NoError(t, <-stopDone)
		result := client.Ask(
			t.Context(), SnapshotRequest{},
		)
		require.ErrorIs(t, result.Err(), ErrShuttingDown)
	})
}

// TestChainStoreAdmissionNonblocking verifies a full mailbox reports an
// error without parking control work or allowing shutdown to skip the write.
func TestChainStoreAdmissionNonblocking(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ops := &blockedChainStoreOps{
			entered: make(chan struct{}),
			release: make(chan struct{}),
		}
		client, err := NewClient(ops)
		require.NoError(t, err)
		first := make(chan Result, 1)
		client.TryAsk(t.Context(), BlockCommit{}, first)
		<-ops.entered

		// Fill the mailbox behind the running turn.
		queued := make(
			[]chan Result, MailboxCapacity,
		)
		for i := range queued {
			queued[i] = make(chan Result, 1)
			client.TryAsk(
				t.Context(), SnapshotRequest{},
				queued[i],
			)
		}
		full := make(chan Result, 1)
		client.TryAsk(t.Context(), SnapshotRequest{}, full)
		require.ErrorIs(t, (<-full).Err, actor.ErrMailboxFull)
		control := make(chan struct{}, 1)
		control <- struct{}{}
		<-control
		stopDone := make(chan error, 1)
		go func() { stopDone <- client.Stop() }()
		synctest.Wait()
		select {
		case err := <-stopDone:
			t.Fatalf(
				"shutdown returned before store operation: %v",
				err,
			)
		default:
		}
		close(ops.release)
		require.NoError(t, (<-first).Err)
		for _, result := range queued {
			require.NoError(t, (<-result).Err)
		}
		require.NoError(t, <-stopDone)
	})
}

// TestChainStoreTryAskBesideBlockingAsk is the regression for dropped header
// commits. blockHandler submits with TryAsk and cfHandler with Ask, and each
// has one command outstanding. While the actor runs one turn, the other
// client's command waits in the mailbox, and the first client's result can be
// delivered before the actor dequeues that command. The next TryAsk must still
// be admitted, or the headers it carries are lost.
func TestChainStoreTryAskBesideBlockingAsk(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ops := &blockedChainStoreOps{
			entered: make(chan struct{}),
			release: make(chan struct{}),
		}
		client, err := NewClient(ops)
		require.NoError(t, err)

		// A turn is running, as when blockHandler's last commit is
		// still executing.
		running := make(chan Result, 1)
		client.TryAsk(t.Context(), BlockCommit{}, running)
		<-ops.entered

		// cfHandler's blocking ask occupies a mailbox slot.
		cf := make(chan error, 1)
		go func() {
			cf <- client.Ask(
				t.Context(), SnapshotRequest{},
			).Err()
		}()
		synctest.Wait()

		// blockHandler's next TryAsk must be admitted beside it.
		next := make(chan Result, 1)
		client.TryAsk(t.Context(), SnapshotRequest{}, next)
		synctest.Wait()
		select {
		case result := <-next:
			t.Fatalf("TryAsk was not admitted: %v", result.Err)
		default:
		}

		close(ops.release)
		require.NoError(t, (<-running).Err)
		require.NoError(t, <-cf)
		require.NoError(t, (<-next).Err)
		require.NoError(t, client.Stop())
	})
}

// TestChainStoreAdmissionReleasesSlot checks completed messages make the
// bounded mailbox available for another nonblocking request.
func TestChainStoreAdmissionReleasesSlot(t *testing.T) {
	client, err := NewClient(&blockedChainStoreOps{})
	require.NoError(t, err)
	results := make(chan Result, 1)
	client.TryAsk(t.Context(), SnapshotRequest{}, results)
	require.NoError(t, (<-results).Err)
	client.TryAsk(t.Context(), SnapshotRequest{}, results)
	require.NoError(t, (<-results).Err)
	require.NoError(t, client.Stop())
}

// orderedChainStoreOps records when a snapshot begins after a blocked write.
type orderedChainStoreOps struct {
	blockedChainStoreOps
	snapshotStarted chan struct{}
}

func (s *orderedChainStoreOps) Snapshot() (Reply, error) {
	close(s.snapshotStarted)
	return Reply{}, nil
}

// TestChainStoreSerializesMutationAndSnapshot checks that a snapshot cannot
// observe an operation while its store write is still in progress.
func TestChainStoreSerializesMutationAndSnapshot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ops := &orderedChainStoreOps{
			blockedChainStoreOps: blockedChainStoreOps{
				entered: make(chan struct{}),
				release: make(chan struct{}),
			},
			snapshotStarted: make(chan struct{}),
		}
		client, err := NewClient(ops)
		require.NoError(t, err)
		mutation := make(chan error, 1)
		go func() {
			result := client.Ask(
				t.Context(), BlockCommit{},
			)
			mutation <- result.Err()
		}()
		<-ops.entered
		snapshot := make(chan error, 1)
		go func() {
			result := client.Ask(
				t.Context(), SnapshotRequest{},
			)
			snapshot <- result.Err()
		}()
		synctest.Wait()
		select {
		case <-ops.snapshotStarted:
			t.Fatal("snapshot entered before the mutation finished")
		default:
		}
		close(ops.release)
		require.NoError(t, <-mutation)
		require.NoError(t, <-snapshot)
		<-ops.snapshotStarted
		require.NoError(t, client.Stop())
	})
}

// TestChainStoreReplyPreservesProgress verifies that a store error and a
// verified durable tip are both available to the handler.
func TestChainStoreReplyPreservesProgress(t *testing.T) {
	storeErr := errors.New("lost acknowledgement")
	ops := &progressChainStoreOps{err: storeErr}
	client, err := NewClient(ops)
	require.NoError(t, err)
	result := client.Ask(
		t.Context(), SnapshotRequest{},
	)
	require.False(t, result.IsErr())
	reply := result.UnwrapOr(Reply{})
	require.Equal(t, int32(7), reply.BlockTip.Height)
	require.ErrorIs(t, reply.Err, storeErr)
	require.NoError(t, client.Stop())
}

// progressChainStoreOps returns a known tip together with an operation error.
type progressChainStoreOps struct {
	blockedChainStoreOps
	err error
}

func (s *progressChainStoreOps) Snapshot() (Reply, error) {
	var reply Reply
	reply.BlockTip.Height = 7
	return reply, s.err
}
