package neutrino

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

func (s *blockedChainStoreOps) commitBlocks(
	chainStoreBlockCommit) (chainStoreReply, error) {

	close(s.entered)
	<-s.release
	return chainStoreReply{}, nil
}

func (*blockedChainStoreOps) rollback(
	chainStoreRollback) (chainStoreReply, error) {

	return chainStoreReply{}, nil
}

func (*blockedChainStoreOps) reorg(
	chainStoreReorg) (chainStoreReply, error) {

	return chainStoreReply{}, nil
}

func (*blockedChainStoreOps) commitFilters(
	chainStoreFilterCommit) (chainStoreReply, error) {

	return chainStoreReply{}, nil
}

func (*blockedChainStoreOps) snapshot() (chainStoreReply, error) {
	return chainStoreReply{}, nil
}

// TestChainStoreShutdownWaitsForAdmittedMutation verifies that canceling an
// ask does not let shutdown return while the actor still mutates its store.
func TestChainStoreShutdownWaitsForAdmittedMutation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ops := &blockedChainStoreOps{
			entered: make(chan struct{}),
			release: make(chan struct{}),
		}
		client, err := newChainStoreClient(ops)
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(t.Context())
		askDone := make(chan error, 1)
		go func() {
			result := client.ask(ctx, chainStoreBlockCommit{})
			askDone <- result.Err()
		}()
		<-ops.entered
		cancel()
		require.ErrorIs(t, <-askDone, context.Canceled)
		stopDone := make(chan error, 1)
		go func() { stopDone <- client.stop() }()
		synctest.Wait()
		select {
		case err := <-stopDone:
			t.Fatalf("shutdown returned before mutation: %v",
				err)
		default:
		}
		close(ops.release)
		require.NoError(t, <-stopDone)
		result := client.ask(
			t.Context(), chainStoreSnapshotRequest{},
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
		client, err := newChainStoreClient(ops)
		require.NoError(t, err)
		first := make(chan chainStoreResult, 1)
		client.tryAsk(t.Context(), chainStoreBlockCommit{}, first)
		<-ops.entered

		// Fill the mailbox behind the running turn.
		queued := make(
			[]chan chainStoreResult, chainStoreMailboxCapacity,
		)
		for i := range queued {
			queued[i] = make(chan chainStoreResult, 1)
			client.tryAsk(
				t.Context(), chainStoreSnapshotRequest{},
				queued[i],
			)
		}
		full := make(chan chainStoreResult, 1)
		client.tryAsk(t.Context(), chainStoreSnapshotRequest{}, full)
		require.ErrorIs(t, (<-full).err, actor.ErrMailboxFull)
		control := make(chan struct{}, 1)
		control <- struct{}{}
		<-control
		stopDone := make(chan error, 1)
		go func() { stopDone <- client.stop() }()
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
		require.NoError(t, (<-first).err)
		for _, result := range queued {
			require.NoError(t, (<-result).err)
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
		client, err := newChainStoreClient(ops)
		require.NoError(t, err)

		// A turn is running, as when blockHandler's last commit is
		// still executing.
		running := make(chan chainStoreResult, 1)
		client.tryAsk(t.Context(), chainStoreBlockCommit{}, running)
		<-ops.entered

		// cfHandler's blocking ask occupies a mailbox slot.
		cf := make(chan error, 1)
		go func() {
			cf <- client.ask(
				t.Context(), chainStoreSnapshotRequest{},
			).Err()
		}()
		synctest.Wait()

		// blockHandler's next TryAsk must be admitted beside it.
		next := make(chan chainStoreResult, 1)
		client.tryAsk(t.Context(), chainStoreSnapshotRequest{}, next)
		synctest.Wait()
		select {
		case result := <-next:
			t.Fatalf("TryAsk was not admitted: %v", result.err)
		default:
		}

		close(ops.release)
		require.NoError(t, (<-running).err)
		require.NoError(t, <-cf)
		require.NoError(t, (<-next).err)
		require.NoError(t, client.stop())
	})
}

// TestChainStoreAdmissionReleasesSlot checks completed messages make the
// bounded mailbox available for another nonblocking request.
func TestChainStoreAdmissionReleasesSlot(t *testing.T) {
	client, err := newChainStoreClient(&blockedChainStoreOps{})
	require.NoError(t, err)
	results := make(chan chainStoreResult, 1)
	client.tryAsk(t.Context(), chainStoreSnapshotRequest{}, results)
	require.NoError(t, (<-results).err)
	client.tryAsk(t.Context(), chainStoreSnapshotRequest{}, results)
	require.NoError(t, (<-results).err)
	require.NoError(t, client.stop())
}

// orderedChainStoreOps records when a snapshot begins after a blocked write.
type orderedChainStoreOps struct {
	blockedChainStoreOps
	snapshotStarted chan struct{}
}

func (s *orderedChainStoreOps) snapshot() (chainStoreReply, error) {
	close(s.snapshotStarted)
	return chainStoreReply{}, nil
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
		client, err := newChainStoreClient(ops)
		require.NoError(t, err)
		mutation := make(chan error, 1)
		go func() {
			result := client.ask(
				t.Context(), chainStoreBlockCommit{},
			)
			mutation <- result.Err()
		}()
		<-ops.entered
		snapshot := make(chan error, 1)
		go func() {
			result := client.ask(
				t.Context(), chainStoreSnapshotRequest{},
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
		require.NoError(t, client.stop())
	})
}

// TestChainStoreReplyPreservesProgress verifies that a store error and a
// verified durable tip are both available to the handler.
func TestChainStoreReplyPreservesProgress(t *testing.T) {
	storeErr := errors.New("lost acknowledgement")
	ops := &progressChainStoreOps{err: storeErr}
	client, err := newChainStoreClient(ops)
	require.NoError(t, err)
	result := client.ask(
		t.Context(), chainStoreSnapshotRequest{},
	)
	require.False(t, result.IsErr())
	reply := result.UnwrapOr(chainStoreReply{})
	require.Equal(t, int32(7), reply.BlockTip.Height)
	require.ErrorIs(t, reply.Err, storeErr)
	require.NoError(t, client.stop())
}

// progressChainStoreOps returns a known tip together with an operation error.
type progressChainStoreOps struct {
	blockedChainStoreOps
	err error
}

func (s *progressChainStoreOps) snapshot() (chainStoreReply, error) {
	var reply chainStoreReply
	reply.BlockTip.Height = 7
	return reply, s.err
}
