package chainstore

import (
	"math/big"

	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/lightningnetwork/lnd/actor"
)

// Message is a command processed by the chain store actor. The interface is
// sealed: only the commands in this file can be sent.
type Message interface {
	actor.Message
	chainStoreCommand()
}

// BlockCommit appends a batch of block headers. The batch was validated
// against ExpectedParent, so it's refused if the durable tip has moved.
type BlockCommit struct {
	actor.BaseMessage

	// ExpectedParent is the committed tip the batch was prepared against.
	ExpectedParent headerfs.BlockStamp

	// Headers is the batch to append, in height order.
	Headers []headerfs.BlockHeader
}

// MessageType returns the name of the command.
func (BlockCommit) MessageType() string { return "block_commit" }
func (BlockCommit) chainStoreCommand()  {}

// Rollback removes block headers and their filter headers down to Height.
type Rollback struct {
	actor.BaseMessage

	// ExpectedTip is the durable tip the rollback was planned against.
	ExpectedTip headerfs.BlockStamp

	// Height is the height of the header that becomes the new tip.
	Height uint32
}

// MessageType returns the name of the command.
func (Rollback) MessageType() string { return "rollback" }
func (Rollback) chainStoreCommand()  {}

// Reorg rolls back to ForkHeight and appends a replacement branch within one
// actor turn, so that no filter commit can land between the two phases.
type Reorg struct {
	actor.BaseMessage

	// ExpectedTip is the durable tip the reorg was planned against.
	ExpectedTip headerfs.BlockStamp

	// ForkHeight is the height of the last header both branches share.
	ForkHeight uint32

	// Headers is the replacement branch, starting at ForkHeight+1.
	Headers []headerfs.BlockHeader

	// Work is the proof of work of the whole branch the peer sent, which
	// can extend past Headers. The branch must beat the durable chain
	// above the fork, or nothing changes.
	Work *big.Int

	// CompareOnly asks only for the work verdict. The caller found a
	// problem with the branch that only matters if it has the most work.
	CompareOnly bool
}

// MessageType returns the name of the command.
func (Reorg) MessageType() string { return "reorg" }
func (Reorg) chainStoreCommand()  {}

// FilterCommit appends a verified batch of filter headers.
type FilterCommit struct {
	actor.BaseMessage

	// Messages is the batch, which must chain from the durable filter tip.
	Messages []*wire.MsgCFHeaders
}

// MessageType returns the name of the command.
func (FilterCommit) MessageType() string { return "filter_commit" }
func (FilterCommit) chainStoreCommand()  {}

// SnapshotRequest finishes any retained rollback, publishes any durable but
// unpublished filter headers, and returns the resulting tips.
type SnapshotRequest struct{ actor.BaseMessage }

// MessageType returns the name of the command.
func (SnapshotRequest) MessageType() string { return "snapshot" }
func (SnapshotRequest) chainStoreCommand()  {}

// barrier acknowledges every command admitted before it. Client.Stop uses it
// to drain the mailbox.
type barrier struct{ actor.BaseMessage }

// MessageType returns the name of the command.
func (barrier) MessageType() string { return "barrier" }
func (barrier) chainStoreCommand()  {}

// FilterSnapshot is a consistent view of both tips, taken on the actor, along
// with the stop block of the next getcfheaders query.
type FilterSnapshot struct {
	// FilterTip is the durable filter header at the tip.
	FilterTip chainhash.Hash

	// FilterHeight is the height of FilterTip.
	FilterHeight uint32

	// BlockHash is the durable block header tip.
	BlockHash chainhash.Hash

	// BlockHeight is the height of BlockHash.
	BlockHeight uint32

	// StopHash is the block the next query should stop at. It's the block
	// tip, or the block one full message past the filter tip if that's
	// lower.
	StopHash chainhash.Hash

	// StopHeight is the height of StopHash.
	StopHeight uint32
}

// ReorgVerdict is the outcome of comparing a branch's work with the durable
// chain above the fork.
type ReorgVerdict uint8

const (
	// ReorgNotRejected means the branch has more work, or the command
	// wasn't a reorg.
	ReorgNotRejected ReorgVerdict = iota

	// ReorgLessWork means the branch has less work than the durable chain.
	ReorgLessWork

	// ReorgEqualWork means the branch ties the durable chain, so the
	// durable chain stays.
	ReorgEqualWork
)

// Reply is the outcome of one command.
type Reply struct {
	// Verdict is the work comparison of a reorg. Nothing changed if it
	// rejects the branch.
	Verdict ReorgVerdict

	// BlockTip is the verified durable block tip, when BlockTipValid is
	// set.
	BlockTip headerfs.BlockStamp

	// BlockHeader is the header at BlockTip. Only a snapshot sets it.
	BlockHeader wire.BlockHeader

	// BlockTipValid reports that BlockTip was verified against the store.
	// It can be set alongside Err: a failed write that left a verified
	// durable prefix reports both.
	BlockTipValid bool

	// Filter is the filter side of a snapshot or a filter commit.
	Filter FilterSnapshot

	// Err is the error the command returned. The mutation may have applied
	// in full or in part, so the caller must reconcile with a snapshot
	// before it trusts its own view of the chain.
	Err error
}

// Result carries an asynchronous reply back to the submitter's event loop.
type Result struct {
	// Reply is the actor's reply.
	Reply Reply

	// Err is set if the command never reached the actor or the wait for
	// its reply ended early.
	Err error

	// Retry marks a timer wakeup rather than an actor reply. It tells the
	// submitter to resend its pending snapshot request.
	Retry bool
}
