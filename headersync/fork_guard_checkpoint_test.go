package headersync

// The tests in this file drive the serial fork guard in Driver.locateFork
// through the driver's own event interface (HeadersReceived) with a mined
// simnet branch, and assert on the effects the driver asks its executor to
// run. The store is a map-only fake.

import (
	"testing"
	"time"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/chainsync"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/lightninglabs/neutrino/headerlist"
	"github.com/stretchr/testify/require"
)

// checkpointPeer is the peer that sends the competing branch. It holds the
// tip-following lease, as the sync peer does.
const checkpointPeer = "peer"

// guardStore is an in-memory headerfs.BlockHeaderStore that holds the durable
// header chain the driver reads ancestors from.
type guardStore struct {
	byHash map[chainhash.Hash]guardStored
	byHei  map[uint32]chainhash.Hash
	tipH   uint32
}

// guardStored is one durable header and its height.
type guardStored struct {
	header *wire.BlockHeader
	height uint32
}

// ChainTip returns the durable tip. The store is written only by the driver's
// executor, which this test does not run, so the tip is the seeded one.
func (s *guardStore) ChainTip() (*wire.BlockHeader, uint32, error) {
	stored, ok := s.byHash[s.byHei[s.tipH]]
	if !ok {
		return nil, 0, headerfs.ErrHeightNotFound
	}

	return stored.header, s.tipH, nil
}

// LatestBlockLocator returns nothing: the driver builds no locator here.
func (s *guardStore) LatestBlockLocator() (blockchain.BlockLocator, error) {
	return nil, nil
}

// FetchHeaderByHeight returns the durable header at a height.
func (s *guardStore) FetchHeaderByHeight(height uint32) (*wire.BlockHeader,
	error) {

	hash, ok := s.byHei[height]
	if !ok {
		return nil, headerfs.ErrHeightNotFound
	}

	return s.byHash[hash].header, nil
}

// FetchHeaderAncestors returns nothing: a simnet retarget never leaves the
// validation cache in this test.
func (s *guardStore) FetchHeaderAncestors(uint32, *chainhash.Hash) (
	[]wire.BlockHeader, uint32, error) {

	return nil, 0, nil
}

// HeightFromHash returns the height of a durable header.
func (s *guardStore) HeightFromHash(hash *chainhash.Hash) (uint32, error) {
	stored, ok := s.byHash[*hash]
	if !ok {
		return 0, headerfs.ErrHeightNotFound
	}

	return stored.height, nil
}

// FetchHeader returns a durable header and its height.
func (s *guardStore) FetchHeader(hash *chainhash.Hash) (*wire.BlockHeader,
	uint32, error) {

	stored, ok := s.byHash[*hash]
	if !ok {
		return nil, 0, headerfs.ErrHeightNotFound
	}

	return stored.header, stored.height, nil
}

// WriteHeaders accepts nothing: the test asserts on the driver's plan, and
// the executor that would write it never runs.
func (s *guardStore) WriteHeaders(...headerfs.BlockHeader) error { return nil }

// RollbackBlockHeaders is unused here.
func (s *guardStore) RollbackBlockHeaders(uint32) (*headerfs.BlockStamp,
	error) {

	return nil, nil
}

// RollbackLastBlock is unused here.
func (s *guardStore) RollbackLastBlock() (*headerfs.BlockStamp, error) {
	return nil, nil
}

// newGuardStore seeds the durable store with the first durable headers of the
// chain, so the store tip sits at height durable-1.
func newGuardStore(chain []*wire.BlockHeader, durable int) *guardStore {
	store := &guardStore{
		byHash: make(map[chainhash.Hash]guardStored),
		byHei:  make(map[uint32]chainhash.Hash),
	}
	for height, header := range chain[:durable] {
		hash := header.BlockHash()
		store.byHash[hash] = guardStored{
			header: header,
			height: uint32(height),
		}
		store.byHei[uint32(height)] = hash
	}
	store.tipH = uint32(durable - 1)

	return store
}

// guardChain mines a connected simnet chain in which element i sits at height
// i, starting from the simnet genesis header at height 0.
func guardChain(t *testing.T, count int) []*wire.BlockHeader {
	t.Helper()

	params := &chaincfg.SimNetParams
	chain := make([]*wire.BlockHeader, 0, count)
	genesis := params.GenesisBlock.Header
	chain = append(chain, &genesis)

	parent := genesis
	now := parent.Timestamp.Add(time.Second)
	for i := 1; i < count; i++ {
		header := &wire.BlockHeader{
			Version:   4,
			PrevBlock: parent.BlockHash(),
			Bits:      params.PowLimitBits,
			Timestamp: now,
		}
		guardMine(t, params, header)

		chain = append(chain, header)
		parent = *header
		now = now.Add(time.Second)
	}

	return chain
}

// guardBranch mines count headers on top of parent. Their timestamps are
// offset from the honest chain's, so a branch header never equals the honest
// header at the same height.
func guardBranch(t *testing.T, params *chaincfg.Params,
	parent *wire.BlockHeader, count int) []*wire.BlockHeader {

	t.Helper()

	branch := make([]*wire.BlockHeader, 0, count)
	prev := *parent
	now := parent.Timestamp.Add(time.Minute)
	for i := 0; i < count; i++ {
		header := &wire.BlockHeader{
			Version:   4,
			PrevBlock: prev.BlockHash(),
			Bits:      params.PowLimitBits,
			Timestamp: now,
		}
		guardMine(t, params, header)

		branch = append(branch, header)
		prev = *header
		now = now.Add(time.Second)
	}

	return branch
}

// guardMine grinds the nonce until the header passes the header sanity check,
// so the only thing that can reject the branch is the checkpoint guard.
func guardMine(t *testing.T, params *chaincfg.Params,
	header *wire.BlockHeader) {

	t.Helper()

	timeSource := blockchain.NewMedianTime()
	for {
		err := blockchain.CheckBlockHeaderSanity(
			header, params.PowLimit, timeSource, 0,
		)
		if err == nil {
			return
		}

		header.Nonce++
		require.Less(t, header.Nonce, uint32(1<<31))
	}
}

// guardDriver builds a driver whose validation cache and durable store both
// end at height durable-1, under the given chain parameters, and gives the
// test peer the tip-following lease.
func guardDriver(t *testing.T, params *chaincfg.Params,
	chain []*wire.BlockHeader, durable int) *Driver {

	t.Helper()

	retarget, err := chainsync.NewRetarget(*params)
	require.NoError(t, err)

	timeSource := blockchain.NewMedianTime()
	store := newGuardStore(chain, durable)
	tip := NewTipState(TipConfig{
		Params:              params,
		BlocksPerRetarget:   retarget.BlocksPerRetarget,
		MinRetargetTimespan: retarget.MinRetargetTimespan,
		MaxRetargetTimespan: retarget.MaxRetargetTimespan,
		TimeSource: func() blockchain.MedianTimeSource {
			return timeSource
		},
		BlockHeaders: func() headerfs.BlockHeaderStore {
			return store
		},
	})

	// Extend, rather than PushBackForTest, so the checkpoint cursor
	// follows the tip the way production moves it.
	nodes := make([]headerlist.Node, 0, durable)
	for height, header := range chain[:durable] {
		nodes = append(nodes, headerlist.Node{
			Header: *header,
			Height: int32(height),
		})
	}
	tip.Extend(nodes)

	fsm := NewHeaderSyncFSM(0, chain[0].BlockHash(), DefaultConfig())
	runtime := &Runtime{manager: NewManager(fsm)}

	driver := newDriver(runtime, NewSession(), tip, DriverConfig{
		RequestTimeout: func() time.Duration {
			return 30 * time.Second
		},
	})

	_, err = driver.ProcessEvent(
		t.Context(), FollowTip{PeerID: checkpointPeer}, View{},
	)
	require.NoError(t, err)

	return driver
}

// guardHeaders feeds one headers message from the test peer to the driver, as
// the block manager does, and returns the effects the driver planned.
func guardHeaders(t *testing.T, driver *Driver,
	headers []*wire.BlockHeader) []DriverEffect {

	t.Helper()

	effects, err := driver.ProcessEvent(t.Context(), HeadersReceived{
		PeerID:        checkpointPeer,
		Headers:       headers,
		HeadersSynced: true,
	}, View{})
	require.NoError(t, err)

	return effects
}

// guardReorgs returns the reorg commits among the effects.
func guardReorgs(effects []DriverEffect) []CommitReorg {
	var reorgs []CommitReorg
	for _, effect := range effects {
		if reorg, ok := effect.(CommitReorg); ok {
			reorgs = append(reorgs, reorg)
		}
	}

	return reorgs
}

// requireKeepsCheckpoint asserts that the driver never asks the store to write
// a header at the checkpoint's height that is not the checkpoint header.
//
// This is the property the fork guard exists for. A CommitReorg without a
// mismatch is submitted to chainstore.Reorg with its headers, and that store
// compares work above the fork and never consults a checkpoint
// (chainstore/rollback.go), so a planned write at the checkpoint height is a
// write that replaces the checkpoint. A CommitReorg with Mismatch set only
// has its work compared and writes nothing, so it is allowed.
func requireKeepsCheckpoint(t *testing.T, checkpoint chaincfg.Checkpoint,
	effects []DriverEffect) {

	t.Helper()

	for _, reorg := range guardReorgs(effects) {
		if reorg.Mismatch != 0 {
			continue
		}

		for _, header := range reorg.Headers {
			t.Logf("planned write height=%d hash=%s checkpoint "+
				"%d/%s", header.Height, header.BlockHash(),
				checkpoint.Height, checkpoint.Hash)

			require.False(t,
				header.Height == uint32(checkpoint.Height) &&
					header.BlockHash() != *checkpoint.Hash,
				"driver planned to write a branch header at "+
					"checkpoint height %d",
				checkpoint.Height)
		}
	}
}

// TestSerialForkGuardKeepsCheckpointAtTip checks that a peer cannot use the
// serial fork path to replace the header at a checkpoint height the tip is
// sitting on. The reorg planner only compares the checkpoint above the tip,
// so the guard has to count the checkpoint at the tip itself.
func TestSerialForkGuardKeepsCheckpointAtTip(t *testing.T) {
	t.Parallel()

	chain := guardChain(t, 8)
	atOne := chain[1].BlockHash()
	atThree := chain[3].BlockHash()
	atFive := chain[5].BlockHash()

	// The tip is the last checkpoint, so the reorg planner has no
	// checkpoint above the tip to compare the branch against.
	lastIsTip := chaincfg.SimNetParams
	lastIsTip.Checkpoints = []chaincfg.Checkpoint{
		{Height: 1, Hash: &atOne},
		{Height: 3, Hash: &atThree},
	}

	driver := guardDriver(t, &lastIsTip, chain, 4)
	require.Equal(t, int32(3), driver.tip.Tip().Height)
	require.Nil(t, driver.tip.NextCheckpoint())

	// The peer forks at height 2 and runs past the tip, so its own header
	// lands at the checkpoint height 3.
	branch := guardBranch(t, &lastIsTip, chain[2], 3)
	require.NotEqual(t, atThree, branch[0].BlockHash())
	effects := guardHeaders(t, driver, branch)
	requireKeepsCheckpoint(t, lastIsTip.Checkpoints[1], effects)
	requireDisconnected(t, effects)

	// A checkpoint above the tip changes nothing: the planner looks at
	// height 5 only, while the branch rewrites height 3 and stops at 4.
	nextBeyond := chaincfg.SimNetParams
	nextBeyond.Checkpoints = []chaincfg.Checkpoint{
		{Height: 1, Hash: &atOne},
		{Height: 3, Hash: &atThree},
		{Height: 5, Hash: &atFive},
	}

	driver = guardDriver(t, &nextBeyond, chain, 4)
	require.Equal(t, int32(3), driver.tip.Tip().Height)
	require.Equal(t, int32(5), driver.tip.NextCheckpoint().Height)

	branch = guardBranch(t, &nextBeyond, chain[2], 2)
	require.NotEqual(t, atThree, branch[0].BlockHash())
	effects = guardHeaders(t, driver, branch)
	requireKeepsCheckpoint(t, nextBeyond.Checkpoints[1], effects)
	requireDisconnected(t, effects)
}

// requireDisconnected asserts that the driver disconnected the test peer, so
// a guard that rejects the branch is what kept the checkpoint, rather than
// the branch never being considered.
func requireDisconnected(t *testing.T, effects []DriverEffect) {
	t.Helper()

	require.Contains(
		t, effects, DriverEffect(DisconnectTipPeer{PeerID: checkpointPeer}),
	)
}
