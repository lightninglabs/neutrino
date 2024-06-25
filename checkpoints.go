package neutrino

import (
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
)

// BlockHeaderCheckpoints manages header checkpoints for efficient
// header validation by providing methods to fetch and find checkpoints based
// on block height.
type BlockHeaderCheckpoints struct {
	// checkpoints is a slice of Checkpoint structs from the blockchain's
	// parameters.
	checkpoints []chaincfg.Checkpoint

	// genesishash is the hash of the genesis block for the blockchain,
	// serving as the initial checkpoint.
	genesishash *chainhash.Hash
}

// NewBlockHeaderCheckpoints creates a new BlockHeaderCheckpoints instance
// using the provided blockchain parameters, which include the genesis hash
// and predefined checkpoints.
func NewBlockHeaderCheckpoints(params chaincfg.Params) *BlockHeaderCheckpoints {
	return &BlockHeaderCheckpoints{
		checkpoints: params.Checkpoints,
		genesishash: params.GenesisHash,
	}
}

// FetchCheckpoint returns the height and hash of the checkpoint at the given
// index. Special indices -1 and -2 return no checkpoint and the genesis
// block respectively.
func (b *BlockHeaderCheckpoints) FetchCheckpoint(idx int32) (uint32,
	*chainhash.Hash) {

	// Handle special index values for no checkpoint and genesis block.
	if idx == -1 {
		return 0, nil
	}
	if idx == -2 {
		return 0, b.genesishash
	}

	// Return the checkpoint corresponding to the provided index.
	checkpoint := b.checkpoints[idx]

	return uint32(checkpoint.Height), checkpoint.Hash
}

// FindNextHeaderCheckpoint finds the next checkpoint after the given height,
// returning its index, height, and hash. If no next checkpoint is found,
// it returns -1 for the index and nil for the hash.
func (b *BlockHeaderCheckpoints) FindNextHeaderCheckpoint(height uint32) (
	int32, uint32, *chainhash.Hash) {

	// TODO: handle no checkpoints case.

	checkpoints := b.checkpoints

	// If height is after the last checkpoint, return no next checkpoint.
	finalCheckpoint := &checkpoints[len(checkpoints)-1]
	if height >= uint32(finalCheckpoint.Height) {
		return -1, 0, nil
	}

	// Iterate to find the next checkpoint after the given height.
	nextCheckpoint := finalCheckpoint
	var index int32
	for i := int32(len(checkpoints) - 2); i >= 0; i-- {
		if height >= uint32(checkpoints[i].Height) {
			break
		}
		nextCheckpoint = &checkpoints[i]
		index = i
	}

	return index, uint32(nextCheckpoint.Height), nextCheckpoint.Hash
}

// FindPreviousHeaderCheckpoint finds the latest checkpoint before the given
// height, returning its index, height, and hash. If the height is before any
// checkpoints, it returns the genesis block as the previous checkpoint.
func (b *BlockHeaderCheckpoints) FindPreviousHeaderCheckpoint(height uint32) (
	int32, uint32, *chainhash.Hash) {

	// Default to genesis block if no previous checkpoint is found.
	prevCheckpointIndex := int32(-1)
	prevCheckpointHeight := uint32(0)
	prevCheckpointHash := b.genesishash

	// Iterate through checkpoints to find the previous checkpoint.
	checkpoints := b.checkpoints
	for i, checkpoint := range checkpoints {
		if uint32(checkpoint.Height) < height {
			prevCheckpointIndex = int32(i)
			prevCheckpointHeight = uint32(checkpoint.Height)
			prevCheckpointHash = checkpoint.Hash
		} else {
			// Checkpoints are sorted, so break on the first higher
			// height.
			break
		}
	}

	return prevCheckpointIndex, prevCheckpointHeight, prevCheckpointHash
}

// Len returns the number of predefined checkpoints.
func (b *BlockHeaderCheckpoints) Len() int32 {
	return int32(len(b.checkpoints))
}
