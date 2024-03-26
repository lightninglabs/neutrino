package neutrino

import (
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
)

type BlockHeaderCheckpoints struct {
	checkpoints []chaincfg.Checkpoint
	genesishash *chainhash.Hash
}

func NewblockHeaderCheckpoints(params chaincfg.Params) *BlockHeaderCheckpoints {
	return &BlockHeaderCheckpoints{
		checkpoints: params.Checkpoints,
		genesishash: params.GenesisHash,
	}
}

func (b *BlockHeaderCheckpoints) FetchCheckpoint(idx int32) (uint32,
	*chainhash.Hash) {

	if idx == -1 {
		return 0, nil
	}

	if idx == -2 {
		return 0, b.genesishash
	}

	checkpoint := b.checkpoints[idx]

	return uint32(checkpoint.Height), checkpoint.Hash
}

func (b *BlockHeaderCheckpoints) FindNextHeaderCheckpoint(height uint32) (
	int32, uint32, *chainhash.Hash) {

	// Check for zero checkpoints.
	checkpoints := b.checkpoints

	// There is no next checkpoint if the height is already after the final
	// checkpoint.
	finalCheckpoint := &checkpoints[len(checkpoints)-1]
	if height >= uint32(finalCheckpoint.Height) {
		return -1, 0, nil
	}

	// Find the next checkpoint.
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

func (b *BlockHeaderCheckpoints) FindPreviousHeaderCheckpoint(height uint32) (
	int32, uint32, *chainhash.Hash) {

	// Initialize to genesis block as the default previous checkpoint.
	prevCheckpointIndex := int32(-1)
	prevCheckpointHeight := uint32(0)
	prevCheckpointHash := b.genesishash

	checkpoints := b.checkpoints
	for i, checkpoint := range checkpoints {
		if uint32(checkpoint.Height) < height {
			// Update to the latest checkpoint lower than height.
			prevCheckpointIndex = int32(i)
			prevCheckpointHeight = uint32(checkpoint.Height)
			prevCheckpointHash = checkpoint.Hash
		} else {
			// Since checkpoints are assumed to be sorted,
			// break on the first checkpoint greater than or equal to height.
			break
		}
	}

	return prevCheckpointIndex, prevCheckpointHeight, prevCheckpointHash
}

func (b *BlockHeaderCheckpoints) Len() int32 {
	return int32(len(b.checkpoints))
}
