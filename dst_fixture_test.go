package neutrino

import (
	"sync"
	"time"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
)

const (
	// dstChainHeight is the honest best height every DST run must reach.
	dstChainHeight = 12

	// dstTipExtra is the number of honest headers mined past
	// dstChainHeight for the tip-following phase.
	dstTipExtra = 3

	// dstReorgFork is the last height shared by the honest chain and the
	// reorg branch.
	dstReorgFork = 10

	// dstReorgLength is the number of reorg branch headers mined above
	// dstReorgFork. It exceeds the honest chain so the branch has more
	// work.
	dstReorgLength = dstChainHeight + dstTipExtra - dstReorgFork + 1

	// dstHarderBits is a valid compact target above the SimNet limit's
	// difficulty, so headers mined at it carry an unexpected difficulty.
	dstHarderBits = 0x200fffff
)

// dstFixture holds every header chain a DST run serves. Mining depends only
// on the SimNet genesis, so it is built once per process and shared by all
// runs, which keeps it out of the per-execution cost.
type dstFixture struct {
	// honest is the honest chain; index i is height i.
	honest []*wire.BlockHeader

	// reorg is a heavier branch sharing honest[:dstReorgFork+1]; index i
	// is height i.
	reorg []*wire.BlockHeader

	// fork maps a start height to a PoW-valid alternative chain that
	// connects to honest[start] and runs through dstChainHeight. It fails
	// only checkpoint and stop-hash checks.
	fork [][]*wire.BlockHeader

	// badTime maps a start height to a chain connecting to honest[start]
	// whose first header's timestamp is not after the median time past.
	badTime [][]*wire.BlockHeader

	// badBits maps a start height to a chain connecting to honest[start]
	// whose first header carries an unexpected difficulty.
	badBits [][]*wire.BlockHeader

	// heights indexes every header hash of every chain by height.
	heights map[chainhash.Hash]uint32
}

var (
	dstFixtureOnce sync.Once
	dstFixtureData *dstFixture
)

// dstChains returns the process-wide DST fixture.
func dstChains() *dstFixture {
	dstFixtureOnce.Do(func() {
		dstFixtureData = buildDSTFixture()
	})
	return dstFixtureData
}

// dstMine searches nonces until header satisfies its own compact target.
func dstMine(parent *wire.BlockHeader, timestamp time.Time,
	bits uint32) *wire.BlockHeader {

	header := &wire.BlockHeader{
		Version:   4,
		PrevBlock: parent.BlockHash(),
		Bits:      bits,
		Timestamp: timestamp,
	}
	target := blockchain.CompactToBig(bits)
	for {
		hash := header.BlockHash()
		if blockchain.HashToBig(&hash).Cmp(target) <= 0 {
			return header
		}
		header.Nonce++
	}
}

// dstExtend mines count headers on top of parent, spacing timestamps by step.
func dstExtend(parent *wire.BlockHeader, count int,
	step time.Duration) []*wire.BlockHeader {

	headers := make([]*wire.BlockHeader, 0, count)
	for i := 0; i < count; i++ {
		parent = dstMine(parent, parent.Timestamp.Add(step), parent.Bits)
		headers = append(headers, parent)
	}
	return headers
}

// buildDSTFixture mines the honest chain and every faulty variant.
func buildDSTFixture() *dstFixture {
	genesis := chaincfg.SimNetParams.GenesisBlock.Header
	fixture := &dstFixture{
		heights: make(map[chainhash.Hash]uint32),
	}

	// The honest chain extends past the sync target so tip following has
	// new blocks to announce.
	fixture.honest = append(
		[]*wire.BlockHeader{&genesis},
		dstExtend(&genesis, dstChainHeight+dstTipExtra, time.Second)...,
	)

	// The reorg branch spaces timestamps differently, so every header
	// differs from the honest chain above the fork point.
	fixture.reorg = append(
		append([]*wire.BlockHeader(nil),
			fixture.honest[:dstReorgFork+1]...),
		dstExtend(
			fixture.honest[dstReorgFork], dstReorgLength,
			3*time.Second,
		)...,
	)

	// Every faulty chain connects to an honest start so it passes the
	// locator match and reaches range validation or commit.
	for start := 0; start < dstChainHeight; start++ {
		parent := fixture.honest[start]
		count := dstChainHeight - start
		fixture.fork = append(fixture.fork,
			dstExtend(parent, count, 2*time.Second))

		stale := dstMine(parent, genesis.Timestamp, parent.Bits)
		fixture.badTime = append(fixture.badTime, append(
			[]*wire.BlockHeader{stale},
			dstExtend(stale, count-1, time.Second)...,
		))

		harder := dstMine(
			parent, parent.Timestamp.Add(time.Second),
			dstHarderBits,
		)
		fixture.badBits = append(fixture.badBits, append(
			[]*wire.BlockHeader{harder},
			dstExtend(harder, count-1, time.Second)...,
		))
	}

	index := func(base uint32, headers []*wire.BlockHeader) {
		for i, header := range headers {
			fixture.heights[header.BlockHash()] = base + uint32(i)
		}
	}
	index(0, fixture.honest)
	index(0, fixture.reorg)
	for start := 0; start < dstChainHeight; start++ {
		index(uint32(start+1), fixture.fork[start])
		index(uint32(start+1), fixture.badTime[start])
		index(uint32(start+1), fixture.badBits[start])
	}

	return fixture
}
