package neutrino

import (
	"compress/bzip2"
	"encoding/binary"
	"fmt"
	"io"
	"math/big"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lightninglabs/neutrino/query"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil"
	"github.com/btcsuite/btcutil/gcs"
	"github.com/btcsuite/btcutil/gcs/builder"
	"github.com/lightninglabs/neutrino/cache"
	"github.com/lightninglabs/neutrino/cache/lru"
	"github.com/lightninglabs/neutrino/filterdb"
	"github.com/lightninglabs/neutrino/headerfs"
)

var (
	// maxPowLimit is used as the max block target to ensure all PoWs are
	// valid.
	bigOne      = big.NewInt(1)
	maxPowLimit = new(big.Int).Sub(new(big.Int).Lsh(bigOne, 255), bigOne)

	// blockDataNet is the expected network in the test block data.
	blockDataNet = wire.MainNet

	// blockDataFile is the path to a file containing the first 256 blocks
	// of the block chain.
	blockDataFile = filepath.Join("testdata", "blocks1-256.bz2")
)

// loadBlocks loads the blocks contained in the testdata directory and returns
// a slice of them.
//
// NOTE: copied from btcsuite/btcd/database/ffldb/interface_test.go.
func loadBlocks(t *testing.T, dataFile string, network wire.BitcoinNet) (
	[]*btcutil.Block, error) {
	// Open the file that contains the blocks for reading.
	fi, err := os.Open(dataFile)
	if err != nil {
		t.Errorf("failed to open file %v, err %v", dataFile, err)
		return nil, err
	}
	defer func() {
		if err := fi.Close(); err != nil {
			t.Errorf("failed to close file %v %v", dataFile,
				err)
		}
	}()
	dr := bzip2.NewReader(fi)

	// Set the first block as the genesis block.
	blocks := make([]*btcutil.Block, 0, 256)
	genesis := btcutil.NewBlock(chaincfg.MainNetParams.GenesisBlock)
	blocks = append(blocks, genesis)

	// Load the remaining blocks.
	for height := 1; ; height++ {
		var net uint32
		err := binary.Read(dr, binary.LittleEndian, &net)
		if err == io.EOF {
			// Hit end of file at the expected offset.  No error.
			break
		}
		if err != nil {
			t.Errorf("Failed to load network type for block %d: %v",
				height, err)
			return nil, err
		}
		if net != uint32(network) {
			t.Errorf("Block doesn't match network: %v expects %v",
				net, network)
			return nil, err
		}

		var blockLen uint32
		err = binary.Read(dr, binary.LittleEndian, &blockLen)
		if err != nil {
			t.Errorf("Failed to load block size for block %d: %v",
				height, err)
			return nil, err
		}

		// Read the block.
		blockBytes := make([]byte, blockLen)
		_, err = io.ReadFull(dr, blockBytes)
		if err != nil {
			t.Errorf("Failed to load block %d: %v", height, err)
			return nil, err
		}

		// Deserialize and store the block.
		block, err := btcutil.NewBlockFromBytes(blockBytes)
		if err != nil {
			t.Errorf("Failed to parse block %v: %v", height, err)
			return nil, err
		}
		blocks = append(blocks, block)
	}

	return blocks, nil
}

// genRandomBlockHash generates a random block hash using math/rand.
func genRandomBlockHash() *chainhash.Hash {
	var seed [32]byte
	rand.Read(seed[:])
	hash := chainhash.Hash(seed)
	return &hash
}

// getRandFilter generates a random GCS filter that contains numElements. It
// will then convert that filter into CacheableFilter to compute it's size for
// convenience. It will return the filter along with it's size and randomly
// generated block hash. testing.T is passed in as a convenience to deal with
// errors in this method and making the test code more straightforward. Method
// originally taken from filterdb/db_test.go.
func genRandFilter(numElements uint32, t *testing.T) (
	*chainhash.Hash, *gcs.Filter, uint64) {
	elements := make([][]byte, numElements)
	for i := uint32(0); i < numElements; i++ {
		var elem [20]byte
		if _, err := rand.Read(elem[:]); err != nil {
			t.Fatalf("unable to create random filter: %v", err)
			return nil, nil, 0
		}

		elements[i] = elem[:]
	}

	var key [16]byte
	if _, err := rand.Read(key[:]); err != nil {
		t.Fatalf("unable to create random filter: %v", err)
		return nil, nil, 0
	}

	filter, err := gcs.BuildGCSFilter(
		builder.DefaultP, builder.DefaultM, key, elements)
	if err != nil {
		t.Fatalf("unable to create random filter: %v", err)
		return nil, nil, 0
	}

	// Convert into CacheableFilter and compute Size.
	c := &cache.CacheableFilter{Filter: filter}
	s, err := c.Size()
	if err != nil {
		t.Fatalf("unable to create random filter: %v", err)
		return nil, nil, 0
	}

	return genRandomBlockHash(), filter, s
}

// getFilter is a convenience method which will extract a value from the cache
// and handle errors, it makes the test code easier to follow.
func getFilter(cs *ChainService, b *chainhash.Hash, t *testing.T) *gcs.Filter {
	val, err := cs.getFilterFromCache(b, filterdb.RegularFilter)
	if err != nil {
		t.Fatal(err)
	}
	return val
}

func assertEqual(t *testing.T, a interface{}, b interface{}, message string) { // nolint:unparam
	if a == b {
		return
	}
	if len(message) == 0 {
		message = fmt.Sprintf("%v != %v", a, b)
	}
	t.Fatal(message)
}

// TestCacheBigEnoughHoldsAllFilter creates a cache big enough to hold all
// filters, then gets them in random order and makes sure they are always there.
func TestCacheBigEnoughHoldsAllFilter(t *testing.T) {
	// Create different sized filters.
	b1, f1, s1 := genRandFilter(1, t)
	b2, f2, s2 := genRandFilter(10, t)
	b3, f3, s3 := genRandFilter(100, t)

	cs := &ChainService{
		FilterCache: lru.NewCache(s1 + s2 + s3),
	}

	// Insert those filters into the cache making sure nothing gets evicted.
	assertEqual(t, cs.FilterCache.Len(), 0, "")
	cs.putFilterToCache(b1, filterdb.RegularFilter, f1)
	assertEqual(t, cs.FilterCache.Len(), 1, "")
	cs.putFilterToCache(b2, filterdb.RegularFilter, f2)
	assertEqual(t, cs.FilterCache.Len(), 2, "")
	cs.putFilterToCache(b3, filterdb.RegularFilter, f3)
	assertEqual(t, cs.FilterCache.Len(), 3, "")

	// Check that we can get those filters back independent of Get order.
	assertEqual(t, getFilter(cs, b1, t), f1, "")
	assertEqual(t, getFilter(cs, b2, t), f2, "")
	assertEqual(t, getFilter(cs, b3, t), f3, "")
	assertEqual(t, getFilter(cs, b2, t), f2, "")
	assertEqual(t, getFilter(cs, b3, t), f3, "")
	assertEqual(t, getFilter(cs, b1, t), f1, "")
	assertEqual(t, getFilter(cs, b3, t), f3, "")

	assertEqual(t, cs.FilterCache.Len(), 3, "")
}

// TestBigFilterEvictsEverything creates a cache big enough to hold a large
// filter and inserts many smaller filters into. Then it inserts the big filter
// and verifies that it's the only one remaining.
func TestBigFilterEvictsEverything(t *testing.T) {
	// Create different sized filters.
	b1, f1, _ := genRandFilter(1, t)
	b2, f2, _ := genRandFilter(3, t)
	b3, f3, s3 := genRandFilter(10, t)

	cs := &ChainService{
		FilterCache: lru.NewCache(s3),
	}

	// Insert the smaller filters.
	assertEqual(t, cs.FilterCache.Len(), 0, "")
	cs.putFilterToCache(b1, filterdb.RegularFilter, f1)
	assertEqual(t, cs.FilterCache.Len(), 1, "")
	cs.putFilterToCache(b2, filterdb.RegularFilter, f2)
	assertEqual(t, cs.FilterCache.Len(), 2, "")

	// Insert the big filter and check all previous filters are evicted.
	cs.putFilterToCache(b3, filterdb.RegularFilter, f3)
	assertEqual(t, cs.FilterCache.Len(), 1, "")
	assertEqual(t, getFilter(cs, b3, t), f3, "")
}

// TestBlockCache checks that blocks are inserted and fetched from the cache
// before peers are queried.
func TestBlockCache(t *testing.T) {
	t.Parallel()

	// Load the first 255 blocks from disk.
	blocks, err := loadBlocks(t, blockDataFile, blockDataNet)
	if err != nil {
		t.Fatalf("loadBlocks: Unexpected error: %v", err)
	}

	// We'll use a simple mock header store since the GetBlocks method
	// assumes we only query for blocks with an already known header.
	headers := newMockBlockHeaderStore()

	// Iterate through the blocks, calculating the size of half of them,
	// and writing them to the header store.
	var size uint64
	for i, b := range blocks {
		header := headerfs.BlockHeader{
			BlockHeader: &b.MsgBlock().Header,
			Height:      uint32(i),
		}
		headers.WriteHeaders(header)

		sz, _ := (&cache.CacheableBlock{Block: b}).Size()
		if i < len(blocks)/2 {
			size += sz
		}
	}

	// Set up a ChainService with a BlockCache that can fit the first half
	// of the blocks.
	cs := &ChainService{
		BlockCache:   lru.NewCache(size),
		BlockHeaders: headers,
		chainParams: chaincfg.Params{
			PowLimit: maxPowLimit,
		},
		timeSource:      blockchain.NewMedianTime(),
		queryDispatcher: &mockDispatcher{},
	}

	// We'll set up the queryPeers method to make sure we are only querying
	// for blocks, and send the block hashes queried over the queries
	// channel.
	queries := make(chan chainhash.Hash, 1)
	cs.queryDispatcher.(*mockDispatcher).query = func(requests []*query.Request,
		options ...query.QueryOption) chan error {

		errChan := make(chan error, 1)
		defer close(errChan)

		if len(requests) != 1 {
			t.Fatalf("unexpected 1 request, got %d", len(requests))
		}

		getData, ok := requests[0].Req.(*wire.MsgGetData)
		if !ok {
			t.Fatalf("unexpected type: %T", requests[0].Req)
		}

		if len(getData.InvList) != 1 {
			t.Fatalf("expected 1 element in inv list, found %v",
				len(getData.InvList))
		}

		inv := getData.InvList[0]
		if inv.Type != wire.InvTypeWitnessBlock {
			t.Fatalf("unexpected inv type: %v", inv.Type)
		}

		// Serve the block that matches the requested block header.
		for _, b := range blocks {
			if *b.Hash() == inv.Hash {
				header, _, err := headers.FetchHeader(b.Hash())
				if err != nil {
					t.Fatalf("")
				}

				resp := &wire.MsgBlock{
					Header:       *header,
					Transactions: b.MsgBlock().Transactions,
				}

				requests[0].HandleResp(requests[0].Req, resp, "")

				// Notify the test about the query.
				select {
				case queries <- inv.Hash:
				case <-time.After(1 * time.Second):
					t.Fatalf("query was not handled")
				}

				return errChan
			}
		}

		t.Fatalf("queried for unknown block: %v", inv.Hash)
		return errChan
	}

	// fetchAndAssertPeersQueried calls GetBlock and makes sure the block
	// is fetched from the peers.
	fetchAndAssertPeersQueried := func(hash chainhash.Hash) {
		found, err := cs.GetBlock(hash)
		if err != nil {
			t.Fatalf("error getting block: %v", err)
		}

		if *found.Hash() != hash {
			t.Fatalf("requested block with hash %v, got %v",
				hash, found.Hash())
		}

		select {
		case q := <-queries:
			if q != hash {
				t.Fatalf("expected hash %v to be queried, "+
					"got %v", hash, q)
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("did not query peers for block")
		}
	}

	// fetchAndAssertInCache calls GetBlock and makes sure the block is not
	// fetched from the peers.
	fetchAndAssertInCache := func(hash chainhash.Hash) {
		found, err := cs.GetBlock(hash)
		if err != nil {
			t.Fatalf("error getting block: %v", err)
		}

		if *found.Hash() != hash {
			t.Fatalf("requested block with hash %v, got %v",
				hash, found.Hash())
		}

		// Make sure we didn't query the peers for this block.
		select {
		case q := <-queries:
			t.Fatalf("did not expect query for block %v", q)
		default:
		}
	}

	// Get the first half of the blocks. Since this is the first time we
	// request them, we expect them all to be fetched from peers.
	for _, b := range blocks[:len(blocks)/2] {
		fetchAndAssertPeersQueried(*b.Hash())
	}

	// Get the first half of the blocks again. This time we expect them all
	// to be fetched from the cache.
	for _, b := range blocks[:len(blocks)/2] {
		fetchAndAssertInCache(*b.Hash())
	}

	// Get the second half of the blocks. These have not been fetched
	// before, and we expect them to be fetched from peers.
	for _, b := range blocks[len(blocks)/2:] {
		fetchAndAssertPeersQueried(*b.Hash())
	}

	// Since the cache only had capacity for the first half of the blocks,
	// some of these should now have been evicted. We only check the first
	// one, since we cannot know for sure how many because of the variable
	// size.
	b := blocks[0]
	fetchAndAssertPeersQueried(*b.Hash())
}
