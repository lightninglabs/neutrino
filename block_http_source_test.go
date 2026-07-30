package neutrino

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightninglabs/neutrino/blockfetch"
	"github.com/lightninglabs/neutrino/cache/lru"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/lightninglabs/neutrino/query"
	"github.com/stretchr/testify/require"
)

// blockServerMode controls how the test HTTP block server responds.
type blockServerMode int

const (
	// serveValidBlocks serves the correct, wire-encoded block for the
	// requested hash.
	serveValidBlocks blockServerMode = iota

	// serveError responds with an HTTP 500 for every request.
	serveError

	// serveGarbage responds with 200 but a body that isn't a valid block.
	serveGarbage
)

// newTestBlockServer spins up an httptest.Server that mimics the block-dn
// "/block/<hash>" endpoint for the given blocks, in the requested mode. It
// returns the server (already registered for cleanup) and a counter of how
// many block requests it received.
func newTestBlockServer(t *testing.T, blocks []*btcutil.Block,
	mode blockServerMode) (*httptest.Server, *int) {

	t.Helper()

	// Index the blocks by hash so we can serve them by the requested hash.
	blockBytes := make(map[string][]byte, len(blocks))
	for _, b := range blocks {
		raw, err := b.Bytes()
		require.NoError(t, err)
		blockBytes[b.Hash().String()] = raw
	}

	var requests int
	handler := func(w http.ResponseWriter, r *http.Request) {
		requests++

		switch mode {
		case serveError:
			w.WriteHeader(http.StatusInternalServerError)
			return

		case serveGarbage:
			_, _ = w.Write([]byte("this is not a block"))
			return
		}

		hash := strings.TrimPrefix(r.URL.Path, "/block/")
		raw, ok := blockBytes[hash]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		_, _ = w.Write(raw)
	}

	server := httptest.NewServer(http.HandlerFunc(handler))
	t.Cleanup(server.Close)

	return server, &requests
}

// newBlockSourceChainService builds a minimal ChainService wired up with a
// mock header store, an in-memory block cache, a mock work manager, and
// (optionally) an HTTP block fetcher pointed at baseURL. The returned queries
// channel receives the hash of every block requested from peers, so a test can
// assert whether the P2P path was exercised.
func newBlockSourceChainService(t *testing.T, blocks []*btcutil.Block,
	baseURL string) (*ChainService, chan chainhash.Hash) {

	t.Helper()

	mBlockHeaderStore := &headerfs.MockBlockHeaderStore{}
	for i, b := range blocks {
		blkHeader := &b.MsgBlock().Header
		mBlockHeaderStore.On("FetchHeader", b.Hash()).Return(
			blkHeader, uint32(i), nil,
		)
	}

	cs := &ChainService{
		BlockCache: lru.NewCache[wire.InvVect, *CacheableBlock](
			DefaultBlockCacheSize,
		),
		BlockHeaders: mBlockHeaderStore,
		chainParams: chaincfg.Params{
			PowLimit: maxPowLimit,
		},
		timeSource:  blockchain.NewMedianTime(),
		workManager: &mockDispatcher{},
		quit:        make(chan struct{}),
	}

	if baseURL != "" {
		cs.blockFetcher = blockfetch.New(blockfetch.Config{
			BaseURL:    baseURL,
			NumRetries: 1,
		})
	}

	// The mock work manager serves the requested block and notifies the
	// test of the query over the queries channel.
	queries := make(chan chainhash.Hash, 1)
	cs.workManager.(*mockDispatcher).query = func(reqs []*query.Request,
		opts ...query.QueryOption) chan error {

		errChan := make(chan error, 1)
		defer close(errChan)

		require.Len(t, reqs, 1)
		getData, ok := reqs[0].Req.(*wire.MsgGetData)
		require.True(t, ok)
		require.Len(t, getData.InvList, 1)

		inv := getData.InvList[0]
		for _, b := range blocks {
			if *b.Hash() != inv.Hash {
				continue
			}

			resp := &wire.MsgBlock{
				Header:       b.MsgBlock().Header,
				Transactions: b.MsgBlock().Transactions,
			}
			progress := reqs[0].HandleResp(getData, resp, "")
			require.True(t, progress.Finished)

			select {
			case queries <- inv.Hash:
			case <-time.After(time.Second):
				require.Fail(t, "query was not handled")
			}

			return errChan
		}

		require.Failf(t, "unknown block", "queried for %v", inv.Hash)

		return errChan
	}

	return cs, queries
}

// requirePeersQueried asserts that the peer path was used for the given hash.
func requirePeersQueried(t *testing.T, queries chan chainhash.Hash,
	hash chainhash.Hash) {

	t.Helper()

	select {
	case q := <-queries:
		require.Equal(t, hash, q)
	case <-time.After(time.Second):
		require.Fail(t, "expected block to be queried from peers")
	}
}

// requirePeersNotQueried asserts that the peer path was not used.
func requirePeersNotQueried(t *testing.T, queries chan chainhash.Hash) {
	t.Helper()

	select {
	case q := <-queries:
		require.Failf(t, "unexpected peer query", "for block %v", q)
	default:
	}
}

// TestGetBlockHTTPSource asserts that, when an HTTP block source is configured,
// GetBlock fetches from it first, caches the result, and only falls back to the
// P2P network when the HTTP fetch fails.
func TestGetBlockHTTPSource(t *testing.T) {
	t.Parallel()

	blocks, err := loadBlocks(t, blockDataFile, blockDataNet)
	require.NoError(t, err)

	// We use a non-genesis block with transactions as the target.
	target := blocks[1]
	targetHash := *target.Hash()

	t.Run("http first, no peer fallback", func(t *testing.T) {
		server, reqs := newTestBlockServer(
			t, blocks, serveValidBlocks,
		)
		cs, queries := newBlockSourceChainService(
			t, blocks, server.URL,
		)

		// The block should be fetched over HTTP and returned.
		got, err := cs.GetBlock(targetHash)
		require.NoError(t, err)
		require.Equal(t, targetHash, *got.Hash())
		require.Equal(t, 1, *reqs)
		requirePeersNotQueried(t, queries)

		// It should now be in the cache, so a second call neither hits
		// HTTP nor the peers.
		got, err = cs.GetBlock(targetHash)
		require.NoError(t, err)
		require.Equal(t, targetHash, *got.Hash())
		require.Equal(t, 1, *reqs)
		requirePeersNotQueried(t, queries)
	})

	t.Run("fallback to peers on http error", func(t *testing.T) {
		server, reqs := newTestBlockServer(t, blocks, serveError)
		cs, queries := newBlockSourceChainService(
			t, blocks, server.URL,
		)

		got, err := cs.GetBlock(targetHash)
		require.NoError(t, err)
		require.Equal(t, targetHash, *got.Hash())

		// The fetcher attempted the configured single request, then we
		// fell back to the peers.
		require.Equal(t, 1, *reqs)
		requirePeersQueried(t, queries, targetHash)
	})

	t.Run("fallback to peers on garbage block", func(t *testing.T) {
		server, reqs := newTestBlockServer(t, blocks, serveGarbage)
		cs, queries := newBlockSourceChainService(
			t, blocks, server.URL,
		)

		got, err := cs.GetBlock(targetHash)
		require.NoError(t, err)
		require.Equal(t, targetHash, *got.Hash())
		require.Equal(t, 1, *reqs)
		requirePeersQueried(t, queries, targetHash)
	})

	t.Run("no source configured queries peers", func(t *testing.T) {
		cs, queries := newBlockSourceChainService(t, blocks, "")

		got, err := cs.GetBlock(targetHash)
		require.NoError(t, err)
		require.Equal(t, targetHash, *got.Hash())
		requirePeersQueried(t, queries, targetHash)
	})
}
