package neutrino

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/btcutil/v2/gcs/builder"
	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/blockdn"
	"github.com/lightninglabs/neutrino/blockntfns"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/lightninglabs/neutrino/sqldb"
	sqldbv2 "github.com/lightningnetwork/lnd/sqldb/v2"
	"github.com/stretchr/testify/require"
)

type httpTestChain struct {
	mu            sync.Mutex
	blocks        []*wire.MsgBlock
	filters       [][]byte
	fheaders      []chainhash.Hash
	corruptFilter bool
	wrongNetwork  bool
	requests      map[string]int
	stallFilters  atomic.Bool
	batchCache    map[string][]byte
	batchKeys     []string
}

func newHTTPTestChain(t *testing.T, n int) *httpTestChain {
	c := &httpTestChain{
		blocks:   []*wire.MsgBlock{chaincfg.SimNetParams.GenesisBlock},
		requests: make(map[string]int),
	}
	c.extend(t, n, 0)
	return c
}

func (c *httpTestChain) extend(t *testing.T, n, salt int) {
	t.Helper()
	for len(c.blocks) <= n {
		parent := c.blocks[len(c.blocks)-1]
		tx := wire.NewMsgTx(1)
		tx.AddTxIn(
			&wire.TxIn{
				PreviousOutPoint: wire.OutPoint{
					Index: math.MaxUint32,
				},
				SignatureScript: []byte{
					2,
					byte(len(c.blocks)),
					byte(salt),
				},
				Sequence: math.MaxUint32,
			},
		)
		tx.AddTxOut(&wire.TxOut{Value: 50e8, PkScript: []byte{0x51}})
		block := &wire.MsgBlock{
			Header: wire.BlockHeader{
				Version:    4,
				PrevBlock:  parent.BlockHash(),
				MerkleRoot: tx.TxHash(),
				Bits:       parent.Header.Bits,
				Timestamp: parent.Header.Timestamp.Add(
					time.Minute,
				),
			},
			Transactions: []*wire.MsgTx{tx},
		}
		target := blockchain.CompactToBig(block.Header.Bits)
		for {
			work := blockchain.HashToBig(ptrHash(block.BlockHash()))
			if work.Cmp(target) <= 0 {
				break
			}
			block.Header.Nonce++
		}
		c.blocks = append(c.blocks, block)
	}
	c.filters, c.fheaders = nil, nil
	var previous chainhash.Hash
	for _, block := range c.blocks {
		filter, err := builder.BuildBasicFilter(block, nil)
		require.NoError(t, err)
		raw, err := filter.NBytes()
		require.NoError(t, err)
		fh, err := builder.MakeHeaderForFilter(filter, previous)
		require.NoError(t, err)
		c.filters = append(c.filters, raw)
		c.fheaders = append(c.fheaders, fh)
		previous = fh
	}
}

func ptrHash(h chainhash.Hash) *chainhash.Hash { return &h }

func (c *httpTestChain) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/filters/") && c.stallFilters.Load() {
		<-r.Context().Done()
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.requests[r.URL.Path]++
	path := r.URL.Path
	switch {
	case path == "/status":
		genesis := c.blocks[0].BlockHash()
		if c.wrongNetwork {
			genesis = *chaincfg.TestNet4Params.GenesisHash
		}
		tip := c.blocks[len(c.blocks)-1]
		status := blockdn.Status{
			GenesisHash:  genesis,
			Height:       uint32(len(c.blocks) - 1),
			Hash:         tip.BlockHash(),
			FilterHeight: uint32(len(c.blocks) - 1),
			FilterHeader: c.fheaders[len(c.fheaders)-1],
		}
		if err := json.NewEncoder(w).Encode(status); err != nil {
			http.Error(
				w, err.Error(), http.StatusInternalServerError,
			)
		}
	case strings.HasPrefix(path, "/headers/"):
		start, _ := strconv.Atoi(strings.TrimPrefix(path, "/headers/"))
		for i := start; i < len(c.blocks) && i < start+100000; i++ {
			_ = c.blocks[i].Header.Serialize(w)
		}
	case strings.HasPrefix(path, "/filter-headers/"):
		start, _ := strconv.Atoi(
			strings.TrimPrefix(path, "/filter-headers/"),
		)
		for i := start; i < len(c.fheaders) && i < start+100000; i++ {
			_, _ = w.Write(c.fheaders[i][:])
		}
	case strings.HasPrefix(path, "/filters/single/"):
		i, _ := strconv.Atoi(
			strings.TrimPrefix(path, "/filters/single/"),
		)
		if i >= len(c.filters) {
			http.NotFound(w, r)
			return
		}
		if c.corruptFilter {
			_, _ = w.Write([]byte{0})
			return
		}
		_, _ = w.Write(c.filters[i])
	case strings.HasPrefix(path, "/filters/"):
		key := r.URL.RequestURI()
		c.batchKeys = append(
			c.batchKeys, r.URL.Query().Get("neutrino_tip"),
		)
		if data, ok := c.batchCache[key]; ok {
			_, _ = w.Write(data)
			return
		}
		start, _ := strconv.Atoi(strings.TrimPrefix(path, "/filters/"))
		var body bytes.Buffer
		for i := start; i < len(c.filters) && i < start+2000; i++ {
			_ = wire.WriteVarBytes(&body, 0, c.filters[i])
		}
		if c.batchCache != nil {
			c.batchCache[key] = body.Bytes()
		}
		_, _ = w.Write(body.Bytes())
	case strings.HasPrefix(path, "/block/"),
		strings.HasPrefix(path, "/header/"):
		for _, block := range c.blocks {
			if strings.HasSuffix(path, block.BlockHash().String()) {
				if strings.HasPrefix(path, "/header/") {
					_ = block.Header.Serialize(w)
				} else {
					_ = block.Serialize(w)
				}
				return
			}
		}
		http.NotFound(w, r)
	default:
		http.NotFound(w, r)
	}
}

func httpService(t *testing.T, url, dir string) *ChainService {
	t.Helper()
	s, err := NewChainService(
		Config{DataDir: dir, ChainParams: chaincfg.SimNetParams,
			SQLConfig: &sqldb.Config{
				Sqlite: &sqldbv2.SqliteConfig{},
			},
			BlockDN: &BlockDNConfig{
				BaseURL:      url,
				PollInterval: 20 * time.Millisecond,
			},
			PersistToDisk: true,
			Dialer: func(net.Addr) (net.Conn, error) {
				t.Error("unexpected P2P dial")
				return nil, ErrP2PDisabled
			},
			NameResolver: func(string) ([]net.IP, error) {
				t.Error("unexpected P2P DNS")
				return nil, ErrP2PDisabled
			},
		},
	)
	require.NoError(t, err)
	require.Nil(t, s.connManager)
	require.Nil(t, s.blockManager)
	require.Nil(t, s.workManager)
	require.NoError(t, s.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, s.Stop()) })
	return s
}

func waitHTTPHeight(t *testing.T, s *ChainService, height uint32) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var status BlockDNSyncStatus
	for time.Now().Before(deadline) {
		status, _ = s.BlockDNStatus()
		if status.Current && status.Height == height {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("sync did not reach %d: %+v", height, status)
}

func TestBlockDNRuntime(t *testing.T) {
	c := newHTTPTestChain(t, 130)
	server := httptest.NewServer(c)
	defer server.Close()
	dir := t.TempDir()
	s := httpService(t, server.URL, dir)
	waitHTTPHeight(t, s, 130)
	require.Zero(t, s.ConnectedCount())
	require.Empty(t, s.Peers())
	require.ErrorIs(
		t,
		s.ConnectNode("127.0.0.1:8333", true),
		ErrP2PDisabled,
	)
	require.ErrorIs(
		t,
		s.SendTransaction(wire.NewMsgTx(2)),
		ErrBlockDNReadOnly,
	)

	// The unchanged tip needs only /status, not a full tail-file download.
	c.mu.Lock()
	requests := c.requests["/headers/0"]
	c.mu.Unlock()
	time.Sleep(100 * time.Millisecond)
	c.mu.Lock()
	require.Equal(t, requests, c.requests["/headers/0"])
	c.mu.Unlock()

	// A corrupt filter never reaches either cache or persistent store.
	c.mu.Lock()
	c.corruptFilter = true
	hash := c.blocks[3].BlockHash()
	c.mu.Unlock()
	_, err := s.GetCFilter(hash, wire.GCSFilterRegular)
	require.ErrorContains(t, err, "commitment mismatch")
	c.mu.Lock()
	c.corruptFilter = false
	c.mu.Unlock()
	_, err = s.GetCFilter(hash, wire.GCSFilterRegular, OptimisticBatch())
	require.NoError(t, err)
	block, err := s.GetBlock(hash)
	require.NoError(t, err)
	require.Equal(t, hash, *block.Hash())

	sub, err := s.blockSubscriptionMgr.NewSubscription(0)
	require.NoError(t, err)
	defer sub.Cancel()
	c.mu.Lock()
	c.extend(t, 132, 0)
	c.mu.Unlock()
	waitHTTPHeight(t, s, 132)
	for height := uint32(131); height <= 132; height++ {
		select {
		case n := <-sub.Notifications:
			require.IsType(t, &blockntfns.Connected{}, n)
			require.Equal(t, height, n.Height())
		case <-time.After(3 * time.Second):
			t.Fatal("missing live connect")
		}
	}

	// Equal-work forks are rejected. A later greater-work extension adopts
	// the branch and emits disconnects before replacement connects.
	c.mu.Lock()
	c.blocks = c.blocks[:130]
	c.extend(t, 132, 7)
	c.mu.Unlock()
	require.Eventually(
		t,
		func() bool {
			status, _ := s.BlockDNStatus()
			return status.LastError != nil
		},
		3*time.Second,
		10*time.Millisecond,
	)
	best, err := s.BestBlock()
	require.NoError(t, err)
	require.EqualValues(t, 132, best.Height)
	c.mu.Lock()
	c.extend(t, 134, 7)
	c.mu.Unlock()
	waitHTTPHeight(t, s, 134)
	heights := []uint32{132, 131, 130, 130, 131, 132, 133, 134}
	for _, height := range heights {
		select {
		case n := <-sub.Notifications:
			require.Equal(t, height, n.Height())
		case <-time.After(3 * time.Second):
			t.Fatal("missing reorg notification")
		}
	}
	sub.Cancel()
	require.NoError(t, s.Stop())
	s = httpService(t, server.URL, dir)
	waitHTTPHeight(t, s, 134)
	best, err = s.BestBlock()
	require.NoError(t, err)
	c.mu.Lock()
	require.Equal(t, c.blocks[134].BlockHash(), best.Hash)
	c.mu.Unlock()
}

func TestBlockDNWrongNetwork(t *testing.T) {
	c := newHTTPTestChain(t, 2)
	c.wrongNetwork = true
	server := httptest.NewServer(c)
	defer server.Close()
	s := httpService(t, server.URL, t.TempDir())
	require.Eventually(t, func() bool {
		status, _ := s.BlockDNStatus()
		return status.LastError != nil &&
			strings.Contains(
				status.LastError.Error(),
				"network mismatch",
			)
	}, 3*time.Second, 10*time.Millisecond)
	best, err := s.BestBlock()
	require.NoError(t, err)
	require.Zero(t, best.Height)
	require.False(t, s.IsCurrent())
}

func TestBlockDNBroadcast(t *testing.T) {
	c := newHTTPTestChain(t, 0)
	server := httptest.NewServer(c)
	defer server.Close()
	var calls atomic.Int32
	broadcast := func(ctx context.Context, _ *wire.MsgTx) error {
		calls.Add(1)
		return ctx.Err()
	}
	s, err := NewChainService(
		Config{DataDir: t.TempDir(), ChainParams: chaincfg.SimNetParams,
			SQLConfig: &sqldb.Config{
				Sqlite: &sqldbv2.SqliteConfig{},
			},
			BlockDN: &BlockDNConfig{
				BaseURL:   server.URL,
				Broadcast: broadcast,
			},
		},
	)
	require.NoError(t, err)
	require.NoError(t, s.Start(t.Context()))
	defer s.Stop()
	require.NoError(t, s.SendTransaction(wire.NewMsgTx(2)))
	require.EqualValues(t, 1, calls.Load())
}

func TestBlockDNConfigValidation(t *testing.T) {
	for _, cfg := range []Config{
		{BlockDN: &BlockDNConfig{BaseURL: "ftp://bad"}},
		{
			BlockDN: &BlockDNConfig{
				BaseURL: "https://example.com",
			},
			ConnectPeers: []string{"localhost"},
		},
	} {
		cfg.SQLConfig = &sqldb.Config{Sqlite: &sqldbv2.SqliteConfig{}}
		cfg.ChainParams = chaincfg.SimNetParams
		_, err := NewChainService(cfg)
		require.Error(t, err, fmt.Sprint(cfg))
	}
}

func TestBlockDNRescanAndCancellation(t *testing.T) {
	c := newHTTPTestChain(t, 5)
	server := httptest.NewServer(c)
	defer server.Close()
	s := httpService(t, server.URL, t.TempDir())
	waitHTTPHeight(t, s, 5)
	// Run the public rescan API with a script that matches every block.
	// This exercises filters, block validation and scanner integration.
	quit := make(chan struct{})
	defer close(quit)
	r := NewRescan(&RescanChainSource{s},
		StartBlock(&headerfs.BlockStamp{}),
		EndBlock(&headerfs.BlockStamp{Height: 5}),
		WatchInputs(InputWithScript{PkScript: []byte{0x51}}),
		QuitChan(quit),
	)
	select {
	case err := <-r.Start():
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("HTTP rescan did not finish")
	}
	r.WaitForShutdown()
	c.mu.Lock()
	for _, block := range c.blocks[1:] {
		require.Positive(
			t,
			c.requests["/block/"+block.BlockHash().String()],
		)
	}
	c.mu.Unlock()
	// A query canceled while the HTTP server is stalled must return at
	// once. Use a new service so persisted filters cannot satisfy the call.
	s2 := httpService(t, server.URL, t.TempDir())
	waitHTTPHeight(t, s2, 5)
	c.stallFilters.Store(true)
	cancel := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := s2.GetCFilter(
			c.blocks[3].BlockHash(),
			wire.GCSFilterRegular,
			CancelQuery(cancel),
		)
		done <- err
	}()
	close(cancel)
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("filter query ignored cancellation")
	}
}

func TestBlockDNInterruptedHeaderCommit(t *testing.T) {
	c := newHTTPTestChain(t, 5)
	server := httptest.NewServer(c)
	defer server.Close()
	dir := t.TempDir()
	s := httpService(t, server.URL, dir)
	waitHTTPHeight(t, s, 5)
	// Simulate a process exiting after block headers commit but before the
	// corresponding filter headers commit. Restart must recover and resync.
	s.blockDN.stop()
	c.mu.Lock()
	c.extend(t, 6, 0)
	extra := c.blocks[6].Header
	c.mu.Unlock()
	require.NoError(
		t,
		s.BlockHeaders.WriteHeaders(
			headerfs.BlockHeader{BlockHeader: &extra, Height: 6},
		),
	)
	require.NoError(t, s.Stop())
	s = httpService(t, server.URL, dir)
	waitHTTPHeight(t, s, 6)
	best, err := s.BestBlock()
	require.NoError(t, err)
	require.Equal(t, extra.BlockHash(), best.Hash)
}

func TestBlockDNWitness(t *testing.T) {
	c := newHTTPTestChain(t, 5)
	w := newHTTPTestChain(t, 5)
	w.blocks = w.blocks[:3]
	w.extend(t, 5, 9)
	primary := httptest.NewServer(c)
	defer primary.Close()
	witness := httptest.NewServer(w)
	defer witness.Close()
	s, err := NewChainService(Config{
		DataDir: t.TempDir(), ChainParams: chaincfg.SimNetParams,
		SQLConfig: &sqldb.Config{Sqlite: &sqldbv2.SqliteConfig{}},
		BlockDN: &BlockDNConfig{BaseURL: primary.URL,
			WitnessURLs:  []string{witness.URL},
			PollInterval: 20 * time.Millisecond},
	})
	require.NoError(t, err)
	require.NoError(t, s.Start(t.Context()))
	defer s.Stop()
	require.Eventually(t, func() bool {
		status, _ := s.BlockDNStatus()
		return status.LastError != nil &&
			strings.Contains(status.LastError.Error(), "witness")
	}, 3*time.Second, 10*time.Millisecond)
	best, err := s.BestBlock()
	require.NoError(t, err)
	require.Zero(t, best.Height)
	w.mu.Lock()
	w.blocks = c.blocks
	w.extend(t, 5, 0)
	w.mu.Unlock()
	waitHTTPHeight(t, s, 5)
}
