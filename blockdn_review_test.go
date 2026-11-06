package neutrino

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/blockntfns"
	"github.com/lightninglabs/neutrino/cache/lru"
	"github.com/lightninglabs/neutrino/filterdb"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/lightninglabs/neutrino/sqldb"
	sqldbv2 "github.com/lightningnetwork/lnd/sqldb/v2"
	"github.com/stretchr/testify/require"
)

// manualHTTPService lets tests control poll and mutation boundaries without a
// concurrent sync loop. Notifications are retained for exact sequence checks.
func manualHTTPService(t *testing.T, url string,
	options ...func(*Config)) *ChainService {

	t.Helper()
	cfg := Config{
		DataDir: t.TempDir(), ChainParams: chaincfg.SimNetParams,
		SQLConfig: &sqldb.Config{Sqlite: &sqldbv2.SqliteConfig{}},
		BlockDN:   &BlockDNConfig{BaseURL: url},
	}
	for _, option := range options {
		option(&cfg)
	}
	s, err := NewChainService(cfg)
	require.NoError(t, err)
	s.blockDN.ctx = t.Context()
	s.blockDN.notifications = make(chan blockntfns.BlockNtfn, 4096)
	t.Cleanup(func() { require.NoError(t, s.Stop()) })
	return s
}

func TestBlockDNFilterBatchLimits(t *testing.T) {
	for _, tc := range []struct {
		name    string
		options []QueryOption
		batch   bool
	}{
		{"single", nil, false},
		{"forward cap one", []QueryOption{
			OptimisticBatch(), MaxBatchSize(1),
		}, false},
		{"reverse cap one", []QueryOption{
			OptimisticReverseBatch(), MaxBatchSize(1),
		}, false},
		{"partial batch cap", []QueryOption{
			OptimisticBatch(), MaxBatchSize(1999),
		}, false},
		{"whole batch cap", []QueryOption{
			OptimisticBatch(), MaxBatchSize(2000),
		}, true},
		{"unbounded", []QueryOption{OptimisticBatch()}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			chain := newHTTPTestChain(t, 5)
			server := httptest.NewServer(chain)
			defer server.Close()
			s := manualHTTPService(t, server.URL)
			require.NoError(t, s.blockDN.syncOnce(t.Context()))
			_, err := s.GetCFilter(chain.blocks[3].BlockHash(),
				wire.GCSFilterRegular, tc.options...)
			require.NoError(t, err)
			chain.mu.Lock()
			defer chain.mu.Unlock()
			if tc.batch {
				require.Equal(t, 1,
					chain.requests["/filters/0"])
			} else {
				require.Zero(t, chain.requests["/filters/0"])
				require.Positive(t,
					chain.requests["/filters/single/3"])
			}
		})
	}
}

func TestBlockDNFilterBatchReorgCacheKey(t *testing.T) {
	chain := newHTTPTestChain(t, 5)
	chain.batchCache = make(map[string][]byte)
	server := httptest.NewServer(chain)
	defer server.Close()
	s := manualHTTPService(t, server.URL)
	require.NoError(t, s.blockDN.syncOnce(t.Context()))
	hash := chain.blocks[1].BlockHash()
	oldTip := chain.blocks[5].BlockHash()
	_, err := s.GetCFilter(hash, wire.GCSFilterRegular, OptimisticBatch())
	require.NoError(t, err)
	chain.mu.Lock()
	chain.blocks = chain.blocks[:3]
	chain.extend(t, 7, 4)
	newTip := chain.blocks[7].BlockHash()
	chain.mu.Unlock()
	require.NoError(t, s.blockDN.syncOnce(t.Context()))
	// The requested block survived the reorg, while later filters in its
	// batch changed. The simulated CDN retains the entire old response.
	s.FilterCache = lru.NewCache[FilterCacheKey, *CacheableFilter](1 << 20)
	_, err = s.GetCFilter(hash, wire.GCSFilterRegular, OptimisticBatch())
	require.NoError(t, err)
	chain.mu.Lock()
	defer chain.mu.Unlock()
	require.Equal(t, []string{oldTip.String(), newTip.String()},
		chain.batchKeys)
}

func TestBlockDNQueriesWithTinyCaches(t *testing.T) {
	chain := newHTTPTestChain(t, 5)
	server := httptest.NewServer(chain)
	defer server.Close()
	s := manualHTTPService(t, server.URL, func(cfg *Config) {
		cfg.FilterCacheSize = 1
		cfg.BlockCacheSize = 1
		cfg.PersistToDisk = true
	})
	require.NoError(t, s.blockDN.syncOnce(t.Context()))
	hash := chain.blocks[3].BlockHash()
	filter, err := s.GetCFilter(hash, wire.GCSFilterRegular,
		OptimisticBatch())
	require.NoError(t, err)
	require.Zero(t, s.FilterCache.Len())
	stored, err := s.FilterDB.FetchFilter(&hash, filterdb.RegularFilter)
	require.NoError(t, err)
	require.Equal(t, filter, stored)
	block, err := s.GetBlock(hash)
	require.NoError(t, err)
	require.Equal(t, hash, *block.Hash())
	require.Zero(t, s.BlockCache.Len())
}

func TestBlockDNCommonAncestorCommitment(t *testing.T) {
	for _, extend := range []bool{false, true} {
		t.Run(fmt.Sprint("extend=", extend), func(t *testing.T) {
			chain := newHTTPTestChain(t, 5)
			server := httptest.NewServer(chain)
			defer server.Close()
			s := manualHTTPService(t, server.URL)
			require.NoError(t, s.blockDN.poll(t.Context()))
			original := chain.fheaders[5]
			chain.mu.Lock()
			if extend {
				chain.extend(t, 140, 0)
			}
			chain.fheaders[5][0] ^= 1
			chain.mu.Unlock()
			err := s.blockDN.poll(t.Context())
			require.ErrorContains(t, err,
				"filter commitment mismatch")
			status, err := s.BlockDNStatus()
			require.NoError(t, err)
			require.False(t, status.Current)
			require.EqualValues(t, 5, status.Height)
			stored, err := s.RegFilterHeaders.FetchHeaderByHeight(5)
			require.NoError(t, err)
			require.Equal(t, original, *stored)
		})
	}
}

var errBlockDNStoreFault = errors.New("injected store failure")

// storeFault models errors both before a mutation and after it is durable.
// Reads may fail during immediate recovery as well, forcing a later replay.
type storeFault struct {
	operation string
	after     bool
	fired     bool
	deferRead bool
	readFails bool
}

func (f *storeFault) mutate(operation string, apply func() error) error {
	if f.operation != operation || f.fired {
		return apply()
	}
	f.fired = true
	if !f.after {
		return errBlockDNStoreFault
	}
	if err := apply(); err != nil {
		return err
	}
	f.readFails = f.deferRead
	return errBlockDNStoreFault
}

type faultyBlockDNHeaders struct {
	headerfs.BlockHeaderStore
	fault *storeFault
}

func (s *faultyBlockDNHeaders) WriteHeaders(h ...headerfs.BlockHeader) error {
	return s.fault.mutate("block write", func() error {
		return s.BlockHeaderStore.WriteHeaders(h...)
	})
}

func (s *faultyBlockDNHeaders) RollbackLastBlock() (*headerfs.BlockStamp,
	error) {

	var tip *headerfs.BlockStamp
	err := s.fault.mutate("block rollback", func() error {
		var err error
		tip, err = s.BlockHeaderStore.RollbackLastBlock()
		return err
	})
	return tip, err
}

func (s *faultyBlockDNHeaders) ChainTip() (*wire.BlockHeader, uint32, error) {
	if s.fault.readFails {
		s.fault.readFails = false
		return nil, 0, errBlockDNStoreFault
	}
	return s.BlockHeaderStore.ChainTip()
}

type faultyBlockDNFilterHeaders struct {
	headerfs.FilterHeaderStore
	fault *storeFault
}

func (s *faultyBlockDNFilterHeaders) WriteHeaders(
	h ...headerfs.FilterHeader) error {

	return s.fault.mutate("filter write", func() error {
		return s.FilterHeaderStore.WriteHeaders(h...)
	})
}

func (s *faultyBlockDNFilterHeaders) RollbackLastBlock(
	hash *chainhash.Hash) (*headerfs.BlockStamp, error) {

	var tip *headerfs.BlockStamp
	err := s.fault.mutate("filter rollback", func() error {
		var err error
		tip, err = s.FilterHeaderStore.RollbackLastBlock(hash)
		return err
	})
	return tip, err
}

func injectBlockDNStoreFault(s *ChainService, fault *storeFault) {
	s.BlockHeaders = &faultyBlockDNHeaders{s.BlockHeaders, fault}
	s.RegFilterHeaders = &faultyBlockDNFilterHeaders{
		s.RegFilterHeaders, fault,
	}
}

func requireBlockDNNotifications(t *testing.T, s *ChainService,
	expected ...blockntfns.BlockNtfn) {

	t.Helper()
	for _, want := range expected {
		select {
		case got := <-s.blockDN.notifications:
			require.Equal(t, want, got)
		default:
			t.Fatalf("missing notification %v", want)
		}
	}
	require.Empty(t, s.blockDN.notifications)
}

func blockDNStoreFaultCases(operations ...string) []storeFault {
	var faults []storeFault
	for _, operation := range operations {
		for _, after := range []bool{false, true} {
			for _, deferRead := range []bool{false, true} {
				faults = append(faults, storeFault{
					operation: operation,
					after:     after,
					deferRead: deferRead,
				})
			}
		}
	}
	return faults
}

func TestBlockDNCommitNotificationRecovery(t *testing.T) {
	cases := blockDNStoreFaultCases("block write", "filter write")
	for _, fault := range cases {
		name := fmt.Sprintf("%s/after=%t/defer=%t",
			fault.operation, fault.after, fault.deferRead)
		t.Run(name, func(t *testing.T) {
			chain := newHTTPTestChain(t, 3)
			server := httptest.NewServer(chain)
			defer server.Close()
			s := manualHTTPService(t, server.URL)
			injectBlockDNStoreFault(s, &fault)
			err := s.blockDN.syncOnce(t.Context())
			require.ErrorIs(t, err, errBlockDNStoreFault)
			require.True(t, fault.fired)
			require.NoError(t,
				s.blockDN.syncOnce(t.Context()))
			require.NoError(t,
				s.blockDN.syncOnce(t.Context()))
			var expected []blockntfns.BlockNtfn
			for height := uint32(1); height <= 3; height++ {
				expected = append(expected,
					blockntfns.NewBlockConnected(
						chain.blocks[height].Header,
						height,
					))
			}
			requireBlockDNNotifications(t, s, expected...)
		})
	}
}

func TestBlockDNRollbackNotificationRecovery(t *testing.T) {
	cases := blockDNStoreFaultCases("block rollback", "filter rollback")
	for _, fault := range cases {
		name := fmt.Sprintf("%s/after=%t/defer=%t",
			fault.operation, fault.after, fault.deferRead)
		t.Run(name, func(t *testing.T) {
			chain := newHTTPTestChain(t, 3)
			server := httptest.NewServer(chain)
			defer server.Close()
			s := manualHTTPService(t, server.URL)
			require.NoError(t,
				s.blockDN.syncOnce(t.Context()))
			old := append([]*wire.MsgBlock(nil),
				chain.blocks...)
			for height := uint32(1); height <= 3; height++ {
				<-s.blockDN.notifications
			}
			chain.mu.Lock()
			chain.blocks = chain.blocks[:2]
			chain.extend(t, 4, 1)
			chain.mu.Unlock()
			injectBlockDNStoreFault(s, &fault)
			err := s.blockDN.syncOnce(t.Context())
			require.ErrorIs(t, err, errBlockDNStoreFault)
			require.True(t, fault.fired)
			require.NoError(t,
				s.blockDN.syncOnce(t.Context()))
			require.NoError(t,
				s.blockDN.syncOnce(t.Context()))
			expected := []blockntfns.BlockNtfn{
				blockntfns.NewBlockDisconnected(
					old[3].Header, 3, old[2].Header,
				),
				blockntfns.NewBlockDisconnected(
					old[2].Header, 2, old[1].Header,
				),
			}
			for height := uint32(2); height <= 4; height++ {
				expected = append(expected,
					blockntfns.NewBlockConnected(
						chain.blocks[height].Header,
						height,
					))
			}
			requireBlockDNNotifications(t, s, expected...)
		})
	}
}

func TestBlockDNCanceledNotificationReplay(t *testing.T) {
	chain := newHTTPTestChain(t, 3)
	server := httptest.NewServer(chain)
	defer server.Close()
	s := manualHTTPService(t, server.URL)
	var blocks []headerfs.BlockHeader
	var filters []headerfs.FilterHeader
	var expected []blockntfns.BlockNtfn
	for height := uint32(1); height <= 3; height++ {
		blocks = append(blocks, headerfs.BlockHeader{
			BlockHeader: &chain.blocks[height].Header,
			Height:      height,
		})
		filters = append(filters, headerfs.FilterHeader{
			HeaderHash: chain.blocks[height].BlockHash(),
			FilterHash: chain.fheaders[height], Height: height,
		})
		expected = append(expected, blockntfns.NewBlockConnected(
			chain.blocks[height].Header, height,
		))
	}
	s.blockDN.notifications = make(chan blockntfns.BlockNtfn)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, s.blockDN.commit(ctx, blocks, filters),
		context.Canceled)
	_, height, err := s.RegFilterHeaders.ChainTip()
	require.NoError(t, err)
	require.EqualValues(t, 3, height)
	s.blockDN.notifications = make(chan blockntfns.BlockNtfn, 10)
	require.NoError(t, s.blockDN.syncOnce(t.Context()))
	require.NoError(t, s.blockDN.syncOnce(t.Context()))
	requireBlockDNNotifications(t, s, expected...)
}

func TestBlockDNObservationFreshness(t *testing.T) {
	chain := newHTTPTestChain(t, 130)
	requested, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	var once sync.Once
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/headers/0" {
				once.Do(func() { close(requested) })
				select {
				case <-release:
				case <-r.Context().Done():
					return
				}
			}
			chain.ServeHTTP(w, r)
		},
	))
	defer server.Close()
	defer unblock()
	s := manualHTTPService(t, server.URL, func(cfg *Config) {
		cfg.BlockDN.PollInterval = 100 * time.Millisecond
	})
	result := make(chan error, 1)
	go func() { result <- s.blockDN.poll(t.Context()) }()
	select {
	case <-requested:
	case <-time.After(5 * time.Second):
		t.Fatal("sync did not request header history")
	}
	// Keep the import blocked beyond the freshness window, then allow it
	// to complete without a newer status observation.
	time.Sleep(4 * s.blockDN.cfg.PollInterval)
	released := time.Now()
	unblock()
	require.NoError(t, <-result)
	status, err := s.BlockDNStatus()
	require.NoError(t, err)
	require.False(t, status.Current)
	require.True(t, status.LastSuccess.Before(released))
	require.NoError(t, s.blockDN.poll(t.Context()))
	status, err = s.BlockDNStatus()
	require.NoError(t, err)
	require.True(t, status.Current)
}
