package blockdn

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClientWireFormat(t *testing.T) {
	genesis := chaincfg.TestNet4Params.GenesisBlock
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case statusPath:
				status := Status{}
				status.GenesisHash = genesis.BlockHash()
				status.Hash = genesis.BlockHash()
				err := json.NewEncoder(w).Encode(status)
				assert.NoError(t, err)
			case "/headers/0",
				"/header/" + genesis.BlockHash().String():
				assert.NoError(t, genesis.Header.Serialize(w))
			case "/filters/0":
				assert.NoError(
					t,
					wire.WriteVarBytes(w, 0, []byte{1, 2}),
				)
				assert.NoError(
					t,
					wire.WriteVarBytes(w, 0, []byte{3}),
				)
			case "/block/" + genesis.BlockHash().String():
				assert.NoError(t, genesis.Serialize(w))
			default:
				http.NotFound(w, r)
			}
		}),
	)
	defer server.Close()
	c, err := NewClient(server.URL, nil)
	require.NoError(t, err)
	ctx := t.Context()
	status, err := c.Status(ctx)
	require.NoError(t, err)
	require.Equal(t, genesis.BlockHash(), status.GenesisHash)
	headers, err := c.Headers(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, []wire.BlockHeader{genesis.Header}, headers)
	header, err := c.Header(ctx, genesis.BlockHash())
	require.NoError(t, err)
	require.Equal(t, genesis.Header, *header)
	filters, err := c.Filters(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, [][]byte{{1, 2}, {3}}, filters)
	block, err := c.Block(ctx, genesis.BlockHash())
	require.NoError(t, err)
	require.Equal(t, genesis.BlockHash(), block.BlockHash())
}

func TestClientRejectsBadResponses(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		code       int
	}{
		{"truncated", "short", http.StatusOK},
		{"oversized", string(make([]byte, 81)), http.StatusOK},
		{"unavailable", "", http.StatusServiceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler := func(
				w http.ResponseWriter, _ *http.Request,
			) {

				w.WriteHeader(tc.code)
				_, _ = w.Write([]byte(tc.body))
			}
			s := httptest.NewServer(http.HandlerFunc(handler))
			defer s.Close()
			c, err := NewClient(s.URL, nil)
			require.NoError(t, err)
			_, err = c.Header(
				t.Context(),
				*chaincfg.TestNet4Params.GenesisHash,
			)
			require.Error(t, err)
		})
	}
}

func TestClientCancellation(t *testing.T) {
	s := httptest.NewServer(
		http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			<-r.Context().Done()
		}),
	)
	defer s.Close()
	c, err := NewClient(s.URL, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(
		t.Context(),
		20*time.Millisecond,
	)
	defer cancel()
	_, err = c.Status(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

// TestClientCacheIsolation verifies distinct keys for network identity,
// mutable tail data and live status observations. This prevents reuse of CDN
// entries left over from a wrong-network origin or an earlier chain tip.
func TestClientCacheIsolation(t *testing.T) {
	var requests []*http.Request
	var mu sync.Mutex
	s := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			requests = append(requests, r.Clone(r.Context()))
			mu.Unlock()
			if r.URL.Path == statusPath {
				_, _ = w.Write([]byte(`{}`))
				return
			}
			header := chaincfg.MainNetParams.GenesisBlock.Header
			assert.NoError(t, header.Serialize(w))
		},
	))
	defer s.Close()
	c, err := NewClient(s.URL, nil)
	require.NoError(t, err)
	c = c.ForNetwork(*chaincfg.MainNetParams.GenesisHash)
	tip := *chaincfg.MainNetParams.GenesisHash
	ctx := t.Context()
	_, err = c.AtTip(150000, tip).Headers(ctx, 0)
	require.NoError(t, err)
	_, err = c.AtTip(150000, tip).Headers(ctx, 100000)
	require.NoError(t, err)
	_, err = c.Status(ctx)
	require.NoError(t, err)
	_, err = c.Status(ctx)
	require.NoError(t, err)
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, requests, 4)
	for _, request := range requests {
		require.Equal(t, tip.String(),
			request.URL.Query().Get("neutrino_network"))
	}
	require.Empty(t, requests[0].URL.Query().Get("neutrino_tip"))
	require.Equal(t, tip.String(),
		requests[1].URL.Query().Get("neutrino_tip"))
	first := requests[2].URL.Query().Get("neutrino_observation")
	second := requests[3].URL.Query().Get("neutrino_observation")
	require.NotEmpty(t, first)
	require.NotEqual(t, first, second)
}
