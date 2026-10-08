//go:build !js

package wasmtransport

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDoHResolverLookupIP(t *testing.T) {
	t.Parallel()

	var (
		mu      sync.Mutex
		queries []string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,
		r *http.Request) {

		if got := r.Header.Get("accept"); got != "application/dns-json" {
			t.Errorf("unexpected accept header %q", got)
		}
		mu.Lock()
		queries = append(queries, r.URL.Query().Get("type"))
		mu.Unlock()

		answers := []map[string]any{}
		switch r.URL.Query().Get("type") {
		case "A":
			answers = append(answers, map[string]any{
				"type": 1,
				"data": "192.0.2.7",
			})
		case "AAAA":
			answers = append(answers, map[string]any{
				"type": 28,
				"data": "2001:db8::7",
			})
		}

		if err := json.NewEncoder(w).Encode(map[string]any{
			"Status": 0,
			"Answer": answers,
		}); err != nil {
			t.Errorf("encode DNS response: %v", err)
		}
	}))
	defer server.Close()

	resolver := &DoHResolver{
		Endpoint: server.URL,
		Client:   server.Client(),
	}
	ips, err := resolver.LookupIP("peer.example")
	require.NoError(t, err)
	require.Len(t, ips, 2)
	require.True(t, ips[0].Equal(net.ParseIP("192.0.2.7")))
	require.True(t, ips[1].Equal(net.ParseIP("2001:db8::7")))

	mu.Lock()
	defer mu.Unlock()
	require.ElementsMatch(t, []string{"A", "AAAA"}, queries)
}

func TestDoHResolverLiteralIP(t *testing.T) {
	t.Parallel()

	resolver := &DoHResolver{}
	ips, err := resolver.LookupIP("192.0.2.9")
	require.NoError(t, err)
	require.Len(t, ips, 1)
	require.True(t, ips[0].Equal(net.ParseIP("192.0.2.9")))
}

func TestDoHResolverPartialFamilyFailure(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,
		r *http.Request) {

		if r.URL.Query().Get("type") == "AAAA" {
			if err := json.NewEncoder(w).Encode(map[string]any{
				"Status": 2,
			}); err != nil {
				t.Errorf("encode DNS response: %v", err)
			}
			return
		}

		if err := json.NewEncoder(w).Encode(map[string]any{
			"Status": 0,
			"Answer": []map[string]any{{
				"type": 1,
				"data": "192.0.2.7",
			}},
		}); err != nil {
			t.Errorf("encode DNS response: %v", err)
		}
	}))
	defer server.Close()

	resolver := &DoHResolver{
		Endpoint: server.URL,
		Client:   server.Client(),
	}
	ips, err := resolver.LookupIP("peer.example")
	require.NoError(t, err)
	require.Len(t, ips, 1)
	require.True(t, ips[0].Equal(net.ParseIP("192.0.2.7")))
}

func TestDoHResolverDNSFailure(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,
		_ *http.Request) {

		if err := json.NewEncoder(w).Encode(map[string]any{
			"Status": 3,
		}); err != nil {
			t.Errorf("encode DNS response: %v", err)
		}
	}))
	defer server.Close()

	resolver := &DoHResolver{
		Endpoint: server.URL,
		Client:   server.Client(),
	}
	_, err := resolver.LookupIP("missing.example")
	require.ErrorContains(t, err, "returned status 3")
}
