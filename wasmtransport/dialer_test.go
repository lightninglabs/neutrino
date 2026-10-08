package wasmtransport

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func ExampleNewDialer() {
	peerAddress := "192.0.2.1:8333"
	endpoints := EndpointMap{
		peerAddress: {
			URL: "https://peer.example/v1/btc-p2p",
		},
	}

	// Assign dialer to neutrino.Config.Dialer and include peerAddress in
	// neutrino.Config.ConnectPeers.
	dialer, err := NewDialer(DialerConfig{
		ResolveEndpoint: endpoints.Resolve,
	})
	fmt.Println(dialer != nil, err)

	// Output:
	// true <nil>
}

func TestValidateDialerConfig(t *testing.T) {
	t.Parallel()

	resolver := EndpointMap{}.Resolve
	cfg, err := validateDialerConfig(DialerConfig{
		ResolveEndpoint: resolver,
	})
	require.NoError(t, err)
	require.Equal(t, DefaultHandshakeTimeout, cfg.HandshakeTimeout)
}

func TestValidatePeerEndpoint(t *testing.T) {
	t.Parallel()

	hash := make([]byte, certificateHashLength)
	cfg, err := validatePeerEndpoint(PeerEndpoint{
		URL:                     "https://peer.example:443/v1/btc-p2p",
		ServerCertificateHashes: [][]byte{hash},
	})
	require.NoError(t, err)
	require.Equal(t, hash, cfg.ServerCertificateHashes[0])

	hash[0] = 1
	require.Zero(t, cfg.ServerCertificateHashes[0][0])
}

func TestValidateDialerConfigRejectsInvalidValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  DialerConfig
	}{
		{name: "missing resolver"},
		{
			name: "negative timeout",
			cfg: DialerConfig{
				ResolveEndpoint:  EndpointMap{}.Resolve,
				HandshakeTimeout: -time.Second,
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := validateDialerConfig(test.cfg)
			require.Error(t, err)
		})
	}
}

func TestValidatePeerEndpointRejectsInvalidValues(t *testing.T) {
	t.Parallel()

	tests := []PeerEndpoint{
		{URL: "http://peer.example/v1/btc-p2p"},
		{URL: "https:///v1/btc-p2p"},
		{URL: "https://user@peer.example/v1/btc-p2p"},
		{URL: "https://peer.example/v1/btc-p2p#fragment"},
		{
			URL:                     "https://peer.example/v1/btc-p2p",
			ServerCertificateHashes: [][]byte{{1, 2, 3}},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.URL, func(t *testing.T) {
			t.Parallel()

			_, err := validatePeerEndpoint(test)
			require.Error(t, err)
		})
	}
}

func TestEndpointMapRejectsUnknownPeer(t *testing.T) {
	t.Parallel()

	known := &net.TCPAddr{IP: net.ParseIP("192.0.2.1"), Port: 8333}
	unknown := &net.TCPAddr{IP: net.ParseIP("192.0.2.2"), Port: 8333}
	endpoints := EndpointMap{
		known.String(): {URL: "https://peer.example/v1/btc-p2p"},
	}

	endpoint, err := endpoints.Resolve(known)
	require.NoError(t, err)
	require.Equal(t, "https://peer.example/v1/btc-p2p", endpoint.URL)

	_, err = endpoints.Resolve(unknown)
	require.ErrorContains(t, err, "no WebTransport endpoint configured")
}
