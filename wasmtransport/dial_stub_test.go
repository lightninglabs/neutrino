//go:build !js || !wasm

package wasmtransport

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNativeDialerFailsClearly(t *testing.T) {
	t.Parallel()

	remote := &net.TCPAddr{
		IP:   net.ParseIP("192.0.2.1"),
		Port: 8333,
	}
	endpoints := EndpointMap{
		remote.String(): {
			URL: "https://peer.example/v1/btc-p2p",
		},
	}
	dialer, err := NewDialer(DialerConfig{
		ResolveEndpoint: endpoints.Resolve,
	})
	require.NoError(t, err)

	_, err = dialer(remote)
	require.ErrorContains(t, err, "requires js/wasm")
}
