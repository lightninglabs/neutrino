//go:build !js || !wasm

package wasmtransport

import (
	"fmt"
	"net"
)

func newDialer(DialerConfig) func(net.Addr) (net.Conn, error) {
	return func(net.Addr) (net.Conn, error) {
		return nil, fmt.Errorf("browser WebTransport dialer requires js/wasm")
	}
}
