//go:build js && wasm

package wasmtransport

import (
	"bytes"
	"io"
	"net"
	"os"
	"syscall/js"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const fakeWebTransport = `
globalThis.__wasmTransportReadyDelay = 0;
globalThis.__wasmTransportStreamDelay = 0;
globalThis.__wasmTransportPendingWrite = false;
globalThis.__wasmTransportAbortOnClose = false;

globalThis.WebTransport = class {
  constructor(url, options) {
    globalThis.__wasmTransportURL = url;
    globalThis.__wasmTransportOptions = options;
    globalThis.__wasmTransportInstance = this;
    this.ready = new Promise((resolve) => {
      setTimeout(resolve, globalThis.__wasmTransportReadyDelay);
    });
    this.closed = new Promise((resolve) => {
      this.resolveClosed = resolve;
    });
  }

  async createBidirectionalStream() {
    await new Promise((resolve) => {
      setTimeout(resolve, globalThis.__wasmTransportStreamDelay);
    });

    let controller;
    const readable = new ReadableStream({
      start(value) {
        controller = value;
      },
    });
    this.controller = controller;

    const writable = new WritableStream({
      write(chunk) {
        if (globalThis.__wasmTransportPendingWrite) {
          return new Promise(() => {});
        }

        controller.enqueue(new Uint8Array(chunk));
      },
    });

    return {readable, writable};
  }

  close(info) {
    if (this.controller) {
      try {
        if (globalThis.__wasmTransportAbortOnClose) {
          this.controller.error(new Error("AbortError"));
        } else {
          this.controller.close();
        }
      } catch (_) {}
    }
    this.resolveClosed(info);
  }
};
`

func installFakeWebTransport(t *testing.T) {
	t.Helper()

	js.Global().Call("eval", fakeWebTransport)
	t.Cleanup(func() {
		js.Global().Call("eval", `
delete globalThis.WebTransport;
delete globalThis.__wasmTransportInstance;
delete globalThis.__wasmTransportURL;
delete globalThis.__wasmTransportOptions;
`)
	})
}

func newTestDialer(t *testing.T, timeout time.Duration) (
	func(net.Addr) (net.Conn, error), net.Addr) {

	t.Helper()

	remote := &net.TCPAddr{IP: net.ParseIP("192.0.2.1"), Port: 8333}
	hash := bytes.Repeat([]byte{1}, certificateHashLength)
	endpoints := EndpointMap{
		remote.String(): {
			URL:                     "https://peer.example/v1/btc-p2p",
			ServerCertificateHashes: [][]byte{hash},
		},
	}
	dialer, err := NewDialer(DialerConfig{
		ResolveEndpoint:  endpoints.Resolve,
		HandshakeTimeout: timeout,
	})
	require.NoError(t, err)

	return dialer, remote
}

func TestBrowserWebTransportConn(t *testing.T) {
	installFakeWebTransport(t)
	dialer, remote := newTestDialer(t, 0)

	connection, err := dialer(remote)
	require.NoError(t, err)
	require.Same(t, remote, connection.RemoteAddr())
	require.Equal(t, "webtransport", connection.LocalAddr().Network())
	require.Equal(t, "wasm", connection.LocalAddr().String())
	require.Equal(
		t, "https://peer.example/v1/btc-p2p",
		js.Global().Get("__wasmTransportURL").String(),
	)

	hashes := js.Global().Get("__wasmTransportOptions").Get(
		"serverCertificateHashes",
	)
	require.Equal(t, 1, hashes.Get("length").Int())
	require.Equal(t, certificateHashLength,
		hashes.Index(0).Get("value").Get("byteLength").Int())

	want := []byte("bitcoin-p2p")
	n, err := connection.Write(want)
	require.NoError(t, err)
	require.Equal(t, len(want), n)

	first := make([]byte, 3)
	n, err = connection.Read(first)
	require.NoError(t, err)
	require.Equal(t, want[:3], first[:n])

	rest := make([]byte, len(want))
	n, err = connection.Read(rest)
	require.NoError(t, err)
	require.Equal(t, want[3:], rest[:n])

	require.NoError(t, connection.Close())
	require.NoError(t, connection.Close())
	_, err = connection.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF)
	_, err = connection.Write([]byte{1})
	require.ErrorIs(t, err, io.ErrClosedPipe)
}

func TestBrowserWebTransportDrainsFinalChunkBeforeEOF(t *testing.T) {
	installFakeWebTransport(t)
	dialer, remote := newTestDialer(t, 0)
	connection, err := dialer(remote)
	require.NoError(t, err)

	want := []byte("final-chunk")
	_, err = connection.Write(want)
	require.NoError(t, err)
	js.Global().Get("__wasmTransportInstance").Get("controller").Call(
		"close",
	)

	got := make([]byte, len(want))
	n, err := io.ReadFull(connection, got)
	require.NoError(t, err)
	require.Equal(t, len(want), n)
	require.Equal(t, want, got)

	_, err = connection.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF)
}

func TestBrowserWebTransportExpiredReadDeadlineWins(t *testing.T) {
	installFakeWebTransport(t)
	dialer, remote := newTestDialer(t, 0)
	connection, err := dialer(remote)
	require.NoError(t, err)
	defer connection.Close()

	_, err = connection.Write([]byte("buffered"))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return len(connection.(*conn).readCh) > 0
	}, time.Second, time.Millisecond)

	require.NoError(
		t, connection.SetReadDeadline(time.Now().Add(-time.Second)),
	)
	_, err = connection.Read(make([]byte, 8))
	require.ErrorIs(t, err, os.ErrDeadlineExceeded)
}

func TestBrowserWebTransportWriteTimeoutClosesConnection(t *testing.T) {
	installFakeWebTransport(t)
	js.Global().Set("__wasmTransportPendingWrite", true)
	dialer, remote := newTestDialer(t, 0)
	connection, err := dialer(remote)
	require.NoError(t, err)

	require.NoError(
		t, connection.SetWriteDeadline(time.Now().Add(10*time.Millisecond)),
	)
	_, err = connection.Write([]byte("ambiguous"))
	require.ErrorIs(t, err, os.ErrDeadlineExceeded)

	_, err = connection.Write([]byte("must-not-retry"))
	require.ErrorIs(t, err, io.ErrClosedPipe)
}

func TestBrowserWebTransportLocalCloseWinsAbortError(t *testing.T) {
	installFakeWebTransport(t)
	js.Global().Set("__wasmTransportAbortOnClose", true)
	dialer, remote := newTestDialer(t, 0)
	connection, err := dialer(remote)
	require.NoError(t, err)

	require.NoError(t, connection.Close())
	_, err = connection.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF)
}

func TestBrowserWebTransportUsesOneHandshakeBudget(t *testing.T) {
	installFakeWebTransport(t)
	js.Global().Set("__wasmTransportReadyDelay", 30)
	js.Global().Set("__wasmTransportStreamDelay", 30)
	dialer, remote := newTestDialer(t, 45*time.Millisecond)

	_, err := dialer(remote)
	require.ErrorIs(t, err, os.ErrDeadlineExceeded)
}

func TestBrowserWebTransportRejectsUnmappedPeer(t *testing.T) {
	installFakeWebTransport(t)
	dialer, _ := newTestDialer(t, 0)
	unknown := &net.TCPAddr{IP: net.ParseIP("192.0.2.2"), Port: 8333}

	_, err := dialer(unknown)
	require.ErrorContains(t, err, "no WebTransport endpoint configured")
	require.True(
		t, js.Global().Get("__wasmTransportInstance").IsUndefined(),
	)
}
