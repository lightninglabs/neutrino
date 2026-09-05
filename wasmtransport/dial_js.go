//go:build js && wasm

package wasmtransport

import (
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"syscall/js"
	"time"
)

const readQueueSize = 32

type promiseResult struct {
	value js.Value
	err   error
}

type deadline struct {
	mu      sync.Mutex
	when    time.Time
	changed chan struct{}
}

func newDeadline() *deadline {
	return &deadline{changed: make(chan struct{})}
}

func (d *deadline) set(when time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.when = when
	close(d.changed)
	d.changed = make(chan struct{})
}

func (d *deadline) snapshot() (time.Time, <-chan struct{}) {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.when, d.changed
}

type conn struct {
	transport js.Value
	reader    js.Value
	writer    js.Value

	local  net.Addr
	remote net.Addr

	readMu  sync.Mutex
	readBuf []byte
	readCh  chan []byte

	writeMu sync.Mutex

	readDeadline  *deadline
	writeDeadline *deadline

	errMu sync.Mutex
	err   error

	closeOnce sync.Once
	closed    chan struct{}
}

func newDialer(cfg DialerConfig) func(net.Addr) (net.Conn, error) {
	return func(remote net.Addr) (net.Conn, error) {
		if remote == nil {
			return nil, fmt.Errorf("WebTransport peer address is nil")
		}
		if isOnionAddr(remote) {
			return nil, fmt.Errorf("onion peer %s is not reachable through browser WebTransport", remote)
		}

		endpoint, err := cfg.ResolveEndpoint(remote)
		if err != nil {
			return nil, fmt.Errorf(
				"resolve WebTransport endpoint for %s: %w", remote, err,
			)
		}
		endpoint, err = validatePeerEndpoint(endpoint)
		if err != nil {
			return nil, fmt.Errorf(
				"invalid WebTransport endpoint for %s: %w", remote, err,
			)
		}

		return dial(cfg, endpoint, remote)
	}
}

func dial(cfg DialerConfig, endpoint PeerEndpoint,
	remote net.Addr) (net.Conn, error) {

	constructor := js.Global().Get("WebTransport")
	if constructor.Type() != js.TypeFunction {
		return nil, fmt.Errorf("browser WebTransport API unavailable")
	}

	options := js.Global().Get("Object").New()
	if len(endpoint.ServerCertificateHashes) > 0 {
		hashes := js.Global().Get("Array").New()
		for _, hash := range endpoint.ServerCertificateHashes {
			value := js.Global().Get("Uint8Array").New(len(hash))
			js.CopyBytesToJS(value, hash)

			entry := js.Global().Get("Object").New()
			entry.Set("algorithm", "sha-256")
			entry.Set("value", value)
			hashes.Call("push", entry)
		}
		options.Set("serverCertificateHashes", hashes)
	}

	transport, err := safeNew(constructor, endpoint.URL, options)
	if err != nil {
		return nil, fmt.Errorf("create WebTransport session: %w", err)
	}

	handshakeDeadline := time.Now().Add(cfg.HandshakeTimeout)
	if _, err := awaitUntil(
		transport.Get("ready"), handshakeDeadline,
	); err != nil {
		closeWebTransport(transport)
		return nil, fmt.Errorf("establish WebTransport session: %w", err)
	}

	streamPromise, err := safeCall(
		transport, "createBidirectionalStream",
	)
	if err != nil {
		closeWebTransport(transport)
		return nil, fmt.Errorf("open WebTransport stream: %w", err)
	}

	stream, err := awaitUntil(streamPromise, handshakeDeadline)
	if err != nil {
		closeWebTransport(transport)
		return nil, fmt.Errorf("open WebTransport stream: %w", err)
	}

	reader, err := safeCall(stream.Get("readable"), "getReader")
	if err != nil {
		closeWebTransport(transport)
		return nil, fmt.Errorf("create WebTransport reader: %w", err)
	}
	writer, err := safeCall(stream.Get("writable"), "getWriter")
	if err != nil {
		closeWebTransport(transport)
		return nil, fmt.Errorf("create WebTransport writer: %w", err)
	}

	c := &conn{
		transport:     transport,
		reader:        reader,
		writer:        writer,
		local:         endpointAddr("wasm"),
		remote:        remote,
		readCh:        make(chan []byte, readQueueSize),
		readDeadline:  newDeadline(),
		writeDeadline: newDeadline(),
		closed:        make(chan struct{}),
	}

	go c.readPump()
	go c.closedPump()

	return c, nil
}

func (c *conn) readPump() {
	defer close(c.readCh)

	for {
		select {
		case <-c.closed:
			return
		default:
		}

		promise, err := safeCall(c.reader, "read")
		if err != nil {
			c.shutdown(err, true)
			return
		}

		var result promiseResult
		select {
		case result = <-promiseResults(promise):
		case <-c.closed:
			return
		}
		if result.err != nil {
			c.shutdown(result.err, true)
			return
		}

		if result.value.Get("done").Bool() {
			c.shutdown(nil, true)
			return
		}

		value := result.value.Get("value")
		if value.IsUndefined() || value.IsNull() {
			continue
		}

		buf := make([]byte, value.Get("byteLength").Int())
		js.CopyBytesToGo(buf, value)
		if len(buf) == 0 {
			continue
		}

		select {
		case c.readCh <- buf:
		case <-c.closed:
			return
		}
	}
}

func (c *conn) closedPump() {
	result := <-promiseResults(c.transport.Get("closed"))
	c.shutdown(result.err, false)
}

func (c *conn) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	c.readMu.Lock()
	defer c.readMu.Unlock()

	when, _ := c.readDeadline.snapshot()
	if !when.IsZero() && time.Until(when) <= 0 {
		return 0, os.ErrDeadlineExceeded
	}

	if len(c.readBuf) > 0 {
		return c.copyReadBuffer(p), nil
	}

	for {
		chunk, err := c.nextReadChunk()
		if err != nil {
			return 0, err
		}
		if len(chunk) == 0 {
			continue
		}

		c.readBuf = chunk
		return c.copyReadBuffer(p), nil
	}
}

func (c *conn) copyReadBuffer(p []byte) int {
	n := copy(p, c.readBuf)
	c.readBuf = c.readBuf[n:]
	return n
}

func (c *conn) nextReadChunk() ([]byte, error) {
	for {
		select {
		case chunk, ok := <-c.readCh:
			if !ok {
				return nil, c.readError()
			}

			return chunk, nil
		default:
		}

		when, changed := c.readDeadline.snapshot()
		if when.IsZero() {
			select {
			case chunk, ok := <-c.readCh:
				if !ok {
					return nil, c.readError()
				}

				return chunk, nil
			case <-changed:
				continue
			}
		}

		remaining := time.Until(when)
		if remaining <= 0 {
			return nil, os.ErrDeadlineExceeded
		}

		timer := time.NewTimer(remaining)
		select {
		case chunk, ok := <-c.readCh:
			stopTimer(timer)
			if !ok {
				return nil, c.readError()
			}

			return chunk, nil
		case <-changed:
			stopTimer(timer)
			continue
		case <-timer.C:
			return nil, os.ErrDeadlineExceeded
		}
	}
}

func (c *conn) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	select {
	case <-c.closed:
		return 0, io.ErrClosedPipe
	default:
	}
	when, _ := c.writeDeadline.snapshot()
	if !when.IsZero() && time.Until(when) <= 0 {
		return 0, os.ErrDeadlineExceeded
	}

	value := js.Global().Get("Uint8Array").New(len(p))
	js.CopyBytesToJS(value, p)

	promise, err := safeCall(c.writer, "write", value)
	if err != nil {
		c.shutdown(err, true)
		return 0, err
	}

	if err := c.waitForWrite(promiseResults(promise)); err != nil {
		// Web Streams cannot cancel one writer.write call independently.
		// Close the session on an in-flight write failure or timeout so a
		// caller cannot retry bytes whose delivery is ambiguous.
		c.shutdown(err, true)
		return 0, err
	}

	return len(p), nil
}

func (c *conn) waitForWrite(results <-chan promiseResult) error {
	for {
		when, changed := c.writeDeadline.snapshot()
		if when.IsZero() {
			select {
			case result := <-results:
				return result.err
			case <-c.closed:
				return io.ErrClosedPipe
			case <-changed:
				continue
			}
		}

		remaining := time.Until(when)
		if remaining <= 0 {
			return os.ErrDeadlineExceeded
		}

		timer := time.NewTimer(remaining)
		select {
		case result := <-results:
			stopTimer(timer)
			return result.err
		case <-c.closed:
			stopTimer(timer)
			return io.ErrClosedPipe
		case <-changed:
			stopTimer(timer)
			continue
		case <-timer.C:
			return os.ErrDeadlineExceeded
		}
	}
}

func (c *conn) Close() error {
	c.shutdown(nil, true)
	return nil
}

func (c *conn) LocalAddr() net.Addr {
	return c.local
}

func (c *conn) RemoteAddr() net.Addr {
	return c.remote
}

func (c *conn) SetDeadline(t time.Time) error {
	c.readDeadline.set(t)
	c.writeDeadline.set(t)
	return nil
}

func (c *conn) SetReadDeadline(t time.Time) error {
	c.readDeadline.set(t)
	return nil
}

func (c *conn) SetWriteDeadline(t time.Time) error {
	c.writeDeadline.set(t)
	return nil
}

func (c *conn) shutdown(err error, notifyBrowser bool) {
	didClose := false
	c.closeOnce.Do(func() {
		didClose = true
		if err != nil {
			c.errMu.Lock()
			c.err = err
			c.errMu.Unlock()
		}
		close(c.closed)
	})

	if didClose && notifyBrowser {
		closeWebTransport(c.transport)
	}
}

func (c *conn) readError() error {
	c.errMu.Lock()
	defer c.errMu.Unlock()

	if c.err != nil {
		return c.err
	}

	return io.EOF
}

func awaitUntil(promise js.Value, deadline time.Time) (js.Value, error) {
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return js.Undefined(), os.ErrDeadlineExceeded
	}

	results := promiseResults(promise)
	timer := time.NewTimer(remaining)
	defer stopTimer(timer)

	select {
	case result := <-results:
		return result.value, result.err
	case <-timer.C:
		return js.Undefined(), os.ErrDeadlineExceeded
	}
}

func promiseResults(promise js.Value) <-chan promiseResult {
	results := make(chan promiseResult, 1)
	var resolve, reject js.Func

	resolve = js.FuncOf(func(_ js.Value, args []js.Value) any {
		value := js.Undefined()
		if len(args) > 0 {
			value = args[0]
		}
		results <- promiseResult{value: value}
		resolve.Release()
		reject.Release()
		return nil
	})
	reject = js.FuncOf(func(_ js.Value, args []js.Value) any {
		value := js.Undefined()
		if len(args) > 0 {
			value = args[0]
		}
		results <- promiseResult{err: errorFromJS(value)}
		resolve.Release()
		reject.Release()
		return nil
	})

	if _, err := safeCall(promise, "then", resolve, reject); err != nil {
		resolve.Release()
		reject.Release()
		results <- promiseResult{err: err}
	}

	return results
}

func closeWebTransport(transport js.Value) {
	if transport.IsUndefined() || transport.IsNull() {
		return
	}

	info := js.Global().Get("Object").New()
	info.Set("closeCode", 0)
	info.Set("reason", "")
	_, _ = safeCall(transport, "close", info)
}

func safeCall(receiver js.Value, method string, args ...any) (
	value js.Value, err error) {

	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("JavaScript %s call failed: %v", method, recovered)
		}
	}()

	return receiver.Call(method, args...), nil
}

func safeNew(constructor js.Value, args ...any) (value js.Value, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("JavaScript constructor failed: %v", recovered)
		}
	}()

	return constructor.New(args...), nil
}

func errorFromJS(value js.Value) error {
	if value.IsUndefined() || value.IsNull() {
		return fmt.Errorf("JavaScript promise rejected")
	}
	if value.Type() == js.TypeString {
		return fmt.Errorf("%s", value.String())
	}
	if value.Type() == js.TypeObject {
		message := value.Get("message")
		if message.Type() == js.TypeString {
			return fmt.Errorf("%s", message.String())
		}
	}

	return fmt.Errorf("JavaScript promise rejected: %s", value.String())
}

func stopTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

var _ net.Conn = (*conn)(nil)
