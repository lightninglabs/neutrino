# Browser WebTransport integration test

This opt-in test runs a complete Neutrino `ChainService` as Go WASM in a real
Chrome browser. It connects to a native simnet btcd process over WebTransport,
synchronizes block and regular filter headers, and then proves that both tips
advance after btcd mines more blocks. At each tip, it also fetches a compact
filter through Neutrino and compares its bytes with btcd's RPC result.

Build the paired btcd WebTransport branch, then run:

```bash
BTCD_WEBTRANSPORT_BIN=/absolute/path/to/btcd \
  go test -tags=rpctest ./itest/webtransport -run TestWebTransportBrowserChainServiceSync -v
```

The test searches common Chrome and Chromium locations. Set `BTCD_CHROME_BIN`
to an absolute browser executable path when automatic discovery is unsuitable.
The WebTransport connection authenticates its short-lived test certificate by
SHA-256 hash; the test does not disable TLS verification.
