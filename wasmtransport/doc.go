// Package wasmtransport adapts the browser WebTransport API to neutrino's
// outbound net.Conn dialer.
//
// The transport uses one WebTransport session and one client-created
// bidirectional stream for each Bitcoin peer. The stream carries the raw
// Bitcoin P2P byte stream; it does not add a framing or proxy protocol.
//
// DialerConfig resolves each Bitcoin peer address to its WebTransport endpoint.
// EndpointMap provides an exact mapping suitable for neutrino's ConnectPeers
// mode. Regular Bitcoin DNS seeds and addr messages do not advertise
// WebTransport URLs, TLS identities, or HTTP paths, so unconfigured addresses
// are rejected instead of being routed to an unrelated server.
package wasmtransport
