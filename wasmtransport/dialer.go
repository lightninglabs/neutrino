package wasmtransport

import (
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"
)

const (
	// DefaultHandshakeTimeout is the maximum time a browser dial waits for
	// the WebTransport session and its bidirectional stream to become ready.
	DefaultHandshakeTimeout = 30 * time.Second

	certificateHashLength = 32
)

// PeerEndpoint describes the WebTransport endpoint for one Bitcoin peer.
type PeerEndpoint struct {
	// URL is the HTTPS WebTransport URL exposed by the peer.
	URL string

	// ServerCertificateHashes contains optional SHA-256 hashes of acceptable
	// leaf certificates. Browser WebTransport applies additional validity and
	// key restrictions to certificates authenticated this way.
	ServerCertificateHashes [][]byte
}

// EndpointResolver maps a Bitcoin peer address to its WebTransport endpoint.
// A resolver must reject peers it doesn't recognize. Bitcoin DNS seeds and
// addr messages don't advertise WebTransport endpoints or TLS identities, so
// silently using one fixed endpoint for every peer would misattribute the
// connection to the address neutrino asked to dial.
type EndpointResolver func(net.Addr) (PeerEndpoint, error)

// EndpointMap is an exact peer-address-to-WebTransport-endpoint mapping. Keys
// are net.Addr.String values, such as "192.0.2.1:8333".
type EndpointMap map[string]PeerEndpoint

// Resolve returns the configured endpoint for peer.
func (m EndpointMap) Resolve(peer net.Addr) (PeerEndpoint, error) {
	if peer == nil {
		return PeerEndpoint{}, fmt.Errorf("WebTransport peer address is nil")
	}

	endpoint, ok := m[peer.String()]
	if !ok {
		return PeerEndpoint{}, fmt.Errorf(
			"no WebTransport endpoint configured for peer %s", peer,
		)
	}

	return endpoint, nil
}

// DialerConfig configures a browser WebTransport dialer. Each dial creates one
// WebTransport session and one bidirectional stream. The stream is exposed as
// a net.Conn carrying the Bitcoin P2P byte stream without additional framing.
type DialerConfig struct {
	// ResolveEndpoint returns the endpoint for the exact peer being dialed.
	ResolveEndpoint EndpointResolver

	// HandshakeTimeout limits session and stream establishment. A zero value
	// uses DefaultHandshakeTimeout.
	HandshakeTimeout time.Duration
}

type endpointAddr string

func (endpointAddr) Network() string {
	return "webtransport"
}

func (a endpointAddr) String() string {
	return string(a)
}

func isOnionAddr(addr net.Addr) bool {
	if addr == nil {
		return false
	}
	if addr.Network() == "onion" {
		return true
	}

	host := addr.String()
	if parsedHost, _, err := net.SplitHostPort(host); err == nil {
		host = parsedHost
	}

	return strings.HasSuffix(
		strings.ToLower(strings.Trim(host, "[]")), ".onion",
	)
}

// NewDialer returns a neutrino-compatible outbound connection dialer backed by
// the browser WebTransport API.
func NewDialer(cfg DialerConfig) (func(net.Addr) (net.Conn, error), error) {
	validated, err := validateDialerConfig(cfg)
	if err != nil {
		return nil, err
	}

	return newDialer(validated), nil
}

func validateDialerConfig(cfg DialerConfig) (DialerConfig, error) {
	if cfg.ResolveEndpoint == nil {
		return DialerConfig{}, fmt.Errorf(
			"WebTransport endpoint resolver is required",
		)
	}

	if cfg.HandshakeTimeout < 0 {
		return DialerConfig{}, fmt.Errorf("WebTransport handshake timeout cannot be negative")
	}
	if cfg.HandshakeTimeout == 0 {
		cfg.HandshakeTimeout = DefaultHandshakeTimeout
	}

	return cfg, nil
}

func validatePeerEndpoint(cfg PeerEndpoint) (PeerEndpoint, error) {
	endpoint, err := url.Parse(cfg.URL)
	if err != nil {
		return PeerEndpoint{}, fmt.Errorf("invalid WebTransport endpoint: %w", err)
	}
	if endpoint.Scheme != "https" {
		return PeerEndpoint{}, fmt.Errorf("WebTransport endpoint must use https")
	}
	if endpoint.Host == "" {
		return PeerEndpoint{}, fmt.Errorf("WebTransport endpoint must include a host")
	}
	if endpoint.User != nil {
		return PeerEndpoint{}, fmt.Errorf("WebTransport endpoint must not include user info")
	}
	if endpoint.Fragment != "" {
		return PeerEndpoint{}, fmt.Errorf("WebTransport endpoint must not include a fragment")
	}

	hashes := make([][]byte, len(cfg.ServerCertificateHashes))
	for i, hash := range cfg.ServerCertificateHashes {
		if len(hash) != certificateHashLength {
			return PeerEndpoint{}, fmt.Errorf("server certificate hash %d must be %d bytes", i, certificateHashLength)
		}

		hashes[i] = append([]byte(nil), hash...)
	}

	cfg.URL = endpoint.String()
	cfg.ServerCertificateHashes = hashes

	return cfg, nil
}
