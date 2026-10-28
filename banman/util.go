package banman

import (
	"bytes"
	"encoding/base32"
	"fmt"
	"net"
	"strings"

	"github.com/btcsuite/btcd/wire/v2"
	"golang.org/x/crypto/sha3"
)

var (
	// defaultIPv4Mask is the default IPv4 mask used when parsing IP
	// networks from an address. This ensures that the IP network only
	// contains *one* IP address -- the one specified.
	defaultIPv4Mask = net.CIDRMask(32, 32)

	// defaultIPv6Mask is the default IPv6 mask used when parsing IP
	// networks from an address. This ensures that the IP network only
	// contains *one* IP address -- the one specified.
	defaultIPv6Mask = net.CIDRMask(128, 128)
)

const (
	// onionSuffix is the suffix shared by all Tor onion addresses.
	onionSuffix = ".onion"

	// torV3ChecksumPrefix is included in the Tor v3 checksum preimage.
	torV3ChecksumPrefix = ".onion checksum"

	// torV3Version identifies a Tor v3 address payload.
	torV3Version = byte(0x03)

	// torV3KeySize is the number of pubkey bytes encoded by a Tor v3 host.
	torV3KeySize = wire.TorV3Size

	// torV3ChecksumSize is the checksum length encoded in a Tor v3 host.
	torV3ChecksumSize = 2

	// torV3PayloadSize is the decoded Tor v3 payload size.
	torV3PayloadSize = torV3KeySize + torV3ChecksumSize + 1
)

// ParseIPNet parses the IP network that contains the given address. An optional
// mask can be provided, to expand the scope of the IP network, otherwise the
// IP's default is used.
//
// NOTE: This assumes that the address has already been resolved.
func ParseIPNet(addr string, mask net.IPMask) (*net.IPNet, error) {
	key, err := parseIPKey(parseHost(addr), mask)
	if err != nil {
		return nil, err
	}

	return keyToIPNet(key)
}

// ParseAddr parses the peer address into a typed ban key.
func ParseAddr(addr string) (*Key, error) {
	host := strings.TrimSpace(parseHost(addr))
	lowerHost := strings.ToLower(host)

	switch {
	case strings.HasSuffix(lowerHost, onionSuffix):
		return parseTorV3Key(lowerHost)

	default:
		return parseIPKey(host, nil)
	}
}

// parseHost extracts the host from an address, ignoring the port when present.
// It also strips IPv6 brackets so bracketed hosts behave the same with and
// without an explicit port.
func parseHost(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}

	if len(host) >= 2 && host[0] == '[' && host[len(host)-1] == ']' {
		return host[1 : len(host)-1]
	}

	return host
}

// parseIPKey parses an IPv4 or IPv6 host into a typed ban key. When no mask is
// given the network covers exactly the one address, so that banning a peer
// does not also ban its neighbours.
func parseIPKey(host string, mask net.IPMask) (*Key, error) {
	ip := net.ParseIP(host)
	if ip == nil {
		return nil, ErrUnsupportedIP
	}

	// The family decides which default applies, so ask the address how it
	// will be stored rather than how wide the mask happens to be: a
	// v4-mapped address takes the IPv4 default, not the IPv6 one its
	// 16-byte form would suggest.
	if mask == nil {
		if ip.To4() != nil {
			mask = defaultIPv4Mask
		} else {
			mask = defaultIPv6Mask
		}
	}

	// Mask the original, not the narrowed form: a 16-byte mask is applied
	// to the 16-byte address it was written for, and a mask wide enough to
	// discard a v4 mapping is meant to yield the IPv6 network it leaves
	// behind. Masking the narrowed address instead would refuse every mask
	// wider than 32 bits.
	masked := ip.Mask(mask)
	if masked == nil {
		return nil, ErrUnsupportedIP
	}

	return keyFromIPNet(&net.IPNet{
		IP:   masked,
		Mask: mask,
	})
}

// parseTorV3Key parses a Tor v3 onion address into a typed ban key.
//
// TODO: this code is a better fit for btcd/addrmgr. Move it there.
func parseTorV3Key(host string) (*Key, error) {
	if len(host) != wire.TorV3EncodedSize {
		return nil, fmt.Errorf("invalid tor v3 address length: %d",
			len(host))
	}

	encodedHost := host[:len(host)-len(onionSuffix)]
	payload, err := base32.StdEncoding.DecodeString(
		strings.ToUpper(encodedHost),
	)
	if err != nil {
		return nil, fmt.Errorf("invalid tor v3 address: %w", err)
	}
	if len(payload) != torV3PayloadSize {
		return nil, fmt.Errorf("invalid tor v3 payload length: %d",
			len(payload))
	}

	pubKey := payload[:torV3KeySize]
	checksum := payload[torV3KeySize : torV3KeySize+torV3ChecksumSize]
	version := payload[torV3PayloadSize-1]

	if version != torV3Version {
		return nil, fmt.Errorf("invalid tor v3 version: %x", version)
	}

	expectedChecksum := torV3Checksum(pubKey)
	if !bytes.Equal(checksum, expectedChecksum[:]) {
		return nil, fmt.Errorf("invalid tor v3 checksum")
	}

	return &Key{
		Net:  NetworkTorV3,
		Addr: bytes.Clone(pubKey),
	}, nil
}

// torV3Checksum computes the BIP-155 checksum for a Tor v3 pubkey.
func torV3Checksum(pubKey []byte) [torV3ChecksumSize]byte {
	h := sha3.New256()

	// Write never returns an error, so there is no need to handle it.
	h.Write([]byte(torV3ChecksumPrefix))
	h.Write(pubKey)
	h.Write([]byte{torV3Version})

	var checksum [torV3ChecksumSize]byte
	copy(checksum[:], h.Sum(nil))

	return checksum
}
