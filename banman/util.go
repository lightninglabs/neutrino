package banman

import (
	"crypto/sha256"
	"encoding/base32"
	"net"
	"strings"
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

	// onionCatPrefix is the IPv6 prefix used for OnionCat/Tor addresses.
	onionCatPrefix = [6]byte{0xfd, 0x87, 0xd8, 0x7e, 0xeb, 0x43}

	// noPaddingBase32 is used for decoding unpadded .onion labels.
	noPaddingBase32 = base32.StdEncoding.WithPadding(base32.NoPadding)
)

// ParseIPNet parses the IP network that contains the given address. An optional
// mask can be provided, to expand the scope of the IP network, otherwise the
// IP's default is used.
//
// NOTE: This assumes that the address has already been resolved.
func ParseIPNet(addr string, mask net.IPMask) (*net.IPNet, error) {
	// If the address includes a port, we'll remove it.
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		// Address doesn't include a port.
		host = addr
	}

	// Parse the IP from the host to ensure it is supported.
	ip := net.ParseIP(host)
	if ip == nil {
		ip = parseOnionHost(host)
	}

	switch {
	case ip.To4() != nil:
		if mask == nil {
			mask = defaultIPv4Mask
		}
	case ip.To16() != nil:
		if mask == nil {
			mask = defaultIPv6Mask
		}
	default:
		return nil, ErrUnsupportedIP
	}

	return &net.IPNet{IP: ip.Mask(mask), Mask: mask}, nil
}

func parseOnionHost(host string) net.IP {
	host = strings.ToLower(strings.TrimSpace(host))
	if !strings.HasSuffix(host, ".onion") {
		return nil
	}

	label := strings.ToUpper(strings.TrimSuffix(host, ".onion"))
	switch len(label) {
	case 16:
		decoded, err := noPaddingBase32.DecodeString(label)
		if err != nil || len(decoded) != 10 {
			return nil
		}

		return onionCatIP(decoded)

	case 56:
		decoded, err := noPaddingBase32.DecodeString(label)
		if err != nil || len(decoded) != 35 || decoded[34] != 0x03 {
			return nil
		}

		sum := sha256.Sum256(decoded[:32])
		return onionCatIP(sum[:10])

	default:
		return nil
	}
}

func onionCatIP(suffix []byte) net.IP {
	ip := make(net.IP, net.IPv6len)
	copy(ip[:6], onionCatPrefix[:])
	copy(ip[6:], suffix[:10])
	return ip
}
