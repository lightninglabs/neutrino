package banman

import (
	"bytes"
	"net"
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

// parseIPKey parses an IPv4 or IPv6 host into a typed ban key.
func parseIPKey(host string, mask net.IPMask) (*Key, error) {
	ip := net.ParseIP(host)
	switch {
	case ip.To4() != nil:
		ip = ip.To4()
		if mask == nil {
			mask = defaultIPv4Mask
		}

		maskedIP := ip.Mask(mask)
		if maskedIP == nil {
			return nil, ErrUnsupportedIP
		}

		return &Key{
			Net:  NetworkIPv4,
			Addr: bytes.Clone(maskedIP),
			Mask: bytes.Clone([]byte(mask)),
		}, nil

	case ip.To16() != nil:
		ip = ip.To16()
		if mask == nil {
			mask = defaultIPv6Mask
		}

		maskedIP := ip.Mask(mask)
		if maskedIP == nil {
			return nil, ErrUnsupportedIP
		}

		return &Key{
			Net:  NetworkIPv6,
			Addr: bytes.Clone(maskedIP),
			Mask: bytes.Clone([]byte(mask)),
		}, nil

	default:
		return nil, ErrUnsupportedIP
	}
}
