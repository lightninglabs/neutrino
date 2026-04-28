package banman

import (
	"bytes"
	"fmt"
	"net"
)

// Network identifies the address family represented by a ban key.
//
// NOTE: These values are part of the on-disk encoding and must remain stable.
type Network byte

const (
	// NetworkIPv4 represents an IPv4 address and mask.
	NetworkIPv4 Network = 0

	// NetworkIPv6 represents an IPv6 address and mask.
	NetworkIPv6 Network = 1
)

// String returns the human-readable name of a network.
func (n Network) String() string {
	switch n {
	case NetworkIPv4:
		return "ipv4"

	case NetworkIPv6:
		return "ipv6"

	default:
		return fmt.Sprintf("network(%d)", n)
	}
}

// Key identifies a banned peer address.
type Key struct {
	// Net describes which address family is used by the key.
	Net Network

	// Addr contains the network-specific address payload.
	Addr []byte

	// Mask contains the network mask for IP-based keys.
	Mask []byte
}

// String returns a human-readable representation of a ban key.
func (k *Key) String() string {
	if k == nil {
		return "<nil>"
	}

	ipNet, err := keyToIPNet(k)
	if err == nil {
		return ipNet.String()
	}

	return fmt.Sprintf("%v:%x", k.Net, k.Addr)
}

// keyFromIPNet converts an IP network into a typed ban key.
func keyFromIPNet(ipNet *net.IPNet) (*Key, error) {
	if ipNet == nil {
		return nil, ErrUnsupportedIP
	}

	switch {
	case ipNet.IP.To4() != nil:
		return &Key{
			Net:  NetworkIPv4,
			Addr: bytes.Clone(ipNet.IP.To4()),
			Mask: bytes.Clone([]byte(ipNet.Mask)),
		}, nil

	case ipNet.IP.To16() != nil:
		return &Key{
			Net:  NetworkIPv6,
			Addr: bytes.Clone(ipNet.IP.To16()),
			Mask: bytes.Clone([]byte(ipNet.Mask)),
		}, nil

	default:
		return nil, ErrUnsupportedIP
	}
}

// keyToIPNet converts a typed key back into an IP network.
func keyToIPNet(key *Key) (*net.IPNet, error) {
	if key == nil {
		return nil, ErrUnsupportedIP
	}

	switch key.Net {
	case NetworkIPv4:
		if len(key.Addr) != net.IPv4len ||
			len(key.Mask) != net.IPv4len {

			return nil, ErrUnsupportedIP
		}

	case NetworkIPv6:
		if len(key.Addr) != net.IPv6len ||
			len(key.Mask) != net.IPv6len {

			return nil, ErrUnsupportedIP
		}

	default:
		return nil, ErrUnsupportedIP
	}

	return &net.IPNet{
		IP:   net.IP(bytes.Clone(key.Addr)),
		Mask: net.IPMask(bytes.Clone(key.Mask)),
	}, nil
}
