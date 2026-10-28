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

	// NetworkTorV3 represents a Tor v3 pubkey without a mask.
	NetworkTorV3 Network = 2
)

// String returns the human-readable name of a network.
func (n Network) String() string {
	switch n {
	case NetworkIPv4:
		return "ipv4"

	case NetworkIPv6:
		return "ipv6"

	case NetworkTorV3:
		return "torv3"

	default:
		return fmt.Sprintf("network(%d)", n)
	}
}

// Key identifies a banned peer address or overlay network identity.
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

// networkForAddr returns the network an address payload belongs to. Only IP
// payloads reach this: a Tor v3 payload has no family to infer from its width.
func networkForAddr(addr []byte) Network {
	if len(addr) == net.IPv4len {
		return NetworkIPv4
	}

	return NetworkIPv6
}

// isAllOnes reports whether every byte of b is 0xff.
func isAllOnes(b []byte) bool {
	for _, v := range b {
		if v != 0xff {
			return false
		}
	}

	return true
}

// ipNetParts reduces an address and its mask to the pieces of a ban key: the
// address at the byte width of its own family, and the mask at that same
// width.
//
// The family comes from the address that will be stored, which is the rule the
// encoder has always applied, and the address is stored as given. Masking is
// the caller's job: ParseIPNet masks before it builds the key, so a mask wide
// enough to discard a v4 mapping leaves an IPv6 network behind and that is what
// gets keyed.
//
// Two pairings are refused here because the record they describe cannot be read
// back, and both need an explicit mask to reach -- every ParseIPNet call in the
// tree passes nil. A 4-byte address beside a 16-byte mask was written as a
// 21-byte record: the decoder reads as many mask bytes as the address is wide,
// so it read that back as the 9-byte form, and a ban recorded through one
// spelling of an address was invisible to Status through the other, which made
// a ban evadable by reconnecting with the other spelling. The leading 12
// all-ones bytes are the ones net.IP.Mask itself discards, so a mask that
// begins that way is trimmed to the 4 bytes that apply, describing exactly the
// IPv4 network the caller asked for; one that does not begin that way cannot
// describe an IPv4 network at all. An IPv6 address beside a 4-byte mask was
// accepted by the parser and then refused by the encoder, so no record could be
// written for it; it is refused where the mistake was made instead.
func ipNetParts(ip net.IP, mask net.IPMask) ([]byte, net.IPMask, error) {
	addr := ip.To4()
	if addr == nil {
		addr = ip.To16()
	}
	if addr == nil {
		return nil, nil, ErrUnsupportedIP
	}

	if len(mask) == len(addr) {
		return addr, mask, nil
	}

	if len(addr) == net.IPv4len && len(mask) == net.IPv6len &&
		isAllOnes(mask[:net.IPv6len-net.IPv4len]) {

		return addr, mask[net.IPv6len-net.IPv4len:], nil
	}

	return nil, nil, ErrUnsupportedIP
}

// keyFromIPNet converts an IP network into a typed ban key.
func keyFromIPNet(ipNet *net.IPNet) (*Key, error) {
	if ipNet == nil {
		return nil, ErrUnsupportedIP
	}

	addr, mask, err := ipNetParts(ipNet.IP, ipNet.Mask)
	if err != nil {
		return nil, err
	}

	return &Key{
		Net:  networkForAddr(addr),
		Addr: bytes.Clone(addr),
		Mask: bytes.Clone(mask),
	}, nil
}

// keyParts validates a key and returns the address payload and mask it
// carries. An IP key's mask is at the byte width of its own family, and a Tor
// v3 key carries no mask, so a nil mask is what tells the two shapes apart
// rather than a separate flag.
//
// Every reader of a Key goes through here, so a key that this returns is
// exactly the key that gets encoded: there is no second, laxer path by which a
// key could be stored in a form the store cannot read back.
func keyParts(key *Key) ([]byte, net.IPMask, error) {
	if key == nil {
		return nil, nil, ErrUnsupportedIP
	}

	switch key.Net {
	case NetworkTorV3:
		if len(key.Addr) != torV3KeySize || len(key.Mask) != 0 {
			return nil, nil, ErrUnsupportedIP
		}

		return bytes.Clone(key.Addr), nil, nil

	case NetworkIPv4, NetworkIPv6:

	default:
		return nil, nil, ErrUnsupportedIP
	}

	addr, mask, err := ipNetParts(key.Addr, key.Mask)
	if err != nil {
		return nil, nil, err
	}

	// The family byte has to agree with the address the key carries, or
	// the key would re-encode under a different one.
	if networkForAddr(addr) != key.Net {
		return nil, nil, ErrUnsupportedIP
	}

	return addr, mask, nil
}

// keyToIPNet converts a typed key back into an IP network. A Tor v3 key has
// no IP form, so it is refused.
func keyToIPNet(key *Key) (*net.IPNet, error) {
	addr, mask, err := keyParts(key)
	if err != nil {
		return nil, err
	}

	if mask == nil {
		return nil, ErrUnsupportedIP
	}

	return &net.IPNet{
		IP:   net.IP(bytes.Clone(addr)),
		Mask: net.IPMask(bytes.Clone(mask)),
	}, nil
}
