package banman

import (
	"io"
	"net"
)

// encodeIPNet serializes the IP network into the given writer using the same
// record layout as encodeKey, so a row written for an IP network and one
// written for the equivalent typed key are byte-identical.
func encodeIPNet(w io.Writer, ipNet *net.IPNet) error {
	key, err := keyFromIPNet(ipNet)
	if err != nil {
		return err
	}

	return encodeKey(w, key)
}

// encodeKey serializes a ban key into the given writer.
func encodeKey(w io.Writer, key *Key) error {
	if key == nil {
		return ErrUnsupportedIP
	}

	// keyParts is the one place a key is validated, so what gets written
	// here is exactly what every other reader of a Key accepts.
	addr, mask, err := keyParts(key)
	if err != nil {
		return err
	}

	// The family byte comes first so the record can be identified when it
	// is read back, followed by the address and, for an IP key, its mask.
	if _, err := w.Write([]byte{byte(key.Net)}); err != nil {
		return err
	}
	if _, err := w.Write(addr); err != nil {
		return err
	}
	if mask == nil {
		return nil
	}

	_, err = w.Write(mask)

	return err
}

// decodeIPNet deserialized an IP network from the given reader.
func decodeIPNet(r io.Reader) (*net.IPNet, error) {
	var network [1]byte
	if _, err := io.ReadFull(r, network[:]); err != nil {
		return nil, err
	}

	return decodeIPNetWithNetwork(r, Network(network[0]))
}

// decodeIPNetWithNetwork deserializes an IP network after its family byte has
// already been read.
func decodeIPNetWithNetwork(r io.Reader, network Network) (*net.IPNet, error) {
	var ipLen int
	switch network {
	case NetworkIPv4:
		ipLen = net.IPv4len
	case NetworkIPv6:
		ipLen = net.IPv6len
	default:
		return nil, ErrUnsupportedIP
	}

	ip := make([]byte, ipLen)
	if _, err := io.ReadFull(r, ip); err != nil {
		return nil, err
	}
	mask := make([]byte, ipLen)
	if _, err := io.ReadFull(r, mask); err != nil {
		return nil, err
	}

	return &net.IPNet{IP: ip, Mask: mask}, nil
}

// decodeKey deserializes a ban key from the given reader.
func decodeKey(r io.Reader) (*Key, error) {
	var network [1]byte
	if _, err := io.ReadFull(r, network[:]); err != nil {
		return nil, err
	}

	switch Network(network[0]) {
	case NetworkIPv4, NetworkIPv6:
		ipNet, err := decodeIPNetWithNetwork(r, Network(network[0]))
		if err != nil {
			return nil, err
		}

		// A record's family byte has to agree with the width of the
		// address it carries, so that a decoded key is one encodeKey
		// will accept.
		key, err := keyFromIPNet(ipNet)
		if err != nil {
			return nil, err
		}

		if key.Net != Network(network[0]) {
			return nil, ErrUnsupportedIP
		}

		return key, nil

	case NetworkTorV3:
		key := &Key{
			Net:  NetworkTorV3,
			Addr: make([]byte, torV3KeySize),
		}
		if _, err := io.ReadFull(r, key.Addr); err != nil {
			return nil, err
		}

		return key, nil

	default:
		return nil, ErrUnsupportedIP
	}
}
