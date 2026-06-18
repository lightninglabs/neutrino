package banman

import (
	"io"
	"net"
)

// encodeIPNet serializes the IP network into the given reader.
func encodeIPNet(w io.Writer, ipNet *net.IPNet) error {
	// Determine the appropriate network for the IP address contained in the
	// network.
	var (
		ip      []byte
		network Network
	)
	switch {
	case ipNet.IP.To4() != nil:
		ip = ipNet.IP.To4()
		network = NetworkIPv4
	case ipNet.IP.To16() != nil:
		ip = ipNet.IP.To16()
		network = NetworkIPv6
	default:
		return ErrUnsupportedIP
	}

	// Write the network first in order to properly identify it when
	// deserializing it, followed by the IP itself and its mask.
	if _, err := w.Write([]byte{byte(network)}); err != nil {
		return err
	}
	if _, err := w.Write(ip); err != nil {
		return err
	}
	if _, err := w.Write([]byte(ipNet.Mask)); err != nil {
		return err
	}

	return nil
}

// encodeKey serializes a ban key into the given writer.
func encodeKey(w io.Writer, key *Key) error {
	if key == nil {
		return ErrUnsupportedIP
	}

	switch key.Net {
	case NetworkIPv4, NetworkIPv6:
		ipNet, err := keyToIPNet(key)
		if err != nil {
			return err
		}

		return encodeIPNet(w, ipNet)

	case NetworkTorV3:
		if len(key.Addr) != torV3KeySize || len(key.Mask) != 0 {
			return ErrUnsupportedIP
		}

		if _, err := w.Write([]byte{byte(key.Net)}); err != nil {
			return err
		}

		_, err := w.Write(key.Addr)
		return err

	default:
		return ErrUnsupportedIP
	}
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

		return keyFromIPNet(ipNet)

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
