package banman

import (
	"bytes"
	"net"
	"reflect"
	"testing"
)

// TestIPNetSerialization ensures that we can serialize different supported IP
// networks and deserialize them into their expected result.
func TestIPNetSerialization(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name  string
		ipNet *net.IPNet
		err   error
	}{
		{
			name: "ipv4 without mask",
			ipNet: &net.IPNet{
				IP:   net.ParseIP("172.217.6.46"),
				Mask: net.IPv4Mask(0x00, 0x00, 0x00, 0x00),
			},
		},
		{
			name: "ipv4 with default mask",
			ipNet: &net.IPNet{
				IP:   net.ParseIP("172.217.6.46"),
				Mask: defaultIPv4Mask,
			},
		},
		{
			name: "ipv4 with non-default mask",
			ipNet: &net.IPNet{
				IP:   net.ParseIP("172.217.6.46"),
				Mask: net.IPv4Mask(0xff, 0xff, 0x00, 0x00),
			},
		},
		{
			name: "ipv6 without mask",
			ipNet: &net.IPNet{
				IP:   net.ParseIP("2001:db8:a0b:12f0::1"),
				Mask: net.IPMask(make([]byte, net.IPv6len)),
			},
		},
		{
			name: "ipv6 with default mask",
			ipNet: &net.IPNet{
				IP:   net.ParseIP("2001:db8:a0b:12f0::1"),
				Mask: defaultIPv6Mask,
			},
		},
		{
			name: "ipv6 with non-default mask",
			ipNet: &net.IPNet{
				IP: net.ParseIP("2001:db8:a0b:12f0::1"),
				Mask: net.IPMask([]byte{
					0xff, 0xff, 0x00, 0x00, 0x00, 0xff,
					0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
					0x00, 0x00, 0x00, 0x00,
				}),
			},
		},
	}

	for _, testCase := range testCases {
		success := t.Run(testCase.name, func(t *testing.T) {
			// Serialize the IP network and deserialize it back.
			// We'll do this to ensure we are properly serializing
			// and deserializing them.
			var b bytes.Buffer
			err := encodeIPNet(&b, testCase.ipNet)
			if testCase.err != nil && err != testCase.err {
				t.Fatalf("encoding IP network %v expected "+
					"error \"%v\", got \"%v\"",
					testCase.ipNet, testCase.err, err)
			}
			ipNet, err := decodeIPNet(&b)
			if testCase.err != nil && err != testCase.err {
				t.Fatalf("decoding IP network %v expected "+
					"error \"%v\", got \"%v\"",
					testCase.ipNet, testCase.err, err)
			}

			// If the test did not expect a result, i.e., an invalid
			// IP network, then we can exit now.
			if testCase.err != nil {
				return
			}

			// Otherwise, ensure the result is what we expect.
			if !ipNet.IP.Equal(testCase.ipNet.IP) {
				t.Fatalf("expected IP %v, got %v",
					testCase.ipNet.IP, ipNet.IP)
			}
			if !reflect.DeepEqual(ipNet.Mask, testCase.ipNet.Mask) {
				t.Fatalf("expected mask %#v, got %#v",
					testCase.ipNet.Mask, ipNet.Mask)
			}
		})
		if !success {
			return
		}
	}
}
