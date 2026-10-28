package banman

import (
	"bytes"
	"net"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
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

// TestIPNetEncodingBytes ensures that IPv4 and IPv6 networks retain their
// current on-disk encoding.
func TestIPNetEncodingBytes(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		ipNet   *net.IPNet
		encoded []byte
	}{
		{
			name: "ipv4",
			ipNet: &net.IPNet{
				IP:   net.ParseIP("172.217.6.46"),
				Mask: net.IPv4Mask(0xff, 0xff, 0x00, 0x00),
			},
			encoded: []byte{
				0x00,
				0xac, 0xd9, 0x06, 0x2e,
				0xff, 0xff, 0x00, 0x00,
			},
		},
		{
			name: "ipv6",
			ipNet: &net.IPNet{
				IP: net.ParseIP("2001:db8:a0b:12f0::1"),
				Mask: net.IPMask([]byte{
					0xff, 0xff, 0x00, 0x00, 0x00, 0xff,
					0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
					0x00, 0x00, 0x00, 0x00,
				}),
			},
			encoded: []byte{
				0x01,
				0x20, 0x01, 0x0d, 0xb8,
				0x0a, 0x0b, 0x12, 0xf0,
				0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x01,
				0xff, 0xff, 0x00, 0x00,
				0x00, 0xff, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00,
			},
		},
	}

	for _, testCase := range testCases {
		success := t.Run(testCase.name, func(t *testing.T) {
			var b bytes.Buffer
			err := encodeIPNet(&b, testCase.ipNet)
			require.NoError(t, err)
			require.Equal(t, testCase.encoded, b.Bytes())
		})
		if !success {
			return
		}
	}
}

// TestKeySerialization ensures that typed ban keys round trip through the
// codec.
func TestKeySerialization(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name string
		key  *Key
	}{
		{
			name: "ipv4",
			key: &Key{
				Net:  NetworkIPv4,
				Addr: []byte{0xac, 0xd9, 0x06, 0x2e},
				Mask: []byte{0xff, 0xff, 0x00, 0x00},
			},
		},
		{
			name: "ipv6",
			key: &Key{
				Net: NetworkIPv6,
				Addr: []byte{
					0x20, 0x01, 0x0d, 0xb8,
					0x0a, 0x0b, 0x12, 0xf0,
					0x00, 0x00, 0x00, 0x00,
					0x00, 0x00, 0x00, 0x01,
				},
				Mask: []byte{
					0xff, 0xff, 0x00, 0x00, 0x00, 0xff,
					0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
					0x00, 0x00, 0x00, 0x00,
				},
			},
		},
		{
			name: "torv3",
			key: &Key{
				Net:  NetworkTorV3,
				Addr: testTorV3PubKey,
			},
		},
	}

	for _, testCase := range testCases {
		success := t.Run(testCase.name, func(t *testing.T) {
			var b bytes.Buffer
			err := encodeKey(&b, testCase.key)
			require.NoError(t, err)

			key, err := decodeKey(&b)
			require.NoError(t, err)
			require.Equal(t, testCase.key, key)
		})
		if !success {
			return
		}
	}
}

// TestTorV3KeyEncodingBytes ensures that Tor v3 keys retain their expected
// on-disk encoding.
func TestTorV3KeyEncodingBytes(t *testing.T) {
	t.Parallel()

	key := &Key{
		Net:  NetworkTorV3,
		Addr: testTorV3PubKey,
	}

	expected := append([]byte{0x02}, testTorV3PubKey...)

	var b bytes.Buffer
	err := encodeKey(&b, key)
	require.NoError(t, err)
	require.Equal(t, expected, b.Bytes())
}

// TestDecodeKeyRejectsUnencodableRecords ensures that decodeKey refuses any
// record it could not write back out. The store never decodes keys today, but
// a decoder laxer than the encoder is how a record that the encoder rejects
// ends up in the database, so the two are held to the same shape.
func TestDecodeKeyRejectsUnencodableRecords(t *testing.T) {
	t.Parallel()

	// A v4-mapped address carries 16 bytes but belongs to the IPv4
	// family, so a record that states the IPv6 family for it decodes to a
	// key the encoder would re-encode under a different family byte.
	_, mapped, err := net.ParseCIDR("::ffff:1.2.3.4/128")
	require.NoError(t, err)

	mismatchedFamily := append(
		[]byte{byte(NetworkIPv6)}, mapped.IP...,
	)
	mismatchedFamily = append(mismatchedFamily, mapped.Mask...)

	testCases := []struct {
		name   string
		record []byte
	}{
		{
			name: "ipv6 family byte with a 4-byte address",
			record: []byte{
				byte(NetworkIPv6),
				0x01, 0x02, 0x03, 0x04,
			},
		},
		{
			name:   "ipv6 family byte with a v4-mapped address",
			record: mismatchedFamily,
		},
		{
			name: "truncated torv3 payload",
			record: append(
				[]byte{byte(NetworkTorV3)}, make([]byte, 4)...,
			),
		},
		{
			name:   "unknown family byte",
			record: []byte{0x07},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			key, err := decodeKey(bytes.NewReader(testCase.record))
			require.Error(t, err)
			require.Nil(t, key)
		})
	}
}

// TestDecodeKeyAcceptsEverythingEncodeKeyWrites is the converse of the test
// above: whatever encodeKey emits, decodeKey reads back into a key that
// encodes to the same bytes.
func TestDecodeKeyAcceptsEverythingEncodeKeyWrites(t *testing.T) {
	t.Parallel()

	testCases := map[string]*Key{
		"ipv4": {
			Net:  NetworkIPv4,
			Addr: []byte{0x01, 0x02, 0x03, 0x04},
			Mask: []byte(defaultIPv4Mask),
		},
		"ipv4 unpadded network": {
			Net:  NetworkIPv4,
			Addr: []byte{0x01, 0x02, 0x00, 0x00},
			Mask: []byte{0xff, 0xff, 0x00, 0x00},
		},
		"ipv6": {
			Net:  NetworkIPv6,
			Addr: bytes.Repeat([]byte{0x01}, net.IPv6len),
			Mask: []byte(defaultIPv6Mask),
		},
		"torv3": {
			Net:  NetworkTorV3,
			Addr: testTorV3PubKey,
		},
	}

	for name, key := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			var encoded bytes.Buffer
			require.NoError(t, encodeKey(&encoded, key))

			// Keep the record aside: decoding consumes the reader.
			want := bytes.Clone(encoded.Bytes())

			decoded, err := decodeKey(bytes.NewReader(want))
			require.NoError(t, err)

			var got bytes.Buffer
			require.NoError(t, encodeKey(&got, decoded))
			require.Equal(t, want, got.Bytes())
		})
	}
}
