package banman

import (
	"errors"
	"net"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var testTorV3PubKey = []byte{
	0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
	0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
	0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
	0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
}

const (
	testTorV3Host = "aaaqeayeaudaocajbifqydiob4ibceqtcqkrmfyydenbwha5" +
		"dyp3kead.onion"
	testTorV3BadChecksumHost = "aaaqeayeaudaocajbifqydiob4ibceqtcqkrm" +
		"fyydenbwha5dypqaaad.onion"
	testTorV3BadVersionHost = "aaaqeayeaudaocajbifqydiob4ibceqtcqkrmf" +
		"yydenbwha5dyp3keae.onion"
)

// TestParseIPNet ensures that we can parse different combinations of
// IPs/addresses and masks.
func TestParseIPNet(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name   string
		addr   string
		mask   net.IPMask
		result *net.IPNet
		err    error
	}{
		{
			name: "ipv4 with default mask",
			addr: "192.168.1.1",
			mask: nil,
			result: &net.IPNet{
				IP:   net.ParseIP("192.168.1.1"),
				Mask: defaultIPv4Mask,
			},
		},
		{
			name: "ipv4 with port and non-default mask",
			addr: "192.168.1.1:80",
			mask: net.CIDRMask(16, 32),
			result: &net.IPNet{
				IP:   net.ParseIP("192.168.0.0"),
				Mask: net.CIDRMask(16, 32),
			},
		},
		{
			name: "ipv6 with port and default mask",
			addr: "[2001:db8:a0b:12f0::1]:80",
			mask: nil,
			result: &net.IPNet{
				IP:   net.ParseIP("2001:db8:a0b:12f0::1"),
				Mask: defaultIPv6Mask,
			},
		},
		{
			name: "ipv6 with non-default mask",
			addr: "2001:db8:a0b:12f0::1",
			mask: net.CIDRMask(32, 128),
			result: &net.IPNet{
				IP:   net.ParseIP("2001:db8::"),
				Mask: net.CIDRMask(32, 128),
			},
		},
		{
			name: "onion address with port",
			addr: "52kldxh5vexi3gmwnbfwoa5ixx65icsuceattoqc2szoh" +
				"owfibzruyid.onion:8333",
			err: ErrUnsupportedIP,
		},
	}

	for _, testCase := range testCases {
		success := t.Run(testCase.name, func(t *testing.T) {
			// Parse the IP network from each test's address and
			// mask.
			ipNet, err := ParseIPNet(testCase.addr, testCase.mask)
			if testCase.err != nil {
				if !errors.Is(err, testCase.err) {
					t.Fatalf("expected error %v, got %v",
						testCase.err, err)
				}
				return
			}
			if testCase.result != nil && err != nil {
				t.Fatalf("unable to parse IP network for "+
					"addr=%v and mask=%v: %v",
					testCase.addr, testCase.mask, err)
			}

			// If the test did not expect a result, i.e., an invalid
			// IP network, then we can exit now.
			if testCase.result == nil {
				return
			}

			// Otherwise, ensure the result is what we expect.
			if !ipNet.IP.Equal(testCase.result.IP) {
				t.Fatalf("expected IP %v, got %v",
					testCase.result.IP, ipNet.IP)
			}
			require.Equal(
				t, testCase.result.Mask, ipNet.Mask,
			)
		})
		if !success {
			return
		}
	}
}

// TestParseIPNetBracketedIPv6WithoutPort ensures bracketed IPv6 addresses are
// parsed consistently with and without an explicit port.
func TestParseIPNetBracketedIPv6WithoutPort(t *testing.T) {
	t.Parallel()

	addrWithoutPort := "[2001:db8:a0b:12f0::1]"
	addrWithPort := "[2001:db8:a0b:12f0::1]:8333"

	ipNetWithoutPort, err := ParseIPNet(addrWithoutPort, nil)
	require.NoError(t, err)

	ipNetWithPort, err := ParseIPNet(addrWithPort, nil)
	require.NoError(t, err)

	require.True(t, ipNetWithoutPort.IP.Equal(ipNetWithPort.IP))
	require.Equal(t, ipNetWithPort.Mask, ipNetWithoutPort.Mask)
}

// TestParseIPNetRejectsTorV3 ensures the IP-only wrapper does not accept
// overlay addresses.
func TestParseIPNetRejectsTorV3(t *testing.T) {
	t.Parallel()

	_, err := ParseIPNet(testTorV3Host, nil)
	require.Error(t, err)
}

// TestParseAddr ensures that peer addresses are parsed into typed ban keys.
func TestParseAddr(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name   string
		addr   string
		result *Key
	}{
		{
			name: "ipv4 with default mask",
			addr: "192.168.1.1",
			result: &Key{
				Net:  NetworkIPv4,
				Addr: []byte{0xc0, 0xa8, 0x01, 0x01},
				Mask: []byte(defaultIPv4Mask),
			},
		},
		{
			name: "ipv6 with default mask",
			addr: "[2001:db8:a0b:12f0::1]:80",
			result: &Key{
				Net: NetworkIPv6,
				Addr: []byte{
					0x20, 0x01, 0x0d, 0xb8,
					0x0a, 0x0b, 0x12, 0xf0,
					0x00, 0x00, 0x00, 0x00,
					0x00, 0x00, 0x00, 0x01,
				},
				Mask: []byte(defaultIPv6Mask),
			},
		},
		{
			name: "torv3 with port and mixed case",
			addr: strings.ToUpper(testTorV3Host) + ":9735",
			result: &Key{
				Net:  NetworkTorV3,
				Addr: testTorV3PubKey,
			},
		},
	}

	for _, testCase := range testCases {
		success := t.Run(testCase.name, func(t *testing.T) {
			key, err := ParseAddr(testCase.addr)
			require.NoError(t, err)
			require.Equal(t, testCase.result.Net, key.Net)
			require.Equal(t, testCase.result.Addr, key.Addr)
			require.Equal(t, testCase.result.Mask, key.Mask)
		})
		if !success {
			return
		}
	}
}

// TestParseAddrRejectsInvalidTorV3 ensures malformed onion addresses are
// rejected.
func TestParseAddrRejectsInvalidTorV3(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name string
		addr string
	}{
		{
			name: "wrong length",
			addr: "short.onion",
		},
		{
			name: "bad checksum",
			addr: testTorV3BadChecksumHost,
		},
		{
			name: "bad version",
			addr: testTorV3BadVersionHost,
		},
	}

	for _, testCase := range testCases {
		success := t.Run(testCase.name, func(t *testing.T) {
			_, err := ParseAddr(testCase.addr)
			require.Error(t, err)
		})
		if !success {
			return
		}
	}
}

// TestParseAddrBracketedIPv6WithoutPort ensures bracketed IPv6 addresses are
// parsed consistently with and without an explicit port.
func TestParseAddrBracketedIPv6WithoutPort(t *testing.T) {
	t.Parallel()

	addrWithoutPort := "[2001:db8:a0b:12f0::1]"
	addrWithPort := "[2001:db8:a0b:12f0::1]:8333"

	keyWithoutPort, err := ParseAddr(addrWithoutPort)
	require.NoError(t, err)

	keyWithPort, err := ParseAddr(addrWithPort)
	require.NoError(t, err)

	require.Equal(t, keyWithPort, keyWithoutPort)
}

// TestParseIPNetIPv4WithIPv6WidthMask ensures a 4-byte IPv4 address paired
// with a 16-byte mask keeps resolving to the network it names. Callers reach
// this shape through net.ParseCIDR on a v4-mapped address, and it worked
// before ban keys were introduced, so it has to keep working.
func TestParseIPNetIPv4WithIPv6WidthMask(t *testing.T) {
	t.Parallel()

	ipNet, err := ParseIPNet("1.2.3.4", net.CIDRMask(128, 128))
	require.NoError(t, err)
	require.Equal(t, "1.2.3.4/32", ipNet.String())

	// A 16-byte mask whose leading 12 bytes are not all ones does not
	// describe an IPv4 network, so it is refused rather than guessed at.
	mask := net.IPMask(make([]byte, net.IPv6len))
	mask[0] = 0xff

	_, err = ParseIPNet("1.2.3.4", mask)
	require.ErrorIs(t, err, ErrUnsupportedIP)
}

// TestParseAddrIPv4MappedCIDR ensures a v4-mapped address from net.ParseCIDR
// resolves to an IPv4 key, and that the ban record it produces is the same one
// the plain dotted form produces.
func TestParseAddrIPv4MappedCIDR(t *testing.T) {
	t.Parallel()

	_, mapped, err := net.ParseCIDR("::ffff:1.2.3.4/128")
	require.NoError(t, err)

	key, err := keyFromIPNet(mapped)
	require.NoError(t, err)
	require.Equal(t, NetworkIPv4, key.Net)

	plain, err := ParseAddr("1.2.3.4")
	require.NoError(t, err)
	require.Equal(t, plain, key)

	// The deprecated IP path has to reach the same place.
	ipNet, err := ParseIPNet(mapped.IP.String(), mapped.Mask)
	require.NoError(t, err)
	require.Equal(t, "1.2.3.4/32", ipNet.String())
}

// TestParseAddrTorV3NoMask ensures a Tor v3 key never carries an IP mask, so
// it cannot be mistaken for an IP record by a reader that dispatches on the
// mask alone.
func TestParseAddrTorV3NoMask(t *testing.T) {
	t.Parallel()

	key, err := ParseAddr(testTorV3Host)
	require.NoError(t, err)
	require.Equal(t, NetworkTorV3, key.Net)
	require.Nil(t, key.Mask)
	require.Len(t, key.Addr, torV3KeySize)

	// A Tor v3 host with a mask is not a shape the encoder accepts.
	_, err = keyToIPNet(key)
	require.ErrorIs(t, err, ErrUnsupportedIP)
}

// TestParseAddrRealTorV3Services checks the parser against addresses of real,
// public Tor v3 services. The parser had to agree on their checksums and
// versions to accept them at all, so a vector that was only ever produced by
// the same code under test cannot hide a wrong checksum.
func TestParseAddrRealTorV3Services(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		host   string
		pubKey []byte
	}{
		{
			host: "duckduckgogg42xjoc72x3sjasowoarfbgcmvfimaftt6" +
				"twagswzczad.onion",
			pubKey: []byte{
				0x1d, 0x04, 0xa1, 0xd0, 0x4a, 0x33, 0x8c, 0x6e,
			},
		},
		{
			host: "facebookwkhpilnemxj7asaniu7vnjjbiltxjqhye3mhbs" +
				"hg7kx5tfyd.onion",
			pubKey: []byte{
				0x28, 0x04, 0x40, 0xb9, 0xca, 0xb2, 0x8e, 0xf4,
			},
		},
		{
			host: "protonmailrmez3lotccipshtkleegetolb73fuirgj7r4" +
				"o4vfu7ozyd.onion",
			pubKey: []byte{
				0x7c, 0x5d, 0x37, 0x35, 0x80, 0x42, 0xe2, 0xc2,
			},
		},
		{
			host: "mempoolhqx4isw62xs7abwphsq7ldayuidyx2v2oethdh" +
				"hj6mlo2r6ad.onion",
			pubKey: []byte{
				0x61, 0x18, 0xf7, 0x39, 0x67, 0x85, 0xf8, 0x89,
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.host[:16], func(t *testing.T) {
			t.Parallel()

			key, err := ParseAddr(testCase.host + ":8333")
			require.NoError(t, err)
			require.Equal(t, NetworkTorV3, key.Net)
			require.Equal(t, testCase.pubKey, key.Addr[:8])
		})
	}
}
