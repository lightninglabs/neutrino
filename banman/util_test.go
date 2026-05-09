package banman

import (
	"net"
	"reflect"
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
	}

	for _, testCase := range testCases {
		success := t.Run(testCase.name, func(t *testing.T) {
			// Parse the IP network from each test's address and
			// mask.
			ipNet, err := ParseIPNet(testCase.addr, testCase.mask)
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
			if !reflect.DeepEqual(ipNet.Mask, testCase.result.Mask) {
				t.Fatalf("expected mask %#v, got %#v",
					testCase.result.Mask, ipNet.Mask)
			}
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
