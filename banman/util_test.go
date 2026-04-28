package banman

import (
	"net"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
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
