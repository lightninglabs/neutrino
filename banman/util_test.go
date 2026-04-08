package banman

import (
	"net"
	"reflect"
	"testing"
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

func TestParseIPNetOnionV3(t *testing.T) {
	t.Parallel()

	host := "yov4edh4vgbgywplxuxv4esroksz2brb64fdtjbryc5wbo43wtlbsiad.onion"

	ipNet, err := ParseIPNet(
		net.JoinHostPort(host, "38333"), nil,
	)
	if err != nil {
		t.Fatalf("unable to parse onion v3 address: %v", err)
	}

	if ipNet == nil || ipNet.IP == nil || ipNet.IP.To16() == nil {
		t.Fatalf("expected ipv6 network for onion v3, got: %#v", ipNet)
	}

	if !reflect.DeepEqual(ipNet.Mask, defaultIPv6Mask) {
		t.Fatalf("expected mask %#v, got %#v",
			defaultIPv6Mask, ipNet.Mask)
	}
}

func TestParseIPNetOnionStableAcrossPortVariants(t *testing.T) {
	t.Parallel()

	host := "yov4edh4vgbgywplxuxv4esroksz2brb64fdtjbryc5wbo43wtlbsiad.onion"

	withPort, err := ParseIPNet(net.JoinHostPort(host, "38333"), nil)
	if err != nil {
		t.Fatalf("unable to parse onion host with port: %v", err)
	}

	withoutPort, err := ParseIPNet(host, nil)
	if err != nil {
		t.Fatalf("unable to parse onion host without port: %v", err)
	}

	if withPort.String() != withoutPort.String() {
		t.Fatalf("expected stable onion mapping: "+
			"withPort=%s withoutPort=%s",
			withPort.String(), withoutPort.String())
	}
}

func TestParseIPNetRejectsInvalidOnion(t *testing.T) {
	t.Parallel()

	_, err := ParseIPNet("invalid.onion:8333", nil)
	if err == nil {
		t.Fatal("expected invalid onion hostname to fail")
	}
}
