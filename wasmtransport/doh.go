package wasmtransport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"time"
)

const (
	// DefaultDoHEndpoint is a browser-compatible DNS-over-HTTPS JSON
	// endpoint. Callers can provide a different endpoint when required.
	DefaultDoHEndpoint = "https://cloudflare-dns.com/dns-query"

	defaultDoHTimeout = 15 * time.Second
)

// DoHResolver resolves peer host names through DNS over HTTPS. It is useful in
// browser WASM, where raw DNS lookups are unavailable.
type DoHResolver struct {
	Endpoint string
	Client   *http.Client
}

// NewDoHNameResolver returns a neutrino-compatible name resolver backed by a
// DNS-over-HTTPS JSON endpoint. An empty endpoint uses DefaultDoHEndpoint.
func NewDoHNameResolver(endpoint string) func(string) ([]net.IP, error) {
	resolver := &DoHResolver{
		Endpoint: endpoint,
		Client: &http.Client{
			Timeout: defaultDoHTimeout,
		},
	}

	return resolver.LookupIP
}

// LookupIP resolves host through A and AAAA DNS-over-HTTPS queries.
func (r *DoHResolver) LookupIP(host string) ([]net.IP, error) {
	if ip := net.ParseIP(host); ip != nil {
		return []net.IP{ip}, nil
	}

	var (
		result     []net.IP
		lookupErrs []error
	)
	for _, recordType := range []string{"A", "AAAA"} {
		ips, err := r.lookup(context.Background(), host, recordType)
		if err != nil {
			lookupErrs = append(lookupErrs, err)
			continue
		}

		result = append(result, ips...)
	}

	if len(result) == 0 {
		if len(lookupErrs) > 0 {
			return nil, fmt.Errorf(
				"DNS lookup failed for %s: %w", host,
				errors.Join(lookupErrs...),
			)
		}

		return nil, fmt.Errorf("no DNS answers for %s", host)
	}

	return result, nil
}

func (r *DoHResolver) lookup(ctx context.Context, host,
	recordType string) ([]net.IP, error) {

	endpoint := r.Endpoint
	if endpoint == "" {
		endpoint = DefaultDoHEndpoint
	}
	client := r.Client
	if client == nil {
		client = &http.Client{Timeout: defaultDoHTimeout}
	}

	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("parse DNS-over-HTTPS endpoint: %w", err)
	}

	query := u.Query()
	query.Set("name", host)
	query.Set("type", recordType)
	u.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("accept", "application/dns-json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("DNS lookup %s %s failed: %s",
			host, recordType, resp.Status)
	}

	var dnsResponse struct {
		Status int `json:"Status"`
		Answer []struct {
			Type int    `json:"type"`
			Data string `json:"data"`
		} `json:"Answer"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&dnsResponse); err != nil {
		return nil, err
	}
	if dnsResponse.Status != 0 {
		return nil, fmt.Errorf("DNS lookup %s %s returned status %d",
			host, recordType, dnsResponse.Status)
	}

	wantType := 1
	if recordType == "AAAA" {
		wantType = 28
	}

	var ips []net.IP
	for _, answer := range dnsResponse.Answer {
		if answer.Type != wantType {
			continue
		}

		ip := net.ParseIP(answer.Data)
		if ip != nil {
			ips = append(ips, ip)
		}
	}

	return ips, nil
}
