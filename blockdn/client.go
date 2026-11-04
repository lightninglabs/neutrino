// Package blockdn implements the binary HTTP API of a Block Delivery Network.
// It fetches data only; callers must validate consensus rules and filter
// commitments before using the data.
package blockdn

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
)

const (
	// HeadersPerFile is the number of records in a sealed header file.
	HeadersPerFile uint32 = 100000
	// FiltersPerFile is the number of records in a sealed filter file.
	FiltersPerFile uint32 = 2000
	maxFilterFile         = 64 << 20
	statusPath            = "/status"
)

// Status describes a single observation of the server's canonical chain.
//
//nolint:tagliatelle // Field names follow the external block-dn JSON API.
type Status struct {
	GenesisHash  chainhash.Hash `json:"chain_genesis_hash"`
	Height       uint32         `json:"best_block_height"`
	Hash         chainhash.Hash `json:"best_block_hash"`
	FilterHeight uint32         `json:"best_filter_height"`
	FilterHeader chainhash.Hash `json:"best_filter_header"`
}

// Client is safe for concurrent use. Responses have bounded sizes and every
// request honors context cancellation. A nil HTTP client selects a 45s timeout.
type Client struct {
	base      string
	http      *http.Client
	network   string
	tipHash   string
	tipHeight uint32
}

// ForNetwork returns a copy that keeps CDN cache entries for different
// networks separate. This does not replace validation of the returned data.
func (c *Client) ForNetwork(genesis chainhash.Hash) *Client {
	clone := *c
	clone.network = genesis.String()
	return &clone
}

// AtTip returns a copy that scopes mutable height-based responses to a chain
// observation. Sealed header files retain a stable cache key.
func (c *Client) AtTip(height uint32, hash chainhash.Hash) *Client {
	clone := *c
	clone.tipHeight, clone.tipHash = height, hash.String()
	return &clone
}

func (c *Client) headerFileClient(start uint32) *Client {
	// block-dn seals records six blocks behind the tip by default. Keep
	// boundary files mutable too; this is a cache choice, not finality.
	if uint64(start)+uint64(HeadersPerFile)+6 > uint64(c.tipHeight) {
		return c
	}
	clone := *c
	clone.tipHash = ""
	return &clone
}

// NewClient constructs a client for an HTTP(S) base URL, including an optional
// path prefix. Plain HTTP is useful for local nodes and tests.
func NewClient(base string, client *http.Client) (*Client, error) {
	u, err := url.Parse(base)
	if err != nil {
		return nil, err
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" ||
		u.RawQuery != "" || u.Fragment != "" || u.User != nil {

		return nil, fmt.Errorf("invalid block-dn base URL %q", base)
	}
	if client == nil {
		client = &http.Client{Timeout: 45 * time.Second}
	}
	return &Client{base: strings.TrimRight(base, "/"), http: client}, nil
}

func (c *Client) get(
	ctx context.Context,
	path string,
	limit int64,
) ([]byte, error) {

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		c.base+path,
		nil,
	)
	if err != nil {
		return nil, err
	}
	query := req.URL.Query()
	if c.network != "" {
		query.Set("neutrino_network", c.network)
	}
	if c.tipHash != "" {
		query.Set("neutrino_tip", c.tipHash)
	}
	if path == statusPath {
		// Some CDNs override origin freshness headers. Status is a live
		// observation and must not reuse a previous poll's cache entry.
		observation := time.Now().UnixNano()
		query.Set("neutrino_observation", fmt.Sprint(observation))
	}
	req.URL.RawQuery = query.Encode()
	// Mutable tail responses can lag at a CDN. Ask intermediaries to
	// revalidate; consumers still verify hashes and chain continuity.
	req.Header.Set("Cache-Control", "no-cache")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf(
			"block-dn %s: HTTP %d",
			path,
			resp.StatusCode,
		)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf(
			"block-dn %s: response exceeds %d bytes",
			path,
			limit,
		)
	}
	return data, nil
}

// Status returns the server's advertised chain and filter tips.
func (c *Client) Status(ctx context.Context) (*Status, error) {
	data, err := c.get(ctx, statusPath, 64<<10)
	if err != nil {
		return nil, err
	}
	var status Status
	if err := json.Unmarshal(data, &status); err != nil {
		return nil, fmt.Errorf("block-dn status: %w", err)
	}
	return &status, nil
}

// Headers returns a header file starting at a multiple of HeadersPerFile.
// The final unsealed file may contain fewer records.
func (c *Client) Headers(
	ctx context.Context,
	start uint32,
) ([]wire.BlockHeader, error) {

	if start%HeadersPerFile != 0 {
		return nil, fmt.Errorf("unaligned header start %d", start)
	}
	data, err := c.headerFileClient(start).get(
		ctx,
		fmt.Sprintf("/headers/%d", start),
		int64(HeadersPerFile)*80,
	)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 || len(data)%80 != 0 {
		return nil, fmt.Errorf(
			"invalid block header file length %d",
			len(data),
		)
	}
	headers := make([]wire.BlockHeader, len(data)/80)
	r := bytes.NewReader(data)
	for i := range headers {
		if err := headers[i].Deserialize(r); err != nil {
			return nil, err
		}
	}
	return headers, nil
}

// FilterHeaders returns a file of serialized BIP157 filter headers.
func (c *Client) FilterHeaders(
	ctx context.Context,
	start uint32,
) ([]chainhash.Hash, error) {

	if start%HeadersPerFile != 0 {
		return nil, fmt.Errorf(
			"unaligned filter header start %d",
			start,
		)
	}
	data, err := c.headerFileClient(start).get(
		ctx,
		fmt.Sprintf("/filter-headers/%d", start),
		int64(HeadersPerFile)*32,
	)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 || len(data)%32 != 0 {
		return nil, fmt.Errorf(
			"invalid filter header file length %d",
			len(data),
		)
	}
	headers := make([]chainhash.Hash, len(data)/32)
	for i := range headers {
		copy(headers[i][:], data[i*32:(i+1)*32])
	}
	return headers, nil
}

// Header retrieves exactly one block header by its hash.
func (c *Client) Header(
	ctx context.Context,
	hash chainhash.Hash,
) (*wire.BlockHeader, error) {

	data, err := c.get(ctx, "/header/"+hash.String(), 80)
	if err != nil {
		return nil, err
	}
	if len(data) != 80 {
		return nil, fmt.Errorf(
			"invalid block header length %d",
			len(data),
		)
	}
	var header wire.BlockHeader
	if err := header.Deserialize(bytes.NewReader(data)); err != nil {
		return nil, err
	}
	if header.BlockHash() != hash {
		return nil, fmt.Errorf(
			"block-dn returned wrong header for %s",
			hash,
		)
	}
	return &header, nil
}

// Filter retrieves a single raw basic filter by height. This endpoint is
// mutable during reorgs; verify it against the expected filter commitment.
func (c *Client) Filter(ctx context.Context, height uint32) ([]byte, error) {
	return c.get(ctx, fmt.Sprintf("/filters/single/%d", height), 4<<20)
}

// Filters retrieves and decodes a batch of length-prefixed basic filters.
func (c *Client) Filters(ctx context.Context, start uint32) ([][]byte, error) {
	if start%FiltersPerFile != 0 {
		return nil, fmt.Errorf("unaligned filter start %d", start)
	}
	data, err := c.get(
		ctx,
		fmt.Sprintf("/filters/%d", start),
		maxFilterFile,
	)
	if err != nil {
		return nil, err
	}
	r := bytes.NewReader(data)
	var filters [][]byte
	for r.Len() > 0 {
		if len(filters) == int(FiltersPerFile) {
			return nil, fmt.Errorf("too many filters in file")
		}
		filter, err := wire.ReadVarBytes(r, 0, 4<<20, "filter")
		if err != nil {
			return nil, err
		}
		filters = append(filters, filter)
	}
	return filters, nil
}

// Block retrieves a serialized witness block. Consensus checks are the
// responsibility of the caller.
func (c *Client) Block(
	ctx context.Context,
	hash chainhash.Hash,
) (*wire.MsgBlock, error) {

	data, err := c.get(ctx, "/block/"+hash.String(), 4<<20)
	if err != nil {
		return nil, err
	}
	var block wire.MsgBlock
	r := bytes.NewReader(data)
	if err := block.Deserialize(r); err != nil {
		return nil, err
	}
	if r.Len() != 0 || block.BlockHash() != hash {
		return nil, fmt.Errorf(
			"block-dn returned wrong block for %s",
			hash,
		)
	}
	return &block, nil
}
