package blockfetch

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
)

const (
	// DefaultNumRetries is the number of times we'll try to fetch a block
	// from the HTTP source before giving up and letting the caller fall
	// back to the P2P network.
	DefaultNumRetries = 3

	// DefaultRetryDelay is the delay we wait between retry attempts.
	DefaultRetryDelay = 500 * time.Millisecond

	// DefaultRequestTimeout is the timeout applied to a single HTTP block
	// request when the default client is used.
	DefaultRequestTimeout = 30 * time.Second
)

// blockPathFormat is the path, relative to the configured base URL, where an
// individual block can be downloaded by its hash. This matches the API served
// by block-dn (https://block-dn.org), which returns the raw, witness-encoded
// block in Bitcoin wire format.
const blockPathFormat = "%s/block/%s"

// ErrShuttingDown is returned when a fetch is aborted because the caller's quit
// channel was closed.
var ErrShuttingDown = errors.New("block fetcher shutting down")

// HTTPClient is the subset of *http.Client that the block fetcher relies on.
// It's deliberately narrow so tests can supply a fake implementation.
type HTTPClient interface {
	// Get issues a GET request to the given URL and returns the response.
	Get(url string) (*http.Response, error)
}

// Config holds the configuration for a BlockFetcher.
type Config struct {
	// BaseURL is the base URL of the HTTP block source, e.g.
	// "https://block-dn.org". Individual blocks are fetched from
	// "<BaseURL>/block/<hash>".
	BaseURL string

	// Client is the HTTP client used to issue requests. If nil, a default
	// client with a DefaultRequestTimeout timeout is used.
	Client HTTPClient

	// NumRetries is the number of attempts made to fetch a block before
	// giving up. If zero or negative, DefaultNumRetries is used.
	NumRetries int

	// RetryDelay is the delay between retry attempts. If zero,
	// DefaultRetryDelay is used.
	RetryDelay time.Duration
}

// BlockFetcher downloads raw blocks from an HTTP block source such as
// block-dn. It is safe for concurrent use as it holds no mutable state.
type BlockFetcher struct {
	baseURL    string
	client     HTTPClient
	numRetries int
	retryDelay time.Duration
}

// New constructs a BlockFetcher from the given config, applying defaults for
// any unset fields.
func New(cfg Config) *BlockFetcher {
	client := cfg.Client
	if client == nil {
		client = &http.Client{Timeout: DefaultRequestTimeout}
	}

	numRetries := cfg.NumRetries
	if numRetries <= 0 {
		numRetries = DefaultNumRetries
	}

	retryDelay := cfg.RetryDelay
	if retryDelay == 0 {
		retryDelay = DefaultRetryDelay
	}

	return &BlockFetcher{
		baseURL:    strings.TrimRight(cfg.BaseURL, "/"),
		client:     client,
		numRetries: numRetries,
		retryDelay: retryDelay,
	}
}

// FetchBlock downloads the block with the given hash from the HTTP source. It
// retries up to the configured number of attempts, waiting RetryDelay between
// them, and aborts early if the quit channel is closed.
//
// The returned block is guaranteed to hash to the requested hash, but the more
// expensive consensus validation (sanity checks, witness commitment) is left
// to the caller, which has the chain context required to perform it.
func (b *BlockFetcher) FetchBlock(quit <-chan struct{},
	hash chainhash.Hash) (*wire.MsgBlock, error) {

	url := fmt.Sprintf(blockPathFormat, b.baseURL, hash.String())

	var lastErr error
	for attempt := 1; attempt <= b.numRetries; attempt++ {
		// Abort early if we're shutting down.
		select {
		case <-quit:
			return nil, ErrShuttingDown
		default:
		}

		block, err := b.fetchOnce(url, hash)
		if err == nil {
			return block, nil
		}

		lastErr = err
		log.Debugf("Attempt %d/%d to fetch block %v from %s failed: %v",
			attempt, b.numRetries, hash, url, err)

		// There's no point waiting after the final attempt.
		if attempt == b.numRetries {
			break
		}

		select {
		case <-time.After(b.retryDelay):
		case <-quit:
			return nil, ErrShuttingDown
		}
	}

	return nil, fmt.Errorf("failed to fetch block %v from HTTP source "+
		"after %d attempt(s): %w", hash, b.numRetries, lastErr)
}

// fetchOnce performs a single download attempt and verifies that the returned
// block hashes to the requested hash.
func (b *BlockFetcher) fetchOnce(url string,
	hash chainhash.Hash) (*wire.MsgBlock, error) {

	resp, err := b.client.Get(url)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code %d",
			resp.StatusCode)
	}

	// The block is served as a raw, witness-encoded block in Bitcoin wire
	// format, so we can deserialize it straight from the response body.
	var msgBlock wire.MsgBlock
	if err := msgBlock.Deserialize(resp.Body); err != nil {
		return nil, fmt.Errorf("failed to deserialize block: %w", err)
	}

	// Make sure the source actually returned the block we asked for. This
	// is a cheap integrity check on the transport; it doesn't replace the
	// consensus validation the caller still has to perform.
	if gotHash := msgBlock.BlockHash(); gotHash != hash {
		return nil, fmt.Errorf("block source returned block %v for "+
			"requested block %v", gotHash, hash)
	}

	return &msgBlock, nil
}
