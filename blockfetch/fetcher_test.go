package blockfetch

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/stretchr/testify/require"
)

// fakeResp describes the outcome of a single mocked HTTP request.
type fakeResp struct {
	body   []byte
	status int
	err    error
}

// fakeHTTPClient is a programmable HTTPClient that returns a preconfigured
// sequence of responses, one per call. Once the sequence is exhausted the last
// entry is repeated, which lets tests describe "fail forever" behavior with a
// single entry.
type fakeHTTPClient struct {
	responses []fakeResp
	calls     int
	lastURL   string
}

// Get returns the next preconfigured response.
func (f *fakeHTTPClient) Get(url string) (*http.Response, error) {
	f.lastURL = url

	idx := f.calls
	if idx >= len(f.responses) {
		idx = len(f.responses) - 1
	}
	f.calls++

	resp := f.responses[idx]
	if resp.err != nil {
		return nil, resp.err
	}

	return &http.Response{
		StatusCode: resp.status,
		Body:       io.NopCloser(bytes.NewReader(resp.body)),
	}, nil
}

// genesisBytes returns the wire-encoded mainnet genesis block along with its
// hash, used as realistic block payload in the tests.
func genesisBytes(t *testing.T) (chainhash.Hash, []byte) {
	t.Helper()

	var buf bytes.Buffer
	require.NoError(t, chaincfg.MainNetParams.GenesisBlock.Serialize(&buf))

	return *chaincfg.MainNetParams.GenesisHash, buf.Bytes()
}

// TestFetchBlockSuccess asserts that a single successful response yields the
// expected block and only one request is made.
func TestFetchBlockSuccess(t *testing.T) {
	t.Parallel()

	hash, raw := genesisBytes(t)
	client := &fakeHTTPClient{responses: []fakeResp{
		{body: raw, status: http.StatusOK},
	}}

	f := New(Config{BaseURL: "https://block-dn.org/", Client: client})

	block, err := f.FetchBlock(nil, hash)
	require.NoError(t, err)
	require.Equal(t, hash, block.BlockHash())
	require.Equal(t, 1, client.calls)

	// The URL must follow the block-dn "/block/<hash>" scheme, with the
	// trailing slash on the base URL stripped.
	require.Equal(
		t, "https://block-dn.org/block/"+hash.String(),
		client.lastURL,
	)
}

// TestFetchBlockRetryThenSuccess asserts that a transient failure is retried
// and a later success is returned.
func TestFetchBlockRetryThenSuccess(t *testing.T) {
	t.Parallel()

	hash, raw := genesisBytes(t)
	client := &fakeHTTPClient{responses: []fakeResp{
		{status: http.StatusInternalServerError},
		{err: errors.New("connection reset")},
		{body: raw, status: http.StatusOK},
	}}

	f := New(Config{
		BaseURL:    "https://block-dn.org",
		Client:     client,
		NumRetries: 3,
		RetryDelay: time.Millisecond,
	})

	block, err := f.FetchBlock(nil, hash)
	require.NoError(t, err)
	require.Equal(t, hash, block.BlockHash())
	require.Equal(t, 3, client.calls)
}

// TestFetchBlockRetriesExhausted asserts that we give up after the configured
// number of attempts and surface the last error.
func TestFetchBlockRetriesExhausted(t *testing.T) {
	t.Parallel()

	hash, _ := genesisBytes(t)
	client := &fakeHTTPClient{responses: []fakeResp{
		{status: http.StatusInternalServerError},
	}}

	f := New(Config{
		BaseURL:    "https://block-dn.org",
		Client:     client,
		NumRetries: 3,
		RetryDelay: time.Millisecond,
	})

	_, err := f.FetchBlock(nil, hash)
	require.ErrorContains(t, err, "after 3 attempt(s)")
	require.ErrorContains(t, err, "status code 500")
	require.Equal(t, 3, client.calls)
}

// TestFetchBlockBadBytes asserts that an undeserializable body is treated as a
// failure and retried.
func TestFetchBlockBadBytes(t *testing.T) {
	t.Parallel()

	hash, _ := genesisBytes(t)
	client := &fakeHTTPClient{responses: []fakeResp{
		{body: []byte("not a block"), status: http.StatusOK},
	}}

	f := New(Config{
		BaseURL:    "https://block-dn.org",
		Client:     client,
		NumRetries: 2,
		RetryDelay: time.Millisecond,
	})

	_, err := f.FetchBlock(nil, hash)
	require.ErrorContains(t, err, "failed to deserialize block")
	require.Equal(t, 2, client.calls)
}

// TestFetchBlockHashMismatch asserts that a well-formed block that doesn't
// match the requested hash is rejected, guarding against a source serving the
// wrong block.
func TestFetchBlockHashMismatch(t *testing.T) {
	t.Parallel()

	_, raw := genesisBytes(t)

	// Request a different block (testnet genesis) but have the source serve
	// the mainnet genesis block.
	wrongHash := *chaincfg.TestNet3Params.GenesisHash
	client := &fakeHTTPClient{responses: []fakeResp{
		{body: raw, status: http.StatusOK},
	}}

	f := New(Config{
		BaseURL:    "https://block-dn.org",
		Client:     client,
		NumRetries: 1,
	})

	_, err := f.FetchBlock(nil, wrongHash)
	require.ErrorContains(t, err, "returned block")
	require.Equal(t, 1, client.calls)
}

// TestFetchBlockQuit asserts that closing the quit channel aborts the fetch
// immediately with ErrShuttingDown.
func TestFetchBlockQuit(t *testing.T) {
	t.Parallel()

	hash, _ := genesisBytes(t)
	client := &fakeHTTPClient{responses: []fakeResp{
		{status: http.StatusInternalServerError},
	}}

	quit := make(chan struct{})
	close(quit)

	f := New(Config{BaseURL: "https://block-dn.org", Client: client})

	_, err := f.FetchBlock(quit, hash)
	require.ErrorIs(t, err, ErrShuttingDown)
	require.Equal(t, 0, client.calls)
}

// TestNewDefaults asserts that New fills in sane defaults for unset config
// fields.
func TestNewDefaults(t *testing.T) {
	t.Parallel()

	f := New(Config{BaseURL: "https://block-dn.org"})
	require.Equal(t, DefaultNumRetries, f.numRetries)
	require.Equal(t, DefaultRetryDelay, f.retryDelay)
	require.Equal(t, "https://block-dn.org", f.baseURL)

	// The default client must be a real *http.Client with a timeout set.
	httpClient, ok := f.client.(*http.Client)
	require.True(t, ok)
	require.Equal(t, DefaultRequestTimeout, httpClient.Timeout)
}
