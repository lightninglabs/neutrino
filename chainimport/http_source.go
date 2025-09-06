package chainimport

import (
	"fmt"
	"io"
	"net/http"
	"os"
)

// httpHeaderImportSource implements headerImportSource for serving header files
// over HTTP(s).
type httpHeaderImportSource struct {
	uri        string
	httpClient HttpClient
	file       HeaderImportSource
}

// Compile-time assertion to ensure httpHeaderImportSource implements
// headerImportSource interface.
var _ HeaderImportSource = (*httpHeaderImportSource)(nil)

// newHTTPHeaderImportSource creates a new HTTP header import source.
func newHTTPHeaderImportSource(uri string, httpClient HttpClient,
	importSource HeaderImportSource) *httpHeaderImportSource {

	return &httpHeaderImportSource{
		uri:        uri,
		httpClient: httpClient,
		file:       importSource,
	}
}

// Open opens the HTTP header import source based on file header import source.
func (h *httpHeaderImportSource) Open() error {
	resp, err := h.httpClient.Get(h.uri)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to download file: status code %d",
			resp.StatusCode)
	}

	tempFile, err := os.CreateTemp("", "neutrino-headers-http-import-*.tmp")
	if err != nil {
		return err
	}
	cleanup := func() {
		tempFile.Close()
		os.Remove(tempFile.Name())
	}

	_, err = io.Copy(tempFile, resp.Body)
	if err != nil {
		cleanup()
		return err
	}

	if err = tempFile.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("failed to sync temporary file: %w", err)
	}

	tempFile.Close()

	h.file.SetURI(tempFile.Name())
	return h.file.Open()
}

// Close closes the HTTP header import source resources.
func (h *httpHeaderImportSource) Close() error {
	if err := h.file.Close(); err != nil {
		return fmt.Errorf("failed to close file import source: %w", err)
	}

	if err := os.Remove(h.file.GetURI()); err != nil {
		return fmt.Errorf("failed to remove temporary file: %w", err)
	}

	return nil
}

// GetHeaderMetadata reads the metadata from the file. The metadata is memoized
// after the first call, with subsequent calls returning the cached result
// without re-reading the file.
func (h *httpHeaderImportSource) GetHeaderMetadata() (*headerMetadata, error) {
	return h.file.GetHeaderMetadata()
}

// Iterator returns an efficient iterator for sequential header access.
func (h *httpHeaderImportSource) Iterator(start, end uint32,
	batchSize uint32) HeaderIterator {

	return h.file.Iterator(start, end, batchSize)
}

// GetHeader retrieves a single header at the specified index.
func (h *httpHeaderImportSource) GetHeader(index uint32) (Header, error) {
	return h.file.GetHeader(index)
}

// GetURI returns the HTTP URL for this import source.
func (h *httpHeaderImportSource) GetURI() string {
	return h.uri
}

// SetURI sets the HTTP URL for this import source.
func (h *httpHeaderImportSource) SetURI(uri string) {
	h.uri = uri
}

// newHTTPClient creates a new HTTP client.
func newHTTPClient() HttpClient {
	return &http.Client{}
}
