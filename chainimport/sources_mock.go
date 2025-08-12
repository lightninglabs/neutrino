package chainimport

import (
	"iter"
	"net/http"

	"github.com/stretchr/testify/mock"
)

// mockHeaderImportSource mocks a header import source for testing import source
// interactions.
type mockHeaderImportSource struct {
	mock.Mock
	uri string
}

// Open opens the mock header import source.
func (m *mockHeaderImportSource) Open() error {
	args := m.Called()
	return args.Error(0)
}

// Close closes the mock header import source.
func (m *mockHeaderImportSource) Close() error {
	args := m.Called()
	return args.Error(0)
}

// GetHeaderMetadata gets header metadata from the mock header import source.
func (m *mockHeaderImportSource) GetHeaderMetadata() (*headerMetadata, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*headerMetadata), args.Error(1)
}

// GetHeader gets a header by index from the mock header import source.
func (m *mockHeaderImportSource) GetHeader(index uint32) (Header, error) {
	args := m.Called(index)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(Header), args.Error(1)
}

// Iterator returns a header iterator from the mock header import source.
func (m *mockHeaderImportSource) Iterator(start, end uint32,
	batchSize uint32) HeaderIterator {

	args := m.Called(start, end, batchSize)
	return args.Get(0).(HeaderIterator)
}

// GetURI gets the URI from the mock header import source.
func (m *mockHeaderImportSource) GetURI() string {
	args := m.Called()
	return args.String(0)
}

// SetURI sets the URI for the mock header import source.
func (m *mockHeaderImportSource) SetURI(uri string) {
	m.Called(uri)
	m.uri = uri
}

// mockHeaderIterator mocks a header iterator for testing header iteration
// logic.
type mockHeaderIterator struct {
	mock.Mock
}

// Iterator returns an iter.Seq2[Header, error] for the specified range.
func (m *mockHeaderIterator) Iterator(start,
	end uint32) iter.Seq2[Header, error] {

	args := m.Called(start, end)
	return args.Get(0).(iter.Seq2[Header, error])
}

// BatchIterator returns an iterator that yields batches of headers.
func (m *mockHeaderIterator) BatchIterator(start,
	end uint32) iter.Seq2[[]Header, error] {

	args := m.Called(start, end)
	return args.Get(0).(iter.Seq2[[]Header, error])
}

// ReadBatch collects all headers from the given range into a slice.
func (m *mockHeaderIterator) ReadBatch(start, end uint32) ([]Header, error) {
	args := m.Called(start, end)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]Header), args.Error(1)
}

// Next returns the next header in the current iteration sequence.
func (m *mockHeaderIterator) Next() (Header, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(Header), args.Error(1)
}

// NextBatch returns the next batch of headers in the current iteration
// sequence.
func (m *mockHeaderIterator) NextBatch() ([]Header, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]Header), args.Error(1)
}

// Close closes the mock header iterator.
func (m *mockHeaderIterator) Close() error {
	args := m.Called()
	return args.Error(0)
}

// mockHTTPClient mocks an HTTP client for testing HTTP header import source
// interactions.
type mockHTTPClient struct {
	mock.Mock
}

// Get returns a response from the mock HTTP client.
func (m *mockHTTPClient) Get(url string) (*http.Response, error) {
	args := m.Called(url)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*http.Response), args.Error(1)
}
