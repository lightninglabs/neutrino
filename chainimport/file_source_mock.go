package chainimport

import (
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
