package chainimport

import (
	"iter"

	"github.com/stretchr/testify/mock"
)

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
func (m *mockHeaderIterator) BatchIterator(start, end,
	batchSize uint32) iter.Seq2[[]Header, error] {

	args := m.Called(start, end, batchSize)
	return args.Get(0).(iter.Seq2[[]Header, error])
}

// ReadBatch collects all headers from the given range into a slice.
func (m *mockHeaderIterator) ReadBatch(start, end,
	batchSize uint32) ([]Header, error) {

	args := m.Called(start, end, batchSize)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]Header), args.Error(1)
}

// GetStartIndex returns the configured start index for this iterator.
func (m *mockHeaderIterator) GetStartIndex() uint32 {
	args := m.Called()
	return args.Get(0).(uint32)
}

// GetEndIndex returns the configured end index for this iterator.
func (m *mockHeaderIterator) GetEndIndex() uint32 {
	args := m.Called()
	return args.Get(0).(uint32)
}

// GetBatchSize returns the configured batch size for this iterator.
func (m *mockHeaderIterator) GetBatchSize() uint32 {
	args := m.Called()
	return args.Get(0).(uint32)
}
