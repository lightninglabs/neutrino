package chainimport

import (
	"io"
	"iter"
)

// importSourceHeaderIterator provides efficient iteration over headers from
// a source.
type importSourceHeaderIterator struct {
	// source is the HeaderImportSource from which headers are retrieved
	// during iteration.
	source HeaderImportSource

	// startIndex is the default inclusive start index exposed by
	// GetStartIndex.
	startIndex uint32

	// endIndex is the default inclusive end index exposed by GetEndIndex.
	endIndex uint32

	// batchSize determines how many headers are fetched in a single
	// operation to optimize performance during iteration.
	batchSize uint32
}

// Compile-time assertion to ensure importSourceHeaderIterator implements
// headerIterator interface.
var _ HeaderIterator = (*importSourceHeaderIterator)(nil)

// Iterator returns a stateless iter.Seq2[header, error] for the specified
// headers range. Each call creates a fresh iterator that independently
// traverses the range from start to end index.
func (it *importSourceHeaderIterator) Iterator(startIdx,
	endIdx uint32) iter.Seq2[Header, error] {

	return func(yield func(Header, error) bool) {
		if startIdx > endIdx {
			yield(nil, io.EOF)
			return
		}

		for idx := startIdx; idx <= endIdx; idx++ {
			header, err := it.source.GetHeader(idx)
			if err != nil && !yield(nil, err) {
				return
			}
			if !yield(header, nil) {
				return
			}
		}
	}
}

// BatchIterator returns a stateless iterator that yields batches of headers for
// the specified range, where each batch respects the configured batch size
// limit. Each call creates a fresh iterator that independently traverses the
// range.
func (it *importSourceHeaderIterator) BatchIterator(startIdx,
	endIdx, batchSize uint32) iter.Seq2[[]Header, error] {

	return func(yield func([]Header, error) bool) {
		if startIdx > endIdx {
			yield(nil, io.EOF)
			return
		}

		currentIdx := startIdx
		for currentIdx <= endIdx {
			batch, err := it.ReadBatch(
				currentIdx, endIdx, batchSize,
			)
			if err != nil && !yield(nil, err) {
				return
			}
			if !yield(batch, nil) {
				return
			}
			currentIdx += uint32(len(batch))
		}
	}
}

// ReadBatch is a stateless method that collects headers from the given range
// into a slice, respecting the configured batch size limit. It returns io.EOF
// if no headers are found.
func (it *importSourceHeaderIterator) ReadBatch(
	startIdx, endIdx, batchSize uint32) ([]Header, error) {

	var headers []Header

	actualEndIdx := min(endIdx, startIdx+batchSize-1)

	for header, err := range it.Iterator(startIdx, actualEndIdx) {
		if err != nil {
			return nil, err
		}

		if header != nil {
			headers = append(headers, header)
		}
	}

	if len(headers) == 0 {
		return nil, io.EOF
	}

	return headers, nil
}

// GetStartIndex returns the configured start index for this iterator.
func (it *importSourceHeaderIterator) GetStartIndex() uint32 {
	return it.startIndex
}

// GetEndIndex returns the configured end index for this iterator.
func (it *importSourceHeaderIterator) GetEndIndex() uint32 {
	return it.endIndex
}

// GetBatchSize returns the configured batch size for this iterator.
func (it *importSourceHeaderIterator) GetBatchSize() uint32 {
	return it.batchSize
}
