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

	// currentIndex tracks the current position in the iteration sequence,
	// representing the next header index to be processed.
	currentIndex uint32

	// endIndex specifies the final header index (inclusive) that will be
	// processed by this iterator.
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
// traverses the range.
func (it *importSourceHeaderIterator) Iterator(startIdx,
	endIdx uint32) iter.Seq2[Header, error] {

	return func(yield func(Header, error) bool) {
		for idx := startIdx; idx <= endIdx; idx++ {
			header, err := it.source.GetHeader(idx)
			if err != nil {
				if !yield(nil, err) {
					return
				}
				return
			}

			if header != nil {
				if !yield(header, nil) {
					return
				}
			}
		}
	}
}

// BatchIterator returns a stateless iterator that yields batches of headers for
// the specified range, where each batch respects the configured batch size
// limit. Each call creates a fresh iterator that independently traverses the
// range.
func (it *importSourceHeaderIterator) BatchIterator(startIdx,
	endIdx uint32) iter.Seq2[[]Header, error] {

	return func(yield func([]Header, error) bool) {
		currentIdx := startIdx
		for currentIdx <= endIdx {
			batch, err := it.ReadBatch(currentIdx, endIdx)
			if err != nil {
				if !yield(nil, err) {
					return
				}
				return
			}

			if len(batch) == 0 {
				return
			}

			if !yield(batch, nil) {
				return
			}

			currentIdx += uint32(len(batch))
		}
	}
}

// Next returns the next header in the current stateful iteration sequence. It
// maintains internal position state. Returns nil, io.EOF when no more headers
// are available.
func (it *importSourceHeaderIterator) Next() (Header, error) {
	if it.currentIndex > it.endIndex {
		return nil, io.EOF
	}

	header, err := it.source.GetHeader(it.currentIndex)
	if err != nil {
		return nil, err
	}

	it.currentIndex++
	return header, nil
}

// NextBatch returns the next batch of headers in the current stateful iteration
// sequence. It maintains internal position state. Returns nil, io.EOF when no
// more batches are available.
func (it *importSourceHeaderIterator) NextBatch() ([]Header, error) {
	if it.currentIndex > it.endIndex {
		return nil, io.EOF
	}

	// Calculate the end index for this batch.
	batchEndIdx := min(it.currentIndex+it.batchSize-1, it.endIndex)

	batch, err := it.ReadBatch(it.currentIndex, batchEndIdx)
	if err != nil {
		return nil, err
	}

	// Update current index to start of next batch.
	it.currentIndex = batchEndIdx + 1

	if len(batch) == 0 {
		return nil, io.EOF
	}

	return batch, nil
}

// ReadBatch is a stateless method that collects headers from the given range
// into a slice, respecting the configured batch size limit.
func (it *importSourceHeaderIterator) ReadBatch(
	startIdx, endIdx uint32) ([]Header, error) {

	var headers []Header

	// Calculate the actual end index based on batch size limit.
	actualEndIdx := min(endIdx, startIdx+it.batchSize-1)

	// Iterate through the headers in this batch.
	for header, err := range it.Iterator(startIdx, actualEndIdx) {
		if err != nil {
			return nil, err
		}

		if header != nil {
			headers = append(headers, header)
		}
	}

	return headers, nil
}

// Close releases any resources used by the headers iterator.
func (it *importSourceHeaderIterator) Close() error {
	// Reset iterator state.
	it.currentIndex = 0
	return nil
}
