package chainimport

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightninglabs/neutrino/headerfs"
	"golang.org/x/exp/mmap"
)

const (
	// Bitcoin Chain Type: 4 bytes.
	BitcoinChainTypeSize = 4

	// Header Type: 1 byte.
	HeaderTypeSize = 1

	// Start Header Height: 4 bytes.
	StartHeaderHeightSize = 4

	// Header Metadata size in bytes (sum of all above): 9 bytes.
	//
	//nolint:lll
	HeaderMetadataSize = BitcoinChainTypeSize + HeaderTypeSize + StartHeaderHeightSize

	// Unknown HeaderSize.
	UnknownHeaderSize = 0
)

// HeaderMetadata contains the metadata about the header source.
type HeaderMetadata struct {
	BitcoinChainType wire.BitcoinNet
	HeaderType       headerfs.HeaderType
	StartHeight      uint32
	EndHeight        uint32
}

// HeaderBase is the base interface for header operations.
type HeaderBase interface {
	// Deserialize reconstructs the header from binary data at the specified
	// height.
	Deserialize(io.Reader, uint32) error

	// ValidateMetadata ensures the header type is compatible with the
	// provided metadata.
	ValidateMetadata(*HeaderMetadata) error
}

// HeaderFactory is the factory interface for creating new headers.
type HeaderFactory[T HeaderBase] interface {
	// HeaderFactory includes all methods from HeaderBase.
	HeaderBase

	// New creates and returns a new instance of the header type T.
	New() T
}

// BlockHeader represents a block header that can be imported into
// the chain store. It wraps a headerfs.BlockHeader with additional
// functionality needed for the import process.
type BlockHeader struct {
	headerfs.BlockHeader
}

// Compile-time assertion to ensure BlockHeader implements
// HeaderFactory interface.
var _ HeaderFactory[*BlockHeader] = (*BlockHeader)(nil)

// Deserialize reconstructs a block header from binary data at the specified
// height.
func (b *BlockHeader) Deserialize(r io.Reader, height uint32) error {
	// Deserialize the wire.BlockHeader portion.
	if err := b.BlockHeader.BlockHeader.Deserialize(r); err != nil {
		return fmt.Errorf("failed to deserialize wire.BlockHeader: "+
			"%w", err)
	}

	// Set block header height.
	b.BlockHeader.Height = height

	return nil
}

// ValidateMetadata checks if the provided HeaderMetadata is of the correct type
// for block headers. It returns an error if the header type is not
// headerfs.Block.
func (b *BlockHeader) ValidateMetadata(m *HeaderMetadata) error {
	if m.HeaderType != headerfs.Block {
		return fmt.Errorf("type mismatch: file contains %v headers, "+
			"but expected block headers", m.HeaderType)
	}
	return nil
}

// New creates and returns a new empty BlockHeader instance. It is part of
// HeaderFactory interface.
func (b *BlockHeader) New() *BlockHeader {
	return &BlockHeader{
		BlockHeader: headerfs.BlockHeader{
			BlockHeader: &wire.BlockHeader{},
		},
	}
}

// FilterHeader represents a filter header that can be imported into
// the chain store. It wraps a headerfs.FilterHeader with additional
// functionality needed for the import process.
type FilterHeader struct {
	headerfs.FilterHeader
}

// Compile-time assertion to ensure FilterHeader implements
// HeaderFactory interface.
var _ HeaderFactory[*FilterHeader] = (*FilterHeader)(nil)

// Deserialize reconstructs a filter header from binary data at the specified
// height.
func (f *FilterHeader) Deserialize(r io.Reader, height uint32) error {
	// Read the filter hash (32 bytes).
	if _, err := io.ReadFull(r, f.FilterHash[:]); err != nil {
		return fmt.Errorf("failed to read filter hash: %w", err)
	}

	// Set filter header height.
	f.FilterHeader.Height = height

	return nil
}

// ValidateMetadata checks if the provided HeaderMetadata is of the correct type
// for filter headers. It returns an error if the header type is not
// headerfs.RegularFilter.
func (f *FilterHeader) ValidateMetadata(m *HeaderMetadata) error {
	if m.HeaderType != headerfs.RegularFilter {
		return fmt.Errorf("type mismatch: file contains %v headers, "+
			"but expected filter headers", m.HeaderType)
	}
	return nil
}

// New creates and returns a new empty FilterHeader instance. It is part of
// HeaderFactory interface.
func (b *FilterHeader) New() *FilterHeader {
	return &FilterHeader{
		FilterHeader: headerfs.FilterHeader{},
	}
}

// HeaderIterator interface for iterating over headers.
type HeaderIterator[T HeaderBase, F HeaderFactory[T]] interface {
	// Next returns the next header in the sequence and advances the
	// iterator.
	Next() (header T, hasMore bool, err error)

	// Close releases any resources associated with the iterator.
	Close() error
}

// ImportSourceHeaderIterator provides efficient iteration over headers from
// a source.
type ImportSourceHeaderIterator[T HeaderBase, F HeaderFactory[T]] struct {
	source  HeaderImportSource[T, F]
	current int
	end     int
}

// Compile-time assertion to ensure ImportSourceHeaderIterator implements
// HeaderIterator interface.
//
//nolint:lll
var _ HeaderIterator[HeaderBase, HeaderFactory[HeaderBase]] = (*ImportSourceHeaderIterator[HeaderBase, HeaderFactory[HeaderBase]])(nil)

// Next returns the next header in the sequence, along with a boolean indicating
// whether there are more headers available and any error encountered.
func (it *ImportSourceHeaderIterator[T, F]) Next() (T, bool, error) {
	var empty T

	if it.current > it.end {
		return empty, false, nil
	}

	header, err := it.source.GetHeader(it.current)
	if err != nil {
		return empty, false, err
	}

	it.current++
	return header, true, nil
}

// Close releases any resources used by the iterator.
func (it *ImportSourceHeaderIterator[T, F]) Close() error {
	// Don't close the source, as it may be used elsewhere.
	return nil
}

// HeaderValidator is a generic interface for validating headers of type T.
type HeadersValidator[T HeaderBase, F HeaderFactory[T]] interface {
	// Validate performs comprehensive validation on a sequence of headers
	// provided by the iterator. It checks that the entire sequence forms
	// a valid chain according to the given chain parameters.
	Validate(HeaderIterator[T, F], chaincfg.Params) error

	// ValidatePair verifies that two consecutive headers (prev and current)
	// form a valid chain link according to the specified chain parameters.
	ValidatePair(prev, current T, targetChainParams chaincfg.Params) error
}

// BlockHeadersImportSourceValidator implements HeadersValidator for block
// headers.
type BlockHeadersImportSourceValidator struct{}

// Compile-time assertion to ensure BlockHeadersImportSourceValidator implements
// HeadersValidator interface.
//
//nolint:lll
var _ HeadersValidator[*BlockHeader, HeaderFactory[*BlockHeader]] = (*BlockHeadersImportSourceValidator)(nil)

// Validate performs thorough validation of a sequence of block headers.
func (v *BlockHeadersImportSourceValidator) Validate(
	iterator HeaderIterator[*BlockHeader, HeaderFactory[*BlockHeader]],
	targetChainParams chaincfg.Params) error {

	var (
		prevHeader *BlockHeader
		hasMore    bool
		err        error
	)

	// Get the first header.
	prevHeader, hasMore, err = iterator.Next()
	if err != nil {
		return fmt.Errorf("failed to get first block header for "+
			"validation: %w", err)
	}

	if !hasMore {
		// No headers to validate.
		return nil
	}

	count := 1
	for {
		// Get the next header.
		var header *BlockHeader
		header, hasMore, err = iterator.Next()
		if err != nil {
			return fmt.Errorf("failed to get next block header at "+
				"position %d: %w", count, err)
		}

		if !hasMore {
			break
		}

		// Validate current header against previous header.
		err := v.ValidatePair(prevHeader, header, targetChainParams)
		if err != nil {
			return fmt.Errorf("block header validation failed at "+
				"position %d: %w", count, err)
		}

		prevHeader = header
		count++
	}

	log.Debugf("Successfully validated %d block headers", count)
	return nil
}

// ValidatePair verifies that two consecutive block headers form a valid chain
// link.
func (v *BlockHeadersImportSourceValidator) ValidatePair(prev,
	current *BlockHeader, targetChainParams chaincfg.Params) error {

	// Extract the block headers and heights directly.
	prevBlockHeader := prev.BlockHeader
	currBlockHeader := current.BlockHeader
	prevHeight, currHeight := prev.Height, current.Height

	// Verify that heights are consecutive.
	if currHeight != prevHeight+1 {
		return fmt.Errorf("height mismatch: previous height=%d, "+
			"current height=%d", prevHeight, currHeight)
	}

	// 1. Verify hash chain - current header's PrevBlock must match previous
	// header's hash.
	prevHash := prevBlockHeader.BlockHash()
	if !currBlockHeader.PrevBlock.IsEqual(&prevHash) {
		return fmt.Errorf("header chain broken: current header's "+
			"PrevBlock (%v) doesn't match previous header's hash "+
			"(%v)", currBlockHeader.PrevBlock, prevHash)
	}

	// 2. Validate block header sanity.
	err := blockchain.CheckBlockHeaderSanity(
		currBlockHeader.BlockHeader, targetChainParams.PowLimit,
		blockchain.NewMedianTime(), blockchain.BFFastAdd,
	)
	if err != nil {
		return err
	}

	return nil
}

// FilterHeadersImportSourceValidator implements HeaderValidator for filter
// headers.
type FilterHeadersImportSourceValidator struct{}

// Compile-time assertion to ensure FilterHeadersImportSourceValidator
// implements HeadersValidator interface.
//
//nolint:lll
var _ HeadersValidator[*FilterHeader, HeaderFactory[*FilterHeader]] = (*FilterHeadersImportSourceValidator)(nil)

// Validate performs basic validation on a stream of filter headers.
//
// Note: During import operations, only limited validation is possible since we
// don't have access to the actual compact filters that were used to generate
// these filter headers. Filter headers are calculated using the formula:
//
// FilterHeader_N = double-SHA256(FilterHeader_N-1 || double-SHA256(Filter_N))
//
// where FilterHeader_N-1 is the previous filter header in little-endian byte
// order, Filter_N is the compact filter for the block, and || represents
// concatenation.
func (v *FilterHeadersImportSourceValidator) Validate(
	iterator HeaderIterator[*FilterHeader, HeaderFactory[*FilterHeader]],
	targetChainParams chaincfg.Params) error {

	return nil
}

// ValidatePair checks if two consecutive filter headers maintain the correct
// cryptographic relationship.
//
// In a full validation, we would verify that each filter header is correctly
// derived by hashing the previous filter header with the current block's
// compact filter:
//
// However, during import, we don't have access to the compact filters, so we
// can't validate if two consecutive filter headers maintain the correct
// cryptographic relationship.
func (v *FilterHeadersImportSourceValidator) ValidatePair(prev,
	current *FilterHeader, targetChainParams chaincfg.Params) error {

	return nil
}

// ProcessingRegions currently contains only the NewHeaders region to process.
type ProcessingRegions struct {
	// ImportStartHeight defines the starting block height for the import
	// process.
	ImportStartHeight uint32

	// ImportEndHeight defines the ending block height for the import
	// process.
	ImportEndHeight uint32

	// EffectiveTip represents the current chain tip height that is
	// effective for processing.
	EffectiveTip uint32

	// NewHeaders contains the region of new headers that need to be
	// processed.
	NewHeaders HeaderRegion
}

// HeaderRegion represents a contiguous range of headers.
type HeaderRegion struct {
	// Start is the beginning height of this header region.
	Start uint32

	// End is the ending height of this header region.
	End uint32

	// Exists indicates whether this region has headers to process.
	Exists bool
}

// HeaderImportSource is a generic interface for loading headers of type T.
type HeaderImportSource[T HeaderBase, F HeaderFactory[T]] interface {
	// Open initializes the header source and prepares it for reading.
	Open() error

	// Close releases any resources associated with the header source.
	Close() error

	// GetHeaderMetadata retrieves metadata information about the headers
	// collection.
	GetHeaderMetadata() (*HeaderMetadata, error)

	// Iterator returns a HeaderIterator that can traverse headers between
	// start and end indices.
	Iterator(start, end int) HeaderIterator[T, F]

	// GetHeader retrieves a single header at the specified index.
	GetHeader(index int) (T, error)

	// HeadersCount returns the total number of headers in the source.
	HeadersCount() int

	// GetPath returns a string identifier for this header source.
	GetPath() string

	// SetPath sets a string identifier for this header source.
	SetPath(path string)
}

// Compile-time assertion to ensure FileHeaderImportSource implements
// HeaderImportSource interface.
//
//nolint:lll
var _ HeaderImportSource[HeaderBase, HeaderFactory[HeaderBase]] = (*FileHeaderImportSource[HeaderBase, HeaderFactory[HeaderBase]])(nil)

// FileHeaderSource implements HeaderImportSource for files.
type FileHeaderImportSource[T HeaderBase, F HeaderFactory[T]] struct {
	Path     string
	reader   *mmap.ReaderAt
	fileSize int
	metadata *HeaderMetadata
	factory  F
}

// Open opens the file and initializes the mmap reader.
func (f *FileHeaderImportSource[T, F]) Open() error {
	// Only open if not already open.
	if f.reader != nil {
		return nil
	}

	reader, err := mmap.Open(f.Path)
	if err != nil {
		return fmt.Errorf("failed to mmap file: %w", err)
	}
	f.reader = reader
	f.fileSize = reader.Len()

	// Read and initialize metadata from the beginning of the file.
	mData, err := f.GetHeaderMetadata()
	if err != nil {
		return fmt.Errorf("failed to get header metadata: %w", err)
	}

	// Validate that the generic type T matches the header type in the file.
	if err := f.validateTypeMatch(mData); err != nil {
		return fmt.Errorf("type validation failed: %w", err)
	}
	f.metadata = mData
	f.metadata.EndHeight = mData.StartHeight + uint32(f.HeadersCount()) - 1

	return nil
}

func (f *FileHeaderImportSource[T, F]) Close() error {
	return f.reader.Close()
}

// GetHeaderMetadata reads the metadata from the file. The metadata is memoized
// after the first call, with subsequent calls returning the cached result
// without re-reading the file.
//
// nolint:lll
func (f *FileHeaderImportSource[T, F]) GetHeaderMetadata() (*HeaderMetadata, error) {
	if f.metadata != nil {
		return f.metadata, nil
	}

	if f.reader == nil {
		return nil, errors.New("file reader not initialized")
	}

	// Read the first HeaderMetadataSize bytes containing metadata.
	buf := make([]byte, HeaderMetadataSize)
	_, err := f.reader.ReadAt(buf, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}

	// Calculate offsets.
	headerTypeOffset := BitcoinChainTypeSize
	startHeightOffset := BitcoinChainTypeSize + HeaderTypeSize

	metadata := HeaderMetadata{
		BitcoinChainType: wire.BitcoinNet(
			binary.LittleEndian.Uint32(buf[:BitcoinChainTypeSize]),
		),
		HeaderType: headerfs.HeaderType(buf[headerTypeOffset]),
		StartHeight: binary.LittleEndian.Uint32(
			buf[startHeightOffset:HeaderMetadataSize],
		),
	}

	// Validate header type.
	switch metadata.HeaderType {
	case headerfs.Block, headerfs.RegularFilter:
	default:
		return nil, fmt.Errorf("invalid header type: %s",
			metadata.HeaderType)
	}

	return &metadata, err
}

// Iterator returns an efficient iterator for sequential header access.
func (f *FileHeaderImportSource[T, F]) Iterator(start,
	end int) HeaderIterator[T, F] {

	return &ImportSourceHeaderIterator[T, F]{
		source: f, current: start, end: end,
	}
}

// GetHeader retrieves a single header at the specified index.
func (f *FileHeaderImportSource[T, F]) GetHeader(index int) (T, error) {
	var empty T

	// Calculate the absolute position for this header.
	headerSize := f.metadata.HeaderType.Size()
	offset := HeaderMetadataSize + (index * headerSize)

	// Check if requested header is within file bounds.
	if offset+headerSize > f.fileSize {
		return empty, fmt.Errorf("header index %d out of bounds", index)
	}

	// Read header data at the calculated offset.
	buf := make([]byte, headerSize)
	_, err := f.reader.ReadAt(buf, int64(offset))
	if err != nil {
		return empty, fmt.Errorf("failed to read header at "+
			"index %d: %w", index, err)
	}
	reader := bytes.NewReader(buf)

	// Calculate the actual height from index and start height.
	height := uint32(index) + f.metadata.StartHeight

	// Create the appropriate chain import header type based on metadata.
	header := f.factory.New()
	if err := header.Deserialize(reader, height); err != nil {
		return empty, err
	}

	return header, nil
}

// HeadersCount returns the number of headers in the file.
func (f *FileHeaderImportSource[T, F]) HeadersCount() int {
	usableFileSize := f.fileSize - HeaderMetadataSize

	// Handle the case where file might be smaller than metadata.
	if usableFileSize <= 0 {
		return 0
	}

	headerSize := f.metadata.HeaderType.Size()
	return usableFileSize / headerSize
}

// SetPath sets a string identifier for this import source.
func (f *FileHeaderImportSource[T, F]) SetPath(path string) {
	f.Path = path
}

// GetPath returns a string identifier for this import source.
func (f *FileHeaderImportSource[T, F]) GetPath() string {
	return f.Path
}

// validateTypeMatch ensures that the generic type T matches the header type in
// the file.
//
//nolint:lll
func (f *FileHeaderImportSource[T, F]) validateTypeMatch(m *HeaderMetadata) error {
	var header T
	return header.ValidateMetadata(m)
}

// HeadersImport uses a generic HeaderImportSource to import headers.
//
//nolint:lll
type HeadersImport struct {
	BlockHeadersImportSource  HeaderImportSource[*BlockHeader, HeaderFactory[*BlockHeader]]
	FilterHeadersImportSource HeaderImportSource[*FilterHeader, HeaderFactory[*FilterHeader]]
	BlockHeadersValidator     HeadersValidator[*BlockHeader, HeaderFactory[*BlockHeader]]
	FilterHeadersValidator    HeadersValidator[*FilterHeader, HeaderFactory[*FilterHeader]]
	options                   *ImportOptions
}

// Import is a multi-pass algorithm that loads, validates, and processes
// headers from the configured import sources into the target header stores. The
// Import process is currently performed only if the target stores are
// completely empty except for gensis block/filter header otherwise it is
// entirely skipped.
func (s *HeadersImport) Import(ctx context.Context) (*ImportResult, error) {
	// Check first if the target header stores are fresh.
	isFresh, err := s.isTargetFresh(
		s.options.TargetBlockHeaderStore,
		s.options.TargetFilterHeaderStore,
	)
	if err != nil {
		return nil, fmt.Errorf("headers import failed: %w", err)
	}
	if !isFresh {
		log.Info("Skipping headers import: target header stores are " +
			"not empty")
		return &ImportResult{}, nil
	}

	// Create result structure at the beginning to capture total duration.
	result := &ImportResult{
		StartTime: time.Now(),
	}

	// Initialize and open header sources before proceeding with validation
	// and import operations.
	if err := s.openSources(); err != nil {
		return nil, fmt.Errorf("headers import failed: %w", err)
	}
	defer s.closeSources()

	// Check if the sources are compatible with each other.
	if err := s.validateSourcesCompatibility(); err != nil {
		return nil, fmt.Errorf("headers import failed: %w", err)
	}

	// Validate chain continuity with the target chain.
	if err := s.validateChainContinuity(); err != nil {
		return nil, fmt.Errorf("headers import failed: %w", err)
	}

	// Validate all block headers from import source.
	if err := s.BlockHeadersValidator.Validate(
		s.BlockHeadersImportSource.Iterator(
			0, s.BlockHeadersImportSource.HeadersCount()-1,
		), s.options.TargetChainParams,
	); err != nil {
		return nil, fmt.Errorf("headers import failed: %w", err)
	}

	// Validate all filter headers from import source.
	if err := s.FilterHeadersValidator.Validate(
		s.FilterHeadersImportSource.Iterator(
			0, s.FilterHeadersImportSource.HeadersCount()-1,
		), s.options.TargetChainParams,
	); err != nil {
		return nil, fmt.Errorf("headers import failed: %w", err)
	}

	// Determine processing regions that partition the import task into
	// disjoint height ranges. Currently we only detect/process the new
	// headers region.
	regions := s.determineProcessingRegions()
	result.StartHeight = regions.ImportStartHeight
	result.EndHeight = regions.ImportEndHeight

	// Process new headers region.
	// Add headers from the import source to the target stores, extending
	// from their highest existing header up to the import source's end
	// height. This assumes the target stores are consistent and valid
	// (i.e., no divergence), allowing for safe extension with new data.
	err = s.processNewHeadersRegion(regions.NewHeaders, result)
	if err != nil {
		return nil, fmt.Errorf("headers import failed: new headers "+
			"processing failed: %w", err)
	}

	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime)

	log.Infof("Headers import completed: processed %d headers "+
		"(block and filter) (added: %d, skipped: %d) from height "+
		"%d to %d in %s (%.2f headers/sec, %.2f%% new)",
		result.ProcessedCount, result.AddedCount, result.SkippedCount,
		result.StartHeight, result.EndHeight, result.Duration,
		result.HeadersPerSecond(), result.NewHeadersPercentage())

	return result, nil
}

// isTargetFresh checks if the target header stores are in their initial state,
// meaning they contain only the genesis header (height 0).
func (s *HeadersImport) isTargetFresh(
	targetBlockHeaderStore headerfs.BlockHeaderStore,
	targetFilterHeaderStore headerfs.FilterHeaderStore) (bool, error) {

	// Get the chain tip from both target stores.
	_, blockTipHeight, err := targetBlockHeaderStore.ChainTip()
	if err != nil {
		return false, fmt.Errorf("failed to get target block header "+
			"chain tip: %w", err)
	}

	_, filterTipHeight, err := targetFilterHeaderStore.ChainTip()
	if err != nil {
		return false, fmt.Errorf("failed to get target filter header "+
			"chain tip: %w", err)
	}

	if blockTipHeight == 0 && filterTipHeight == 0 {
		return true, nil
	}

	return false, nil
}

// openSources initializes and opens all required header import sources. It
// verifies that all necessary import sources and validators are properly
// configured, then opens each source to prepare for data reading. Returns an
// error if any source is missing or fails to open.
func (s *HeadersImport) openSources() error {
	// Check if required import sources are provided.
	if s.BlockHeadersImportSource == nil ||
		s.FilterHeadersImportSource == nil {

		return fmt.Errorf("missing required header sources - block "+
			"headers source: %v, filter headers source: %v",
			s.BlockHeadersImportSource != nil,
			s.FilterHeadersImportSource != nil)
	}

	// Check if required validators are provided.
	if s.BlockHeadersValidator == nil || s.FilterHeadersValidator == nil {
		return fmt.Errorf("missing required header validators - block "+
			"headers validator: %v, filter headers "+
			"validator: %v", s.BlockHeadersValidator != nil,
			s.FilterHeadersValidator != nil)
	}

	// Init Block Headers Import Source.
	if err := s.BlockHeadersImportSource.Open(); err != nil {
		return err
	}

	// Init Filter Headers Import Source.
	if err := s.FilterHeadersImportSource.Open(); err != nil {
		return err
	}

	return nil
}

// closeSources safely closes all open header sources and logs any warnings
// encountered during cleanup.
func (s *HeadersImport) closeSources() {
	// Close block headers source.
	if err := s.BlockHeadersImportSource.Close(); err != nil {
		log.Warnf("Failed to close block headers source: %v", err)
	}

	// Close filter headers source.
	if err := s.FilterHeadersImportSource.Close(); err != nil {
		log.Warnf("Failed to close block headers source: %v", err)
	}
}

// validateSourcesCompatibility ensures that block and filter header sources
// are compatible with each other and with the target chain.
func (s *HeadersImport) validateSourcesCompatibility() error {
	// Get metadata from both header and filter sources.
	blockMetadata, err := s.BlockHeadersImportSource.GetHeaderMetadata()
	if err != nil {
		return err
	}
	filterMetadata, err := s.FilterHeadersImportSource.GetHeaderMetadata()
	if err != nil {
		return err
	}

	// Validate that network types match.
	if blockMetadata.BitcoinChainType != filterMetadata.BitcoinChainType {
		return fmt.Errorf("network type mismatch: block headers "+
			"from %s (%v), filter headers from "+
			"%s (%v)", s.BlockHeadersImportSource.GetPath(),
			blockMetadata.BitcoinChainType,
			s.FilterHeadersImportSource.GetPath(),
			filterMetadata.BitcoinChainType)
	}

	// Validate that the source network matches the target network.
	if blockMetadata.BitcoinChainType != s.options.TargetChainParams.Net {
		return fmt.Errorf("network mismatch: headers from import "+
			"sources are for %v, but target is %v",
			blockMetadata.BitcoinChainType,
			s.options.TargetChainParams.Net)
	}

	// Validate starting heights match.
	if blockMetadata.StartHeight != filterMetadata.StartHeight {
		return fmt.Errorf("start height mismatch: block headers start "+
			"at %d, filter headers start at %d",
			blockMetadata.StartHeight, filterMetadata.StartHeight)
	}

	// Validate header counts match exactly.
	var (
		blockCount  = s.BlockHeadersImportSource.HeadersCount()
		filterCount = s.FilterHeadersImportSource.HeadersCount()
	)
	if filterCount != blockCount {
		return fmt.Errorf("headers count mismatch: block headers "+
			"import source %s (%d), filter headers import source "+
			"%s (%d)", s.BlockHeadersImportSource.GetPath(),
			blockCount, s.FilterHeadersImportSource.GetPath(),
			filterCount)
	}

	// Verify header counts are greater than zero.
	if blockCount == 0 {
		return errors.New("no headers available in sources")
	}

	return nil
}

// validateChainContinuity ensures that headers from import sources can be
// properly connected to the existing headers in the target stores.
func (s *HeadersImport) validateChainContinuity() error {
	// Get metadata from block header source.
	sourceBlockMeta, err := s.BlockHeadersImportSource.GetHeaderMetadata()
	if err != nil {
		return fmt.Errorf("failed to get block header "+
			"metadata: %w", err)
	}

	// Get the chain tip from both target stores.
	_, blockTipHeight, err := s.options.TargetBlockHeaderStore.ChainTip()
	if err != nil {
		return fmt.Errorf("failed to get target block header chain "+
			"tip: %w", err)
	}

	_, filterTipHeight, err := s.options.TargetFilterHeaderStore.ChainTip()
	if err != nil {
		return fmt.Errorf("failed to get target block header chain "+
			"tip: %w", err)
	}

	// Verify that both target stores have the same tip height.
	// Instead of requiring exact equality, take the minimum of the two
	// heights as the effective chain tip to handle cases where one store
	// might be ahead.
	effectiveTipHeight := min(blockTipHeight, filterTipHeight)
	if blockTipHeight != filterTipHeight {
		log.Infof("Target header stores at different heights "+
			"(block=%d, filter=%d), using effective tip height %d",
			blockTipHeight, filterTipHeight, effectiveTipHeight)
	}

	// Extract import height range.
	importStartHeight := sourceBlockMeta.StartHeight
	importEndHeight := sourceBlockMeta.EndHeight

	// We know both target stores only contain the genesis block (height 0)
	// otherwise we wouldn't reach here. That means we're essentially
	// starting with a fresh chain, so we only need to verify the import
	// data starts at height 0 or 1.

	// If import wants to start after height 1, we'd have a gap
	if importStartHeight > 1 {
		return fmt.Errorf("target stores contain only genesis block "+
			"(height 0) but import data starts at height %d, "+
			"creating a gap", importStartHeight)
	}

	// If import includes genesis block (starts at 0), verify it matches.
	if importStartHeight == 0 {
		err := s.verifyHeadersAtHeight(importStartHeight)
		if err != nil {
			return fmt.Errorf("genesis block mismatch: %v", err)
		}
		log.Infof("Genesis block verified, import data will extend " +
			"chain from genesis")
	} else {
		// Import starts at height 1, which connects to genesis.

		// Validate that the block header at height 1 from the import
		// source connects with the previous header in the target block
		// store.
		targetBlockHeaderStore := s.options.TargetBlockHeaderStore
		prevBlkHdr, err := targetBlockHeaderStore.FetchHeaderByHeight(
			blockTipHeight,
		)
		if err != nil {
			return fmt.Errorf("failed to get block header from "+
				"target at height %d: %w", blockTipHeight, err)
		}

		// Retrieve and verify the current block header for import.
		sourceIndex := heightToSrcIndex(
			importStartHeight, sourceBlockMeta.StartHeight,
		)
		currBlkHeader, err := s.BlockHeadersImportSource.GetHeader(
			sourceIndex,
		)
		if err != nil {
			return fmt.Errorf("failed to get block header from "+
				"import source at height %d: %w",
				importStartHeight, err)
		}

		// Ensure the current header's previous block hash matches the
		// hash of the previously fetched block header to maintain chain
		// integrity.
		prevHash := prevBlkHdr.BlockHash()
		if !currBlkHeader.PrevBlock.IsEqual(&prevHash) {
			return fmt.Errorf("header chain broken: current "+
				"header's PrevBlock (%v) doesn't match "+
				"previous header's hash (%v)",
				currBlkHeader.PrevBlock, prevHash)
		}

		log.Info("Target stores contain only genesis block, import " +
			"data will extend chain from height 1")
	}

	log.Infof("Chain continuity validation successful: import data "+
		"(%d-%d) connects properly with target chain (tip: %d)",
		importStartHeight, importEndHeight, effectiveTipHeight)

	return nil
}

// determineProcessingRegions partitions the header height space into regions
// satisfying the MECE property (Mutually Exclusive, Collectively Exhaustive).
// This strict partitioning is critical for ensuring the idempotence of the
// overall import operation - repeated imports with the same parameters will
// produce identical results without side effects. By cleanly separating heights
// into non-overlapping regions with distinct processing logic, we can ensure
// consistent application of import policies regardless of how many times the
// operation is performed. The regions are:
//  1. Overlap: Heights common to both source and targets
//  2. Divergence: Heights where target stores differ within import range
//  3. BeyondImport: Heights in targets beyond source range
//  4. NewHeaders: Heights in source not yet in targets
func (s *HeadersImport) determineProcessingRegions() *ProcessingRegions {
	// Get necessary information.
	blockMeta, _ := s.BlockHeadersImportSource.GetHeaderMetadata()
	importStartHeight := blockMeta.StartHeight
	importEndHeight := blockMeta.EndHeight

	_, blockTipHeight, _ := s.options.TargetBlockHeaderStore.ChainTip()
	_, filterTipHeight, _ := s.options.TargetFilterHeaderStore.ChainTip()
	effectiveTipHeight := min(blockTipHeight, filterTipHeight)

	// Create regions.
	regions := &ProcessingRegions{
		ImportStartHeight: importStartHeight,
		ImportEndHeight:   importEndHeight,
		EffectiveTip:      effectiveTipHeight,
	}

	// New Headers region.
	// This region contains headers that are in the import source but not
	// yet in either target store. They start one height beyond the highest
	// tip of either store (ensuring no overlap with divergence region) and
	// extend to the end of the import data. These headers need to be added
	// to both stores. It only exists if there are headers beyond both tips.
	//
	// Note: This region is supposed to be processed after handling the
	// overlap, divergence, and beyond import regions, ensuring that any
	// potential inconsistencies in existing data are resolved before adding
	// new headers. This sequential processing guarantees that new headers
	// are only added on top of a verified and consistent chain state.
	newStart := max(blockTipHeight, filterTipHeight) + 1
	newEnd := importEndHeight
	regions.NewHeaders = HeaderRegion{
		Start:  newStart,
		End:    newEnd,
		Exists: newStart <= newEnd,
	}

	return regions
}

// verifyHeadersAtHeight compares headers at a specific height between import
// sources and target stores to ensure they match exactly.
func (s *HeadersImport) verifyHeadersAtHeight(height uint32) error {
	// Get metadata from block header source.
	sourceBlockMeta, err := s.BlockHeadersImportSource.GetHeaderMetadata()
	if err != nil {
		return fmt.Errorf("failed to get block header "+
			"metadata: %w", err)
	}

	// Get and verify block headers.
	sourceIndex := heightToSrcIndex(height, sourceBlockMeta.StartHeight)
	sourceBlkHeader, err := s.BlockHeadersImportSource.GetHeader(
		sourceIndex,
	)
	if err != nil {
		return fmt.Errorf("failed to get block header from import "+
			"source at height %d: %w", height, err)
	}

	blockHeaderStore := s.options.TargetBlockHeaderStore
	targetBlkHeader, err := blockHeaderStore.FetchHeaderByHeight(height)
	if err != nil {
		return fmt.Errorf("failed to get block header from target at "+
			"height %d: %w", height, err)
	}

	// Compare block headers.
	sourceBlkHeaderHash := sourceBlkHeader.BlockHash()
	targetBlkHeaderHash := targetBlkHeader.BlockHash()
	if !sourceBlkHeaderHash.IsEqual(&targetBlkHeaderHash) {
		return fmt.Errorf("block header mismatch at height %d: source "+
			"has %v but target has %v", height,
			sourceBlkHeaderHash, targetBlkHeaderHash)
	}

	// Get and verify filter headers.
	sourceFilterHeader, err := s.FilterHeadersImportSource.GetHeader(
		sourceIndex,
	)
	if err != nil {
		return fmt.Errorf("failed to get filter header from import "+
			"source at height %d: %w", height, err)
	}

	filterHeaderStore := s.options.TargetFilterHeaderStore
	targetFilterHeader, err := filterHeaderStore.FetchHeaderByHeight(height)
	if err != nil {
		return fmt.Errorf("failed to get filter header from target at "+
			"height %d: %w", height, err)
	}

	// Compare filter headers.
	sourceFilterHeaderHash := sourceFilterHeader.FilterHash
	targetFilterHeaderHash := *targetFilterHeader
	if !sourceFilterHeaderHash.IsEqual(&targetFilterHeaderHash) {
		return fmt.Errorf("filter header mismatch at height %d: "+
			"source has %v but target has %v", height,
			sourceFilterHeaderHash, targetFilterHeaderHash)
	}

	log.Debugf("Headers from %s (block) and %s (filter) verified at "+
		"height %d", s.BlockHeadersImportSource.GetPath(),
		s.FilterHeadersImportSource.GetPath(), height)

	return nil
}

// processNewHeadersRegion imports headers from the specified region into the
// target stores. This method handles the case where headers exist in the import
// source but not in the target stores.
func (s *HeadersImport) processNewHeadersRegion(region HeaderRegion,
	result *ImportResult) error {

	if !region.Exists {
		return nil
	}

	log.Infof("Adding %d new headers (block and filter) from heights "+
		"%d to %d", region.End-region.Start+1, region.Start, region.End)

	err := s.appendNewHeaders(region.Start, region.End)
	if err != nil {
		return fmt.Errorf("failed to append new headers: %w", err)
	}

	result.ProcessedCount += int(region.End - region.Start + 1)
	result.AddedCount += int(region.End - region.Start + 1)

	return nil
}

// appendNewHeaders adds new headers from import source.
func (s *HeadersImport) appendNewHeaders(startHeight, endHeight uint32) error {
	// Get metadata from sources.
	blockMeta, err := s.BlockHeadersImportSource.GetHeaderMetadata()
	if err != nil {
		return fmt.Errorf("failed to get block header "+
			"metadata: %w", err)
	}

	totalHeaders := endHeight - startHeight + 1
	log.Infof("Appending %d new headers in batches of %d", totalHeaders,
		s.options.WriteBatchSizePerRegion)

	// Process headers in batches.
	for batchStart := startHeight; batchStart <= endHeight; {
		// Calculate batch end, ensuring we don't exceed endHeight.
		batchEnd := min(
			batchStart+uint32(s.options.WriteBatchSizePerRegion)-1,
			endHeight,
		)

		// Calculate indices in the source based on the metadata start
		// height.
		sourceStartIdx := heightToSrcIndex(
			batchStart, blockMeta.StartHeight,
		)
		sourceEndIdx := heightToSrcIndex(
			batchEnd, blockMeta.StartHeight,
		)

		// Read block headers batch.
		blockHeaders := make(
			[]headerfs.BlockHeader, 0, batchEnd-batchStart+1,
		)
		blockIter := s.BlockHeadersImportSource.Iterator(
			sourceStartIdx, sourceEndIdx,
		)
		defer blockIter.Close()

		for {
			header, hasMore, err := blockIter.Next()
			if err != nil {
				return fmt.Errorf("failed to read block "+
					"header at height %d: %w",
					batchStart+uint32(len(blockHeaders)),
					err)
			}

			if !hasMore {
				break
			}

			blockHeaders = append(blockHeaders, header.BlockHeader)
		}

		// Read filter headers batch.
		filterHeaders := make(
			[]headerfs.FilterHeader, 0, batchEnd-batchStart+1,
		)
		filterIter := s.FilterHeadersImportSource.Iterator(
			sourceStartIdx, sourceEndIdx,
		)
		defer filterIter.Close()

		for {
			header, hasMore, err := filterIter.Next()
			if err != nil {
				return fmt.Errorf("failed to read filter "+
					"header at height %d: %w",
					batchStart+uint32(len(filterHeaders)),
					err)
			}

			if !hasMore {
				break
			}

			filterHeaders = append(
				filterHeaders, header.FilterHeader,
			)
		}

		if len(blockHeaders) == 0 {
			return fmt.Errorf("no headers read for "+
				"batch %d-%d", batchStart, batchEnd)
		}

		err := s.writeHeadersToTargetStores(
			blockHeaders, filterHeaders, batchStart, batchEnd,
		)
		if err != nil {
			return fmt.Errorf("failed to write headers to "+
				"target stores: %v", err)
		}

		log.Debugf("Wrote headers batch from height "+
			"%d to %d (%d headers)", batchStart, batchEnd,
			batchEnd-batchStart+1)

		// Move to next batch.
		batchStart = batchEnd + 1
	}

	log.Infof("Successfully added %d new headers from heights %d to %d",
		totalHeaders, startHeight, endHeight)

	return nil
}

// Write block and filter headers to the target stores in a specific order to
// ensure proper rollback operations if needed. It also rollbacks any headers
// written if any to target stores incase of any failures.
func (s *HeadersImport) writeHeadersToTargetStores(
	blockHeaders []headerfs.BlockHeader,
	filterHeaders []headerfs.FilterHeader, batchStart,
	batchEnd uint32) error {

	// Write block headers batch to target store.
	err := s.options.TargetBlockHeaderStore.WriteHeaders(
		blockHeaders...,
	)
	if err != nil {
		return fmt.Errorf("failed to write block headers "+
			"batch %d-%d: %w", batchStart, batchEnd, err)
	}

	// We only need to set the block header hash of the last filter
	// header to maintain chain tip consistency for regular tip.
	lastIdx := len(filterHeaders) - 1
	chainTipHash := blockHeaders[lastIdx].BlockHeader.BlockHash()
	filterHeaders[lastIdx].HeaderHash = chainTipHash

	// Write filter headers batch to target store.
	err = s.options.TargetFilterHeaderStore.WriteHeaders(filterHeaders...)
	if err != nil {
		// If we've reached here, the headers failed to be written to
		// target filter store because of I/O errors regarding binary
		// file or filter db store, and it is automatically rolled back
		// upstream.
		//
		// The whole import operation needs to satisfy the conjunction
		// property for both block and filter header stores - it's all
		// or nothing, so they need to be at the same length. We must
		// rollback the block headers to maintain this consistency.
		blockHeadersToTruncate := uint32(len(blockHeaders))
		_, err := s.options.TargetBlockHeaderStore.RollbackBlockHeaders(
			blockHeadersToTruncate,
		)
		if err != nil {
			return fmt.Errorf("failed to rollback %d headers "+
				"from target block header store to match "+
				"with the target filter header store, err: %w",
				blockHeadersToTruncate, err)
		}

		// We need to roll back to previous state.
		return fmt.Errorf("failed to write filter headers "+
			"batch %d-%d: %w", batchStart, batchEnd, err)
	}

	return nil
}

// ImportOptions defines parameters for the import process.
type ImportOptions struct {
	// TargetChainParams specifies the blockchain network parameters
	// for the chain into which headers will be imported.
	TargetChainParams chaincfg.Params

	// TargetBlockHeaderStore is the storage backend where block headers
	// will be written during the import.
	TargetBlockHeaderStore headerfs.BlockHeaderStore

	// TargetFilterHeaderStore is the storage backend where filter headers
	// will be written during the import.
	TargetFilterHeaderStore headerfs.FilterHeaderStore

	// BlockHeadersSource is the file path or source location for block
	// headers to be imported.
	BlockHeadersSource string

	// FilterHeadersSource is the file path or source location for filter
	// headers to be imported.
	FilterHeadersSource string

	// WriteBatchSizePerRegion specifies the number of headers to write in
	// each batch per region. This controls performance characteristics of
	// the import.
	WriteBatchSizePerRegion int
}

// Import executes the header import process with the configuration specified in
// the options. It handles the validation, construction of necessary components,
// and orchestration of the import. This method is NOT concurrency safe and
// should not be called simultaneously from multiple goroutines.
func (options *ImportOptions) Import(
	ctx context.Context) (*ImportResult, error) {

	// Create a derived context that we'll use for cancellable operations.
	importCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Validate required sources.
	if options.BlockHeadersSource == "" {
		return nil, errors.New("missing block headers source path")
	}

	if options.FilterHeadersSource == "" {
		return nil, errors.New("missing filter headers source path")
	}

	// Set default batch size if not specified.
	if options.WriteBatchSizePerRegion <= 0 {
		options.WriteBatchSizePerRegion = 10000
		log.Infof("Using default write batch size of %d "+
			"headers per region", options.WriteBatchSizePerRegion)
	}

	// Create block headers import source based on source string format.
	blockHeadersSource := options.createBlockHeadersImportSource()

	// Create filter headers import source based on source string format.
	filterHeadersSource := options.createFilterHeadersImportSource()

	// Create block headers import source validator.
	blockHeadersValidator := &BlockHeadersImportSourceValidator{}

	// Create filter headers import source validator.
	filterHeadersValidator := &FilterHeadersImportSourceValidator{}

	// Construct the importer with all required components.
	importer := &HeadersImport{
		BlockHeadersImportSource:  blockHeadersSource,
		FilterHeadersImportSource: filterHeadersSource,
		BlockHeadersValidator:     blockHeadersValidator,
		FilterHeadersValidator:    filterHeadersValidator,
		options:                   options,
	}

	// Execute the import operation with the current options.
	return importer.Import(importCtx)
}

// createBlockHeadersImportSource creates the appropriate import source for
// block headers.
//
// nolint:lll
func (options *ImportOptions) createBlockHeadersImportSource() HeaderImportSource[*BlockHeader, HeaderFactory[*BlockHeader]] {
	return &FileHeaderImportSource[*BlockHeader, HeaderFactory[*BlockHeader]]{
		Path:    options.BlockHeadersSource,
		factory: &BlockHeader{},
	}
}

// createFilterHeadersImportSource creates the appropriate import source for
// filter headers.
//
// nolint:lll
func (options *ImportOptions) createFilterHeadersImportSource() HeaderImportSource[*FilterHeader, HeaderFactory[*FilterHeader]] {
	return &FileHeaderImportSource[*FilterHeader, HeaderFactory[*FilterHeader]]{
		Path:    options.FilterHeadersSource,
		factory: &FilterHeader{},
	}
}

// ImportResult contains statistics about a header import operation.
type ImportResult struct {
	// ProcessedCount is the total number of headers examined.
	ProcessedCount int

	// AddedCount is the number of headers newly added to destination.
	AddedCount int

	// SkippedCount is the number of headers already in destination.
	SkippedCount int

	// StartHeight is the first height processed.
	StartHeight uint32

	// EndHeight is the last height processed.
	EndHeight uint32

	// StartTime is the time when import operation started.
	StartTime time.Time

	// EndTime is the time when import operation completed.
	EndTime time.Time

	// Duration is the total time taken for the import operation.
	Duration time.Duration
}

// HeadersPerSecond calculates the processing rate in headers per second as a
// performance metric. Returns 0 if Duration is zero to avoid division by zero.
func (r *ImportResult) HeadersPerSecond() float64 {
	if r.Duration.Seconds() > 0 {
		return float64(r.ProcessedCount) / r.Duration.Seconds()
	}
	return 0
}

// NewHeadersPercentage calculates the percentage of processed headers that were
// newly added (not already present in the target). Returns 0 if no headers were
// processed to avoid division by zero.
func (r *ImportResult) NewHeadersPercentage() float64 {
	if r.ProcessedCount > 0 {
		return float64(r.AddedCount) / float64(r.ProcessedCount) * 100
	}
	return 0
}

// heightToSrcIndex converts an absolute blockchain height to a
// source-relative index based on the provided start height.
func heightToSrcIndex(height, startHeight uint32) int {
	return int(height - startHeight)
}

// AddHeadersImportMetadata prepares a header file for import by adding the
// necessary metadata to the file. This function takes an existing header file
// and prepends metadata required by Neutrino's header import feature.
func AddHeadersImportMetadata(sourceFilePath string, chainType wire.BitcoinNet,
	headerType headerfs.HeaderType, startHeight uint32) error {

	// Open the existing file for reading.
	sourceFile, err := os.Open(sourceFilePath)
	if err != nil {
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer sourceFile.Close()

	// Create a temporary file pattern using the original filename.
	tmpPattern := fmt.Sprintf("%s_with_metadata_*.tmp",
		filepath.Base(sourceFilePath))

	// Create a temporary file for writing.
	tempFile, err := os.CreateTemp(filepath.Dir(sourceFilePath), tmpPattern)
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %w", err)
	}
	tempFilePath := tempFile.Name()

	// Set up cleanup for the temporary file.
	defer func() {
		tempFile.Close()
		// Only attempt to remove if the file still exists at the path.
		if _, statErr := os.Stat(tempFilePath); statErr == nil {
			os.Remove(tempFilePath)
		}
	}()

	// Create a buffer for binary metadata.
	var metadataBuf bytes.Buffer

	// Write chainType (4 bytes).
	err = binary.Write(&metadataBuf, binary.LittleEndian, chainType)
	if err != nil {
		return fmt.Errorf("failed to write chain type: %w", err)
	}

	// Write headerType (1 byte).
	err = metadataBuf.WriteByte(byte(headerType))
	if err != nil {
		return fmt.Errorf("failed to write header type: %w", err)
	}

	// Write startHeight (4 bytes).
	err = binary.Write(&metadataBuf, binary.LittleEndian, startHeight)
	if err != nil {
		return fmt.Errorf("failed to write start height: %w", err)
	}

	// Write metadata to the temp file.
	if _, err = tempFile.Write(metadataBuf.Bytes()); err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	// Copy the original file content to the temp file.
	if _, err = io.Copy(tempFile, sourceFile); err != nil {
		return fmt.Errorf("failed to copy original file content: %w",
			err)
	}

	// Sync the file to ensure all data is written to disk.
	if err = tempFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync temporary file: %w", err)
	}

	// Replace the original file with our new file.
	if err = os.Rename(tempFilePath, sourceFilePath); err != nil {
		return fmt.Errorf("failed to replace original file: %w", err)
	}

	return nil
}
