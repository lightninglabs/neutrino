package chainimport

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btclog"
	"github.com/lightninglabs/neutrino/headerfs"
	"golang.org/x/exp/mmap"
)

var log btclog.Logger

// BitcoinChainNetType is a compact uint8 identifier for different Bitcoin
// chain networks.
type BitcoinChainType uint8

const (
	MainNetID  BitcoinChainType = 0x00
	TestNetID  BitcoinChainType = 0x01
	TestNet3ID BitcoinChainType = 0x02
	TestNet4ID BitcoinChainType = 0x03
	SigNetID   BitcoinChainType = 0x04
	SimNetID   BitcoinChainType = 0x05
)

// HeaderType represents the type of Bitcoin headers being processed.
type HeaderType uint8

const (
	// Header Type.
	BlockHeader HeaderType = iota
	FilterHeader
	UnknownHeader HeaderType = 255
)

const (
	// Header Metadata size in bytes.
	HeaderMetadataSize = 6

	// Unknown HeaderSize.
	UnknownHeaderSize = 0
)

// String returns a human-readable representation of the Bitcoin chain type.
func (c BitcoinChainType) String() string {
	switch c {
	case MainNetID:
		return "MainNet"
	case TestNetID:
		return "TestNet"
	case TestNet3ID:
		return "TestNet3"
	case TestNet4ID:
		return "TestNet4"
	case SigNetID:
		return "SigNet"
	case SimNetID:
		return "SimNet"
	default:
		return fmt.Sprintf("UnknownChain(%d)", c)
	}
}

// String returns a human-readable representation of the header type.
func (h HeaderType) String() string {
	switch h {
	case BlockHeader:
		return "BlockHeader"
	case FilterHeader:
		return "FilterHeader"
	case UnknownHeader:
		return "UnknownHeader"
	default:
		return fmt.Sprintf("HeaderType(%d)", h)
	}
}

// BitcoinNetToChainType converts a wire.BitcoinNet value to the
// corresponding BitcoinChainType value.
func BitcoinNetToChainType(n wire.BitcoinNet) (BitcoinChainType, error) {
	switch n {
	case wire.MainNet:
		return MainNetID, nil
	case wire.TestNet:
		return TestNetID, nil
	case wire.TestNet3:
		return TestNet3ID, nil
	case wire.TestNet4:
		return TestNet4ID, nil
	case wire.SigNet:
		return SigNetID, nil
	case wire.SimNet:
		return SimNetID, nil
	default:
		return 0, fmt.Errorf("unknown bitcoin network: %#x", uint32(n))
	}
}

// ChainImportHeader defines the common interface for both block and filter
// headers that can be processed during chain import operations.
type ChainImportHeader interface {
	// Deserialize reconstructs the header from binary data at the specified
	// height.
	Deserialize(io.Reader, uint32) error

	// ValidateMetadata ensures the header type is compatible with the
	// provided metadata.
	ValidateMetadata(*HeaderMetadata) error
}

// ChainImportBlockHeader represents a block header that can be imported into
// the chain store. It wraps a headerfs.BlockHeader with additional
// functionality needed for the import process.
type ChainImportBlockHeader struct {
	*headerfs.BlockHeader
}

// ChainImportFilterHeader represents a filter header that can be imported into
// the chain store. It wraps a headerfs.FilterHeader with additional
// functionality needed for the import process.
type ChainImportFilterHeader struct {
	*headerfs.FilterHeader
}

// Deserialize reconstructs a block header from binary data at the specified
// height. It handles nil internal fields by properly initializing them before
// deserialization.
func (h *ChainImportBlockHeader) Deserialize(r io.Reader,
	height uint32) error {

	// Initialize if nil.
	if h.BlockHeader == nil {
		h.BlockHeader = &headerfs.BlockHeader{
			BlockHeader: &wire.BlockHeader{},
		}
	} else if h.BlockHeader.BlockHeader == nil {
		// Also ensure the wire.BlockHeader is initialized.
		h.BlockHeader.BlockHeader = &wire.BlockHeader{}
	}

	// Deserialize the wire.BlockHeader portion.
	if err := h.BlockHeader.BlockHeader.Deserialize(r); err != nil {
		return fmt.Errorf("failed to deserialize "+
			"wire.BlockHeader: %w", err)
	}

	h.BlockHeader.Height = height

	return nil
}

// Deserialize reconstructs a filter header from binary data at the specified
// height. It ensures the FilterHeader field is properly initialized before
// reading data.
func (f *ChainImportFilterHeader) Deserialize(r io.Reader,
	height uint32) error {

	// Initialize if nil
	if f.FilterHeader == nil {
		f.FilterHeader = &headerfs.FilterHeader{}
	}

	// Read the filter hash (32 bytes).
	if _, err := io.ReadFull(r, f.FilterHash[:]); err != nil {
		return fmt.Errorf("failed to read filter hash: %w", err)
	}

	// Set the height.
	f.FilterHeader.Height = height

	return nil
}

func (h *ChainImportBlockHeader) ValidateMetadata(m *HeaderMetadata) error {
	if m.HeaderType != BlockHeader {
		return fmt.Errorf("type mismatch: file contains %v headers, "+
			"but expected block headers", m.HeaderType)
	}
	return nil
}

func (h *ChainImportFilterHeader) ValidateMetadata(m *HeaderMetadata) error {
	if m.HeaderType != FilterHeader {
		return fmt.Errorf("type mismatch: file contains %v headers, "+
			"but expected filter headers", m.HeaderType)
	}
	return nil
}

// Ensure BlockHeader and FilterHeader implements ChainImportHeader.
var _ ChainImportHeader = (*ChainImportBlockHeader)(nil)
var _ ChainImportHeader = (*ChainImportFilterHeader)(nil)

// HeaderIterator interface for iterating over headers.
type HeaderIterator[T any] interface {
	Next() (header T, hasMore bool, err error)
	Close() error
}

// HeaderSize returns the size in bytes for a given header type.
func (ht HeaderType) Size() int {
	switch ht {
	case BlockHeader:
		return headerfs.BlockHeaderSize
	case FilterHeader:
		return headerfs.RegularFilterHeaderSize
	default:
		return UnknownHeaderSize
	}
}

// HeaderMetadata contains the metadata about the header source.
type HeaderMetadata struct {
	BitcoinChainType BitcoinChainType
	HeaderType       HeaderType
	StartHeight      uint32
	EndHeight        uint32
}

// HeaderImportSource is a generic interface for loading headers of type T.
type HeaderImportSource[T any] interface {
	Open() error
	Close() error
	GetHeaderMetadata() (*HeaderMetadata, error)
	Iterator(start, end int) HeaderIterator[T]
	GetHeader(index int) (T, error)
	HeadersCount() int
	SourceName() string
}

// FileHeaderSource implements HeaderSource for files.
type FileHeaderImportSource[T any] struct {
	Path     string
	reader   *mmap.ReaderAt
	fileSize int
	metadata *HeaderMetadata
}

// Open opens the file and initializes the mmap reader.
func (f *FileHeaderImportSource[T]) Open() error {
	// Only open if not already open
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

func (f *FileHeaderImportSource[T]) Close() error {
	return f.reader.Close()
}

// GetHeaderMetadata reads the metadata from the file.
func (f *FileHeaderImportSource[T]) GetHeaderMetadata() (*HeaderMetadata, error) {
	if f.metadata != nil {
		return f.metadata, nil
	}

	if f.reader == nil {
		return nil, errors.New("file reader not initialized")
	}

	// Read the first 6 bytes containing metadata.
	buf := make([]byte, HeaderMetadataSize)
	_, err := f.reader.ReadAt(buf, 0)
	if err != nil {
		return &HeaderMetadata{}, fmt.Errorf("failed to read "+
			"metadata: %w", err)
	}

	// Parse metadata.
	metadata := HeaderMetadata{
		BitcoinChainType: BitcoinChainType(buf[0]),
		HeaderType:       HeaderType(buf[1]),
		StartHeight:      binary.LittleEndian.Uint32(buf[2:6]),
	}

	// Validate header type.
	switch metadata.HeaderType {
	case BlockHeader, FilterHeader:
	default:
		metadata = HeaderMetadata{}
		err = fmt.Errorf("invalid header type: %s", metadata.HeaderType)
	}

	return &metadata, err
}

// validateTypeMatch ensures that the generic type T matches the header type in
// the file.
func (f *FileHeaderImportSource[T]) validateTypeMatch(
	metadata *HeaderMetadata) error {

	var empty T

	// Create the appropriate header type
	var header ChainImportHeader
	switch any(empty).(type) {
	case *ChainImportBlockHeader:
		header = &ChainImportBlockHeader{}
	case *ChainImportFilterHeader:
		header = &ChainImportFilterHeader{}
	default:
		return fmt.Errorf("unsupported generic type: %T", empty)
	}

	return header.ValidateMetadata(metadata)
}

// Iterator returns an efficient iterator for sequential header access.
func (f *FileHeaderImportSource[T]) Iterator(start,
	end int) HeaderIterator[T] {

	return &ImportSourceHeaderIterator[T]{
		source: f, current: start, end: end,
	}
}

func (f *FileHeaderImportSource[T]) GetHeader(index int) (T, error) {
	var empty T

	// Calculate the absolute position for this header.
	headerSize := f.metadata.HeaderType.Size()
	offset := HeaderMetadataSize + (index * headerSize)

	// Check if requested header is within file bounds.
	if offset+headerSize > f.fileSize {
		return empty, fmt.Errorf("header index %d out of bounds", index)
	}

	// Calculate the actual height from index and start height.
	height := uint32(index) + f.metadata.StartHeight

	// Read header data at the calculated offset.
	buf := make([]byte, headerSize)
	_, err := f.reader.ReadAt(buf, int64(offset))
	if err != nil {
		return empty, fmt.Errorf("failed to read header at "+
			"index %d: %w", index, err)
	}
	reader := bytes.NewReader(buf)

	// Create the appropriate chain import header type based on metadata.
	var header ChainImportHeader
	switch f.metadata.HeaderType {
	case BlockHeader:
		header = &ChainImportBlockHeader{
			BlockHeader: &headerfs.BlockHeader{},
		}
	case FilterHeader:
		header = &ChainImportFilterHeader{
			FilterHeader: &headerfs.FilterHeader{},
		}
	default:
		return empty, fmt.Errorf("unsupported header type: %s",
			f.metadata.HeaderType)
	}

	if err := header.Deserialize(reader, height); err != nil {
		return empty, err
	}

	return any(header).(T), err
}

// HeadersCount returns the number of headers in the file.
func (f *FileHeaderImportSource[T]) HeadersCount() int {
	usableFileSize := f.fileSize - HeaderMetadataSize

	// Handle the case where file might be smaller than metadata.
	if usableFileSize <= 0 {
		return 0
	}

	headerSize := f.metadata.HeaderType.Size()
	return usableFileSize / headerSize
}

func (f *FileHeaderImportSource[T]) SourceName() string {
	return fmt.Sprintf("file(%s)", f.Path)
}

// HeaderValidator is a generic interface for validating headers of type T.
type HeadersValidator[T any] interface {
	Validate(HeaderIterator[T], chaincfg.Params) error
	ValidatePair(prev, current T, targetChainParams chaincfg.Params) error
}

// BlockHeadersImportSourceValidator implements HeadersValidator for block
// headers.
type BlockHeadersImportSourceValidator struct{}

func (v *BlockHeadersImportSourceValidator) Validate(
	iterator HeaderIterator[*ChainImportBlockHeader],
	targetChainParams chaincfg.Params) error {

	var (
		prevHeader *ChainImportBlockHeader
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
		var header *ChainImportBlockHeader
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

func (v *BlockHeadersImportSourceValidator) ValidatePair(prev,
	current *ChainImportBlockHeader, targetChainParams chaincfg.Params) error {

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
	iterator HeaderIterator[*ChainImportFilterHeader],
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
	current *ChainImportFilterHeader, targetChainParams chaincfg.Params) error {

	return nil
}

// HeadersImport uses a generic HeaderSource to import headers.
type HeadersImport struct {
	BlockHeadersImportSource  HeaderImportSource[*ChainImportBlockHeader]
	FilterHeadersImportSource HeaderImportSource[*ChainImportFilterHeader]
	BlockHeadersValidator     HeadersValidator[*ChainImportBlockHeader]
	FilterHeadersValidator    HeadersValidator[*ChainImportFilterHeader]
}

// Import is a multi-pass algorithm that loads, validates, and processes
// headers from the configured import sources into the target header stores. The
// Import process is currently performed only if the target stores are
// completely empty except for gensis block/filter header otherwise it is
// entirely skipped.
func (s *HeadersImport) Import(ctx context.Context,
	options ImportOptions) (*ImportResult, error) {

	// Check first if the target header stores are fresh.
	isFresh, err := s.isTargetFresh(
		options.TargetBlockHeaderStore,
		options.TargetFilterHeaderStore,
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
	if err := s.initSources(); err != nil {
		return nil, fmt.Errorf("headers import failed: %w", err)
	}
	defer s.deinitSources()

	// Check if the sources are compatible with each other.
	if err := s.validateSourcesCompatibility(
		s.BlockHeadersImportSource, s.FilterHeadersImportSource,
		options.TargetChainParams.Net,
	); err != nil {
		return nil, fmt.Errorf("headers import failed: %w", err)
	}

	// Validate chain continuity with the target chain.
	if err := s.validateChainContinuity(
		options.TargetBlockHeaderStore, options.TargetFilterHeaderStore,
	); err != nil {
		return nil, fmt.Errorf("headers import failed: %w", err)
	}

	// Validate all block headers from import source.
	if err := s.BlockHeadersValidator.Validate(
		s.BlockHeadersImportSource.Iterator(
			0, s.BlockHeadersImportSource.HeadersCount()-1,
		), options.TargetChainParams,
	); err != nil {
		return nil, fmt.Errorf("headers import failed: %w", err)
	}

	// Validate all filter headers from import source.
	if err := s.FilterHeadersValidator.Validate(
		s.FilterHeadersImportSource.Iterator(
			0, s.FilterHeadersImportSource.HeadersCount()-1,
		), options.TargetChainParams,
	); err != nil {
		return nil, fmt.Errorf("headers import failed: %w", err)
	}

	// Determine processing regions that partition the import task into
	// disjoint height ranges. Currently we only detect/process the new
	// headers region.
	regions := s.determineProcessingRegions(options)
	result.StartHeight = regions.ImportStartHeight
	result.EndHeight = regions.ImportEndHeight

	// Process new headers region.
	// Add headers from the import source to the target stores, extending
	// from their highest existing header up to the import source's end
	// height. This assumes the target stores are consistent and valid
	// (i.e., no divergence), allowing for safe extension with new data.
	err = s.processNewHeadersRegion(regions.NewHeaders, options, result)
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
		result.StartHeight, result.EndHeight,
		formatDuration(result.Duration), result.HeadersPerSecond(),
		result.NewHeadersPercentage())

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

func (s *HeadersImport) initSources() error {
	// Check if required import sources are provided.
	if s.BlockHeadersImportSource == nil || s.FilterHeadersImportSource == nil {
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

// deinitSources safely closes all open header sources and logs any warnings
// encountered during cleanup.
func (s *HeadersImport) deinitSources() {
	// Close block headers source.
	if err := s.BlockHeadersImportSource.Close(); err != nil {
		log.Warnf("Failed to close block headers source: %v", err)
	}

	// Close filter headers source.
	if err := s.FilterHeadersImportSource.Close(); err != nil {
		log.Warnf("Failed to close block headers source: %v", err)
	}

	// Uinitialize logger.
	log = nil
}

// heightToSrcIndex converts an absolute blockchain height to a
// source-relative index based on the provided start height.
func (s *HeadersImport) heightToSrcIndex(height, startHeight uint32) int {
	return int(height - startHeight)
}

// ImportSourceHeaderIterator provides efficient iteration over headers from
// a source.
type ImportSourceHeaderIterator[T any] struct {
	source  HeaderImportSource[T]
	current int
	end     int
}

// Next returns the next header in the sequence, along with a boolean indicating
// whether there are more headers available and any error encountered.
func (it *ImportSourceHeaderIterator[T]) Next() (T, bool, error) {
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
func (it *ImportSourceHeaderIterator[T]) Close() error {
	// Don't close the source, as it may be used elsewhere.
	return nil
}

// Compile-time check to ensure ImportSourceHeaderIterator implements
// HeaderIterator.
var _ HeaderIterator[any] = (*ImportSourceHeaderIterator[any])(nil)

func (s *HeadersImport) validateSourcesCompatibility(
	blockHeadersSource HeaderImportSource[*ChainImportBlockHeader],
	filterHeadersSource HeaderImportSource[*ChainImportFilterHeader],
	targetBitcoinNet wire.BitcoinNet) error {

	// Get metadata from both header and filter sources.
	blockMetadata, err := blockHeadersSource.GetHeaderMetadata()
	if err != nil {
		return err
	}
	filterMetadata, err := filterHeadersSource.GetHeaderMetadata()
	if err != nil {
		return err
	}

	// Validate that network types match.
	if blockMetadata.BitcoinChainType != filterMetadata.BitcoinChainType {
		return fmt.Errorf("network type mismatch: block headers "+
			"from %s (%v), filter headers from "+
			"%s (%v)", blockHeadersSource.SourceName(),
			blockMetadata.BitcoinChainType,
			filterHeadersSource.SourceName(),
			filterMetadata.BitcoinChainType)
	}

	// Validate that the source network matches the target network.
	targetChainType, err := BitcoinNetToChainType(targetBitcoinNet)
	if err != nil {
		return fmt.Errorf("invalid target bitcoin network: %w", err)
	}
	if blockMetadata.BitcoinChainType != targetChainType {
		return fmt.Errorf("network mismatch: headers from import "+
			"sources are for %v, but target is %v",
			blockMetadata.BitcoinChainType, targetChainType)
	}

	// Validate starting heights match.
	if blockMetadata.StartHeight != filterMetadata.StartHeight {
		return fmt.Errorf("start height mismatch: block headers start "+
			"at %d, filter headers start at %d",
			blockMetadata.StartHeight, filterMetadata.StartHeight)
	}

	// Validate header counts match exactly.
	var (
		blockCount  = blockHeadersSource.HeadersCount()
		filterCount = filterHeadersSource.HeadersCount()
	)
	if filterCount != blockCount {
		return fmt.Errorf("headers count mismatch: block headers "+
			"import source %s (%d), filter headers import source "+
			"%s (%d)", blockHeadersSource.SourceName(), blockCount,
			filterHeadersSource.SourceName(), filterCount)
	}

	// Verify header counts are greater than zero.
	if blockCount == 0 {
		return errors.New("no headers available in sources")
	}

	return nil
}

// validateChainContinuity ensures that headers from import sources can be
// properly connected to the existing headers in the target stores.
func (s *HeadersImport) validateChainContinuity(
	targetBlockHeaderStore headerfs.BlockHeaderStore,
	targetFilterHeaderStore headerfs.FilterHeaderStore) error {

	// Get metadata from block header source.
	sourceBlockMeta, err := s.BlockHeadersImportSource.GetHeaderMetadata()
	if err != nil {
		return fmt.Errorf("failed to get block header "+
			"metadata: %w", err)
	}

	// Get the chain tip from both target stores.
	_, blockTipHeight, err := targetBlockHeaderStore.ChainTip()
	if err != nil {
		return fmt.Errorf("failed to get target block header chain "+
			"tip: %w", err)
	}

	_, filterTipHeight, err := targetFilterHeaderStore.ChainTip()
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
		err := s.verifyHeadersAtHeight(
			importStartHeight, targetBlockHeaderStore,
			targetFilterHeaderStore,
		)
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
		prevBlkHdr, err := targetBlockHeaderStore.FetchHeaderByHeight(
			blockTipHeight,
		)
		if err != nil {
			return fmt.Errorf("failed to get block header from "+
				"target at height %d: %w", blockTipHeight, err)
		}

		// Retrieve and verify the current block header for import.
		sourceIndex := s.heightToSrcIndex(
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

func (s *HeadersImport) verifyHeadersAtHeight(height uint32,
	targetBlockHeaderStore headerfs.BlockHeaderStore,
	targetFilterHeaderStore headerfs.FilterHeaderStore) error {

	// Get metadata from block header source.
	sourceBlockMeta, err := s.BlockHeadersImportSource.GetHeaderMetadata()
	if err != nil {
		return fmt.Errorf("failed to get block header "+
			"metadata: %w", err)
	}

	// Get and verify block headers.
	sourceIndex := s.heightToSrcIndex(height, sourceBlockMeta.StartHeight)
	sourceBlkHeader, err := s.BlockHeadersImportSource.GetHeader(sourceIndex)
	if err != nil {
		return fmt.Errorf("failed to get block header from import "+
			"source at height %d: %w", height, err)
	}

	targetBlkHeader, err := targetBlockHeaderStore.FetchHeaderByHeight(height)
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

	targetFilterHeader, err := targetFilterHeaderStore.FetchHeaderByHeight(
		height,
	)
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
		"height %d", s.BlockHeadersImportSource.SourceName(),
		s.FilterHeadersImportSource.SourceName(), height)

	return nil
}

// HeaderRegion represents a contiguous range of headers.
type HeaderRegion struct {
	Start  uint32
	End    uint32
	Exists bool // Whether this region has headers to process
}

// ProcessingRegions currently contains only the NewHeaders region to process.
type ProcessingRegions struct {
	ImportStartHeight uint32
	ImportEndHeight   uint32
	EffectiveTip      uint32
	NewHeaders        HeaderRegion
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
func (s *HeadersImport) determineProcessingRegions(
	options ImportOptions) *ProcessingRegions {

	// Get necessary information.
	blockMeta, _ := s.BlockHeadersImportSource.GetHeaderMetadata()
	importStartHeight := blockMeta.StartHeight
	importEndHeight := blockMeta.EndHeight

	_, blockTipHeight, _ := options.TargetBlockHeaderStore.ChainTip()
	_, filterTipHeight, _ := options.TargetFilterHeaderStore.ChainTip()
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

func (s *HeadersImport) processNewHeadersRegion(region HeaderRegion,
	options ImportOptions, result *ImportResult) error {

	if !region.Exists {
		return nil
	}

	log.Infof("Adding %d new headers (block and filter) from heights "+
		"%d to %d", region.End-region.Start+1, region.Start, region.End)

	err := s.appendNewHeaders(region.Start, region.End, options)
	if err != nil {
		return fmt.Errorf("failed to append new headers: %w", err)
	}

	result.ProcessedCount += int(region.End - region.Start + 1)
	result.AddedCount += int(region.End - region.Start + 1)

	return nil
}

// appendNewHeaders adds new headers from import source.
func (s *HeadersImport) appendNewHeaders(startHeight, endHeight uint32,
	options ImportOptions) error {

	// Get metadata from sources.
	blockMeta, err := s.BlockHeadersImportSource.GetHeaderMetadata()
	if err != nil {
		return fmt.Errorf("failed to get block header "+
			"metadata: %w", err)
	}

	totalHeaders := endHeight - startHeight + 1
	log.Infof("Appending %d new headers in batches of %d", totalHeaders,
		options.WriteBatchSize)

	// Process headers in batches.
	for batchStart := startHeight; batchStart <= endHeight; {
		// Calculate batch end, ensuring we don't exceed endHeight.
		batchEnd := min(
			batchStart+uint32(options.WriteBatchSize)-1, endHeight,
		)

		// Calculate indices in the source based on the metadata start
		// height.
		sourceStartIdx := s.heightToSrcIndex(
			batchStart, blockMeta.StartHeight,
		)
		sourceEndIdx := s.heightToSrcIndex(
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

			blockHeaders = append(blockHeaders, *header.BlockHeader)
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
				filterHeaders, *header.FilterHeader,
			)
		}

		if len(blockHeaders) == 0 {
			return fmt.Errorf("no headers read for "+
				"batch %d-%d", batchStart, batchEnd)
		}

		// TODO(mohamedawnallah): Writing Headers to both target block
		// and filters header store need to be done atomically. On any
		// failure we need to revert to previous valid state and abort
		// the import process.

		// Write block headers batch to target store.
		err = options.TargetBlockHeaderStore.WriteHeaders(
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
		err = options.TargetFilterHeaderStore.WriteHeaders(
			filterHeaders...,
		)
		if err != nil {
			return fmt.Errorf("failed to write filter headers "+
				"batch %d-%d: %w", batchStart, batchEnd, err)
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

// Import executes the header import process with the configuration specified in
// the options. It handles the validation, construction of necessary components,
// and orchestration of the import.
func (options *ImportOptions) Import(ctx context.Context) (*ImportResult, error) {
	// Create a derived context that we'll use for cancellable operations
	importCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Validate required sources.
	if options.BlockHeadersSource == "" {
		return nil, errors.New("missing block headers source path")
	}

	if options.FilterHeadersSource == "" {
		return nil, errors.New("missing filter headers source path")
	}

	// Set Logger.
	log = options.Logger

	// Set default batch size if not specified.
	if options.WriteBatchSize <= 0 {
		options.WriteBatchSize = 10000
		log.Infof("Using default write batch size of %d "+
			"headers", options.WriteBatchSize)
	}

	// Create block headers import source based on source string format.
	blockHeadersSource, err := options.createBlockHeadersImportSource()
	if err != nil {
		return nil, fmt.Errorf("failed to create block headers import "+
			"source: %w", err)
	}

	// Create filter headers import source based on source string format.
	filterHeadersSource, err := options.createFilterHeadersImportSource()
	if err != nil {
		return nil, fmt.Errorf("failed to create filter headers "+
			"import source: %w", err)
	}
	// Construct the importer with all required components.
	importer := &HeadersImport{
		BlockHeadersImportSource:  blockHeadersSource,
		FilterHeadersImportSource: filterHeadersSource,
		BlockHeadersValidator:     &BlockHeadersImportSourceValidator{},
		FilterHeadersValidator:    &FilterHeadersImportSourceValidator{},
	}

	// Execute the import operation with the current options.
	return importer.Import(importCtx, *options)
}

// createBlockHeadersImportSource creates the appropriate import source for
// block headers.
func (options *ImportOptions) createBlockHeadersImportSource() (HeaderImportSource[*ChainImportBlockHeader], error) {
	return createSpecificImportSource[*ChainImportBlockHeader](
		options.BlockHeadersSource,
	)
}

// createFilterHeadersImportSource creates the appropriate import source for
// filter headers.
func (options *ImportOptions) createFilterHeadersImportSource() (HeaderImportSource[*ChainImportFilterHeader], error) {
	return createSpecificImportSource[*ChainImportFilterHeader](
		options.FilterHeadersSource,
	)
}

// createSpecificImportSource is a private helper function (not a method) that
// handles the generic implementation.
//
// nolint:unparam
func createSpecificImportSource[T any](source string) (HeaderImportSource[T], error) {
	// Check if source could be interpreted as a file path
	// This is our default fallback for any string that doesn't match other
	// patterns.
	return &FileHeaderImportSource[T]{
		Path: source,
	}, nil

	// For future expansion, additional source types can be added here with
	// their own pattern detection and source construction.
}

// ImportOptions defines parameters for the import process.
type ImportOptions struct {
	TargetChainParams       chaincfg.Params
	TargetBlockHeaderStore  headerfs.BlockHeaderStore
	TargetFilterHeaderStore headerfs.FilterHeaderStore

	// Import source identifiers.
	BlockHeadersSource  string
	FilterHeadersSource string

	// Processing parameters.
	WriteBatchSize int
	Logger         btclog.Logger
}

// ImportResult contains statistics about a header import operation.
type ImportResult struct {
	// Counts of processed headers.
	ProcessedCount int // Total number of headers examined
	AddedCount     int // Number of headers newly added to destination
	SkippedCount   int // Number of headers already in destination

	// Actual height range processed.
	StartHeight uint32 // First height processed
	EndHeight   uint32 // Last height processed

	// Duration information.
	StartTime time.Time     // When import started
	EndTime   time.Time     // When import completed
	Duration  time.Duration // Total time taken
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

// formatDuration converts a duration to a human-readable string format
// with appropriate units (ns, μs, ms, s, m, h) based on the duration length.
func formatDuration(d time.Duration) string {
	// For very short operations (under 1ms), use microseconds.
	if d < time.Millisecond {
		if d < time.Microsecond {
			return fmt.Sprintf("%d ns", d.Nanoseconds())
		}
		return fmt.Sprintf("%.2f μs",
			float64(d.Nanoseconds())/float64(time.Microsecond))
	}

	// For operations less than a second, use milliseconds.
	if d < time.Second {
		return fmt.Sprintf("%.2f ms",
			float64(d.Nanoseconds())/float64(time.Millisecond))
	}

	// For operations less than a minute, use seconds.
	if d < time.Minute {
		return fmt.Sprintf("%.2f s",
			d.Seconds())
	}

	// For operations less than an hour, use minutes and seconds.
	if d < time.Hour {
		minutes := int(d.Minutes())
		seconds := int(d.Seconds()) % 60
		return fmt.Sprintf("%dm %ds", minutes, seconds)
	}

	// For longer operations, use hours, minutes and seconds.
	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60
	seconds := int(d.Seconds()) % 60
	return fmt.Sprintf("%dh %dm %ds", hours, minutes, seconds)
}
