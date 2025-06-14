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

// Header defines the common interface for both block and filter
// headers that can be processed during chain import operations.
type Header interface {
	// Deserialize reconstructs the header from binary data at the specified
	// height.
	Deserialize(io.Reader, uint32) error

	// ValidateMetadata ensures the header type is compatible with the
	// provided metadata.
	ValidateMetadata(*HeaderMetadata) error
}

// BlockHeader represents a block header that can be imported into
// the chain store. It wraps a headerfs.BlockHeader with additional
// functionality needed for the import process.
type BlockHeader struct {
	headerfs.BlockHeader
}

// Compile-time assertion to ensure BlockHeader implements Header interface.
var _ Header = (*BlockHeader)(nil)

// Deserialize reconstructs a block header from binary data at the specified
// height.
func (b *BlockHeader) Deserialize(r io.Reader, height uint32) error {
	// Deserialize the wire.BlockHeader portion.
	if err := b.BlockHeader.BlockHeader.Deserialize(r); err != nil {
		return fmt.Errorf("failed to deserialize "+
			"wire.BlockHeader: %w", err)
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

// FilterHeader represents a filter header that can be imported into
// the chain store. It wraps a headerfs.FilterHeader with additional
// functionality needed for the import process.
type FilterHeader struct {
	headerfs.FilterHeader
}

// Compile-time assertion to ensure FilterHeader implements Header interface.
var _ Header = (*FilterHeader)(nil)

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

// HeaderIterator interface for iterating over headers.
type HeaderIterator[T any] interface {
	Next() (header T, hasMore bool, err error)
	Close() error
}

// ImportSourceHeaderIterator provides efficient iteration over headers from
// a source.
type ImportSourceHeaderIterator[T any] struct {
	source  HeaderImportSource[T]
	current int
	end     int
}

// Compile-time assertion to ensure ImportSourceHeaderIterator implements
// HeaderIterator interface.
var _ HeaderIterator[any] = (*ImportSourceHeaderIterator[any])(nil)

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

// HeaderValidator is a generic interface for validating headers of type T.
type HeadersValidator[T any] interface {
	Validate(HeaderIterator[T], chaincfg.Params) error
	ValidatePair(prev, current T, targetChainParams chaincfg.Params) error
}

// BlockHeadersImportSourceValidator implements HeadersValidator for block
// headers.
type BlockHeadersImportSourceValidator struct{}

// Compile-time assertion to ensure BlockHeadersImportSourceValidator implements
// HeadersValidator interface.
var _ HeadersValidator[*BlockHeader] = (*BlockHeadersImportSourceValidator)(nil)

func (v *BlockHeadersImportSourceValidator) Validate(
	iterator HeaderIterator[*BlockHeader],
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
var _ HeadersValidator[*FilterHeader] = (*FilterHeadersImportSourceValidator)(nil)

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
	iterator HeaderIterator[*FilterHeader],
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
	ImportStartHeight uint32
	ImportEndHeight   uint32
	EffectiveTip      uint32
	NewHeaders        HeaderRegion
}

// HeaderRegion represents a contiguous range of headers.
type HeaderRegion struct {
	Start  uint32
	End    uint32
	Exists bool // Whether this region has headers to process
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

// Compile-time assertion to ensure FileHeaderImportSource implements
// HeaderImportSource interface.
var _ HeaderImportSource[Header] = (*FileHeaderImportSource[Header])(nil)

// FileHeaderSource implements HeaderImportSource for files.
type FileHeaderImportSource[T Header] struct {
	Path     string
	reader   *mmap.ReaderAt
	fileSize int
	metadata *HeaderMetadata
}

// Open opens the file and initializes the mmap reader.
func (f *FileHeaderImportSource[T]) Open() error {
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

func (f *FileHeaderImportSource[T]) Close() error {
	return f.reader.Close()
}

// GetHeaderMetadata reads the metadata from the file.
//
// nolint:lll
func (f *FileHeaderImportSource[T]) GetHeaderMetadata() (*HeaderMetadata, error) {
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
	var header T
	if err := header.Deserialize(reader, height); err != nil {
		return empty, err
	}

	return header, err
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

// validateTypeMatch ensures that the generic type T matches the header type in
// the file.
func (f *FileHeaderImportSource[T]) validateTypeMatch(
	metadata *HeaderMetadata) error {

	var header T
	return header.ValidateMetadata(metadata)
}

// HeadersImport uses a generic HeaderImportSource to import headers.
type HeadersImport struct {
	BlockHeadersImportSource  HeaderImportSource[*BlockHeader]
	FilterHeadersImportSource HeaderImportSource[*FilterHeader]
	BlockHeadersValidator     HeadersValidator[*BlockHeader]
	FilterHeadersValidator    HeadersValidator[*FilterHeader]
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
			"%s (%v)", s.BlockHeadersImportSource.SourceName(),
			blockMetadata.BitcoinChainType,
			s.FilterHeadersImportSource.SourceName(),
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
			"%s (%d)", s.BlockHeadersImportSource.SourceName(),
			blockCount, s.FilterHeadersImportSource.SourceName(),
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
		err := s.verifyHeadersAtHeight(
			importStartHeight, s.options.TargetBlockHeaderStore,
			s.options.TargetFilterHeaderStore,
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
	sourceIndex := heightToSrcIndex(height, sourceBlockMeta.StartHeight)
	sourceBlkHeader, err := s.BlockHeadersImportSource.GetHeader(
		sourceIndex,
	)
	if err != nil {
		return fmt.Errorf("failed to get block header from import "+
			"source at height %d: %w", height, err)
	}

	targetBlkHeader, err := targetBlockHeaderStore.FetchHeaderByHeight(
		height,
	)
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

// Write block and filter headers to the target stores in a
// specific order to ensure proper rollback operations
// if needed.
//
// The current implementation writes headers sequentially:
// 1. Block headers are written to the binary headers file
// 2. Then they are indexed in the headers db for random access
//
// Failures are handled atomically in both the binary file
// (via appendRaw) and the target DB. This means we only need
// to roll back block headers in the target binary file if block
// headers are successfully written but filter headers fail to
// write.
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
		// If we've reached here we know the headers failed
		// to be written to target filter store because of I/O
		// error regards binary file or filter store and it is
		// automatically rolledback upstream.
		//
		// Given the whole import operation need to satisfy
		// the conjunction property for both block and filter
		// header stores all or nothing so they need to be at the same
		// length.
		blockHeadersToTruncate := uint32(len(blockHeaders))
		_, err := s.options.TargetBlockHeaderStore.RollbackBlockHeaders(
			blockHeadersToTruncate,
		)
		if err != nil {
			return fmt.Errorf("failed to rollback %d headers "+
				"from target block header store to match "+
				"with the target filter header store",
				blockHeadersToTruncate)
		}

		// We need to roll back to previous state.
		return fmt.Errorf("failed to write filter headers "+
			"batch %d-%d: %w", batchStart, batchEnd, err)
	}

	return nil
}

// ImportOptions defines parameters for the import process.
type ImportOptions struct {
	// Target chain configuration and header stores.
	// These define the blockchain parameters and storage backends
	// where imported headers will be written.
	TargetChainParams       chaincfg.Params
	TargetBlockHeaderStore  headerfs.BlockHeaderStore
	TargetFilterHeaderStore headerfs.FilterHeaderStore

	// Import source identifiers.
	// These specify the file paths or locations from which
	// block and filter headers will be imported.
	BlockHeadersSource  string
	FilterHeadersSource string

	// Processing parameters.
	// These control how the import process executes, including
	// performance-related settings.
	WriteBatchSizePerRegion int
}

// Import executes the header import process with the configuration specified in
// the options. It handles the validation, construction of necessary components,
// and orchestration of the import.
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
func (options *ImportOptions) createBlockHeadersImportSource() HeaderImportSource[*BlockHeader] {
	return &FileHeaderImportSource[*BlockHeader]{
		Path: options.BlockHeadersSource,
	}
}

// createFilterHeadersImportSource creates the appropriate import source for
// filter headers.
//
// nolint:lll
func (options *ImportOptions) createFilterHeadersImportSource() HeaderImportSource[*FilterHeader] {
	return &FileHeaderImportSource[*FilterHeader]{
		Path: options.FilterHeadersSource,
	}
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

// heightToSrcIndex converts an absolute blockchain height to a
// source-relative index based on the provided start height.
func heightToSrcIndex(height, startHeight uint32) int {
	return int(height - startHeight)
}
