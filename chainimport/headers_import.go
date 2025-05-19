package chainimport

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"time"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btclog"
	"github.com/lightninglabs/neutrino/headerfs"
	"golang.org/x/exp/mmap"
	"golang.org/x/sync/errgroup"
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

// OverlapMode defines how to handle headers that overlap between the import
// source and existing headers in the stores.
type OverlapMode uint8

const (
	// AppendOnly adds headers only beyond what already exists in each store.
	// If import source contains headers that overlap with existing data,
	// it will skip those and only append the new ones. This is the default
	// as it preserves existing data and minimizes the risk of unintended
	// chain reorganizations.
	AppendOnly OverlapMode = iota

	// ValidateAndAppend validates that any overlapping headers match before
	// appending new ones. If mismatches are found during validation,
	// the import operation is aborted.
	ValidateAndAppend

	// ValidateAndOverwrite validates overlapping headers, overwrites any
	// mismatches, and appends new headers beyond what exists.
	ValidateAndOverwrite

	// ForceOverwrite overwrites any overlapping headers without validation
	// and appends new headers.
	ForceOverwrite
)

// DivergenceMode defines how to handle cases where one store
// (block headers or filter headers) has headers beyond the effective tip height
// (the minimum tip height between both stores). This can happen due to
// interrupted syncs or other operational scenarios.
type DivergenceMode int

const (
	// ForceReconcile will overwrite divergent headers without validation.
	// This is the default mode, which reconciles divergent stores by
	// replacing headers from the authoritative import source. This approach
	// improves backward observability by ensuring both target stores (block
	// headers and filter headers) maintain perfect consistency with each other
	// and with the authoritative chain history, facilitating reliable chain
	// traversal and verification operations. This is the default.
	ForceReconcile DivergenceMode = iota

	// ValidateLeadAndSyncLag will validate headers in the leading store
	// against the import source. If they match, it preserves them and
	// synchronizes the lagging store to match. If validation fails,
	// the import operation aborts.
	ValidateLeadAndSyncLag

	// ValidateAndReconcile will validate existing headers and reconcile
	// (overwrite) if different.
	ValidateAndReconcile

	// AbortIfDiverged will stop import if stores have diverged.
	AbortIfDiverged
)

// BeyondImportRangeMode defines how to handle headers in stores that
// extend beyond the range covered by the import source.
type BeyondImportRangeMode uint8

const (
	// PreserveBeyond keeps any headers that exist beyond the
	// import source range. This preserves any existing divergence
	// between stores (if they already have different tip heights)
	// but retains all existing data. This is the default.
	PreserveBeyond BeyondImportRangeMode = iota

	// TruncateBeyond removes any headers beyond the import source
	// range, ensuring both stores end at the same height.
	TruncateBeyond
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

// String returns a human-readable representation of the overlap mode.
func (m OverlapMode) String() string {
	switch m {
	case AppendOnly:
		return "AppendOnly"
	case ValidateAndAppend:
		return "ValidateAndAppend"
	case ValidateAndOverwrite:
		return "ValidateAndOverwrite"
	case ForceOverwrite:
		return "ForceOverwrite"
	default:
		return fmt.Sprintf("OverlapMode(%d)", m)
	}
}

// String returns a human-readable representation of the divergence mode.
func (m DivergenceMode) String() string {
	switch m {
	case ForceReconcile:
		return "ForceReconcile"
	case ValidateAndReconcile:
		return "ValidateAndReconcile"
	case ValidateLeadAndSyncLag:
		return "ValidateLeadAndSyncLag"
	case AbortIfDiverged:
		return "AbortIfDiverged"
	default:
		return fmt.Sprintf("DivergenceMode(%d)", m)
	}
}

// String returns a human-readable representation of the beyond import range
// mode.
func (m BeyondImportRangeMode) String() string {
	switch m {
	case PreserveBeyond:
		return "PreserveBeyond"
	case TruncateBeyond:
		return "TruncateBeyond"
	default:
		return fmt.Sprintf("BeyondImportRangeMode(%d)", m)
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

	// ValidateMetadata ensures the header type is compatible with the provided
	// metadata.
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
		return fmt.Errorf("failed to deserialize wire.BlockHeader: %w", err)
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
	metadata, err := f.GetHeaderMetadata()
	if err != nil {
		return fmt.Errorf("failed to get header metadata: %w", err)
	}

	// Validate that the generic type T matches the header type in the file.
	if err := f.validateTypeMatch(metadata); err != nil {
		return fmt.Errorf("type validation failed: %w", err)
	}
	f.metadata = metadata
	f.metadata.EndHeight = metadata.StartHeight + uint32(f.HeadersCount()) - 1

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
		return &HeaderMetadata{}, fmt.Errorf("failed to read metadata: %w", err)
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

	return &ImportSourceHeaderIterator[T]{source: f, current: start, end: end}
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

// HTTPHeaderImportSource implements HeaderImportSource for headers retrieved
// over HTTP.
type HTTPHeaderImportSource[T any] struct {
	URL string
}

// Close releases any resources used by the HTTP header source.
func (h *HTTPHeaderImportSource[T]) Close() error {
	return nil
}

// GetHeader retrieves a header at the specified index from the HTTP source.
func (h *HTTPHeaderImportSource[T]) GetHeader(index int) (T, error) {
	var empty T
	return empty, nil
}

// GetHeaderMetadata retrieves metadata about the headers from the HTTP source.
func (f *HTTPHeaderImportSource[T]) GetHeaderMetadata() (*HeaderMetadata, error) {
	return nil, nil
}

// HeadersCount returns the number of headers available from the HTTP source.
func (f *HTTPHeaderImportSource[T]) HeadersCount() int {
	return 0
}

// Iterator returns an iterator for headers within the specified range.
func (f *HTTPHeaderImportSource[T]) Iterator(start,
	end int) HeaderIterator[T] {

	return nil
}

// Open initializes the HTTP connection to the header source.
func (f *HTTPHeaderImportSource[T]) Open() error {
	return nil
}

// SourceName returns a string representation of the HTTP source.
func (h *HTTPHeaderImportSource[T]) SourceName() string {
	return fmt.Sprintf("http(%s)", h.URL)
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
		return fmt.Errorf("failed to get first block header for validation: "+
			"%w", err)
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
			return fmt.Errorf("failed to get next block header at position "+
				"%d: %w", count, err)
		}

		if !hasMore {
			break
		}

		// Validate current header against previous header.
		err := v.ValidatePair(prevHeader, header, targetChainParams)
		if err != nil {
			return fmt.Errorf("block header validation failed at position "+
				"%d: %w", count, err)
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
		return fmt.Errorf("height mismatch: previous height=%d, current "+
			"height=%d", prevHeight, currHeight)
	}

	// 1. Verify hash chain - current header's PrevBlock must match previous
	// header's hash
	prevHash := prevBlockHeader.BlockHash()
	if !currBlockHeader.PrevBlock.IsEqual(&prevHash) {
		return fmt.Errorf("header chain broken: current header's PrevBlock "+
			"(%v) doesn't match previous header's hash (%v)",
			currBlockHeader.PrevBlock, prevHash)
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
//	FilterHeader_N = double-SHA256(FilterHeader_N-1 || double-SHA256(Filter_N))
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
// headers from the configured import sources into the target header stores.
// The process executes in four sequential phases:
//
//  1. Overlap Region: Handles headers that exist in both import sources and
//     target stores (from importStartHeight to effectiveTipHeight). Options
//     determine whether to preserve target headers, validate them, or overwrite
//     them.
//
//  2. Divergence Region: Reconciles heights where block and filter header
//     stores have diverged but remain within import source range
//     from effectiveTipHeight+1 to the lower of highest existing header or
//     import end height.
//
//  3. Beyond Import Region: Addresses headers in target stores that extend
//     beyond the import source range from importEndHeight+1 to highest existing
//     header. Can either preserve these headers or truncate them.
//
//  4. New Headers Region: Adds headers from import sources that don't exist in
//     target stores (from highest existing header to import source end height).
//
// Each phase handles a disjoint height range with specific validation and
// transformation logic based on the configured import modes. The process
// ensures header consistency throughout the chain while maintaining
// cryptographic validation and chain continuity.
//
// The method returns detailed import statistics or an error if any phase fails.
// Cancellation is supported through the provided context.
func (s *HeadersImport) Import(ctx context.Context,
	options ImportOptions) (*ImportResult, error) {

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

	// Determine processing regions that partition the import task into disjoint
	// height ranges: overlap (shared between source and targets), divergence
	// (where target stores differ), beyond import (outside source range), and
	// new headers (in source but not targets). Each region requires distinct
	// transformation based on configured import modes.
	regions := s.determineProcessingRegions(options)
	result.StartHeight = regions.ImportStartHeight
	result.EndHeight = regions.ImportEndHeight

	// Process overlap region first.
	// This region contains headers that exist in both the import source and
	// target stores. It covers heights from importStartHeight up to the
	// effective tip height of the target stores. We must decide whether to
	// preserve the headers in the target stores, validate them against the
	// import source, or overwrite them with headers from the import source.
	// This foundational region must be processed first as it affects the lowest
	// heights.
	err := s.processOverlapRegion(ctx, regions.Overlap, options, result)
	if err != nil {
		return nil, fmt.Errorf("overlap processing failed: %w", err)
	}

	// Process divergence region second.
	// After handling the common base, we address heights where the target block
	// and filter header stores have diverged from each other, but still within
	// the import source range. This region sits directly above the overlap
	// region, covering heights from effectiveTipHeight+1 up to either the
	// highest existing header in the target stores or the import source end
	// height, whichever is lower.
	err = s.processDivergenceRegion(ctx, regions.Divergence, options, result)
	if err != nil {
		return nil, fmt.Errorf("divergence processing failed: %w", err)
	}

	// Process beyond import region third.
	// Next, we must address any headers in the target stores that extend beyond
	// the import source's end height. This region starts at importEndHeight+1
	// and continues to the highest existing header in either target store. We
	// either preserve these headers (maintaining any existing divergence) or
	// truncate them (ensuring alignment with the import source's range) before
	// adding new headers.
	err = s.processBeyondImportRegion(regions.BeyondImport, options)
	if err != nil {
		return nil, fmt.Errorf("headers import failed: beyond import "+
			"processing failed: %w", err)
	}

	// Process new headers region last.
	// Finally, we add headers from the import source that don't yet exist in
	// either target store. This region covers heights from the highest existing
	// header in the target stores up to the import source's end height. With
	// all existing headers in the target stores now properly reconciled, we can
	// safely extend both stores with consistent new data from the import
	// source.
	err = s.processNewHeadersRegion(regions.NewHeaders, options, result)
	if err != nil {
		return nil, fmt.Errorf("headers import failed: new headers "+
			"processing failed: %w", err)
	}

	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime)

	log.Infof("Headers import completed: processed %d headers "+
		"(block and filter) (added: %d, skipped: %d) from height %d to %d in "+
		"%s (%.2f headers/sec, %.2f%% new)", result.ProcessedCount,
		result.AddedCount, result.SkippedCount, result.StartHeight,
		result.EndHeight, formatDuration(result.Duration),
		result.HeadersPerSecond(), result.NewHeadersPercentage())

	return result, nil
}

func (s *HeadersImport) initSources() error {
	// Check if required import sources are provided.
	if s.BlockHeadersImportSource == nil || s.FilterHeadersImportSource == nil {
		return fmt.Errorf("missing required header sources - block headers "+
			"source: %v, filter headers "+
			"source: %v", s.BlockHeadersImportSource != nil,
			s.FilterHeadersImportSource != nil)
	}

	// Check if required validators are provided.
	if s.BlockHeadersValidator == nil || s.FilterHeadersValidator == nil {
		return fmt.Errorf("missing required header validators - block headers "+
			"validator: %v, filter headers "+
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
			blockMetadata.BitcoinChainType, filterHeadersSource.SourceName(),
			filterMetadata.BitcoinChainType)
	}

	// Validate that the source network matches the target network.
	targetChainType, err := BitcoinNetToChainType(targetBitcoinNet)
	if err != nil {
		return fmt.Errorf("invalid target bitcoin network: %w", err)
	}
	if blockMetadata.BitcoinChainType != targetChainType {
		return fmt.Errorf("network mismatch: headers from import sources are "+
			"for %v, but target is %v", blockMetadata.BitcoinChainType,
			targetChainType)
	}

	// Validate starting heights match.
	if blockMetadata.StartHeight != filterMetadata.StartHeight {
		return fmt.Errorf("start height mismatch: block headers start at %d, "+
			"filter headers start at %d", blockMetadata.StartHeight,
			filterMetadata.StartHeight)
	}

	// Validate header counts match exactly.
	var (
		blockCount  = blockHeadersSource.HeadersCount()
		filterCount = filterHeadersSource.HeadersCount()
	)
	if filterCount != blockCount {
		return fmt.Errorf("headers count mismatch: block headers import "+
			"source %s (%d), filter headers import source %s (%d)",
			blockHeadersSource.SourceName(), blockCount,
			filterHeadersSource.SourceName(), filterCount)
	}

	// Verify header counts are greater than zero.
	if blockCount == 0 {
		return errors.New("no headers available in sources")
	}

	return nil
}

// validateChainContinuity ensures that headers from import sources can be
// properly connected to the existing headers in the target stores. It performs
// the following validations:
//
//  1. Verifies there are no gaps between existing headers and imported headers
//  2. For genesis-only chains (height 0), verifies the import starts at
//     height 0 or 1
//  3. For overlapping regions, validates compatibility by checking headers at
//     the start, middle, and end of the overlap region
//  4. Ensures both block headers and filter headers match at all verification
//     points
//
// This function uses a sampling approach to validate overlapping regions,
// rather than validating every header in the overlap. This trade-off favors
// speed over exhaustive validation, which is important for resource-constrained
// environments. Since import source data is likely coming from trusted sources,
// and the assumption is that if headers match at the boundaries and a sample
// point in the middle, the entire chain is likely to be consistent.
//
// The actual import process will verify additional consistency properties when
// connecting new headers to the chain.
func (s *HeadersImport) validateChainContinuity(
	targetBlockHeaderStore headerfs.BlockHeaderStore,
	targetFilterHeaderStore headerfs.FilterHeaderStore) error {

	// Get metadata from block header source.
	sourceBlockMeta, err := s.BlockHeadersImportSource.GetHeaderMetadata()
	if err != nil {
		return fmt.Errorf("failed to get block header metadata: %w", err)
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
	// Instead of requiring exact equality, take the minimum of the two heights
	// as the effective chain tip to handle cases where one store might
	// be ahead.
	effectiveTipHeight := min(blockTipHeight, filterTipHeight)
	if blockTipHeight != filterTipHeight {
		log.Infof("Target header stores at different heights "+
			"(block=%d, filter=%d), using effective tip height %d",
			blockTipHeight, filterTipHeight, effectiveTipHeight)
	}

	// Extract import height range.
	importStartHeight := sourceBlockMeta.StartHeight
	importEndHeight := sourceBlockMeta.EndHeight

	// If both target stores only contain the genesis block (height 0),
	// we're essentially starting with a fresh chain, so we only need to
	// verify the import data starts at height 0 or 1.
	if effectiveTipHeight == 0 {
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
			log.Infof("Genesis block verified, import data will extend chain " +
				"from genesis")
		} else {
			// Import starts at height 1, which connects to genesis.
			log.Info("Target stores contain only genesis block, import data " +
				"will extend chain from height 1")
		}

		return nil
	}

	// Case 1: Import data starts after the target tip.
	// This is a forward extension, the import start must connect to the tip.
	if importStartHeight > effectiveTipHeight {
		// If import data doesn't start at the next height after tip,
		// there would be a gap in the chain.
		if importStartHeight > effectiveTipHeight+1 {
			return fmt.Errorf("import data starts at height %d but target "+
				"tip is at %d, creating a gap of %d blocks",
				importStartHeight, effectiveTipHeight,
				importStartHeight-effectiveTipHeight-1)
		}

		// For connecting at tip+1, we don't need to verify headers here
		// as the actual import process will validate the connections.
		log.Infof("Import data will extend chain from height %d",
			importStartHeight)
		return nil
	}

	// Case 2: Import data starts before or at the target tip.
	// This means there's overlap, so we need to verify compatibility.

	// Determine the overlap range.
	overlapStart := importStartHeight
	overlapEnd := min(effectiveTipHeight, importEndHeight)

	// Verify headers at start of overlap.
	err = s.verifyHeadersAtHeight(
		overlapStart, targetBlockHeaderStore, targetFilterHeaderStore,
	)
	if err != nil {
		return err
	}

	// If overlap is more than 2 blocks, also verify at the middle.
	if overlapEnd > overlapStart+2 {
		middleHeight := (overlapStart + overlapEnd) / 2
		err = s.verifyHeadersAtHeight(
			middleHeight, targetBlockHeaderStore, targetFilterHeaderStore,
		)
		if err != nil {
			return err
		}
	}

	// If overlap is more than 1 block, also verify at the end
	if overlapEnd > overlapStart {
		err = s.verifyHeadersAtHeight(
			overlapEnd, targetBlockHeaderStore, targetFilterHeaderStore,
		)
		if err != nil {
			return err
		}
	}

	log.Infof("Chain continuity validation successful: import data (%d-%d) "+
		"connects properly with target chain (tip: %d)",
		importStartHeight, importEndHeight, effectiveTipHeight)

	return nil
}

func (s *HeadersImport) verifyHeadersAtHeight(height uint32,
	targetBlockHeaderStore headerfs.BlockHeaderStore,
	targetFilterHeaderStore headerfs.FilterHeaderStore) error {

	// Get metadata from block header source.
	sourceBlockMeta, err := s.BlockHeadersImportSource.GetHeaderMetadata()
	if err != nil {
		return fmt.Errorf("failed to get block header metadata: %w", err)
	}

	// Get and verify block headers.
	sourceIndex := s.heightToSrcIndex(height, sourceBlockMeta.StartHeight)
	sourceBlkHeader, err := s.BlockHeadersImportSource.GetHeader(sourceIndex)
	if err != nil {
		return fmt.Errorf("failed to get block header from import source "+
			"at height %d: %w", height, err)
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
		return fmt.Errorf("failed to get filter header from import source "+
			"at height %d: %w", height, err)
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
		return fmt.Errorf("filter header mismatch at height %d: source "+
			"has %v but target has %v", height,
			sourceFilterHeaderHash, targetFilterHeaderHash)
	}

	log.Debugf("Headers from %s (block) and %s (filter) verified at height %d",
		s.BlockHeadersImportSource.SourceName(),
		s.FilterHeadersImportSource.SourceName(),
		height)

	return nil
}

// HeaderRegion represents a contiguous range of headers.
type HeaderRegion struct {
	Start  uint32
	End    uint32
	Exists bool // Whether this region has headers to process
}

// ProcessingRegions contains all regions that need processing.
type ProcessingRegions struct {
	ImportStartHeight uint32
	ImportEndHeight   uint32
	EffectiveTip      uint32
	Overlap           HeaderRegion
	Divergence        HeaderRegion
	BeyondImport      HeaderRegion
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

	// 1. Overlap region.
	// This region contains headers that exist in both the import source and
	// target stores, from the start of the import range up to the effective tip
	// height. Using the default AppendOnly mode, these headers will be skipped
	// to preserve existing data. These headers need to be processed according
	// to the selected OverlapMode.
	overlapStart := importStartHeight
	overlapEnd := min(effectiveTipHeight, importEndHeight)
	regions.Overlap = HeaderRegion{
		Start:  overlapStart,
		End:    overlapEnd,
		Exists: overlapStart <= overlapEnd,
	}

	// 2. Divergence region.
	// This region contains headers where one store extends beyond the effective
	// tip but still within the import range. It represents heights where block
	// and filter headers are out of sync and need reconciliation. Using the
	// default ForceReconcile mode, these headers will be overwritten from the
	// import source. These headers need to be processed according to the
	// selected DivergenceMode.
	divergeStart := effectiveTipHeight + 1
	divergeEnd := min(max(blockTipHeight, filterTipHeight), importEndHeight)
	regions.Divergence = HeaderRegion{
		Start:  divergeStart,
		End:    divergeEnd,
		Exists: blockTipHeight != filterTipHeight && divergeStart <= divergeEnd,
	}

	// 3. Beyond import range.
	// This region contains headers that exist in target stores but extend
	// beyond the import source range. Using the default PreserveBeyond mode,
	// these headers will be kept intact, preserving any existing divergence
	// between the block and filter header target stores if they have different
	// tip heights.
	beyondStart := importEndHeight + 1
	beyondEnd := max(blockTipHeight, filterTipHeight)
	regions.BeyondImport = HeaderRegion{
		Start:  beyondStart,
		End:    beyondEnd,
		Exists: beyondStart <= beyondEnd,
	}

	// 4. New Headers region.
	// This region contains headers that are in the import source but not yet in
	// either target store. They start one height beyond the highest tip of
	// either store (ensuring no overlap with divergence region) and extend to
	// the end of the import data. These headers need to be added to both
	// stores. It only exists if there are headers beyond both tips.
	//
	// Note: This region is processed after handling the overlap, divergence,
	// and beyond import regions, ensuring that any potential inconsistencies
	// in existing data are resolved before adding new headers. This sequential
	// processing guarantees that new headers are only added on top of a
	// verified and consistent chain state.
	newStart := max(blockTipHeight, filterTipHeight) + 1
	newEnd := importEndHeight
	regions.NewHeaders = HeaderRegion{
		Start:  newStart,
		End:    newEnd,
		Exists: newStart <= newEnd,
	}

	return regions
}

func (s *HeadersImport) processOverlapRegion(ctx context.Context,
	region HeaderRegion, options ImportOptions, result *ImportResult) error {

	if !region.Exists {
		return nil
	}

	log.Infof("Processing overlap region from heights %d to %d for block "+
		"and filter headers", region.Start, region.End)

	switch options.OverlapMode {
	case AppendOnly:
		// Skip all headers in overlap region.
		log.Infof("Skipping %d headers (block and filter) in overlap region "+
			"due to %s mode", region.End-region.Start+1, options.OverlapMode)

		result.SkippedCount += int(region.End - region.Start + 1)

	case ValidateAndAppend:
		// Validate all headers in overlap region match. If mismatches are found
		// during validation, the import operation is aborted.
		err := s.validateHeadersExactMatch(
			ctx, region.Start, region.End, options,
		)
		if err != nil {
			return fmt.Errorf("overlap validation failed: %w", err)
		}
		log.Infof("Successfully validated %d headers (block and filter) in "+
			"overlap region", region.End-region.Start+1)

		result.SkippedCount += int(region.End - region.Start + 1)

	case ValidateAndOverwrite:
		// Validate and overwrite any that don't match.
		overwritten, err := s.validateAndOverwriteHeaders(
			region.Start, region.End, options,
		)
		if err != nil {
			return fmt.Errorf("overlap validation and overwrite "+
				"failed: %w", err)
		}
		result.ProcessedCount += int(region.End - region.Start + 1)
		result.AddedCount += overwritten
		result.SkippedCount += int(region.End-region.Start+1) - overwritten

	case ForceOverwrite:
		// Overwrite all headers in overlap region without validation.
		err := s.overwriteHeaders(region.Start, region.End, options)
		if err != nil {
			return fmt.Errorf("overlap overwrite failed: %w", err)
		}
		result.ProcessedCount += int(region.End - region.Start + 1)
		result.AddedCount += int(region.End - region.Start + 1)
	}

	return nil
}

func (s *HeadersImport) processDivergenceRegion(ctx context.Context,
	region HeaderRegion, options ImportOptions, result *ImportResult) error {

	if !region.Exists {
		return nil
	}

	log.Infof("Processing divergence region from heights %d to %d for block "+
		"and filter headers", region.Start, region.End)

	switch options.DivergenceMode {
	case ForceReconcile:
		// Force reconcile all headers in divergence region by directly
		// overwriting both target stores with headers from the authoritative
		// import source, without performing any validation of existing headers.
		// This ensures both target stores are synchronized at the same height
		// with identical data from the import source. This also would help
		err := s.reconcileHeaders(region.Start, region.End, options)
		if err != nil {
			return fmt.Errorf("forced divergent headers reconciliation "+
				"failed: %w", err)
		}
		result.ProcessedCount += int(region.End - region.Start + 1)
		result.AddedCount += int(region.End - region.Start + 1)

	case ValidateLeadAndSyncLag:
		// First, validate that the headers in the leading store match the
		// import source. If they match, preserve those headers in the leading
		// store and synchronize the lagging store by copying headers from the
		// import source to fill any gaps. This ensures both stores end up at
		// the same height without modifying valid headers. If any validation
		// mismatch is found, the entire import operation is aborted.
		err := s.validateLeadAndSyncLagTargetStore(
			ctx, region.Start, region.End, options,
		)
		if err != nil {
			return fmt.Errorf("divergent headers validation failed: %w", err)
		}
		result.SkippedCount += int(region.End - region.Start + 1)

	case ValidateAndReconcile:
		// Compare existing headers in both stores against the import source.
		// Headers that match the import source are preserved, while any that
		// differ are overwritten with data from the import source. This ensures
		// both stores are synchronized with accurate data while minimizing
		// unnecessary writes. The function returns the count of headers that
		// needed reconciliation.
		reconciled, err := s.validateAndReconcileHeaders(
			region.Start, region.End, options,
		)
		if err != nil {
			return fmt.Errorf("divergent headers reconciliation "+
				"failed: %w", err)
		}
		result.ProcessedCount += int(region.End - region.Start + 1)
		result.AddedCount += reconciled
		result.SkippedCount += int(region.End-region.Start+1) - reconciled

	case AbortIfDiverged:
		// Immediately abort the import operation upon detecting divergent
		// stores. This strict safety mode prevents any automatic
		// reconciliation. It's useful when divergence might indicate corruption
		// or when exact header state verification is required before
		// proceeding. The error includes the exact height range of the
		// divergence to aid in investigation and manual resolution.
		return fmt.Errorf("aborting import: stores have diverged between "+
			"heights %d and %d", region.Start, region.End)
	}
	return nil
}

func (s *HeadersImport) processBeyondImportRegion(region HeaderRegion,
	options ImportOptions) error {

	if !region.Exists {
		return nil
	}

	log.Infof("Processing %d headers (block and filter) beyond import range "+
		"(heights %d-%d)", region.End-region.Start+1, region.Start, region.End)

	switch options.BeyondImportRangeMode {
	case PreserveBeyond:
		// Nothing to do, just log.
		log.Infof("Preserving headers (block and filter) beyond import range "+
			"due to %s mode", options.BeyondImportRangeMode)

	case TruncateBeyond:
		// Truncate headers beyond import range.
		if err := s.truncateHeaders(region.Start, options); err != nil {
			return fmt.Errorf("failed to truncate headers beyond import "+
				"range: %w", err)
		}
		log.Infof("Truncated all headers (block and filter) beyond height %d",
			region.Start-1)
	}

	return nil
}

func (s *HeadersImport) processNewHeadersRegion(region HeaderRegion,
	options ImportOptions, result *ImportResult) error {

	if !region.Exists {
		return nil
	}

	log.Infof("Adding %d new headers (block and filter) from heights %d to %d",
		region.End-region.Start+1, region.Start, region.End)

	err := s.appendNewHeaders(region.Start, region.End, options)
	if err != nil {
		return fmt.Errorf("failed to append new headers: %w", err)
	}

	result.ProcessedCount += int(region.End - region.Start + 1)
	result.AddedCount += int(region.End - region.Start + 1)

	return nil
}

// validateHeadersExactMatch performs exact validation of all headers in the
// specified range, ensuring that headers from the import source match exactly
// with their counterparts in the target store. This is used by
// ValidateAndAppend mode to verify consistency of overlapping headers before
// proceeding with the import operation.
//
// The validation is performed concurrently for improved performance since this
// is primarily an I/O bound operation. Each worker safely validates headers at
// different heights without interfering with other workers. The function adapts
// to available system resources and uses a fail-fast approach, stopping all
// validation work as soon as any mismatch is detected.
func (s *HeadersImport) validateHeadersExactMatch(ctx context.Context,
	startHeight, endHeight uint32, options ImportOptions) error {

	// Calculate the range size
	rangeSize := endHeight - startHeight + 1

	// If range is small, process sequentially
	if rangeSize <= 100 {
		return s.validateHeadersSequential(startHeight, endHeight, options)
	}

	// Create an errgroup with a derived context.
	g, ctx := errgroup.WithContext(ctx)

	// Set concurrency limit based on CPU cores, but cap reasonably.
	g.SetLimit(min(runtime.NumCPU(), 8))

	// Queue up all the heights to validate
	for height := startHeight; height <= endHeight; height++ {
		// Add work to the errgroup.
		g.Go(func() error {
			// Check if context has been canceled before starting work.
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			// Verify headers at this height
			err := s.verifyHeadersAtHeight(
				height, options.TargetBlockHeaderStore,
				options.TargetFilterHeaderStore,
			)

			if err != nil {
				return fmt.Errorf("header verification failed at height %d: %w",
					height, err)
			}

			// Occasional progress logging.
			if height%1000 == 0 {
				log.Debugf("Parallel validation in progress: height %d", height)
			}

			return nil
		})
	}

	// Wait for all verification goroutines to complete or for any error
	err := g.Wait()
	if err != nil {
		return err
	}

	log.Debugf("Parallel validation of headers (block and filter) completed "+
		"for heights %d to %d", startHeight, endHeight)
	return nil
}

// validateHeadersSequential performs sequential validation for smaller ranges.
func (s *HeadersImport) validateHeadersSequential(startHeight, endHeight uint32,
	options ImportOptions) error {

	for height := startHeight; height <= endHeight; height++ {
		err := s.verifyHeadersAtHeight(
			height, options.TargetBlockHeaderStore,
			options.TargetFilterHeaderStore,
		)
		if err != nil {
			return fmt.Errorf("header verification failed at height %d: %w",
				height, err)
		}
	}

	return nil
}

// validateAndOverwriteHeaders validates headers and overwrites mismatches.
func (s *HeadersImport) validateAndOverwriteHeaders(start, end uint32,
	options ImportOptions) (int, error) {

	return 0, nil
}

// overwriteHeaders unconditionally overwrites all headers in range.
func (s *HeadersImport) overwriteHeaders(start, end uint32,
	options ImportOptions) error {
	// Implementation that overwrites all headers.
	return nil
}

// validateLeadAndSyncLagTargetStore performs a two-step process on the
// divergent region:
//  1. It validates that headers in the leading target store match those from
//     the import source
//  2. It synchronizes the lagging target store by copying missing headers from
//     the import source
//
// This ensures both stores end up synchronized at the same height while
// preserving valid headers in the leading store. If any validation mismatch is
// found, the function returns an error that is supoosed to fail the import
// process entirely.
func (s *HeadersImport) validateLeadAndSyncLagTargetStore(ctx context.Context,
	startHeight, endHeight uint32, options ImportOptions) error {

	return nil
}

// validateAndReconcileHeaders validates and reconciles divergent headers.
func (s *HeadersImport) validateAndReconcileHeaders(start, end uint32,
	options ImportOptions) (int, error) {

	// Implementation that validates and conditionally reconciles.
	return 0, nil
}

// reconcileHeaders unconditionally reconciles all headers in range.
func (s *HeadersImport) reconcileHeaders(start, end uint32,
	options ImportOptions) error {
	// Implementation that reconciles all headers.
	return nil
}

// truncateHeaders removes headers beyond specified height.
func (s *HeadersImport) truncateHeaders(start uint32,
	options ImportOptions) error {
	// Implementation that truncates headers.
	return nil
}

// appendNewHeaders adds new headers from import source.
func (s *HeadersImport) appendNewHeaders(startHeight, endHeight uint32,
	options ImportOptions) error {

	// Get metadata from sources.
	blockMeta, err := s.BlockHeadersImportSource.GetHeaderMetadata()
	if err != nil {
		return fmt.Errorf("failed to get block header metadata: %w", err)
	}

	totalHeaders := endHeight - startHeight + 1
	log.Infof("Appending %d new headers in batches of %d", totalHeaders,
		options.WriteBatchSizePerRegion)

	// Process headers in batches.
	for batchStart := startHeight; batchStart <= endHeight; {
		// Calculate batch end, ensuring we don't exceed endHeight.
		batchEnd := min(
			batchStart+uint32(options.WriteBatchSizePerRegion)-1, endHeight,
		)

		// Calculate indices in the source based on the metadata start height.
		sourceStartIdx := s.heightToSrcIndex(batchStart, blockMeta.StartHeight)
		sourceEndIdx := s.heightToSrcIndex(batchEnd, blockMeta.StartHeight)

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
				return fmt.Errorf("failed to read block header at height "+
					"%d: %w", batchStart+uint32(len(blockHeaders)), err)
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
				return fmt.Errorf("failed to read filter header at height "+
					"%d: %w", batchStart+uint32(len(filterHeaders)), err)
			}

			if !hasMore {
				break
			}

			filterHeaders = append(filterHeaders, *header.FilterHeader)
		}

		if len(blockHeaders) == 0 {
			return fmt.Errorf("no headers read for batch %d-%d", batchStart,
				batchEnd)
		}

		// TODO(mohamedawnallah): Writing Headers to both target block and
		// filters header store need to be done atomically. On any failure we
		// need to revert to previous valid state and abort the import process.

		// Write block headers batch to target store.
		err = options.TargetBlockHeaderStore.WriteHeaders(blockHeaders...)
		if err != nil {
			return fmt.Errorf("failed to write block headers batch %d-%d: %w",
				batchStart, batchEnd, err)
		}

		// We only need to set the block header hash of the last filter
		// header to maintain chain tip consistency for regular tip.
		lastIdx := len(filterHeaders) - 1
		chainTipHash := blockHeaders[lastIdx].BlockHeader.BlockHash()
		filterHeaders[lastIdx].HeaderHash = chainTipHash

		// Write filter headers batch to target store.
		err = options.TargetFilterHeaderStore.WriteHeaders(filterHeaders...)
		if err != nil {
			return fmt.Errorf("failed to write filter headers batch %d-%d: %w",
				batchStart, batchEnd, err)
		}

		log.Debugf("Wrote headers batch from height %d to %d (%d headers)",
			batchStart, batchEnd, batchEnd-batchStart+1)

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
	if options.WriteBatchSizePerRegion <= 0 {
		options.WriteBatchSizePerRegion = 10000
		log.Infof("Using default write batch size of %d "+
			"headers per region", options.WriteBatchSizePerRegion)
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
		return nil, fmt.Errorf("failed to create filter headers import "+
			"source: %w", err)
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
	// Check if source is an HTTP or HTTPS URL.
	if strings.HasPrefix(source, "http://") ||
		strings.HasPrefix(source, "https://") {

		return &HTTPHeaderImportSource[T]{
			URL: source,
		}, nil
	}

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

	// Import behavior modes.

	// OverlapMode defines how to handle headers that overlap between
	// the import source and existing data in the stores. Defaults to
	// AppendOnly.
	OverlapMode OverlapMode

	// DivergenceMode defines how to handle cases where one store has headers
	// beyond the effective tip height. Defaults to ForceReconcile.
	DivergenceMode DivergenceMode

	// BeyondImportRangeMode defines how to handle headers in stores that
	// extend beyond the range covered by the import source.
	// Defaults to PreserveBeyondImport.
	BeyondImportRangeMode BeyondImportRangeMode

	// Processing parameters.
	WriteBatchSizePerRegion int
	Logger                  btclog.Logger

	// Could add source-specific options later:
	// HTTPOptions HTTPSourceOptions
	// FileOptions FileSourceOptions
	// etc.
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
func formatDuration(duration time.Duration) string {
	// For very short operations (under 1ms), use microseconds.
	if duration < time.Millisecond {
		if duration < time.Microsecond {
			return fmt.Sprintf("%d ns", duration.Nanoseconds())
		}
		return fmt.Sprintf("%.2f μs",
			float64(duration.Nanoseconds())/float64(time.Microsecond))
	}

	// For operations less than a second, use milliseconds.
	if duration < time.Second {
		return fmt.Sprintf("%.2f ms",
			float64(duration.Nanoseconds())/float64(time.Millisecond))
	}

	// For operations less than a minute, use seconds.
	if duration < time.Minute {
		return fmt.Sprintf("%.2f s",
			duration.Seconds())
	}

	// For operations less than an hour, use minutes and seconds.
	if duration < time.Hour {
		minutes := int(duration.Minutes())
		seconds := int(duration.Seconds()) % 60
		return fmt.Sprintf("%dm %ds", minutes, seconds)
	}

	// For longer operations, use hours, minutes and seconds.
	hours := int(duration.Hours())
	minutes := int(duration.Minutes()) % 60
	seconds := int(duration.Seconds()) % 60
	return fmt.Sprintf("%dh %dm %ds", hours, minutes, seconds)
}
