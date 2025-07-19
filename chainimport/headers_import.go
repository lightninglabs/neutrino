package chainimport

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightninglabs/neutrino/headerfs"
	"golang.org/x/exp/mmap"
)

const (
	// BitcoinChainTypeSize is the Bitcoin Chain Type size: 4 bytes.
	BitcoinChainTypeSize = 4

	// HeaderTypeSize is the Header Type size: 1 byte.
	HeaderTypeSize = 1

	// StartHeaderHeightSize is the Start Header Height size: 4 bytes.
	StartHeaderHeightSize = 4

	// HeaderMetadataSize is the Header Metadata size in bytes
	// (sum of all above): 9 bytes.
	//
	//nolint:lll
	HeaderMetadataSize = BitcoinChainTypeSize + HeaderTypeSize + StartHeaderHeightSize

	// UnknownHeaderSize represents an unknown HeaderSize value.
	UnknownHeaderSize = 0

	// DefaultWriteBatchSizePerRegion defines the default number of
	// headers to process in a single batch when no specific batch
	// size is provided.
	DefaultWriteBatchSizePerRegion = 16384
)

// HeaderMetadata contains the metadata about the header source.
type HeaderMetadata struct {
	BitcoinChainType wire.BitcoinNet
	HeaderType       headerfs.HeaderType
	HeaderSize       int
	StartHeight      uint32
	EndHeight        uint32
	HeadersCount     uint32
}

// Header is the base interface for header operations.
type Header interface {
	// Deserialize reconstructs the header from binary data at the specified
	// height.
	Deserialize(io.Reader, uint32) error
}

// BlockHeader represents a block header that can be imported into
// the chain store. It wraps a headerfs.BlockHeader with additional
// functionality needed for the import process.
type BlockHeader struct {
	headerfs.BlockHeader
}

// Compile-time assertion to ensure BlockHeader implements
// HeaderBase interface.
var _ Header = (*BlockHeader)(nil)

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

// NewBlockHeader creates and returns a new empty BlockHeader instance.
func NewBlockHeader() Header {
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
// HeaderBase interface.
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

// NewFilterHeader creates and returns a new empty FilterHeader instance. It is
// part of HeaderFactory interface.
func NewFilterHeader() Header {
	return &FilterHeader{
		FilterHeader: headerfs.FilterHeader{},
	}
}

// HeaderIterator interface for iterating over headers.
type HeaderIterator interface {
	// Next returns the next header in the sequence and advances the
	// iterator.
	Next() (header Header, hasMore bool, err error)

	// Close releases any resources associated with the iterator.
	Close() error
}

// ImportSourceHeaderIterator provides efficient iteration over headers from
// a source.
type ImportSourceHeaderIterator struct {
	source  HeaderImportSource
	current uint32
	end     uint32
}

// Compile-time assertion to ensure ImportSourceHeaderIterator implements
// HeaderIterator interface.
var _ HeaderIterator = (*ImportSourceHeaderIterator)(nil)

// Next returns the next header in the sequence, along with a boolean indicating
// whether there are more headers available and any error encountered.
func (it *ImportSourceHeaderIterator) Next() (Header, bool, error) {
	var empty Header

	if it.current > it.end {
		return empty, false, nil
	}

	header, err := it.source.GetHeader(it.current)
	if err != nil {
		return empty, false, err
	}

	it.current++

	hasMore := it.current <= it.end

	return header, hasMore, nil
}

// Close releases any resources used by the iterator.
func (it *ImportSourceHeaderIterator) Close() error {
	// Don't close the source, as it may be used elsewhere.
	return nil
}

// HeaderValidator is a generic interface for validating headers of type T.
type HeadersValidator interface {
	// Validate performs comprehensive validation on a sequence of headers
	// provided by the iterator. It checks that the entire sequence forms
	// a valid chain according to the given chain parameters.
	Validate(HeaderIterator, chaincfg.Params) error

	// ValidatePair verifies that two consecutive headers (prev and current)
	// form a valid chain link according to the specified chain parameters.
	ValidatePair(prev, current Header, targetCh chaincfg.Params) error
}

// BlockHeadersImportSourceValidator implements HeadersValidator for block
// headers.
type BlockHeadersImportSourceValidator struct{}

// Compile-time assertion to ensure BlockHeadersImportSourceValidator implements
// HeadersValidator interface.
var _ HeadersValidator = (*BlockHeadersImportSourceValidator)(nil)

// Validate performs thorough validation of a sequence of block headers.
func (v *BlockHeadersImportSourceValidator) Validate(
	iterator HeaderIterator, targetChainParams chaincfg.Params) error {

	var (
		prevHeader Header
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
		var header Header
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
	current Header, targetChainParams chaincfg.Params) error {

	// Type assert prev and current headers to be block headers.
	prevBlk, ok := prev.(*BlockHeader)
	if !ok {
		return fmt.Errorf("expected BlockHeader type, got %T", prev)
	}
	currentBlk, ok := current.(*BlockHeader)
	if !ok {
		return fmt.Errorf("expected BlockHeader type, got %T", current)
	}

	// Extract the block headers and heights directly.
	prevBlockHeader := prevBlk.BlockHeader
	currBlockHeader := currentBlk.BlockHeader
	prevHeight, currHeight := prevBlk.Height, currentBlk.Height

	// 1. Verify that heights are consecutive.
	if currHeight != prevHeight+1 {
		return fmt.Errorf("height mismatch: previous height=%d, "+
			"current height=%d", prevHeight, currHeight)
	}

	// 2. Verify hash chain. Current header's PrevBlock must match previous
	// header's hash.
	prevHash := prevBlockHeader.BlockHash()
	if !currBlockHeader.PrevBlock.IsEqual(&prevHash) {
		return fmt.Errorf("header chain broken: current header's "+
			"PrevBlock (%v) doesn't match previous header's hash "+
			"(%v)", currBlockHeader.PrevBlock, prevHash)
	}

	// 3. Validate block header sanity.
	err := blockchain.CheckBlockHeaderSanity(
		currBlockHeader.BlockHeader, targetChainParams.PowLimit,
		blockchain.NewMedianTime(), blockchain.BFFastAdd,
	)
	if err != nil {
		return err
	}

	return nil
}

// NewBlockHeadersImportSourceValidator creates a new block headers import
// source validator.
func NewBlockHeadersImportSourceValidator() HeadersValidator {
	return &BlockHeadersImportSourceValidator{}
}

// FilterHeadersImportSourceValidator implements HeaderValidator for filter
// headers.
type FilterHeadersImportSourceValidator struct{}

// Compile-time assertion to ensure FilterHeadersImportSourceValidator
// implements HeadersValidator interface.
var _ HeadersValidator = (*FilterHeadersImportSourceValidator)(nil)

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
	iterator HeaderIterator,
	targetChainParams chaincfg.Params) error {

	log.Debug("Skipping filter headers validation - missing access to " +
		"original compact filters")
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
	current Header, targetChainParams chaincfg.Params) error {

	return nil
}

// NewFilterHeadersImportSourceValidator creates a new filter headers import
// source validator.
func NewFilterHeadersImportSourceValidator() HeadersValidator {
	return &FilterHeadersImportSourceValidator{}
}

// ProcessingRegions currently contains regions to process. Those regions
// overlap, divergence, and new headers region are all detected
// but only new headers region processed.
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

	// Overlap contains the region of headers that overlap with the target
	// chain.
	Overlap HeaderRegion

	// Divergence contains the region of headers that diverge from the
	// target chain.
	Divergence HeaderRegion

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
type HeaderImportSource interface {
	// Open initializes the header source and prepares it for reading.
	Open() error

	// Close releases any resources associated with the header source.
	Close() error

	// GetHeaderMetadata retrieves metadata information about the headers
	// collection.
	GetHeaderMetadata() (*HeaderMetadata, error)

	// Iterator returns a HeaderIterator that can traverse headers between
	// start and end indices.
	Iterator(start, end uint32) HeaderIterator

	// GetHeader retrieves a single header at the specified index.
	GetHeader(index uint32) (Header, error)

	// GetURI returns a string identifier for this header source.
	GetURI() string

	// SetURI sets a string identifier for this header source.
	SetURI(uri string)
}

// FileHeaderImportSource implements HeaderImportSource for header files.
type FileHeaderImportSource struct {
	uri           string
	reader        *mmap.ReaderAt
	fileSize      int
	metadata      *HeaderMetadata
	headerFactory func() Header
}

// Compile-time assertion to ensure FileHeaderImportSource implements
// HeaderImportSource interface.
var _ HeaderImportSource = (*FileHeaderImportSource)(nil)

// Open opens the file and initializes the mmap reader.
func (f *FileHeaderImportSource) Open() error {
	reader, err := mmap.Open(f.GetURI())
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
	f.metadata = mData
	f.metadata.EndHeight = mData.StartHeight + mData.HeadersCount - 1

	return nil
}

// Close closes the file and releases the mmap reader.
func (f *FileHeaderImportSource) Close() error {
	return f.reader.Close()
}

// GetHeaderMetadata reads the metadata from the file. The metadata is memoized
// after the first call, with subsequent calls returning the cached result
// without re-reading the file.
func (f *FileHeaderImportSource) GetHeaderMetadata() (*HeaderMetadata, error) {
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

	// Extract the bitcoin chain type.
	bitcoinChainType := wire.BitcoinNet(
		binary.LittleEndian.Uint32(buf[:BitcoinChainTypeSize]),
	)

	// Extract the header type
	headerType := headerfs.HeaderType(buf[headerTypeOffset])

	// Extract start height. First check if the value is negative.
	startHeightRaw := buf[startHeightOffset:HeaderMetadataSize]
	var startHeight int32
	if err = binary.Read(
		bytes.NewReader(startHeightRaw), binary.LittleEndian,
		&startHeight,
	); err != nil {
		return nil, fmt.Errorf("failed to read start height: %w", err)
	}

	if startHeight < 0 {
		return nil, fmt.Errorf("invalid negative value detected for "+
			"StartHeight: %d", startHeight)
	}

	// Construct the header metadata.
	metadata := HeaderMetadata{
		BitcoinChainType: bitcoinChainType,
		HeaderType:       headerType,
		StartHeight:      uint32(startHeight),
	}

	// Construct header type size. This also validates the header type. If
	// the header type is not valid, we will get an error while getting the
	// size of this header type.
	headerSize, err := metadata.HeaderType.Size()
	if err != nil {
		return nil, fmt.Errorf("failed to get header size: %v", err)
	}
	metadata.HeaderSize = headerSize

	// Compute the usable headers file size.
	usableFileSize := f.fileSize - HeaderMetadataSize

	// Check if there are any headers available in the import source.
	if usableFileSize == 0 {
		return nil, errors.New("no headers available in import source")
	}

	// Verify that the file contains complete headers with no partial data.
	if usableFileSize%headerSize != 0 {
		return nil, fmt.Errorf("file size (%d) is not a multiple of "+
			"header size (%d); possible data corruption",
			usableFileSize, headerSize)
	}

	// Compute headers count.
	metadata.HeadersCount = uint32(usableFileSize / headerSize)

	return &metadata, err
}

// Iterator returns an efficient iterator for sequential header access.
func (f *FileHeaderImportSource) Iterator(start, end uint32) HeaderIterator {
	return &ImportSourceHeaderIterator{
		source: f, current: start, end: end,
	}
}

// GetHeader retrieves a single header at the specified index.
func (f *FileHeaderImportSource) GetHeader(index uint32) (Header, error) {
	var empty Header

	// First check if we have data initialized before attempting to get
	// header. This validates both reader and metadata are properly set up.
	if f.reader == nil {
		return empty, errors.New("file reader not initialized")
	}
	if f.metadata == nil {
		return empty, errors.New("header metadata not initialized")
	}

	// Calculate the absolute position for this header.
	offset := HeaderMetadataSize + (index * uint32(f.metadata.HeaderSize))

	// Read header data at the calculated offset.
	buf := make([]byte, f.metadata.HeaderSize)
	_, err := f.reader.ReadAt(buf, int64(offset))
	if err != nil {
		return empty, fmt.Errorf("failed to read header at "+
			"index %d: %w", index, err)
	}
	reader := bytes.NewReader(buf)

	// Calculate the actual height from index and start height.
	height := index + f.metadata.StartHeight

	// Create the appropriate chain import header type based on metadata.
	header := f.headerFactory()
	if err := header.Deserialize(reader, height); err != nil {
		return empty, err
	}

	return header, nil
}

// SetURI sets a string identifier for this import source.
func (f *FileHeaderImportSource) SetURI(uri string) {
	f.uri = uri
}

// GetURI returns a string identifier for this import source.
func (f *FileHeaderImportSource) GetURI() string {
	return f.uri
}

// NewFileHeaderImportSource creates a new file header import source with the
// given URI and header factory.
func NewFileHeaderImportSource(uri string,
	headerFactory func() Header) *FileHeaderImportSource {

	return &FileHeaderImportSource{
		uri:           uri,
		headerFactory: headerFactory,
	}
}

// HTTPClient defines the interface for making HTTP GET requests.
type HTTPClient interface {
	Get(url string) (*http.Response, error)
}

// NewHTTPClient creates a new HTTP client.
func NewHTTPClient() HTTPClient {
	return &http.Client{}
}

// HTTPHeaderImportSource implements HeaderImportSource for serving header files
// over HTTP(s).
type HTTPHeaderImportSource struct {
	uri        string
	httpClient HTTPClient
	file       HeaderImportSource
}

// Compile-time assertion to ensure HTTPHeaderImportSource implements
// HeaderImportSource interface.
var _ HeaderImportSource = (*HTTPHeaderImportSource)(nil)

// Open opens the HTTP header import source based on file header import source.
func (h *HTTPHeaderImportSource) Open() error {
	// Download the headers file.
	resp, err := h.httpClient.Get(h.uri)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Check status code.
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to download file: status code %d",
			resp.StatusCode)
	}

	// Create a temporary file.
	tempFile, err := os.CreateTemp("", "neutrino-headers-http-import-*.tmp")
	if err != nil {
		return err
	}
	cleanup := func() {
		tempFile.Close()
		os.Remove(tempFile.Name())
	}

	// Copy the response body to the temporary file.
	_, err = io.Copy(tempFile, resp.Body)
	if err != nil {
		cleanup()
		return err
	}

	// Sync the file to ensure all data is written to disk.
	if err = tempFile.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("failed to sync temporary file: %w", err)
	}

	// Close before using it with FileHeaderImportSource.
	tempFile.Close()

	// Set he URI for FileHeaderImportSource.
	h.file.SetURI(tempFile.Name())
	return h.file.Open()
}

// Close closes the HTTP header import source resources.
func (h *HTTPHeaderImportSource) Close() error {
	if err := h.file.Close(); err != nil {
		return fmt.Errorf("failed to close file import source: %w", err)
	}

	// Remove the temporary file.
	if err := os.Remove(h.file.GetURI()); err != nil {
		return fmt.Errorf("failed to remove temporary file: %w", err)
	}

	return nil
}

// GetHeaderMetadata reads the metadata from the file. The metadata is memoized
// after the first call, with subsequent calls returning the cached result
// without re-reading the file.
func (h *HTTPHeaderImportSource) GetHeaderMetadata() (*HeaderMetadata, error) {
	return h.file.GetHeaderMetadata()
}

// Iterator returns an efficient iterator for sequential header access.
func (h *HTTPHeaderImportSource) Iterator(start, end uint32) HeaderIterator {
	return h.file.Iterator(start, end)
}

// GetHeader retrieves a single header at the specified index.
func (h *HTTPHeaderImportSource) GetHeader(index uint32) (Header, error) {
	return h.file.GetHeader(index)
}

// GetURI returns a string identifier for this import source.
func (h *HTTPHeaderImportSource) GetURI() string {
	return h.uri
}

// SetURI sets a string identifier for this import source.
func (h *HTTPHeaderImportSource) SetURI(uri string) {
	h.uri = uri
}

// NewHTTPHeaderImportSource creates a new HTTP header import source.
func NewHTTPHeaderImportSource(uri string, httpClient HTTPClient,
	importSource HeaderImportSource) *HTTPHeaderImportSource {

	return &HTTPHeaderImportSource{
		uri:        uri,
		httpClient: httpClient,
		file:       importSource,
	}
}

// HeadersImport uses a generic HeaderImportSource to import headers.
type HeadersImport struct {
	BlockHeadersImportSource  HeaderImportSource
	FilterHeadersImportSource HeaderImportSource
	BlockHeadersValidator     HeadersValidator
	FilterHeadersValidator    HeadersValidator
	options                   *ImportOptions
}

// Import is a multi-pass algorithm that loads, validates, and processes
// headers from the configured import sources into the target header stores. The
// Import process is currently performed only if the target stores are
// completely empty except for gensis block/filter header otherwise it is
// entirely skipped. On first development iteration, it is designed to serve new
// users who don't yet have headers data, or existing users who are willing to
// reset their headers data.
func (s *HeadersImport) Import(ctx context.Context) (*ImportResult, error) {
	// Check first if the target header stores are fresh.
	isFresh, err := s.isTargetFresh(
		s.options.TargetBlockHeaderStore,
		s.options.TargetFilterHeaderStore,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to detect if target stores "+
			"are fresh import failed: %w", err)
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
		return nil, fmt.Errorf("failed to open sources: %w", err)
	}
	defer s.closeSources()

	// Check if the sources are compatible with each other.
	if err := s.validateSourcesCompatibility(); err != nil {
		return nil, fmt.Errorf("failed to validate compatibility of "+
			"header import sources with each other: %w", err)
	}

	// Validate chain continuity with the target chain.
	if err := s.validateChainContinuity(); err != nil {
		return nil, fmt.Errorf("failed to validate continuity of "+
			"import headers chain with target chain: %v", err)
	}

	// Get header metadata from import souces. We can safely use this header
	// metadata for both block headers and filter headers since we've
	// already validated that those header metadata are compatible with each
	// other.
	metadata, err := s.BlockHeadersImportSource.GetHeaderMetadata()
	if err != nil {
		return nil, err
	}

	// Validate all block headers from import source.
	log.Debugf("Validating %d block headers", metadata.HeadersCount)
	if err := s.BlockHeadersValidator.Validate(
		s.BlockHeadersImportSource.Iterator(
			0, metadata.HeadersCount-1,
		),
		s.options.TargetChainParams,
	); err != nil {
		return nil, fmt.Errorf("failed to validate block "+
			"headers: %w", err)
	}

	// Validate all filter headers from import source.
	log.Debugf("Validating %d filter headers", metadata.HeadersCount)
	if err := s.FilterHeadersValidator.Validate(
		s.FilterHeadersImportSource.Iterator(
			0, metadata.HeadersCount-1,
		),
		s.options.TargetChainParams,
	); err != nil {
		return nil, fmt.Errorf("failed to validate filter "+
			"headers: %w", err)
	}

	// Determine processing regions that partition the import task into
	// disjoint height ranges.
	regions, err := s.determineProcessingRegions()
	if err != nil {
		return nil, fmt.Errorf("failed to determine processing "+
			"regions: %w", err)
	}
	result.StartHeight = regions.ImportStartHeight
	result.EndHeight = regions.ImportEndHeight

	// TODO(mohamedawnallah): process the overlap region. This mainly
	// includes a validation strategy for the overlap region between headers
	// from import and target sources.

	// TODO(mohamedawnallah): Process the divergence region. This includes
	// strategy/strategies for handling divergence region that may exist in
	// the target header stores while importing.

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

	// Open block headers import source.
	if err := s.BlockHeadersImportSource.Open(); err != nil {
		return err
	}

	// Open filter headers import source.
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
		log.Warnf("Failed to close filter headers source: %v", err)
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

	// Validate that block headers are of the correct type.
	if blockMetadata.HeaderType != headerfs.Block {
		return fmt.Errorf("incorrect block header type: expected %s, "+
			"got %s", headerfs.Block, blockMetadata.HeaderType)
	}

	// Validate that filter headers are of the correct type.
	if filterMetadata.HeaderType != headerfs.RegularFilter {
		return fmt.Errorf("incorrect filter header type: expected %v, "+
			"got %v", headerfs.RegularFilter,
			filterMetadata.HeaderType)
	}

	// Validate that network types match.
	if blockMetadata.BitcoinChainType != filterMetadata.BitcoinChainType {
		return fmt.Errorf("network type mismatch: block headers "+
			"from %s (%v), filter headers from "+
			"%s (%v)", s.BlockHeadersImportSource.GetURI(),
			blockMetadata.BitcoinChainType,
			s.FilterHeadersImportSource.GetURI(),
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

	// Validate header counts match exactly. This also implicitly validates
	// the end height. The end height is constructed from the start height
	// and headers count.
	if filterMetadata.HeadersCount != blockMetadata.HeadersCount {
		return fmt.Errorf("headers count mismatch: block headers "+
			"import source %s (%d), filter headers import source "+
			"%s (%d)", s.BlockHeadersImportSource.GetURI(),
			blockMetadata.HeadersCount,
			s.FilterHeadersImportSource.GetURI(),
			filterMetadata.HeadersCount)
	}

	return nil
}

// validateChainContinuity ensures that headers from import sources can be
// properly connected to the existing headers in the target stores.
func (s *HeadersImport) validateChainContinuity() error {
	// Get metadata from block header source. We can safely use this count
	// for both block headers and filter headers since we've already
	// validated that the counts match across all import sources.
	sourceMetadata, err := s.BlockHeadersImportSource.GetHeaderMetadata()
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
		return fmt.Errorf("failed to get target filter header chain "+
			"tip: %w", err)
	}

	// Ensure that both target header stores have the same tip height.
	// A mismatch indicates a divergence region that has not yet been
	// processed. Once a resolution strategy is implemented, this check
	// will no longer return an error, and the effective tip height
	// will be defined as the minimum of the two.
	if blockTipHeight != filterTipHeight {
		return fmt.Errorf("divergence detected between target header "+
			"store tip heights (block=%d, filter=%d)",
			blockTipHeight, filterTipHeight)
	}

	// Extract import height range.
	importStartHeight := sourceMetadata.StartHeight
	importEndHeight := sourceMetadata.EndHeight

	// If import wants to start after height 1, we'd have a gap.
	if importStartHeight > 1 {
		return fmt.Errorf("target stores contain only genesis block "+
			"(height 0) but import data starts at height %d, "+
			"creating a gap", importStartHeight)
	}

	// If import includes genesis block (starts at 0), verify it matches.
	if importStartHeight == 0 {
		err := s.verifyHeadersAtTargetHeight(importStartHeight)
		if err != nil {
			return fmt.Errorf("genesis header mismatch: %v", err)
		}
		log.Infof("Genesis headers verified, import data will extend " +
			"chain from genesis")
	} else {
		// Import starts at height 1, which connects to genesis.
		// Validate that the block header at height 1 from the import
		// source connects with the previous header in the target block
		// store.
		if err := s.validateHeaderConnection(
			importStartHeight, blockTipHeight, sourceMetadata,
		); err != nil {
			return fmt.Errorf("failed to validate header "+
				"connection: %v", err)
		}

		log.Info("Target stores contain only genesis block, import " +
			"data will extend chain from height 1")
	}

	log.Infof("Chain continuity validation successful: import data "+
		"(%d-%d) connects properly with target chain",
		importStartHeight, importEndHeight)

	return nil
}

// validateHeaderConnection verifies that a block header from the import source
// properly connects with the previous header in the target block store.
func (s *HeadersImport) validateHeaderConnection(targetStartHeight,
	prevTargetBlockHeight uint32,
	importHeaderMetadata *HeaderMetadata) error {

	// Get the previous block header from target store.
	prevBlkHdr, err := s.options.TargetBlockHeaderStore.FetchHeaderByHeight(
		prevTargetBlockHeight,
	)
	if err != nil {
		return fmt.Errorf("failed to get block header from "+
			"target at height %d: %w", prevTargetBlockHeight, err)
	}

	// Convert target height to the equivalent index for import sources.
	importSourceIndex := targetHeightToImportSourceIndex(
		targetStartHeight, importHeaderMetadata.StartHeight,
	)

	// Get block header at that index from the import source.
	currHeader, err := s.BlockHeadersImportSource.GetHeader(
		importSourceIndex,
	)
	if err != nil {
		return fmt.Errorf("failed to get block header from "+
			"import source at height %d: %w",
			targetStartHeight, err)
	}

	// Type assert the header retrieved from import source to be block
	// header.
	currBlkHeader, ok := currHeader.(*BlockHeader)
	if !ok {
		return fmt.Errorf("expected BlockHeader type, got %T",
			currHeader)
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

	log.Debugf("Validated block header connection: import height %d "+
		"properly connects to target chain at height %d "+
		"(prev hash: %v)", targetStartHeight, prevTargetBlockHeight,
		prevHash)

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
//  3. NewHeaders: Heights in source not yet in targets
//
//nolint:lll
func (s *HeadersImport) determineProcessingRegions() (*ProcessingRegions, error) {
	// Get header metadata for import sources. The start and end height
	// constraint range validated upstream.
	metadata, err := s.BlockHeadersImportSource.GetHeaderMetadata()
	if err != nil {
		return nil, fmt.Errorf("failed to get header metadata: %v", err)
	}
	importStartHeight := metadata.StartHeight
	importEndHeight := metadata.EndHeight

	// Get chain tips for target stores.
	_, bTipHeight, err := s.options.TargetBlockHeaderStore.ChainTip()
	if err != nil {
		return nil, fmt.Errorf("failed to get target block header "+
			"store chain tip: %v", err)
	}
	_, fTipHeight, err := s.options.TargetFilterHeaderStore.ChainTip()
	if err != nil {
		return nil, fmt.Errorf("failed to get target filter header "+
			"store chain tip: %v", err)
	}
	effectiveTipHeight := min(bTipHeight, fTipHeight)

	// Create regions construct.
	regions := &ProcessingRegions{
		ImportStartHeight: importStartHeight,
		ImportEndHeight:   importEndHeight,
		EffectiveTip:      effectiveTipHeight,
	}

	// 1. Overlap region.
	// This region contains headers that exist in both the import source and
	// target stores, from the start of the import range up to the effective
	// tip height.
	overlapStart := importStartHeight
	overlapEnd := min(effectiveTipHeight, importEndHeight)
	regions.Overlap = HeaderRegion{
		Start:  overlapStart,
		End:    overlapEnd,
		Exists: overlapStart <= overlapEnd,
	}

	// 2. Divergence region.
	// This region contains headers where one store extends beyond the
	// effective tip but still within the import range. It represents
	// heights where targetblock and filter headers are out of sync and need
	// reconciliation.
	divergeStart := effectiveTipHeight + 1
	divergeEnd := min(max(bTipHeight, fTipHeight), importEndHeight)
	regions.Divergence = HeaderRegion{
		Start:  divergeStart,
		End:    divergeEnd,
		Exists: bTipHeight != fTipHeight && divergeStart <= divergeEnd,
	}

	// 3. New Headers region.
	// This region contains headers that are in the import source but not
	// yet in either target store. They start one height beyond the highest
	// tip of either store (ensuring no overlap with divergence region) and
	// extend to the end of the import data. These headers need to be added
	// to both stores. It only exists if there are headers beyond both tips.
	//
	// Note: This region is supposed to be processed after handling the
	// overlap, and divergence regions, ensuring that any potential
	// inconsistencies in existing data are resolved before adding new
	// headers. This sequential processing guarantees that new headers
	// are only added on top of a verified and consistent chain state.
	newStart := max(bTipHeight, fTipHeight) + 1
	newEnd := importEndHeight
	regions.NewHeaders = HeaderRegion{
		Start:  newStart,
		End:    newEnd,
		Exists: newStart <= newEnd,
	}

	return regions, nil
}

// verifyHeadersAtTargetHeight ensures headers at the specified height match
// exactly between import and target sources by performing a byte-level
// comparison.
//
// The function retrieves the header from both sources at the given height and
// verifies they are identical, returning an error if any discrepancy is found.
func (s *HeadersImport) verifyHeadersAtTargetHeight(height uint32) error {
	// Get header metadata from import souces. We can safely use this header
	// metadata for both block headers and filter headers since we've
	// already validated that those header metadata are compatible with each
	// other.
	headerMetadata, err := s.BlockHeadersImportSource.GetHeaderMetadata()
	if err != nil {
		return fmt.Errorf("failed to get block header "+
			"metadata: %w", err)
	}

	// Convert target height to the equivalent index for import sources.
	importSourceIndex := targetHeightToImportSourceIndex(
		height, headerMetadata.StartHeight,
	)

	// Get block header at that index from the import source.
	importSourceHeader, err := s.BlockHeadersImportSource.GetHeader(
		importSourceIndex,
	)
	if err != nil {
		return fmt.Errorf("failed to get block header from import "+
			"source at height %d: %w", height, err)
	}

	// Type assert the header retrieved from import source to be block
	// header.
	importSourceBlkHeader, ok := importSourceHeader.(*BlockHeader)
	if !ok {
		return fmt.Errorf("expected BlockHeader type, got %T",
			importSourceHeader)
	}

	// Get the equivalent block header from target store.
	targetBlkHeaderStore := s.options.TargetBlockHeaderStore
	targetBlkHeader, err := targetBlkHeaderStore.FetchHeaderByHeight(
		height,
	)
	if err != nil {
		return fmt.Errorf("failed to get block header from target at "+
			"height %d: %w", height, err)
	}

	// Compare block headers from import and target sources.
	sourceBlkHeaderHash := importSourceBlkHeader.BlockHash()
	targetBlkHeaderHash := targetBlkHeader.BlockHash()
	if !sourceBlkHeaderHash.IsEqual(&targetBlkHeaderHash) {
		return fmt.Errorf("block header mismatch at height %d: source "+
			"has %v but target has %v", height,
			sourceBlkHeaderHash, targetBlkHeaderHash)
	}

	// Get and verify filter headers. We can safely use the same import
	// source index used for import source block header store since we've
	// validated earlier their start height and headers count match exactly.
	importSourceHeader, err = s.FilterHeadersImportSource.GetHeader(
		importSourceIndex,
	)
	if err != nil {
		return fmt.Errorf("failed to get filter header from import "+
			"source at height %d: %w", height, err)
	}

	// Type assert the header retrieved from import source to be filter
	// header.
	importSourceFilterHeader, ok := importSourceHeader.(*FilterHeader)
	if !ok {
		return fmt.Errorf("expected FilterHeader type, got %T",
			importSourceHeader)
	}

	// Get the equivalent filter header from target store.s
	filterHeaderStore := s.options.TargetFilterHeaderStore
	targetFilterHeader, err := filterHeaderStore.FetchHeaderByHeight(
		height,
	)
	if err != nil {
		return fmt.Errorf("failed to get filter header from target at "+
			"height %d: %w", height, err)
	}

	// Compare filter headers from import and target sources.
	sourceFilterHeaderHash := importSourceFilterHeader.FilterHash
	targetFilterHeaderHash := *targetFilterHeader
	if !sourceFilterHeaderHash.IsEqual(&targetFilterHeaderHash) {
		return fmt.Errorf("filter header mismatch at height %d: "+
			"source has %v but target has %v", height,
			sourceFilterHeaderHash, targetFilterHeaderHash)
	}

	log.Debugf("Headers from %s (block) and %s (filter) verified at "+
		"height %d", s.BlockHeadersImportSource.GetURI(),
		s.FilterHeadersImportSource.GetURI(), height)

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
	metadata, err := s.BlockHeadersImportSource.GetHeaderMetadata()
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
		sourceStartIdx := targetHeightToImportSourceIndex(
			batchStart, metadata.StartHeight,
		)
		sourceEndIdx := targetHeightToImportSourceIndex(
			batchEnd, metadata.StartHeight,
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

			if header != nil {
				blkHeader, ok := header.(*BlockHeader)
				if !ok {
					return fmt.Errorf("expected "+
						"BlockHeader type, got %T",
						header)
				}

				blockHeaders = append(
					blockHeaders, blkHeader.BlockHeader,
				)
			}

			if !hasMore {
				break
			}
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

			if header != nil {
				fHeader, ok := header.(*FilterHeader)
				if !ok {
					return fmt.Errorf("expected "+
						"FilterHeader type, got %T",
						header)
				}

				filterHeaders = append(
					filterHeaders, fHeader.FilterHeader,
				)
			}

			if !hasMore {
				break
			}
		}

		// The length check conditions should never be triggered
		// during normal import operations as validation occurs earlier.
		// They serve as sanity checks to catch unexpected
		// inconsistencies.
		if len(blockHeaders) != len(filterHeaders) {
			return fmt.Errorf("mismatch between block headers "+
				"(%d) and filter headers (%d) for batch %d-%d",
				len(blockHeaders), len(filterHeaders),
				batchStart, batchEnd)
		}
		if len(blockHeaders) == 0 {
			return fmt.Errorf("no headers read for "+
				"batch %d-%d", batchStart, batchEnd)
		}

		setLastFilterHeaderHash(filterHeaders, blockHeaders)

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
	filterHeaders []headerfs.FilterHeader,
	batchStart, batchEnd uint32) error {

	// Write block headers batch to target store.
	if err := s.options.TargetBlockHeaderStore.WriteHeaders(
		blockHeaders...,
	); err != nil {
		return fmt.Errorf("failed to write block headers "+
			"batch %d-%d: %w", batchStart, batchEnd, err)
	}

	// Write filter headers batch to target store.
	if err := s.options.TargetFilterHeaderStore.WriteHeaders(
		filterHeaders...,
	); err != nil {
		// If we've reached here, the headers failed to be written to
		// target filter store because of I/O errors regarding binary
		// file or filter db store, and it is automatically rolled back
		// upstream.
		//
		// The whole import operation needs to satisfy the conjunction
		// property for both block and filter header stores - it's all
		// or nothing, so they need to be at the same length. We must
		// rollback the block headers to maintain this consistency.
		blkStore := s.options.TargetBlockHeaderStore
		blockHeadersToTruncate := uint32(len(blockHeaders))
		_, rollbackErr := blkStore.RollbackBlockHeaders(
			blockHeadersToTruncate,
		)
		if rollbackErr != nil {
			return fmt.Errorf("failed to rollback %d headers from "+
				"target block header store after filter "+
				"headers write failure. Block error: %w, "+
				"filter error: %v", blockHeadersToTruncate,
				rollbackErr, err)
		}

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
		options.WriteBatchSizePerRegion = DefaultWriteBatchSizePerRegion
		log.Infof("Using default write batch size of %d "+
			"headers per region", options.WriteBatchSizePerRegion)
	}

	// Create block headers import source based on source string format.
	blockHeadersSource := options.createBlockHeaderImportSrc()

	// Create filter headers import source based on source string format.
	filterHeadersSource := options.createFilterHeaderImportSrc()

	// Create block headers import source validator.
	blockHeadersValidator := options.createBlockHeaderValidator()

	// Create filter headers import source validator.
	filterHeadersValidator := options.createFilterHeaderValidator()

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

// createBlockHeaderImportSrc creates the appropriate import source for block
// headers.
func (options *ImportOptions) createBlockHeaderImportSrc() HeaderImportSource {
	// Check if the block headers source is a HTTP(s) URI.
	if strings.HasPrefix(options.BlockHeadersSource, "http") {
		// The empty string ("") URI passed to NewFileHeaderImportSource
		// will be replaced by the temporary file name once the file has
		// been downloaded from the HTTP source.
		return NewHTTPHeaderImportSource(
			options.BlockHeadersSource, NewHTTPClient(),
			NewFileHeaderImportSource("", NewBlockHeader),
		)
	}

	// Otherwise, fallback to file headers import source.
	return NewFileHeaderImportSource(
		options.BlockHeadersSource, NewBlockHeader,
	)
}

// createBlockHeaderValidator creates the appropriate validator for block
// headers.
func (options *ImportOptions) createBlockHeaderValidator() HeadersValidator {
	return NewBlockHeadersImportSourceValidator()
}

// createFilterHeaderImportSrc creates the appropriate import source for
// filter headers.
func (options *ImportOptions) createFilterHeaderImportSrc() HeaderImportSource {
	// Check if the filter headers source is a HTTP(s) URI.
	if strings.HasPrefix(options.FilterHeadersSource, "http") {
		return NewHTTPHeaderImportSource(
			options.FilterHeadersSource, NewHTTPClient(),
			NewFileHeaderImportSource("", NewFilterHeader),
		)
	}

	// Otherwise, fallback to file headers import source.
	return NewFileHeaderImportSource(
		options.FilterHeadersSource, NewFilterHeader,
	)
}

// createFilterHeaderValidator creates the appropriate validator for filter
// headers.
func (options *ImportOptions) createFilterHeaderValidator() HeadersValidator {
	return NewFilterHeadersImportSourceValidator()
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

// setLastFilterHeaderHash updates the HeaderHash of the last filter header
// to match the block hash of the corresponding block header. This maintains
// chain tip consistency for the regular tip.
func setLastFilterHeaderHash(filterHeaders []headerfs.FilterHeader,
	blockHeaders []headerfs.BlockHeader) {

	// We only need to set the block header hash of the last filter
	// header to maintain chain tip consistency for regular tip.
	lastIdx := len(filterHeaders) - 1
	chainTipHash := blockHeaders[lastIdx].BlockHeader.BlockHash()
	filterHeaders[lastIdx].HeaderHash = chainTipHash
}

// targetHeightToImportSourceIndex converts the absolute blockchain target
// height to the equivalent import source height based on the start height
// input.
func targetHeightToImportSourceIndex(targetH, importStartH uint32) uint32 {
	return targetH - importStartH
}
