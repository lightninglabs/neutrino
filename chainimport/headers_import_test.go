package chainimport

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/mmap"
)

func TestImportOptionsInit(t *testing.T) {
	t.Parallel()
	t.Run("HeadersConjunctionProperty", func(t *testing.T) {
		t.Parallel()
		testCases := []struct {
			name         string
			options      *ImportOptions
			expectErr    bool
			expectErrMsg string
		}{
			{
				name: "MissingFilterHeaderImportSource",
				options: &ImportOptions{
					BlockHeadersSource:  "/path/to/blocks",
					FilterHeadersSource: "",
				},
				expectErr: true,
				expectErrMsg: "missing filter headers source " +
					"path",
			},
			{
				name: "MissingBlockHeaderImportSource",
				options: &ImportOptions{
					BlockHeadersSource:  "",
					FilterHeadersSource: "/path/to/filters",
				},
				expectErr: true,
				expectErrMsg: "missing block headers source " +
					"path",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				ctx := context.Background()
				_, err := tc.options.Import(ctx)
				if tc.expectErr {
					require.ErrorContains(
						t, err, tc.expectErrMsg,
					)
					return
				}
				require.NoError(t, err)
			})
		}
	})
	t.Run("ImportSkipOperation", func(t *testing.T) {})
}

func TestTargetStoreFreshnessDetection(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name                 string
		blockHeight          uint32
		filterHeight         uint32
		expectFresh          bool
		expectErr            bool
		expectFilterStoreErr error
		expectBlockStoreErr  error
	}{
		{
			name:         "OnlyGenesisHeadersExist",
			blockHeight:  0,
			filterHeight: 0,
			expectFresh:  true,
		},
		{
			name:         "BlockHeightGreaterThanZero",
			blockHeight:  1,
			filterHeight: 0,
			expectFresh:  false,
		},
		{
			name:         "FilterHeightGreaterThanZero",
			blockHeight:  0,
			filterHeight: 1,
			expectFresh:  false,
		},
		{
			name:         "BothHeightsGreaterThanZero",
			blockHeight:  10,
			filterHeight: 10,
			expectFresh:  false,
		},
		{
			name:      "ErrorOnBlockStoreFailure",
			expectErr: true,
			expectBlockStoreErr: errors.New(
				"failed to get target block header",
			),
		},
		{
			name:      "ErrorOnFilterStoreFailure",
			expectErr: true,
			expectFilterStoreErr: errors.New(
				"failed to get target filter header",
			),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup mock block header store.
			mockBlockStore := &mockBlockHeaderStore{}
			mockBlockStore.On("ChainTip").Return(
				&wire.BlockHeader{}, tc.blockHeight,
				tc.expectBlockStoreErr,
			)

			// Setup mock filter header store.
			mockFilterStore := &mockFilterHeaderStore{}
			mockFilterStore.On("ChainTip").Return(
				&chainhash.Hash{}, tc.filterHeight,
				tc.expectFilterStoreErr,
			)

			// Create importer.
			importer := &HeadersImport{}

			// Test the function.
			isFresh, err := importer.isTargetFresh(
				mockBlockStore, mockFilterStore,
			)
			if tc.expectErr {
				if tc.expectBlockStoreErr != nil {
					require.Error(t, err)
					require.ErrorContains(
						t, err,
						tc.expectBlockStoreErr.Error(),
					)
				}
				if tc.expectFilterStoreErr != nil {
					require.Error(t, err)
					require.ErrorContains(
						t, err,
						tc.expectFilterStoreErr.Error(),
					)
				}
				return
			}
			require.NoError(t, err)
			require.Equal(t, isFresh, tc.expectFresh)
		})
	}
}

func TestImportOpenSources(t *testing.T) {
	t.Parallel()
	t.Run("FileHeaderImportSource", func(t *testing.T) {
		t.Parallel()
		testCases := []struct {
			name         string
			prep         func() (*HeadersImport, func(), error)
			verify       func(*testing.T, *HeadersImport)
			expectErr    bool
			expectErrMsg string
		}{
			{
				name: "MissingBlockANDFilterHeaderImportSource",
				prep: func() (*HeadersImport, func(), error) {
					opts := &ImportOptions{}
					headersImport := &HeadersImport{
						options:                   opts,
						BlockHeadersImportSource:  nil,
						FilterHeadersImportSource: nil,
					}

					return headersImport, func() {}, nil
				},
				verify:       func(*testing.T, *HeadersImport) {},
				expectErr:    true,
				expectErrMsg: "missing required header sources",
			},
			{
				name: "MissingBlockHeaderImportSource",
				prep: func() (*HeadersImport, func(), error) {
					opts := &ImportOptions{}
					blockSource := opts.createBlockHeadersImportSource()
					headersImport := &HeadersImport{
						options:                   opts,
						BlockHeadersImportSource:  blockSource,
						FilterHeadersImportSource: nil,
					}
					return headersImport, func() {}, nil
				},
				verify:       func(*testing.T, *HeadersImport) {},
				expectErr:    true,
				expectErrMsg: "missing required header sources",
			},
			{
				name: "MissingFilterHeaderImportSource",
				prep: func() (*HeadersImport, func(), error) {
					opts := &ImportOptions{}
					filterSource := opts.createFilterHeadersImportSource()
					headersImport := &HeadersImport{
						options:                   opts,
						BlockHeadersImportSource:  nil,
						FilterHeadersImportSource: filterSource,
					}
					return headersImport, func() {}, nil
				},
				verify:       func(*testing.T, *HeadersImport) {},
				expectErr:    true,
				expectErrMsg: "missing required header sources",
			},
			{
				name: "MissingBlockANDFilterHeaderValidators",
				prep: func() (*HeadersImport, func(), error) {
					opts := &ImportOptions{}
					blockSource := opts.createBlockHeadersImportSource()
					filterSource := opts.createFilterHeadersImportSource()
					headersImport := &HeadersImport{
						options:                   opts,
						BlockHeadersImportSource:  blockSource,
						FilterHeadersImportSource: filterSource,
						BlockHeadersValidator:     nil,
						FilterHeadersValidator:    nil,
					}
					return headersImport, func() {}, nil
				},
				verify:    func(*testing.T, *HeadersImport) {},
				expectErr: true,
				expectErrMsg: "missing required header " +
					"validators",
			},
			{
				name: "MissingBlockHeaderValidator",
				prep: func() (*HeadersImport, func(), error) {
					opts := &ImportOptions{}
					blockSource := opts.createBlockHeadersImportSource()
					filterSource := opts.createFilterHeadersImportSource()
					filterValidator := &FilterHeadersImportSourceValidator{}
					headersImport := &HeadersImport{
						options:                   opts,
						BlockHeadersImportSource:  blockSource,
						FilterHeadersImportSource: filterSource,
						BlockHeadersValidator:     nil,
						FilterHeadersValidator:    filterValidator,
					}
					return headersImport, func() {}, nil
				},
				verify:    func(*testing.T, *HeadersImport) {},
				expectErr: true,
				expectErrMsg: "missing required header " +
					"validators",
			},
			{
				name: "MissingFilterHeaderValidator",
				prep: func() (*HeadersImport, func(), error) {
					opts := &ImportOptions{}
					blockSource := opts.createBlockHeadersImportSource()
					filterSource := opts.createFilterHeadersImportSource()
					blockValidator := &BlockHeadersImportSourceValidator{}
					headersImport := &HeadersImport{
						options:                   opts,
						BlockHeadersImportSource:  blockSource,
						FilterHeadersImportSource: filterSource,
						BlockHeadersValidator:     blockValidator,
						FilterHeadersValidator:    nil,
					}
					return headersImport, func() {}, nil
				},
				verify:    func(*testing.T, *HeadersImport) {},
				expectErr: true,
				expectErrMsg: "missing required header " +
					"validators",
			},
			{
				name: "ErrorOnBlockFileNotExist",
				prep: func() (*HeadersImport, func(), error) {
					opts := &ImportOptions{}
					blockSource := opts.createBlockHeadersImportSource()
					filterSource := opts.createFilterHeadersImportSource()
					blockValidator := &BlockHeadersImportSourceValidator{}
					filterValidator := &FilterHeadersImportSourceValidator{}

					filePath := "/path/to/nonexistent/file"
					blockSource.SetPath(filePath)

					headersImport := &HeadersImport{
						options:                   opts,
						BlockHeadersImportSource:  blockSource,
						FilterHeadersImportSource: filterSource,
						BlockHeadersValidator:     blockValidator,
						FilterHeadersValidator:    filterValidator,
					}
					return headersImport, func() {}, nil
				},
				verify:    func(*testing.T, *HeadersImport) {},
				expectErr: true,
				expectErrMsg: "failed to mmap file: open " +
					"/path/to/nonexistent/file",
			},
			{
				name: "ErrorOnFilterFileNotExist",
				prep: func() (*HeadersImport, func(), error) {
					// Create block headers file.
					bFile, c1, err := setupFileWithHdrs(
						headerfs.Block, true,
					)
					if err != nil {
						return nil, c1, err
					}

					// Configure import options.
					opts := &ImportOptions{}
					blockSource := opts.createBlockHeadersImportSource()
					filterSource := opts.createFilterHeadersImportSource()
					blockValidator := &BlockHeadersImportSourceValidator{}
					filterValidator := &FilterHeadersImportSourceValidator{}

					blockSource.SetPath(bFile.Name())

					filePath := "/path/to/nonexistent/file"
					blockSource.SetPath(filePath)

					headersImport := &HeadersImport{
						options:                   opts,
						BlockHeadersImportSource:  blockSource,
						FilterHeadersImportSource: filterSource,
						BlockHeadersValidator:     blockValidator,
						FilterHeadersValidator:    filterValidator,
					}
					return headersImport, c1, nil
				},
				verify:    func(*testing.T, *HeadersImport) {},
				expectErr: true,
				expectErrMsg: "failed to mmap file: open " +
					"/path/to/nonexistent/file",
			},
			{
				name: "ErrorOnGetBlockHeaderMetadataFail",
				prep: func() (*HeadersImport, func(), error) {
					// Create block headers empty file.
					blockFile, err := os.CreateTemp(
						"", "empty-block-header-*",
					)
					cleanup := func() {
						blockFile.Close()
						os.Remove(blockFile.Name())
					}
					if err != nil {
						return nil, cleanup, err
					}

					// Configure import options.
					opts := &ImportOptions{}
					blockSource := opts.createBlockHeadersImportSource()
					filterSource := opts.createFilterHeadersImportSource()
					blockValidator := &BlockHeadersImportSourceValidator{}
					filterValidator := &FilterHeadersImportSourceValidator{}

					blockSource.SetPath(blockFile.Name())

					headersImport := &HeadersImport{
						options:                   opts,
						BlockHeadersImportSource:  blockSource,
						FilterHeadersImportSource: filterSource,
						BlockHeadersValidator:     blockValidator,
						FilterHeadersValidator:    filterValidator,
					}
					return headersImport, cleanup, nil
				},
				verify:       func(*testing.T, *HeadersImport) {},
				expectErr:    true,
				expectErrMsg: "failed to read metadata: EOF",
			},
			{
				name: "ErrorOnGetFilterHeaderMetadataFail",
				prep: func() (*HeadersImport, func(), error) {
					// Create block headers file.
					bFile, c1, err := setupFileWithHdrs(
						headerfs.Block, true,
					)
					if err != nil {
						return nil, c1, err
					}

					// Create filter headers empty file.
					fFile, err := os.CreateTemp(
						"", "empty-filter-header-*",
					)
					c2 := func() {
						fFile.Close()
						os.Remove(fFile.Name())
					}
					cleanup := func() {
						c2()
						c1()
					}
					if err != nil {
						return nil, cleanup, err
					}

					// Configure import options.
					opts := &ImportOptions{}
					blockSource := opts.createBlockHeadersImportSource()
					filterSource := opts.createFilterHeadersImportSource()
					blockValidator := &BlockHeadersImportSourceValidator{}
					filterValidator := &FilterHeadersImportSourceValidator{}

					blockSource.SetPath(bFile.Name())
					filterSource.SetPath(fFile.Name())

					headersImport := &HeadersImport{
						options:                   opts,
						BlockHeadersImportSource:  blockSource,
						FilterHeadersImportSource: filterSource,
						BlockHeadersValidator:     blockValidator,
						FilterHeadersValidator:    filterValidator,
					}
					return headersImport, cleanup, nil
				},
				verify:       func(*testing.T, *HeadersImport) {},
				expectErr:    true,
				expectErrMsg: "failed to read metadata: EOF",
			},
			{
				name: "ErrorOnBlockTypeValidationFails",
				prep: func() (*HeadersImport, func(), error) {
					// Create filter headers file.
					fFile, c1, err := setupFileWithHdrs(
						headerfs.RegularFilter, true,
					)
					if err != nil {
						return nil, c1, err
					}

					// Configure import options.
					opts := &ImportOptions{}
					blockSource := opts.createBlockHeadersImportSource()
					filterSource := opts.createFilterHeadersImportSource()
					blockValidator := &BlockHeadersImportSourceValidator{}
					filterValidator := &FilterHeadersImportSourceValidator{}

					blockSource.SetPath(fFile.Name())

					headersImport := &HeadersImport{
						options:                   opts,
						BlockHeadersImportSource:  blockSource,
						FilterHeadersImportSource: filterSource,
						BlockHeadersValidator:     blockValidator,
						FilterHeadersValidator:    filterValidator,
					}
					return headersImport, c1, nil
				},
				verify:    func(*testing.T, *HeadersImport) {},
				expectErr: true,
				expectErrMsg: "file contains " +
					"RegularFilterHeader headers, but " +
					"expected block headers",
			},
			{
				name: "ErrorOnFilterTypeValidationFails",
				prep: func() (*HeadersImport, func(), error) {
					// Create block headers file.
					bFile, c1, err := setupFileWithHdrs(
						headerfs.Block, true,
					)
					if err != nil {
						return nil, c1, err
					}

					// Configure import options.
					opts := &ImportOptions{}
					blockSource := opts.createBlockHeadersImportSource()
					filterSource := opts.createFilterHeadersImportSource()
					blockValidator := &BlockHeadersImportSourceValidator{}
					filterValidator := &FilterHeadersImportSourceValidator{}

					blockSource.SetPath(bFile.Name())
					filterSource.SetPath(bFile.Name())

					headersImport := &HeadersImport{
						options:                   opts,
						BlockHeadersImportSource:  blockSource,
						FilterHeadersImportSource: filterSource,
						BlockHeadersValidator:     blockValidator,
						FilterHeadersValidator:    filterValidator,
					}
					return headersImport, c1, nil
				},
				verify:    func(*testing.T, *HeadersImport) {},
				expectErr: true,
				expectErrMsg: "file contains BlockHeader " +
					"headers, but expected filter " +
					"headers",
			},
			{
				name: "OpenSourcesCorrectly",
				prep: func() (*HeadersImport, func(), error) {
					// Create block headers file.
					bFile, c1, err := setupFileWithHdrs(
						headerfs.Block, true,
					)
					if err != nil {
						return nil, c1, err
					}

					// Create filter headers file.
					fFile, c2, err := setupFileWithHdrs(
						headerfs.RegularFilter, true,
					)
					cleanup := func() {
						c2()
						c1()
					}
					if err != nil {
						return nil, cleanup, err
					}

					// Configure import options.
					opts := &ImportOptions{}
					blockSource := opts.createBlockHeadersImportSource()
					filterSource := opts.createFilterHeadersImportSource()
					blockValidator := &BlockHeadersImportSourceValidator{}
					filterValidator := &FilterHeadersImportSourceValidator{}

					blockSource.SetPath(bFile.Name())
					filterSource.SetPath(fFile.Name())

					headersImport := &HeadersImport{
						options:                   opts,
						BlockHeadersImportSource:  blockSource,
						FilterHeadersImportSource: filterSource,
						BlockHeadersValidator:     blockValidator,
						FilterHeadersValidator:    filterValidator,
					}
					return headersImport, cleanup, nil
				},
				verify: func(t *testing.T, hdrsImport *HeadersImport) {
					// Prep block and filter hdrs metadata.
					expectBlockMetadata := &HeaderMetadata{
						BitcoinChainType: wire.SimNet,
						HeaderType:       headerfs.Block,
						StartHeight:      0,
						EndHeight:        4,
						HeadersCount:     5,
					}
					expectFilterMetadata := &HeaderMetadata{
						BitcoinChainType: wire.SimNet,
						HeaderType:       headerfs.RegularFilter,
						StartHeight:      0,
						EndHeight:        4,
						HeadersCount:     5,
					}

					// Verify block header metadata.
					metadata, err := hdrsImport.BlockHeadersImportSource.GetHeaderMetadata()
					require.NoError(t, err)
					require.Equal(
						t, expectBlockMetadata,
						metadata,
					)

					// Verify filter header metadata.
					metadata, err = hdrsImport.FilterHeadersImportSource.GetHeaderMetadata()
					require.NoError(t, err)
					require.Equal(
						t, expectFilterMetadata,
						metadata,
					)
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				headersImport, cleanup, err := tc.prep()
				t.Cleanup(cleanup)
				require.NoError(t, err)

				err = headersImport.openSources()
				if tc.expectErr {
					require.ErrorContains(
						t, err, tc.expectErrMsg,
					)
					tc.verify(t, headersImport)
					return
				}
				require.NoError(t, err)
				tc.verify(t, headersImport)
			})
		}
	})
}

func TestHeaderMetadata(t *testing.T) {
	t.Parallel()
	t.Run("Retrieval", func(t *testing.T) {
		t.Parallel()
		testCases := []struct {
			name         string
			prep         func() (*HeadersImport, func(), error)
			verify       func(*testing.T, *HeaderMetadata)
			expectErr    bool
			expectErrMsg string
		}{
			{
				name: "ReturnsCachedMetadataWhenAvailable",
				prep: func() (*HeadersImport, func(), error) {
					// Create block headers file.
					bFile, c1, err := setupFileWithHdrs(
						headerfs.Block, true,
					)
					if err != nil {
						return nil, c1, err
					}

					// Configure import options.
					opts := &ImportOptions{}
					blockSource := opts.createBlockHeadersImportSource()
					blockSource.SetPath(bFile.Name())

					// Force a cache miss by opening the
					// source file for the first time.
					err = blockSource.Open()
					c2 := func() {
						blockSource.Close()
						os.Remove(blockSource.GetPath())
						c1()
					}
					if err != nil {
						return nil, c2, err
					}

					// Remove the source file to ensure next
					// call must use cached data.
					err = blockSource.Close()
					if err != nil {
						return nil, c2, err
					}

					headersImport := &HeadersImport{
						options:                  opts,
						BlockHeadersImportSource: blockSource,
					}
					return headersImport, c1, nil
				},
				verify: func(t *testing.T, m *HeaderMetadata) {
					// Next call should result in a cache
					// hit since the file is gone.
					expectBlockMetadata := &HeaderMetadata{
						BitcoinChainType: wire.SimNet,
						HeaderType:       headerfs.Block,
						StartHeight:      0,
						EndHeight:        4,
						HeadersCount:     5,
					}
					require.Equal(t, expectBlockMetadata, m)
				},
			},
			{
				name: "ErrorOnReaderNotInitialized",
				prep: func() (*HeadersImport, func(), error) {
					// Create block headers file.
					bFile, cleanup, err := setupFileWithHdrs(
						headerfs.Block, true,
					)
					if err != nil {
						return nil, cleanup, err
					}

					// Configure import options.
					opts := &ImportOptions{}
					blockSource := opts.createBlockHeadersImportSource()
					blockSource.SetPath(bFile.Name())

					headersImport := &HeadersImport{
						options:                  opts,
						BlockHeadersImportSource: blockSource,
					}
					return headersImport, cleanup, nil
				},
				verify: func(t *testing.T, m *HeaderMetadata) {
					require.Nil(t, m)
				},
				expectErr:    true,
				expectErrMsg: "file reader not initialized",
			},
			{
				name: "ErrorOnHeaderReadFails",
				prep: func() (*HeadersImport, func(), error) {
					// Create block headers empty file.
					bFile, err := os.CreateTemp(
						"", "invalid-block-header-*",
					)
					c1 := func() {
						bFile.Close()
						os.Remove(bFile.Name())
					}
					if err != nil {
						return nil, c1, err
					}

					// Add header metadata to the file.
					err = AddHeadersImportMetadata(
						bFile.Name(), wire.SimNet,
						headerfs.Block, 0,
					)
					if err != nil {
						return nil, c1, err
					}

					// Reopen the file to get an updated
					// file descriptor.
					bFile.Close()
					bFile, err = os.OpenFile(bFile.Name(), os.O_RDWR, 0644)
					c1 = func() {
						bFile.Close()
						os.Remove(bFile.Name())
					}
					if err != nil {
						return nil, c1, err
					}

					// Configure import options.
					opts := &ImportOptions{}
					blockSource := opts.createBlockHeadersImportSource()
					blockSource.SetPath(bFile.Name())

					// Remove the last byte of header
					// metadata to simulate EOF.
					fileInfo, err := bFile.Stat()
					if err != nil {
						return nil, c1, err
					}
					fileSize := fileInfo.Size()
					if fileSize == 0 {
						err := fmt.Errorf("empty "+
							"file: %s",
							bFile.Name())
						return nil, c1, err
					}
					err = bFile.Truncate(fileSize - 1)
					if err != nil {
						return nil, c1, err
					}
					err = bFile.Sync()
					if err != nil {
						return nil, c1, err
					}

					// Convert to file header import source.
					blockFileSource, ok := blockSource.(*FileHeaderImportSource[*BlockHeader, HeaderFactory[*BlockHeader]])
					require.True(t, ok)

					// Set the internal reader.
					reader, err := mmap.Open(bFile.Name())
					cleanup := func() {
						reader.Close()
						os.Remove(bFile.Name())
					}
					if err != nil {
						return nil, cleanup, err
					}
					blockFileSource.reader = reader

					// Make sure the metadata is empty.
					blockFileSource.metadata = nil

					headersImport := &HeadersImport{
						options:                  opts,
						BlockHeadersImportSource: blockFileSource,
					}
					return headersImport, cleanup, nil
				},
				verify: func(t *testing.T, hm *HeaderMetadata) {
				},
				expectErr:    true,
				expectErrMsg: "failed to read metadata: EOF",
			},
			{
				name: "ErrorOnUnknownHeaderType",
				prep: func() (*HeadersImport, func(), error) {
					// Create block headers empty file.
					bFile, err := os.CreateTemp(
						"", "invalid-block-header-*",
					)
					c1 := func() {
						bFile.Close()
						os.Remove(bFile.Name())
					}
					if err != nil {
						return nil, c1, err
					}

					// Add header metadata to the file.
					err = AddHeadersImportMetadata(
						bFile.Name(), wire.SimNet,
						headerfs.UnknownHeader, 0,
					)
					if err != nil {
						return nil, c1, err
					}

					// Reopen the file to get an updated
					// file descriptor.
					bFile.Close()
					bFile, err = os.OpenFile(bFile.Name(), os.O_RDWR, 0644)
					c1 = func() {
						bFile.Close()
						os.Remove(bFile.Name())
					}
					if err != nil {
						return nil, c1, err
					}

					// Configure import options.
					opts := &ImportOptions{}
					blockSource := opts.createBlockHeadersImportSource()
					blockSource.SetPath(bFile.Name())

					// Convert to file header import source.
					blockFileSource, ok := blockSource.(*FileHeaderImportSource[*BlockHeader, HeaderFactory[*BlockHeader]])
					require.True(t, ok)

					// Set the internal reader.
					reader, err := mmap.Open(bFile.Name())
					cleanup := func() {
						reader.Close()
						os.Remove(bFile.Name())
					}
					if err != nil {
						return nil, cleanup, err
					}
					blockFileSource.reader = reader

					// Make sure the metadata is empty.
					blockFileSource.metadata = nil

					headersImport := &HeadersImport{
						options:                  opts,
						BlockHeadersImportSource: blockFileSource,
					}
					return headersImport, cleanup, nil
				},
				verify:    func(t *testing.T, hm *HeaderMetadata) {},
				expectErr: true,
				expectErrMsg: "failed to get header size: " +
					"unknown header type: 255",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				headersImport, cleanup, err := tc.prep()
				t.Cleanup(cleanup)
				require.NoError(t, err)

				metadata, err := headersImport.BlockHeadersImportSource.GetHeaderMetadata()
				if tc.expectErr {
					require.ErrorContains(
						t, err, tc.expectErrMsg,
					)
					tc.verify(t, metadata)
					return
				}
				require.NoError(t, err)
				tc.verify(t, metadata)
			})
		}
	})

	t.Run("Storage", func(t *testing.T) {
		t.Parallel()
		testCases := []struct {
			name         string
			chainType    wire.BitcoinNet
			headerType   headerfs.HeaderType
			startHeight  uint32
			prep         func() (headerfs.File, []byte, func(), error)
			verify       func(*testing.T, headerfs.File, []byte)
			expectErr    bool
			expectErrMsg string
		}{
			{
				name: "ErrorOnSourceFileNotExist",
				prep: func() (headerfs.File, []byte, func(),
					error) {
					return nil, nil, func() {}, nil
				},
				verify: func(*testing.T, headerfs.File,
					[]byte) {
				},
				expectErr:    true,
				expectErrMsg: "failed to open source file",
			},
			{
				name:        "PreservesOriginalFileContents",
				chainType:   wire.SimNet,
				headerType:  headerfs.Block,
				startHeight: 0,
				prep: func() (headerfs.File, []byte, func(),
					error) {

					// Create block headers file.
					bFile, c1, err := setupFileWithHdrs(
						headerfs.Block, false,
					)
					if err != nil {
						return nil, nil, c1, err
					}

					// Read the data before adding metadata
					// for later assertion.
					dataBefore, err := io.ReadAll(bFile)
					if err != nil {
						return nil, nil, c1, err
					}

					return bFile, dataBefore, c1, nil
				},
				verify: func(t *testing.T,
					srcFile headerfs.File, before []byte) {

					// Reopen the file to get an updated
					// file descriptor after adding header
					// metadata atomically.
					srcFile.Close()
					srcFile, err := os.OpenFile(
						srcFile.Name(), os.O_RDONLY,
						0644,
					)
					cleanup := func() {
						srcFile.Close()
						os.Remove(srcFile.Name())
					}
					t.Cleanup(cleanup)
					require.NoError(t, err)

					// Read all content of srcFile after
					// headers added.
					data, err := io.ReadAll(srcFile)
					require.NoError(t, err)

					// Compare dataBefore with file content.
					after := data[HeaderMetadataSize:]
					areEqual := bytes.Equal(after, before)
					require.True(t, areEqual)
				},
			},
			{
				name:        "AddsBlockHeaderMetadataToFile",
				chainType:   wire.TestNet3,
				headerType:  headerfs.Block,
				startHeight: 1,
				prep: func() (headerfs.File, []byte, func(),
					error) {

					// Create block headers file.
					bFile, c1, err := setupFileWithHdrs(
						headerfs.Block, false,
					)
					if err != nil {
						return nil, nil, c1, err
					}

					// Read the data before adding metadata
					// for later assertion.
					dataBefore, err := io.ReadAll(bFile)
					if err != nil {
						return nil, nil, c1, err
					}

					return bFile, dataBefore, c1, nil
				},
				verify: func(t *testing.T,
					srcFile headerfs.File, before []byte) {

					// Reopen the file to get an updated
					// file descriptor after adding header
					// metadata atomically.
					srcFile.Close()
					srcFile, err := os.OpenFile(
						srcFile.Name(), os.O_RDONLY,
						0644,
					)
					cleanup := func() {
						srcFile.Close()
						os.Remove(srcFile.Name())
					}
					t.Cleanup(cleanup)
					require.NoError(t, err)

					// Read all content of srcFile after
					// headers added.
					data, err := io.ReadAll(srcFile)
					require.NoError(t, err)

					// Assert individal metadata units.
					// Assert on the chain type.
					headerTypeOffset := BitcoinChainTypeSize
					startHeightOffset := BitcoinChainTypeSize + HeaderTypeSize
					bitcoinChainType := wire.BitcoinNet(
						binary.LittleEndian.Uint32(
							data[:BitcoinChainTypeSize],
						),
					)
					require.Equal(
						t, wire.TestNet3,
						bitcoinChainType,
					)

					// Assert on the header type.
					headerType := headerfs.HeaderType(
						data[headerTypeOffset],
					)
					require.Equal(
						t, headerfs.Block,
						headerType,
					)

					// Assert on the startHeight.
					startHeight := binary.LittleEndian.Uint32(
						data[startHeightOffset:HeaderMetadataSize],
					)
					require.Equal(
						t, uint32(1),
						startHeight,
					)

					// Compare dataBefore with file content.
					after := data[HeaderMetadataSize:]
					areEqual := bytes.Equal(after, before)
					require.True(t, areEqual)
				},
			},
			{
				name:        "AddsFilterHeaderMetadataToFile",
				chainType:   wire.TestNet4,
				headerType:  headerfs.RegularFilter,
				startHeight: 3,
				prep: func() (headerfs.File, []byte, func(),
					error) {
					// Create block headers file.
					bFile, c1, err := setupFileWithHdrs(
						headerfs.RegularFilter, false,
					)
					if err != nil {
						return nil, nil, c1, err
					}

					// Read the data before adding metadata
					// for later assertion.
					dataBefore, err := io.ReadAll(bFile)
					if err != nil {
						return nil, nil, c1, err
					}

					return bFile, dataBefore, c1, nil
				},
				verify: func(t *testing.T,
					srcFile headerfs.File, before []byte) {

					// Reopen the file to get an updated
					// file descriptor after adding header
					// metadata atomically.
					srcFile.Close()
					srcFile, err := os.OpenFile(
						srcFile.Name(), os.O_RDONLY,
						0644,
					)
					cleanup := func() {
						srcFile.Close()
						os.Remove(srcFile.Name())
					}
					t.Cleanup(cleanup)
					require.NoError(t, err)

					// Read all content of srcFile after
					// headers added.
					data, err := io.ReadAll(srcFile)
					require.NoError(t, err)

					// Assert individal metadata units.
					// Assert on the chain type.
					headerTypeOffset := BitcoinChainTypeSize
					startHeightOffset := BitcoinChainTypeSize + HeaderTypeSize
					bitcoinChainType := wire.BitcoinNet(
						binary.LittleEndian.Uint32(
							data[:BitcoinChainTypeSize],
						),
					)
					require.Equal(
						t, wire.TestNet4,
						bitcoinChainType,
					)

					// Assert on the header type.
					headerType := headerfs.HeaderType(
						data[headerTypeOffset],
					)
					require.Equal(
						t, headerfs.RegularFilter,
						headerType,
					)

					// Assert on the startHeight.
					startHeight := binary.LittleEndian.Uint32(
						data[startHeightOffset:HeaderMetadataSize],
					)
					require.Equal(
						t, uint32(3),
						startHeight,
					)

					// Compare dataBefore with file content.
					after := data[HeaderMetadataSize:]
					areEqual := bytes.Equal(after, before)
					require.True(t, areEqual)
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				srcFile, dataBefore, cleanup, err := tc.prep()
				t.Cleanup(cleanup)
				require.NoError(t, err)

				var srcFilePath string
				if srcFile != nil {
					srcFilePath = srcFile.Name()
				}

				err = AddHeadersImportMetadata(
					srcFilePath, tc.chainType,
					tc.headerType, tc.startHeight,
				)
				if tc.expectErr {
					require.ErrorContains(
						t, err, tc.expectErrMsg,
					)
					tc.verify(t, srcFile, dataBefore)
					return
				}
				require.NoError(t, err)
				tc.verify(t, srcFile, dataBefore)
			})
		}
	})
}

func TestHeader(t *testing.T) {
	t.Parallel()
	t.Run("Retrieval", func(t *testing.T) {
		t.Parallel()
		testCases := []struct {
			name       string
			index      int
			headerType headerfs.HeaderType
			// prep         func() (HeaderImportSource[HeaderBase, HeaderFactory[HeaderBase]], func(), error)
			verify       func(*testing.T, HeaderBase)
			expectErr    bool
			expectErrMsg string
		}{
			// {
			// 	name: "ErrorOnReaderNotInitialized",
			// 	prep: func() (HeaderImportSource[HeaderBase, HeaderFactory[HeaderBase]], func(), error) {
			// 		opts := &ImportOptions{}
			// 		blockSource := opts.createBlockHeadersImportSource()
			// 		blockImportSource, ok := blockSource.(HeaderImportSource[HeaderBase, HeaderFactory[HeaderBase]])
			// 		if !ok {
			// 			return nil, func() {}, errors.New("s")
			// 		}
			// 		return blockImportSource, func() {}, nil
			// 	},
			// 	verify:       func(*testing.T, HeaderBase) {},
			// 	expectErr:    true,
			// 	expectErrMsg: "file reader not initialized",
			// },
			// {
			// 	name: "ErrorOnHeaderMetadataNotInitialized",
			// 	prep: func() error {
			// 		return nil
			// 	},
			// 	expectErr:    true,
			// 	expectErrMsg: "header metadata not initialized",
			// },
			// {
			// 	name:       "ErrorOnHeaderIsOfUnknownType",
			// 	headerType: headerfs.UnknownHeader,
			// 	prep: func() error {

			// 		return nil
			// 	},
			// },
			// {
			// 	name: "ErrorOnHeaderIndexOutOfBounds",
			// },
			// {
			// 	name: "ErrorOnBlockHeaderDeserializeFail",
			// },
			// {
			// 	name: "ErrorOnFilterHeaderDeserializeFail",
			// },
			// {
			// 	name: "GetBlockHeaderSuccessfully",
			// },
			// {
			// 	name: "GetFilterHeaderSuccessfully",
			// },
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				importSource, cleanup, err := tc.prep()
				t.Cleanup(cleanup)
				require.NoError(t, err)

				header, err := importSource.GetHeader(tc.index)
				if tc.expectErr {
					require.ErrorContains(
						t, err, tc.expectErrMsg,
					)
					tc.verify(t, header)
					return
				}
				require.NoError(t, err)
				tc.verify(t, header)
			})
		}
	})

	t.Run("Processing", func(t *testing.T) {
		t.Parallel()
		testCases := []struct {
			name string
		}{}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {})
		}
	})

	t.Run("Storage", func(t *testing.T) {
		t.Parallel()
		testCases := []struct {
			name string
		}{}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {})
		}
	})

	t.Run("Validation", func(t *testing.T) {
		t.Parallel()
		testCases := []struct {
			name string
		}{}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {})
		}
	})
}

func TestImportAndTargetSourcesConstraintsSatisfaction(t *testing.T) {
	t.Parallel()
	t.Run("Compatibility", func(t *testing.T) {
		t.Parallel()
		testCases := []struct {
			name string
		}{}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {})
		}
	})

	t.Run("ChainContinuity", func(t *testing.T) {
		t.Parallel()
		testCases := []struct {
			name string
		}{}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {})
		}
	})

	t.Run("HeadersVerification", func(t *testing.T) {
		t.Parallel()
		testCases := []struct {
			name string
		}{}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {})
		}
	})
}

type mockBlockHeaderStore struct {
	mock.Mock
}

func (m *mockBlockHeaderStore) ChainTip() (*wire.BlockHeader, uint32, error) {
	args := m.Called()
	return args.Get(0).(*wire.BlockHeader),
		args.Get(1).(uint32),
		args.Error(2)
}

func (m *mockBlockHeaderStore) LatestBlockLocator() (blockchain.BlockLocator, error) {
	args := m.Called()
	return args.Get(0).(blockchain.BlockLocator), args.Error(1)
}

func (m *mockBlockHeaderStore) FetchHeaderByHeight(
	height uint32) (*wire.BlockHeader, error) {

	args := m.Called(height)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*wire.BlockHeader), args.Error(1)
}

func (m *mockBlockHeaderStore) FetchHeaderAncestors(numHeaders uint32,
	stopHash *chainhash.Hash) ([]wire.BlockHeader, uint32, error) {

	args := m.Called(numHeaders, stopHash)
	return args.Get(0).([]wire.BlockHeader),
		args.Get(1).(uint32),
		args.Error(2)
}

func (m *mockBlockHeaderStore) HeightFromHash(
	hash *chainhash.Hash) (uint32, error) {

	args := m.Called(hash)
	return args.Get(0).(uint32), args.Error(1)
}

func (m *mockBlockHeaderStore) FetchHeader(
	hash *chainhash.Hash) (*wire.BlockHeader, uint32, error) {

	args := m.Called(hash)
	if args.Get(0) == nil {
		return nil, args.Get(1).(uint32), args.Error(2)
	}
	return args.Get(0).(*wire.BlockHeader),
		args.Get(1).(uint32),
		args.Error(2)
}

func (m *mockBlockHeaderStore) WriteHeaders(
	hdrs ...headerfs.BlockHeader) error {

	args := m.Called(hdrs)
	return args.Error(0)
}

func (m *mockBlockHeaderStore) RollbackBlockHeaders(
	numHeaders uint32) (*headerfs.BlockStamp, error) {

	args := m.Called(numHeaders)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*headerfs.BlockStamp), args.Error(1)
}

func (m *mockBlockHeaderStore) RollbackLastBlock() (*headerfs.BlockStamp, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*headerfs.BlockStamp), args.Error(1)
}

type mockFilterHeaderStore struct {
	mock.Mock
}

func (m *mockFilterHeaderStore) ChainTip() (
	*chainhash.Hash, uint32, error) {

	args := m.Called()
	return args.Get(0).(*chainhash.Hash),
		args.Get(1).(uint32),
		args.Error(2)
}

func (m *mockFilterHeaderStore) FetchHeader(
	hash *chainhash.Hash) (*chainhash.Hash, error) {

	args := m.Called(hash)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*chainhash.Hash), args.Error(1)
}

func (m *mockFilterHeaderStore) FetchHeaderAncestors(numHeaders uint32,
	stopHash *chainhash.Hash) ([]chainhash.Hash, uint32, error) {

	args := m.Called(numHeaders, stopHash)
	return args.Get(0).([]chainhash.Hash),
		args.Get(1).(uint32),
		args.Error(2)
}

func (m *mockFilterHeaderStore) FetchHeaderByHeight(
	height uint32) (*chainhash.Hash, error) {

	args := m.Called(height)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*chainhash.Hash), args.Error(1)
}

func (m *mockFilterHeaderStore) WriteHeaders(
	hdrs ...headerfs.FilterHeader) error {

	args := m.Called(hdrs)
	return args.Error(0)
}

func (m *mockFilterHeaderStore) RollbackLastBlock(
	newTip *chainhash.Hash) (*headerfs.BlockStamp, error) {

	args := m.Called(newTip)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*headerfs.BlockStamp), args.Error(1)
}

func setupFileWithHdrs(hT headerfs.HeaderType,
	include_metadata bool) (headerfs.File, func(), error) {

	// Create a temporary file.
	fileName := fmt.Sprintf("test-%s-*", hT)
	tempFile, err := os.CreateTemp("", fileName)
	cleanup := func() {
		tempFile.Close()
		os.Remove(tempFile.Name())
	}
	if err != nil {
		return nil, cleanup, err
	}

	// Prep headers data to write based on the header type input.
	blockHdrs := []string{
		"010000000000000000000000000000000000000000000000000000000000" +
			"0000000000003ba3edfd7a7b12b27ac72c3e67768f617fc81bc3" +
			"888a51323a9fb8aa4b1e5e4a45068653ffff7f2002000000",
		"00000020f67ad7695d9b662a72ff3d8edbbb2de0bfa67b13974bb9910d11" +
			"6d5cbd863e68c552826d121f12fcb288895d9488d189891ce0a6" +
			"5a56193ea2ff3d4b99eabb875fac5a68ffff7f2003000000",
		"000000200582f786cda8187a3bb13c044a70f11a5f299cbdb55dd43744a2" +
			"de24cef76a72964688cc27da9f45261b8c35b00edea462f26469" +
			"67fcb6052063d0140a1275de60ac5a68ffff7f2001000000",
		"00000020f83e8ae2309315ff0a36646e2d43e7aa777b7aaa1eadb4876073" +
			"e7a8dac11c1dc3a5e71065b6ab83ed8972d277de2670ceed1fc4" +
			"3fd03f066cc84047d95eeaa360ac5a68ffff7f2002000000",
		"000000203513820c27ba7b218bb6732e851ef404986f299f44b4275334d5" +
			"eab0db09710835f6fc14632ebb23e141f680ae6aec6bdf76557b" +
			"46daf1b4c0160631d89e1ac461ac5a68ffff7f2000000000",
	}

	filterHdrs := []string{
		"b2ef0f5c5d790832d79fc9c9a7b3cef02dd94f143c63feba9d836248cad6" +
			"24cf",
		"b14a448b043b12401327695318318bbb53ec955e1e7963e3fd569a450448" +
			"9177",
		"75ae9eebc6e956fcb4fa00853aec5f252cf0046ed03587feece580386a6c" +
			"d113",
		"f99cbb96ca78c36c741b3765d78b22f0c1039add8afa6d2f6284b5cd6ab9" +
			"d8d6",
		"33e95706f9580a84e2cb167faf2239079805113cc7d3aaefff194b1ce6e6" +
			"a26c",
	}

	var hdrsData []string

	switch hT {
	case headerfs.Block:
		hdrsData = blockHdrs
	case headerfs.RegularFilter:
		hdrsData = filterHdrs
	default:
		return nil, cleanup, fmt.Errorf("%s", hT)
	}

	if include_metadata {
		// Utilize AddHeadersImportMetadata func part of the chainimport
		// package.
		err = AddHeadersImportMetadata(
			tempFile.Name(), wire.SimNet, hT, 0,
		)
		if err != nil {
			return nil, cleanup, err
		}
	}

	// Now append header data to the file. Since the
	// AddHeadersImportMetadata function closes the file we need to reopen
	// it again.
	tempFile, err = os.OpenFile(
		tempFile.Name(), os.O_RDWR|os.O_APPEND, 0644,
	)
	if err != nil {
		return nil, cleanup, fmt.Errorf("failed to open file for "+
			"writing headers: %w", err)
	}

	// Write each header as raw bytes.
	for _, hdrHex := range hdrsData {
		hdrBytes, err := hex.DecodeString(hdrHex)
		if err != nil {
			return nil, cleanup, fmt.Errorf("failed to decode "+
				"header hex: %w", err)
		}

		_, err = tempFile.Write(hdrBytes)
		if err != nil {
			return nil, cleanup, fmt.Errorf("failed to write "+
				"header data: %w", err)
		}
	}

	// Sync to ensure all data is written to disk.
	if err := tempFile.Sync(); err != nil {
		return nil, cleanup, fmt.Errorf("failed to sync file: %w", err)
	}

	// Reset file position to beginning for reading.
	if _, err := tempFile.Seek(0, 0); err != nil {
		return nil, cleanup, fmt.Errorf("failed to reset file "+
			"position: %w", err)
	}

	return tempFile, cleanup, nil
}
