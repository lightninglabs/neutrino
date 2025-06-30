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

// Block headers for unit testing.
var blockHdrs = []string{
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

// Filter headers for unit testing.
var filterHdrs = []string{
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
					require.ErrorContains(
						t, err,
						tc.expectBlockStoreErr.Error(),
					)
				}
				if tc.expectFilterStoreErr != nil {
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
		type Prep struct {
			hImport *HeadersImport
			cleanup func()
			err     error
		}
		type Verify struct {
			tc      *testing.T
			hImport *HeadersImport
		}
		testCases := []struct {
			name         string
			prep         func() Prep
			verify       func(Verify)
			expectErr    bool
			expectErrMsg string
		}{
			{
				name: "MissingBlockANDFilterHeaderImportSource",
				prep: func() Prep {
					opts := &ImportOptions{}
					headersImport := &HeadersImport{
						options:                   opts,
						BlockHeadersImportSource:  nil,
						FilterHeadersImportSource: nil,
					}
					return Prep{
						hImport: headersImport,
						cleanup: func() {},
						err:     nil,
					}
				},
				verify:       func(Verify) {},
				expectErr:    true,
				expectErrMsg: "missing required header sources",
			},
			{
				name: "MissingBlockHeaderImportSource",
				prep: func() Prep {
					opts := &ImportOptions{}
					bS := opts.createBlkHdrImportSrc()
					headersImport := &HeadersImport{
						options:                   opts,
						BlockHeadersImportSource:  bS,
						FilterHeadersImportSource: nil,
					}
					return Prep{
						hImport: headersImport,
						cleanup: func() {},
						err:     nil,
					}
				},
				verify:       func(Verify) {},
				expectErr:    true,
				expectErrMsg: "missing required header sources",
			},
			{
				name: "MissingFilterHeaderImportSource",
				prep: func() Prep {
					opts := &ImportOptions{}
					fS := opts.createFilterHdrImportSrc()
					headersImport := &HeadersImport{
						options:                   opts,
						BlockHeadersImportSource:  nil,
						FilterHeadersImportSource: fS,
					}
					return Prep{
						hImport: headersImport,
						cleanup: func() {},
						err:     nil,
					}
				},
				verify:       func(Verify) {},
				expectErr:    true,
				expectErrMsg: "missing required header sources",
			},
			{
				name: "MissingBlockANDFilterHeaderValidators",
				prep: func() Prep {
					opts := &ImportOptions{}
					bS := opts.createBlkHdrImportSrc()
					fS := opts.createFilterHdrImportSrc()
					headersImport := &HeadersImport{
						options:                   opts,
						BlockHeadersImportSource:  bS,
						FilterHeadersImportSource: fS,
						BlockHeadersValidator:     nil,
						FilterHeadersValidator:    nil,
					}
					return Prep{
						hImport: headersImport,
						cleanup: func() {},
						err:     nil,
					}
				},
				verify:    func(Verify) {},
				expectErr: true,
				expectErrMsg: "missing required header " +
					"validators",
			},
			{
				name: "MissingBlockHeaderValidator",
				prep: func() Prep {
					opts := &ImportOptions{}
					bS := opts.createBlkHdrImportSrc()
					fS := opts.createFilterHdrImportSrc()
					fV := opts.createFilterHdrValidator()
					headersImport := &HeadersImport{
						options:                   opts,
						BlockHeadersImportSource:  bS,
						FilterHeadersImportSource: fS,
						BlockHeadersValidator:     nil,
						FilterHeadersValidator:    fV,
					}
					return Prep{
						hImport: headersImport,
						cleanup: func() {},
						err:     nil,
					}
				},
				verify:    func(Verify) {},
				expectErr: true,
				expectErrMsg: "missing required header " +
					"validators",
			},
			{
				name: "MissingFilterHeaderValidator",
				prep: func() Prep {
					opts := &ImportOptions{}
					bS := opts.createBlkHdrImportSrc()
					fS := opts.createFilterHdrImportSrc()
					bV := opts.createBlkHdrValidator()
					headersImport := &HeadersImport{
						options:                   opts,
						BlockHeadersImportSource:  bS,
						FilterHeadersImportSource: fS,
						BlockHeadersValidator:     bV,
						FilterHeadersValidator:    nil,
					}
					return Prep{
						hImport: headersImport,
						cleanup: func() {},
						err:     nil,
					}
				},
				verify:    func(Verify) {},
				expectErr: true,
				expectErrMsg: "missing required header " +
					"validators",
			},
			{
				name: "ErrorOnBlockFileNotExist",
				prep: func() Prep {
					opts := &ImportOptions{}
					bS := opts.createBlkHdrImportSrc()
					fS := opts.createFilterHdrImportSrc()
					bV := opts.createBlkHdrValidator()
					fV := opts.createFilterHdrValidator()
					filePath := "/path/to/nonexistent/file"
					bS.SetPath(filePath)
					headersImport := &HeadersImport{
						options:                   opts,
						BlockHeadersImportSource:  bS,
						FilterHeadersImportSource: fS,
						BlockHeadersValidator:     bV,
						FilterHeadersValidator:    fV,
					}
					return Prep{
						hImport: headersImport,
						cleanup: func() {},
						err:     nil,
					}
				},
				verify:    func(Verify) {},
				expectErr: true,
				expectErrMsg: "failed to mmap file: open " +
					"/path/to/nonexistent/file",
			},
			{
				name: "ErrorOnFilterFileNotExist",
				prep: func() Prep {
					// Create block headers file.
					bFile, c1, err := setupFileWithHdrs(
						headerfs.Block, true,
					)
					if err != nil {
						return Prep{
							hImport: nil,
							cleanup: c1,
							err:     err,
						}
					}

					// Configure import options.
					opts := &ImportOptions{}
					bS := opts.createBlkHdrImportSrc()
					fS := opts.createFilterHdrImportSrc()
					bV := opts.createBlkHdrValidator()
					fV := opts.createFilterHdrValidator()

					bS.SetPath(bFile.Name())

					filePath := "/path/to/nonexistent/file"
					bS.SetPath(filePath)

					headersImport := &HeadersImport{
						options:                   opts,
						BlockHeadersImportSource:  bS,
						FilterHeadersImportSource: fS,
						BlockHeadersValidator:     bV,
						FilterHeadersValidator:    fV,
					}
					return Prep{
						hImport: headersImport,
						cleanup: c1,
						err:     nil,
					}
				},
				verify:    func(Verify) {},
				expectErr: true,
				expectErrMsg: "failed to mmap file: open " +
					"/path/to/nonexistent/file",
			},
			{
				name: "ErrorOnGetBlockHeaderMetadataFail",
				prep: func() Prep {
					// Create block headers empty file.
					blockFile, err := os.CreateTemp(
						"", "empty-block-header-*",
					)
					cleanup := func() {
						blockFile.Close()
						os.Remove(blockFile.Name())
					}
					if err != nil {
						return Prep{
							hImport: nil,
							cleanup: cleanup,
							err:     err,
						}
					}

					// Configure import options.
					opts := &ImportOptions{}
					bS := opts.createBlkHdrImportSrc()
					fS := opts.createFilterHdrImportSrc()
					bV := opts.createBlkHdrValidator()
					fV := opts.createFilterHdrValidator()

					bS.SetPath(blockFile.Name())

					headersImport := &HeadersImport{
						options:                   opts,
						BlockHeadersImportSource:  bS,
						FilterHeadersImportSource: fS,
						BlockHeadersValidator:     bV,
						FilterHeadersValidator:    fV,
					}
					return Prep{
						hImport: headersImport,
						cleanup: cleanup,
						err:     nil,
					}
				},
				verify:       func(Verify) {},
				expectErr:    true,
				expectErrMsg: "failed to read metadata: EOF",
			},
			{
				name: "ErrorOnGetFilterHeaderMetadataFail",
				prep: func() Prep {
					// Create block headers file.
					bFile, c1, err := setupFileWithHdrs(
						headerfs.Block, true,
					)
					if err != nil {
						return Prep{
							hImport: nil,
							cleanup: c1,
							err:     err,
						}
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
						return Prep{
							hImport: nil,
							cleanup: cleanup,
							err:     err,
						}
					}

					// Configure import options.
					opts := &ImportOptions{}
					bS := opts.createBlkHdrImportSrc()
					fS := opts.createFilterHdrImportSrc()
					bV := opts.createBlkHdrValidator()
					fV := opts.createFilterHdrValidator()

					bS.SetPath(bFile.Name())
					fS.SetPath(fFile.Name())

					headersImport := &HeadersImport{
						options:                   opts,
						BlockHeadersImportSource:  bS,
						FilterHeadersImportSource: fS,
						BlockHeadersValidator:     bV,
						FilterHeadersValidator:    fV,
					}
					return Prep{
						hImport: headersImport,
						cleanup: cleanup,
						err:     nil,
					}
				},
				verify:       func(Verify) {},
				expectErr:    true,
				expectErrMsg: "failed to read metadata: EOF",
			},
			{
				name: "OpenSourcesCorrectly",
				prep: func() Prep {
					// Create block headers file.
					bFile, c1, err := setupFileWithHdrs(
						headerfs.Block, true,
					)
					if err != nil {
						return Prep{
							hImport: nil,
							cleanup: c1,
							err:     err,
						}
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
						return Prep{
							hImport: nil,
							cleanup: cleanup,
							err:     err,
						}
					}

					// Configure import options.
					opts := &ImportOptions{}
					bS := opts.createBlkHdrImportSrc()
					fS := opts.createFilterHdrImportSrc()
					bV := opts.createBlkHdrValidator()
					fV := opts.createFilterHdrValidator()

					bS.SetPath(bFile.Name())
					fS.SetPath(fFile.Name())

					headersImport := &HeadersImport{
						options:                   opts,
						BlockHeadersImportSource:  bS,
						FilterHeadersImportSource: fS,
						BlockHeadersValidator:     bV,
						FilterHeadersValidator:    fV,
					}
					return Prep{
						hImport: headersImport,
						cleanup: cleanup,
						err:     nil,
					}
				},
				verify: func(v Verify) {
					// Prep block and filter hdrs metadata.
					bHdrType := headerfs.Block
					expectBlockMetadata := &HeaderMetadata{
						BitcoinChainType: wire.SimNet,
						HeaderType:       bHdrType,
						StartHeight:      0,
						EndHeight:        4,
						HeadersCount:     5,
					}

					fHdrType := headerfs.RegularFilter
					expectFilterMetadata := &HeaderMetadata{
						BitcoinChainType: wire.SimNet,
						HeaderType:       fHdrType,
						StartHeight:      0,
						EndHeight:        4,
						HeadersCount:     5,
					}

					// Verify block header metadata.
					bS := v.hImport.BlockHeadersImportSource
					metadata, err := bS.GetHeaderMetadata()
					require.NoError(v.tc, err)
					require.Equal(
						v.tc, expectBlockMetadata,
						metadata,
					)

					// Verify filter header metadata.
					fS := v.hImport.FilterHeadersImportSource
					metadata, err = fS.GetHeaderMetadata()
					require.NoError(v.tc, err)
					require.Equal(
						v.tc, expectFilterMetadata,
						metadata,
					)
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				prep := tc.prep()
				t.Cleanup(prep.cleanup)
				require.NoError(t, prep.err)

				err := prep.hImport.openSources()
				verify := Verify{
					tc:      t,
					hImport: prep.hImport,
				}
				if tc.expectErr {
					require.ErrorContains(
						t, err, tc.expectErrMsg,
					)
					tc.verify(verify)
					return
				}
				require.NoError(t, err)
				tc.verify(verify)
			})
		}
	})
}

func TestHeaderMetadata(t *testing.T) {
	t.Parallel()
	t.Run("Retrieval", func(t *testing.T) {
		t.Parallel()
		type Prep struct {
			hImport *HeadersImport
			cleanup func()
			err     error
		}
		type Verify struct {
			tc        *testing.T
			hMetadata *HeaderMetadata
		}
		testCases := []struct {
			name         string
			prep         func() Prep
			verify       func(Verify)
			expectErr    bool
			expectErrMsg string
		}{
			{
				name: "ReturnsCachedMetadataWhenAvailable",
				prep: func() Prep {
					// Create block headers file.
					bFile, c1, err := setupFileWithHdrs(
						headerfs.Block, true,
					)
					if err != nil {
						return Prep{
							hImport: nil,
							cleanup: c1,
							err:     err,
						}
					}

					// Configure import options.
					opts := &ImportOptions{}
					bS := opts.createBlkHdrImportSrc()
					bS.SetPath(bFile.Name())

					// Force a cache miss by opening the
					// source file for the first time.
					err = bS.Open()
					c2 := func() {
						bS.Close()
						os.Remove(bS.GetPath())
						c1()
					}
					if err != nil {
						return Prep{
							hImport: nil,
							cleanup: c2,
							err:     err,
						}
					}

					// Remove the source file to ensure next
					// call must use cached data.
					err = bS.Close()
					if err != nil {
						return Prep{
							hImport: nil,
							cleanup: c2,
							err:     err,
						}
					}

					headersImport := &HeadersImport{
						options:                  opts,
						BlockHeadersImportSource: bS,
					}

					return Prep{
						hImport: headersImport,
						cleanup: c1,
						err:     nil,
					}
				},
				verify: func(v Verify) {
					// Next call should result in a cache
					// hit since the file is gone.
					expectBlockMetadata := &HeaderMetadata{
						BitcoinChainType: wire.SimNet,
						HeaderType:       headerfs.Block,
						StartHeight:      0,
						EndHeight:        4,
						HeadersCount:     5,
					}
					require.Equal(
						v.tc, expectBlockMetadata,
						v.hMetadata,
					)
				},
			},
			{
				name: "ErrorOnReaderNotInitialized",
				prep: func() Prep {
					// Create block headers file.
					bF, cleanup, err := setupFileWithHdrs(
						headerfs.Block, true,
					)
					if err != nil {
						return Prep{
							hImport: nil,
							cleanup: cleanup,
							err:     err,
						}
					}

					// Configure import options.
					opts := &ImportOptions{}
					bS := opts.createBlkHdrImportSrc()
					bS.SetPath(bF.Name())

					headersImport := &HeadersImport{
						options:                  opts,
						BlockHeadersImportSource: bS,
					}

					return Prep{
						hImport: headersImport,
						cleanup: cleanup,
						err:     nil,
					}
				},
				verify:       func(Verify) {},
				expectErr:    true,
				expectErrMsg: "file reader not initialized",
			},
			{
				name: "ErrorOnHeaderReadFails",
				prep: func() Prep {
					// Create block headers empty file.
					bFile, err := os.CreateTemp(
						"", "invalid-block-header-*",
					)
					c1 := func() {
						bFile.Close()
						os.Remove(bFile.Name())
					}
					if err != nil {
						return Prep{
							hImport: nil,
							cleanup: c1,
							err:     err,
						}
					}

					// Add header metadata to the file.
					err = AddHeadersImportMetadata(
						bFile.Name(), wire.SimNet,
						headerfs.Block, 0,
					)
					if err != nil {
						return Prep{
							hImport: nil,
							cleanup: c1,
							err:     err,
						}
					}

					// Reopen the file to get an updated
					// file descriptor.
					bFile.Close()
					bFile, err = os.OpenFile(
						bFile.Name(), os.O_RDWR, 0644,
					)
					c1 = func() {
						bFile.Close()
						os.Remove(bFile.Name())
					}
					if err != nil {
						return Prep{
							hImport: nil,
							cleanup: c1,
							err:     err,
						}
					}

					// Configure import options.
					opts := &ImportOptions{}
					bS := opts.createBlkHdrImportSrc()
					bS.SetPath(bFile.Name())

					// Remove the last byte of header
					// metadata to simulate EOF.
					fileInfo, err := bFile.Stat()
					if err != nil {
						return Prep{
							hImport: nil,
							cleanup: c1,
							err:     err,
						}
					}
					fileSize := fileInfo.Size()
					if fileSize == 0 {
						err := fmt.Errorf("empty "+
							"file: %s",
							bFile.Name())
						return Prep{
							hImport: nil,
							cleanup: c1,
							err:     err,
						}
					}
					err = bFile.Truncate(fileSize - 1)
					if err != nil {
						return Prep{
							hImport: nil,
							cleanup: c1,
							err:     err,
						}
					}
					err = bFile.Sync()
					if err != nil {
						return Prep{
							hImport: nil,
							cleanup: c1,
							err:     err,
						}
					}

					// Convert to file header import source.
					bFS, ok := bS.(*FileHeaderImportSource)
					require.True(t, ok)

					// Set the internal reader.
					reader, err := mmap.Open(bFile.Name())
					cleanup := func() {
						reader.Close()
						os.Remove(bFile.Name())
					}
					if err != nil {
						return Prep{
							hImport: nil,
							cleanup: cleanup,
							err:     err,
						}
					}
					bFS.reader = reader

					// Make sure the metadata is empty.
					bFS.metadata = nil

					headersImport := &HeadersImport{
						options:                  opts,
						BlockHeadersImportSource: bFS,
					}

					return Prep{
						hImport: headersImport,
						cleanup: cleanup,
						err:     nil,
					}
				},
				verify:       func(Verify) {},
				expectErr:    true,
				expectErrMsg: "failed to read metadata: EOF",
			},
			{
				name: "ErrorOnUnknownHeaderType",
				prep: func() Prep {
					// Create block headers empty file.
					bFile, err := os.CreateTemp(
						"", "invalid-block-header-*",
					)
					c1 := func() {
						bFile.Close()
						os.Remove(bFile.Name())
					}
					if err != nil {
						return Prep{
							hImport: nil,
							cleanup: c1,
							err:     err,
						}
					}

					// Add header metadata to the file.
					err = AddHeadersImportMetadata(
						bFile.Name(), wire.SimNet,
						headerfs.UnknownHeader, 0,
					)
					if err != nil {
						return Prep{
							hImport: nil,
							cleanup: c1,
							err:     err,
						}
					}

					// Reopen the file to get an updated
					// file descriptor.
					bFile.Close()
					bFile, err = os.OpenFile(
						bFile.Name(), os.O_RDWR, 0644,
					)
					c1 = func() {
						bFile.Close()
						os.Remove(bFile.Name())
					}
					if err != nil {
						return Prep{
							hImport: nil,
							cleanup: c1,
							err:     err,
						}
					}

					// Configure import options.
					opts := &ImportOptions{}
					bs := opts.createBlkHdrImportSrc()
					bs.SetPath(bFile.Name())

					// Convert to file header import source.
					bFS, ok := bs.(*FileHeaderImportSource)
					require.True(t, ok)

					// Set the internal reader.
					reader, err := mmap.Open(bFile.Name())
					cleanup := func() {
						reader.Close()
						os.Remove(bFile.Name())
					}
					if err != nil {
						return Prep{
							hImport: nil,
							cleanup: cleanup,
							err:     err,
						}
					}
					bFS.reader = reader

					// Make sure the metadata is empty.
					bFS.metadata = nil

					headersImport := &HeadersImport{
						options:                  opts,
						BlockHeadersImportSource: bFS,
					}

					return Prep{
						hImport: headersImport,
						cleanup: cleanup,
						err:     nil,
					}
				},
				verify:    func(Verify) {},
				expectErr: true,
				expectErrMsg: "failed to get header size: " +
					"unknown header type: 255",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				prep := tc.prep()
				t.Cleanup(prep.cleanup)
				require.NoError(t, prep.err)

				bS := prep.hImport.BlockHeadersImportSource
				metadata, err := bS.GetHeaderMetadata()
				verify := Verify{
					tc:        t,
					hMetadata: metadata,
				}
				if tc.expectErr {
					require.ErrorContains(
						t, err, tc.expectErrMsg,
					)
					tc.verify(verify)
					return
				}
				require.NoError(t, err)
				tc.verify(verify)
			})
		}
	})

	t.Run("Storage", func(t *testing.T) {
		t.Parallel()
		type Prep struct {
			file    headerfs.File
			data    []byte
			cleanup func()
			err     error
		}
		type Verify struct {
			tc   *testing.T
			file headerfs.File
			data []byte
		}
		testCases := []struct {
			name         string
			chainType    wire.BitcoinNet
			headerType   headerfs.HeaderType
			startHeight  uint32
			prep         func() Prep
			verify       func(Verify)
			expectErr    bool
			expectErrMsg string
		}{
			{
				name: "ErrorOnSourceFileNotExist",
				prep: func() Prep {
					return Prep{
						file:    nil,
						data:    nil,
						cleanup: func() {},
						err:     nil,
					}
				},
				verify:       func(Verify) {},
				expectErr:    true,
				expectErrMsg: "failed to open source file",
			},
			{
				name:        "PreservesOriginalFileContents",
				chainType:   wire.SimNet,
				headerType:  headerfs.Block,
				startHeight: 0,
				prep: func() Prep {
					// Create block headers file.
					bFile, c1, err := setupFileWithHdrs(
						headerfs.Block, false,
					)
					if err != nil {
						return Prep{
							file:    nil,
							data:    nil,
							cleanup: c1,
							err:     err,
						}
					}

					// Read the data before adding metadata
					// for later assertion.
					dataBefore, err := io.ReadAll(bFile)
					if err != nil {
						return Prep{
							file:    nil,
							data:    nil,
							cleanup: c1,
							err:     err,
						}
					}

					return Prep{
						file:    bFile,
						data:    dataBefore,
						cleanup: c1,
						err:     nil,
					}
				},
				verify: func(v Verify) {
					// Reopen the file to get an updated
					// file descriptor after adding header
					// metadata atomically.
					v.file.Close()
					srcFile, err := os.OpenFile(
						v.file.Name(), os.O_RDONLY,
						0644,
					)
					cleanup := func() {
						srcFile.Close()
						os.Remove(srcFile.Name())
					}
					v.tc.Cleanup(cleanup)
					require.NoError(v.tc, err)

					// Read all content of srcFile after
					// headers added.
					data, err := io.ReadAll(srcFile)
					require.NoError(v.tc, err)

					// Compare dataBefore with file content.
					after := data[HeaderMetadataSize:]
					areEqual := bytes.Equal(after, v.data)
					require.True(v.tc, areEqual)
				},
			},
			{
				name:        "AddsBlockHeaderMetadataToFile",
				chainType:   wire.TestNet3,
				headerType:  headerfs.Block,
				startHeight: 1,
				prep: func() Prep {
					// Create block headers file.
					bFile, c1, err := setupFileWithHdrs(
						headerfs.Block, false,
					)
					if err != nil {
						return Prep{
							file:    nil,
							data:    nil,
							cleanup: c1,
							err:     err,
						}
					}

					// Read the data before adding metadata
					// for later assertion.
					dataBefore, err := io.ReadAll(bFile)
					if err != nil {
						return Prep{
							file:    nil,
							data:    nil,
							cleanup: c1,
							err:     err,
						}
					}

					return Prep{
						file:    bFile,
						data:    dataBefore,
						cleanup: c1,
						err:     nil,
					}
				},
				verify: func(v Verify) {

					// Reopen the file to get an updated
					// file descriptor after adding header
					// metadata atomically.
					v.file.Close()
					srcFile, err := os.OpenFile(
						v.file.Name(), os.O_RDONLY,
						0644,
					)
					cleanup := func() {
						srcFile.Close()
						os.Remove(srcFile.Name())
					}
					v.tc.Cleanup(cleanup)
					require.NoError(v.tc, err)

					// Read all content of srcFile after
					// headers added.
					data, err := io.ReadAll(srcFile)
					require.NoError(v.tc, err)

					// Assert individal metadata units.
					// Assert on the chain type.
					headerTypeOffset := BitcoinChainTypeSize
					sHeightOffset := headerTypeOffset
					sHeightOffset += HeaderTypeSize
					btcChainType := wire.BitcoinNet(
						binary.LittleEndian.Uint32(
							data[:headerTypeOffset],
						),
					)
					require.Equal(
						v.tc, wire.TestNet3,
						btcChainType,
					)

					// Assert on the header type.
					headerType := headerfs.HeaderType(
						data[headerTypeOffset],
					)
					require.Equal(
						v.tc, headerfs.Block,
						headerType,
					)

					// Assert on the startHeight.
					hMS := HeaderMetadataSize
					sHeightD := data[sHeightOffset:hMS]
					sHeight := binary.LittleEndian.Uint32(
						sHeightD,
					)
					require.Equal(v.tc, uint32(1), sHeight)

					// Compare dataBefore with file content.
					after := data[hMS:]
					areEqual := bytes.Equal(after, v.data)
					require.True(v.tc, areEqual)
				},
			},
			{
				name:        "AddsFilterHeaderMetadataToFile",
				chainType:   wire.TestNet4,
				headerType:  headerfs.RegularFilter,
				startHeight: 3,
				prep: func() Prep {
					// Create block headers file.
					bFile, c1, err := setupFileWithHdrs(
						headerfs.RegularFilter, false,
					)
					if err != nil {
						return Prep{
							file:    nil,
							data:    nil,
							cleanup: c1,
							err:     err,
						}
					}

					// Read the data before adding metadata
					// for later assertion.
					dataBefore, err := io.ReadAll(bFile)
					if err != nil {
						return Prep{
							file:    nil,
							data:    nil,
							cleanup: c1,
							err:     err,
						}
					}

					return Prep{
						file:    bFile,
						data:    dataBefore,
						cleanup: c1,
						err:     nil,
					}
				},
				verify: func(v Verify) {
					// Reopen the file to get an updated
					// file descriptor after adding header
					// metadata atomically.
					v.file.Close()
					srcFile, err := os.OpenFile(
						v.file.Name(), os.O_RDONLY,
						0644,
					)
					cleanup := func() {
						srcFile.Close()
						os.Remove(srcFile.Name())
					}
					v.tc.Cleanup(cleanup)
					require.NoError(v.tc, err)

					// Read all content of srcFile after
					// headers added.
					data, err := io.ReadAll(srcFile)
					require.NoError(v.tc, err)

					// Assert individal metadata units.
					// Assert on the chain type.
					headerTypeOffset := BitcoinChainTypeSize
					sHeightOffset := headerTypeOffset
					sHeightOffset += HeaderTypeSize
					bitcoinChainType := wire.BitcoinNet(
						binary.LittleEndian.Uint32(
							data[:headerTypeOffset],
						),
					)
					require.Equal(
						v.tc, wire.TestNet4,
						bitcoinChainType,
					)

					// Assert on the header type.
					headerType := headerfs.HeaderType(
						data[headerTypeOffset],
					)
					require.Equal(
						v.tc, headerfs.RegularFilter,
						headerType,
					)

					// Assert on the startHeight.
					hMS := HeaderMetadataSize
					sHeightD := data[sHeightOffset:hMS]
					sHeight := binary.LittleEndian.Uint32(
						sHeightD,
					)
					require.Equal(v.tc, uint32(3), sHeight)

					// Compare dataBefore with file content.
					after := data[HeaderMetadataSize:]
					areEqual := bytes.Equal(after, v.data)
					require.True(v.tc, areEqual)
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				prep := tc.prep()
				t.Cleanup(prep.cleanup)
				require.NoError(t, prep.err)

				var srcFilePath string
				if prep.file != nil {
					srcFilePath = prep.file.Name()
				}

				err := AddHeadersImportMetadata(
					srcFilePath, tc.chainType,
					tc.headerType, tc.startHeight,
				)
				verify := Verify{
					tc:   t,
					file: prep.file,
					data: prep.data,
				}
				if tc.expectErr {
					require.ErrorContains(
						t, err, tc.expectErrMsg,
					)
					tc.verify(verify)
					return
				}
				require.NoError(t, err)
				tc.verify(verify)
			})
		}
	})
}

func TestHeader(t *testing.T) {
	t.Parallel()
	t.Run("Retrieval", func(t *testing.T) {
		t.Parallel()
		type Prep struct {
			hISource   HeaderImportSource
			headerType headerfs.HeaderType
			cleanup    func()
			err        error
		}
		type Verify struct {
			tc     *testing.T
			header Header
			index  int
		}
		testCases := []struct {
			name         string
			index        int
			hType        headerfs.HeaderType
			prep         func(headerfs.HeaderType) Prep
			verify       func(Verify)
			expectErr    bool
			expectErrMsg string
		}{
			{
				name:  "ErrorOnReaderNotInitialized",
				hType: headerfs.Block,
				prep: func(hType headerfs.HeaderType) Prep {
					opts := &ImportOptions{}
					bS := opts.createBlkHdrImportSrc()
					return Prep{
						hISource:   bS,
						headerType: hType,
						cleanup:    func() {},
						err:        nil,
					}
				},
				verify:       func(Verify) {},
				expectErr:    true,
				expectErrMsg: "file reader not initialized",
			},
			{
				name:  "ErrorOnHeaderMetadataNotInitialized",
				hType: headerfs.Block,
				prep: func(hType headerfs.HeaderType) Prep {
					// Create block headers file.
					bFile, c1, err := setupFileWithHdrs(
						headerfs.Block, true,
					)
					if err != nil {
						return Prep{
							hISource: nil,
							cleanup:  c1,
							err:      err,
						}
					}

					// Configure import options.
					opts := &ImportOptions{}
					bS := opts.createBlkHdrImportSrc()
					bS.SetPath(bFile.Name())

					// Convert to file header import source.
					bFS, ok := bS.(*FileHeaderImportSource)
					require.True(t, ok)

					// Set the internal reader.
					reader, err := mmap.Open(bFile.Name())
					cleanup := func() {
						reader.Close()
						os.Remove(bFile.Name())
					}
					if err != nil {
						return Prep{
							hISource:   nil,
							headerType: hType,
							cleanup:    cleanup,
							err:        err,
						}
					}
					bFS.reader = reader

					// Illustrate that the metdata is not
					// initialized.
					bFS.metadata = nil

					return Prep{
						hISource:   bFS,
						headerType: hType,
						cleanup:    cleanup,
						err:        nil,
					}
				},
				verify:       func(Verify) {},
				expectErr:    true,
				expectErrMsg: "header metadata not initialized",
			},
			{
				name:  "ErrorOnHeaderIsOfUnknownType",
				hType: headerfs.UnknownHeader,
				prep: func(hType headerfs.HeaderType) Prep {
					// Create block headers file.
					bFile, c1, err := setupFileWithHdrs(
						headerfs.Block, true,
					)
					if err != nil {
						return Prep{
							hISource: nil,
							cleanup:  c1,
							err:      err,
						}
					}

					// Configure import options.
					opts := &ImportOptions{}
					bS := opts.createBlkHdrImportSrc()
					bS.SetPath(bFile.Name())

					// Convert to file header import source.
					bFS, ok := bS.(*FileHeaderImportSource)
					require.True(t, ok)

					// Set the internal reader.
					reader, err := mmap.Open(bFile.Name())
					cleanup := func() {
						reader.Close()
						os.Remove(bFile.Name())
					}
					if err != nil {
						return Prep{
							hISource:   nil,
							headerType: hType,
							cleanup:    cleanup,
							err:        err,
						}
					}
					bFS.reader = reader

					// Set header metadata.
					bFS.metadata = &HeaderMetadata{}
					bFS.metadata.HeaderType = hType

					return Prep{
						hISource:   bFS,
						headerType: hType,
						cleanup:    cleanup,
						err:        nil,
					}
				},
				verify:    func(v Verify) {},
				expectErr: true,
				expectErrMsg: "failed to get header size: " +
					"unknown header type: 255",
			},
			{
				name:  "ErrorOnHeaderIndexOutOfBounds",
				hType: headerfs.Block,
				index: 101,
				prep: func(hType headerfs.HeaderType) Prep {
					// Create block headers file.
					bFile, c1, err := setupFileWithHdrs(
						hType, true,
					)
					if err != nil {
						return Prep{
							hISource: nil,
							cleanup:  c1,
							err:      err,
						}
					}

					// Configure import options.
					opts := &ImportOptions{}
					bS := opts.createBlkHdrImportSrc()
					bS.SetPath(bFile.Name())

					err = bS.Open()
					cleanup := func() {
						bS.Close()
						os.Remove(bFile.Name())
					}
					if err != nil {
						return Prep{
							hISource:   nil,
							headerType: hType,
							cleanup:    cleanup,
							err:        err,
						}
					}

					return Prep{
						hISource:   bS,
						headerType: hType,
						cleanup:    cleanup,
						err:        nil,
					}
				},
				verify:    func(Verify) {},
				expectErr: true,
				expectErrMsg: "failed to read header at " +
					"index 101",
			},
			{
				name:  "GetBlockHeaderSuccessfully",
				hType: headerfs.Block,
				index: 3,
				prep: func(hType headerfs.HeaderType) Prep {
					// Create block headers file.
					bFile, c1, err := setupFileWithHdrs(
						hType, true,
					)
					if err != nil {
						return Prep{
							hISource: nil,
							cleanup:  c1,
							err:      err,
						}
					}

					// Configure import options.
					opts := &ImportOptions{}
					bS := opts.createBlkHdrImportSrc()
					bS.SetPath(bFile.Name())

					err = bS.Open()
					cleanup := func() {
						bS.Close()
						os.Remove(bFile.Name())
					}
					if err != nil {
						return Prep{
							hISource:   nil,
							headerType: hType,
							cleanup:    cleanup,
							err:        err,
						}
					}

					return Prep{
						hISource:   bS,
						headerType: hType,
						cleanup:    cleanup,
						err:        nil,
					}
				},
				verify: func(v Verify) {
					// Assert it is of block header type.
					bH, ok := v.header.(*BlockHeader)
					require.True(v.tc, ok)

					// Construct the expected block header.
					buff, err := hex.DecodeString(
						blockHdrs[v.index],
					)
					require.NoError(t, err)
					reader := bytes.NewReader(buff)
					bHExpected := NewBlockHeader()
					bHExpected.Deserialize(
						reader, uint32(v.index),
					)

					// Assert that the known block header at
					// this index matches the retrieved one.
					require.Equal(v.tc, bHExpected, bH)
				},
			},
			{
				name:  "GetFilterHeaderSuccessfully",
				hType: headerfs.RegularFilter,
				index: 3,
				prep: func(hType headerfs.HeaderType) Prep {
					// Create filter headers file.
					fFile, c1, err := setupFileWithHdrs(
						hType, true,
					)
					if err != nil {
						return Prep{
							hISource: nil,
							cleanup:  c1,
							err:      err,
						}
					}

					// Configure import options.
					opts := &ImportOptions{}
					fS := opts.createFilterHdrImportSrc()
					fS.SetPath(fFile.Name())

					err = fS.Open()
					cleanup := func() {
						fS.Close()
						os.Remove(fFile.Name())
					}
					if err != nil {
						return Prep{
							hISource:   nil,
							headerType: hType,
							cleanup:    cleanup,
							err:        err,
						}
					}

					return Prep{
						hISource:   fS,
						headerType: hType,
						cleanup:    cleanup,
						err:        nil,
					}
				},
				verify: func(v Verify) {
					// Assert it is of filter header type.
					fH, ok := v.header.(*FilterHeader)
					require.True(v.tc, ok)

					// Construct the expected filter header.
					buff, err := hex.DecodeString(
						filterHdrs[v.index],
					)
					require.NoError(t, err)
					reader := bytes.NewReader(buff)
					fHExpected := NewFilterHeader()
					fHExpected.Deserialize(
						reader, uint32(v.index),
					)

					// Assert that the known filter header
					// at this index matches the retrieved
					// one.
					require.Equal(v.tc, fHExpected, fH)
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				prep := tc.prep(tc.hType)
				t.Cleanup(prep.cleanup)
				require.NoError(t, prep.err)

				header, err := prep.hISource.GetHeader(tc.index)
				verify := Verify{
					tc:     t,
					header: header,
					index:  tc.index,
				}
				if tc.expectErr {
					require.ErrorContains(
						t, err, tc.expectErrMsg,
					)
					tc.verify(verify)
					return
				}
				require.NoError(t, err)
				tc.verify(verify)
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

		// We ne to reopen it again to update the file descriptor since
		// AddHeadersImportMetadata closes the file for more atomic op.
		tempFile, err = os.OpenFile(
			tempFile.Name(), os.O_RDWR|os.O_APPEND, 0644,
		)
		if err != nil {
			return nil, cleanup, fmt.Errorf("failed to open file for "+
				"writing headers: %w", err)
		}
	}

	// Now append header data to the file. Write each header as raw bytes.
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
