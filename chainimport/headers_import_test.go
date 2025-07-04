package chainimport

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcwallet/walletdb"
	_ "github.com/btcsuite/btcwallet/walletdb/bdb"
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
					cleanup = func() {
						headersImport.closeSources()
					}
					return Prep{
						hImport: headersImport,
						cleanup: cleanup,
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
					cleanup = func() {
						headersImport.closeSources()
					}
					return Prep{
						hImport: headersImport,
						cleanup: cleanup,
					}
				},
				verify: func(v Verify) {
					// Prep block and filter hdrs metadata.
					bHdrType := headerfs.Block
					expectBlockMetadata := &HeaderMetadata{
						BitcoinChainType: wire.SimNet,
						HeaderType:       bHdrType,
						HeaderSize:       80,
						StartHeight:      0,
						EndHeight:        4,
						HeadersCount:     5,
					}

					fHdrType := headerfs.RegularFilter
					expectFilterMetadata := &HeaderMetadata{
						BitcoinChainType: wire.SimNet,
						HeaderType:       fHdrType,
						HeaderSize:       32,
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
					f := v.hImport.FilterHeadersImportSource
					metadata, err = f.GetHeaderMetadata()
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
				name: "ErrorOnReaderNotInitialized",
				prep: func() Prep {
					// Create block headers file.
					bF, cleanup, err := setupFileWithHdrs(
						headerfs.Block, true,
					)
					if err != nil {
						return Prep{
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
							cleanup: c1,
							err:     err,
						}
					}
					err = bFile.Truncate(fileSize - 1)
					if err != nil {
						return Prep{
							cleanup: c1,
							err:     err,
						}
					}
					err = bFile.Sync()
					if err != nil {
						return Prep{
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
					}
				},
				verify:    func(Verify) {},
				expectErr: true,
				expectErrMsg: "failed to get header size: " +
					"unknown header type: 255",
			},
			{
				name: "ErrorOnNegativeStartHegiht",
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
							cleanup: c1,
							err:     err,
						}
					}

					// Create a buffer for binary metadata.
					var metadataBuf bytes.Buffer

					// Write chainType (4 bytes).
					err = binary.Write(
						&metadataBuf,
						binary.LittleEndian,
						wire.SimNet,
					)
					if err != nil {
						return Prep{
							cleanup: c1,
							err:     err,
						}
					}

					// Write headerType (1 byte).
					err = metadataBuf.WriteByte(
						byte(headerfs.Block),
					)
					if err != nil {
						return Prep{
							cleanup: c1,
							err:     err,
						}
					}

					// Write startHeight (4 bytes). It is a
					// negative value in two-complement
					// format.
					err = binary.Write(
						&metadataBuf,
						binary.LittleEndian, int32(-1),
					)
					if err != nil {
						return Prep{
							cleanup: c1,
							err:     err,
						}
					}

					// Write metadata to the temp file.
					if _, err = bFile.Write(
						metadataBuf.Bytes(),
					); err != nil {
						return Prep{
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
					}
				},
				verify:    func(Verify) {},
				expectErr: true,
				expectErrMsg: "invalid negative value " +
					"detected for StartHeight: -1",
			},
			{
				name: "ReturnsCachedMetadataWhenAvailable",
				prep: func() Prep {
					// Create block headers file.
					bFile, c1, err := setupFileWithHdrs(
						headerfs.Block, true,
					)
					if err != nil {
						return Prep{
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
							cleanup: c2,
							err:     err,
						}
					}

					// Remove the source file to ensure next
					// call must use cached data.
					err = bS.Close()
					if err != nil {
						return Prep{
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
					}
				},
				verify: func(v Verify) {
					// Next call should result in a cache
					// hit since the file is gone.
					bHdrType := headerfs.Block
					expectBlockMetadata := &HeaderMetadata{
						BitcoinChainType: wire.SimNet,
						HeaderType:       bHdrType,
						HeaderSize:       80,
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
						cleanup: func() {},
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
							cleanup: c1,
							err:     err,
						}
					}

					// Read the data before adding metadata
					// for later assertion.
					dataBefore, err := io.ReadAll(bFile)
					if err != nil {
						return Prep{
							cleanup: c1,
							err:     err,
						}
					}

					return Prep{
						file:    bFile,
						data:    dataBefore,
						cleanup: c1,
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
							cleanup: c1,
							err:     err,
						}
					}

					// Read the data before adding metadata
					// for later assertion.
					dataBefore, err := io.ReadAll(bFile)
					if err != nil {
						return Prep{
							cleanup: c1,
							err:     err,
						}
					}

					return Prep{
						file:    bFile,
						data:    dataBefore,
						cleanup: c1,
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
							cleanup: c1,
							err:     err,
						}
					}

					// Read the data before adding metadata
					// for later assertion.
					dataBefore, err := io.ReadAll(bFile)
					if err != nil {
						return Prep{
							cleanup: c1,
							err:     err,
						}
					}

					return Prep{
						file:    bFile,
						data:    dataBefore,
						cleanup: c1,
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
	t.Run("Retrieval/SingleHeader", func(t *testing.T) {
		t.Parallel()
		type Prep struct {
			hISource HeaderImportSource
			cleanup  func()
			err      error
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
						hISource: bS,
						cleanup:  func() {},
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
							cleanup: c1,
							err:     err,
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
							cleanup: cleanup,
							err:     err,
						}
					}
					bFS.reader = reader

					// Illustrate that the metdata is not
					// initialized.
					bFS.metadata = nil

					return Prep{
						hISource: bFS,
						cleanup:  cleanup,
					}
				},
				verify:       func(Verify) {},
				expectErr:    true,
				expectErrMsg: "header metadata not initialized",
			},
			{
				name:  "ErrorOnDeserializeUnknownHeaderType",
				hType: headerfs.UnknownHeader,
				prep: func(hType headerfs.HeaderType) Prep {
					// Create block headers file.
					bFile, c1, err := setupFileWithHdrs(
						headerfs.Block, true,
					)
					if err != nil {
						return Prep{
							cleanup: c1,
							err:     err,
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
							cleanup: cleanup,
							err:     err,
						}
					}
					bFS.reader = reader

					// Set header metadata.
					bFS.metadata = &HeaderMetadata{}
					bFS.metadata.HeaderType = hType

					return Prep{
						hISource: bFS,
						cleanup:  cleanup,
					}
				},
				verify:    func(v Verify) {},
				expectErr: true,
				expectErrMsg: "failed to deserialize " +
					"wire.BlockHeader: EOF",
			},
			{
				name:  "ErrorOnHeaderIndexOutOfBounds",
				hType: headerfs.Block,
				index: len(blockHdrs),
				prep: func(hType headerfs.HeaderType) Prep {
					// Create block headers file.
					bFile, c1, err := setupFileWithHdrs(
						hType, true,
					)
					if err != nil {
						return Prep{
							cleanup: c1,
							err:     err,
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
							cleanup: cleanup,
							err:     err,
						}
					}

					return Prep{
						hISource: bS,
						cleanup:  cleanup,
					}
				},
				verify:    func(Verify) {},
				expectErr: true,
				expectErrMsg: fmt.Sprintf("failed to read "+
					"header at index %d", len(blockHdrs)),
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
							cleanup: c1,
							err:     err,
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
							cleanup: cleanup,
							err:     err,
						}
					}

					return Prep{
						hISource: bS,
						cleanup:  cleanup,
					}
				},
				verify: func(v Verify) {
					// Assert it is of block header type.
					bH, ok := v.header.(*BlockHeader)
					require.True(v.tc, ok)

					// Construct the expected block header.
					bHExpected, err := constructBlkHdr(
						blockHdrs[v.index],
						uint32(v.index),
					)
					require.NoError(t, err)

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
							cleanup: c1,
							err:     err,
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
							cleanup: cleanup,
							err:     err,
						}
					}

					return Prep{
						hISource: fS,
						cleanup:  cleanup,
					}
				},
				verify: func(v Verify) {
					// Assert it is of filter header type.
					fH, ok := v.header.(*FilterHeader)
					require.True(v.tc, ok)

					// Construct the expected filter header.
					fHExpected, err := constructFilterHdr(
						filterHdrs[v.index],
						uint32(v.index),
					)
					require.NoError(t, err)

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

	t.Run("Retrieval/SequentialHeaders", func(t *testing.T) {
		t.Parallel()
		type Prep struct {
			hIterator HeaderIterator
			cleanup   func()
			err       error
		}
		type Verify struct {
			tc        *testing.T
			hIterator HeaderIterator
			header    Header
			hasMore   bool
		}
		testCases := []struct {
			name         string
			index        int
			hType        headerfs.HeaderType
			prep         func(hT headerfs.HeaderType, indx int) Prep
			verify       func(Verify)
			expectErr    bool
			expectErrMsg string
		}{
			{
				name:  "ErrorOnHeaderOutOfBounds",
				index: len(blockHdrs),
				hType: headerfs.Block,
				prep: func(hT headerfs.HeaderType, i int) Prep {
					// Create block headers file.
					bFile, c1, err := setupFileWithHdrs(
						hT, true,
					)
					if err != nil {
						return Prep{
							cleanup: c1,
							err:     err,
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
							cleanup: cleanup,
							err:     err,
						}
					}

					iter := &ImportSourceHeaderIterator{
						source:  bS,
						current: i,
						end:     i,
					}

					return Prep{
						hIterator: iter,
						cleanup:   cleanup,
					}
				},
				verify: func(v Verify) {
					require.Nil(v.tc, v.header)
					require.False(v.tc, v.hasMore)
				},
				expectErr: true,
				expectErrMsg: fmt.Sprintf("failed to read "+
					"header at index %d", len(blockHdrs)),
			},
			{
				name:  "NoMoreHeadersToIterateOver",
				index: len(blockHdrs) - 1,
				hType: headerfs.Block,
				prep: func(hT headerfs.HeaderType, i int) Prep {
					// Create block headers file.
					bFile, c1, err := setupFileWithHdrs(
						hT, true,
					)
					if err != nil {
						return Prep{
							cleanup: c1,
							err:     err,
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
							cleanup: cleanup,
							err:     err,
						}
					}

					iter := &ImportSourceHeaderIterator{
						source:  bS,
						current: i + 1,
						end:     i,
					}

					return Prep{
						hIterator: iter,
						cleanup:   cleanup,
					}
				},
				verify: func(v Verify) {
					require.Nil(v.tc, v.header)
					require.False(v.tc, v.hasMore)
				},
			},
			{
				name:  "IterateOverBlockHeadersSuccessfully",
				index: 0,
				hType: headerfs.Block,
				prep: func(hT headerfs.HeaderType, i int) Prep {
					// Create block headers file.
					bFile, c1, err := setupFileWithHdrs(
						hT, true,
					)
					if err != nil {
						return Prep{
							cleanup: c1,
							err:     err,
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
							cleanup: cleanup,
							err:     err,
						}
					}

					iter := &ImportSourceHeaderIterator{
						source:  bS,
						current: i,
						end:     len(blockHdrs) - 1,
					}

					return Prep{
						hIterator: iter,
						cleanup:   cleanup,
					}
				},
				verify: func(v Verify) {
					iter := v.hIterator
					nBHs := len(blockHdrs)
					for i := 1; i < nBHs; i++ {
						h, more, err := iter.Next()
						require.True(v.tc, more)
						require.NoError(v.tc, err)
						hE, err := constructBlkHdr(
							blockHdrs[i], uint32(i),
						)
						require.NoError(t, err)
						require.Equal(v.tc, hE, h)
					}
					h, more, err := iter.Next()
					require.Nil(v.tc, h)
					require.False(v.tc, more)
					require.Nil(v.tc, err)
				},
			},
			{
				name:  "IterateOverFilterHeadersSuccessfully",
				index: 0,
				hType: headerfs.RegularFilter,
				prep: func(hT headerfs.HeaderType, i int) Prep {
					// Create filter headers file.
					fFile, c1, err := setupFileWithHdrs(
						hT, true,
					)
					if err != nil {
						return Prep{
							cleanup: c1,
							err:     err,
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
							cleanup: cleanup,
							err:     err,
						}
					}

					iter := &ImportSourceHeaderIterator{
						source:  fS,
						current: i,
						end:     len(filterHdrs) - 1,
					}

					return Prep{
						hIterator: iter,
						cleanup:   cleanup,
					}
				},
				verify: func(v Verify) {
					iter := v.hIterator
					nFHs := len(filterHdrs)
					for i := 1; i < nFHs; i++ {
						h, more, err := iter.Next()
						require.True(v.tc, more)
						require.NoError(v.tc, err)
						hE, err := constructFilterHdr(
							filterHdrs[i],
							uint32(i),
						)
						require.NoError(t, err)
						require.Equal(v.tc, hE, h)
					}
					h, more, err := iter.Next()
					require.Nil(v.tc, h)
					require.False(v.tc, more)
					require.Nil(v.tc, err)
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				prep := tc.prep(tc.hType, tc.index)
				t.Cleanup(prep.cleanup)
				require.NoError(t, prep.err)

				header, hasMore, err := prep.hIterator.Next()
				verify := Verify{
					tc:        t,
					hIterator: prep.hIterator,
					header:    header,
					hasMore:   hasMore,
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

	t.Run("Validation/BlockHeadersPair", func(t *testing.T) {
		t.Parallel()
		type Prep struct {
			hValidator HeadersValidator
			prev       Header
			current    Header
			err        error
		}
		testCases := []struct {
			name         string
			tCP          chaincfg.Params
			prep         func(tCP chaincfg.Params) Prep
			expectErr    bool
			expectErrMsg string
		}{
			{
				name: "ErrorOnMismatchPreviousHeaderType",
				tCP:  chaincfg.Params{},
				prep: func(chaincfg.Params) Prep {
					opts := &ImportOptions{}
					bHV := opts.createBlkHdrValidator()
					return Prep{
						hValidator: bHV,
						prev:       NewFilterHeader(),
						current:    NewBlockHeader(),
					}
				},
				expectErr: true,
				expectErrMsg: "expected BlockHeader type, " +
					"got \\*chainimport.FilterHeader",
			},
			{
				name: "ErrorOnMismatchCurrentHeaderType",
				tCP:  chaincfg.Params{},
				prep: func(chaincfg.Params) Prep {
					opts := &ImportOptions{}
					bHV := opts.createBlkHdrValidator()
					return Prep{
						hValidator: bHV,
						prev:       NewBlockHeader(),
						current:    NewFilterHeader(),
					}
				},
				expectErr: true,
				expectErrMsg: "expected BlockHeader type, " +
					"got \\*chainimport.FilterHeader",
			},
			{
				name: "ErrorOnNonConsecutiveHeaderChain",
				tCP:  chaincfg.Params{},
				prep: func(chaincfg.Params) Prep {
					opts := &ImportOptions{}
					bHV := opts.createBlkHdrValidator()

					// Construct previous block header.
					prevH, err := constructBlkHdr(
						blockHdrs[0],
						uint32(0),
					)
					if err != nil {
						return Prep{
							err: err,
						}
					}

					// Construct current block header.
					nBH := len(blockHdrs)
					currentH, err := constructBlkHdr(
						blockHdrs[nBH-1],
						uint32(nBH-1),
					)
					if err != nil {
						return Prep{
							err: err,
						}
					}

					return Prep{
						hValidator: bHV,
						prev:       prevH,
						current:    currentH,
					}
				},
				expectErr: true,
				expectErrMsg: "height mismatch: previous " +
					"height=0, current height=4",
			},
			{
				name: "ErrorOnInvalidHeaderHashChain",
				tCP:  chaincfg.Params{},
				prep: func(chaincfg.Params) Prep {
					opts := &ImportOptions{}
					bHV := opts.createBlkHdrValidator()

					// Construct origin block header.
					originH, err := constructBlkHdr(
						blockHdrs[0],
						uint32(0),
					)
					if err != nil {
						return Prep{
							err: err,
						}
					}

					// Construct previous block header.
					prevH, err := constructBlkHdr(
						blockHdrs[1],
						uint32(1),
					)
					if err != nil {
						return Prep{
							err: err,
						}
					}

					// Construct current block header.
					currentH, err := constructBlkHdr(
						blockHdrs[2],
						uint32(2),
					)
					if err != nil {
						return Prep{
							err: err,
						}
					}

					currentH.PrevBlock = originH.BlockHash()

					return Prep{
						hValidator: bHV,
						prev:       prevH,
						current:    currentH,
					}
				},
				expectErr: true,
				expectErrMsg: "header chain broken: current " +
					"header's PrevBlock (.*) doesn't " +
					"match previous header's hash (.*)",
			},
			{
				name: "ErrorOnZeroDifficultyTarget",
				tCP:  chaincfg.Params{},
				prep: func(chaincfg.Params) Prep {
					opts := &ImportOptions{}
					bHV := opts.createBlkHdrValidator()

					// Construct previous block header.
					prevH, err := constructBlkHdr(
						blockHdrs[0],
						uint32(0),
					)
					if err != nil {
						return Prep{
							err: err,
						}
					}

					// Construct current block header.
					currentH, err := constructBlkHdr(
						blockHdrs[1],
						uint32(1),
					)
					if err != nil {
						return Prep{
							err: err,
						}
					}

					currentH.Bits = 0

					return Prep{
						hValidator: bHV,
						prev:       prevH,
						current:    currentH,
					}
				},
				expectErr: true,
				expectErrMsg: "block target difficulty of .* " +
					"is too low",
			},
			{
				name: "ErrorOnDifficultyExceedsMaximumAllowed",
				tCP:  chaincfg.SimNetParams,
				prep: func(tCP chaincfg.Params) Prep {
					opts := &ImportOptions{}
					bHV := opts.createBlkHdrValidator()

					// Construct previous block header.
					prevH, err := constructBlkHdr(
						blockHdrs[0],
						uint32(0),
					)
					if err != nil {
						return Prep{
							err: err,
						}
					}

					// Construct current block header.
					currentH, err := constructBlkHdr(
						blockHdrs[1],
						uint32(1),
					)
					if err != nil {
						return Prep{
							err: err,
						}
					}

					invT := new(big.Int).Add(
						tCP.PowLimit, big.NewInt(1),
					)
					newT := blockchain.BigToCompact(invT)
					currentH.Bits = newT

					return Prep{
						hValidator: bHV,
						prev:       prevH,
						current:    currentH,
					}
				},
				expectErr: true,
				expectErrMsg: "block target difficulty of " +
					".* is higher than max of .*",
			},
			{
				name: "ErrorOnHashHigherThanTarget",
				tCP:  chaincfg.SimNetParams,
				prep: func(tCP chaincfg.Params) Prep {
					opts := &ImportOptions{}
					bHV := opts.createBlkHdrValidator()

					// Construct previous block header.
					prevH, err := constructBlkHdr(
						blockHdrs[0],
						uint32(0),
					)
					if err != nil {
						return Prep{
							err: err,
						}
					}

					// Construct current block header.
					currentH, err := constructBlkHdr(
						blockHdrs[1],
						uint32(1),
					)
					if err != nil {
						return Prep{
							err: err,
						}
					}

					// Set an extremely high difficulty
					// (very small target).
					currentH.Bits = 0x1f000001

					// We need to ensure the block hash will
					// be higher than target.
					currentH.Nonce = 0xffffffff
					currentH.MerkleRoot = chainhash.Hash{
						0xff, 0xff, 0xff,
					}

					return Prep{
						hValidator: bHV,
						prev:       prevH,
						current:    currentH,
					}
				},
				expectErr: true,
				expectErrMsg: "block hash of .* is higher " +
					"than expected max of .*",
			},
			{
				name: "ErrorOnSubSecondTimestampPrecision",
				tCP:  chaincfg.SimNetParams,
				prep: func(tCP chaincfg.Params) Prep {
					opts := &ImportOptions{}
					bHV := opts.createBlkHdrValidator()

					// Construct previous block header.
					prevH, err := constructBlkHdr(
						blockHdrs[0],
						uint32(0),
					)
					if err != nil {
						return Prep{
							err: err,
						}
					}

					// Construct current block header.
					currentH, err := constructBlkHdr(
						blockHdrs[1],
						uint32(1),
					)
					if err != nil {
						return Prep{
							err: err,
						}
					}

					currentH.Timestamp = time.Unix(0, 50)
					return Prep{
						hValidator: bHV,
						prev:       prevH,
						current:    currentH,
					}
				},
				expectErr: true,
				expectErrMsg: "block timestamp of .* has a " +
					"higher precision than one second",
			},
			{
				name: "ValidateBlockHeadersPairSuccessfully",
				tCP:  chaincfg.SimNetParams,
				prep: func(tCP chaincfg.Params) Prep {
					opts := &ImportOptions{}
					bHV := opts.createBlkHdrValidator()

					// Construct previous block header.
					prevH, err := constructBlkHdr(
						blockHdrs[0],
						uint32(0),
					)
					if err != nil {
						return Prep{
							err: err,
						}
					}

					// Construct current block header.
					currentH, err := constructBlkHdr(
						blockHdrs[1],
						uint32(1),
					)
					if err != nil {
						return Prep{
							err: err,
						}
					}

					return Prep{
						hValidator: bHV,
						prev:       prevH,
						current:    currentH,
					}
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				prep := tc.prep(tc.tCP)
				require.NoError(t, prep.err)

				err := prep.hValidator.ValidatePair(
					prep.prev, prep.current,
					tc.tCP,
				)
				if tc.expectErr {
					matched, matchErr := regexp.MatchString(
						tc.expectErrMsg, err.Error(),
					)
					require.NoError(t, matchErr)
					require.True(
						t, matched,
						fmt.Sprintf("failed with: %v",
							err.Error()),
					)
					return
				}
				require.NoError(t, err)
			})
		}
	})

	t.Run("Validation/SequentialBlockHeaders", func(t *testing.T) {
		t.Parallel()
		type Prep struct {
			iterator  HeaderIterator
			validator HeadersValidator
			cleanup   func()
			err       error
		}
		testCases := []struct {
			name         string
			index        int
			prep         func() Prep
			expectErr    bool
			expectErrMsg string
		}{
			{
				name:  "ErrorOnHeaderOutOfBounds",
				index: len(blockHdrs),
				prep: func() Prep {
					// Create block headers file.
					bFile, c1, err := setupFileWithHdrs(
						headerfs.Block, true,
					)
					if err != nil {
						return Prep{
							cleanup: c1,
							err:     err,
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
							cleanup: cleanup,
							err:     err,
						}
					}

					index := len(blockHdrs)
					bIterator := bS.Iterator(index-1, index)

					// Create block validator.
					bV := opts.createBlkHdrValidator()

					return Prep{
						iterator:  bIterator,
						validator: bV,
						cleanup:   cleanup,
					}
				},
				expectErr: true,
				expectErrMsg: fmt.Sprintf("failed to read "+
					"header at index %d", len(blockHdrs)),
			},
			{
				name: "NoMoreHeadersToValidate",
				prep: func() Prep {
					// Create block headers file.
					bFile, c1, err := setupFileWithHdrs(
						headerfs.Block, true,
					)
					if err != nil {
						return Prep{
							cleanup: c1,
							err:     err,
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
							cleanup: cleanup,
							err:     err,
						}
					}

					index := len(blockHdrs) - 1
					bIterator := bS.Iterator(index+1, index)

					// Create block validator.
					bV := opts.createBlkHdrValidator()

					return Prep{
						iterator:  bIterator,
						validator: bV,
						cleanup:   cleanup,
					}
				},
			},
			{
				name: "ValidSequentialHeaders",
				prep: func() Prep {
					// Create a file with valid block
					// headers and metadata.
					bFile, c1, err := setupFileWithHdrs(
						headerfs.Block, true,
					)
					if err != nil {
						return Prep{
							cleanup: c1,
							err:     err,
						}
					}

					// Configure import options
					opts := &ImportOptions{}
					bS := opts.createBlkHdrImportSrc()
					bS.SetPath(bFile.Name())
					err = bS.Open()
					cleanup := func() {
						bS.Close()
						os.Remove(bS.GetPath())
					}
					if err != nil {
						return Prep{
							cleanup: cleanup,
							err:     err,
						}
					}

					// Get header metadata.
					meta, err := bS.GetHeaderMetadata()
					if err != nil {
						return Prep{
							cleanup: cleanup,
							err:     err,
						}
					}

					// Create block header iterator.
					bIt := bS.Iterator(
						0, int(meta.HeadersCount-1),
					)

					// Create block validator.
					bV := opts.createBlkHdrValidator()

					return Prep{
						iterator:  bIt,
						validator: bV,
						cleanup:   cleanup,
					}
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				prep := tc.prep()
				t.Cleanup(prep.cleanup)
				require.NoError(t, prep.err)
				err := prep.validator.Validate(
					prep.iterator, chaincfg.SimNetParams,
				)
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

	// The goal of these test cases is to explicitly illustrate that we
	// don't do any validation on the import filter headers as yet since we
	// don't have access to compact filters that generated those headers. In
	// the future if a way is found to validate filter headers, some of
	// these test cases are supposed to fail.
	t.Run("Validation/FilterHeadersPair", func(t *testing.T) {
		t.Parallel()
		type Prep struct {
			validator HeadersValidator
			prev      Header
			current   Header
			err       error
		}
		testCases := []struct {
			name         string
			tCP          chaincfg.Params
			prep         func() Prep
			expectErr    bool
			expectErrMsg string
		}{
			{
				name: "NoErrorOnNonConsecutiveHeaderChain",
				tCP:  chaincfg.SimNetParams,
				prep: func() Prep {
					opts := &ImportOptions{}
					fHV := opts.createFilterHdrValidator()

					// Construct previous filter header.
					prev, err := constructFilterHdr(
						filterHdrs[0], 0,
					)
					if err != nil {
						return Prep{
							err: err,
						}
					}

					// Construct current filter header.
					nFH := len(filterHdrs)
					current, err := constructBlkHdr(
						filterHdrs[nFH-1],
						uint32(nFH-1),
					)
					if err != nil {
						return Prep{
							err: err,
						}
					}

					return Prep{
						validator: fHV,
						prev:      prev,
						current:   current,
					}
				},
				expectErr: false,
			},
			{
				name: "NoErrorOnInvalidHeaderHashChain",
				tCP:  chaincfg.SimNetParams,
				prep: func() Prep {
					opts := &ImportOptions{}
					fHV := opts.createFilterHdrValidator()

					// Construct origin filter header.
					originH, err := constructFilterHdr(
						filterHdrs[0],
						uint32(0),
					)
					if err != nil {
						return Prep{
							validator: fHV,
							err:       err,
						}
					}

					// Construct previous filter header.
					prevH, err := constructFilterHdr(
						filterHdrs[1],
						uint32(1),
					)
					if err != nil {
						return Prep{
							err: err,
						}
					}

					// Construct current filter header.
					currentH, err := constructFilterHdr(
						filterHdrs[2],
						uint32(2),
					)
					if err != nil {
						return Prep{
							err: err,
						}
					}

					currentH.FilterHash = originH.FilterHash

					return Prep{
						validator: fHV,
						prev:      prevH,
						current:   currentH,
					}
				},
				expectErr: false,
			},
			{
				name: "ValidFilterHeader",
				tCP:  chaincfg.SimNetParams,
				prep: func() Prep {
					opts := &ImportOptions{}
					fHV := opts.createFilterHdrValidator()

					// Construct previous filter header.
					prev, err := constructFilterHdr(
						filterHdrs[0], 0,
					)
					if err != nil {
						return Prep{
							err: err,
						}
					}

					// Construct current filter header.
					current, err := constructBlkHdr(
						filterHdrs[1],
						uint32(1),
					)
					if err != nil {
						return Prep{
							err: err,
						}
					}

					return Prep{
						validator: fHV,
						prev:      prev,
						current:   current,
					}
				},
				expectErr: false,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				prep := tc.prep()
				require.NoError(t, prep.err)
				err := prep.validator.ValidatePair(
					prep.prev, prep.current, tc.tCP,
				)
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

	// The goal of these test cases is to explicitly illustrate that we
	// don't do any validation on the import filter headers as yet since we
	// don't have access to compact filters that generated those headers. In
	// the future if a way is found to validate filter headers, some of
	// these test cases are supposed to fail.
	t.Run("Validation/SequentialFilterHeaders", func(t *testing.T) {
		t.Parallel()
		type Prep struct {
			iterator  HeaderIterator
			validator HeadersValidator
			cleanup   func()
			err       error
		}
		testCases := []struct {
			name         string
			index        int
			prep         func() Prep
			expectErr    bool
			expectErrMsg string
		}{
			{
				name: "NoErrorOnInvalidSequentialHeaders",
				prep: func() Prep {
					// Create a file with valid filter
					// headers and metadata.
					fFile, c1, err := setupFileWithHdrs(
						headerfs.Block, true,
					)
					if err != nil {
						return Prep{
							cleanup: c1,
							err:     err,
						}
					}

					// Configure import options
					opts := &ImportOptions{}
					fS := opts.createFilterHdrImportSrc()
					fS.SetPath(fFile.Name())
					err = fS.Open()
					cleanup := func() {
						fS.Close()
						os.Remove(fS.GetPath())
					}
					if err != nil {
						return Prep{
							cleanup: cleanup,
							err:     err,
						}
					}

					// Get header metadata.
					meta, err := fS.GetHeaderMetadata()
					if err != nil {
						return Prep{
							cleanup: cleanup,
							err:     err,
						}
					}

					// Create filter header iterator.
					fIterator := fS.Iterator(
						0, int(meta.HeadersCount-1),
					)

					// Create filter validator.
					fV := opts.createFilterHdrValidator()

					return Prep{
						iterator:  fIterator,
						validator: fV,
						cleanup:   cleanup,
					}
				},
				expectErr: false,
			},
			{
				name: "ValidSequentialHeaders",
				prep: func() Prep {
					// Create a file with valid filter
					// headers and metadata.
					fFile, c1, err := setupFileWithHdrs(
						headerfs.RegularFilter, true,
					)
					if err != nil {
						return Prep{
							cleanup: c1,
							err:     err,
						}
					}

					// Configure import options
					opts := &ImportOptions{}
					fS := opts.createFilterHdrImportSrc()
					fS.SetPath(fFile.Name())
					err = fS.Open()
					cleanup := func() {
						fS.Close()
						os.Remove(fS.GetPath())
					}
					if err != nil {
						return Prep{
							cleanup: cleanup,
							err:     err,
						}
					}

					// Get header metadata.
					meta, err := fS.GetHeaderMetadata()
					if err != nil {
						return Prep{
							cleanup: cleanup,
							err:     err,
						}
					}

					// Create filter header iterator.
					fIterator := fS.Iterator(
						0, int(meta.HeadersCount-1),
					)

					// Create filter validator.
					fV := opts.createFilterHdrValidator()

					return Prep{
						iterator:  fIterator,
						validator: fV,
						cleanup:   cleanup,
					}
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				prep := tc.prep()
				t.Cleanup(prep.cleanup)
				require.NoError(t, prep.err)
				err := prep.validator.Validate(
					prep.iterator, chaincfg.SimNetParams,
				)
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

	// These test cases mainly determine the processing regions. For
	// convenience and shorter test case names, we'll use the following
	// notation:
	// - A refers to data from import sources. No divergence.
	// - B refers to data from target sources when both target stores have
	// the same length. In case of divergence, B specifically refers to the
	// target block headers store, while F refers to the target filter
	// headers store.
	t.Run("Processing", func(t *testing.T) {
		t.Parallel()
		type Prep struct {
			hImport *HeadersImport
			err     error
		}
		type Verify struct {
			tc                *testing.T
			processingRegions *ProcessingRegions
		}
		testCases := []struct {
			name         string
			prep         func() Prep
			verify       func(Verify)
			expectErr    bool
			expectErrMsg string
		}{
			// ❌ Error validation test cases.
			{
				name: "ErrorOnGetHeaderMetadataFail",
				prep: func() Prep {
					// Prep error to mock.
					expectErr := errors.New("failed to " +
						"get header metadata")

					// Mock GetHeaderMetadata.
					hIS := &mockHeaderImportSource{}
					hIS.On("GetHeaderMetadata").Return(
						nil, expectErr,
					)

					// Mock ChainTip on target header store.
					bHS := &mockBlockHeaderStore{}
					bHS.On("ChainTip").Return(
						&wire.BlockHeader{}, uint32(60),
						nil,
					)

					// Mock ChainTip on target header store.
					fHS := &mockFilterHeaderStore{}
					fHS.On("ChainTip").Return(
						&chainhash.Hash{}, uint32(60),
						nil,
					)

					// Configure Import options.
					ops := &ImportOptions{
						TargetBlockHeaderStore:  bHS,
						TargetFilterHeaderStore: fHS,
					}

					h := &HeadersImport{
						BlockHeadersImportSource: hIS,
						options:                  ops,
					}

					return Prep{
						hImport: h,
					}
				},
				verify:       func(Verify) {},
				expectErr:    true,
				expectErrMsg: "failed to get header metadata",
			},
			{
				name: "ErrorOnTargetBlockStoreChainTipFailure",
				prep: func() Prep {
					// Prep error to mock.
					expectErr := errors.New("failed to " +
						"get block header store " +
						"chain tip")

					// Prep header metadata.
					hM := &HeaderMetadata{
						StartHeight: 50,
						EndHeight:   90,
					}

					// Mock GetHeaderMetadata.
					hIS := &mockHeaderImportSource{}
					hIS.On("GetHeaderMetadata").Return(
						hM, nil,
					)

					// Mock ChainTip on target header store.
					bHS := &mockBlockHeaderStore{}
					bHS.On("ChainTip").Return(
						&wire.BlockHeader{}, uint32(0),
						expectErr,
					)

					// Mock ChainTip on target header store.
					fHS := &mockFilterHeaderStore{}
					fHS.On("ChainTip").Return(
						&chainhash.Hash{}, uint32(60),
						nil,
					)

					// Configure Import options.
					ops := &ImportOptions{
						TargetBlockHeaderStore:  bHS,
						TargetFilterHeaderStore: fHS,
					}

					h := &HeadersImport{
						BlockHeadersImportSource: hIS,
						options:                  ops,
					}

					return Prep{
						hImport: h,
					}
				},
				verify:    func(Verify) {},
				expectErr: true,
				expectErrMsg: "failed to get block header " +
					"store chain tip",
			},
			{
				name: "ErrorOnTargetFilterStoreChainTipFailure",
				prep: func() Prep {
					// Prep error to mock.
					expectErr := errors.New("failed to " +
						"get filter header store " +
						"chain tip")

					// Prep header metadata.
					hM := &HeaderMetadata{
						StartHeight: 50,
						EndHeight:   90,
					}

					// Mock GetHeaderMetadata.
					hIS := &mockHeaderImportSource{}
					hIS.On("GetHeaderMetadata").Return(
						hM, nil,
					)

					// Mock ChainTip on target header store.
					bHS := &mockBlockHeaderStore{}
					bHS.On("ChainTip").Return(
						&wire.BlockHeader{}, uint32(60),
						nil,
					)

					// Mock ChainTip on target header store.
					fHS := &mockFilterHeaderStore{}
					fHS.On("ChainTip").Return(
						&chainhash.Hash{}, uint32(0),
						expectErr,
					)

					// Configure Import options.
					ops := &ImportOptions{
						TargetBlockHeaderStore:  bHS,
						TargetFilterHeaderStore: fHS,
					}

					h := &HeadersImport{
						BlockHeadersImportSource: hIS,
						options:                  ops,
					}

					return Prep{
						hImport: h,
					}
				},
				verify:    func(Verify) {},
				expectErr: true,
				expectErrMsg: "failed to get filter header " +
					"store chain tip",
			},

			// ✅ Non-divergent cases (B = F).

			/*
				A:     [=========]
				B: [=======]
				F: [=======]
			*/
			{
				name: "AAndBOverlap_AEndsAfterB",
				prep: func() Prep {
					// Prep header metadata.
					hM := &HeaderMetadata{
						StartHeight: 50,
						EndHeight:   90,
					}

					// Mock GetHeaderMetadata.
					hIS := &mockHeaderImportSource{}
					hIS.On("GetHeaderMetadata").Return(
						hM, nil,
					)

					// Mock ChainTip on target header store.
					bHS := &mockBlockHeaderStore{}
					bHS.On("ChainTip").Return(
						&wire.BlockHeader{}, uint32(60),
						nil,
					)

					// Mock ChainTip on target header store.
					fHS := &mockFilterHeaderStore{}
					fHS.On("ChainTip").Return(
						&chainhash.Hash{}, uint32(60),
						nil,
					)

					// Configure Import options.
					ops := &ImportOptions{
						TargetBlockHeaderStore:  bHS,
						TargetFilterHeaderStore: fHS,
					}

					h := &HeadersImport{
						BlockHeadersImportSource: hIS,
						options:                  ops,
					}

					return Prep{
						hImport: h,
					}
				},
				verify: func(v Verify) {
					// Assert that the divergence region
					// doesn't exist.
					dR := v.processingRegions.Divergence
					require.False(v.tc, dR.Exists)

					// Assert that the overlap region was
					// properly detected.
					oR := v.processingRegions.Overlap
					oRE := HeaderRegion{
						Start:  50,
						End:    60,
						Exists: true,
					}
					require.Equal(v.tc, oRE, oR)

					// Assert that the new headers region
					// was properly detected.
					nHR := v.processingRegions.NewHeaders
					nHRE := HeaderRegion{
						Start:  61,
						End:    90,
						Exists: true,
					}
					require.Equal(v.tc, nHRE, nHR)
				},
			},

			/*
				A:     [=====]
				B: [===========]
				F: [===========]
			*/
			{
				name: "BCompletelyOverlapsA",
				prep: func() Prep {
					// Prep header metadata.
					hM := &HeaderMetadata{
						StartHeight: 50,
						EndHeight:   70,
					}

					// Mock GetHeaderMetadata.
					hIS := &mockHeaderImportSource{}
					hIS.On("GetHeaderMetadata").Return(
						hM, nil,
					)

					// Mock ChainTip on target header store.
					bHS := &mockBlockHeaderStore{}
					bHS.On("ChainTip").Return(
						&wire.BlockHeader{}, uint32(90),
						nil,
					)

					// Mock ChainTip on target header store.
					fHS := &mockFilterHeaderStore{}
					fHS.On("ChainTip").Return(
						&chainhash.Hash{}, uint32(90),
						nil,
					)

					// Configure Import options.
					ops := &ImportOptions{
						TargetBlockHeaderStore:  bHS,
						TargetFilterHeaderStore: fHS,
					}

					h := &HeadersImport{
						BlockHeadersImportSource: hIS,
						options:                  ops,
					}

					return Prep{
						hImport: h,
					}
				},
				verify: func(v Verify) {
					// Assert that the divergence region
					// doesn't exist.
					dR := v.processingRegions.Divergence
					require.False(v.tc, dR.Exists)

					// Assert that the new headers region
					// doesn't exist.
					nHR := v.processingRegions.NewHeaders
					require.False(v.tc, nHR.Exists)

					// Assert that the overlap region is
					// properly detected.
					oR := v.processingRegions.Overlap
					oRE := HeaderRegion{
						Start:  50,
						End:    70,
						Exists: true,
					}
					require.Equal(v.tc, oRE, oR)
				},
			},

			/*
				A:         [======]
				B: [======]
				F: [======]
			*/
			{
				name: "AAndBDoNotOverlap_AComesAfterB",
				prep: func() Prep {
					// Prep header metadata.
					hM := &HeaderMetadata{
						StartHeight: 51,
						EndHeight:   90,
					}

					// Mock GetHeaderMetadata.
					hIS := &mockHeaderImportSource{}
					hIS.On("GetHeaderMetadata").Return(
						hM, nil,
					)

					// Mock ChainTip on target header store.
					bHS := &mockBlockHeaderStore{}
					bHS.On("ChainTip").Return(
						&wire.BlockHeader{}, uint32(50),
						nil,
					)

					// Mock ChainTip on target header store.
					fHS := &mockFilterHeaderStore{}
					fHS.On("ChainTip").Return(
						&chainhash.Hash{}, uint32(50),
						nil,
					)

					// Configure Import options.
					ops := &ImportOptions{
						TargetBlockHeaderStore:  bHS,
						TargetFilterHeaderStore: fHS,
					}

					h := &HeadersImport{
						BlockHeadersImportSource: hIS,
						options:                  ops,
					}

					return Prep{
						hImport: h,
					}
				},
				verify: func(v Verify) {
					// Assert that the divergence region
					// doesn't exist.
					dR := v.processingRegions.Divergence
					require.False(v.tc, dR.Exists)

					// Assert that the overlap region was
					// properly detected.
					oR := v.processingRegions.Overlap
					require.False(v.tc, oR.Exists)

					// Assert that the new headers region
					// was properly detected.
					nHR := v.processingRegions.NewHeaders
					nHRE := HeaderRegion{
						Start:  51,
						End:    90,
						Exists: true,
					}
					require.Equal(v.tc, nHRE, nHR)
				},
			},

			// ⚠️ Divergent cases (B ≠ F), B & F start at same pos.

			/*
				A: [===========]
				B: [=====]
				F: [===============]
			*/
			{
				name: "Divergence_AFFullyOverlapsB",
				prep: func() Prep {
					// Prep header metadata.
					hM := &HeaderMetadata{
						StartHeight: 0,
						EndHeight:   70,
					}

					// Mock GetHeaderMetadata.
					hIS := &mockHeaderImportSource{}
					hIS.On("GetHeaderMetadata").Return(
						hM, nil,
					)

					// Mock ChainTip on target header store.
					bHS := &mockBlockHeaderStore{}
					bHS.On("ChainTip").Return(
						&wire.BlockHeader{}, uint32(40),
						nil,
					)

					// Mock ChainTip on target header store.
					fHS := &mockFilterHeaderStore{}
					fHS.On("ChainTip").Return(
						&chainhash.Hash{}, uint32(90),
						nil,
					)

					// Configure Import options.
					ops := &ImportOptions{
						TargetBlockHeaderStore:  bHS,
						TargetFilterHeaderStore: fHS,
					}

					h := &HeadersImport{
						BlockHeadersImportSource: hIS,
						options:                  ops,
					}

					return Prep{
						hImport: h,
					}
				},
				verify: func(v Verify) {
					// Assert that the new headers region
					// doesn't exist.
					nHR := v.processingRegions.NewHeaders
					require.False(v.tc, nHR.Exists)

					// Assert that the overlap region was
					// properly detected.
					oR := v.processingRegions.Overlap
					oRE := HeaderRegion{
						Start:  0,
						End:    40,
						Exists: true,
					}
					require.Equal(v.tc, oRE, oR)

					// Assert that the divergence region
					// was properly detected
					dR := v.processingRegions.Divergence
					dRE := HeaderRegion{
						Start:  41,
						End:    70,
						Exists: true,
					}
					require.Equal(v.tc, dRE, dR)
				},
			},

			/*
				A: [===============]
				B: [=====]
				F: [===========]
			*/
			{
				name: "Divergence_AFullyOverlapsBF",
				prep: func() Prep {
					// Prep header metadata.
					hM := &HeaderMetadata{
						StartHeight: 0,
						EndHeight:   90,
					}

					// Mock GetHeaderMetadata.
					hIS := &mockHeaderImportSource{}
					hIS.On("GetHeaderMetadata").Return(
						hM, nil,
					)

					// Mock ChainTip on target header store.
					bHS := &mockBlockHeaderStore{}
					bHS.On("ChainTip").Return(
						&wire.BlockHeader{}, uint32(40),
						nil,
					)

					// Mock ChainTip on target header store.
					fHS := &mockFilterHeaderStore{}
					fHS.On("ChainTip").Return(
						&chainhash.Hash{}, uint32(70),
						nil,
					)

					// Configure Import options.
					ops := &ImportOptions{
						TargetBlockHeaderStore:  bHS,
						TargetFilterHeaderStore: fHS,
					}

					h := &HeadersImport{
						BlockHeadersImportSource: hIS,
						options:                  ops,
					}

					return Prep{
						hImport: h,
					}
				},
				verify: func(v Verify) {
					// Assert that the overlap region was
					// properly detected.
					oR := v.processingRegions.Overlap
					oRE := HeaderRegion{
						Start:  0,
						End:    40,
						Exists: true,
					}
					require.Equal(v.tc, oRE, oR)

					// Assert that the divergence region
					// was properly detected
					dR := v.processingRegions.Divergence
					dRE := HeaderRegion{
						Start:  41,
						End:    70,
						Exists: true,
					}
					require.Equal(v.tc, dRE, dR)

					// Assert that the new headers region
					// was properly detected.
					nHR := v.processingRegions.NewHeaders
					nHRE := HeaderRegion{
						Start:  71,
						End:    90,
						Exists: true,
					}
					require.Equal(v.tc, nHRE, nHR)
				},
			},

			/*
				A: [===========]
				B: [===============]
				F: [=====]
			*/
			{
				name: "Divergence_ABFullyOverlapsF",
				prep: func() Prep {
					// Prep header metadata.
					hM := &HeaderMetadata{
						StartHeight: 0,
						EndHeight:   70,
					}

					// Mock GetHeaderMetadata.
					hIS := &mockHeaderImportSource{}
					hIS.On("GetHeaderMetadata").Return(
						hM, nil,
					)

					// Mock ChainTip on target header store.
					bHS := &mockBlockHeaderStore{}
					bHS.On("ChainTip").Return(
						&wire.BlockHeader{}, uint32(90),
						nil,
					)

					// Mock ChainTip on target header store.
					fHS := &mockFilterHeaderStore{}
					fHS.On("ChainTip").Return(
						&chainhash.Hash{}, uint32(40),
						nil,
					)

					// Configure Import options.
					ops := &ImportOptions{
						TargetBlockHeaderStore:  bHS,
						TargetFilterHeaderStore: fHS,
					}

					h := &HeadersImport{
						BlockHeadersImportSource: hIS,
						options:                  ops,
					}

					return Prep{
						hImport: h,
					}
				},
				verify: func(v Verify) {
					// Assert that the new headers region
					// doesn't exist.
					nHR := v.processingRegions.NewHeaders
					require.False(v.tc, nHR.Exists)

					// Assert that the overlap region was
					// properly detected.
					oR := v.processingRegions.Overlap
					oRE := HeaderRegion{
						Start:  0,
						End:    40,
						Exists: true,
					}
					require.Equal(v.tc, oRE, oR)

					// Assert that the divergence region
					// was properly detected
					dR := v.processingRegions.Divergence
					dRE := HeaderRegion{
						Start:  41,
						End:    70,
						Exists: true,
					}
					require.Equal(v.tc, dRE, dR)
				},
			},

			/*
				A:     [=======]
				B: [===========]
				F: [=========]
			*/
			{
				name: "Divergence_BAndFCompletelyContainA",
				prep: func() Prep {
					// Prep header metadata.
					hM := &HeaderMetadata{
						StartHeight: 40,
						EndHeight:   90,
					}

					// Mock GetHeaderMetadata.
					hIS := &mockHeaderImportSource{}
					hIS.On("GetHeaderMetadata").Return(
						hM, nil,
					)

					// Mock ChainTip on target header store.
					bHS := &mockBlockHeaderStore{}
					bHS.On("ChainTip").Return(
						&wire.BlockHeader{}, uint32(90),
						nil,
					)

					// Mock ChainTip on target header store.
					fHS := &mockFilterHeaderStore{}
					fHS.On("ChainTip").Return(
						&chainhash.Hash{}, uint32(70),
						nil,
					)

					// Configure Import options.
					ops := &ImportOptions{
						TargetBlockHeaderStore:  bHS,
						TargetFilterHeaderStore: fHS,
					}

					h := &HeadersImport{
						BlockHeadersImportSource: hIS,
						options:                  ops,
					}

					return Prep{
						hImport: h,
					}
				},
				verify: func(v Verify) {
					// Assert that the new headers region
					// doesn't exist.
					nHR := v.processingRegions.NewHeaders
					require.False(v.tc, nHR.Exists)

					// Assert that the overlap region was
					// properly detected.
					oR := v.processingRegions.Overlap
					oRE := HeaderRegion{
						Start:  40,
						End:    70,
						Exists: true,
					}
					require.Equal(v.tc, oRE, oR)

					// Assert that the divergence region
					// was properly detected
					dR := v.processingRegions.Divergence
					dRE := HeaderRegion{
						Start:  71,
						End:    90,
						Exists: true,
					}
					require.Equal(v.tc, dRE, dR)
				},
			},

			/*
				A:     [===========]
				B: [=====]
				F: [======]
			*/
			{
				name: "Divergence_AEndsAfterBothBAndF",
				prep: func() Prep {
					// Prep header metadata.
					hM := &HeaderMetadata{
						StartHeight: 30,
						EndHeight:   90,
					}

					// Mock GetHeaderMetadata.
					hIS := &mockHeaderImportSource{}
					hIS.On("GetHeaderMetadata").Return(
						hM, nil,
					)

					// Mock ChainTip on target header store.
					bHS := &mockBlockHeaderStore{}
					bHS.On("ChainTip").Return(
						&wire.BlockHeader{}, uint32(40),
						nil,
					)

					// Mock ChainTip on target header store.
					fHS := &mockFilterHeaderStore{}
					fHS.On("ChainTip").Return(
						&chainhash.Hash{}, uint32(50),
						nil,
					)

					// Configure Import options.
					ops := &ImportOptions{
						TargetBlockHeaderStore:  bHS,
						TargetFilterHeaderStore: fHS,
					}

					h := &HeadersImport{
						BlockHeadersImportSource: hIS,
						options:                  ops,
					}

					return Prep{
						hImport: h,
					}
				},
				verify: func(v Verify) {
					// Assert that the overlap region was
					// properly detected.
					oR := v.processingRegions.Overlap
					oRE := HeaderRegion{
						Start:  30,
						End:    40,
						Exists: true,
					}
					require.Equal(v.tc, oRE, oR)

					// Assert that the divergence region
					// was properly detected
					dR := v.processingRegions.Divergence
					dRE := HeaderRegion{
						Start:  41,
						End:    50,
						Exists: true,
					}
					require.Equal(v.tc, dRE, dR)

					// Assert that the new headers region
					// was properly detected.
					nHR := v.processingRegions.NewHeaders
					nHRE := HeaderRegion{
						Start:  51,
						End:    90,
						Exists: true,
					}
					require.Equal(v.tc, nHRE, nHR)
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				prep := tc.prep()
				require.NoError(t, prep.err)
				imprt := prep.hImport
				regs, err := imprt.determineProcessingRegions()
				verify := Verify{
					tc:                t,
					processingRegions: regs,
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
			hImport       *HeadersImport
			blockHeaders  []headerfs.BlockHeader
			filterHeaders []headerfs.FilterHeader
			cleanup       func()
			err           error
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
				name: "ErrorOnBlockHeadersWrite",
				prep: func() Prep {
					// Setup mock block header store.
					b := &mockBlockHeaderStore{}
					args := mock.Anything
					b.On("WriteHeaders", args).Return(
						errors.New("I/O write error"),
					)

					f := &mockFilterHeaderStore{}

					// Configure import options.
					ops := &ImportOptions{
						TargetBlockHeaderStore:  b,
						TargetFilterHeaderStore: f,
					}

					headersImport := &HeadersImport{
						options: ops,
					}

					return Prep{
						hImport: headersImport,
						cleanup: func() {},
					}
				},
				verify:       func(v Verify) {},
				expectErr:    true,
				expectErrMsg: "",
			},
			{
				name: "RollbackBlockHdrsAfterFilterHdrsFailure",
				prep: func() Prep {
					tempDir, err := os.MkdirTemp(
						"", "headers_store_test",
					)
					c1 := func() {
						os.RemoveAll(tempDir)
					}
					if err != nil {
						return Prep{
							cleanup: c1,
							err:     err,
						}
					}

					dbPath := filepath.Join(
						tempDir, "test.db",
					)
					db, err := walletdb.Create(
						"bdb", dbPath, true,
						time.Second*10,
					)
					cleanup := func() {
						db.Close()
						c1()
					}
					if err != nil {
						return Prep{
							cleanup: cleanup,
							err:     err,
						}
					}

					b, err := headerfs.NewBlockHeaderStore(
						tempDir, db,
						&chaincfg.SimNetParams,
					)
					if err != nil {
						return Prep{
							cleanup: cleanup,
							err:     err,
						}
					}

					// Setup mock filter header store.
					f := &mockFilterHeaderStore{}
					args := mock.Anything
					f.On("WriteHeaders", args).Return(
						errors.New("I/O write error"),
					)

					// Prep block headers to write to the
					// target headers store. Ignore the
					// genesis block header since
					// NewBlockHeaderStore already wrote it.
					nBHs := len(blockHdrs)
					blkHdrsToWrite := make(
						[]headerfs.BlockHeader,
						nBHs-1,
					)
					for i := 1; i < nBHs; i++ {
						blockHdr := blockHdrs[i]
						h, err := constructBlkHdr(
							blockHdr,
							uint32(i),
						)
						res := Prep{
							cleanup: cleanup,
							err:     err,
						}
						if err != nil {
							return res
						}
						bHValue := h.BlockHeader
						blkHdrsToWrite[i-1] = bHValue
					}

					// Configure import options.
					ops := &ImportOptions{
						TargetBlockHeaderStore:  b,
						TargetFilterHeaderStore: f,
					}

					headersImport := &HeadersImport{
						options: ops,
					}

					return Prep{
						hImport:       headersImport,
						blockHeaders:  blkHdrsToWrite,
						filterHeaders: nil,
						cleanup:       cleanup,
					}
				},
				verify: func(v Verify) {
					// Verify chain tip of the target block
					// header store.
					options := v.hImport.options
					tBS := options.TargetBlockHeaderStore
					chainTipB, height, err := tBS.ChainTip()
					require.NoError(v.tc, err)

					// Since we have wrote 4 headers and
					// those rolledback on filter headers
					// write failure, we can expect the
					// chain tip height to be 0.
					require.Equal(v.tc, height, uint32(0))

					// Assert that the known block header at
					// this index matches the retrieved one.
					chainTipBEx, err := constructBlkHdr(
						blockHdrs[0],
						uint32(0),
					)
					require.NoError(v.tc, err)
					b := chainTipBEx.BlockHeader.BlockHeader
					require.Equal(
						v.tc, b, chainTipB,
					)
				},
				expectErr:    true,
				expectErrMsg: "I/O write error",
			},
			{
				name: "ErrorOnBlockHeadersRollback",
				prep: func() Prep {
					// Setup mock block header store.
					b := &mockBlockHeaderStore{}
					a := mock.Anything
					b.On("WriteHeaders", a).Return(nil)
					b.On("RollbackBlockHeaders", a).Return(
						nil,
						errors.New("I/O rollback err"),
					)

					// Setup mock filter header store.
					f := &mockFilterHeaderStore{}
					f.On("WriteHeaders", a).Return(
						errors.New("I/O write err"),
					)

					// Configure import options.
					ops := &ImportOptions{
						TargetBlockHeaderStore:  b,
						TargetFilterHeaderStore: f,
					}

					headersImport := &HeadersImport{
						options: ops,
					}

					return Prep{
						hImport: headersImport,
						cleanup: func() {},
					}
				},
				verify:       func(v Verify) {},
				expectErr:    true,
				expectErrMsg: "I/O rollback err",
			},
			{
				name: "NoErrorOnEmptyBlockAndFilterHeaders",
				prep: func() Prep {
					// Setup mock block header store.
					b := &mockBlockHeaderStore{}
					args := mock.Anything
					b.On("WriteHeaders", args).Return(nil)

					// Setup mock filter header store.
					f := &mockFilterHeaderStore{}
					f.On("WriteHeaders", args).Return(nil)

					// Configure import options.
					ops := &ImportOptions{
						TargetBlockHeaderStore:  b,
						TargetFilterHeaderStore: f,
					}

					headersImport := &HeadersImport{
						options: ops,
					}

					return Prep{
						hImport:       headersImport,
						blockHeaders:  nil,
						filterHeaders: nil,
						cleanup:       func() {},
					}
				},
				verify: func(v Verify) {},
			},
			{
				name: "StoreBothBlockAndFilterHeaders",
				prep: func() Prep {
					tempDir, err := os.MkdirTemp(
						"", "headers_store_test",
					)
					c1 := func() {
						os.RemoveAll(tempDir)
					}
					if err != nil {
						return Prep{
							cleanup: c1,
							err:     err,
						}
					}

					dbPath := filepath.Join(
						tempDir, "test.db",
					)
					db, err := walletdb.Create(
						"bdb", dbPath, true,
						time.Second*10,
					)
					cleanup := func() {
						db.Close()
						c1()
					}
					if err != nil {
						return Prep{
							cleanup: cleanup,
							err:     err,
						}
					}

					b, err := headerfs.NewBlockHeaderStore(
						tempDir, db,
						&chaincfg.SimNetParams,
					)
					if err != nil {
						return Prep{
							cleanup: cleanup,
							err:     err,
						}
					}

					f, err := headerfs.NewFilterHeaderStore(
						tempDir, db,
						headerfs.RegularFilter,
						&chaincfg.SimNetParams,
						nil,
					)
					if err != nil {
						return Prep{
							cleanup: cleanup,
							err:     err,
						}
					}

					// Prep block headers to write to the
					// target headers store. Ignore the
					// genesis block header since
					// NewBlockHeaderStore already wrote it.
					nBHs := len(blockHdrs)
					blkHdrsToWrite := make(
						[]headerfs.BlockHeader,
						nBHs-1,
					)
					for i := 1; i < nBHs; i++ {
						blockHdr := blockHdrs[i]
						h, err := constructBlkHdr(
							blockHdr,
							uint32(i),
						)
						res := Prep{
							cleanup: cleanup,
							err:     err,
						}
						if err != nil {
							return res
						}
						bHValue := h.BlockHeader
						blkHdrsToWrite[i-1] = bHValue
					}

					// Prep filter headers to write to the
					// target headers store. Ignore the
					// genesis filter header since
					// NewFilterHeaderStore already wrote
					// it.
					nFHs := len(filterHdrs)
					filtHdrsToWrite := make(
						[]headerfs.FilterHeader,
						nFHs-1,
					)
					for i := 1; i < nFHs; i++ {
						filterHdr := filterHdrs[i]
						h, err := constructFilterHdr(
							filterHdr,
							uint32(i),
						)
						res := Prep{
							cleanup: cleanup,
							err:     err,
						}
						if err != nil {
							return res
						}
						fH := h.FilterHeader
						filtHdrsToWrite[i-1] = fH
					}

					setLastFilterHeaderHash(
						filtHdrsToWrite, blkHdrsToWrite,
					)

					// Configure import options.
					ops := &ImportOptions{
						TargetBlockHeaderStore:  b,
						TargetFilterHeaderStore: f,
					}

					headersImport := &HeadersImport{
						options: ops,
					}

					return Prep{
						hImport:       headersImport,
						blockHeaders:  blkHdrsToWrite,
						filterHeaders: filtHdrsToWrite,
						cleanup:       cleanup,
					}
				},
				verify: func(v Verify) {
					options := v.hImport.options
					// Verify chain tip of the target block
					// header store.
					tBS := options.TargetBlockHeaderStore
					chainTipB, height, err := tBS.ChainTip()
					require.NoError(v.tc, err)

					// Assert on the height.
					nBHs := len(blockHdrs)
					require.Equal(
						v.tc, uint32(nBHs-1), height,
					)

					// Assert that the known block header at
					// this index matches the retrieved one.
					chainTipBEx, err := constructBlkHdr(
						blockHdrs[nBHs-1],
						uint32(nBHs-1),
					)
					require.NoError(v.tc, err)
					b := chainTipBEx.BlockHeader.BlockHeader
					require.Equal(
						v.tc, b, chainTipB,
					)

					// Verify chain tip of the target filter
					// header store.
					tFS := options.TargetFilterHeaderStore
					chainTipF, height, err := tFS.ChainTip()
					require.NoError(v.tc, err)

					// Assert on the height.
					nFHs := len(filterHdrs)
					require.Equal(
						v.tc, uint32(nFHs-1), height,
					)

					// Assert that the known filter header
					// at this index matches the retrieved
					// one.
					chainTipFEx, err := constructFilterHdr(
						filterHdrs[nFHs-1],
						uint32(nFHs-1),
					)
					require.NoError(v.tc, err)
					f := &chainTipFEx.FilterHash
					require.Equal(
						v.tc, f, chainTipF,
					)
				},
			},
			{
				name: "StoreBlockHeadersOnly",
				prep: func() Prep {
					tempDir, err := os.MkdirTemp(
						"", "block_headers_store_test",
					)
					c1 := func() {
						os.RemoveAll(tempDir)
					}
					if err != nil {
						return Prep{
							cleanup: c1,
							err:     err,
						}
					}

					dbPath := filepath.Join(
						tempDir, "test.db",
					)
					db, err := walletdb.Create(
						"bdb", dbPath, true,
						time.Second*10,
					)
					cleanup := func() {
						db.Close()
						c1()
					}
					if err != nil {
						return Prep{
							cleanup: cleanup,
							err:     err,
						}
					}

					b, err := headerfs.NewBlockHeaderStore(
						tempDir, db,
						&chaincfg.SimNetParams,
					)
					if err != nil {
						return Prep{
							cleanup: cleanup,
							err:     err,
						}
					}

					// Setup mock filter header store.
					f := &mockFilterHeaderStore{}
					args := mock.Anything
					f.On("WriteHeaders", args).Return(nil)

					// Prep block headers to write to the
					// target headers store. Ignore the
					// genesis block header since
					// NewBlockHeaderStore already wrote it.
					nBHs := len(blockHdrs)
					blkHdrsToWrite := make(
						[]headerfs.BlockHeader,
						nBHs-1,
					)
					for i := 1; i < nBHs; i++ {
						blockHdr := blockHdrs[i]
						h, err := constructBlkHdr(
							blockHdr,
							uint32(i),
						)
						res := Prep{
							cleanup: cleanup,
							err:     err,
						}
						if err != nil {
							return res
						}
						bHValue := h.BlockHeader
						blkHdrsToWrite[i-1] = bHValue
					}

					// Configure import options.
					ops := &ImportOptions{
						TargetBlockHeaderStore:  b,
						TargetFilterHeaderStore: f,
					}

					headersImport := &HeadersImport{
						options: ops,
					}

					return Prep{
						hImport:       headersImport,
						blockHeaders:  blkHdrsToWrite,
						filterHeaders: nil,
						cleanup:       cleanup,
					}
				},
				verify: func(v Verify) {
					// Verify chain tip of the target block
					// header store.
					options := v.hImport.options
					tBS := options.TargetBlockHeaderStore
					chainTipB, height, err := tBS.ChainTip()
					require.NoError(v.tc, err)

					// Assert on the height.
					nBHs := len(blockHdrs)
					require.Equal(
						v.tc, uint32(nBHs-1), height,
					)

					// Assert that the known block header at
					// this index matches the retrieved one.
					chainTipBEx, err := constructBlkHdr(
						blockHdrs[nBHs-1],
						uint32(nBHs-1),
					)
					require.NoError(v.tc, err)
					b := chainTipBEx.BlockHeader.BlockHeader
					require.Equal(
						v.tc, b, chainTipB,
					)
				},
			},
			{
				name: "StoreFilterHeadersOnly",
				prep: func() Prep {
					tempDir, err := os.MkdirTemp(
						"", "filter_headers_store_test",
					)
					c1 := func() {
						os.RemoveAll(tempDir)
					}
					if err != nil {
						return Prep{
							cleanup: c1,
							err:     err,
						}
					}

					dbPath := filepath.Join(
						tempDir, "test.db",
					)
					db, err := walletdb.Create(
						"bdb", dbPath, true,
						time.Second*10,
					)
					cleanup := func() {
						db.Close()
						c1()
					}
					if err != nil {
						return Prep{
							cleanup: cleanup,
							err:     err,
						}
					}

					f, err := headerfs.NewFilterHeaderStore(
						tempDir, db,
						headerfs.RegularFilter,
						&chaincfg.SimNetParams,
						nil,
					)
					if err != nil {
						return Prep{
							cleanup: cleanup,
							err:     err,
						}
					}

					// Setup mock block header store.
					b := &mockBlockHeaderStore{}
					args := mock.Anything
					b.On("WriteHeaders", args).Return(nil)

					// Prep filter headers to write to the
					// target headers store. Ignore the
					// genesis filter header since
					// NewFilterHeaderStore already wrote
					// it.
					nFHs := len(filterHdrs)
					filtHdrsToWrite := make(
						[]headerfs.FilterHeader,
						nFHs-1,
					)
					for i := 1; i < nFHs; i++ {
						filterHdr := filterHdrs[i]
						h, err := constructFilterHdr(
							filterHdr,
							uint32(i),
						)
						res := Prep{
							cleanup: cleanup,
							err:     err,
						}
						if err != nil {
							return res
						}
						fH := h.FilterHeader
						filtHdrsToWrite[i-1] = fH
					}

					lbH, err := constructBlkHdr(
						blockHdrs[len(blockHdrs)-1],
						uint32(len(blockHdrs)-1),
					)
					if err != nil {
						return Prep{
							cleanup: cleanup,
							err:     err,
						}
					}
					bH := lbH.BlockHash()
					filtHdrsToWrite[nFHs-2].HeaderHash = bH

					// Configure import options.
					ops := &ImportOptions{
						TargetFilterHeaderStore: f,
						TargetBlockHeaderStore:  b,
					}

					headersImport := &HeadersImport{
						options: ops,
					}

					return Prep{
						hImport:       headersImport,
						blockHeaders:  nil,
						filterHeaders: filtHdrsToWrite,
						cleanup:       cleanup,
					}
				},
				verify: func(v Verify) {
					options := v.hImport.options
					tFS := options.TargetFilterHeaderStore
					_, _, err := tFS.ChainTip()
					expectErrMsg := "target height not " +
						"found in index"
					require.ErrorContains(
						v.tc, err, expectErrMsg,
					)
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				prep := tc.prep()
				t.Cleanup(prep.cleanup)
				require.NoError(t, prep.err)
				err := prep.hImport.writeHeadersToTargetStores(
					prep.blockHeaders, prep.filterHeaders,
					0, 0,
				)
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

	t.Run("Storage/NewHeadersRegion", func(t *testing.T) {
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

type mockHeaderImportSource struct {
	mock.Mock
	path string
}

func (m *mockHeaderImportSource) Open() error {
	args := m.Called()
	return args.Error(0)
}

func (m *mockHeaderImportSource) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *mockHeaderImportSource) GetHeaderMetadata() (*HeaderMetadata, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*HeaderMetadata), args.Error(1)
}

func (m *mockHeaderImportSource) Iterator(start, end int) HeaderIterator {
	args := m.Called(start, end)
	return args.Get(0).(HeaderIterator)
}

func (m *mockHeaderImportSource) GetHeader(index int) (Header, error) {
	args := m.Called(index)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(Header), args.Error(1)
}

func (m *mockHeaderImportSource) GetPath() string {
	args := m.Called()
	return args.String(0)
}

func (m *mockHeaderImportSource) SetPath(path string) {
	m.Called(path)
	m.path = path
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

		// We need to reopen it again to update the file descriptor
		// since AddHeadersImportMetadata closes the file atomically.
		tempFile, err = os.OpenFile(
			tempFile.Name(), os.O_RDWR|os.O_APPEND, 0644,
		)
		if err != nil {
			return nil, cleanup, fmt.Errorf("failed to open file "+
				"for writing headers: %w", err)
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

func constructBlkHdr(blockHeaderHex string,
	height uint32) (*BlockHeader, error) {

	buff, err := hex.DecodeString(blockHeaderHex)
	if err != nil {
		return nil, fmt.Errorf("failed to decode block header hex: %v",
			err)
	}
	reader := bytes.NewReader(buff)
	bHExpected := NewBlockHeader()
	bHExpected.Deserialize(reader, height)
	bH, ok := bHExpected.(*BlockHeader)
	if !ok {
		return nil, errors.New("failed to assert *BlockHeader type")
	}
	return bH, nil
}

func constructFilterHdr(filterHeaderHex string,
	height uint32) (*FilterHeader, error) {

	buff, err := hex.DecodeString(filterHeaderHex)
	if err != nil {
		return nil, fmt.Errorf("failed to decode filter header hex: %v",
			err)
	}
	reader := bytes.NewReader(buff)
	fHExpected := NewFilterHeader()
	fHExpected.Deserialize(reader, height)
	fH, ok := fHExpected.(*FilterHeader)
	if !ok {
		return nil, errors.New("failed to assert *FilterHeader type")
	}
	return fH, nil
}
