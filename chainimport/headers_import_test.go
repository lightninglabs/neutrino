package chainimport

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	_ "github.com/btcsuite/btcwallet/walletdb/bdb"
	"github.com/lightninglabs/neutrino/chainsync"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/mmap"
)

// Block headers for testing captured from simnet network.
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

// Filter headers for testing captured from simnet network.
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

// TestHeadersConjunctionProperty tests the headers conjunction property for
// the import sources. Both block and filter headers are required to be
// present for the import to start.
func TestHeadersConjunctionProperty(t *testing.T) {
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
			_, err := NewHeadersImport(tc.options)
			if tc.expectErr {
				require.ErrorContains(t, err, tc.expectErrMsg)
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestTargetStoreFreshnessDetection tests the logic for detecting whether the
// target header stores are fresh (only contain genesis headers). It covers
// various combinations of block and filter header heights and error cases.
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
			name:      "ErrorOnBlockStore",
			expectErr: true,
			expectBlockStoreErr: errors.New(
				"failed to get target block header",
			),
		},
		{
			name:      "ErrorOnFilterStore",
			expectErr: true,
			expectFilterStoreErr: errors.New(
				"failed to get target filter header",
			),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup mock block header store.
			mockBlockStore := &headerfs.MockBlockHeaderStore{}
			mockBlockStore.On("ChainTip").Return(
				&wire.BlockHeader{}, tc.blockHeight,
				tc.expectBlockStoreErr,
			)

			// Setup mock filter header store.
			mockFilterStore := &headerfs.MockFilterHeaderStore{}
			mockFilterStore.On("ChainTip").Return(
				&chainhash.Hash{}, tc.filterHeight,
				tc.expectFilterStoreErr,
			)

			// Create importer.
			importer := &headersImport{}

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

// TestHeaderMetadataRetrieval tests the header metadata retrieval from the
// import sources. It checks that the header metadata is retrieved correctly
// from the import sources.
func TestHeaderMetadataRetrieval(t *testing.T) {
	t.Parallel()
	type Prep struct {
		hImport *headersImport
		cleanup func()
		err     error
	}
	type Verify struct {
		tc        *testing.T
		hMetadata *headerMetadata
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
				bS := opts.createBlockHeaderImportSrc()
				bS.SetURI(bF.Name())

				headersImport := &headersImport{
					options:                  opts,
					blockHeadersImportSource: bS,
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
			name: "ErrorOnHeaderRead",
			prep: func() Prep {
				// Create block headers empty file.
				bFile, err := os.CreateTemp(
					t.TempDir(), "invalid-block-header-*",
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
				bS := opts.createBlockHeaderImportSrc()
				bS.SetURI(bFile.Name())

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
					err := fmt.Errorf("empty file: %s",
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
				bFS, ok := bS.(*fileHeaderImportSource)
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
				bFS.file = newMmapFile(reader)
				bFS.fileSize = reader.Len()

				// Make sure the metadata is empty.
				bFS.metadata = nil

				headersImport := &headersImport{
					options:                  opts,
					blockHeadersImportSource: bFS,
				}

				return Prep{
					hImport: headersImport,
					cleanup: cleanup,
				}
			},
			verify:       func(Verify) {},
			expectErr:    true,
			expectErrMsg: "failed to read start height",
		},
		{
			name: "ErrorOnUnknownHeaderType",
			prep: func() Prep {
				// Create block headers empty file.
				bFile, err := os.CreateTemp(
					t.TempDir(),
					"invalid-block-header-*",
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
				bs := opts.createBlockHeaderImportSrc()
				bs.SetURI(bFile.Name())

				// Convert to file header import source.
				bFS, ok := bs.(*fileHeaderImportSource)
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
				bFS.file = newMmapFile(reader)
				bFS.fileSize = reader.Len()

				// Make sure the metadata is empty.
				bFS.metadata = nil

				headersImport := &headersImport{
					options:                  opts,
					blockHeadersImportSource: bFS,
				}

				return Prep{
					hImport: headersImport,
					cleanup: cleanup,
				}
			},
			verify:    func(Verify) {},
			expectErr: true,
			expectErrMsg: "failed to get header size: unknown " +
				"header type: 255",
		},
		{
			name: "ErrorOnNoHeadersData",
			prep: func() Prep {
				// Create block headers empty file.
				bFile, err := os.CreateTemp(
					t.TempDir(), "invalid-block-header-*",
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

				// Reopen the file to get an updated file
				// descriptor.
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
				bs := opts.createBlockHeaderImportSrc()
				bs.SetURI(bFile.Name())

				// Convert to file header import source.
				bFS, ok := bs.(*fileHeaderImportSource)
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
				bFS.file = newMmapFile(reader)
				bFS.fileSize = reader.Len()

				// Make sure the metadata is empty.
				bFS.metadata = nil

				headersImport := &headersImport{
					options:                  opts,
					blockHeadersImportSource: bFS,
				}

				return Prep{
					hImport: headersImport,
					cleanup: cleanup,
				}
			},
			verify:       func(Verify) {},
			expectErr:    true,
			expectErrMsg: "no headers available in import source",
		},
		{
			name: "ErrorOnPartialHeadersData",
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

				// Remove the last byte of the file to trigger
				// data corruption.
				fileInfo, err := bFile.Stat()
				if err != nil {
					return Prep{
						cleanup: c1,
						err:     err,
					}
				}
				fileSize := fileInfo.Size()
				if fileSize == 0 {
					err := fmt.Errorf("empty file: %s",
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

				// Configure import options.
				opts := &ImportOptions{}
				bs := opts.createBlockHeaderImportSrc()
				bs.SetURI(bFile.Name())

				// Convert to file header import source.
				bFS, ok := bs.(*fileHeaderImportSource)
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
				bFS.file = newMmapFile(reader)
				bFS.fileSize = reader.Len()

				// Make sure the metadata is empty.
				bFS.metadata = nil

				headersImport := &headersImport{
					options:                  opts,
					blockHeadersImportSource: bFS,
				}

				return Prep{
					hImport: headersImport,
					cleanup: cleanup,
				}
			},
			verify:       func(Verify) {},
			expectErr:    true,
			expectErrMsg: "possible data corruption",
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
				bS := opts.createBlockHeaderImportSrc()
				bS.SetURI(bFile.Name())

				// Force a cache miss by opening the source file
				// for the first time.
				err = bS.Open()
				c2 := func() {
					bS.Close()
					os.Remove(bS.GetURI())
					c1()
				}
				if err != nil {
					return Prep{
						cleanup: c2,
						err:     err,
					}
				}

				// Remove the source file to ensure next call
				// must use cached data.
				err = bS.Close()
				if err != nil {
					return Prep{
						cleanup: c2,
						err:     err,
					}
				}

				headersImport := &headersImport{
					options:                  opts,
					blockHeadersImportSource: bS,
				}

				return Prep{
					hImport: headersImport,
					cleanup: c1,
				}
			},
			verify: func(v Verify) {
				// Next call should result in a cache hit since
				// the file is gone.
				bHdrType := headerfs.Block
				expectBlockMetadata := &headerMetadata{
					importMetadata: &importMetadata{
						bitcoinChainType: wire.SimNet,
						headerType:       bHdrType,
						startHeight:      0,
					},
					endHeight:    4,
					headerSize:   80,
					headersCount: 5,
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

			bS := prep.hImport.blockHeadersImportSource
			metadata, err := bS.GetHeaderMetadata()
			verify := Verify{
				tc:        t,
				hMetadata: metadata,
			}
			if tc.expectErr {
				require.ErrorContains(t, err, tc.expectErrMsg)
				tc.verify(verify)
				return
			}
			require.NoError(t, err)
			tc.verify(verify)
		})
	}
}

// TestHeaderMetadataStorage tests the header metadata storage to the file.
// It checks that the header metadata is stored correctly to the file.
func TestHeaderMetadataStorage(t *testing.T) {
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
					v.file.Name(), os.O_RDONLY, 0644,
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
				after := data[importMetadataSize:]
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
					v.file.Name(), os.O_RDONLY, 0644,
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
				headerTypeOffset := bitcoinChainTypeSize
				sHeightOffset := headerTypeOffset
				sHeightOffset += headerTypeSize
				btcChainType := wire.BitcoinNet(
					binary.LittleEndian.Uint32(
						data[:headerTypeOffset],
					),
				)
				require.Equal(v.tc, wire.TestNet3, btcChainType)

				// Assert on the header type.
				headerType := headerfs.HeaderType(
					data[headerTypeOffset],
				)
				require.Equal(v.tc, headerfs.Block, headerType)

				// Assert on the startHeight.
				hMS := importMetadataSize
				sHeightD := data[sHeightOffset:hMS]
				sHeight := binary.LittleEndian.Uint32(sHeightD)
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
					v.file.Name(), os.O_RDONLY, 0644,
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
				headerTypeOffset := bitcoinChainTypeSize
				sHeightOffset := headerTypeOffset
				sHeightOffset += headerTypeSize
				bitcoinChainType := wire.BitcoinNet(
					binary.LittleEndian.Uint32(
						data[:headerTypeOffset],
					),
				)
				require.Equal(
					v.tc, wire.TestNet4, bitcoinChainType,
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
				hMS := importMetadataSize
				sHeightD := data[sHeightOffset:hMS]
				sHeight := binary.LittleEndian.Uint32(sHeightD)
				require.Equal(v.tc, uint32(3), sHeight)

				// Compare dataBefore with file content.
				after := data[hMS:]
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
				srcFilePath, tc.chainType, tc.headerType,
				tc.startHeight,
			)
			verify := Verify{
				tc:   t,
				file: prep.file,
				data: prep.data,
			}
			if tc.expectErr {
				require.ErrorContains(t, err, tc.expectErrMsg)
				tc.verify(verify)
				return
			}
			require.NoError(t, err)
			tc.verify(verify)
		})
	}
}

// TestOpenFileHeaderImportSources tests the open operation on a file header
// import source.
func TestOpenFileHeaderImportSources(t *testing.T) {
	t.Parallel()
	type Prep struct {
		hImport *headersImport
		cleanup func()
		err     error
	}
	type Verify struct {
		tc      *testing.T
		hImport *headersImport
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
				headersImport := &headersImport{
					options:                   opts,
					blockHeadersImportSource:  nil,
					filterHeadersImportSource: nil,
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
				bS := opts.createBlockHeaderImportSrc()
				headersImport := &headersImport{
					options:                   opts,
					blockHeadersImportSource:  bS,
					filterHeadersImportSource: nil,
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
				fS := opts.createFilterHeaderImportSrc()
				headersImport := &headersImport{
					options:                   opts,
					blockHeadersImportSource:  nil,
					filterHeadersImportSource: fS,
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
				bS := opts.createBlockHeaderImportSrc()
				fS := opts.createFilterHeaderImportSrc()
				headersImport := &headersImport{
					options:                   opts,
					blockHeadersImportSource:  bS,
					filterHeadersImportSource: fS,
					blockHeadersValidator:     nil,
					filterHeadersValidator:    nil,
				}
				return Prep{
					hImport: headersImport,
					cleanup: func() {},
					err:     nil,
				}
			},
			verify:       func(Verify) {},
			expectErr:    true,
			expectErrMsg: "missing required header validators",
		},
		{
			name: "MissingBlockHeaderValidator",
			prep: func() Prep {
				opts := &ImportOptions{}
				bS := opts.createBlockHeaderImportSrc()
				fS := opts.createFilterHeaderImportSrc()
				fV := opts.createFilterHeaderValidator()
				headersImport := &headersImport{
					options:                   opts,
					blockHeadersImportSource:  bS,
					filterHeadersImportSource: fS,
					blockHeadersValidator:     nil,
					filterHeadersValidator:    fV,
				}
				return Prep{
					hImport: headersImport,
					cleanup: func() {},
					err:     nil,
				}
			},
			verify:       func(Verify) {},
			expectErr:    true,
			expectErrMsg: "missing required header validators",
		},
		{
			name: "MissingFilterHeaderValidator",
			prep: func() Prep {
				opts := &ImportOptions{}
				bS := opts.createBlockHeaderImportSrc()
				fS := opts.createFilterHeaderImportSrc()
				bV := opts.createBlockHeaderValidator()
				headersImport := &headersImport{
					options:                   opts,
					blockHeadersImportSource:  bS,
					filterHeadersImportSource: fS,
					blockHeadersValidator:     bV,
					filterHeadersValidator:    nil,
				}
				return Prep{
					hImport: headersImport,
					cleanup: func() {},
					err:     nil,
				}
			},
			verify:       func(Verify) {},
			expectErr:    true,
			expectErrMsg: "missing required header validators",
		},
		{
			name: "ErrorOnBlockFileNotExist",
			prep: func() Prep {
				opts := &ImportOptions{}
				bS := opts.createBlockHeaderImportSrc()
				fS := opts.createFilterHeaderImportSrc()
				bV := opts.createBlockHeaderValidator()
				fV := opts.createFilterHeaderValidator()
				filePath := "/path/to/nonexistent/file"
				bS.SetURI(filePath)
				headersImport := &headersImport{
					options:                   opts,
					blockHeadersImportSource:  bS,
					filterHeadersImportSource: fS,
					blockHeadersValidator:     bV,
					filterHeadersValidator:    fV,
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
				bS := opts.createBlockHeaderImportSrc()
				fS := opts.createFilterHeaderImportSrc()
				bV := opts.createBlockHeaderValidator()
				fV := opts.createFilterHeaderValidator()

				bS.SetURI(bFile.Name())

				filePath := "/path/to/nonexistent/file"
				bS.SetURI(filePath)

				headersImport := &headersImport{
					options:                   opts,
					blockHeadersImportSource:  bS,
					filterHeadersImportSource: fS,
					blockHeadersValidator:     bV,
					filterHeadersValidator:    fV,
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
			name: "ErrorOnGetBlockHeaderMetadata",
			prep: func() Prep {
				// Create block headers empty file.
				blockFile, err := os.CreateTemp(
					t.TempDir(),
					"empty-block-header-*",
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
				bS := opts.createBlockHeaderImportSrc()
				fS := opts.createFilterHeaderImportSrc()
				bV := opts.createBlockHeaderValidator()
				fV := opts.createFilterHeaderValidator()

				bS.SetURI(blockFile.Name())

				headersImport := &headersImport{
					options:                   opts,
					blockHeadersImportSource:  bS,
					filterHeadersImportSource: fS,
					blockHeadersValidator:     bV,
					filterHeadersValidator:    fV,
				}
				return Prep{
					hImport: headersImport,
					cleanup: cleanup,
				}
			},
			verify:       func(Verify) {},
			expectErr:    true,
			expectErrMsg: "failed to read chain type: EOF",
		},
		{
			name: "ErrorOnGetFilterHeaderMetadata",
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
					t.TempDir(), "empty-filter-header-*",
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
				bS := opts.createBlockHeaderImportSrc()
				fS := opts.createFilterHeaderImportSrc()
				bV := opts.createBlockHeaderValidator()
				fV := opts.createFilterHeaderValidator()

				bS.SetURI(bFile.Name())
				fS.SetURI(fFile.Name())

				headersImport := &headersImport{
					options:                   opts,
					blockHeadersImportSource:  bS,
					filterHeadersImportSource: fS,
					blockHeadersValidator:     bV,
					filterHeadersValidator:    fV,
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
			expectErrMsg: "failed to read chain type: EOF",
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
				bS := opts.createBlockHeaderImportSrc()
				fS := opts.createFilterHeaderImportSrc()
				bV := opts.createBlockHeaderValidator()
				fV := opts.createFilterHeaderValidator()

				bS.SetURI(bFile.Name())
				fS.SetURI(fFile.Name())

				headersImport := &headersImport{
					options:                   opts,
					blockHeadersImportSource:  bS,
					filterHeadersImportSource: fS,
					blockHeadersValidator:     bV,
					filterHeadersValidator:    fV,
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
				expectBlockMetadata := &headerMetadata{
					importMetadata: &importMetadata{
						bitcoinChainType: wire.SimNet,
						headerType:       bHdrType,
						startHeight:      0,
					},
					endHeight:    4,
					headerSize:   80,
					headersCount: 5,
				}

				fHdrType := headerfs.RegularFilter
				expectFilterMetadata := &headerMetadata{
					importMetadata: &importMetadata{
						bitcoinChainType: wire.SimNet,
						headerType:       fHdrType,
						startHeight:      0,
					},
					endHeight:    4,
					headerSize:   32,
					headersCount: 5,
				}

				// Verify block header metadata.
				bS := v.hImport.blockHeadersImportSource
				metadata, err := bS.GetHeaderMetadata()
				require.NoError(v.tc, err)
				require.Equal(
					v.tc, expectBlockMetadata, metadata,
				)

				// Verify filter header metadata.
				f := v.hImport.filterHeadersImportSource
				metadata, err = f.GetHeaderMetadata()
				require.NoError(v.tc, err)
				require.Equal(
					v.tc, expectFilterMetadata, metadata,
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
				require.ErrorContains(t, err, tc.expectErrMsg)
				tc.verify(verify)
				return
			}
			require.NoError(t, err)
			tc.verify(verify)
		})
	}
}

// TestImportAndTargetSourcesCompatibilityConstraint tests the import and
// target sources compatibility constraint. It checks that the import and
// target sources are compatible.
func TestImportAndTargetSourcesCompatibilityConstraint(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name         string
		prep         func() *headersImport
		expectErr    bool
		expectErrMsg string
	}{
		{
			name: "ErrorOnGetBlockHeaderMetadata",
			prep: func() *headersImport {
				// Mock block import store.
				bIS := &mockHeaderImportSource{}
				bIS.On("GetHeaderMetadata").Return(
					nil, errors.New("I/O read error"),
				)
				hImport := &headersImport{
					blockHeadersImportSource: bIS,
				}
				return hImport
			},
			expectErr:    true,
			expectErrMsg: "I/O read error",
		},
		{
			name: "ErrorOnGetFilterHeaderMetadata",
			prep: func() *headersImport {
				// Mock block import store.
				bIS := &mockHeaderImportSource{}
				bIS.On("GetHeaderMetadata").Return(
					&headerMetadata{}, nil,
				)

				// Mock filter import store.
				fIS := &mockHeaderImportSource{}
				fIS.On("GetHeaderMetadata").Return(
					nil, errors.New("I/O read error"),
				)

				hImport := &headersImport{
					blockHeadersImportSource:  bIS,
					filterHeadersImportSource: fIS,
				}
				return hImport
			},
			expectErr:    true,
			expectErrMsg: "I/O read error",
		},
		{
			name: "ErrorOnIncorrectBlockHeaderType",
			prep: func() *headersImport {
				// Mock block import store.
				bIS := &mockHeaderImportSource{}
				rFT := headerfs.RegularFilter
				bM := &headerMetadata{
					importMetadata: &importMetadata{
						headerType: rFT,
					},
				}
				bIS.On("GetHeaderMetadata").Return(bM, nil)

				// Mock filter import store.
				fIS := &mockHeaderImportSource{}
				fIS.On("GetHeaderMetadata").Return(
					&headerMetadata{}, nil,
				)

				hImport := &headersImport{
					blockHeadersImportSource:  bIS,
					filterHeadersImportSource: fIS,
				}
				return hImport
			},
			expectErr: true,
			expectErrMsg: "incorrect block header type: expected " +
				"BlockHeader, got RegularFilterHeader",
		},
		{
			name: "ErrorOnIncorrectFilterHeaderType",
			prep: func() *headersImport {
				// Mock block import store.
				bIS := &mockHeaderImportSource{}
				bM := &headerMetadata{
					importMetadata: &importMetadata{
						headerType: headerfs.Block,
					},
				}
				bIS.On("GetHeaderMetadata").Return(bM, nil)

				// Mock filter import store.
				fIS := &mockHeaderImportSource{}
				fM := &headerMetadata{
					importMetadata: &importMetadata{
						headerType: headerfs.Block,
					},
				}
				fIS.On("GetHeaderMetadata").Return(fM, nil)

				hImport := &headersImport{
					blockHeadersImportSource:  bIS,
					filterHeadersImportSource: fIS,
				}
				return hImport
			},
			expectErr: true,
			expectErrMsg: "incorrect filter header type: " +
				"expected RegularFilterHeader, got BlockHeader",
		},
		{
			name: "ErrorOnMismatchImportChainTypes",
			prep: func() *headersImport {
				// Mock block import store.
				bIS := &mockHeaderImportSource{}
				bT := headerfs.Block
				bM := &headerMetadata{
					importMetadata: &importMetadata{
						bitcoinChainType: wire.MainNet,
						headerType:       bT,
					},
				}
				bIS.On("GetHeaderMetadata").Return(bM, nil)
				bIS.On("GetURI").Return("/path/to/blocks")

				// Mock filter import store.
				fIS := &mockHeaderImportSource{}
				rFT := headerfs.RegularFilter
				fM := &headerMetadata{
					importMetadata: &importMetadata{
						bitcoinChainType: wire.SimNet,
						headerType:       rFT,
					},
				}
				fIS.On("GetHeaderMetadata").Return(fM, nil)
				fIS.On("GetURI").Return("/path/to/filters")

				hImport := &headersImport{
					blockHeadersImportSource:  bIS,
					filterHeadersImportSource: fIS,
				}
				return hImport
			},
			expectErr: true,
			expectErrMsg: fmt.Sprintf("network type mismatch: "+
				"block headers from /path/to/blocks (%s), "+
				"filter headers from /path/to/filters (%s)",
				wire.MainNet, wire.SimNet),
		},
		{
			name: "ErrorOnMismatchImportTargetChainTypes",
			prep: func() *headersImport {
				// Mock block import store.
				bIS := &mockHeaderImportSource{}
				bT := headerfs.Block
				bM := &headerMetadata{
					importMetadata: &importMetadata{
						bitcoinChainType: wire.SimNet,
						headerType:       bT,
					},
				}
				bIS.On("GetHeaderMetadata").Return(bM, nil)

				// Mock filter import store.
				fIS := &mockHeaderImportSource{}
				rFT := headerfs.RegularFilter
				fM := &headerMetadata{
					importMetadata: &importMetadata{
						bitcoinChainType: wire.SimNet,
						headerType:       rFT,
					},
				}
				fIS.On("GetHeaderMetadata").Return(fM, nil)

				// Configure target chain parameters.
				tCP := chaincfg.Params{
					Net: wire.MainNet,
				}

				// Configure import options.
				ops := &ImportOptions{
					TargetChainParams: tCP,
				}

				hImport := &headersImport{
					blockHeadersImportSource:  bIS,
					filterHeadersImportSource: fIS,
					options:                   ops,
				}
				return hImport
			},
			expectErr: true,
			expectErrMsg: fmt.Sprintf("network mismatch: headers "+
				"from import sources are for %s, but target "+
				"is %s", wire.SimNet, wire.MainNet),
		},
		{
			name: "ErrorOnMismatchImportStartHeights",
			prep: func() *headersImport {
				// Mock block import store.
				bIS := &mockHeaderImportSource{}
				bT := headerfs.Block
				bM := &headerMetadata{
					importMetadata: &importMetadata{
						bitcoinChainType: wire.SimNet,
						headerType:       bT,
						startHeight:      1,
					},
				}
				bIS.On("GetHeaderMetadata").Return(bM, nil)
				bIS.On("GetURI").Return("/path/to/blocks")

				// Mock filter import store.
				fIS := &mockHeaderImportSource{}
				rFT := headerfs.RegularFilter
				fM := &headerMetadata{
					importMetadata: &importMetadata{
						bitcoinChainType: wire.SimNet,
						headerType:       rFT,
						startHeight:      3,
					},
				}
				fIS.On("GetHeaderMetadata").Return(fM, nil)
				fIS.On("GetURI").Return("/path/to/filters")

				// Configure target chain parameters.
				tCP := chaincfg.Params{
					Net: wire.SimNet,
				}

				// Configure import options.
				ops := &ImportOptions{
					TargetChainParams: tCP,
				}

				hImport := &headersImport{
					blockHeadersImportSource:  bIS,
					filterHeadersImportSource: fIS,
					options:                   ops,
				}
				return hImport
			},
			expectErr: true,
			expectErrMsg: "start height mismatch: block headers " +
				"start at 1, filter headers start at 3",
		},
		{
			name: "ErrorOnMismatchImportHeadersCount",
			prep: func() *headersImport {
				// Mock block import store.
				bIS := &mockHeaderImportSource{}
				bT := headerfs.Block
				bM := &headerMetadata{
					importMetadata: &importMetadata{
						bitcoinChainType: wire.SimNet,
						headerType:       bT,
						startHeight:      1,
					},
					headersCount: 3,
				}
				bIS.On("GetHeaderMetadata").Return(bM, nil)
				bIS.On("GetURI").Return("/path/to/blocks")

				// Mock filter import store.
				fIS := &mockHeaderImportSource{}
				rFT := headerfs.RegularFilter
				fM := &headerMetadata{
					importMetadata: &importMetadata{
						bitcoinChainType: wire.SimNet,
						headerType:       rFT,
						startHeight:      1,
					},
					headersCount: 5,
				}
				fIS.On("GetHeaderMetadata").Return(fM, nil)
				fIS.On("GetURI").Return("/path/to/filters")

				// Configure target chain parameters.
				tCP := chaincfg.Params{
					Net: wire.SimNet,
				}

				// Configure import options.
				ops := &ImportOptions{
					TargetChainParams: tCP,
				}

				hImport := &headersImport{
					blockHeadersImportSource:  bIS,
					filterHeadersImportSource: fIS,
					options:                   ops,
				}
				return hImport
			},
			expectErr: true,
			expectErrMsg: "headers count mismatch: block headers " +
				"import source /path/to/blocks (3), filter " +
				"headers import source /path/to/filters (5)",
		},
		{
			name: "ValidateSourcesSuccessfully",
			prep: func() *headersImport {
				// Mock block import store.
				bIS := &mockHeaderImportSource{}
				bT := headerfs.Block
				bM := &headerMetadata{
					importMetadata: &importMetadata{
						bitcoinChainType: wire.SimNet,
						headerType:       bT,
						startHeight:      1,
					},
					headersCount: 3,
				}
				bIS.On("GetHeaderMetadata").Return(bM, nil)

				// Mock filter import store.
				fIS := &mockHeaderImportSource{}
				rFT := headerfs.RegularFilter
				fM := &headerMetadata{
					importMetadata: &importMetadata{
						bitcoinChainType: wire.SimNet,
						headerType:       rFT,
						startHeight:      1,
					},
					headersCount: 3,
				}
				fIS.On("GetHeaderMetadata").Return(fM, nil)

				// Configure target chain parameters.
				tCP := chaincfg.Params{
					Net: wire.SimNet,
				}

				// Configure import options.
				ops := &ImportOptions{
					TargetChainParams: tCP,
				}

				hImport := &headersImport{
					blockHeadersImportSource:  bIS,
					filterHeadersImportSource: fIS,
					options:                   ops,
				}
				return hImport
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			hImport := tc.prep()
			err := hImport.validateSourcesCompatibility()
			if tc.expectErr {
				require.ErrorContains(t, err, tc.expectErrMsg)
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestImportAndTargetSourcesChainContinuityConstraint tests the import and
// target sources chain continuity constraint. It checks that the import and
// target sources are continuous.
func TestImportAndTargetSourcesChainContinuityConstraint(t *testing.T) {
	t.Parallel()
	type Prep struct {
		hImport *headersImport
		err     error
	}
	testCases := []struct {
		name         string
		prep         func() Prep
		expectErr    bool
		expectErrMsg string
	}{
		{
			name: "ErrorOnGetHeaderMetadata",
			prep: func() Prep {
				// Mock block import store.
				bIS := &mockHeaderImportSource{}
				bIS.On("GetHeaderMetadata").Return(
					nil, errors.New("I/O read error"),
				)
				hImport := &headersImport{
					blockHeadersImportSource: bIS,
				}
				return Prep{
					hImport: hImport,
				}
			},
			expectErr: true,
			expectErrMsg: "failed to get block header metadata: " +
				"I/O read error",
		},
		{
			name: "ErrorOnGetChainTipForTargetBlockStore",
			prep: func() Prep {
				// Mock block import source.
				bIS := &mockHeaderImportSource{}
				bIS.On("GetHeaderMetadata").Return(
					&headerMetadata{}, nil,
				)

				// Mock target block header store.
				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("ChainTip").Return(
					&wire.BlockHeader{}, uint32(0),
					errors.New("I/O read error"),
				)

				// Configure Import options.
				ops := &ImportOptions{
					TargetBlockHeaderStore: bHS,
				}

				h := &headersImport{
					blockHeadersImportSource: bIS,
					options:                  ops,
				}

				return Prep{
					hImport: h,
				}
			},
			expectErr: true,
			expectErrMsg: "failed to get target block " +
				"header chain tip: I/O read error",
		},
		{
			name: "ErrorOnGetChainTipForTargetFilterStore",
			prep: func() Prep {
				// Mock block import source.
				bIS := &mockHeaderImportSource{}
				bIS.On("GetHeaderMetadata").Return(
					&headerMetadata{}, nil,
				)

				// Mock target block header store.
				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("ChainTip").Return(
					&wire.BlockHeader{}, uint32(0), nil,
				)

				// Mock target filter header store.
				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("ChainTip").Return(
					&chainhash.Hash{}, uint32(0),
					errors.New("I/O read error"),
				)

				// Configure Import options.
				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				h := &headersImport{
					blockHeadersImportSource: bIS,
					options:                  ops,
				}

				return Prep{
					hImport: h,
				}
			},
			expectErr: true,
			expectErrMsg: "failed to get target filter header " +
				"chain tip: I/O read error",
		},
		{
			name: "ErrorOnDivergenceInTargetHeaderStores",
			prep: func() Prep {
				// Mock block import source.
				bIS := &mockHeaderImportSource{}
				bIS.On("GetHeaderMetadata").Return(
					&headerMetadata{}, nil,
				)

				// Mock target block header store.
				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("ChainTip").Return(
					&wire.BlockHeader{}, uint32(0), nil,
				)

				// Mock target filter header store.
				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("ChainTip").Return(
					&chainhash.Hash{}, uint32(2), nil,
				)

				// Configure Import options.
				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				h := &headersImport{
					blockHeadersImportSource: bIS,
					options:                  ops,
				}

				return Prep{
					hImport: h,
				}
			},
			expectErr: true,
			expectErrMsg: "divergence detected between target " +
				"header store tip heights (block=0, filter=2)",
		},
		{
			name: "ErrorOnImportStartHeightCreatesGap",
			prep: func() Prep {
				// Mock block import source.
				bIS := &mockHeaderImportSource{}
				metadata := &headerMetadata{
					importMetadata: &importMetadata{
						startHeight: 2,
					},
				}
				bIS.On("GetHeaderMetadata").Return(
					metadata, nil,
				)

				// Mock target block header store.
				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("ChainTip").Return(
					&wire.BlockHeader{}, uint32(0), nil,
				)

				// Mock target filter header store.
				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("ChainTip").Return(
					&chainhash.Hash{}, uint32(0), nil,
				)

				// Configure Import options.
				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				h := &headersImport{
					blockHeadersImportSource: bIS,
					options:                  ops,
				}

				return Prep{
					hImport: h,
				}
			},
			expectErr: true,
			expectErrMsg: "target stores contain only genesis " +
				"block (height 0) but import data starts at " +
				"height 2, creating a gap",
		},
		{
			name: "ErrorOnGenesisBlockHeadersMismatch",
			prep: func() Prep {
				// Prep block header for testing.
				bH, err := constructBlkHdr(
					blockHdrs[0], uint32(0),
				)
				if err != nil {
					return Prep{err: err}
				}

				// Mock block import source.
				bIS := &mockHeaderImportSource{}
				importMetadata := &importMetadata{}
				bIS.On("GetHeaderMetadata").Return(
					&headerMetadata{
						importMetadata: importMetadata,
					}, nil,
				)
				bIS.On("GetHeader", uint32(0)).Return(
					newBlockHeader(), nil,
				)

				// Mock target block header store.
				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("ChainTip").Return(
					&wire.BlockHeader{}, uint32(0), nil,
				)
				bHS.On("FetchHeaderByHeight", uint32(0)).Return(
					bH.BlockHeader.BlockHeader, nil,
				)

				// Mock target filter header store.
				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("ChainTip").Return(
					&chainhash.Hash{}, uint32(0), nil,
				)

				// Configure Import options.
				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				h := &headersImport{
					blockHeadersImportSource: bIS,
					options:                  ops,
				}

				return Prep{
					hImport: h,
				}
			},
			expectErr: true,
			expectErrMsg: "genesis header mismatch: block header " +
				"mismatch at height 0",
		},
		{
			name: "ErrorOnGenesisFilterHeadersMismatch",
			prep: func() Prep {
				// Prep block header for testing.
				bH, err := constructBlkHdr(
					blockHdrs[0], uint32(0),
				)
				if err != nil {
					return Prep{err: err}
				}

				// Prep filter header for testing.
				fH, err := constructFilterHdr(
					filterHdrs[0], uint32(0),
				)
				if err != nil {
					return Prep{err: err}
				}

				// Mock block import source.
				bIS := &mockHeaderImportSource{}
				importMetadata := &importMetadata{}
				bIS.On("GetHeaderMetadata").Return(
					&headerMetadata{
						importMetadata: importMetadata,
					}, nil,
				)
				bIS.On("GetHeader", uint32(0)).Return(bH, nil)

				// Mock target block header store.
				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("ChainTip").Return(
					&wire.BlockHeader{}, uint32(0), nil,
				)
				bHS.On("FetchHeaderByHeight", uint32(0)).Return(
					bH.BlockHeader.BlockHeader, nil,
				)

				// Mock target filter header store.
				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("ChainTip").Return(
					&chainhash.Hash{}, uint32(0), nil,
				)
				fHS.On("FetchHeaderByHeight", uint32(0)).Return(
					&fH.FilterHash, nil,
				)

				// Mock import filter header store.
				fIS := &mockHeaderImportSource{}
				fIS.On("GetHeaderMetadata").Return(
					&headerMetadata{}, nil,
				)
				fIS.On("GetHeader", uint32(0)).Return(
					newFilterHeader(), nil,
				)

				// Configure Import options.
				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				h := &headersImport{
					blockHeadersImportSource:  bIS,
					filterHeadersImportSource: fIS,
					options:                   ops,
				}

				return Prep{
					hImport: h,
				}
			},
			expectErr: true,
			expectErrMsg: "genesis header mismatch: filter " +
				"header mismatch at height 0",
		},
		{
			name: "ValidateOnGenesisHeightSuccessfully",
			prep: func() Prep {
				// Prep block header for testing.
				bH, err := constructBlkHdr(
					blockHdrs[0], uint32(0),
				)
				if err != nil {
					return Prep{err: err}
				}

				// Prep filter header for testing.
				fH, err := constructFilterHdr(
					filterHdrs[0], uint32(0),
				)
				if err != nil {
					return Prep{err: err}
				}

				// Mock block import source.
				bIS := &mockHeaderImportSource{}
				importMetadata := &importMetadata{}
				bIS.On("GetHeaderMetadata").Return(
					&headerMetadata{
						importMetadata: importMetadata,
					}, nil,
				)
				bIS.On("GetHeader", uint32(0)).Return(bH, nil)

				// Mock target block header store.
				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("ChainTip").Return(
					&wire.BlockHeader{}, uint32(0), nil,
				)
				bHS.On("FetchHeaderByHeight", uint32(0)).Return(
					bH.BlockHeader.BlockHeader, nil,
				)
				bIS.On("GetURI").Return("/path/to/blocks")

				// Mock target filter header store.
				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("ChainTip").Return(
					&chainhash.Hash{}, uint32(0), nil,
				)
				fHS.On("FetchHeaderByHeight", uint32(0)).Return(
					&fH.FilterHash, nil,
				)

				// Mock import filter header store.
				fIS := &mockHeaderImportSource{}
				fIS.On("GetHeaderMetadata").Return(
					&headerMetadata{}, nil,
				)
				fIS.On("GetHeader", uint32(0)).Return(
					fH, nil,
				)
				fIS.On("GetURI").Return("/path/to/filters")

				// Configure Import options.
				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				h := &headersImport{
					blockHeadersImportSource:  bIS,
					filterHeadersImportSource: fIS,
					options:                   ops,
				}

				return Prep{
					hImport: h,
				}
			},
		},
		{
			name: "ErrorOnGetBlockHeaderFromTargetStore",
			prep: func() Prep {
				// Mock block import source.
				bIS := &mockHeaderImportSource{}
				metadata := &headerMetadata{
					importMetadata: &importMetadata{
						startHeight: 1,
					},
				}
				bIS.On("GetHeaderMetadata").Return(
					metadata, nil,
				)
				bIS.On("GetHeader", uint32(0)).Return(
					newBlockHeader(), nil,
				)

				// Mock target block header store.
				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("ChainTip").Return(
					&wire.BlockHeader{}, uint32(0), nil,
				)
				bHS.On("FetchHeaderByHeight", uint32(0)).Return(
					nil, errors.New("I/O read error"),
				)

				// Mock target filter header store.
				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("ChainTip").Return(
					&chainhash.Hash{}, uint32(0), nil,
				)

				// Configure Import options.
				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				h := &headersImport{
					blockHeadersImportSource: bIS,
					options:                  ops,
				}

				return Prep{
					hImport: h,
				}
			},
			expectErr: true,
			expectErrMsg: "failed to get block header from " +
				"target at height 0: I/O read error",
		},
		{
			name: "ErrorOnGetBlockHeaderFromImportSource",
			prep: func() Prep {
				// Prep block header for testing.
				bH, err := constructBlkHdr(
					blockHdrs[0], uint32(0),
				)
				if err != nil {
					return Prep{err: err}
				}

				// Mock block import source.
				bIS := &mockHeaderImportSource{}
				metadata := &headerMetadata{
					importMetadata: &importMetadata{
						startHeight: 1,
					},
				}
				bIS.On("GetHeaderMetadata").Return(
					metadata, nil,
				)
				bIS.On("GetHeader", uint32(0)).Return(
					nil, errors.New("I/O read error"),
				)

				// Mock target block header store.
				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("ChainTip").Return(
					&wire.BlockHeader{}, uint32(0),
					nil,
				)
				bHS.On("FetchHeaderByHeight", uint32(0)).Return(
					bH.BlockHeader.BlockHeader, nil,
				)

				// Mock target filter header store.
				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("ChainTip").Return(
					&chainhash.Hash{}, uint32(0), nil,
				)

				// Configure Import options.
				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				h := &headersImport{
					blockHeadersImportSource: bIS,
					options:                  ops,
				}

				return Prep{
					hImport: h,
				}
			},
			expectErr: true,
			expectErrMsg: "failed to get block header from " +
				"import source at height 1: I/O read error",
		},
		{
			name: "ErrorOnTypeAssertingBlockHeader",
			prep: func() Prep {
				// Prep block header for testing.
				bH, err := constructBlkHdr(
					blockHdrs[0], uint32(0),
				)
				if err != nil {
					return Prep{err: err}
				}

				// Mock block import source.
				bIS := &mockHeaderImportSource{}
				metadata := &headerMetadata{
					importMetadata: &importMetadata{
						startHeight: 1,
					},
				}
				bIS.On("GetHeaderMetadata").Return(
					metadata, nil,
				)
				bIS.On("GetHeader", uint32(0)).Return(
					newFilterHeader(), nil,
				)

				// Mock target block header store.
				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("ChainTip").Return(
					&wire.BlockHeader{}, uint32(0), nil,
				)
				bHS.On(
					"FetchHeaderByHeight", uint32(0),
				).Return(
					bH.BlockHeader.BlockHeader, nil,
				)

				// Mock target filter header store.
				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("ChainTip").Return(
					&chainhash.Hash{}, uint32(0), nil,
				)

				// Configure Import options.
				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				h := &headersImport{
					blockHeadersImportSource: bIS,
					options:                  ops,
				}

				return Prep{
					hImport: h,
				}
			},
			expectErr: true,
			expectErrMsg: "expected blockHeader type, got " +
				"*chainimport.filterHeader",
		},
		{
			name: "ErrorOnFirstHeaderChainBroken",
			prep: func() Prep {
				// Prep block header for testing.
				bH, err := constructBlkHdr(
					blockHdrs[0], uint32(0),
				)
				if err != nil {
					return Prep{err: err}
				}

				// Mock block import source.
				bIS := &mockHeaderImportSource{}
				metadata := &headerMetadata{
					importMetadata: &importMetadata{
						startHeight: 1,
					},
				}
				bIS.On("GetHeaderMetadata").Return(
					metadata, nil,
				)
				bIS.On("GetHeader", uint32(0)).Return(
					newBlockHeader(), nil,
				)

				// Mock target block header store.
				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("ChainTip").Return(
					&wire.BlockHeader{}, uint32(0), nil,
				)
				bHS.On("FetchHeaderByHeight", uint32(0)).Return(
					bH.BlockHeader.BlockHeader, nil,
				)

				// Mock target filter header store.
				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("ChainTip").Return(
					&chainhash.Hash{}, uint32(0), nil,
				)

				// Configure Import options.
				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				h := &headersImport{
					blockHeadersImportSource: bIS,
					options:                  ops,
				}

				return Prep{
					hImport: h,
				}
			},
			expectErr: true,
			expectErrMsg: "failed to validate header connection: " +
				"header chain broken",
		},
		{
			name: "ValidateOnHeightOneSuccessfully",
			prep: func() Prep {
				// Prep block header for testing.
				bH, err := constructBlkHdr(
					blockHdrs[0], uint32(0),
				)
				if err != nil {
					return Prep{err: err}
				}

				// Prep block header for testing.
				bH2, err := constructBlkHdr(
					blockHdrs[1], uint32(1),
				)
				if err != nil {
					return Prep{err: err}
				}

				// Mock block import source.
				bIS := &mockHeaderImportSource{}
				metadata := &headerMetadata{
					importMetadata: &importMetadata{
						startHeight: 1,
					},
				}
				bIS.On("GetHeaderMetadata").Return(
					metadata, nil,
				)
				bIS.On("GetHeader", uint32(0)).Return(
					bH2, nil,
				)

				// Mock target block header store.
				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("ChainTip").Return(
					&wire.BlockHeader{}, uint32(0), nil,
				)
				bHS.On("FetchHeaderByHeight", uint32(0)).Return(
					bH.BlockHeader.BlockHeader, nil,
				)

				// Mock target filter header store.
				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("ChainTip").Return(
					&chainhash.Hash{}, uint32(0), nil,
				)

				// Configure Import options.
				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				h := &headersImport{
					blockHeadersImportSource: bIS,
					options:                  ops,
				}

				return Prep{
					hImport: h,
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			prep := tc.prep()
			require.NoError(t, prep.err)
			err := prep.hImport.validateChainContinuity()
			if tc.expectErr {
				require.ErrorContains(t, err, tc.expectErrMsg)
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestImportAndTargetSourcesHeadersVerificationConstraint tests the import and
// target sources headers verification constraint. It checks that the import
// and target sources are verified correctly.
func TestImportAndTargetSourcesHeadersVerificationConstraint(t *testing.T) {
	t.Parallel()
	type Prep struct {
		hI  *headersImport
		err error
	}
	testCases := []struct {
		name         string
		height       uint32
		prep         func(height uint32) Prep
		expectErr    bool
		expectErrMsg string
	}{
		{
			name: "ErrorOnGetHeaderMetadata",
			prep: func(uint32) Prep {
				// Mock block import store.
				bIS := &mockHeaderImportSource{}
				bIS.On("GetHeaderMetadata").Return(
					nil, errors.New("I/O read error"),
				)
				hImport := &headersImport{
					blockHeadersImportSource: bIS,
				}
				return Prep{
					hI: hImport,
				}
			},
			expectErr: true,
			expectErrMsg: "failed to get block header metadata: " +
				"I/O read error",
		},
		{
			name:   "ErrorOnGetBlockHeaderFromImportSrc",
			height: 101,
			prep: func(height uint32) Prep {
				// Mock block import store.
				bIS := &mockHeaderImportSource{}
				bIS.On("GetHeaderMetadata").Return(
					&headerMetadata{
						importMetadata: &importMetadata{
							startHeight: uint32(1),
						},
					}, nil,
				)
				importIndx := height - 1
				bIS.On("GetHeader", importIndx).Return(
					nil, errors.New("I/O read error"),
				)
				hImport := &headersImport{
					blockHeadersImportSource: bIS,
				}
				return Prep{
					hI: hImport,
				}
			},
			expectErr: true,
			expectErrMsg: "failed to get block header from " +
				"import source at height 101: I/O read error",
		},
		{
			name:   "ErrorOnTypeAssertingBlockHeader",
			height: 101,
			prep: func(height uint32) Prep {
				// Mock block import store.
				bIS := &mockHeaderImportSource{}
				bIS.On("GetHeaderMetadata").Return(
					&headerMetadata{
						importMetadata: &importMetadata{
							startHeight: uint32(1),
						},
					}, nil,
				)
				importIndx := height - 1
				bIS.On("GetHeader", importIndx).Return(
					newFilterHeader(), nil,
				)
				hImport := &headersImport{
					blockHeadersImportSource: bIS,
				}
				return Prep{
					hI: hImport,
				}
			},
			expectErr: true,
			expectErrMsg: "expected blockHeader type, got " +
				"*chainimport.filterHeader",
		},
		{
			name:   "ErrorOnGetBlockHeaderFromTargetStore",
			height: 101,
			prep: func(height uint32) Prep {
				// Mock block import store.
				bIS := &mockHeaderImportSource{}
				bIS.On("GetHeaderMetadata").Return(
					&headerMetadata{
						importMetadata: &importMetadata{
							startHeight: uint32(1),
						},
					}, nil,
				)
				importIndx := height - 1
				bIS.On("GetHeader", importIndx).Return(
					newBlockHeader(), nil,
				)

				// Mock block target store on target
				// header store.
				bHS := &headerfs.MockBlockHeaderStore{}
				h := height
				bHS.On("FetchHeaderByHeight", h).Return(
					nil, errors.New("I/O read error"),
				)

				// Configure import options.
				ops := &ImportOptions{
					TargetBlockHeaderStore: bHS,
				}

				hImport := &headersImport{
					blockHeadersImportSource: bIS,
					options:                  ops,
				}
				return Prep{
					hI: hImport,
				}
			},
			expectErr: true,
			expectErrMsg: "failed to get block header from " +
				"target at height 101: I/O read error",
		},
		{
			name:   "ErrorOnBlockHeadersMismatch",
			height: 101,
			prep: func(height uint32) Prep {
				// Prep block header for testing.
				bH, err := constructBlkHdr(
					blockHdrs[1], uint32(1),
				)
				if err != nil {
					return Prep{err: err}
				}

				// Mock block import store.
				bIS := &mockHeaderImportSource{}
				bIS.On("GetHeaderMetadata").Return(
					&headerMetadata{
						importMetadata: &importMetadata{
							startHeight: uint32(1),
						},
					}, nil,
				)
				importIndx := height - 1
				bIS.On("GetHeader", importIndx).Return(
					newBlockHeader(), nil,
				)

				// Mock target block header store.
				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("FetchHeaderByHeight", height).Return(
					bH.BlockHeader.BlockHeader, nil,
				)

				// Configure import options.
				ops := &ImportOptions{
					TargetBlockHeaderStore: bHS,
				}

				hImport := &headersImport{
					blockHeadersImportSource: bIS,
					options:                  ops,
				}
				return Prep{
					hI: hImport,
				}
			},
			expectErr:    true,
			expectErrMsg: "block header mismatch at height 101",
		},
		{
			name:   "ErrorOnGetFilterHeaderFromImportSrc",
			height: 101,
			prep: func(height uint32) Prep {
				// Prep block header for testing.
				bH, err := constructBlkHdr(
					blockHdrs[1], uint32(1),
				)
				if err != nil {
					return Prep{err: err}
				}

				// Mock block import store.
				bIS := &mockHeaderImportSource{}
				bIS.On("GetHeaderMetadata").Return(
					&headerMetadata{
						importMetadata: &importMetadata{
							startHeight: uint32(1),
						},
					}, nil,
				)
				importIndx := height - 1
				bIS.On("GetHeader", importIndx).Return(bH, nil)

				// Mock target block header store.
				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("FetchHeaderByHeight", height).Return(
					bH.BlockHeader.BlockHeader, nil,
				)

				// Mock filter import store.
				fIS := &mockHeaderImportSource{}
				fIS.On("GetHeaderMetadata").Return(
					&headerMetadata{
						importMetadata: &importMetadata{
							startHeight: uint32(1),
						},
					}, nil,
				)
				importIndx = height - 1
				fIS.On("GetHeader", importIndx).Return(
					nil, errors.New("I/O read error"),
				)

				// Configure import options.
				ops := &ImportOptions{
					TargetBlockHeaderStore: bHS,
				}

				// Configure headers import.
				hImport := &headersImport{
					blockHeadersImportSource:  bIS,
					filterHeadersImportSource: fIS,
					options:                   ops,
				}

				return Prep{
					hI: hImport,
				}
			},
			expectErr: true,
			expectErrMsg: "failed to get filter header from " +
				"import source at height 101: I/O read error",
		},
		{
			name:   "ErrorOnTypeAssertingFilterHeader",
			height: 101,
			prep: func(height uint32) Prep {
				// Prep block header for testing.
				bH, err := constructBlkHdr(
					blockHdrs[1], uint32(1),
				)
				if err != nil {
					return Prep{err: err}
				}

				// Mock block import store.
				bIS := &mockHeaderImportSource{}
				bIS.On("GetHeaderMetadata").Return(
					&headerMetadata{
						importMetadata: &importMetadata{
							startHeight: uint32(1),
						},
					}, nil,
				)
				importIndx := height - 1
				bIS.On("GetHeader", importIndx).Return(
					bH, nil,
				)

				// Mock target block header store.
				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("FetchHeaderByHeight", height).Return(
					bH.BlockHeader.BlockHeader, nil,
				)

				// Mock filter import store.
				fIS := &mockHeaderImportSource{}
				fIS.On("GetHeaderMetadata").Return(
					&headerMetadata{
						importMetadata: &importMetadata{
							startHeight: uint32(1),
						},
					}, nil,
				)
				importIndx = height - 1
				fIS.On("GetHeader", importIndx).Return(bH, nil)

				// Configure import options.
				ops := &ImportOptions{
					TargetBlockHeaderStore: bHS,
				}

				// Configure headers import.
				hImport := &headersImport{
					blockHeadersImportSource:  bIS,
					filterHeadersImportSource: fIS,
					options:                   ops,
				}

				return Prep{
					hI: hImport,
				}
			},
			expectErr: true,
			expectErrMsg: "expected filterHeader type, got " +
				"*chainimport.blockHeader",
		},
		{
			name:   "ErrorOnGetFilterHeaderFromTargetStore",
			height: 101,
			prep: func(height uint32) Prep {
				// Prep block header for testing.
				bH, err := constructBlkHdr(
					blockHdrs[1], uint32(1),
				)
				if err != nil {
					return Prep{err: err}
				}

				// Mock block import store.
				bIS := &mockHeaderImportSource{}
				bIS.On("GetHeaderMetadata").Return(
					&headerMetadata{
						importMetadata: &importMetadata{
							startHeight: uint32(1),
						},
					}, nil,
				)
				importIndx := height - 1
				bIS.On("GetHeader", importIndx).Return(bH, nil)

				// Mock target block header store.
				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("FetchHeaderByHeight", height).Return(
					bH.BlockHeader.BlockHeader, nil,
				)

				// Mock filter import store.
				fIS := &mockHeaderImportSource{}
				fIS.On("GetHeaderMetadata").Return(
					&headerMetadata{
						importMetadata: &importMetadata{
							startHeight: uint32(1),
						},
					}, nil,
				)
				importIndx = height - 1
				fIS.On("GetHeader", importIndx).Return(
					newFilterHeader(), nil,
				)

				// Mock target filter header store.
				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("FetchHeaderByHeight", height).Return(
					nil, errors.New("I/O read error"),
				)

				// Configure import options.
				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				// Configure headers import.
				hImport := &headersImport{
					blockHeadersImportSource:  bIS,
					filterHeadersImportSource: fIS,
					options:                   ops,
				}

				return Prep{
					hI: hImport,
				}
			},
			expectErr: true,
			expectErrMsg: "failed to get filter header from " +
				"target at height 101: I/O read error",
		},
		{
			name:   "ErrorOnFilterHeadersMismatch",
			height: 101,
			prep: func(height uint32) Prep {
				// Prep block header for testing.
				bH, err := constructBlkHdr(
					blockHdrs[1], uint32(1),
				)
				if err != nil {
					return Prep{err: err}
				}

				// Prep filter header for testing.
				fH, err := constructFilterHdr(
					filterHdrs[1], uint32(1),
				)
				if err != nil {
					return Prep{err: err}
				}

				// Mock block import store.
				bIS := &mockHeaderImportSource{}
				bIS.On("GetHeaderMetadata").Return(
					&headerMetadata{
						importMetadata: &importMetadata{
							startHeight: uint32(1),
						},
					}, nil,
				)
				importIndx := height - 1
				bIS.On("GetHeader", importIndx).Return(bH, nil)

				// Mock target block header store.
				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("FetchHeaderByHeight", height).Return(
					bH.BlockHeader.BlockHeader, nil,
				)

				// Mock filter import store.
				fIS := &mockHeaderImportSource{}
				fIS.On("GetHeaderMetadata").Return(
					&headerMetadata{
						importMetadata: &importMetadata{
							startHeight: uint32(1),
						},
					}, nil,
				)
				importIndx = height - 1
				fIS.On("GetHeader", importIndx).Return(
					newFilterHeader(), nil,
				)

				// Mock target filter header store.
				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("FetchHeaderByHeight", height).Return(
					&fH.FilterHash, nil,
				)

				// Configure import options.
				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				// Configure headers import.
				hImport := &headersImport{
					blockHeadersImportSource:  bIS,
					filterHeadersImportSource: fIS,
					options:                   ops,
				}

				return Prep{
					hI: hImport,
				}
			},
			expectErr:    true,
			expectErrMsg: "filter header mismatch at height 101",
		},
		{
			name:   "VerifyHeadersAtTargetHeightWithNoErrs",
			height: 101,
			prep: func(height uint32) Prep {
				// Prep block header for testing.
				bH, err := constructBlkHdr(
					blockHdrs[1], uint32(1),
				)
				if err != nil {
					return Prep{err: err}
				}

				// Prep filter header for testing.
				fH, err := constructFilterHdr(
					filterHdrs[1],
					uint32(1),
				)
				if err != nil {
					return Prep{err: err}
				}

				// Mock block import store.
				bIS := &mockHeaderImportSource{}
				bIS.On("GetHeaderMetadata").Return(
					&headerMetadata{
						importMetadata: &importMetadata{
							startHeight: uint32(1),
						},
					}, nil,
				)
				importIndx := height - 1
				bIS.On("GetHeader", importIndx).Return(bH, nil)
				bIS.On("GetURI").Return("/path/to/blocks")

				// Mock target block header store.
				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("FetchHeaderByHeight", height).Return(
					bH.BlockHeader.BlockHeader, nil,
				)

				// Mock filter import store.
				fIS := &mockHeaderImportSource{}
				fIS.On("GetHeaderMetadata").Return(
					&headerMetadata{
						importMetadata: &importMetadata{
							startHeight: uint32(1),
						},
					}, nil,
				)
				importIndx = height - 1
				fIS.On("GetHeader", importIndx).Return(fH, nil)
				fIS.On("GetURI").Return("/path/to/filters")

				// Mock target filter header store.
				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("FetchHeaderByHeight", height).Return(
					&fH.FilterHash, nil,
				)

				// Configure import options.
				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				// Configure headers import.
				hImport := &headersImport{
					blockHeadersImportSource:  bIS,
					filterHeadersImportSource: fIS,
					options:                   ops,
				}

				return Prep{
					hI: hImport,
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			prep := tc.prep(tc.height)
			require.NoError(t, prep.err)
			err := prep.hI.verifyHeadersAtTargetHeight(tc.height)
			if tc.expectErr {
				require.ErrorContains(t, err, tc.expectErrMsg)
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestHeaderRetrievalOnSingleHeader tests the header retrieval on a single
// header. It checks that the header is retrieved correctly from the import
// source.
func TestHeaderRetrievalOnSingleHeader(t *testing.T) {
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
				bS := opts.createBlockHeaderImportSrc()
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
				bS := opts.createBlockHeaderImportSrc()
				bS.SetURI(bFile.Name())

				// Convert to file header import source.
				bFS, ok := bS.(*fileHeaderImportSource)
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
				bFS.file = newMmapFile(reader)

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
				bS := opts.createBlockHeaderImportSrc()
				bS.SetURI(bFile.Name())

				// Convert to file header import source.
				bFS, ok := bS.(*fileHeaderImportSource)
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
				bFS.file = newMmapFile(reader)

				// Set header metadata.
				bFS.metadata = &headerMetadata{
					importMetadata: &importMetadata{
						headerType: hType,
					},
				}

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
				bFile, c1, err := setupFileWithHdrs(hType, true)
				if err != nil {
					return Prep{
						cleanup: c1,
						err:     err,
					}
				}

				// Configure import options.
				opts := &ImportOptions{}
				bS := opts.createBlockHeaderImportSrc()
				bS.SetURI(bFile.Name())

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
			expectErrMsg: fmt.Sprintf("failed to read header at "+
				"index %d", len(blockHdrs)),
		},
		{
			name:  "GetBlockHeaderSuccessfully",
			hType: headerfs.Block,
			index: 3,
			prep: func(hType headerfs.HeaderType) Prep {
				// Create block headers file.
				bFile, c1, err := setupFileWithHdrs(hType, true)
				if err != nil {
					return Prep{
						cleanup: c1,
						err:     err,
					}
				}

				// Configure import options.
				opts := &ImportOptions{}
				bS := opts.createBlockHeaderImportSrc()
				bS.SetURI(bFile.Name())

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
				bH, ok := v.header.(*blockHeader)
				require.True(v.tc, ok)

				// Construct the expected block header.
				bHExpected, err := constructBlkHdr(
					blockHdrs[v.index], uint32(v.index),
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
				fFile, c1, err := setupFileWithHdrs(hType, true)
				if err != nil {
					return Prep{
						cleanup: c1,
						err:     err,
					}
				}

				// Configure import options.
				opts := &ImportOptions{}
				fS := opts.createFilterHeaderImportSrc()
				fS.SetURI(fFile.Name())

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
				fH, ok := v.header.(*filterHeader)
				require.True(v.tc, ok)

				// Construct the expected filter header.
				fHExpected, err := constructFilterHdr(
					filterHdrs[v.index], uint32(v.index),
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

			header, err := prep.hISource.GetHeader(uint32(tc.index))
			verify := Verify{
				tc:     t,
				header: header,
				index:  tc.index,
			}
			if tc.expectErr {
				require.ErrorContains(t, err, tc.expectErrMsg)
				tc.verify(verify)
				return
			}
			require.NoError(t, err)
			tc.verify(verify)
		})
	}
}

// TestHeaderRetrievalOnSequentialHeaders tests the header retrieval on
// sequential headers. It checks that the header is retrieved correctly from
// the import source.
func TestHeaderRetrievalOnSequentialHeaders(t *testing.T) {
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
				bFile, c1, err := setupFileWithHdrs(hT, true)
				if err != nil {
					return Prep{
						cleanup: c1,
						err:     err,
					}
				}

				// Configure import options.
				opts := &ImportOptions{}
				bS := opts.createBlockHeaderImportSrc()
				bS.SetURI(bFile.Name())

				err = bS.Open()
				c2 := func() {
					bS.Close()
					os.Remove(bFile.Name())
				}
				if err != nil {
					return Prep{
						cleanup: c2,
						err:     err,
					}
				}

				current := uint32(i)
				end := uint32(i)
				iter := &importSourceHeaderIterator{
					source:       bS,
					currentIndex: current,
					endIndex:     end,
					batchSize:    128,
				}

				cleanup := func() {
					iter.Close()
					c2()
				}

				return Prep{
					hIterator: iter,
					cleanup:   cleanup,
				}
			},
			verify: func(v Verify) {
				require.Nil(v.tc, v.header)
			},
			expectErr:    true,
			expectErrMsg: io.EOF.Error(),
		},
		{
			name:  "NoMoreHeadersToIterateOver",
			index: len(blockHdrs) - 1,
			hType: headerfs.Block,
			prep: func(hT headerfs.HeaderType, i int) Prep {
				// Create block headers file.
				bFile, c1, err := setupFileWithHdrs(hT, true)
				if err != nil {
					return Prep{
						cleanup: c1,
						err:     err,
					}
				}

				// Configure import options.
				opts := &ImportOptions{}
				bS := opts.createBlockHeaderImportSrc()
				bS.SetURI(bFile.Name())

				err = bS.Open()
				c2 := func() {
					bS.Close()
					os.Remove(bFile.Name())
				}
				if err != nil {
					return Prep{
						cleanup: c2,
						err:     err,
					}
				}

				current := uint32(i + 1)
				end := uint32(i)
				iter := &importSourceHeaderIterator{
					source:       bS,
					currentIndex: current,
					endIndex:     end,
					batchSize:    128,
				}

				cleanup := func() {
					iter.Close()
					c2()
				}

				return Prep{
					hIterator: iter,
					cleanup:   cleanup,
				}
			},
			verify: func(v Verify) {
				require.Nil(v.tc, v.header)
			},
			expectErr:    true,
			expectErrMsg: io.EOF.Error(),
		},
		{
			name:  "IterateOverBlockHeadersSuccessfully",
			index: 0,
			hType: headerfs.Block,
			prep: func(hT headerfs.HeaderType, i int) Prep {
				// Create block headers file.
				bFile, c1, err := setupFileWithHdrs(hT, true)
				if err != nil {
					return Prep{
						cleanup: c1,
						err:     err,
					}
				}

				// Configure import options.
				opts := &ImportOptions{}
				bS := opts.createBlockHeaderImportSrc()
				bS.SetURI(bFile.Name())

				err = bS.Open()
				c2 := func() {
					bS.Close()
					os.Remove(bFile.Name())
				}
				if err != nil {
					return Prep{
						cleanup: c2,
						err:     err,
					}
				}

				current := uint32(i)
				end := uint32(len(blockHdrs) - 1)
				iter := &importSourceHeaderIterator{
					source:       bS,
					currentIndex: current,
					endIndex:     end,
					batchSize:    128,
				}

				cleanup := func() {
					iter.Close()
					c2()
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
					h, err := iter.Next()
					require.NoError(v.tc, err)
					hE, err := constructBlkHdr(
						blockHdrs[i], uint32(i),
					)
					require.NoError(t, err)
					require.Equal(v.tc, hE, h)
				}
				h, err := iter.Next()
				require.ErrorIs(v.tc, err, io.EOF)
				require.Nil(v.tc, h)
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
				fS := opts.createFilterHeaderImportSrc()
				fS.SetURI(fFile.Name())

				err = fS.Open()
				c2 := func() {
					fS.Close()
					os.Remove(fFile.Name())
				}
				if err != nil {
					return Prep{
						cleanup: c2,
						err:     err,
					}
				}

				current := uint32(i)
				end := uint32(len(filterHdrs) - 1)
				iter := &importSourceHeaderIterator{
					source:       fS,
					currentIndex: current,
					endIndex:     end,
					batchSize:    128,
				}

				cleanup := func() {
					iter.Close()
					c2()
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
					h, err := iter.Next()
					require.NoError(v.tc, err)
					hE, err := constructFilterHdr(
						filterHdrs[i], uint32(i),
					)
					require.NoError(t, err)
					require.Equal(v.tc, hE, h)
				}
				h, err := iter.Next()
				require.ErrorIs(v.tc, err, io.EOF)
				require.Nil(v.tc, h)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			prep := tc.prep(tc.hType, tc.index)
			t.Cleanup(prep.cleanup)
			require.NoError(t, prep.err)

			header, err := prep.hIterator.Next()
			verify := Verify{
				tc:        t,
				hIterator: prep.hIterator,
				header:    header,
			}
			if tc.expectErr {
				require.ErrorContains(t, err, tc.expectErrMsg)
				tc.verify(verify)
				return
			}
			require.NoError(t, err)
			tc.verify(verify)
		})
	}
}

// TestHeaderValidationOnBlockHeadersPair tests the header validation on a
// block header pair. It checks that the header is validated correctly.
func TestHeaderValidationOnBlockHeadersPair(t *testing.T) {
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
				bHV := opts.createBlockHeaderValidator()
				return Prep{
					hValidator: bHV,
					prev:       newFilterHeader(),
					current:    newBlockHeader(),
				}
			},
			expectErr: true,
			expectErrMsg: "expected blockHeader type, got " +
				"\\*chainimport.filterHeader",
		},
		{
			name: "ErrorOnMismatchCurrentHeaderType",
			tCP:  chaincfg.Params{},
			prep: func(chaincfg.Params) Prep {
				opts := &ImportOptions{}
				bHV := opts.createBlockHeaderValidator()
				return Prep{
					hValidator: bHV,
					prev:       newBlockHeader(),
					current:    newFilterHeader(),
				}
			},
			expectErr: true,
			expectErrMsg: "expected blockHeader type, got " +
				"\\*chainimport.filterHeader",
		},
		{
			name: "ErrorOnNonConsecutiveHeaderChain",
			tCP:  chaincfg.Params{},
			prep: func(chaincfg.Params) Prep {
				opts := &ImportOptions{}
				bHV := opts.createBlockHeaderValidator()

				// Construct previous block header.
				prevH, err := constructBlkHdr(
					blockHdrs[0], uint32(0),
				)
				if err != nil {
					return Prep{
						err: err,
					}
				}

				// Construct current block header.
				nBH := len(blockHdrs)
				currentH, err := constructBlkHdr(
					blockHdrs[nBH-1], uint32(nBH-1),
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
			expectErrMsg: "height mismatch: previous height=0, " +
				"current height=4",
		},
		{
			name: "ErrorOnInvalidHeaderHashChain",
			tCP:  chaincfg.Params{},
			prep: func(chaincfg.Params) Prep {
				opts := &ImportOptions{}
				bHV := opts.createBlockHeaderValidator()

				// Construct origin block header.
				originH, err := constructBlkHdr(
					blockHdrs[0], uint32(0),
				)
				if err != nil {
					return Prep{
						err: err,
					}
				}

				// Construct previous block header.
				prevH, err := constructBlkHdr(
					blockHdrs[1], uint32(1),
				)
				if err != nil {
					return Prep{
						err: err,
					}
				}

				// Construct current block header.
				currentH, err := constructBlkHdr(
					blockHdrs[2], uint32(2),
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
			expectErrMsg: "header chain broken: current header's " +
				"PrevBlock (.*) doesn't match previous " +
				"header's hash (.*)",
		},
		{
			name: "ErrorOnZeroDifficultyTarget",
			tCP:  chaincfg.Params{},
			prep: func(chaincfg.Params) Prep {
				opts := &ImportOptions{}
				bHV := opts.createBlockHeaderValidator()

				// Construct previous block header.
				prevH, err := constructBlkHdr(
					blockHdrs[0], uint32(0),
				)
				if err != nil {
					return Prep{
						err: err,
					}
				}

				// Construct current block header.
				currentH, err := constructBlkHdr(
					blockHdrs[1], uint32(1),
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
			expectErrMsg: "block target difficulty of .* is too " +
				"low",
		},
		{
			name: "ErrorOnDifficultyExceedsMaximumAllowed",
			tCP:  chaincfg.SimNetParams,
			prep: func(tCP chaincfg.Params) Prep {
				opts := &ImportOptions{}
				bHV := opts.createBlockHeaderValidator()

				// Construct previous block header.
				prevH, err := constructBlkHdr(
					blockHdrs[0], uint32(0),
				)
				if err != nil {
					return Prep{
						err: err,
					}
				}

				// Construct current block header.
				currentH, err := constructBlkHdr(
					blockHdrs[1], uint32(1),
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
			expectErrMsg: "block target difficulty of .* is " +
				"higher than max of .*",
		},
		{
			name: "ErrorOnHashHigherThanTarget",
			tCP:  chaincfg.SimNetParams,
			prep: func(tCP chaincfg.Params) Prep {
				opts := &ImportOptions{}
				bHV := opts.createBlockHeaderValidator()

				// Construct previous block header.
				prevH, err := constructBlkHdr(
					blockHdrs[0], uint32(0),
				)
				if err != nil {
					return Prep{
						err: err,
					}
				}

				// Construct current block header.
				currentH, err := constructBlkHdr(
					blockHdrs[1], uint32(1),
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
			expectErrMsg: "block hash of .* is higher than " +
				"expected max of .*",
		},
		{
			name: "ErrorOnSubSecondTimestampPrecision",
			tCP:  chaincfg.SimNetParams,
			prep: func(tCP chaincfg.Params) Prep {
				opts := &ImportOptions{}
				bHV := opts.createBlockHeaderValidator()

				// Construct previous block header.
				prevH, err := constructBlkHdr(
					blockHdrs[0], uint32(0),
				)
				if err != nil {
					return Prep{
						err: err,
					}
				}

				// Construct current block header.
				currentH, err := constructBlkHdr(
					blockHdrs[1], uint32(1),
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
			expectErrMsg: "block timestamp of .* has a higher " +
				"precision than one second",
		},
		{
			name: "ValidateBlockHeadersPairSuccessfully",
			tCP:  chaincfg.SimNetParams,
			prep: func(tCP chaincfg.Params) Prep {
				opts := &ImportOptions{}
				bHV := opts.createBlockHeaderValidator()

				// Construct previous block header.
				prevH, err := constructBlkHdr(
					blockHdrs[0], uint32(0),
				)
				if err != nil {
					return Prep{
						err: err,
					}
				}

				// Construct current block header.
				currentH, err := constructBlkHdr(
					blockHdrs[1], uint32(1),
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
				prep.prev, prep.current, tc.tCP,
			)
			if tc.expectErr {
				matched, matchErr := regexp.MatchString(
					tc.expectErrMsg, err.Error(),
				)
				require.NoError(t, matchErr)
				require.True(
					t, matched, "failed with: "+err.Error(),
				)
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestHeaderValidationOnSequentialBlockHeaders tests the header validation on
// sequential block headers. It checks that the header is validated correctly.
func TestHeaderValidationOnSequentialBlockHeaders(t *testing.T) {
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
				bS := opts.createBlockHeaderImportSrc()
				bS.SetURI(bFile.Name())

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
				start := uint32(index - 1)
				end := uint32(index)
				bIterator := bS.Iterator(
					start, end,
					defaultWriteBatchSizePerRegion,
				)

				// Create block validator.
				bV := opts.createBlockHeaderValidator()

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
				bS := opts.createBlockHeaderImportSrc()
				bS.SetURI(bFile.Name())

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
				start := uint32(index + 1)
				end := uint32(index)
				bIterator := bS.Iterator(
					start, end,
					defaultWriteBatchSizePerRegion,
				)

				// Create block validator.
				bV := opts.createBlockHeaderValidator()

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
				bS := opts.createBlockHeaderImportSrc()
				bS.SetURI(bFile.Name())
				err = bS.Open()
				cleanup := func() {
					bS.Close()
					os.Remove(bS.GetURI())
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
					0, meta.headersCount-1,
					defaultWriteBatchSizePerRegion,
				)

				// Create block validator.
				bV := opts.createBlockHeaderValidator()

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
}

// TestHeaderValidationOnSequentialFilterHeaders tests the header validation
// on sequential filter headers. It checks that the header is validated
// correctly.
func TestHeaderValidationOnSequentialFilterHeaders(t *testing.T) {
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
		tCP          *chaincfg.Params
		prep         func() Prep
		expectErr    bool
		expectErrMsg string
	}{
		{
			name: "ErrorOnInvalidSequentialHeaders",
			tCP:  &chaincfg.MainNetParams,
			prep: func() Prep {
				// Create a file with valid filter headers.
				fFile, c1, err := setupFileWithHdrs(
					headerfs.RegularFilter, false,
				)

				if err != nil {
					return Prep{
						cleanup: c1,
						err:     err,
					}
				}

				fFile.Close()

				// Add header metadata to the file. Deliberately
				// make the start height one before the
				// hardcoded checkpointed filter header
				// (height=100000) to evaluate the filter header
				// validation behavior
				err = AddHeadersImportMetadata(
					fFile.Name(), wire.MainNet,
					headerfs.RegularFilter, 99999,
				)
				if err != nil {
					return Prep{
						cleanup: c1,
						err:     err,
					}
				}

				fFile, err = os.OpenFile(
					fFile.Name(), os.O_RDWR, 0644,
				)
				c1 = func() {
					fFile.Close()
					os.Remove(fFile.Name())
				}
				if err != nil {
					return Prep{
						cleanup: c1,
						err:     err,
					}
				}

				// Configure import options
				opts := &ImportOptions{}
				fS := opts.createFilterHeaderImportSrc()
				fS.SetURI(fFile.Name())
				err = fS.Open()
				cleanup := func() {
					fS.Close()
					os.Remove(fS.GetURI())
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
					0, meta.headersCount-1,
					uint32(opts.WriteBatchSizePerRegion),
				)

				// Create filter validator.
				fV := opts.createFilterHeaderValidator()

				return Prep{
					iterator:  fIterator,
					validator: fV,
					cleanup:   cleanup,
				}
			},
			expectErr: true,
			expectErrMsg: "batch validation failed at position " +
				"0: " + chainsync.ErrCheckpointMismatch.Error(),
		},
		{
			name: "ValidSequentialHeaders",
			tCP:  &chaincfg.SimNetParams,
			prep: func() Prep {
				// Create a file with valid filter headers and
				// metadata.
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
				fS := opts.createFilterHeaderImportSrc()
				fS.SetURI(fFile.Name())
				err = fS.Open()
				cleanup := func() {
					fS.Close()
					os.Remove(fS.GetURI())
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
					0, meta.headersCount-1,
					uint32(opts.WriteBatchSizePerRegion),
				)

				// Create filter validator.
				fV := opts.createFilterHeaderValidator()

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
			err := prep.validator.Validate(prep.iterator, *tc.tCP)
			if tc.expectErr {
				require.ErrorContains(t, err, tc.expectErrMsg)
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestHeaderProcessing tests the header processing. It checks that the header
// processing is done correctly.
//
// The goal of these test cases is to determine the processing regions. For
// convenience and shorter test case names, we'll use the following notation:
// - A refers to data from import sources. No divergence.
// - B refers to data from target sources when both target stores have the same
// length. In case of divergence, B specifically refers to the target block
// headers store, while F refers to the target filter headers store.
func TestHeaderProcessing(t *testing.T) {
	t.Parallel()
	type Prep struct {
		hImport *headersImport
		err     error
	}
	type Verify struct {
		tc                *testing.T
		processingRegions *processingRegions
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
			name: "ErrorOnGetHeaderMetadataFailure",
			prep: func() Prep {
				// Prep error to mock.
				expectErr := errors.New("failed to get " +
					"header metadata")

				// Mock GetHeaderMetadata.
				hIS := &mockHeaderImportSource{}
				hIS.On("GetHeaderMetadata").Return(
					nil, expectErr,
				)

				// Mock ChainTip on target header store.
				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("ChainTip").Return(
					&wire.BlockHeader{}, uint32(60), nil,
				)

				// Mock ChainTip on target header store.
				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("ChainTip").Return(
					&chainhash.Hash{}, uint32(60), nil,
				)

				// Configure Import options.
				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				h := &headersImport{
					blockHeadersImportSource: hIS,
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
				expectErr := errors.New("failed to get block " +
					"header store chain tip")

				// Prep header metadata.
				hM := &headerMetadata{
					importMetadata: &importMetadata{
						startHeight: 50,
					},
					endHeight: 90,
				}

				// Mock GetHeaderMetadata.
				hIS := &mockHeaderImportSource{}
				hIS.On("GetHeaderMetadata").Return(hM, nil)

				// Mock ChainTip on target header store.
				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("ChainTip").Return(
					&wire.BlockHeader{}, uint32(0),
					expectErr,
				)

				// Mock ChainTip on target header store.
				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("ChainTip").Return(
					&chainhash.Hash{}, uint32(60), nil,
				)

				// Configure Import options.
				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				h := &headersImport{
					blockHeadersImportSource: hIS,
					options:                  ops,
				}

				return Prep{
					hImport: h,
				}
			},
			verify:    func(Verify) {},
			expectErr: true,
			expectErrMsg: "failed to get block header store " +
				"chain tip",
		},
		{
			name: "ErrorOnGetChainTipForTargetFilterStore",
			prep: func() Prep {
				// Prep error to mock.
				expectErr := errors.New("failed to get " +
					"filter header store chain tip")

				// Prep header metadata.
				hM := &headerMetadata{
					importMetadata: &importMetadata{
						startHeight: 50,
					},
					endHeight: 90,
				}

				// Mock GetHeaderMetadata.
				hIS := &mockHeaderImportSource{}
				hIS.On("GetHeaderMetadata").Return(hM, nil)

				// Mock ChainTip on target header store.
				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("ChainTip").Return(
					&wire.BlockHeader{}, uint32(60), nil,
				)

				// Mock ChainTip on target header store.
				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("ChainTip").Return(
					&chainhash.Hash{}, uint32(0), expectErr,
				)

				// Configure Import options.
				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				h := &headersImport{
					blockHeadersImportSource: hIS,
					options:                  ops,
				}

				return Prep{
					hImport: h,
				}
			},
			verify:    func(Verify) {},
			expectErr: true,
			expectErrMsg: "failed to get filter header store " +
				"chain tip",
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
				hM := &headerMetadata{
					importMetadata: &importMetadata{
						startHeight: 50,
					},
					endHeight: 90,
				}

				// Mock GetHeaderMetadata.
				hIS := &mockHeaderImportSource{}
				hIS.On("GetHeaderMetadata").Return(hM, nil)

				// Mock ChainTip on target header store.
				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("ChainTip").Return(
					&wire.BlockHeader{}, uint32(60), nil,
				)

				// Mock ChainTip on target header store.
				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("ChainTip").Return(
					&chainhash.Hash{}, uint32(60), nil,
				)

				// Configure Import options.
				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				h := &headersImport{
					blockHeadersImportSource: hIS,
					options:                  ops,
				}

				return Prep{
					hImport: h,
				}
			},
			verify: func(v Verify) {
				// Assert that the divergence region doesn't
				// exist.
				dR := v.processingRegions.divergence
				require.False(v.tc, dR.exists)

				// Assert that the overlap region was properly
				// detected.
				oR := v.processingRegions.overlap
				oRE := headerRegion{
					start:  50,
					end:    60,
					exists: true,
				}
				require.Equal(v.tc, oRE, oR)

				// Assert that the new headers region was
				// properly detected.
				nHR := v.processingRegions.newHeaders
				nHRE := headerRegion{
					start:  61,
					end:    90,
					exists: true,
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
				hM := &headerMetadata{
					importMetadata: &importMetadata{
						startHeight: 50,
					},
					endHeight: 70,
				}

				// Mock GetHeaderMetadata.
				hIS := &mockHeaderImportSource{}
				hIS.On("GetHeaderMetadata").Return(hM, nil)

				// Mock ChainTip on target header store.
				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("ChainTip").Return(
					&wire.BlockHeader{}, uint32(90), nil,
				)

				// Mock ChainTip on target header store.
				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("ChainTip").Return(
					&chainhash.Hash{}, uint32(90), nil,
				)

				// Configure Import options.
				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				h := &headersImport{
					blockHeadersImportSource: hIS,
					options:                  ops,
				}

				return Prep{
					hImport: h,
				}
			},
			verify: func(v Verify) {
				// Assert that the divergence region doesn't
				// exist.
				dR := v.processingRegions.divergence
				require.False(v.tc, dR.exists)

				// Assert that the new headers region doesn't
				// exist.
				nHR := v.processingRegions.newHeaders
				require.False(v.tc, nHR.exists)

				// Assert that the overlap region is properly
				// detected.
				oR := v.processingRegions.overlap
				oRE := headerRegion{
					start:  50,
					end:    70,
					exists: true,
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
				hM := &headerMetadata{
					importMetadata: &importMetadata{
						startHeight: 51,
					},
					endHeight: 90,
				}

				// Mock GetHeaderMetadata.
				hIS := &mockHeaderImportSource{}
				hIS.On("GetHeaderMetadata").Return(hM, nil)

				// Mock ChainTip on target header store.
				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("ChainTip").Return(
					&wire.BlockHeader{}, uint32(50), nil,
				)

				// Mock ChainTip on target header store.
				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("ChainTip").Return(
					&chainhash.Hash{}, uint32(50), nil,
				)

				// Configure Import options.
				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				h := &headersImport{
					blockHeadersImportSource: hIS,
					options:                  ops,
				}

				return Prep{
					hImport: h,
				}
			},
			verify: func(v Verify) {
				// Assert that the divergence region doesn't
				// exist.
				dR := v.processingRegions.divergence
				require.False(v.tc, dR.exists)

				// Assert that the overlap region was properly
				// detected.
				oR := v.processingRegions.overlap
				require.False(v.tc, oR.exists)

				// Assert that the new headers region was
				// properly detected.
				nHR := v.processingRegions.newHeaders
				nHRE := headerRegion{
					start:  51,
					end:    90,
					exists: true,
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
				hM := &headerMetadata{
					importMetadata: &importMetadata{
						startHeight: 0,
					},
					endHeight: 70,
				}

				// Mock GetHeaderMetadata.
				hIS := &mockHeaderImportSource{}
				hIS.On("GetHeaderMetadata").Return(hM, nil)

				// Mock ChainTip on target header store.
				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("ChainTip").Return(
					&wire.BlockHeader{}, uint32(40), nil,
				)

				// Mock ChainTip on target header store.
				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("ChainTip").Return(
					&chainhash.Hash{}, uint32(90), nil,
				)

				// Configure Import options.
				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				h := &headersImport{
					blockHeadersImportSource: hIS,
					options:                  ops,
				}

				return Prep{
					hImport: h,
				}
			},
			verify: func(v Verify) {
				// Assert that the new headers region
				// doesn't exist.
				nHR := v.processingRegions.newHeaders
				require.False(v.tc, nHR.exists)

				// Assert that the overlap region was properly
				// detected.
				oR := v.processingRegions.overlap
				oRE := headerRegion{
					start:  0,
					end:    40,
					exists: true,
				}
				require.Equal(v.tc, oRE, oR)

				// Assert that the divergence region was
				// properly detected.
				dR := v.processingRegions.divergence
				dRE := headerRegion{
					start:  41,
					end:    70,
					exists: true,
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
				hM := &headerMetadata{
					importMetadata: &importMetadata{
						startHeight: 0,
					},
					endHeight: 90,
				}

				// Mock GetHeaderMetadata.
				hIS := &mockHeaderImportSource{}
				hIS.On("GetHeaderMetadata").Return(hM, nil)

				// Mock ChainTip on target header store.
				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("ChainTip").Return(
					&wire.BlockHeader{}, uint32(40), nil,
				)

				// Mock ChainTip on target header store.
				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("ChainTip").Return(
					&chainhash.Hash{}, uint32(70), nil,
				)

				// Configure Import options.
				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				h := &headersImport{
					blockHeadersImportSource: hIS,
					options:                  ops,
				}

				return Prep{
					hImport: h,
				}
			},
			verify: func(v Verify) {
				// Assert that the overlap region was properly
				// detected.
				oR := v.processingRegions.overlap
				oRE := headerRegion{
					start:  0,
					end:    40,
					exists: true,
				}
				require.Equal(v.tc, oRE, oR)

				// Assert that the divergence region was
				// properly detected
				dR := v.processingRegions.divergence
				dRE := headerRegion{
					start:  41,
					end:    70,
					exists: true,
				}
				require.Equal(v.tc, dRE, dR)

				// Assert that the new headers region was
				// properly detected.
				nHR := v.processingRegions.newHeaders
				nHRE := headerRegion{
					start:  71,
					end:    90,
					exists: true,
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
				hM := &headerMetadata{
					importMetadata: &importMetadata{
						startHeight: 0,
					},
					endHeight: 70,
				}

				// Mock GetHeaderMetadata.
				hIS := &mockHeaderImportSource{}
				hIS.On("GetHeaderMetadata").Return(hM, nil)

				// Mock ChainTip on target header store.
				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("ChainTip").Return(
					&wire.BlockHeader{}, uint32(90), nil,
				)

				// Mock ChainTip on target header store.
				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("ChainTip").Return(
					&chainhash.Hash{}, uint32(40), nil,
				)

				// Configure Import options.
				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				h := &headersImport{
					blockHeadersImportSource: hIS,
					options:                  ops,
				}

				return Prep{
					hImport: h,
				}
			},
			verify: func(v Verify) {
				// Assert that the new headers region doesn't
				// exist.
				nHR := v.processingRegions.newHeaders
				require.False(v.tc, nHR.exists)

				// Assert that the overlap region was properly
				// detected.
				oR := v.processingRegions.overlap
				oRE := headerRegion{
					start:  0,
					end:    40,
					exists: true,
				}
				require.Equal(v.tc, oRE, oR)

				// Assert that the divergence region was
				// properly detected
				dR := v.processingRegions.divergence
				dRE := headerRegion{
					start:  41,
					end:    70,
					exists: true,
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
				hM := &headerMetadata{
					importMetadata: &importMetadata{
						startHeight: 40,
					},
					endHeight: 90,
				}

				// Mock GetHeaderMetadata.
				hIS := &mockHeaderImportSource{}
				hIS.On("GetHeaderMetadata").Return(hM, nil)

				// Mock ChainTip on target header store.
				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("ChainTip").Return(
					&wire.BlockHeader{}, uint32(90), nil,
				)

				// Mock ChainTip on target header store.
				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("ChainTip").Return(
					&chainhash.Hash{}, uint32(70), nil,
				)

				// Configure Import options.
				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				h := &headersImport{
					blockHeadersImportSource: hIS,
					options:                  ops,
				}

				return Prep{
					hImport: h,
				}
			},
			verify: func(v Verify) {
				// Assert that the new headers region doesn't
				// exist.
				nHR := v.processingRegions.newHeaders
				require.False(v.tc, nHR.exists)

				// Assert that the overlap region was properly
				// detected.
				oR := v.processingRegions.overlap
				oRE := headerRegion{
					start:  40,
					end:    70,
					exists: true,
				}
				require.Equal(v.tc, oRE, oR)

				// Assert that the divergence region was
				// properly detected
				dR := v.processingRegions.divergence
				dRE := headerRegion{
					start:  71,
					end:    90,
					exists: true,
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
				hM := &headerMetadata{
					importMetadata: &importMetadata{
						startHeight: 30,
					},
					endHeight: 90,
				}

				// Mock GetHeaderMetadata.
				hIS := &mockHeaderImportSource{}
				hIS.On("GetHeaderMetadata").Return(hM, nil)

				// Mock ChainTip on target header store.
				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("ChainTip").Return(
					&wire.BlockHeader{}, uint32(40), nil,
				)

				// Mock ChainTip on target header store.
				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("ChainTip").Return(
					&chainhash.Hash{}, uint32(50), nil,
				)

				// Configure Import options.
				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				h := &headersImport{
					blockHeadersImportSource: hIS,
					options:                  ops,
				}

				return Prep{
					hImport: h,
				}
			},
			verify: func(v Verify) {
				// Assert that the overlap region was properly
				// detected.
				oR := v.processingRegions.overlap
				oRE := headerRegion{
					start:  30,
					end:    40,
					exists: true,
				}
				require.Equal(v.tc, oRE, oR)

				// Assert that the divergence region was
				// properly detected.
				dR := v.processingRegions.divergence
				dRE := headerRegion{
					start:  41,
					end:    50,
					exists: true,
				}
				require.Equal(v.tc, dRE, dR)

				// Assert that the new headers region was
				// properly detected.
				nHR := v.processingRegions.newHeaders
				nHRE := headerRegion{
					start:  51,
					end:    90,
					exists: true,
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
				require.ErrorContains(t, err, tc.expectErrMsg)
				tc.verify(verify)
				return
			}
			require.NoError(t, err)
			tc.verify(verify)
		})
	}
}

// setupFileWithHdrs creates a temporary file with headers and returns the file,
// a cleanup function, and an error if any.
func setupFileWithHdrs(hT headerfs.HeaderType,
	includeMetadata bool) (headerfs.File, func(), error) {

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

	if includeMetadata {
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

// constructBlkHdr constructs a block header from a hex string and height.
func constructBlkHdr(blockHeaderHex string,
	height uint32) (*blockHeader, error) {

	buff, err := hex.DecodeString(blockHeaderHex)
	if err != nil {
		return nil, fmt.Errorf("failed to decode block header hex: %v",
			err)
	}
	reader := bytes.NewReader(buff)
	bHExpected := newBlockHeader()
	bHExpected.Deserialize(reader, height)

	bH, err := assertBlockHeader(bHExpected)
	if err != nil {
		return nil, err
	}

	return bH, nil
}

// constructFilterHdr constructs a filter header from a hex string and height.
func constructFilterHdr(filterHeaderHex string,
	height uint32) (*filterHeader, error) {

	buff, err := hex.DecodeString(filterHeaderHex)
	if err != nil {
		return nil, fmt.Errorf("failed to decode filter header hex: %v",
			err)
	}
	reader := bytes.NewReader(buff)
	fHExpected := newFilterHeader()
	fHExpected.Deserialize(reader, height)

	fH, err := assertFilterHeader(fHExpected)
	if err != nil {
		return nil, err
	}

	return fH, nil
}
