package chainimport

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	_ "github.com/btcsuite/btcwallet/walletdb/bdb"
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
