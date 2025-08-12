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

	"github.com/btcsuite/btcd/chaincfg"
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
// the import sources. Both block and filter headers are required to be present
// for the import to start.
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
			mockBlockStore := &headerfs.MockBlockHeaderStore{}
			mockBlockStore.On("ChainTip").Return(
				&wire.BlockHeader{}, tc.blockHeight,
				tc.expectBlockStoreErr,
			)

			mockFilterStore := &headerfs.MockFilterHeaderStore{}
			mockFilterStore.On("ChainTip").Return(
				&chainhash.Hash{}, tc.filterHeight,
				tc.expectFilterStoreErr,
			)

			importer := &headersImport{}

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
	type prep struct {
		hImport *headersImport
		cleanup func()
		err     error
	}
	type verify struct {
		tc        *testing.T
		hMetadata *headerMetadata
	}
	testCases := []struct {
		name         string
		prep         func() prep
		verify       func(verify)
		expectErr    bool
		expectErrMsg string
	}{
		{
			name: "ErrorOnReaderNotInitialized",
			prep: func() prep {
				bF, cleanup, err := setupFileWithHdrs(
					headerfs.Block, true,
				)
				if err != nil {
					return prep{
						cleanup: cleanup,
						err:     err,
					}
				}

				opts := &ImportOptions{}
				bS := opts.createBlockHeaderImportSrc()
				bS.SetURI(bF.Name())

				headersImport := &headersImport{
					options:                  opts,
					blockHeadersImportSource: bS,
				}

				return prep{
					hImport: headersImport,
					cleanup: cleanup,
				}
			},
			verify:       func(verify) {},
			expectErr:    true,
			expectErrMsg: "file reader not initialized",
		},
		{
			name: "ErrorOnHeaderRead",
			prep: func() prep {
				bFile, err := os.CreateTemp(
					t.TempDir(), "invalid-block-header-*",
				)
				c1 := func() {
					bFile.Close()
					os.Remove(bFile.Name())
				}
				if err != nil {
					return prep{
						cleanup: c1,
						err:     err,
					}
				}

				err = AddHeadersImportMetadata(
					bFile.Name(), wire.SimNet, 0,
					headerfs.Block, 0,
				)
				if err != nil {
					return prep{
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
					return prep{
						cleanup: c1,
						err:     err,
					}
				}

				opts := &ImportOptions{}
				bS := opts.createBlockHeaderImportSrc()
				bS.SetURI(bFile.Name())

				// Remove the last byte of header
				// metadata to simulate EOF error.
				fileInfo, err := bFile.Stat()
				if err != nil {
					return prep{
						cleanup: c1,
						err:     err,
					}
				}
				fileSize := fileInfo.Size()
				if fileSize == 0 {
					err := fmt.Errorf("empty file: %s",
						bFile.Name())
					return prep{
						cleanup: c1,
						err:     err,
					}
				}
				err = bFile.Truncate(fileSize - 1)
				if err != nil {
					return prep{
						cleanup: c1,
						err:     err,
					}
				}
				err = bFile.Sync()
				if err != nil {
					return prep{
						cleanup: c1,
						err:     err,
					}
				}

				bFS, ok := bS.(*fileHeaderImportSource)
				require.True(t, ok)

				reader, err := mmap.Open(bFile.Name())
				cleanup := func() {
					reader.Close()
					os.Remove(bFile.Name())
				}
				if err != nil {
					return prep{
						cleanup: cleanup,
						err:     err,
					}
				}
				bFS.file = newMmapFile(reader)
				bFS.fileSize = reader.Len()

				bFS.metadata = nil

				headersImport := &headersImport{
					options:                  opts,
					blockHeadersImportSource: bFS,
				}

				return prep{
					hImport: headersImport,
					cleanup: cleanup,
				}
			},
			verify:       func(verify) {},
			expectErr:    true,
			expectErrMsg: "failed to read start height",
		},
		{
			name: "ErrorOnUnknownHeaderType",
			prep: func() prep {
				bFile, err := os.CreateTemp(
					t.TempDir(),
					"invalid-block-header-*",
				)
				c1 := func() {
					bFile.Close()
					os.Remove(bFile.Name())
				}
				if err != nil {
					return prep{
						cleanup: c1,
						err:     err,
					}
				}

				err = AddHeadersImportMetadata(
					bFile.Name(), wire.SimNet, 0,
					headerfs.UnknownHeader, 0,
				)
				if err != nil {
					return prep{
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
					return prep{
						cleanup: c1,
						err:     err,
					}
				}

				opts := &ImportOptions{}
				bs := opts.createBlockHeaderImportSrc()
				bs.SetURI(bFile.Name())

				bFS, ok := bs.(*fileHeaderImportSource)
				require.True(t, ok)

				reader, err := mmap.Open(bFile.Name())
				cleanup := func() {
					reader.Close()
					os.Remove(bFile.Name())
				}
				if err != nil {
					return prep{
						cleanup: cleanup,
						err:     err,
					}
				}
				bFS.file = newMmapFile(reader)
				bFS.fileSize = reader.Len()

				bFS.metadata = nil

				headersImport := &headersImport{
					options:                  opts,
					blockHeadersImportSource: bFS,
				}

				return prep{
					hImport: headersImport,
					cleanup: cleanup,
				}
			},
			verify:    func(verify) {},
			expectErr: true,
			expectErrMsg: "failed to get header size: unknown " +
				"header type: 255",
		},
		{
			name: "ErrorOnNoHeadersData",
			prep: func() prep {
				bFile, err := os.CreateTemp(
					t.TempDir(), "invalid-block-header-*",
				)
				c1 := func() {
					bFile.Close()
					os.Remove(bFile.Name())
				}
				if err != nil {
					return prep{
						cleanup: c1,
						err:     err,
					}
				}

				err = AddHeadersImportMetadata(
					bFile.Name(), wire.SimNet, 0,
					headerfs.Block, 0,
				)
				if err != nil {
					return prep{
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
					return prep{
						cleanup: c1,
						err:     err,
					}
				}

				opts := &ImportOptions{}
				bs := opts.createBlockHeaderImportSrc()
				bs.SetURI(bFile.Name())

				bFS, ok := bs.(*fileHeaderImportSource)
				require.True(t, ok)

				reader, err := mmap.Open(bFile.Name())
				cleanup := func() {
					reader.Close()
					os.Remove(bFile.Name())
				}
				if err != nil {
					return prep{
						cleanup: cleanup,
						err:     err,
					}
				}
				bFS.file = newMmapFile(reader)
				bFS.fileSize = reader.Len()

				bFS.metadata = nil

				headersImport := &headersImport{
					options:                  opts,
					blockHeadersImportSource: bFS,
				}

				return prep{
					hImport: headersImport,
					cleanup: cleanup,
				}
			},
			verify:       func(verify) {},
			expectErr:    true,
			expectErrMsg: "no headers available in import source",
		},
		{
			name: "ErrorOnPartialHeadersData",
			prep: func() prep {
				bFile, c1, err := setupFileWithHdrs(
					headerfs.Block, true,
				)
				if err != nil {
					return prep{
						cleanup: c1,
						err:     err,
					}
				}

				// Remove the last byte of the file to trigger
				// data corruption.
				fileInfo, err := bFile.Stat()
				if err != nil {
					return prep{
						cleanup: c1,
						err:     err,
					}
				}
				fileSize := fileInfo.Size()
				if fileSize == 0 {
					err := fmt.Errorf("empty file: %s",
						bFile.Name())
					return prep{
						cleanup: c1,
						err:     err,
					}
				}
				err = bFile.Truncate(fileSize - 1)
				if err != nil {
					return prep{
						cleanup: c1,
						err:     err,
					}
				}
				err = bFile.Sync()
				if err != nil {
					return prep{
						cleanup: c1,
						err:     err,
					}
				}

				opts := &ImportOptions{}
				bs := opts.createBlockHeaderImportSrc()
				bs.SetURI(bFile.Name())

				bFS, ok := bs.(*fileHeaderImportSource)
				require.True(t, ok)

				reader, err := mmap.Open(bFile.Name())
				cleanup := func() {
					reader.Close()
					os.Remove(bFile.Name())
				}
				if err != nil {
					return prep{
						cleanup: cleanup,
						err:     err,
					}
				}
				bFS.file = newMmapFile(reader)
				bFS.fileSize = reader.Len()

				bFS.metadata = nil

				headersImport := &headersImport{
					options:                  opts,
					blockHeadersImportSource: bFS,
				}

				return prep{
					hImport: headersImport,
					cleanup: cleanup,
				}
			},
			verify:       func(verify) {},
			expectErr:    true,
			expectErrMsg: "possible data corruption",
		},
		{
			name: "ReturnsCachedMetadataWhenAvailable",
			prep: func() prep {
				bFile, c1, err := setupFileWithHdrs(
					headerfs.Block, true,
				)
				if err != nil {
					return prep{
						cleanup: c1,
						err:     err,
					}
				}

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
					return prep{
						cleanup: c2,
						err:     err,
					}
				}

				// Remove the source file to ensure next call
				// must use cached data.
				err = bS.Close()
				if err != nil {
					return prep{
						cleanup: c2,
						err:     err,
					}
				}

				headersImport := &headersImport{
					options:                  opts,
					blockHeadersImportSource: bS,
				}

				return prep{
					hImport: headersImport,
					cleanup: c1,
				}
			},
			verify: func(v verify) {
				// Next call should result in a cache hit since
				// the file is gone.
				bHdrType := headerfs.Block
				expectBlockMetadata := &headerMetadata{
					importMetadata: &importMetadata{
						networkMagic: wire.SimNet,
						version:      0,
						headerType:   bHdrType,
						startHeight:  0,
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
			verify := verify{
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
	type prep struct {
		file    headerfs.File
		data    []byte
		cleanup func()
		err     error
	}
	type verify struct {
		tc   *testing.T
		file headerfs.File
		data []byte
	}
	testCases := []struct {
		name         string
		networkMagic wire.BitcoinNet
		version      uint8
		headerType   headerfs.HeaderType
		startHeight  uint32
		prep         func() prep
		verify       func(verify)
		expectErr    bool
		expectErrMsg string
	}{
		{
			name: "ErrorOnSourceFileNotExist",
			prep: func() prep {
				return prep{
					cleanup: func() {},
				}
			},
			verify:       func(verify) {},
			expectErr:    true,
			expectErrMsg: "failed to open source file",
		},
		{
			name:         "PreservesOriginalFileContents",
			networkMagic: wire.SimNet,
			version:      0,
			headerType:   headerfs.Block,
			startHeight:  0,
			prep: func() prep {
				bFile, c1, err := setupFileWithHdrs(
					headerfs.Block, false,
				)
				if err != nil {
					return prep{
						cleanup: c1,
						err:     err,
					}
				}

				// Read the data before adding metadata for
				// later assertion.
				dataBefore, err := io.ReadAll(bFile)
				if err != nil {
					return prep{
						cleanup: c1,
						err:     err,
					}
				}

				return prep{
					file:    bFile,
					data:    dataBefore,
					cleanup: c1,
				}
			},
			verify: func(v verify) {
				// Reopen the file to get an updated file
				// descriptor after adding header metadata
				// atomically.
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

				data, err := io.ReadAll(srcFile)
				require.NoError(v.tc, err)

				// Ensure original file content's not modified.
				after := data[ImportMetadataSize:]
				areEqual := bytes.Equal(after, v.data)
				require.True(v.tc, areEqual)
			},
		},
		{
			name:         "AddsBlockHeaderMetadataToFile",
			networkMagic: wire.TestNet3,
			version:      0,
			headerType:   headerfs.Block,
			startHeight:  1,
			prep: func() prep {
				bFile, c1, err := setupFileWithHdrs(
					headerfs.Block, false,
				)
				if err != nil {
					return prep{
						cleanup: c1,
						err:     err,
					}
				}

				// Read the data before adding metadata for
				// later assertion.
				dataBefore, err := io.ReadAll(bFile)
				if err != nil {
					return prep{
						cleanup: c1,
						err:     err,
					}
				}

				return prep{
					file:    bFile,
					data:    dataBefore,
					cleanup: c1,
				}
			},
			verify: func(v verify) {
				// Reopen the file to get an updated file
				// descriptor after adding header metadata
				// atomically.
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

				data, err := io.ReadAll(srcFile)
				require.NoError(v.tc, err)

				networkMagicOffset := 0
				versionOffset := networkMagicSize
				hTOffset := versionOffset + versionSize
				startHeightOffset := hTOffset + headerTypeSize

				net := data[networkMagicOffset:versionOffset]
				networkMagic := wire.BitcoinNet(
					binary.LittleEndian.Uint32(net),
				)
				require.Equal(v.tc, wire.TestNet3, networkMagic)

				version := data[versionOffset]
				require.Equal(v.tc, uint8(0), version)

				headerType := headerfs.HeaderType(
					data[hTOffset],
				)
				require.Equal(v.tc, headerfs.Block, headerType)

				hMS := ImportMetadataSize
				sHeightD := data[startHeightOffset:hMS]
				sHeight := binary.LittleEndian.Uint32(sHeightD)
				require.Equal(v.tc, uint32(1), sHeight)

				// Ensure original file content's not modified.
				after := data[hMS:]
				areEqual := bytes.Equal(after, v.data)
				require.True(v.tc, areEqual)
			},
		},
		{
			name:         "AddsFilterHeaderMetadataToFile",
			networkMagic: wire.TestNet4,
			version:      0,
			headerType:   headerfs.RegularFilter,
			startHeight:  3,
			prep: func() prep {
				bFile, c1, err := setupFileWithHdrs(
					headerfs.RegularFilter, false,
				)
				if err != nil {
					return prep{
						cleanup: c1,
						err:     err,
					}
				}

				// Read the data before adding metadata
				// for later assertion.
				dataBefore, err := io.ReadAll(bFile)
				if err != nil {
					return prep{
						cleanup: c1,
						err:     err,
					}
				}

				return prep{
					file:    bFile,
					data:    dataBefore,
					cleanup: c1,
				}
			},
			verify: func(v verify) {
				// Reopen the file to get an updated file
				// descriptor after adding header metadata
				// atomically.
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

				data, err := io.ReadAll(srcFile)
				require.NoError(v.tc, err)

				networkMagicOffset := 0
				versionOffset := networkMagicSize
				hTOffset := versionOffset + versionSize
				startHeightOffset := hTOffset + headerTypeSize

				net := data[networkMagicOffset:versionOffset]
				networkMagic := wire.BitcoinNet(
					binary.LittleEndian.Uint32(net),
				)
				require.Equal(v.tc, wire.TestNet4, networkMagic)

				version := data[versionOffset]
				require.Equal(v.tc, uint8(0), version)

				headerType := headerfs.HeaderType(
					data[hTOffset],
				)
				require.Equal(
					v.tc, headerfs.RegularFilter,
					headerType,
				)

				hMS := ImportMetadataSize
				sHeightD := data[startHeightOffset:hMS]
				sHeight := binary.LittleEndian.Uint32(sHeightD)
				require.Equal(v.tc, uint32(3), sHeight)

				// Ensure original file content's not modified.
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
				srcFilePath, tc.networkMagic, tc.version,
				tc.headerType, tc.startHeight,
			)
			verify := verify{
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
	type prep struct {
		hImport *headersImport
		cleanup func()
		err     error
	}
	type verify struct {
		tc      *testing.T
		hImport *headersImport
	}
	testCases := []struct {
		name         string
		prep         func() prep
		verify       func(verify)
		expectErr    bool
		expectErrMsg string
	}{
		{
			name: "MissingBlockANDFilterHeaderImportSource",
			prep: func() prep {
				opts := &ImportOptions{}
				headersImport := &headersImport{
					options:                   opts,
					blockHeadersImportSource:  nil,
					filterHeadersImportSource: nil,
				}
				return prep{
					hImport: headersImport,
					cleanup: func() {},
					err:     nil,
				}
			},
			verify:       func(verify) {},
			expectErr:    true,
			expectErrMsg: "missing required header sources",
		},
		{
			name: "MissingBlockHeaderImportSource",
			prep: func() prep {
				opts := &ImportOptions{}
				bS := opts.createBlockHeaderImportSrc()
				headersImport := &headersImport{
					options:                   opts,
					blockHeadersImportSource:  bS,
					filterHeadersImportSource: nil,
				}
				return prep{
					hImport: headersImport,
					cleanup: func() {},
					err:     nil,
				}
			},
			verify:       func(verify) {},
			expectErr:    true,
			expectErrMsg: "missing required header sources",
		},
		{
			name: "MissingFilterHeaderImportSource",
			prep: func() prep {
				opts := &ImportOptions{}
				fS := opts.createFilterHeaderImportSrc()
				headersImport := &headersImport{
					options:                   opts,
					blockHeadersImportSource:  nil,
					filterHeadersImportSource: fS,
				}
				return prep{
					hImport: headersImport,
					cleanup: func() {},
					err:     nil,
				}
			},
			verify:       func(verify) {},
			expectErr:    true,
			expectErrMsg: "missing required header sources",
		},
		{
			name: "ErrorOnBlockFileNotExist",
			prep: func() prep {
				opts := &ImportOptions{}
				bS := opts.createBlockHeaderImportSrc()
				fS := opts.createFilterHeaderImportSrc()
				filePath := "/path/to/nonexistent/file"
				bS.SetURI(filePath)
				headersImport := &headersImport{
					options:                   opts,
					blockHeadersImportSource:  bS,
					filterHeadersImportSource: fS,
				}
				return prep{
					hImport: headersImport,
					cleanup: func() {},
					err:     nil,
				}
			},
			verify:    func(verify) {},
			expectErr: true,
			expectErrMsg: "failed to mmap file: open " +
				"/path/to/nonexistent/file",
		},
		{
			name: "ErrorOnFilterFileNotExist",
			prep: func() prep {
				bFile, c1, err := setupFileWithHdrs(
					headerfs.Block, true,
				)
				if err != nil {
					return prep{
						cleanup: c1,
						err:     err,
					}
				}

				opts := &ImportOptions{}
				bS := opts.createBlockHeaderImportSrc()
				fS := opts.createFilterHeaderImportSrc()

				bS.SetURI(bFile.Name())

				filePath := "/path/to/nonexistent/file"
				bS.SetURI(filePath)

				headersImport := &headersImport{
					options:                   opts,
					blockHeadersImportSource:  bS,
					filterHeadersImportSource: fS,
				}
				return prep{
					hImport: headersImport,
					cleanup: c1,
					err:     nil,
				}
			},
			verify:    func(verify) {},
			expectErr: true,
			expectErrMsg: "failed to mmap file: open " +
				"/path/to/nonexistent/file",
		},
		{
			name: "ErrorOnGetBlockHeaderMetadata",
			prep: func() prep {
				blockFile, err := os.CreateTemp(
					t.TempDir(),
					"empty-block-header-*",
				)
				cleanup := func() {
					blockFile.Close()
					os.Remove(blockFile.Name())
				}
				if err != nil {
					return prep{
						cleanup: cleanup,
						err:     err,
					}
				}

				opts := &ImportOptions{}
				bS := opts.createBlockHeaderImportSrc()
				fS := opts.createFilterHeaderImportSrc()

				bS.SetURI(blockFile.Name())

				headersImport := &headersImport{
					options:                   opts,
					blockHeadersImportSource:  bS,
					filterHeadersImportSource: fS,
				}
				return prep{
					hImport: headersImport,
					cleanup: cleanup,
				}
			},
			verify:       func(verify) {},
			expectErr:    true,
			expectErrMsg: "failed to read network magic: EOF",
		},
		{
			name: "ErrorOnGetFilterHeaderMetadata",
			prep: func() prep {
				bFile, c1, err := setupFileWithHdrs(
					headerfs.Block, true,
				)
				if err != nil {
					return prep{
						cleanup: c1,
						err:     err,
					}
				}

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
					return prep{
						cleanup: cleanup,
						err:     err,
					}
				}

				opts := &ImportOptions{}
				bS := opts.createBlockHeaderImportSrc()
				fS := opts.createFilterHeaderImportSrc()

				bS.SetURI(bFile.Name())
				fS.SetURI(fFile.Name())

				headersImport := &headersImport{
					options:                   opts,
					blockHeadersImportSource:  bS,
					filterHeadersImportSource: fS,
				}
				cleanup = func() {
					headersImport.closeSources()
				}
				return prep{
					hImport: headersImport,
					cleanup: cleanup,
				}
			},
			verify:       func(verify) {},
			expectErr:    true,
			expectErrMsg: "failed to read network magic: EOF",
		},
		{
			name: "OpenSourcesCorrectly",
			prep: func() prep {
				bFile, c1, err := setupFileWithHdrs(
					headerfs.Block, true,
				)
				if err != nil {
					return prep{
						cleanup: c1,
						err:     err,
					}
				}

				fFile, c2, err := setupFileWithHdrs(
					headerfs.RegularFilter, true,
				)
				cleanup := func() {
					c2()
					c1()
				}
				if err != nil {
					return prep{
						cleanup: cleanup,
						err:     err,
					}
				}

				opts := &ImportOptions{}
				bS := opts.createBlockHeaderImportSrc()
				fS := opts.createFilterHeaderImportSrc()

				bS.SetURI(bFile.Name())
				fS.SetURI(fFile.Name())

				headersImport := &headersImport{
					options:                   opts,
					blockHeadersImportSource:  bS,
					filterHeadersImportSource: fS,
				}
				cleanup = func() {
					headersImport.closeSources()
				}
				return prep{
					hImport: headersImport,
					cleanup: cleanup,
				}
			},
			verify: func(v verify) {
				bHdrType := headerfs.Block
				expectBlockMetadata := &headerMetadata{
					importMetadata: &importMetadata{
						networkMagic: wire.SimNet,
						version:      0,
						headerType:   bHdrType,
						startHeight:  0,
					},
					endHeight:    4,
					headerSize:   80,
					headersCount: 5,
				}

				fHdrType := headerfs.RegularFilter
				expectFilterMetadata := &headerMetadata{
					importMetadata: &importMetadata{
						networkMagic: wire.SimNet,
						version:      0,
						headerType:   fHdrType,
						startHeight:  0,
					},
					endHeight:    4,
					headerSize:   32,
					headersCount: 5,
				}

				bS := v.hImport.blockHeadersImportSource
				metadata, err := bS.GetHeaderMetadata()
				require.NoError(v.tc, err)
				require.Equal(
					v.tc, expectBlockMetadata, metadata,
				)

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
			verify := verify{
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
				bIS := &mockHeaderImportSource{}
				bIS.On("GetHeaderMetadata").Return(
					&headerMetadata{}, nil,
				)

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
				bIS := &mockHeaderImportSource{}
				rFT := headerfs.RegularFilter
				bM := &headerMetadata{
					importMetadata: &importMetadata{
						headerType: rFT,
					},
				}
				bIS.On("GetHeaderMetadata").Return(bM, nil)

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
				bIS := &mockHeaderImportSource{}
				bM := &headerMetadata{
					importMetadata: &importMetadata{
						headerType: headerfs.Block,
					},
				}
				bIS.On("GetHeaderMetadata").Return(bM, nil)

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
				bIS := &mockHeaderImportSource{}
				bT := headerfs.Block
				bM := &headerMetadata{
					importMetadata: &importMetadata{
						networkMagic: wire.MainNet,
						version:      0,
						headerType:   bT,
					},
				}
				bIS.On("GetHeaderMetadata").Return(bM, nil)
				bIS.On("GetURI").Return("/path/to/blocks")

				fIS := &mockHeaderImportSource{}
				rFT := headerfs.RegularFilter
				fM := &headerMetadata{
					importMetadata: &importMetadata{
						networkMagic: wire.SimNet,
						version:      0,
						headerType:   rFT,
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
				bIS := &mockHeaderImportSource{}
				bT := headerfs.Block
				bM := &headerMetadata{
					importMetadata: &importMetadata{
						networkMagic: wire.SimNet,
						version:      0,
						headerType:   bT,
					},
				}
				bIS.On("GetHeaderMetadata").Return(bM, nil)

				fIS := &mockHeaderImportSource{}
				rFT := headerfs.RegularFilter
				fM := &headerMetadata{
					importMetadata: &importMetadata{
						networkMagic: wire.SimNet,
						version:      0,
						headerType:   rFT,
					},
				}
				fIS.On("GetHeaderMetadata").Return(fM, nil)

				tCP := chaincfg.Params{
					Net: wire.MainNet,
				}

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
				bIS := &mockHeaderImportSource{}
				bT := headerfs.Block
				bM := &headerMetadata{
					importMetadata: &importMetadata{
						networkMagic: wire.SimNet,
						version:      0,
						headerType:   bT,
						startHeight:  1,
					},
				}
				bIS.On("GetHeaderMetadata").Return(bM, nil)
				bIS.On("GetURI").Return("/path/to/blocks")

				fIS := &mockHeaderImportSource{}
				rFT := headerfs.RegularFilter
				fM := &headerMetadata{
					importMetadata: &importMetadata{
						networkMagic: wire.SimNet,
						version:      0,
						headerType:   rFT,
						startHeight:  3,
					},
				}
				fIS.On("GetHeaderMetadata").Return(fM, nil)
				fIS.On("GetURI").Return("/path/to/filters")

				tCP := chaincfg.Params{
					Net: wire.SimNet,
				}

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
				bIS := &mockHeaderImportSource{}
				bT := headerfs.Block
				bM := &headerMetadata{
					importMetadata: &importMetadata{
						networkMagic: wire.SimNet,
						version:      0,
						headerType:   bT,
						startHeight:  1,
					},
					headersCount: 3,
				}
				bIS.On("GetHeaderMetadata").Return(bM, nil)
				bIS.On("GetURI").Return("/path/to/blocks")

				fIS := &mockHeaderImportSource{}
				rFT := headerfs.RegularFilter
				fM := &headerMetadata{
					importMetadata: &importMetadata{
						networkMagic: wire.SimNet,
						version:      0,
						headerType:   rFT,
						startHeight:  1,
					},
					headersCount: 5,
				}
				fIS.On("GetHeaderMetadata").Return(fM, nil)
				fIS.On("GetURI").Return("/path/to/filters")

				tCP := chaincfg.Params{
					Net: wire.SimNet,
				}

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
				bIS := &mockHeaderImportSource{}
				bT := headerfs.Block
				bM := &headerMetadata{
					importMetadata: &importMetadata{
						networkMagic: wire.SimNet,

						headerType:  bT,
						startHeight: 1,
					},
					headersCount: 3,
				}
				bIS.On("GetHeaderMetadata").Return(bM, nil)

				fIS := &mockHeaderImportSource{}
				rFT := headerfs.RegularFilter
				fM := &headerMetadata{
					importMetadata: &importMetadata{
						networkMagic: wire.SimNet,
						version:      0,
						headerType:   rFT,
						startHeight:  1,
					},
					headersCount: 3,
				}
				fIS.On("GetHeaderMetadata").Return(fM, nil)

				tCP := chaincfg.Params{
					Net: wire.SimNet,
				}

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
	type prep struct {
		hImport *headersImport
		err     error
	}
	testCases := []struct {
		name         string
		prep         func() prep
		expectErr    bool
		expectErrMsg string
	}{
		{
			name: "ErrorOnGetHeaderMetadata",
			prep: func() prep {
				bIS := &mockHeaderImportSource{}
				bIS.On("GetHeaderMetadata").Return(
					nil, errors.New("I/O read error"),
				)
				hImport := &headersImport{
					blockHeadersImportSource: bIS,
				}
				return prep{
					hImport: hImport,
				}
			},
			expectErr: true,
			expectErrMsg: "failed to get block header metadata: " +
				"I/O read error",
		},
		{
			name: "ErrorOnGetChainTipForTargetBlockStore",
			prep: func() prep {
				bIS := &mockHeaderImportSource{}
				bIS.On("GetHeaderMetadata").Return(
					&headerMetadata{}, nil,
				)

				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("ChainTip").Return(
					&wire.BlockHeader{}, uint32(0),
					errors.New("I/O read error"),
				)

				ops := &ImportOptions{
					TargetBlockHeaderStore: bHS,
				}

				h := &headersImport{
					blockHeadersImportSource: bIS,
					options:                  ops,
				}

				return prep{
					hImport: h,
				}
			},
			expectErr: true,
			expectErrMsg: "failed to get target block " +
				"header chain tip: I/O read error",
		},
		{
			name: "ErrorOnGetChainTipForTargetFilterStore",
			prep: func() prep {
				bIS := &mockHeaderImportSource{}
				bIS.On("GetHeaderMetadata").Return(
					&headerMetadata{}, nil,
				)

				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("ChainTip").Return(
					&wire.BlockHeader{}, uint32(0), nil,
				)

				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("ChainTip").Return(
					&chainhash.Hash{}, uint32(0),
					errors.New("I/O read error"),
				)

				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				h := &headersImport{
					blockHeadersImportSource: bIS,
					options:                  ops,
				}

				return prep{
					hImport: h,
				}
			},
			expectErr: true,
			expectErrMsg: "failed to get target filter header " +
				"chain tip: I/O read error",
		},
		{
			name: "ErrorOnDivergenceInTargetHeaderStores",
			prep: func() prep {
				bIS := &mockHeaderImportSource{}
				bIS.On("GetHeaderMetadata").Return(
					&headerMetadata{}, nil,
				)

				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("ChainTip").Return(
					&wire.BlockHeader{}, uint32(0), nil,
				)

				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("ChainTip").Return(
					&chainhash.Hash{}, uint32(2), nil,
				)

				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				h := &headersImport{
					blockHeadersImportSource: bIS,
					options:                  ops,
				}

				return prep{
					hImport: h,
				}
			},
			expectErr: true,
			expectErrMsg: "divergence detected between target " +
				"header store tip heights (block=0, filter=2)",
		},
		{
			name: "ErrorOnImportStartHeightCreatesGap",
			prep: func() prep {
				bIS := &mockHeaderImportSource{}
				metadata := &headerMetadata{
					importMetadata: &importMetadata{
						startHeight: 2,
					},
				}
				bIS.On("GetHeaderMetadata").Return(
					metadata, nil,
				)

				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("ChainTip").Return(
					&wire.BlockHeader{}, uint32(0), nil,
				)

				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("ChainTip").Return(
					&chainhash.Hash{}, uint32(0), nil,
				)

				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				h := &headersImport{
					blockHeadersImportSource: bIS,
					options:                  ops,
				}

				return prep{
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
			prep: func() prep {
				bH, err := constructBlkHdr(
					blockHdrs[0], uint32(0),
				)
				if err != nil {
					return prep{err: err}
				}

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

				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("ChainTip").Return(
					&wire.BlockHeader{}, uint32(0), nil,
				)
				bHS.On("FetchHeaderByHeight", uint32(0)).Return(
					bH.BlockHeader.BlockHeader, nil,
				)

				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("ChainTip").Return(
					&chainhash.Hash{}, uint32(0), nil,
				)

				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				h := &headersImport{
					blockHeadersImportSource: bIS,
					options:                  ops,
				}

				return prep{
					hImport: h,
				}
			},
			expectErr: true,
			expectErrMsg: "genesis header mismatch: block header " +
				"mismatch at height 0",
		},
		{
			name: "ErrorOnGenesisFilterHeadersMismatch",
			prep: func() prep {
				bH, err := constructBlkHdr(
					blockHdrs[0], uint32(0),
				)
				if err != nil {
					return prep{err: err}
				}

				fH, err := constructFilterHdr(
					filterHdrs[0], uint32(0),
				)
				if err != nil {
					return prep{err: err}
				}

				bIS := &mockHeaderImportSource{}
				importMetadata := &importMetadata{}
				bIS.On("GetHeaderMetadata").Return(
					&headerMetadata{
						importMetadata: importMetadata,
					}, nil,
				)
				bIS.On("GetHeader", uint32(0)).Return(bH, nil)

				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("ChainTip").Return(
					&wire.BlockHeader{}, uint32(0), nil,
				)
				bHS.On("FetchHeaderByHeight", uint32(0)).Return(
					bH.BlockHeader.BlockHeader, nil,
				)

				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("ChainTip").Return(
					&chainhash.Hash{}, uint32(0), nil,
				)
				fHS.On("FetchHeaderByHeight", uint32(0)).Return(
					&fH.FilterHash, nil,
				)

				fIS := &mockHeaderImportSource{}
				fIS.On("GetHeaderMetadata").Return(
					&headerMetadata{}, nil,
				)
				fIS.On("GetHeader", uint32(0)).Return(
					newFilterHeader(), nil,
				)

				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				h := &headersImport{
					blockHeadersImportSource:  bIS,
					filterHeadersImportSource: fIS,
					options:                   ops,
				}

				return prep{
					hImport: h,
				}
			},
			expectErr: true,
			expectErrMsg: "genesis header mismatch: filter " +
				"header mismatch at height 0",
		},
		{
			name: "ValidateOnGenesisHeightSuccessfully",
			prep: func() prep {
				bH, err := constructBlkHdr(
					blockHdrs[0], uint32(0),
				)
				if err != nil {
					return prep{err: err}
				}

				fH, err := constructFilterHdr(
					filterHdrs[0], uint32(0),
				)
				if err != nil {
					return prep{err: err}
				}

				bIS := &mockHeaderImportSource{}
				importMetadata := &importMetadata{}
				bIS.On("GetHeaderMetadata").Return(
					&headerMetadata{
						importMetadata: importMetadata,
					}, nil,
				)
				bIS.On("GetHeader", uint32(0)).Return(bH, nil)

				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("ChainTip").Return(
					&wire.BlockHeader{}, uint32(0), nil,
				)
				bHS.On("FetchHeaderByHeight", uint32(0)).Return(
					bH.BlockHeader.BlockHeader, nil,
				)
				bIS.On("GetURI").Return("/path/to/blocks")

				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("ChainTip").Return(
					&chainhash.Hash{}, uint32(0), nil,
				)
				fHS.On("FetchHeaderByHeight", uint32(0)).Return(
					&fH.FilterHash, nil,
				)

				fIS := &mockHeaderImportSource{}
				fIS.On("GetHeaderMetadata").Return(
					&headerMetadata{}, nil,
				)
				fIS.On("GetHeader", uint32(0)).Return(
					fH, nil,
				)
				fIS.On("GetURI").Return("/path/to/filters")

				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				h := &headersImport{
					blockHeadersImportSource:  bIS,
					filterHeadersImportSource: fIS,
					options:                   ops,
				}

				return prep{
					hImport: h,
				}
			},
		},
		{
			name: "ErrorOnGetBlockHeaderFromTargetStore",
			prep: func() prep {
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

				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("ChainTip").Return(
					&wire.BlockHeader{}, uint32(0), nil,
				)
				bHS.On("FetchHeaderByHeight", uint32(0)).Return(
					nil, errors.New("I/O read error"),
				)

				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("ChainTip").Return(
					&chainhash.Hash{}, uint32(0), nil,
				)

				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				h := &headersImport{
					blockHeadersImportSource: bIS,
					options:                  ops,
				}

				return prep{
					hImport: h,
				}
			},
			expectErr: true,
			expectErrMsg: "failed to get block header from " +
				"target at height 0: I/O read error",
		},
		{
			name: "ErrorOnGetBlockHeaderFromImportSource",
			prep: func() prep {
				bH, err := constructBlkHdr(
					blockHdrs[0], uint32(0),
				)
				if err != nil {
					return prep{err: err}
				}

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

				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("ChainTip").Return(
					&wire.BlockHeader{}, uint32(0),
					nil,
				)
				bHS.On("FetchHeaderByHeight", uint32(0)).Return(
					bH.BlockHeader.BlockHeader, nil,
				)

				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("ChainTip").Return(
					&chainhash.Hash{}, uint32(0), nil,
				)

				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				h := &headersImport{
					blockHeadersImportSource: bIS,
					options:                  ops,
				}

				return prep{
					hImport: h,
				}
			},
			expectErr: true,
			expectErrMsg: "failed to get block header from " +
				"import source at height 1: I/O read error",
		},
		{
			name: "ErrorOnTypeAssertingBlockHeader",
			prep: func() prep {
				bH, err := constructBlkHdr(
					blockHdrs[0], uint32(0),
				)
				if err != nil {
					return prep{err: err}
				}

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

				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("ChainTip").Return(
					&wire.BlockHeader{}, uint32(0), nil,
				)
				bHS.On(
					"FetchHeaderByHeight", uint32(0),
				).Return(
					bH.BlockHeader.BlockHeader, nil,
				)

				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("ChainTip").Return(
					&chainhash.Hash{}, uint32(0), nil,
				)

				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				h := &headersImport{
					blockHeadersImportSource: bIS,
					options:                  ops,
				}

				return prep{
					hImport: h,
				}
			},
			expectErr: true,
			expectErrMsg: "expected blockHeader type, got " +
				"*chainimport.filterHeader",
		},
		{
			name: "ErrorOnFirstHeaderChainBroken",
			prep: func() prep {
				bH, err := constructBlkHdr(
					blockHdrs[0], uint32(0),
				)
				if err != nil {
					return prep{err: err}
				}

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

				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("ChainTip").Return(
					&wire.BlockHeader{}, uint32(0), nil,
				)
				bHS.On("FetchHeaderByHeight", uint32(0)).Return(
					bH.BlockHeader.BlockHeader, nil,
				)

				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("ChainTip").Return(
					&chainhash.Hash{}, uint32(0), nil,
				)

				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				h := &headersImport{
					blockHeadersImportSource: bIS,
					options:                  ops,
				}

				return prep{
					hImport: h,
				}
			},
			expectErr: true,
			expectErrMsg: "failed to validate header connection: " +
				"header chain broken",
		},
		{
			name: "ValidateOnHeightOneSuccessfully",
			prep: func() prep {
				bH, err := constructBlkHdr(
					blockHdrs[0], uint32(0),
				)
				if err != nil {
					return prep{err: err}
				}

				bH2, err := constructBlkHdr(
					blockHdrs[1], uint32(1),
				)
				if err != nil {
					return prep{err: err}
				}

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

				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("ChainTip").Return(
					&wire.BlockHeader{}, uint32(0), nil,
				)
				bHS.On("FetchHeaderByHeight", uint32(0)).Return(
					bH.BlockHeader.BlockHeader, nil,
				)

				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("ChainTip").Return(
					&chainhash.Hash{}, uint32(0), nil,
				)

				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				h := &headersImport{
					blockHeadersImportSource: bIS,
					options:                  ops,
				}

				return prep{
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
	type prep struct {
		hI  *headersImport
		err error
	}
	testCases := []struct {
		name         string
		height       uint32
		prep         func(height uint32) prep
		expectErr    bool
		expectErrMsg string
	}{
		{
			name: "ErrorOnGetHeaderMetadata",
			prep: func(uint32) prep {
				bIS := &mockHeaderImportSource{}
				bIS.On("GetHeaderMetadata").Return(
					nil, errors.New("I/O read error"),
				)
				hImport := &headersImport{
					blockHeadersImportSource: bIS,
				}
				return prep{
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
			prep: func(height uint32) prep {
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
				return prep{
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
			prep: func(height uint32) prep {
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
				return prep{
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
			prep: func(height uint32) prep {
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

				bHS := &headerfs.MockBlockHeaderStore{}
				h := height
				bHS.On("FetchHeaderByHeight", h).Return(
					nil, errors.New("I/O read error"),
				)

				ops := &ImportOptions{
					TargetBlockHeaderStore: bHS,
				}

				hImport := &headersImport{
					blockHeadersImportSource: bIS,
					options:                  ops,
				}
				return prep{
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
			prep: func(height uint32) prep {
				bH, err := constructBlkHdr(
					blockHdrs[1], uint32(1),
				)
				if err != nil {
					return prep{err: err}
				}

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

				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("FetchHeaderByHeight", height).Return(
					bH.BlockHeader.BlockHeader, nil,
				)

				ops := &ImportOptions{
					TargetBlockHeaderStore: bHS,
				}

				hImport := &headersImport{
					blockHeadersImportSource: bIS,
					options:                  ops,
				}
				return prep{
					hI: hImport,
				}
			},
			expectErr:    true,
			expectErrMsg: "block header mismatch at height 101",
		},
		{
			name:   "ErrorOnGetFilterHeaderFromImportSrc",
			height: 101,
			prep: func(height uint32) prep {
				bH, err := constructBlkHdr(
					blockHdrs[1], uint32(1),
				)
				if err != nil {
					return prep{err: err}
				}

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

				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("FetchHeaderByHeight", height).Return(
					bH.BlockHeader.BlockHeader, nil,
				)

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

				ops := &ImportOptions{
					TargetBlockHeaderStore: bHS,
				}

				hImport := &headersImport{
					blockHeadersImportSource:  bIS,
					filterHeadersImportSource: fIS,
					options:                   ops,
				}

				return prep{
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
			prep: func(height uint32) prep {
				bH, err := constructBlkHdr(
					blockHdrs[1], uint32(1),
				)
				if err != nil {
					return prep{err: err}
				}

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

				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("FetchHeaderByHeight", height).Return(
					bH.BlockHeader.BlockHeader, nil,
				)

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

				ops := &ImportOptions{
					TargetBlockHeaderStore: bHS,
				}

				hImport := &headersImport{
					blockHeadersImportSource:  bIS,
					filterHeadersImportSource: fIS,
					options:                   ops,
				}

				return prep{
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
			prep: func(height uint32) prep {
				bH, err := constructBlkHdr(
					blockHdrs[1], uint32(1),
				)
				if err != nil {
					return prep{err: err}
				}

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

				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("FetchHeaderByHeight", height).Return(
					bH.BlockHeader.BlockHeader, nil,
				)

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

				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("FetchHeaderByHeight", height).Return(
					nil, errors.New("I/O read error"),
				)

				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				hImport := &headersImport{
					blockHeadersImportSource:  bIS,
					filterHeadersImportSource: fIS,
					options:                   ops,
				}

				return prep{
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
			prep: func(height uint32) prep {
				bH, err := constructBlkHdr(
					blockHdrs[1], uint32(1),
				)
				if err != nil {
					return prep{err: err}
				}

				fH, err := constructFilterHdr(
					filterHdrs[1], uint32(1),
				)
				if err != nil {
					return prep{err: err}
				}

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

				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("FetchHeaderByHeight", height).Return(
					bH.BlockHeader.BlockHeader, nil,
				)

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

				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("FetchHeaderByHeight", height).Return(
					&fH.FilterHash, nil,
				)

				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				hImport := &headersImport{
					blockHeadersImportSource:  bIS,
					filterHeadersImportSource: fIS,
					options:                   ops,
				}

				return prep{
					hI: hImport,
				}
			},
			expectErr:    true,
			expectErrMsg: "filter header mismatch at height 101",
		},
		{
			name:   "VerifyHeadersAtTargetHeightWithNoErrs",
			height: 101,
			prep: func(height uint32) prep {
				bH, err := constructBlkHdr(
					blockHdrs[1], uint32(1),
				)
				if err != nil {
					return prep{err: err}
				}

				fH, err := constructFilterHdr(
					filterHdrs[1],
					uint32(1),
				)
				if err != nil {
					return prep{err: err}
				}

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

				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("FetchHeaderByHeight", height).Return(
					bH.BlockHeader.BlockHeader, nil,
				)

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

				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("FetchHeaderByHeight", height).Return(
					&fH.FilterHash, nil,
				)

				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				hImport := &headersImport{
					blockHeadersImportSource:  bIS,
					filterHeadersImportSource: fIS,
					options:                   ops,
				}

				return prep{
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

// setupFileWithHdrs creates a temporary file with headers and returns the file,
// a cleanup function, and an error if any.
func setupFileWithHdrs(hT headerfs.HeaderType,
	includeMetadata bool) (headerfs.File, func(), error) {

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
		err = AddHeadersImportMetadata(
			tempFile.Name(), wire.SimNet, 0, hT, 0,
		)
		if err != nil {
			return nil, cleanup, err
		}

		// We need to reopen it again to update the file descriptor
		// since previous operation closes the file atomically.
		tempFile, err = os.OpenFile(
			tempFile.Name(), os.O_RDWR|os.O_APPEND, 0644,
		)
		if err != nil {
			return nil, cleanup, fmt.Errorf("failed to open file "+
				"for writing headers: %w", err)
		}
	}

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

	if err := tempFile.Sync(); err != nil {
		return nil, cleanup, fmt.Errorf("failed to sync file: %w", err)
	}

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
