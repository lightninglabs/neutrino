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
	"net/http"
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
	"github.com/lightninglabs/neutrino/chainsync"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/stretchr/testify/mock"
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

// TestImportSkipOperation tests the import skip operation. It checks that the
// import is skipped if the target header store is populated with headers.
func TestImportSkipOperation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	type prep struct {
		options *ImportOptions
	}
	type verify struct {
		tc            *testing.T
		importOptions *ImportOptions
		importResult  *ImportResult
	}
	testCases := []struct {
		name         string
		prep         func() prep
		verify       func(verify)
		expectErr    bool
		expectErrMsg string
	}{
		{
			name: "TargetFilterStorePopulatedWithHeaders",
			prep: func() prep {
				tBS := &headerfs.MockBlockHeaderStore{}
				tBS.On("ChainTip").Return(
					&wire.BlockHeader{}, uint32(0), nil,
				)

				tFS := &headerfs.MockFilterHeaderStore{}
				tFS.On("ChainTip").Return(
					&chainhash.Hash{}, uint32(3), nil,
				)

				bIS := "/path/to/blocks"
				fIS := "/path/to/filters"

				ops := &ImportOptions{
					BlockHeadersSource:      bIS,
					FilterHeadersSource:     fIS,
					TargetBlockHeaderStore:  tBS,
					TargetFilterHeaderStore: tFS,
				}

				return prep{
					options: ops,
				}
			},
			verify: func(v verify) {
				// Verify the default batch size is set.
				ops := v.importOptions
				require.Equal(
					v.tc, defaultWriteBatchSizePerRegion,
					ops.WriteBatchSizePerRegion,
				)

				// Verify no headers added/processed.
				require.Equal(
					v.tc, 0, v.importResult.AddedCount,
				)
				require.Equal(
					v.tc, 0, v.importResult.ProcessedCount,
				)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			prep := tc.prep()
			hI, err := NewHeadersImport(prep.options)
			require.NoError(t, err)
			importResult, err := hI.Import(ctx)
			verify := verify{
				tc:            t,
				importOptions: prep.options,
				importResult:  importResult,
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

// TestImportOperationOnFileHeaderSource tests the import operation on a file
// header source. It checks that the import is successful and that the headers
// are written to the target header stores.
func TestImportOperationOnFileHeaderSource(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	type prep struct {
		options *ImportOptions
		cleanup func()
		err     error
	}
	type verify struct {
		tc            *testing.T
		importOptions *ImportOptions
		importResult  *ImportResult
	}
	testCases := []struct {
		name         string
		prep         func() prep
		verify       func(verify)
		expectErr    bool
		expectErrMsg string
	}{
		{
			name: "ImportWithoutErrors",
			prep: func() prep {
				tempDir := t.TempDir()
				c1 := func() {
					os.RemoveAll(tempDir)
				}

				dbPath := filepath.Join(tempDir, "test.db")
				db, err := walletdb.Create(
					"bdb", dbPath, true, time.Second*10,
				)
				c2 := func() {
					db.Close()
					c1()
				}
				if err != nil {
					return prep{
						cleanup: c2,
						err:     err,
					}
				}

				b, err := headerfs.NewBlockHeaderStore(
					tempDir, db, &chaincfg.SimNetParams,
				)
				if err != nil {
					return prep{
						cleanup: c2,
						err:     err,
					}
				}

				f, err := headerfs.NewFilterHeaderStore(
					tempDir, db, headerfs.RegularFilter,
					&chaincfg.SimNetParams, nil,
				)
				if err != nil {
					return prep{
						cleanup: c2,
						err:     err,
					}
				}

				bFile, c3, err := setupFileWithHdrs(
					headerfs.Block, true,
				)
				c4 := func() {
					c3()
					c2()
				}
				if err != nil {
					return prep{
						cleanup: c4,
						err:     err,
					}
				}
				bPath := bFile.Name()

				// Close block headers import source to be
				// opened during import.
				bFile.Close()

				fFile, c5, err := setupFileWithHdrs(
					headerfs.RegularFilter, true,
				)
				cleanup := func() {
					c5()
					c4()
				}
				if err != nil {
					return prep{
						cleanup: cleanup,
						err:     err,
					}
				}
				fPath := fFile.Name()

				// Close filter headers import source to be
				// opened during import.
				fFile.Close()

				tCP := chaincfg.SimNetParams
				flags := blockchain.BFFastAdd

				ops := &ImportOptions{
					BlockHeadersSource:      bPath,
					FilterHeadersSource:     fPath,
					TargetBlockHeaderStore:  b,
					TargetFilterHeaderStore: f,
					TargetChainParams:       tCP,
					WriteBatchSizePerRegion: 128,
					ValidationFlags:         flags,
				}

				return prep{
					options: ops,
					cleanup: cleanup,
				}
			},
			verify: func(v verify) {
				// Verify the user write batch size is used
				// instead of the default one.
				ops := v.importOptions
				require.Equal(
					v.tc, 128, ops.WriteBatchSizePerRegion,
				)

				// Verify headers added/processed excluding the
				// genesis header.
				require.Equal(
					v.tc, len(blockHdrs)-1,
					v.importResult.AddedCount,
				)
				require.Equal(
					v.tc, len(blockHdrs)-1,
					v.importResult.ProcessedCount,
				)

				// Verify no headers skipped.
				require.Equal(
					v.tc, 0, v.importResult.SkippedCount,
				)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			prep := tc.prep()
			t.Cleanup(prep.cleanup)
			require.NoError(t, prep.err)

			hI, err := NewHeadersImport(prep.options)
			require.NoError(t, err)
			importResult, err := hI.Import(ctx)
			verify := verify{
				tc:            t,
				importOptions: prep.options,
				importResult:  importResult,
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

// TestImportOperationOnHTTPHeaderSource tests the import operation on a HTTP
// header source. It checks that the import is successful and that the headers
// are written to the target header stores.
func TestImportOperationOnHTTPHeaderSource(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	type prep struct {
		hImport *headersImport
		cleanup func()
		err     error
	}
	type verify struct {
		tc           *testing.T
		importResult *ImportResult
	}
	testCases := []struct {
		name         string
		prep         func() prep
		verify       func(verify)
		expectErr    bool
		expectErrMsg string
	}{
		{
			name: "ImportWithoutErrors",
			prep: func() prep {
				tempDir := t.TempDir()
				c1 := func() {
					os.RemoveAll(tempDir)
				}

				dbPath := filepath.Join(tempDir, "test.db")
				db, err := walletdb.Create(
					"bdb", dbPath, true, time.Second*10,
				)
				c2 := func() {
					db.Close()
					c1()
				}
				if err != nil {
					return prep{
						cleanup: c2,
						err:     err,
					}
				}

				b, err := headerfs.NewBlockHeaderStore(
					tempDir, db, &chaincfg.SimNetParams,
				)
				if err != nil {
					return prep{
						cleanup: c2,
						err:     err,
					}
				}

				f, err := headerfs.NewFilterHeaderStore(
					tempDir, db, headerfs.RegularFilter,
					&chaincfg.SimNetParams, nil,
				)
				if err != nil {
					return prep{
						cleanup: c2,
						err:     err,
					}
				}

				bFile, c3, err := setupFileWithHdrs(
					headerfs.Block, true,
				)
				c4 := func() {
					c3()
					c2()
				}
				if err != nil {
					return prep{
						cleanup: c4,
						err:     err,
					}
				}

				fFile, c5, err := setupFileWithHdrs(
					headerfs.RegularFilter, true,
				)
				cleanup := func() {
					c5()
					c4()
				}
				if err != nil {
					return prep{
						cleanup: cleanup,
						err:     err,
					}
				}

				mockHTTPClient := &mockHTTPClient{}
				origin := "http://localhost:8311"

				bRS := origin + "/headers/0"

				blockStatus := http.StatusOK
				blockBody := io.NopCloser(bFile)
				blockRes := &http.Response{
					StatusCode: blockStatus,
					Body:       blockBody,
				}
				mockHTTPClient.On("Get", bRS).Return(
					blockRes, nil,
				)

				fRS := origin + "/filter-headers/0"

				filterStatus := http.StatusOK
				filterBody := io.NopCloser(fFile)
				filterRes := &http.Response{
					StatusCode: filterStatus,
					Body:       filterBody,
				}
				mockHTTPClient.On("Get", fRS).Return(
					filterRes, nil,
				)

				bIS := newFileHeaderImportSource(
					"", newBlockHeader,
				)

				fIS := newFileHeaderImportSource(
					"", newFilterHeader,
				)

				bS := newHTTPHeaderImportSource(
					bRS, mockHTTPClient, bIS,
				)

				fS := newHTTPHeaderImportSource(
					fRS, mockHTTPClient, fIS,
				)

				tCP := chaincfg.SimNetParams
				flags := blockchain.BFFastAdd

				ops := &ImportOptions{
					TargetChainParams:       tCP,
					TargetBlockHeaderStore:  b,
					TargetFilterHeaderStore: f,
					BlockHeadersSource:      bRS,
					FilterHeadersSource:     fRS,
					WriteBatchSizePerRegion: 101,
					ValidationFlags:         flags,
				}

				bV := ops.createBlockHeaderValidator(bS)
				fV := ops.createFilterHeaderValidator()

				headersImport := &headersImport{
					options:                   ops,
					blockHeadersImportSource:  bS,
					filterHeadersImportSource: fS,
					blockHeadersValidator:     bV,
					filterHeadersValidator:    fV,
				}

				return prep{
					hImport: headersImport,
					cleanup: cleanup,
				}
			},
			verify: func(v verify) {
				// Verify headers added/processed excluding the
				// genesis header.
				require.Equal(
					v.tc, len(blockHdrs)-1,
					v.importResult.AddedCount,
				)
				require.Equal(
					v.tc, len(blockHdrs)-1,
					v.importResult.ProcessedCount,
				)

				// Verify no headers skipped.
				require.Equal(
					v.tc, 0, v.importResult.SkippedCount,
				)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			prep := tc.prep()
			importResult, err := prep.hImport.Import(ctx)
			verify := verify{
				tc:           t,
				importResult: importResult,
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
			name: "MissingBlockANDFilterHeaderValidators",
			prep: func() prep {
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
				return prep{
					hImport: headersImport,
					cleanup: func() {},
					err:     nil,
				}
			},
			verify:       func(verify) {},
			expectErr:    true,
			expectErrMsg: "missing required header validators",
		},
		{
			name: "MissingBlockHeaderValidator",
			prep: func() prep {
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
				return prep{
					hImport: headersImport,
					cleanup: func() {},
					err:     nil,
				}
			},
			verify:       func(verify) {},
			expectErr:    true,
			expectErrMsg: "missing required header validators",
		},
		{
			name: "MissingFilterHeaderValidator",
			prep: func() prep {
				opts := &ImportOptions{}
				bS := opts.createBlockHeaderImportSrc()
				fS := opts.createFilterHeaderImportSrc()
				bV := opts.createBlockHeaderValidator(bS)
				headersImport := &headersImport{
					options:                   opts,
					blockHeadersImportSource:  bS,
					filterHeadersImportSource: fS,
					blockHeadersValidator:     bV,
					filterHeadersValidator:    nil,
				}
				return prep{
					hImport: headersImport,
					cleanup: func() {},
					err:     nil,
				}
			},
			verify:       func(verify) {},
			expectErr:    true,
			expectErrMsg: "missing required header validators",
		},
		{
			name: "ErrorOnBlockFileNotExist",
			prep: func() prep {
				opts := &ImportOptions{}
				bS := opts.createBlockHeaderImportSrc()
				fS := opts.createFilterHeaderImportSrc()
				bV := opts.createBlockHeaderValidator(bS)
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
				bV := opts.createBlockHeaderValidator(bS)
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
				bV := opts.createBlockHeaderValidator(bS)
				fV := opts.createFilterHeaderValidator()

				bS.SetURI(blockFile.Name())

				headersImport := &headersImport{
					options:                   opts,
					blockHeadersImportSource:  bS,
					filterHeadersImportSource: fS,
					blockHeadersValidator:     bV,
					filterHeadersValidator:    fV,
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
				bV := opts.createBlockHeaderValidator(bS)
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
				bV := opts.createBlockHeaderValidator(bS)
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

// TestOpenHTTPHeaderImportSources tests the open operation on a HTTP header
// import source.
func TestOpenHTTPHeaderImportSources(t *testing.T) {
	t.Parallel()
	type prep struct {
		hImport *headersImport
		cleanup func()
		err     error
	}
	testCases := []struct {
		name         string
		prep         func() prep
		expectErr    bool
		expectErrMsg string
	}{
		{
			name: "ErrorOnGetBlockHeadersOverHTTP",
			prep: func() prep {
				mockHTTPClient := &mockHTTPClient{}
				origin := "http://localhost:8234"

				bRS := origin + "/headers/0"
				mockHTTPClient.On("Get", bRS).Return(
					nil, errors.New("failed to "+
						"download block "+
						"headers"),
				).Once()

				fRS := origin + "/filter-headers/0"

				status := http.StatusOK
				body := io.NopCloser(bytes.NewBufferString(""))
				res := &http.Response{
					StatusCode: status,
					Body:       body,
				}
				mockHTTPClient.On("Get", fRS).Return(res, nil)

				opts := &ImportOptions{
					BlockHeadersSource:  bRS,
					FilterHeadersSource: fRS,
				}
				bS := newHTTPHeaderImportSource(
					bRS, mockHTTPClient,
					&mockHeaderImportSource{},
				)
				fS := newHTTPHeaderImportSource(
					fRS, mockHTTPClient,
					&mockHeaderImportSource{},
				)
				bV := opts.createBlockHeaderValidator(bS)
				fV := opts.createFilterHeaderValidator()

				headersImport := &headersImport{
					options:                   opts,
					blockHeadersImportSource:  bS,
					filterHeadersImportSource: fS,
					blockHeadersValidator:     bV,
					filterHeadersValidator:    fV,
				}

				return prep{
					hImport: headersImport,
					cleanup: func() {},
				}
			},
			expectErr:    true,
			expectErrMsg: "failed to download block headers",
		},
		{
			name: "ErrorOnGetBlockHeadersOverHTTPNotFound",
			prep: func() prep {
				mockHTTPClient := &mockHTTPClient{}
				origin := "http://localhost:7334"

				bRS := origin + "/headers/0"
				status := http.StatusNotFound
				body := io.NopCloser(
					bytes.NewBufferString(""),
				)
				res := &http.Response{
					StatusCode: status,
					Body:       body,
				}
				mockHTTPClient.On("Get", bRS).Return(
					res, nil,
				).Once()

				fRS := origin + "/filter-headers/0"

				filterStatus := http.StatusOK
				filterBody := io.NopCloser(
					bytes.NewBufferString(""),
				)
				filterRes := &http.Response{
					StatusCode: filterStatus,
					Body:       filterBody,
				}
				mockHTTPClient.On("Get", fRS).Return(
					filterRes, nil,
				)

				opts := &ImportOptions{
					BlockHeadersSource:  bRS,
					FilterHeadersSource: fRS,
				}
				bS := newHTTPHeaderImportSource(
					bRS, mockHTTPClient,
					&mockHeaderImportSource{},
				)
				fS := newHTTPHeaderImportSource(
					fRS, mockHTTPClient,
					&mockHeaderImportSource{},
				)
				bV := opts.createBlockHeaderValidator(bS)
				fV := opts.createFilterHeaderValidator()

				headersImport := &headersImport{
					options:                   opts,
					blockHeadersImportSource:  bS,
					filterHeadersImportSource: fS,
					blockHeadersValidator:     bV,
					filterHeadersValidator:    fV,
				}

				return prep{
					hImport: headersImport,
					cleanup: func() {},
				}
			},
			expectErr: true,
			expectErrMsg: fmt.Sprintf("failed to download file: "+
				"status code %d", http.StatusNotFound),
		},
		{
			name: "ErrorOnGetFilterHeadersOverHTTP",
			prep: func() prep {
				mockHTTPClient := &mockHTTPClient{}
				origin := "http://localhost:8344"

				bRS := origin + "/headers/0"

				blockStatus := http.StatusOK
				blockBody := io.NopCloser(
					bytes.NewBufferString(""),
				)
				blockRes := &http.Response{
					StatusCode: blockStatus,
					Body:       blockBody,
				}
				mockHTTPClient.On("Get", bRS).Return(
					blockRes, nil,
				)

				fRS := origin + "/filter-headers/0"
				mockHTTPClient.On("Get", fRS).Return(
					nil, errors.New("failed to "+
						"download filter "+
						"headers"),
				).Once()

				bIS := &mockHeaderImportSource{}
				bIS.On("Open").Return(nil)
				bIS.On("SetURI", mock.Anything).Return()

				opts := &ImportOptions{
					BlockHeadersSource:  bRS,
					FilterHeadersSource: fRS,
				}
				bS := newHTTPHeaderImportSource(
					bRS, mockHTTPClient, bIS,
				)
				fS := newHTTPHeaderImportSource(
					fRS, mockHTTPClient,
					&mockHeaderImportSource{},
				)
				cleanup := func() {
					os.Remove(bS.uri)
				}

				bV := opts.createBlockHeaderValidator(bS)
				fV := opts.createFilterHeaderValidator()

				headersImport := &headersImport{
					options:                   opts,
					blockHeadersImportSource:  bS,
					filterHeadersImportSource: fS,
					blockHeadersValidator:     bV,
					filterHeadersValidator:    fV,
				}

				return prep{
					hImport: headersImport,
					cleanup: cleanup,
				}
			},
			expectErr:    true,
			expectErrMsg: "failed to download filter headers",
		},
		{
			name: "ErrorOnGetHTTPFilterHeadersNotFound",
			prep: func() prep {
				mockHTTPClient := &mockHTTPClient{}
				origin := "http://localhost:8334"

				bRS := origin + "/headers/0"

				blockStatus := http.StatusOK
				blockBody := io.NopCloser(
					bytes.NewBufferString(""),
				)
				blockRes := &http.Response{
					StatusCode: blockStatus,
					Body:       blockBody,
				}
				mockHTTPClient.On("Get", bRS).Return(
					blockRes, nil,
				)

				fRS := origin + "/filter-headers/0"
				status := http.StatusNotFound
				body := io.NopCloser(bytes.NewBufferString(""))
				res := &http.Response{
					StatusCode: status,
					Body:       body,
				}
				mockHTTPClient.On("Get", fRS).Return(
					res, nil,
				).Once()

				bIS := &mockHeaderImportSource{}
				bIS.On("Open").Return(nil)
				bIS.On("SetURI", mock.Anything).Return()

				opts := &ImportOptions{
					BlockHeadersSource:  bRS,
					FilterHeadersSource: fRS,
				}
				bS := newHTTPHeaderImportSource(
					bRS, mockHTTPClient, bIS,
				)
				fS := newHTTPHeaderImportSource(
					fRS, mockHTTPClient,
					&mockHeaderImportSource{},
				)
				cleanup := func() {
					os.Remove(bS.uri)
				}

				bV := opts.createBlockHeaderValidator(bS)
				fV := opts.createFilterHeaderValidator()

				headersImport := &headersImport{
					options:                   opts,
					blockHeadersImportSource:  bS,
					filterHeadersImportSource: fS,
					blockHeadersValidator:     bV,
					filterHeadersValidator:    fV,
				}

				return prep{
					hImport: headersImport,
					cleanup: cleanup,
				}
			},
			expectErr: true,
			expectErrMsg: fmt.Sprintf("failed to download file: "+
				"status code %d", http.StatusNotFound),
		},
		{
			name: "OpenSourcesCorrectly",
			prep: func() prep {
				mockHTTPClient := &mockHTTPClient{}
				origin := "http://localhost:8323"

				bRS := origin + "/headers/0"

				blockStatus := http.StatusOK
				blockBody := io.NopCloser(
					bytes.NewBufferString(""),
				)
				blockRes := &http.Response{
					StatusCode: blockStatus,
					Body:       blockBody,
				}
				mockHTTPClient.On("Get", bRS).Return(
					blockRes, nil,
				)

				fRS := origin + "/filter-headers/0"

				filterStatus := http.StatusOK
				filterBody := io.NopCloser(
					bytes.NewBufferString(""),
				)
				filterRes := &http.Response{
					StatusCode: filterStatus,
					Body:       filterBody,
				}
				mockHTTPClient.On("Get", fRS).Return(
					filterRes, nil,
				)

				bIS := &mockHeaderImportSource{}
				bIS.On("Open").Return(nil)
				bIS.On("SetURI", mock.Anything).Return()

				fIS := &mockHeaderImportSource{}
				fIS.On("Open").Return(nil)
				fIS.On("SetURI", mock.Anything).Return()

				opts := &ImportOptions{
					BlockHeadersSource:  bRS,
					FilterHeadersSource: fRS,
				}
				bS := newHTTPHeaderImportSource(
					bRS, mockHTTPClient,
					bIS,
				)
				fS := newHTTPHeaderImportSource(
					fRS, mockHTTPClient,
					fIS,
				)
				cleanup := func() {
					os.Remove(fS.uri)
					os.Remove(bS.uri)
				}

				bV := opts.createBlockHeaderValidator(bS)
				fV := opts.createFilterHeaderValidator()

				headersImport := &headersImport{
					options:                   opts,
					blockHeadersImportSource:  bS,
					filterHeadersImportSource: fS,
					blockHeadersValidator:     bV,
					filterHeadersValidator:    fV,
				}

				return prep{
					hImport: headersImport,
					cleanup: cleanup,
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			prep := tc.prep()
			t.Cleanup(prep.cleanup)
			require.NoError(t, prep.err)

			err := prep.hImport.openSources()
			if tc.expectErr {
				require.ErrorContains(t, err, tc.expectErrMsg)
				return
			}
			require.NoError(t, err)
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

// TestHeaderRetrievalOnSingleHeader tests the header retrieval on a single
// header. It checks that the header is retrieved correctly from the import
// source.
func TestHeaderRetrievalOnSingleHeader(t *testing.T) {
	t.Parallel()
	type prep struct {
		hISource HeaderImportSource
		cleanup  func()
		err      error
	}
	type verify struct {
		tc     *testing.T
		header Header
		index  int
	}
	testCases := []struct {
		name         string
		index        int
		hType        headerfs.HeaderType
		prep         func(headerfs.HeaderType) prep
		verify       func(verify)
		expectErr    bool
		expectErrMsg string
	}{
		{
			name:  "ErrorOnReaderNotInitialized",
			hType: headerfs.Block,
			prep: func(hType headerfs.HeaderType) prep {
				opts := &ImportOptions{}
				bS := opts.createBlockHeaderImportSrc()
				return prep{
					hISource: bS,
					cleanup:  func() {},
				}
			},
			verify:       func(verify) {},
			expectErr:    true,
			expectErrMsg: "file reader not initialized",
		},
		{
			name:  "ErrorOnHeaderMetadataNotInitialized",
			hType: headerfs.Block,
			prep: func(hType headerfs.HeaderType) prep {
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

				bFS.metadata = nil

				return prep{
					hISource: bFS,
					cleanup:  cleanup,
				}
			},
			verify:       func(verify) {},
			expectErr:    true,
			expectErrMsg: "header metadata not initialized",
		},
		{
			name:  "ErrorOnDeserializeUnknownHeaderType",
			hType: headerfs.UnknownHeader,
			prep: func(hType headerfs.HeaderType) prep {
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

				bFS.metadata = &headerMetadata{
					importMetadata: &importMetadata{
						headerType: hType,
					},
				}

				return prep{
					hISource: bFS,
					cleanup:  cleanup,
				}
			},
			verify:    func(v verify) {},
			expectErr: true,
			expectErrMsg: "failed to deserialize " +
				"wire.BlockHeader: EOF",
		},
		{
			name:  "ErrorOnHeaderIndexOutOfBounds",
			hType: headerfs.Block,
			index: len(blockHdrs),
			prep: func(hType headerfs.HeaderType) prep {
				bFile, c1, err := setupFileWithHdrs(hType, true)
				if err != nil {
					return prep{
						cleanup: c1,
						err:     err,
					}
				}

				opts := &ImportOptions{}
				bS := opts.createBlockHeaderImportSrc()
				bS.SetURI(bFile.Name())

				err = bS.Open()
				cleanup := func() {
					bS.Close()
					os.Remove(bFile.Name())
				}
				if err != nil {
					return prep{
						cleanup: cleanup,
						err:     err,
					}
				}

				return prep{
					hISource: bS,
					cleanup:  cleanup,
				}
			},
			verify:    func(verify) {},
			expectErr: true,
			expectErrMsg: fmt.Sprintf("failed to read header at "+
				"index %d", len(blockHdrs)),
		},
		{
			name:  "GetBlockHeaderSuccessfully",
			hType: headerfs.Block,
			index: 3,
			prep: func(hType headerfs.HeaderType) prep {
				bFile, c1, err := setupFileWithHdrs(hType, true)
				if err != nil {
					return prep{
						cleanup: c1,
						err:     err,
					}
				}

				opts := &ImportOptions{}
				bS := opts.createBlockHeaderImportSrc()
				bS.SetURI(bFile.Name())

				err = bS.Open()
				cleanup := func() {
					bS.Close()
					os.Remove(bFile.Name())
				}
				if err != nil {
					return prep{
						cleanup: cleanup,
						err:     err,
					}
				}

				return prep{
					hISource: bS,
					cleanup:  cleanup,
				}
			},
			verify: func(v verify) {
				bH, ok := v.header.(*blockHeader)
				require.True(v.tc, ok)

				bHExpected, err := constructBlkHdr(
					blockHdrs[v.index], uint32(v.index),
				)
				require.NoError(t, err)

				// Assert that the known block header at this
				// index matches the retrieved one.
				require.Equal(v.tc, bHExpected, bH)
			},
		},
		{
			name:  "GetFilterHeaderSuccessfully",
			hType: headerfs.RegularFilter,
			index: 3,
			prep: func(hType headerfs.HeaderType) prep {
				fFile, c1, err := setupFileWithHdrs(hType, true)
				if err != nil {
					return prep{
						cleanup: c1,
						err:     err,
					}
				}

				opts := &ImportOptions{}
				fS := opts.createFilterHeaderImportSrc()
				fS.SetURI(fFile.Name())

				err = fS.Open()
				cleanup := func() {
					fS.Close()
					os.Remove(fFile.Name())
				}
				if err != nil {
					return prep{
						cleanup: cleanup,
						err:     err,
					}
				}

				return prep{
					hISource: fS,
					cleanup:  cleanup,
				}
			},
			verify: func(v verify) {
				fH, ok := v.header.(*filterHeader)
				require.True(v.tc, ok)

				fHExpected, err := constructFilterHdr(
					filterHdrs[v.index], uint32(v.index),
				)
				require.NoError(t, err)

				// Assert that the known filter header at this
				// index matches the retrieved one.
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
			verify := verify{
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
	type prep struct {
		hIterator HeaderIterator
		cleanup   func()
		err       error
	}
	type verify struct {
		tc        *testing.T
		hIterator HeaderIterator
		header    Header
	}
	testCases := []struct {
		name         string
		index        int
		hType        headerfs.HeaderType
		prep         func(hT headerfs.HeaderType, indx int) prep
		verify       func(verify)
		expectErr    bool
		expectErrMsg string
	}{
		{
			name:  "ErrorOnHeaderOutOfBounds",
			index: len(blockHdrs),
			hType: headerfs.Block,
			prep: func(hT headerfs.HeaderType, i int) prep {
				bFile, c1, err := setupFileWithHdrs(hT, true)
				if err != nil {
					return prep{
						cleanup: c1,
						err:     err,
					}
				}

				opts := &ImportOptions{}
				bS := opts.createBlockHeaderImportSrc()
				bS.SetURI(bFile.Name())

				err = bS.Open()
				c2 := func() {
					bS.Close()
					os.Remove(bFile.Name())
				}
				if err != nil {
					return prep{
						cleanup: c2,
						err:     err,
					}
				}

				current := uint32(i)
				end := uint32(i)
				iter := &importSourceHeaderIterator{
					source:     bS,
					startIndex: current,
					endIndex:   end,
					batchSize:  128,
				}

				cleanup := func() {
					c2()
				}

				return prep{
					hIterator: iter,
					cleanup:   cleanup,
				}
			},
			verify: func(v verify) {
				require.Nil(v.tc, v.header)
			},
			expectErr:    true,
			expectErrMsg: io.EOF.Error(),
		},
		{
			name:  "NoMoreHeadersToIterateOver",
			index: len(blockHdrs) - 1,
			hType: headerfs.Block,
			prep: func(hT headerfs.HeaderType, i int) prep {
				bFile, c1, err := setupFileWithHdrs(hT, true)
				if err != nil {
					return prep{
						cleanup: c1,
						err:     err,
					}
				}

				opts := &ImportOptions{}
				bS := opts.createBlockHeaderImportSrc()
				bS.SetURI(bFile.Name())

				err = bS.Open()
				c2 := func() {
					bS.Close()
					os.Remove(bFile.Name())
				}
				if err != nil {
					return prep{
						cleanup: c2,
						err:     err,
					}
				}

				current := uint32(i + 1)
				end := uint32(i)
				iter := &importSourceHeaderIterator{
					source:     bS,
					startIndex: current,
					endIndex:   end,
					batchSize:  128,
				}

				cleanup := func() {
					c2()
				}

				return prep{
					hIterator: iter,
					cleanup:   cleanup,
				}
			},
			verify: func(v verify) {
				require.Nil(v.tc, v.header)
			},
			expectErr:    true,
			expectErrMsg: io.EOF.Error(),
		},
		{
			name:  "IterateOverBlockHeadersSuccessfully",
			index: 0,
			hType: headerfs.Block,
			prep: func(hT headerfs.HeaderType, i int) prep {
				bFile, c1, err := setupFileWithHdrs(hT, true)
				if err != nil {
					return prep{
						cleanup: c1,
						err:     err,
					}
				}

				opts := &ImportOptions{}
				bS := opts.createBlockHeaderImportSrc()
				bS.SetURI(bFile.Name())

				err = bS.Open()
				c2 := func() {
					bS.Close()
					os.Remove(bFile.Name())
				}
				if err != nil {
					return prep{
						cleanup: c2,
						err:     err,
					}
				}

				current := uint32(i)
				end := uint32(len(blockHdrs) - 1)
				iter := &importSourceHeaderIterator{
					source:     bS,
					startIndex: current,
					endIndex:   end,
					batchSize:  128,
				}

				cleanup := func() {
					c2()
				}

				return prep{
					hIterator: iter,
					cleanup:   cleanup,
				}
			},
			verify: func(v verify) {
				iter := v.hIterator
				nBHs := len(blockHdrs)
				headerSeq := iter.Iterator(1, uint32(nBHs-1))
				i := 1
				for h, err := range headerSeq {
					require.NoError(v.tc, err)
					hE, err := constructBlkHdr(
						blockHdrs[i], uint32(i),
					)
					require.NoError(t, err)
					require.Equal(v.tc, hE, h)
					i++
				}
			},
		},
		{
			name:  "IterateOverFilterHeadersSuccessfully",
			index: 0,
			hType: headerfs.RegularFilter,
			prep: func(hT headerfs.HeaderType, i int) prep {
				fFile, c1, err := setupFileWithHdrs(
					hT, true,
				)
				if err != nil {
					return prep{
						cleanup: c1,
						err:     err,
					}
				}

				opts := &ImportOptions{}
				fS := opts.createFilterHeaderImportSrc()
				fS.SetURI(fFile.Name())

				err = fS.Open()
				c2 := func() {
					fS.Close()
					os.Remove(fFile.Name())
				}
				if err != nil {
					return prep{
						cleanup: c2,
						err:     err,
					}
				}

				current := uint32(i)
				end := uint32(len(filterHdrs) - 1)
				iter := &importSourceHeaderIterator{
					source:     fS,
					startIndex: current,
					endIndex:   end,
					batchSize:  128,
				}

				cleanup := func() {
					c2()
				}

				return prep{
					hIterator: iter,
					cleanup:   cleanup,
				}
			},
			verify: func(v verify) {
				iter := v.hIterator
				nFHs := len(filterHdrs)
				headerSeq := iter.Iterator(1, uint32(nFHs-1))
				i := 1
				for h, err := range headerSeq {
					require.NoError(v.tc, err)
					hE, err := constructFilterHdr(
						filterHdrs[i], uint32(i),
					)
					require.NoError(t, err)
					require.Equal(v.tc, hE, h)
					i++
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			prep := tc.prep(tc.hType, tc.index)
			t.Cleanup(prep.cleanup)
			require.NoError(t, prep.err)

			headerSeq := prep.hIterator.Iterator(
				prep.hIterator.GetStartIndex(),
				prep.hIterator.GetEndIndex(),
			)

			// Attempt to get the first header from the sequence.
			var (
				header Header
				err    error
			)
			for header, err = range headerSeq {
				break
			}

			verify := verify{
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

// TestHeaderValidationOnBlockHeadersPair tests the header validation on a block
// header pair. It checks that the header is validated correctly.
func TestHeaderValidationOnBlockHeadersPair(t *testing.T) {
	t.Parallel()
	type prep struct {
		hValidator HeadersValidator
		prev       Header
		current    Header
		err        error
	}
	testCases := []struct {
		name         string
		tCP          chaincfg.Params
		prep         func(tCP chaincfg.Params) prep
		expectErr    bool
		expectErrMsg string
	}{
		{
			name: "ErrorOnMismatchPreviousHeaderType",
			tCP:  chaincfg.Params{},
			prep: func(chaincfg.Params) prep {
				opts := &ImportOptions{}
				bS := opts.createBlockHeaderImportSrc()
				bHV := opts.createBlockHeaderValidator(bS)
				return prep{
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
			prep: func(chaincfg.Params) prep {
				opts := &ImportOptions{}
				bS := opts.createBlockHeaderImportSrc()
				bHV := opts.createBlockHeaderValidator(bS)
				return prep{
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
			prep: func(chaincfg.Params) prep {
				opts := &ImportOptions{}
				bS := opts.createBlockHeaderImportSrc()
				bHV := opts.createBlockHeaderValidator(bS)

				prevH, err := constructBlkHdr(
					blockHdrs[0], uint32(0),
				)
				if err != nil {
					return prep{
						err: err,
					}
				}

				nBH := len(blockHdrs)
				currentH, err := constructBlkHdr(
					blockHdrs[nBH-1], uint32(nBH-1),
				)
				if err != nil {
					return prep{
						err: err,
					}
				}

				return prep{
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
			prep: func(chaincfg.Params) prep {
				opts := &ImportOptions{}
				bS := opts.createBlockHeaderImportSrc()
				bHV := opts.createBlockHeaderValidator(bS)

				originH, err := constructBlkHdr(
					blockHdrs[0], uint32(0),
				)
				if err != nil {
					return prep{
						err: err,
					}
				}

				prevH, err := constructBlkHdr(
					blockHdrs[1], uint32(1),
				)
				if err != nil {
					return prep{
						err: err,
					}
				}

				currentH, err := constructBlkHdr(
					blockHdrs[2], uint32(2),
				)
				if err != nil {
					return prep{
						err: err,
					}
				}

				currentH.PrevBlock = originH.BlockHash()

				return prep{
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
			name: "ErrorOnDifficultyDropExceedsLimit",
			tCP:  chaincfg.SimNetParams,
			prep: func(tCP chaincfg.Params) prep {
				opts := &ImportOptions{
					TargetChainParams: tCP,
				}
				bS := opts.createBlockHeaderImportSrc()
				bHV := opts.createBlockHeaderValidator(bS)

				prevH, err := constructBlkHdr(
					blockHdrs[0], uint32(0),
				)
				if err != nil {
					return prep{
						err: err,
					}
				}

				currentH, err := constructBlkHdr(
					blockHdrs[1], uint32(1),
				)
				if err != nil {
					return prep{
						err: err,
					}
				}

				// Set a difficulty that would cause a drop
				// exceeding simnet's retarget adjustment factor
				// limit. Simnet allows difficulty to drop by up
				// to 4x (RetargetAdjustmentFactor = 4).
				prevDifficulty := blockchain.CompactToBig(
					prevH.Bits,
				)

				// Create a difficulty that's 5x lower
				// (exceeds the 4x limit).
				excessiveDrop := new(big.Int).Div(
					prevDifficulty, big.NewInt(5),
				)
				currentH.Bits = blockchain.BigToCompact(
					excessiveDrop,
				)

				return prep{
					hValidator: bHV,
					prev:       prevH,
					current:    currentH,
				}
			},
			expectErr: true,
			expectErrMsg: "block header contextual validation " +
				"failed: block difficulty of .* is not the " +
				"expected value of .*",
		},
		{
			name: "ErrorOnZeroDifficultyTarget",
			tCP:  chaincfg.SimNetParams,
			prep: func(tCP chaincfg.Params) prep {
				opts := &ImportOptions{
					TargetChainParams: tCP,
					ValidationFlags:   blockchain.BFFastAdd,
				}
				bS := opts.createBlockHeaderImportSrc()
				bHV := opts.createBlockHeaderValidator(bS)

				prevH, err := constructBlkHdr(
					blockHdrs[0], uint32(0),
				)
				if err != nil {
					return prep{
						err: err,
					}
				}

				currentH, err := constructBlkHdr(
					blockHdrs[1], uint32(1),
				)
				if err != nil {
					return prep{
						err: err,
					}
				}

				currentH.Bits = 0

				return prep{
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
			prep: func(tCP chaincfg.Params) prep {
				opts := &ImportOptions{
					TargetChainParams: tCP,
					ValidationFlags:   blockchain.BFFastAdd,
				}
				bS := opts.createBlockHeaderImportSrc()
				bHV := opts.createBlockHeaderValidator(bS)

				prevH, err := constructBlkHdr(
					blockHdrs[0], uint32(0),
				)
				if err != nil {
					return prep{
						err: err,
					}
				}

				currentH, err := constructBlkHdr(
					blockHdrs[1], uint32(1),
				)
				if err != nil {
					return prep{
						err: err,
					}
				}

				invT := new(big.Int).Add(
					tCP.PowLimit, big.NewInt(1),
				)
				newT := blockchain.BigToCompact(invT)
				currentH.Bits = newT

				return prep{
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
			prep: func(tCP chaincfg.Params) prep {
				opts := &ImportOptions{
					TargetChainParams: tCP,
					ValidationFlags:   blockchain.BFFastAdd,
				}
				bS := opts.createBlockHeaderImportSrc()
				bHV := opts.createBlockHeaderValidator(bS)

				prevH, err := constructBlkHdr(
					blockHdrs[0], uint32(0),
				)
				if err != nil {
					return prep{
						err: err,
					}
				}

				currentH, err := constructBlkHdr(
					blockHdrs[1], uint32(1),
				)
				if err != nil {
					return prep{
						err: err,
					}
				}

				// Set an extremely high difficulty
				// (very small target).
				currentH.Bits = 0x1f000001

				// We need to ensure the block hash will be
				// higher than target.
				currentH.Nonce = 0xffffffff
				currentH.MerkleRoot = chainhash.Hash{
					0xff, 0xff, 0xff,
				}

				return prep{
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
			prep: func(tCP chaincfg.Params) prep {
				opts := &ImportOptions{
					TargetChainParams: tCP,
					ValidationFlags:   blockchain.BFFastAdd,
				}
				bS := opts.createBlockHeaderImportSrc()
				bHV := opts.createBlockHeaderValidator(bS)

				prevH, err := constructBlkHdr(
					blockHdrs[0], uint32(0),
				)
				if err != nil {
					return prep{
						err: err,
					}
				}

				currentH, err := constructBlkHdr(
					blockHdrs[1], uint32(1),
				)
				if err != nil {
					return prep{
						err: err,
					}
				}

				currentH.Timestamp = time.Unix(0, 50)
				return prep{
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
			prep: func(tCP chaincfg.Params) prep {
				opts := &ImportOptions{
					TargetChainParams: tCP,
					ValidationFlags:   blockchain.BFFastAdd,
				}
				bS := opts.createBlockHeaderImportSrc()
				bHV := opts.createBlockHeaderValidator(bS)

				prevH, err := constructBlkHdr(
					blockHdrs[0], uint32(0),
				)
				if err != nil {
					return prep{
						err: err,
					}
				}

				currentH, err := constructBlkHdr(
					blockHdrs[1], uint32(1),
				)
				if err != nil {
					return prep{
						err: err,
					}
				}

				return prep{
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
	ctx := context.Background()
	type prep struct {
		iterator  HeaderIterator
		validator HeadersValidator
		cleanup   func()
		err       error
	}
	testCases := []struct {
		name         string
		index        int
		prep         func() prep
		expectErr    bool
		expectErrMsg string
	}{
		{
			name:  "ErrorOnHeaderOutOfBounds",
			index: len(blockHdrs),
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

				tCP := chaincfg.SimNetParams
				opts := &ImportOptions{
					TargetChainParams: tCP,
				}
				bS := opts.createBlockHeaderImportSrc()
				bS.SetURI(bFile.Name())

				err = bS.Open()
				cleanup := func() {
					bS.Close()
					os.Remove(bFile.Name())
				}
				if err != nil {
					return prep{
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

				bV := opts.createBlockHeaderValidator(bS)

				return prep{
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

				tCP := chaincfg.SimNetParams
				opts := &ImportOptions{
					TargetChainParams: tCP,
					ValidationFlags:   blockchain.BFFastAdd,
				}
				bS := opts.createBlockHeaderImportSrc()
				bS.SetURI(bFile.Name())

				err = bS.Open()
				cleanup := func() {
					bS.Close()
					os.Remove(bFile.Name())
				}
				if err != nil {
					return prep{
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

				bV := opts.createBlockHeaderValidator(bS)

				return prep{
					iterator:  bIterator,
					validator: bV,
					cleanup:   cleanup,
				}
			},
			expectErr:    true,
			expectErrMsg: io.EOF.Error(),
		},
		{
			name: "ValidSequentialHeaders",
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

				tCP := chaincfg.SimNetParams
				opts := &ImportOptions{
					TargetChainParams: tCP,
					ValidationFlags:   blockchain.BFFastAdd,
				}
				bS := opts.createBlockHeaderImportSrc()
				bS.SetURI(bFile.Name())
				err = bS.Open()
				cleanup := func() {
					bS.Close()
					os.Remove(bS.GetURI())
				}
				if err != nil {
					return prep{
						cleanup: cleanup,
						err:     err,
					}
				}

				meta, err := bS.GetHeaderMetadata()
				if err != nil {
					return prep{
						cleanup: cleanup,
						err:     err,
					}
				}

				bIt := bS.Iterator(
					0, meta.headersCount-1,
					defaultWriteBatchSizePerRegion,
				)

				bV := opts.createBlockHeaderValidator(bS)

				return prep{
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
			err := prep.validator.Validate(ctx, prep.iterator)
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
	ctx := context.Background()
	type prep struct {
		iterator  HeaderIterator
		validator HeadersValidator
		cleanup   func()
		err       error
	}
	testCases := []struct {
		name         string
		index        int
		tCP          *chaincfg.Params
		prep         func(chaincfg.Params) prep
		expectErr    bool
		expectErrMsg string
	}{
		{
			name: "ErrorOnInvalidSequentialHeaders",
			tCP:  &chaincfg.MainNetParams,
			prep: func(tCP chaincfg.Params) prep {
				fFile, c1, err := setupFileWithHdrs(
					headerfs.RegularFilter, false,
				)

				if err != nil {
					return prep{
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
					fFile.Name(), wire.MainNet, 0,
					headerfs.RegularFilter, 99999,
				)
				if err != nil {
					return prep{
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
					return prep{
						cleanup: c1,
						err:     err,
					}
				}

				opts := &ImportOptions{
					TargetChainParams: tCP,
				}
				fS := opts.createFilterHeaderImportSrc()
				fS.SetURI(fFile.Name())
				err = fS.Open()
				cleanup := func() {
					fS.Close()
					os.Remove(fS.GetURI())
				}
				if err != nil {
					return prep{
						cleanup: cleanup,
						err:     err,
					}
				}

				meta, err := fS.GetHeaderMetadata()
				if err != nil {
					return prep{
						cleanup: cleanup,
						err:     err,
					}
				}

				fIterator := fS.Iterator(
					0, meta.headersCount-1,
					uint32(opts.WriteBatchSizePerRegion),
				)

				fV := opts.createFilterHeaderValidator()

				return prep{
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
			prep: func(tCP chaincfg.Params) prep {
				fFile, c1, err := setupFileWithHdrs(
					headerfs.RegularFilter, true,
				)
				if err != nil {
					return prep{
						cleanup: c1,
						err:     err,
					}
				}

				opts := &ImportOptions{
					TargetChainParams: tCP,
				}
				fS := opts.createFilterHeaderImportSrc()
				fS.SetURI(fFile.Name())
				err = fS.Open()
				cleanup := func() {
					fS.Close()
					os.Remove(fS.GetURI())
				}
				if err != nil {
					return prep{
						cleanup: cleanup,
						err:     err,
					}
				}

				meta, err := fS.GetHeaderMetadata()
				if err != nil {
					return prep{
						cleanup: cleanup,
						err:     err,
					}
				}

				fIterator := fS.Iterator(
					0, meta.headersCount-1,
					uint32(opts.WriteBatchSizePerRegion),
				)

				fV := opts.createFilterHeaderValidator()

				return prep{
					iterator:  fIterator,
					validator: fV,
					cleanup:   cleanup,
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			prep := tc.prep(*tc.tCP)
			t.Cleanup(prep.cleanup)
			require.NoError(t, prep.err)
			err := prep.validator.Validate(ctx, prep.iterator)
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
	type prep struct {
		hImport *headersImport
		err     error
	}
	type verify struct {
		tc                *testing.T
		processingRegions *processingRegions
	}
	testCases := []struct {
		name         string
		prep         func() prep
		verify       func(verify)
		expectErr    bool
		expectErrMsg string
	}{
		// ❌ Error validation test cases.
		{
			name: "ErrorOnGetHeaderMetadataFailure",
			prep: func() prep {
				expectErr := errors.New("failed to get " +
					"header metadata")

				hIS := &mockHeaderImportSource{}
				hIS.On("GetHeaderMetadata").Return(
					nil, expectErr,
				)

				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("ChainTip").Return(
					&wire.BlockHeader{}, uint32(60), nil,
				)

				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("ChainTip").Return(
					&chainhash.Hash{}, uint32(60), nil,
				)

				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				h := &headersImport{
					blockHeadersImportSource: hIS,
					options:                  ops,
				}

				return prep{
					hImport: h,
				}
			},
			verify:       func(verify) {},
			expectErr:    true,
			expectErrMsg: "failed to get header metadata",
		},
		{
			name: "ErrorOnTargetBlockStoreChainTipFailure",
			prep: func() prep {
				expectErr := errors.New("failed to get block " +
					"header store chain tip")

				hM := &headerMetadata{
					importMetadata: &importMetadata{
						startHeight: 50,
					},
					endHeight: 90,
				}

				hIS := &mockHeaderImportSource{}
				hIS.On("GetHeaderMetadata").Return(hM, nil)

				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("ChainTip").Return(
					&wire.BlockHeader{}, uint32(0),
					expectErr,
				)

				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("ChainTip").Return(
					&chainhash.Hash{}, uint32(60), nil,
				)

				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				h := &headersImport{
					blockHeadersImportSource: hIS,
					options:                  ops,
				}

				return prep{
					hImport: h,
				}
			},
			verify:    func(verify) {},
			expectErr: true,
			expectErrMsg: "failed to get block header store " +
				"chain tip",
		},
		{
			name: "ErrorOnGetChainTipForTargetFilterStore",
			prep: func() prep {
				expectErr := errors.New("failed to get " +
					"filter header store chain tip")

				hM := &headerMetadata{
					importMetadata: &importMetadata{
						startHeight: 50,
					},
					endHeight: 90,
				}

				hIS := &mockHeaderImportSource{}
				hIS.On("GetHeaderMetadata").Return(hM, nil)

				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("ChainTip").Return(
					&wire.BlockHeader{}, uint32(60), nil,
				)

				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("ChainTip").Return(
					&chainhash.Hash{}, uint32(0), expectErr,
				)

				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				h := &headersImport{
					blockHeadersImportSource: hIS,
					options:                  ops,
				}

				return prep{
					hImport: h,
				}
			},
			verify:    func(verify) {},
			expectErr: true,
			expectErrMsg: "failed to get filter header store " +
				"chain tip",
		},

		// ⚠️ Divergent cases (B ≠ F), B & F start at same pos.

		/*
			A: [===========]
			B: [=====]
			F: [===============]
		*/
		{
			name: "Divergence_AFFullyOverlapsB",
			prep: func() prep {
				hM := &headerMetadata{
					importMetadata: &importMetadata{
						startHeight: 0,
					},
					endHeight: 70,
				}

				hIS := &mockHeaderImportSource{}
				hIS.On("GetHeaderMetadata").Return(hM, nil)

				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("ChainTip").Return(
					&wire.BlockHeader{}, uint32(40), nil,
				)

				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("ChainTip").Return(
					&chainhash.Hash{}, uint32(90), nil,
				)

				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				h := &headersImport{
					blockHeadersImportSource: hIS,
					options:                  ops,
				}

				return prep{
					hImport: h,
				}
			},
			verify: func(v verify) {
				nHR := v.processingRegions.newHeaders
				require.False(v.tc, nHR.exists)

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
			prep: func() prep {
				hM := &headerMetadata{
					importMetadata: &importMetadata{
						startHeight: 0,
					},
					endHeight: 90,
				}

				hIS := &mockHeaderImportSource{}
				hIS.On("GetHeaderMetadata").Return(hM, nil)

				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("ChainTip").Return(
					&wire.BlockHeader{}, uint32(40), nil,
				)

				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("ChainTip").Return(
					&chainhash.Hash{}, uint32(70), nil,
				)

				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				h := &headersImport{
					blockHeadersImportSource: hIS,
					options:                  ops,
				}

				return prep{
					hImport: h,
				}
			},
			verify: func(v verify) {
				dR := v.processingRegions.divergence
				dRE := headerRegion{
					start:  41,
					end:    70,
					exists: true,
				}
				require.Equal(v.tc, dRE, dR)

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
			prep: func() prep {
				hM := &headerMetadata{
					importMetadata: &importMetadata{
						startHeight: 0,
					},
					endHeight: 70,
				}

				hIS := &mockHeaderImportSource{}
				hIS.On("GetHeaderMetadata").Return(hM, nil)

				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("ChainTip").Return(
					&wire.BlockHeader{}, uint32(90), nil,
				)

				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("ChainTip").Return(
					&chainhash.Hash{}, uint32(40), nil,
				)

				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				h := &headersImport{
					blockHeadersImportSource: hIS,
					options:                  ops,
				}

				return prep{
					hImport: h,
				}
			},
			verify: func(v verify) {
				nHR := v.processingRegions.newHeaders
				require.False(v.tc, nHR.exists)

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
			prep: func() prep {
				hM := &headerMetadata{
					importMetadata: &importMetadata{
						startHeight: 40,
					},
					endHeight: 90,
				}

				hIS := &mockHeaderImportSource{}
				hIS.On("GetHeaderMetadata").Return(hM, nil)

				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("ChainTip").Return(
					&wire.BlockHeader{}, uint32(90), nil,
				)

				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("ChainTip").Return(
					&chainhash.Hash{}, uint32(70), nil,
				)

				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				h := &headersImport{
					blockHeadersImportSource: hIS,
					options:                  ops,
				}

				return prep{
					hImport: h,
				}
			},
			verify: func(v verify) {
				nHR := v.processingRegions.newHeaders
				require.False(v.tc, nHR.exists)

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
			prep: func() prep {
				hM := &headerMetadata{
					importMetadata: &importMetadata{
						startHeight: 30,
					},
					endHeight: 90,
				}

				hIS := &mockHeaderImportSource{}
				hIS.On("GetHeaderMetadata").Return(hM, nil)

				bHS := &headerfs.MockBlockHeaderStore{}
				bHS.On("ChainTip").Return(
					&wire.BlockHeader{}, uint32(40), nil,
				)

				fHS := &headerfs.MockFilterHeaderStore{}
				fHS.On("ChainTip").Return(
					&chainhash.Hash{}, uint32(50), nil,
				)

				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				h := &headersImport{
					blockHeadersImportSource: hIS,
					options:                  ops,
				}

				return prep{
					hImport: h,
				}
			},
			verify: func(v verify) {
				dR := v.processingRegions.divergence
				dRE := headerRegion{
					start:  41,
					end:    50,
					exists: true,
				}
				require.Equal(v.tc, dRE, dR)

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
			verify := verify{
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

// TestHeaderStorage tests the header storage to the target header stores. It
// checks that the headers are written correctly to the target header stores.
func TestHeaderStorage(t *testing.T) {
	t.Parallel()
	type prep struct {
		hImport       *headersImport
		blockHeaders  []headerfs.BlockHeader
		filterHeaders []headerfs.FilterHeader
		cleanup       func()
		err           error
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
			name: "ErrorOnBlockHeadersWrite",
			prep: func() prep {
				b := &headerfs.MockBlockHeaderStore{}
				args := mock.Anything
				b.On("WriteHeaders", args).Return(
					errors.New("I/O write error"),
				)

				f := &headerfs.MockFilterHeaderStore{}

				ops := &ImportOptions{
					TargetBlockHeaderStore:  b,
					TargetFilterHeaderStore: f,
				}

				headersImport := &headersImport{
					options: ops,
				}

				return prep{
					hImport: headersImport,
					cleanup: func() {},
				}
			},
			verify:    func(v verify) {},
			expectErr: true,
			expectErrMsg: fmt.Sprintf("failed to write block "+
				"headers batch 0-%d: I/O write error",
				len(blockHdrs)-1),
		},
		{
			name: "RollbackBlockHdrsAfterFilterHdrsFailure",
			prep: func() prep {
				tempDir := t.TempDir()
				c1 := func() {
					os.RemoveAll(tempDir)
				}

				dbPath := filepath.Join(tempDir, "test.db")
				db, err := walletdb.Create(
					"bdb", dbPath, true, time.Second*10,
				)
				cleanup := func() {
					db.Close()
					c1()
				}
				if err != nil {
					return prep{
						cleanup: cleanup,
						err:     err,
					}
				}

				b, err := headerfs.NewBlockHeaderStore(
					tempDir, db, &chaincfg.SimNetParams,
				)
				if err != nil {
					return prep{
						cleanup: cleanup,
						err:     err,
					}
				}

				f := &headerfs.MockFilterHeaderStore{}
				args := mock.Anything
				f.On("WriteHeaders", args).Return(
					errors.New("I/O write error"),
				)

				// Prep block headers to write to the target
				// headers store. Ignore the genesis block
				// header since it was already written when
				// creating the block header store.
				nBHs := len(blockHdrs)
				blkHdrsToWrite := make(
					[]headerfs.BlockHeader, nBHs-1,
				)
				for i := 1; i < nBHs; i++ {
					blockHdr := blockHdrs[i]
					h, err := constructBlkHdr(
						blockHdr, uint32(i),
					)
					res := prep{
						cleanup: cleanup,
						err:     err,
					}
					if err != nil {
						return res
					}
					bHValue := h.BlockHeader
					blkHdrsToWrite[i-1] = bHValue
				}

				ops := &ImportOptions{
					TargetBlockHeaderStore:  b,
					TargetFilterHeaderStore: f,
				}

				headersImport := &headersImport{
					options: ops,
				}

				return prep{
					hImport:       headersImport,
					blockHeaders:  blkHdrsToWrite,
					filterHeaders: nil,
					cleanup:       cleanup,
				}
			},
			verify: func(v verify) {
				options := v.hImport.options
				tBS := options.TargetBlockHeaderStore
				chainTipB, height, err := tBS.ChainTip()
				require.NoError(v.tc, err)

				// Since we have wrote 4 headers and those
				// rolledback on filter headers write failure,
				// we can expect the chain tip height to be 0.
				require.Equal(v.tc, uint32(0), height)

				// Assert that the known block header at this
				// index matches the retrieved one.
				chainTipBEx, err := constructBlkHdr(
					blockHdrs[0], uint32(0),
				)
				require.NoError(v.tc, err)
				b := chainTipBEx.BlockHeader.BlockHeader
				require.Equal(v.tc, b, chainTipB)
			},
			expectErr: true,
			expectErrMsg: fmt.Sprintf("failed to write filter "+
				"headers batch 0-%d: I/O write error",
				len(blockHdrs)-1),
		},
		{
			name: "ErrorOnBlockHeadersRollback",
			prep: func() prep {
				b := &headerfs.MockBlockHeaderStore{}
				a := mock.Anything
				b.On("WriteHeaders", a).Return(nil)
				b.On("RollbackBlockHeaders", a).Return(
					nil,
					errors.New("I/O blocks rollback error"),
				)

				f := &headerfs.MockFilterHeaderStore{}
				f.On("WriteHeaders", a).Return(
					errors.New("I/O write err"),
				)

				ops := &ImportOptions{
					TargetBlockHeaderStore:  b,
					TargetFilterHeaderStore: f,
				}

				headersImport := &headersImport{
					options: ops,
				}

				return prep{
					hImport: headersImport,
					cleanup: func() {},
				}
			},
			verify:    func(v verify) {},
			expectErr: true,
			expectErrMsg: "failed to rollback 0 headers from " +
				"target block header store after filter " +
				"headers write failure. Block error: I/O " +
				"blocks rollback error",
		},
		{
			name: "NoErrorOnEmptyBlockAndFilterHeaders",
			prep: func() prep {
				b := &headerfs.MockBlockHeaderStore{}
				args := mock.Anything
				b.On("WriteHeaders", args).Return(nil)

				f := &headerfs.MockFilterHeaderStore{}
				f.On("WriteHeaders", args).Return(nil)

				ops := &ImportOptions{
					TargetBlockHeaderStore:  b,
					TargetFilterHeaderStore: f,
				}

				headersImport := &headersImport{
					options: ops,
				}

				return prep{
					hImport:       headersImport,
					blockHeaders:  nil,
					filterHeaders: nil,
					cleanup:       func() {},
				}
			},
			verify: func(v verify) {},
		},
		{
			name: "StoreBothBlockAndFilterHeaders",
			prep: func() prep {
				tempDir := t.TempDir()
				c1 := func() {
					os.RemoveAll(tempDir)
				}

				dbPath := filepath.Join(tempDir, "test.db")
				db, err := walletdb.Create(
					"bdb", dbPath, true, time.Second*10,
				)
				cleanup := func() {
					db.Close()
					c1()
				}
				if err != nil {
					return prep{
						cleanup: cleanup,
						err:     err,
					}
				}

				b, err := headerfs.NewBlockHeaderStore(
					tempDir, db, &chaincfg.SimNetParams,
				)
				if err != nil {
					return prep{
						cleanup: cleanup,
						err:     err,
					}
				}

				f, err := headerfs.NewFilterHeaderStore(
					tempDir, db, headerfs.RegularFilter,
					&chaincfg.SimNetParams, nil,
				)
				if err != nil {
					return prep{
						cleanup: cleanup,
						err:     err,
					}
				}

				// Prep block headers to write to the target
				// headers store. Ignore the genesis block
				// header since it was already written when
				// creating the block header store.
				nBHs := len(blockHdrs)
				blkHdrsToWrite := make(
					[]headerfs.BlockHeader,
					nBHs-1,
				)
				for i := 1; i < nBHs; i++ {
					blockHdr := blockHdrs[i]
					h, err := constructBlkHdr(
						blockHdr, uint32(i),
					)
					res := prep{
						cleanup: cleanup,
						err:     err,
					}
					if err != nil {
						return res
					}
					bHValue := h.BlockHeader
					blkHdrsToWrite[i-1] = bHValue
				}

				// Prep filter headers to write to the target
				// headers store. Ignore the genesis filter
				// header since it was already written when
				// creating the filter header store.
				nFHs := len(filterHdrs)
				filtHdrsToWrite := make(
					[]headerfs.FilterHeader, nFHs-1,
				)
				for i := 1; i < nFHs; i++ {
					filterHdr := filterHdrs[i]
					h, err := constructFilterHdr(
						filterHdr, uint32(i),
					)
					res := prep{
						cleanup: cleanup,
						err:     err,
					}
					if err != nil {
						return res
					}
					fH := h.FilterHeader
					filtHdrsToWrite[i-1] = fH
				}

				lastBH := blkHdrsToWrite[len(blkHdrsToWrite)-1]
				setLastFilterHeaderHash(filtHdrsToWrite, lastBH)

				ops := &ImportOptions{
					TargetBlockHeaderStore:  b,
					TargetFilterHeaderStore: f,
				}

				headersImport := &headersImport{
					options: ops,
				}

				return prep{
					hImport:       headersImport,
					blockHeaders:  blkHdrsToWrite,
					filterHeaders: filtHdrsToWrite,
					cleanup:       cleanup,
				}
			},
			verify: func(v verify) {
				options := v.hImport.options
				tBS := options.TargetBlockHeaderStore
				chainTipB, height, err := tBS.ChainTip()
				require.NoError(v.tc, err)

				nBHs := len(blockHdrs)
				require.Equal(v.tc, uint32(nBHs-1), height)

				// Assert that the known block header at this
				// index matches the retrieved one.
				chainTipBEx, err := constructBlkHdr(
					blockHdrs[nBHs-1], uint32(nBHs-1),
				)
				require.NoError(v.tc, err)
				b := chainTipBEx.BlockHeader.BlockHeader
				require.Equal(v.tc, b, chainTipB)

				tFS := options.TargetFilterHeaderStore
				chainTipF, height, err := tFS.ChainTip()
				require.NoError(v.tc, err)

				nFHs := len(filterHdrs)
				require.Equal(v.tc, uint32(nFHs-1), height)

				// Assert that the known filter header at this
				// index matches the retrieved one.
				chainTipFEx, err := constructFilterHdr(
					filterHdrs[nFHs-1], uint32(nFHs-1),
				)
				require.NoError(v.tc, err)
				f := &chainTipFEx.FilterHash
				require.Equal(v.tc, f, chainTipF)
			},
		},
		{
			name: "StoreBlockHeadersOnly",
			prep: func() prep {
				tempDir := t.TempDir()
				c1 := func() {
					os.RemoveAll(tempDir)
				}

				dbPath := filepath.Join(tempDir, "test.db")
				db, err := walletdb.Create(
					"bdb", dbPath, true, time.Second*10,
				)
				cleanup := func() {
					db.Close()
					c1()
				}
				if err != nil {
					return prep{
						cleanup: cleanup,
						err:     err,
					}
				}

				b, err := headerfs.NewBlockHeaderStore(
					tempDir, db, &chaincfg.SimNetParams,
				)
				if err != nil {
					return prep{
						cleanup: cleanup,
						err:     err,
					}
				}

				f := &headerfs.MockFilterHeaderStore{}
				args := mock.Anything
				f.On("WriteHeaders", args).Return(nil)

				// Prep block headers to write to the target
				// headers store. Ignore the genesis block
				// header since it was already written when
				// creating the block header store.
				nBHs := len(blockHdrs)
				blkHdrsToWrite := make(
					[]headerfs.BlockHeader, nBHs-1,
				)
				for i := 1; i < nBHs; i++ {
					blockHdr := blockHdrs[i]
					h, err := constructBlkHdr(
						blockHdr, uint32(i),
					)
					res := prep{
						cleanup: cleanup,
						err:     err,
					}
					if err != nil {
						return res
					}
					bHValue := h.BlockHeader
					blkHdrsToWrite[i-1] = bHValue
				}

				ops := &ImportOptions{
					TargetBlockHeaderStore:  b,
					TargetFilterHeaderStore: f,
				}

				headersImport := &headersImport{
					options: ops,
				}

				return prep{
					hImport:       headersImport,
					blockHeaders:  blkHdrsToWrite,
					filterHeaders: nil,
					cleanup:       cleanup,
				}
			},
			verify: func(v verify) {
				options := v.hImport.options
				tBS := options.TargetBlockHeaderStore
				chainTipB, height, err := tBS.ChainTip()
				require.NoError(v.tc, err)

				nBHs := len(blockHdrs)
				require.Equal(v.tc, uint32(nBHs-1), height)

				// Assert that the known block header at this
				// index matches the retrieved one.
				chainTipBEx, err := constructBlkHdr(
					blockHdrs[nBHs-1], uint32(nBHs-1),
				)
				require.NoError(v.tc, err)
				b := chainTipBEx.BlockHeader.BlockHeader
				require.Equal(v.tc, b, chainTipB)
			},
		},
		{
			name: "StoreFilterHeadersOnly",
			prep: func() prep {
				tempDir := t.TempDir()
				c1 := func() {
					os.RemoveAll(tempDir)
				}

				dbPath := filepath.Join(tempDir, "test.db")
				db, err := walletdb.Create(
					"bdb", dbPath, true, time.Second*10,
				)
				cleanup := func() {
					db.Close()
					c1()
				}
				if err != nil {
					return prep{
						cleanup: cleanup,
						err:     err,
					}
				}

				f, err := headerfs.NewFilterHeaderStore(
					tempDir, db, headerfs.RegularFilter,
					&chaincfg.SimNetParams, nil,
				)
				if err != nil {
					return prep{
						cleanup: cleanup,
						err:     err,
					}
				}

				b := &headerfs.MockBlockHeaderStore{}
				args := mock.Anything
				b.On("WriteHeaders", args).Return(nil)

				// Prep filter headers to write to the target
				// headers store. Ignore the genesis filter
				// header since it was already written when
				// creating the filter header store.
				nFHs := len(filterHdrs)
				filtHdrsToWrite := make(
					[]headerfs.FilterHeader, nFHs-1,
				)
				for i := 1; i < nFHs; i++ {
					filterHdr := filterHdrs[i]
					h, err := constructFilterHdr(
						filterHdr, uint32(i),
					)
					res := prep{
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
					return prep{
						cleanup: cleanup,
						err:     err,
					}
				}
				bH := lbH.BlockHash()
				filtHdrsToWrite[nFHs-2].HeaderHash = bH

				ops := &ImportOptions{
					TargetFilterHeaderStore: f,
					TargetBlockHeaderStore:  b,
				}

				headersImport := &headersImport{
					options: ops,
				}

				return prep{
					hImport:       headersImport,
					blockHeaders:  nil,
					filterHeaders: filtHdrsToWrite,
					cleanup:       cleanup,
				}
			},
			verify: func(v verify) {
				options := v.hImport.options
				tFS := options.TargetFilterHeaderStore
				_, _, err := tFS.ChainTip()
				expectErrMsg := "target height not found in " +
					"index"
				require.ErrorContains(v.tc, err, expectErrMsg)
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
				0, uint32(len(blockHdrs)-1),
			)
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

// TestHeaderStorageOnNewHeadersRegion tests the header storage on the new
// headers region. It checks that the headers are written correctly to the
// target header stores.
func TestHeaderStorageOnNewHeadersRegion(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	type prep struct {
		hImport *headersImport
		cleanup func()
		err     error
	}
	type verify struct {
		tc            *testing.T
		importOptions *ImportOptions
		importResult  *ImportResult
	}
	testCases := []struct {
		name         string
		region       headerRegion
		importResult *ImportResult
		prep         func() prep
		verify       func(verify)
		expectErr    bool
		expectErrMsg string
	}{
		{
			name: "NoErrorOnNonExistentRegion",
			region: headerRegion{
				start:  1000,
				end:    2000,
				exists: false,
			},
			importResult: &ImportResult{},
			prep: func() prep {
				return prep{
					hImport: &headersImport{},
					cleanup: func() {},
				}
			},
			verify: func(verify) {},
		},
		{
			name: "ErrorOnGetHeaderMetadata",
			region: headerRegion{
				start:  1,
				end:    100,
				exists: true,
			},
			importResult: &ImportResult{},
			prep: func() prep {
				// Mock GetHeaderMetadata.
				bIS := &mockHeaderImportSource{}
				bIS.On("GetHeaderMetadata").Return(
					nil, errors.New("I/O read error"),
				)
				hImport := &headersImport{
					blockHeadersImportSource: bIS,
				}
				return prep{
					hImport: hImport,
					cleanup: func() {},
				}
			},
			verify:       func(verify) {},
			expectErr:    true,
			expectErrMsg: "I/O read error",
		},
		{
			name: "ErrorOnGetBlockHeader",
			region: headerRegion{
				start:  1,
				end:    100,
				exists: true,
			},
			importResult: &ImportResult{},
			prep: func() prep {
				bIS := &mockHeaderImportSource{}
				importMetadata := &importMetadata{}
				bIS.On("GetHeaderMetadata").Return(
					&headerMetadata{
						importMetadata: importMetadata,
					}, nil,
				)

				bIt := &mockHeaderIterator{}

				in := mock.Anything
				bIt.On("ReadBatch", in, in, in).Return(
					nil, errors.New("I/O read error"),
				)
				bIt.On("GetEndIndex").Return(uint32(100))
				bIt.On("GetBatchSize").Return(uint32(100))
				bIS.On("Iterator", in, in, in).Return(bIt)

				fIS := &mockHeaderImportSource{}

				fIt := &mockHeaderIterator{}
				fIt.On("ReadBatch", in, in, in).Return(nil, nil)
				fIt.On("GetEndIndex").Return(uint32(100))
				fIt.On("GetBatchSize").Return(uint32(100))
				fIS.On("Iterator", in, in, in).Return(fIt)

				ops := &ImportOptions{
					WriteBatchSizePerRegion: 100,
				}

				hImport := &headersImport{
					blockHeadersImportSource:  bIS,
					filterHeadersImportSource: fIS,
					options:                   ops,
				}

				return prep{
					hImport: hImport,
					cleanup: func() {},
				}
			},
			verify:    func(verify) {},
			expectErr: true,
			expectErrMsg: "failed to read block headers batch at " +
				"height 1: I/O read error",
		},
		{
			name: "ErrorOnTypeAssertingBlockHeader",
			region: headerRegion{
				start:  1,
				end:    100,
				exists: true,
			},
			importResult: &ImportResult{},
			prep: func() prep {
				bIS := &mockHeaderImportSource{}
				importMetadata := &importMetadata{}
				bIS.On("GetHeaderMetadata").Return(
					&headerMetadata{
						importMetadata: importMetadata,
					}, nil,
				)

				bIt := &mockHeaderIterator{}

				in := mock.Anything
				bIt.On("ReadBatch", in, in, in).Return(
					[]Header{
						newFilterHeader(),
					}, nil,
				)
				bIt.On("GetEndIndex").Return(uint32(100))
				bIt.On("GetBatchSize").Return(uint32(100))
				bIS.On("Iterator", in, in, in).Return(bIt)

				fIS := &mockHeaderImportSource{}

				fIt := &mockHeaderIterator{}
				fIt.On("ReadBatch", in, in, in).Return(nil, nil)
				fIt.On("GetEndIndex").Return(uint32(100))
				fIt.On("GetBatchSize").Return(uint32(100))
				fIS.On("Iterator", in, in, in).Return(fIt)

				ops := &ImportOptions{
					WriteBatchSizePerRegion: 100,
				}

				hImport := &headersImport{
					blockHeadersImportSource:  bIS,
					filterHeadersImportSource: fIS,
					options:                   ops,
				}

				return prep{
					hImport: hImport,
					cleanup: func() {},
				}
			},
			verify:    func(verify) {},
			expectErr: true,
			expectErrMsg: "expected blockHeader type, got " +
				"*chainimport.filterHeader",
		},
		{
			name: "ErrorOnGetFilterHeader",
			region: headerRegion{
				start:  1,
				end:    100,
				exists: true,
			},
			importResult: &ImportResult{},
			prep: func() prep {
				bIS := &mockHeaderImportSource{}
				importMetadata := &importMetadata{}
				bIS.On("GetHeaderMetadata").Return(
					&headerMetadata{
						importMetadata: importMetadata,
					}, nil,
				)

				bIt := &mockHeaderIterator{}

				in := mock.Anything
				bIt.On("ReadBatch", in, in, in).Return(
					[]Header{}, nil,
				)
				bIt.On("GetEndIndex").Return(uint32(100))
				bIt.On("GetBatchSize").Return(uint32(100))
				bIS.On("Iterator", in, in, in).Return(bIt)

				fIS := &mockHeaderImportSource{}

				fIt := &mockHeaderIterator{}

				fIt.On("ReadBatch", in, in, in).Return(
					nil, errors.New("I/O read error"),
				)
				fIt.On("GetEndIndex").Return(uint32(100))
				fIt.On("GetBatchSize").Return(uint32(100))
				fIS.On("Iterator", in, in, in).Return(fIt)

				ops := &ImportOptions{
					WriteBatchSizePerRegion: 100,
				}

				hImport := &headersImport{
					blockHeadersImportSource:  bIS,
					filterHeadersImportSource: fIS,
					options:                   ops,
				}

				return prep{
					hImport: hImport,
					cleanup: func() {},
				}
			},
			verify:    func(verify) {},
			expectErr: true,
			expectErrMsg: "failed to read filter headers batch " +
				"at height 1: I/O read error",
		},
		{
			name: "ErrorOnTypeAssertingFilterHeader",
			region: headerRegion{
				start:  1,
				end:    100,
				exists: true,
			},
			importResult: &ImportResult{},
			prep: func() prep {
				bIS := &mockHeaderImportSource{}
				importMetadata := &importMetadata{}
				bIS.On("GetHeaderMetadata").Return(
					&headerMetadata{
						importMetadata: importMetadata,
					}, nil,
				)

				bIt := &mockHeaderIterator{}

				in := mock.Anything
				bIt.On("ReadBatch", in, in, in).Return(
					[]Header{}, nil,
				)
				bIt.On("GetEndIndex").Return(uint32(100))
				bIt.On("GetBatchSize").Return(uint32(100))
				bIS.On("Iterator", in, in, in).Return(bIt)

				fIS := &mockHeaderImportSource{}

				fIt := &mockHeaderIterator{}
				fIt.On("ReadBatch", in, in, in).Return(
					[]Header{
						newBlockHeader(),
					}, nil,
				)
				fIt.On("GetEndIndex").Return(uint32(100))
				fIt.On("GetBatchSize").Return(uint32(100))
				fIS.On("Iterator", in, in, in).Return(fIt)

				ops := &ImportOptions{
					WriteBatchSizePerRegion: 100,
				}

				hImport := &headersImport{
					blockHeadersImportSource:  bIS,
					filterHeadersImportSource: fIS,
					options:                   ops,
				}

				return prep{
					hImport: hImport,
					cleanup: func() {},
				}
			},
			verify:    func(verify) {},
			expectErr: true,
			expectErrMsg: "expected filterHeader type, got " +
				"*chainimport.blockHeader",
		},
		{
			name: "ErrorOnHeadersLengthMismatch",
			region: headerRegion{
				start:  1,
				end:    100,
				exists: true,
			},
			importResult: &ImportResult{},
			prep: func() prep {
				bIS := &mockHeaderImportSource{}
				importMetadata := &importMetadata{}
				bIS.On("GetHeaderMetadata").Return(
					&headerMetadata{
						importMetadata: importMetadata,
					}, nil,
				)

				bIt := &mockHeaderIterator{}

				in := mock.Anything
				bIt.On("ReadBatch", in, in, in).Return(
					[]Header{}, nil,
				)
				bIt.On("GetEndIndex").Return(uint32(100))
				bIt.On("GetBatchSize").Return(uint32(100))
				bIS.On("Iterator", in, in, in).Return(bIt)

				fIS := &mockHeaderImportSource{}

				fIt := &mockHeaderIterator{}
				fIt.On("ReadBatch", in, in, in).Return(
					[]Header{
						newFilterHeader(),
					}, nil,
				)
				fIt.On("GetEndIndex").Return(uint32(100))
				fIt.On("GetBatchSize").Return(uint32(100))
				fIS.On("Iterator", in, in, in).Return(fIt)

				ops := &ImportOptions{
					WriteBatchSizePerRegion: 100,
				}

				hImport := &headersImport{
					blockHeadersImportSource:  bIS,
					filterHeadersImportSource: fIS,
					options:                   ops,
				}

				return prep{
					hImport: hImport,
					cleanup: func() {},
				}
			},
			verify:    func(verify) {},
			expectErr: true,
			expectErrMsg: "mismatch between block headers (0) " +
				"and filter headers (1)",
		},
		{
			name: "ErrorOnWriteHeadersToTargetStores",
			region: headerRegion{
				start:  1,
				end:    100,
				exists: true,
			},
			importResult: &ImportResult{},
			prep: func() prep {
				bIS := &mockHeaderImportSource{}
				importMetadata := &importMetadata{}
				bIS.On("GetHeaderMetadata").Return(
					&headerMetadata{
						importMetadata: importMetadata,
					}, nil,
				)

				bIt := &mockHeaderIterator{}

				in := mock.Anything
				bIt.On("ReadBatch", in, in, in).Return(
					[]Header{
						newBlockHeader(),
					}, nil,
				)
				bIt.On("GetEndIndex").Return(uint32(100))
				bIt.On("GetBatchSize").Return(uint32(100))
				bIS.On("Iterator", in, in, in).Return(bIt)

				fIS := &mockHeaderImportSource{}

				fIt := &mockHeaderIterator{}

				fIt.On("ReadBatch", in, in, in).Return(
					[]Header{
						newFilterHeader(),
					}, nil,
				)
				fIt.On("GetEndIndex").Return(uint32(100))
				fIt.On("GetBatchSize").Return(uint32(100))
				fIS.On("Iterator", in, in, in).Return(fIt)

				b := &headerfs.MockBlockHeaderStore{}
				in = mock.Anything
				b.On("WriteHeaders", in).Return(
					errors.New("I/O write error"),
				)

				ops := &ImportOptions{
					WriteBatchSizePerRegion: 100,
					TargetBlockHeaderStore:  b,
				}

				hImport := &headersImport{
					blockHeadersImportSource:  bIS,
					filterHeadersImportSource: fIS,
					options:                   ops,
				}

				return prep{
					hImport: hImport,
					cleanup: func() {},
				}
			},
			verify:    func(verify) {},
			expectErr: true,
			expectErrMsg: "failed to write headers to target " +
				"stores",
		},
		{
			name: "ProcessNewHeadersRegionSuccessfully",
			region: headerRegion{
				start:  1,
				end:    4,
				exists: true,
			},
			importResult: &ImportResult{},
			prep: func() prep {
				tempDir := t.TempDir()
				c1 := func() {
					os.RemoveAll(tempDir)
				}

				dbPath := filepath.Join(tempDir, "test.db")
				db, err := walletdb.Create(
					"bdb", dbPath, true, time.Second*10,
				)
				cleanup := func() {
					db.Close()
					c1()
				}
				if err != nil {
					return prep{
						cleanup: cleanup,
						err:     err,
					}
				}

				b, err := headerfs.NewBlockHeaderStore(
					tempDir, db, &chaincfg.SimNetParams,
				)
				if err != nil {
					return prep{
						cleanup: cleanup,
						err:     err,
					}
				}

				f, err := headerfs.NewFilterHeaderStore(
					tempDir, db, headerfs.RegularFilter,
					&chaincfg.SimNetParams, nil,
				)
				if err != nil {
					return prep{
						cleanup: cleanup,
						err:     err,
					}
				}

				bIS := &mockHeaderImportSource{}
				importMetadata := &importMetadata{}
				bIS.On("GetHeaderMetadata").Return(
					&headerMetadata{
						importMetadata: importMetadata,
					}, nil,
				)

				bIt := &mockHeaderIterator{}

				// Prep block headers to write to the target
				// headers store. Ignore the genesis block
				// header since it was already written when
				// creating the block header store.
				nBHs := len(blockHdrs)
				blkHdrsToWrite := make([]Header, nBHs-1)
				for i := 1; i < nBHs; i++ {
					blockHdr := blockHdrs[i]
					h, err := constructBlkHdr(
						blockHdr, uint32(i),
					)
					res := prep{
						cleanup: cleanup,
						err:     err,
					}
					if err != nil {
						return res
					}
					blkHdrsToWrite[i-1] = h
				}

				in := mock.Anything
				bIt.On("ReadBatch", in, in, in).Return(
					blkHdrsToWrite, nil,
				).Once()
				bIt.On("ReadBatch", in, in, in).Return(
					nil, io.EOF,
				)
				bIt.On("GetEndIndex").Return(uint32(4))
				bIt.On("GetBatchSize").Return(uint32(128))
				bIS.On("Iterator", in, in, in).Return(bIt)

				fIS := &mockHeaderImportSource{}

				fIt := &mockHeaderIterator{}

				// Prep filter headers to write to the target
				// headers store. Ignore the genesis filter
				// header since it was already written when
				// creating the filter header store.
				nFHs := len(filterHdrs)
				filtHdrsToWrite := make([]Header, nFHs-1)
				for i := 1; i < nFHs; i++ {
					filterHdr := filterHdrs[i]
					h, err := constructFilterHdr(
						filterHdr, uint32(i),
					)
					res := prep{
						cleanup: cleanup,
						err:     err,
					}
					if err != nil {
						return res
					}
					filtHdrsToWrite[i-1] = h
				}

				fIt.On("ReadBatch", in, in, in).Return(
					filtHdrsToWrite, nil,
				).Once()
				fIt.On("ReadBatch", in, in, in).Return(
					nil, io.EOF,
				)
				fIt.On("GetEndIndex").Return(uint32(4))
				fIt.On("GetBatchSize").Return(uint32(128))
				fIS.On("Iterator", in, in, in).Return(fIt)

				ops := &ImportOptions{
					WriteBatchSizePerRegion: 128,
					TargetBlockHeaderStore:  b,
					TargetFilterHeaderStore: f,
				}

				hImport := &headersImport{
					blockHeadersImportSource:  bIS,
					filterHeadersImportSource: fIS,
					options:                   ops,
				}

				return prep{
					hImport: hImport,
					cleanup: cleanup,
				}
			},
			verify: func(v verify) {
				// Assert that added/processed headers count
				// equal the prep test data ignoring the genesis
				// header.
				require.Equal(
					v.tc, len(blockHdrs)-1,
					v.importResult.AddedCount,
				)
				require.Equal(
					v.tc, len(blockHdrs)-1,
					v.importResult.ProcessedCount,
				)

				// Ensure no headers are skipped in the new
				// headers region.
				require.Equal(
					v.tc, 0, v.importResult.SkippedCount,
				)

				// Verify those added align with the data
				// inserted in the target stores.
				ops := v.importOptions
				tBS := ops.TargetBlockHeaderStore
				chainTipB, height, err := tBS.ChainTip()
				require.NoError(v.tc, err)
				require.Equal(
					v.tc, uint32(len(blockHdrs)-1), height,
				)

				// Assert that the known block header at this
				// index matches the retrieved one.
				chainTipBEx, err := constructBlkHdr(
					blockHdrs[len(blockHdrs)-1],
					uint32(len(blockHdrs)-1),
				)
				require.NoError(v.tc, err)
				b := chainTipBEx.BlockHeader.BlockHeader
				require.Equal(v.tc, b, chainTipB)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			prep := tc.prep()
			t.Cleanup(prep.cleanup)
			require.NoError(t, prep.err)
			err := prep.hImport.processNewHeadersRegion(
				ctx, tc.region, tc.importResult,
			)
			verify := verify{
				tc:            t,
				importOptions: prep.hImport.options,
				importResult:  tc.importResult,
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
