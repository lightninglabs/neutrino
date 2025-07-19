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
// the import sources. Both block and filter headers are required to be
// present for the import to start.
func TestHeadersConjunctionProperty(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
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
			_, err := tc.options.Import(ctx)
			if tc.expectErr {
				require.ErrorContains(t, err, tc.expectErrMsg)
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestImportSkipOperation tests the import skip operation. It checks that
// the import is skipped if the target header store is populated with headers.
func TestImportSkipOperation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	type Prep struct {
		options *ImportOptions
	}
	type Verify struct {
		tc            *testing.T
		importOptions *ImportOptions
		importResult  *ImportResult
	}
	testCases := []struct {
		name         string
		prep         func() Prep
		verify       func(Verify)
		expectErr    bool
		expectErrMsg string
	}{
		{
			name: "TargetFilterStorePopulatedWithHeaders",
			prep: func() Prep {
				// Mock target block header store.
				tBS := &headerfs.MockBlockHeaderStore{}
				tBS.On("ChainTip").Return(
					&wire.BlockHeader{}, uint32(0), nil,
				)

				// Mock target filter header store.
				tFS := &headerfs.MockFilterHeaderStore{}
				tFS.On("ChainTip").Return(
					&chainhash.Hash{}, uint32(3), nil,
				)

				// Configure header import sources.
				bIS := "/path/to/blocks"
				fIS := "/path/to/filters"

				ops := &ImportOptions{
					BlockHeadersSource:      bIS,
					FilterHeadersSource:     fIS,
					TargetBlockHeaderStore:  tBS,
					TargetFilterHeaderStore: tFS,
				}

				return Prep{
					options: ops,
				}
			},
			verify: func(v Verify) {
				// Verify the default batch size is set.
				ops := v.importOptions
				require.Equal(
					v.tc, DefaultWriteBatchSizePerRegion,
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
			importResult, err := prep.options.Import(ctx)
			verify := Verify{
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
	type Prep struct {
		options *ImportOptions
		cleanup func()
		err     error
	}
	type Verify struct {
		tc            *testing.T
		importOptions *ImportOptions
		importResult  *ImportResult
	}
	testCases := []struct {
		name         string
		prep         func() Prep
		verify       func(Verify)
		expectErr    bool
		expectErrMsg string
	}{
		{
			name: "ImportWithoutErrors",
			prep: func() Prep {
				// Prep target header stores.
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
					return Prep{
						cleanup: c2,
						err:     err,
					}
				}

				b, err := headerfs.NewBlockHeaderStore(
					tempDir, db, &chaincfg.SimNetParams,
				)
				if err != nil {
					return Prep{
						cleanup: c2,
						err:     err,
					}
				}

				f, err := headerfs.NewFilterHeaderStore(
					tempDir, db, headerfs.RegularFilter,
					&chaincfg.SimNetParams, nil,
				)
				if err != nil {
					return Prep{
						cleanup: c2,
						err:     err,
					}
				}

				// Create block headers import source file.
				bFile, c3, err := setupFileWithHdrs(
					headerfs.Block, true,
				)
				c4 := func() {
					c3()
					c2()
				}
				if err != nil {
					return Prep{
						cleanup: c4,
						err:     err,
					}
				}
				bPath := bFile.Name()

				// Close block headers import source to be
				// opened during import.
				bFile.Close()

				// Create filter headers import source file.
				fFile, c5, err := setupFileWithHdrs(
					headerfs.RegularFilter, true,
				)
				cleanup := func() {
					c5()
					c4()
				}
				if err != nil {
					return Prep{
						cleanup: cleanup,
						err:     err,
					}
				}
				fPath := fFile.Name()

				// Close filter headers import source to be
				// opened during import.
				fFile.Close()

				// Configure target chain parameters.
				tCP := chaincfg.SimNetParams

				ops := &ImportOptions{
					BlockHeadersSource:      bPath,
					FilterHeadersSource:     fPath,
					TargetBlockHeaderStore:  b,
					TargetFilterHeaderStore: f,
					TargetChainParams:       tCP,
					WriteBatchSizePerRegion: 128,
				}

				return Prep{
					options: ops,
					cleanup: cleanup,
				}
			},
			verify: func(v Verify) {
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

			importResult, err := prep.options.Import(ctx)
			verify := Verify{
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
	type Prep struct {
		hImport *HeadersImport
		cleanup func()
		err     error
	}
	type Verify struct {
		tc           *testing.T
		importResult *ImportResult
	}
	testCases := []struct {
		name         string
		prep         func() Prep
		verify       func(Verify)
		expectErr    bool
		expectErrMsg string
	}{
		{
			name: "ImportWithoutErrors",
			prep: func() Prep {
				// Prep target header stores.
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
					return Prep{
						cleanup: c2,
						err:     err,
					}
				}

				b, err := headerfs.NewBlockHeaderStore(
					tempDir, db, &chaincfg.SimNetParams,
				)
				if err != nil {
					return Prep{
						cleanup: c2,
						err:     err,
					}
				}

				f, err := headerfs.NewFilterHeaderStore(
					tempDir, db, headerfs.RegularFilter,
					&chaincfg.SimNetParams, nil,
				)
				if err != nil {
					return Prep{
						cleanup: c2,
						err:     err,
					}
				}

				// Create block headers import source
				// file.
				bFile, c3, err := setupFileWithHdrs(
					headerfs.Block, true,
				)
				c4 := func() {
					c3()
					c2()
				}
				if err != nil {
					return Prep{
						cleanup: c4,
						err:     err,
					}
				}

				// Create filter headers import source
				// file.
				fFile, c5, err := setupFileWithHdrs(
					headerfs.RegularFilter, true,
				)
				cleanup := func() {
					c5()
					c4()
				}
				if err != nil {
					return Prep{
						cleanup: cleanup,
						err:     err,
					}
				}

				// Mock HTTP client.
				mockHTTPClient := &mockHTTPClient{}
				origin := "http://localhost:8311"

				// Mock HTTP client on block headers
				// resource.
				bRS := origin + "/headers/0"

				// Serve bFile for filter headers.
				blockStatus := http.StatusOK
				blockBody := io.NopCloser(bFile)
				blockRes := &http.Response{
					StatusCode: blockStatus,
					Body:       blockBody,
				}
				mockHTTPClient.On("Get", bRS).Return(
					blockRes, nil,
				)

				// Mock HTTP client on filter headers
				// resource.
				fRS := origin + "/filter-headers/0"

				// Serve fFile for filter headers.
				filterStatus := http.StatusOK
				filterBody := io.NopCloser(fFile)
				filterRes := &http.Response{
					StatusCode: filterStatus,
					Body:       filterBody,
				}
				mockHTTPClient.On("Get", fRS).Return(
					filterRes, nil,
				)

				// Create block headers import
				// source.
				bIS := NewFileHeaderImportSource(
					"", NewBlockHeader,
				)

				// Create filter headers import
				// source.
				fIS := NewFileHeaderImportSource(
					"", NewFilterHeader,
				)

				// Create block headers over http
				// import source utilizing file system
				// interface.
				bS := NewHTTPHeaderImportSource(
					bRS, mockHTTPClient, bIS,
				)

				// Create filter headers over http
				// import source utilizing file system
				// interface.
				fS := NewHTTPHeaderImportSource(
					fRS, mockHTTPClient, fIS,
				)

				// Configure target chain parameters.
				tCP := chaincfg.SimNetParams

				// Configure import options.
				ops := &ImportOptions{
					TargetChainParams:       tCP,
					TargetBlockHeaderStore:  b,
					TargetFilterHeaderStore: f,
					BlockHeadersSource:      bRS,
					FilterHeadersSource:     fRS,
					WriteBatchSizePerRegion: 101,
				}

				// Create validators.
				bV := ops.createBlockHeaderValidator()
				fV := ops.createFilterHeaderValidator()

				headersImport := &HeadersImport{
					options:                   ops,
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
			verify: func(v Verify) {
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
			verify := Verify{
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

// TestOpenFileHeaderImportSources tests the open operation on a file header
// import source.
func TestOpenFileHeaderImportSources(t *testing.T) {
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
				bS := opts.createBlockHeaderImportSrc()
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
				fS := opts.createFilterHeaderImportSrc()
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
				bS := opts.createBlockHeaderImportSrc()
				fS := opts.createFilterHeaderImportSrc()
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
				bS := opts.createBlockHeaderImportSrc()
				fS := opts.createFilterHeaderImportSrc()
				fV := opts.createFilterHeaderValidator()
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
				bS := opts.createBlockHeaderImportSrc()
				fS := opts.createFilterHeaderImportSrc()
				bV := opts.createBlockHeaderValidator()
				fV := opts.createFilterHeaderValidator()

				bS.SetURI(bFile.Name())

				filePath := "/path/to/nonexistent/file"
				bS.SetURI(filePath)

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
				bS := opts.createBlockHeaderImportSrc()
				fS := opts.createFilterHeaderImportSrc()
				bV := opts.createBlockHeaderValidator()
				fV := opts.createFilterHeaderValidator()

				bS.SetURI(bFile.Name())
				fS.SetURI(fFile.Name())

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
					v.tc, expectBlockMetadata, metadata,
				)

				// Verify filter header metadata.
				f := v.hImport.FilterHeadersImportSource
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

// TestOpenHTTPHeaderImportSources tests the open operation on a HTTP header
// import source.
func TestOpenHTTPHeaderImportSources(t *testing.T) {
	t.Parallel()
	type Prep struct {
		hImport *HeadersImport
		cleanup func()
		err     error
	}
	testCases := []struct {
		name         string
		prep         func() Prep
		expectErr    bool
		expectErrMsg string
	}{
		{
			name: "ErrorOnGetBlockHeadersOverHTTP",
			prep: func() Prep {
				// Mock HTTP client.
				mockHTTPClient := &mockHTTPClient{}
				origin := "http://localhost:8234"

				// Mock HTTP client on block headers resource.
				bRS := origin + "/headers/0"
				mockHTTPClient.On("Get", bRS).Return(
					nil, errors.New("failed to "+
						"download block "+
						"headers"),
				).Once()

				// Mock HTTP client on filter headers resource.
				fRS := origin + "/filter-headers/0"

				// Return empty response with no errors.
				status := http.StatusOK
				body := io.NopCloser(bytes.NewBufferString(""))
				res := &http.Response{
					StatusCode: status,
					Body:       body,
				}
				mockHTTPClient.On("Get", fRS).Return(res, nil)

				// Configure import options.
				opts := &ImportOptions{
					BlockHeadersSource:  bRS,
					FilterHeadersSource: fRS,
				}
				bS := NewHTTPHeaderImportSource(
					bRS, mockHTTPClient,
					&mockHeaderImportSource{},
				)
				fS := NewHTTPHeaderImportSource(
					fRS, mockHTTPClient,
					&mockHeaderImportSource{},
				)
				bV := opts.createBlockHeaderValidator()
				fV := opts.createFilterHeaderValidator()

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
				}
			},
			expectErr:    true,
			expectErrMsg: "failed to download block headers",
		},
		{
			name: "ErrorOnGetBlockHeadersOverHTTPNotFound",
			prep: func() Prep {
				// Mock HTTP client.
				mockHTTPClient := &mockHTTPClient{}
				origin := "http://localhost:7334"

				// Mock HTTP client on block headers resource to
				// return 404.
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

				// Mock HTTP client on filter headers resource.
				fRS := origin + "/filter-headers/0"

				// Return empty response with no errors for
				// filter headers.
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

				// Configure import options.
				opts := &ImportOptions{
					BlockHeadersSource:  bRS,
					FilterHeadersSource: fRS,
				}
				bS := NewHTTPHeaderImportSource(
					bRS, mockHTTPClient,
					&mockHeaderImportSource{},
				)
				fS := NewHTTPHeaderImportSource(
					fRS, mockHTTPClient,
					&mockHeaderImportSource{},
				)
				bV := opts.createBlockHeaderValidator()
				fV := opts.createFilterHeaderValidator()

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
				}
			},
			expectErr: true,
			expectErrMsg: fmt.Sprintf("failed to download file: "+
				"status code %d", http.StatusNotFound),
		},
		{
			name: "ErrorOnGetFilterHeadersOverHTTP",
			prep: func() Prep {
				// Mock HTTP client.
				mockHTTPClient := &mockHTTPClient{}
				origin := "http://localhost:8344"

				// Mock HTTP client on block headers resource.
				bRS := origin + "/headers/0"

				// Return empty response with no errors for
				// block headers.
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

				// Mock HTTP client on filter headers resource.
				fRS := origin + "/filter-headers/0"
				mockHTTPClient.On("Get", fRS).Return(
					nil, errors.New("failed to "+
						"download filter "+
						"headers"),
				).Once()

				// Mock open operation on block header import
				// source to focus solely on testing HTTP header
				// import.
				bIS := &mockHeaderImportSource{}
				bIS.On("Open").Return(nil)
				bIS.On("SetURI", mock.Anything).Return()

				// Configure import options.
				opts := &ImportOptions{
					BlockHeadersSource:  bRS,
					FilterHeadersSource: fRS,
				}
				bS := NewHTTPHeaderImportSource(
					bRS, mockHTTPClient, bIS,
				)
				fS := NewHTTPHeaderImportSource(
					fRS, mockHTTPClient,
					&mockHeaderImportSource{},
				)
				cleanup := func() {
					os.Remove(bS.uri)
				}

				bV := opts.createBlockHeaderValidator()
				fV := opts.createFilterHeaderValidator()

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
			expectErr:    true,
			expectErrMsg: "failed to download filter headers",
		},
		{
			name: "ErrorOnGetHTTPFilterHeadersNotFound",
			prep: func() Prep {
				// Mock HTTP client.
				mockHTTPClient := &mockHTTPClient{}
				origin := "http://localhost:8334"

				// Mock HTTP client on block headers resource.
				bRS := origin + "/headers/0"

				// Return empty response with no errors for
				// block headers.
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

				// Mock HTTP client on filter headers resource
				// to return 404.
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

				// Mock open operation on block header import
				// source to focus solely on testing HTTP header
				// import.
				bIS := &mockHeaderImportSource{}
				bIS.On("Open").Return(nil)
				bIS.On("SetURI", mock.Anything).Return()

				// Configure import options.
				opts := &ImportOptions{
					BlockHeadersSource:  bRS,
					FilterHeadersSource: fRS,
				}
				bS := NewHTTPHeaderImportSource(
					bRS, mockHTTPClient, bIS,
				)
				fS := NewHTTPHeaderImportSource(
					fRS, mockHTTPClient,
					&mockHeaderImportSource{},
				)
				cleanup := func() {
					os.Remove(bS.uri)
				}

				bV := opts.createBlockHeaderValidator()
				fV := opts.createFilterHeaderValidator()

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
			expectErr: true,
			expectErrMsg: fmt.Sprintf("failed to download file: "+
				"status code %d", http.StatusNotFound),
		},
		{
			name: "OpenSourcesCorrectly",
			prep: func() Prep {
				// Mock HTTP client.
				mockHTTPClient := &mockHTTPClient{}
				origin := "http://localhost:8323"

				// Mock HTTP client on block header resource.
				bRS := origin + "/headers/0"

				// Return empty response with no errors for
				// block headers.
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

				// Mock HTTP client on filter headers resource.
				fRS := origin + "/filter-headers/0"

				// Return empty response with no errors for
				// filter headers.
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

				// Mock open operation on block header import
				// source to focus solely on testing HTTP header
				// import.
				bIS := &mockHeaderImportSource{}
				bIS.On("Open").Return(nil)
				bIS.On("SetURI", mock.Anything).Return()

				// Mock open operation on filter header import
				// source to focus solely on testing HTTP header
				// import.
				fIS := &mockHeaderImportSource{}
				fIS.On("Open").Return(nil)
				fIS.On("SetURI", mock.Anything).Return()

				// Configure import options.
				opts := &ImportOptions{
					BlockHeadersSource:  bRS,
					FilterHeadersSource: fRS,
				}
				bS := NewHTTPHeaderImportSource(
					bRS, mockHTTPClient,
					bIS,
				)
				fS := NewHTTPHeaderImportSource(
					fRS, mockHTTPClient,
					fIS,
				)
				cleanup := func() {
					os.Remove(fS.uri)
					os.Remove(bS.uri)
				}

				bV := opts.createBlockHeaderValidator()
				fV := opts.createFilterHeaderValidator()

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

// TestHeaderMetadataRetrieval tests the header metadata retrieval from the
// import sources. It checks that the header metadata is retrieved correctly
// from the import sources.
func TestHeaderMetadataRetrieval(t *testing.T) {
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
				bS := opts.createBlockHeaderImportSrc()
				bS.SetURI(bF.Name())

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
				bFS.fileSize = reader.Len()

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
				bFS.fileSize = reader.Len()

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
			expectErrMsg: "failed to get header size: unknown " +
				"header type: 255",
		},
		{
			name: "ErrorOnNegativeStartHeight",
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

				// Create a buffer for binary metadata.
				var metadataBuf bytes.Buffer

				// Write chainType (4 bytes).
				err = binary.Write(
					&metadataBuf, binary.LittleEndian,
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

				// Write startHeight (4 bytes). It is a negative
				// value in two-complement format.
				err = binary.Write(
					&metadataBuf, binary.LittleEndian,
					int32(-1),
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
				bFS.fileSize = reader.Len()

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
			expectErrMsg: "invalid negative value detected for " +
				"StartHeight: -1",
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
				bFS.fileSize = reader.Len()

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
				bFS.fileSize = reader.Len()

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
				// Next call should result in a cache hit since
				// the file is gone.
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
				headerTypeOffset := BitcoinChainTypeSize
				sHeightOffset := headerTypeOffset
				sHeightOffset += HeaderTypeSize
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
				hMS := HeaderMetadataSize
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
				headerTypeOffset := BitcoinChainTypeSize
				sHeightOffset := headerTypeOffset
				sHeightOffset += HeaderTypeSize
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
				hMS := HeaderMetadataSize
				sHeightD := data[sHeightOffset:hMS]
				sHeight := binary.LittleEndian.Uint32(sHeightD)
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
				bS := opts.createBlockHeaderImportSrc()
				bS.SetURI(bFile.Name())

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
				fH, ok := v.header.(*FilterHeader)
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

				current := uint32(i)
				end := uint32(i)
				iter := &ImportSourceHeaderIterator{
					source:  bS,
					current: current,
					end:     end,
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
			expectErrMsg: fmt.Sprintf("failed to read header at "+
				"index %d", len(blockHdrs)),
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

				current := uint32(i + 1)
				end := uint32(i)
				iter := &ImportSourceHeaderIterator{
					source:  bS,
					current: current,
					end:     end,
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

				current := uint32(i)
				end := uint32(len(blockHdrs) - 1)
				iter := &ImportSourceHeaderIterator{
					source:  bS,
					current: current,
					end:     end,
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
					if i <= nBHs-2 {
						require.True(v.tc, more)
					} else {
						require.False(v.tc, more)
					}
					require.NoError(v.tc, err)
					hE, err := constructBlkHdr(
						blockHdrs[i], uint32(i),
					)
					require.NoError(t, err)
					require.Equal(v.tc, hE, h)
				}
				h, more, err := iter.Next()
				require.NoError(v.tc, err)
				require.Nil(v.tc, h)
				require.False(v.tc, more)
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

				current := uint32(i)
				end := uint32(len(filterHdrs) - 1)
				iter := &ImportSourceHeaderIterator{
					source:  fS,
					current: current,
					end:     end,
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
					if i <= nFHs-2 {
						require.True(v.tc, more)
					} else {
						require.False(v.tc, more)
					}
					require.NoError(v.tc, err)
					hE, err := constructFilterHdr(
						filterHdrs[i], uint32(i),
					)
					require.NoError(t, err)
					require.Equal(v.tc, hE, h)
				}
				h, more, err := iter.Next()
				require.NoError(v.tc, err)
				require.Nil(v.tc, h)
				require.False(v.tc, more)
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
					prev:       NewFilterHeader(),
					current:    NewBlockHeader(),
				}
			},
			expectErr: true,
			expectErrMsg: "expected BlockHeader type, got " +
				"\\*chainimport.FilterHeader",
		},
		{
			name: "ErrorOnMismatchCurrentHeaderType",
			tCP:  chaincfg.Params{},
			prep: func(chaincfg.Params) Prep {
				opts := &ImportOptions{}
				bHV := opts.createBlockHeaderValidator()
				return Prep{
					hValidator: bHV,
					prev:       NewBlockHeader(),
					current:    NewFilterHeader(),
				}
			},
			expectErr: true,
			expectErrMsg: "expected BlockHeader type, got " +
				"\\*chainimport.FilterHeader",
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
				bIterator := bS.Iterator(
					uint32(index-1), uint32(index),
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
				bIterator := bS.Iterator(
					uint32(index+1), uint32(index),
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
					0, meta.HeadersCount-1,
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

// TestHeaderValidationOnFilterHeadersPair tests the header validation on a
// filter header pair. It checks that the header is validated correctly.
//
// The goal of these test cases is to explicitly illustrate that we don't do any
// validation on the import filter headers as yet since we don't have access to
// compact filters that generated those headers. In the future if a way is found
// to validate filter headers, some of these test cases are supposed to fail.
func TestHeaderValidationOnFilterHeadersPair(t *testing.T) {
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
				fV := opts.createFilterHeaderValidator()

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
					filterHdrs[nFH-1], uint32(nFH-1),
				)
				if err != nil {
					return Prep{
						err: err,
					}
				}

				return Prep{
					validator: fV,
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
				fV := opts.createFilterHeaderValidator()

				// Construct origin filter header.
				originH, err := constructFilterHdr(
					filterHdrs[0], uint32(0),
				)
				if err != nil {
					return Prep{
						validator: fV,
						err:       err,
					}
				}

				// Construct previous filter header.
				prevH, err := constructFilterHdr(
					filterHdrs[1], uint32(1),
				)
				if err != nil {
					return Prep{
						err: err,
					}
				}

				// Construct current filter header.
				currentH, err := constructFilterHdr(
					filterHdrs[2], uint32(2),
				)
				if err != nil {
					return Prep{
						err: err,
					}
				}

				currentH.FilterHash = originH.FilterHash

				return Prep{
					validator: fV,
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
				fV := opts.createFilterHeaderValidator()

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
					filterHdrs[1], uint32(1),
				)
				if err != nil {
					return Prep{
						err: err,
					}
				}

				return Prep{
					validator: fV,
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
				require.ErrorContains(t, err, tc.expectErrMsg)
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestHeaderValidationOnSequentialFilterHeaders tests the header validation
// on sequential filter headers. It checks that the header is validated
// correctly.
//
// The goal of these test cases is to explicitly illustrate that we don't do any
// validation on the import filter headers as yet since we don't have access to
// compact filters that generated those headers. In the future if a way is found
// to validate filter headers, some of these test cases are supposed to fail.
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
				fIterator := fS.Iterator(0, meta.HeadersCount-1)

				// Create filter validator.
				fV := opts.createFilterHeaderValidator()

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
				fIterator := fS.Iterator(0, meta.HeadersCount-1)

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
			err := prep.validator.Validate(
				prep.iterator, chaincfg.SimNetParams,
			)
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
				expectErr := errors.New("failed to get block " +
					"header store chain tip")

				// Prep header metadata.
				hM := &HeaderMetadata{
					StartHeight: 50,
					EndHeight:   90,
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
				hM := &HeaderMetadata{
					StartHeight: 50,
					EndHeight:   90,
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
				hM := &HeaderMetadata{
					StartHeight: 50,
					EndHeight:   90,
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

				h := &HeadersImport{
					BlockHeadersImportSource: hIS,
					options:                  ops,
				}

				return Prep{
					hImport: h,
				}
			},
			verify: func(v Verify) {
				// Assert that the divergence region doesn't
				// exist.
				dR := v.processingRegions.Divergence
				require.False(v.tc, dR.Exists)

				// Assert that the overlap region was properly
				// detected.
				oR := v.processingRegions.Overlap
				oRE := HeaderRegion{
					Start:  50,
					End:    60,
					Exists: true,
				}
				require.Equal(v.tc, oRE, oR)

				// Assert that the new headers region was
				// properly detected.
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

				h := &HeadersImport{
					BlockHeadersImportSource: hIS,
					options:                  ops,
				}

				return Prep{
					hImport: h,
				}
			},
			verify: func(v Verify) {
				// Assert that the divergence region doesn't
				// exist.
				dR := v.processingRegions.Divergence
				require.False(v.tc, dR.Exists)

				// Assert that the new headers region doesn't
				// exist.
				nHR := v.processingRegions.NewHeaders
				require.False(v.tc, nHR.Exists)

				// Assert that the overlap region is properly
				// detected.
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

				h := &HeadersImport{
					BlockHeadersImportSource: hIS,
					options:                  ops,
				}

				return Prep{
					hImport: h,
				}
			},
			verify: func(v Verify) {
				// Assert that the divergence region doesn't
				// exist.
				dR := v.processingRegions.Divergence
				require.False(v.tc, dR.Exists)

				// Assert that the overlap region was properly
				// detected.
				oR := v.processingRegions.Overlap
				require.False(v.tc, oR.Exists)

				// Assert that the new headers region was
				// properly detected.
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

				// Assert that the overlap region was properly
				// detected.
				oR := v.processingRegions.Overlap
				oRE := HeaderRegion{
					Start:  0,
					End:    40,
					Exists: true,
				}
				require.Equal(v.tc, oRE, oR)

				// Assert that the divergence region was
				// properly detected.
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

				h := &HeadersImport{
					BlockHeadersImportSource: hIS,
					options:                  ops,
				}

				return Prep{
					hImport: h,
				}
			},
			verify: func(v Verify) {
				// Assert that the overlap region was properly
				// detected.
				oR := v.processingRegions.Overlap
				oRE := HeaderRegion{
					Start:  0,
					End:    40,
					Exists: true,
				}
				require.Equal(v.tc, oRE, oR)

				// Assert that the divergence region was
				// properly detected
				dR := v.processingRegions.Divergence
				dRE := HeaderRegion{
					Start:  41,
					End:    70,
					Exists: true,
				}
				require.Equal(v.tc, dRE, dR)

				// Assert that the new headers region was
				// properly detected.
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

				h := &HeadersImport{
					BlockHeadersImportSource: hIS,
					options:                  ops,
				}

				return Prep{
					hImport: h,
				}
			},
			verify: func(v Verify) {
				// Assert that the new headers region doesn't
				// exist.
				nHR := v.processingRegions.NewHeaders
				require.False(v.tc, nHR.Exists)

				// Assert that the overlap region was properly
				// detected.
				oR := v.processingRegions.Overlap
				oRE := HeaderRegion{
					Start:  0,
					End:    40,
					Exists: true,
				}
				require.Equal(v.tc, oRE, oR)

				// Assert that the divergence region was
				// properly detected
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

				h := &HeadersImport{
					BlockHeadersImportSource: hIS,
					options:                  ops,
				}

				return Prep{
					hImport: h,
				}
			},
			verify: func(v Verify) {
				// Assert that the new headers region doesn't
				// exist.
				nHR := v.processingRegions.NewHeaders
				require.False(v.tc, nHR.Exists)

				// Assert that the overlap region was properly
				// detected.
				oR := v.processingRegions.Overlap
				oRE := HeaderRegion{
					Start:  40,
					End:    70,
					Exists: true,
				}
				require.Equal(v.tc, oRE, oR)

				// Assert that the divergence region was
				// properly detected
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

				h := &HeadersImport{
					BlockHeadersImportSource: hIS,
					options:                  ops,
				}

				return Prep{
					hImport: h,
				}
			},
			verify: func(v Verify) {
				// Assert that the overlap region was properly
				// detected.
				oR := v.processingRegions.Overlap
				oRE := HeaderRegion{
					Start:  30,
					End:    40,
					Exists: true,
				}
				require.Equal(v.tc, oRE, oR)

				// Assert that the divergence region was
				// properly detected.
				dR := v.processingRegions.Divergence
				dRE := HeaderRegion{
					Start:  41,
					End:    50,
					Exists: true,
				}
				require.Equal(v.tc, dRE, dR)

				// Assert that the new headers region was
				// properly detected.
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
				require.ErrorContains(t, err, tc.expectErrMsg)
				tc.verify(verify)
				return
			}
			require.NoError(t, err)
			tc.verify(verify)
		})
	}
}

// TestHeaderStorage tests the header storage to the target header stores.
// It checks that the headers are written correctly to the target header stores.
func TestHeaderStorage(t *testing.T) {
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
				b := &headerfs.MockBlockHeaderStore{}
				args := mock.Anything
				b.On("WriteHeaders", args).Return(
					errors.New("I/O write error"),
				)

				f := &headerfs.MockFilterHeaderStore{}

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
			verify:    func(v Verify) {},
			expectErr: true,
			expectErrMsg: fmt.Sprintf("failed to write block "+
				"headers batch 0-%d: I/O write error",
				len(blockHdrs)-1),
		},
		{
			name: "RollbackBlockHdrsAfterFilterHdrsFailure",
			prep: func() Prep {
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
					return Prep{
						cleanup: cleanup,
						err:     err,
					}
				}

				b, err := headerfs.NewBlockHeaderStore(
					tempDir, db, &chaincfg.SimNetParams,
				)
				if err != nil {
					return Prep{
						cleanup: cleanup,
						err:     err,
					}
				}

				// Setup mock filter header store.
				f := &headerfs.MockFilterHeaderStore{}
				args := mock.Anything
				f.On("WriteHeaders", args).Return(
					errors.New("I/O write error"),
				)

				// Prep block headers to write to the target
				// headers store. Ignore the genesis block
				// header since NewBlockHeaderStore already
				// wrote it.
				nBHs := len(blockHdrs)
				blkHdrsToWrite := make(
					[]headerfs.BlockHeader, nBHs-1,
				)
				for i := 1; i < nBHs; i++ {
					blockHdr := blockHdrs[i]
					h, err := constructBlkHdr(
						blockHdr, uint32(i),
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
				// Verify chain tip of the target block header
				// store.
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
			prep: func() Prep {
				// Setup mock block header store.
				b := &headerfs.MockBlockHeaderStore{}
				a := mock.Anything
				b.On("WriteHeaders", a).Return(nil)
				b.On("RollbackBlockHeaders", a).Return(
					nil,
					errors.New("I/O blocks rollback error"),
				)

				// Setup mock filter header store.
				f := &headerfs.MockFilterHeaderStore{}
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
			verify:    func(v Verify) {},
			expectErr: true,
			expectErrMsg: "failed to rollback 0 headers from " +
				"target block header store after filter " +
				"headers write failure. Block error: I/O " +
				"blocks rollback error",
		},
		{
			name: "NoErrorOnEmptyBlockAndFilterHeaders",
			prep: func() Prep {
				// Setup mock block header store.
				b := &headerfs.MockBlockHeaderStore{}
				args := mock.Anything
				b.On("WriteHeaders", args).Return(nil)

				// Setup mock filter header store.
				f := &headerfs.MockFilterHeaderStore{}
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
					return Prep{
						cleanup: cleanup,
						err:     err,
					}
				}

				b, err := headerfs.NewBlockHeaderStore(
					tempDir, db, &chaincfg.SimNetParams,
				)
				if err != nil {
					return Prep{
						cleanup: cleanup,
						err:     err,
					}
				}

				f, err := headerfs.NewFilterHeaderStore(
					tempDir, db, headerfs.RegularFilter,
					&chaincfg.SimNetParams, nil,
				)
				if err != nil {
					return Prep{
						cleanup: cleanup,
						err:     err,
					}
				}

				// Prep block headers to write to the target
				// headers store. Ignore the genesis block
				// header since NewBlockHeaderStore already
				// wrote it.
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

				// Prep filter headers to write to the target
				// headers store. Ignore the genesis filter
				// header since NewFilterHeaderStore already
				// wrote it.
				nFHs := len(filterHdrs)
				filtHdrsToWrite := make(
					[]headerfs.FilterHeader, nFHs-1,
				)
				for i := 1; i < nFHs; i++ {
					filterHdr := filterHdrs[i]
					h, err := constructFilterHdr(
						filterHdr, uint32(i),
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
				// Verify chain tip of the target block header
				// store.
				tBS := options.TargetBlockHeaderStore
				chainTipB, height, err := tBS.ChainTip()
				require.NoError(v.tc, err)

				// Assert on the height.
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

				// Verify chain tip of the target filter header
				// store.
				tFS := options.TargetFilterHeaderStore
				chainTipF, height, err := tFS.ChainTip()
				require.NoError(v.tc, err)

				// Assert on the height.
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
			prep: func() Prep {
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
					return Prep{
						cleanup: cleanup,
						err:     err,
					}
				}

				b, err := headerfs.NewBlockHeaderStore(
					tempDir, db, &chaincfg.SimNetParams,
				)
				if err != nil {
					return Prep{
						cleanup: cleanup,
						err:     err,
					}
				}

				// Setup mock filter header store.
				f := &headerfs.MockFilterHeaderStore{}
				args := mock.Anything
				f.On("WriteHeaders", args).Return(nil)

				// Prep block headers to write to the target
				// headers store. Ignore the genesis block
				// header since NewBlockHeaderStore already
				// wrote it.
				nBHs := len(blockHdrs)
				blkHdrsToWrite := make(
					[]headerfs.BlockHeader, nBHs-1,
				)
				for i := 1; i < nBHs; i++ {
					blockHdr := blockHdrs[i]
					h, err := constructBlkHdr(
						blockHdr, uint32(i),
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
				// Verify chain tip of the target block header
				// store.
				options := v.hImport.options
				tBS := options.TargetBlockHeaderStore
				chainTipB, height, err := tBS.ChainTip()
				require.NoError(v.tc, err)

				// Assert on the height.
				nBHs := len(blockHdrs)
				require.Equal(v.tc, uint32(nBHs-1), height)

				// Assert that the known block header at
				// this index matches the retrieved one.
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
			prep: func() Prep {
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
					return Prep{
						cleanup: cleanup,
						err:     err,
					}
				}

				f, err := headerfs.NewFilterHeaderStore(
					tempDir, db, headerfs.RegularFilter,
					&chaincfg.SimNetParams, nil,
				)
				if err != nil {
					return Prep{
						cleanup: cleanup,
						err:     err,
					}
				}

				// Setup mock block header store.
				b := &headerfs.MockBlockHeaderStore{}
				args := mock.Anything
				b.On("WriteHeaders", args).Return(nil)

				// Prep filter headers to write to the target
				// headers store. Ignore the genesis filter
				// header since NewFilterHeaderStore already
				// wrote it.
				nFHs := len(filterHdrs)
				filtHdrsToWrite := make(
					[]headerfs.FilterHeader, nFHs-1,
				)
				for i := 1; i < nFHs; i++ {
					filterHdr := filterHdrs[i]
					h, err := constructFilterHdr(
						filterHdr, uint32(i),
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

// TestHeaderStorageOnNewHeadersRegion tests the header storage on the new
// headers region. It checks that the headers are written correctly to the
// target header stores.
func TestHeaderStorageOnNewHeadersRegion(t *testing.T) {
	t.Parallel()
	type Prep struct {
		hImport *HeadersImport
		cleanup func()
		err     error
	}
	type Verify struct {
		tc            *testing.T
		importOptions *ImportOptions
		importResult  *ImportResult
	}
	testCases := []struct {
		name         string
		region       HeaderRegion
		importResult *ImportResult
		prep         func() Prep
		verify       func(Verify)
		expectErr    bool
		expectErrMsg string
	}{
		{
			name: "NoErrorOnNonExistentRegion",
			region: HeaderRegion{
				Start:  1000,
				End:    2000,
				Exists: false,
			},
			importResult: &ImportResult{},
			prep: func() Prep {
				return Prep{
					hImport: &HeadersImport{},
					cleanup: func() {},
				}
			},
			verify: func(Verify) {},
		},
		{
			name: "ErrorOnGetHeaderMetadata",
			region: HeaderRegion{
				Start:  1,
				End:    100,
				Exists: true,
			},
			importResult: &ImportResult{},
			prep: func() Prep {
				// Mock GetHeaderMetadata.
				bIS := &mockHeaderImportSource{}
				bIS.On("GetHeaderMetadata").Return(
					nil, errors.New("I/O read error"),
				)
				hImport := &HeadersImport{
					BlockHeadersImportSource: bIS,
				}
				return Prep{
					hImport: hImport,
					cleanup: func() {},
				}
			},
			verify:       func(Verify) {},
			expectErr:    true,
			expectErrMsg: "I/O read error",
		},
		{
			name: "ErrorOnGetBlockHeader",
			region: HeaderRegion{
				Start:  1,
				End:    100,
				Exists: true,
			},
			importResult: &ImportResult{},
			prep: func() Prep {
				// Mock GetHeaderMetadata.
				bIS := &mockHeaderImportSource{}
				bIS.On("GetHeaderMetadata").Return(
					&HeaderMetadata{}, nil,
				)

				// Mock Block iterator.
				bIt := &mockHeaderIterator{}
				bIt.On("Next").Return(
					nil, false,
					errors.New("I/O read error"),
				)
				bIt.On("Close").Return(nil)

				// Mock block header iterator.
				in := mock.Anything
				bIS.On("Iterator", in, in).Return(bIt)

				// Configure import options.
				ops := &ImportOptions{
					WriteBatchSizePerRegion: 100,
				}

				hImport := &HeadersImport{
					BlockHeadersImportSource: bIS,
					options:                  ops,
				}

				return Prep{
					hImport: hImport,
					cleanup: func() {},
				}
			},
			verify:    func(Verify) {},
			expectErr: true,
			expectErrMsg: "failed to read block header at height " +
				"1: I/O read error",
		},
		{
			name: "ErrorOnTypeAssertingBlockHeader",
			region: HeaderRegion{
				Start:  1,
				End:    100,
				Exists: true,
			},
			importResult: &ImportResult{},
			prep: func() Prep {
				// Mock GetHeaderMetadata.
				bIS := &mockHeaderImportSource{}
				bIS.On("GetHeaderMetadata").Return(
					&HeaderMetadata{}, nil,
				)

				// Mock Block iterator on returning
				// filter header to trigger type assert
				// failure.
				bIt := &mockHeaderIterator{}
				bIt.On("Next").Return(
					NewFilterHeader(), true, nil,
				)
				bIt.On("Close").Return(nil)

				// Mock header import iterator.
				in := mock.Anything
				bIS.On("Iterator", in, in).Return(bIt)

				// Configure import options.
				ops := &ImportOptions{
					WriteBatchSizePerRegion: 100,
				}

				hImport := &HeadersImport{
					BlockHeadersImportSource: bIS,
					options:                  ops,
				}

				return Prep{
					hImport: hImport,
					cleanup: func() {},
				}
			},
			verify:    func(Verify) {},
			expectErr: true,
			expectErrMsg: "expected BlockHeader type, got " +
				"*chainimport.FilterHeader",
		},
		{
			name: "ErrorOnGetFilterHeader",
			region: HeaderRegion{
				Start:  1,
				End:    100,
				Exists: true,
			},
			importResult: &ImportResult{},
			prep: func() Prep {
				// Mock GetHeaderMetadata on block
				// import source.
				bIS := &mockHeaderImportSource{}
				bIS.On("GetHeaderMetadata").Return(
					&HeaderMetadata{}, nil,
				)

				// Mock Block iterator.
				bIt := &mockHeaderIterator{}
				bIt.On("Next").Return(nil, false, nil)
				bIt.On("Close").Return(nil)

				// Mock block header iteartor.
				in := mock.Anything
				bIS.On("Iterator", in, in).Return(bIt)

				// Mock filter header import source.
				fIS := &mockHeaderImportSource{}

				// Mock filter iterator.
				fIt := &mockHeaderIterator{}
				fIt.On("Next").Return(
					nil, true, errors.New("I/O read error"),
				)
				fIt.On("Close").Return(nil)

				// Mock filter header iteartor.
				in = mock.Anything
				fIS.On("Iterator", in, in).Return(fIt)

				// Configure import options.
				ops := &ImportOptions{
					WriteBatchSizePerRegion: 100,
				}

				hImport := &HeadersImport{
					BlockHeadersImportSource:  bIS,
					FilterHeadersImportSource: fIS,
					options:                   ops,
				}

				return Prep{
					hImport: hImport,
					cleanup: func() {},
				}
			},
			verify:    func(Verify) {},
			expectErr: true,
			expectErrMsg: "failed to read filter header at " +
				"height 1: I/O read error",
		},
		{
			name: "ErrorOnTypeAssertingFilterHeader",
			region: HeaderRegion{
				Start:  1,
				End:    100,
				Exists: true,
			},
			importResult: &ImportResult{},
			prep: func() Prep {
				// Mock GetHeaderMetadata on block import
				// source.
				bIS := &mockHeaderImportSource{}
				bIS.On("GetHeaderMetadata").Return(
					&HeaderMetadata{}, nil,
				)

				// Mock Block iterator.
				bIt := &mockHeaderIterator{}
				bIt.On("Next").Return(nil, false, nil)
				bIt.On("Close").Return(nil)

				// Mock block header iteartor.
				in := mock.Anything
				bIS.On("Iterator", in, in).Return(bIt)

				// Mock filter header import source.
				fIS := &mockHeaderImportSource{}

				// Mock filter iterator.
				fIt := &mockHeaderIterator{}
				fIt.On("Next").Return(
					NewBlockHeader(), true, nil,
				)
				fIt.On("Close").Return(nil)

				// Mock filter header iteartor.
				in = mock.Anything
				fIS.On("Iterator", in, in).Return(fIt)

				// Configure import options.
				ops := &ImportOptions{
					WriteBatchSizePerRegion: 100,
				}

				hImport := &HeadersImport{
					BlockHeadersImportSource:  bIS,
					FilterHeadersImportSource: fIS,
					options:                   ops,
				}

				return Prep{
					hImport: hImport,
					cleanup: func() {},
				}
			},
			verify:    func(Verify) {},
			expectErr: true,
			expectErrMsg: "expected FilterHeader type, got " +
				"*chainimport.BlockHeader",
		},
		{
			name: "ErrorOnHeadersLengthMismatch",
			region: HeaderRegion{
				Start:  1,
				End:    100,
				Exists: true,
			},
			importResult: &ImportResult{},
			prep: func() Prep {
				// Mock GetHeaderMetadata on block import
				// source.
				bIS := &mockHeaderImportSource{}
				bIS.On("GetHeaderMetadata").Return(
					&HeaderMetadata{}, nil,
				)

				// Mock Block iterator.
				bIt := &mockHeaderIterator{}
				bIt.On("Next").Return(
					NewBlockHeader(), false, nil,
				)
				bIt.On("Close").Return(nil)

				// Mock block header iteartor.
				in := mock.Anything
				bIS.On("Iterator", in, in).Return(bIt)

				// Mock filter header import source.
				fIS := &mockHeaderImportSource{}

				// Mock filter iterator.
				fIt := &mockHeaderIterator{}
				fIt.On("Next").Return(nil, false, nil)
				fIt.On("Close").Return(nil)

				// Mock filter header iteartor.
				in = mock.Anything
				fIS.On("Iterator", in, in).Return(fIt)

				// Configure import options.
				ops := &ImportOptions{
					WriteBatchSizePerRegion: 100,
				}

				hImport := &HeadersImport{
					BlockHeadersImportSource:  bIS,
					FilterHeadersImportSource: fIS,
					options:                   ops,
				}

				return Prep{
					hImport: hImport,
					cleanup: func() {},
				}
			},
			verify:    func(Verify) {},
			expectErr: true,
			expectErrMsg: "mismatch between block headers (1) " +
				"and filter headers (0) for batch 1-100",
		},
		{
			name: "ErrorOnNoHeadersRead",
			region: HeaderRegion{
				Start:  1,
				End:    100,
				Exists: true,
			},
			importResult: &ImportResult{},
			prep: func() Prep {
				// Mock GetHeaderMetadata on block import
				// source.
				bIS := &mockHeaderImportSource{}
				bIS.On("GetHeaderMetadata").Return(
					&HeaderMetadata{}, nil,
				)

				// Mock Block iterator.
				bIt := &mockHeaderIterator{}
				bIt.On("Next").Return(nil, false, nil)
				bIt.On("Close").Return(nil)

				// Mock block header iteartor.
				in := mock.Anything
				bIS.On("Iterator", in, in).Return(bIt)

				// Mock filter header import source.
				fIS := &mockHeaderImportSource{}

				// Mock filter iterator.
				fIt := &mockHeaderIterator{}
				fIt.On("Next").Return(nil, false, nil)
				fIt.On("Close").Return(nil)

				// Mock filter header iteartor.
				in = mock.Anything
				fIS.On("Iterator", in, in).Return(fIt)

				// Configure import options.
				ops := &ImportOptions{
					WriteBatchSizePerRegion: 100,
				}

				hImport := &HeadersImport{
					BlockHeadersImportSource:  bIS,
					FilterHeadersImportSource: fIS,
					options:                   ops,
				}

				return Prep{
					hImport: hImport,
					cleanup: func() {},
				}
			},
			verify:       func(Verify) {},
			expectErr:    true,
			expectErrMsg: "no headers read for batch 1-100",
		},
		{
			name: "ErrorOnWriteHeadersToTargetStores",
			region: HeaderRegion{
				Start:  1,
				End:    100,
				Exists: true,
			},
			importResult: &ImportResult{},
			prep: func() Prep {
				// Mock GetHeaderMetadata on block import
				// source.
				bIS := &mockHeaderImportSource{}
				bIS.On("GetHeaderMetadata").Return(
					&HeaderMetadata{}, nil,
				)

				// Mock Block iterator.
				bIt := &mockHeaderIterator{}
				bIt.On("Next").Return(
					NewBlockHeader(), false, nil,
				)
				bIt.On("Close").Return(nil)

				// Mock block header iteartor.
				in := mock.Anything
				bIS.On("Iterator", in, in).Return(bIt)

				// Mock filter header import source.
				fIS := &mockHeaderImportSource{}

				// Mock filter iterator.
				fIt := &mockHeaderIterator{}
				fIt.On("Next").Return(
					NewFilterHeader(), false, nil,
				)
				fIt.On("Close").Return(nil)

				// Mock filter header iteartor.
				in = mock.Anything
				fIS.On("Iterator", in, in).Return(fIt)

				// Setup mock block header store to check if
				// write error properly propagated.
				b := &headerfs.MockBlockHeaderStore{}
				in = mock.Anything
				b.On("WriteHeaders", in).Return(
					errors.New("I/O write error"),
				)

				// Configure import options.
				ops := &ImportOptions{
					WriteBatchSizePerRegion: 100,
					TargetBlockHeaderStore:  b,
				}

				hImport := &HeadersImport{
					BlockHeadersImportSource:  bIS,
					FilterHeadersImportSource: fIS,
					options:                   ops,
				}

				return Prep{
					hImport: hImport,
					cleanup: func() {},
				}
			},
			verify:    func(Verify) {},
			expectErr: true,
			expectErrMsg: "failed to write headers to target " +
				"stores",
		},
		{
			name: "ProcessNewHeadersOfMultipleBatches",
			region: HeaderRegion{
				Start:  1,
				End:    4,
				Exists: true,
			},
			importResult: &ImportResult{},
			prep: func() Prep {
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
					return Prep{
						cleanup: cleanup,
						err:     err,
					}
				}

				// Setup target block header store.
				b, err := headerfs.NewBlockHeaderStore(
					tempDir, db, &chaincfg.SimNetParams,
				)
				if err != nil {
					return Prep{
						cleanup: cleanup,
						err:     err,
					}
				}

				// Setup target filter header store.
				f, err := headerfs.NewFilterHeaderStore(
					tempDir, db, headerfs.RegularFilter,
					&chaincfg.SimNetParams, nil,
				)
				if err != nil {
					return Prep{
						cleanup: cleanup,
						err:     err,
					}
				}

				// Configure import options. With write batch
				// size per region equal 1 it would result in 4
				// batches of writing to target stores.
				ops := &ImportOptions{
					WriteBatchSizePerRegion: 1,
					TargetBlockHeaderStore:  b,
					TargetFilterHeaderStore: f,
				}

				// Mock GetHeaderMetadata on block import
				// source.
				bIS := &mockHeaderImportSource{}
				bIS.On("GetHeaderMetadata").Return(
					&HeaderMetadata{}, nil,
				)

				// Mock Block iterator.
				bIt := &mockHeaderIterator{}

				// Prep block headers to write to the target
				// headers store. Ignore the genesis block
				// header since NewBlockHeaderStore already
				// wrote it.
				nBHs := len(blockHdrs)
				blkHdrsToWrite := make(
					[]headerfs.BlockHeader, nBHs-1,
				)
				for i := 1; i < nBHs; i++ {
					blockHdr := blockHdrs[i]
					h, err := constructBlkHdr(
						blockHdr, uint32(i),
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
					// For all headers except the
					// last one.
					if i < nBHs-1 {
						bIt.On("Next").Return(
							h, true, nil,
						).Once()
					} else {
						// For the last header, indicate
						// end of iteration.
						bIt.On("Next").Return(
							h, false, nil,
						)
					}
				}
				bIt.On("Close").Return(nil)

				// Mock block header iteartor.
				in := mock.Anything
				bIS.On("Iterator", in, in).Return(bIt)

				// Mock filter header import source.
				fIS := &mockHeaderImportSource{}

				// Mock filter iterator.
				fIt := &mockHeaderIterator{}

				// Prep filter headers to write to the target
				// headers store. Ignore the genesis filter
				// header since NewFilterHeaderStore already
				// wrote it.
				nFHs := len(filterHdrs)
				filtHdrsToWrite := make(
					[]headerfs.FilterHeader, nFHs-1,
				)
				for i := 1; i < nFHs; i++ {
					filterHdr := filterHdrs[i]
					h, err := constructFilterHdr(
						filterHdr, uint32(i),
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
					// For all headers except the last one.
					if i < nFHs-1 {
						fIt.On("Next").Return(
							h, true, nil,
						).Once()
					} else {
						// For the last header, indicate
						// end of iteration.
						fIt.On("Next").Return(
							h, false, nil,
						)
					}
				}
				fIt.On("Close").Return(nil)

				// Mock filter header iteartor.
				in = mock.Anything
				fIS.On("Iterator", in, in).Return(fIt)

				hImport := &HeadersImport{
					BlockHeadersImportSource:  bIS,
					FilterHeadersImportSource: fIS,
					options:                   ops,
				}

				return Prep{
					hImport: hImport,
					cleanup: cleanup,
				}
			},
			verify: func(v Verify) {
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
				tc.region, tc.importResult,
			)
			verify := Verify{
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

// TestImportAndTargetSourcesCompatibilityConstraint tests the import and
// target sources compatibility constraint. It checks that the import and
// target sources are compatible.
func TestImportAndTargetSourcesCompatibilityConstraint(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name         string
		prep         func() *HeadersImport
		expectErr    bool
		expectErrMsg string
	}{
		{
			name: "ErrorOnGetBlockHeaderMetadata",
			prep: func() *HeadersImport {
				// Mock block import store.
				bIS := &mockHeaderImportSource{}
				bIS.On("GetHeaderMetadata").Return(
					nil, errors.New("I/O read error"),
				)
				hImport := &HeadersImport{
					BlockHeadersImportSource: bIS,
				}
				return hImport
			},
			expectErr:    true,
			expectErrMsg: "I/O read error",
		},
		{
			name: "ErrorOnGetFilterHeaderMetadata",
			prep: func() *HeadersImport {
				// Mock block import store.
				bIS := &mockHeaderImportSource{}
				bIS.On("GetHeaderMetadata").Return(
					&HeaderMetadata{}, nil,
				)

				// Mock filter import store.
				fIS := &mockHeaderImportSource{}
				fIS.On("GetHeaderMetadata").Return(
					nil, errors.New("I/O read error"),
				)

				hImport := &HeadersImport{
					BlockHeadersImportSource:  bIS,
					FilterHeadersImportSource: fIS,
				}
				return hImport
			},
			expectErr:    true,
			expectErrMsg: "I/O read error",
		},
		{
			name: "ErrorOnIncorrectBlockHeaderType",
			prep: func() *HeadersImport {
				// Mock block import store.
				bIS := &mockHeaderImportSource{}
				rFT := headerfs.RegularFilter
				bM := &HeaderMetadata{
					HeaderType: rFT,
				}
				bIS.On("GetHeaderMetadata").Return(bM, nil)

				// Mock filter import store.
				fIS := &mockHeaderImportSource{}
				fIS.On("GetHeaderMetadata").Return(
					&HeaderMetadata{}, nil,
				)

				hImport := &HeadersImport{
					BlockHeadersImportSource:  bIS,
					FilterHeadersImportSource: fIS,
				}
				return hImport
			},
			expectErr: true,
			expectErrMsg: "incorrect block header type: expected " +
				"BlockHeader, got RegularFilterHeader",
		},
		{
			name: "ErrorOnIncorrectFilterHeaderType",
			prep: func() *HeadersImport {
				// Mock block import store.
				bIS := &mockHeaderImportSource{}
				bM := &HeaderMetadata{
					HeaderType: headerfs.Block,
				}
				bIS.On("GetHeaderMetadata").Return(bM, nil)

				// Mock filter import store.
				fIS := &mockHeaderImportSource{}
				fM := &HeaderMetadata{
					HeaderType: headerfs.Block,
				}
				fIS.On("GetHeaderMetadata").Return(fM, nil)

				hImport := &HeadersImport{
					BlockHeadersImportSource:  bIS,
					FilterHeadersImportSource: fIS,
				}
				return hImport
			},
			expectErr: true,
			expectErrMsg: "incorrect filter header type: " +
				"expected RegularFilterHeader, got BlockHeader",
		},
		{
			name: "ErrorOnMismatchImportChainTypes",
			prep: func() *HeadersImport {
				// Mock block import store.
				bIS := &mockHeaderImportSource{}
				bT := headerfs.Block
				bM := &HeaderMetadata{
					HeaderType:       bT,
					BitcoinChainType: wire.MainNet,
				}
				bIS.On("GetHeaderMetadata").Return(bM, nil)
				bIS.On("GetURI").Return("/path/to/blocks")

				// Mock filter import store.
				fIS := &mockHeaderImportSource{}
				rFT := headerfs.RegularFilter
				fM := &HeaderMetadata{
					HeaderType:       rFT,
					BitcoinChainType: wire.SimNet,
				}
				fIS.On("GetHeaderMetadata").Return(fM, nil)
				fIS.On("GetURI").Return("/path/to/filters")

				hImport := &HeadersImport{
					BlockHeadersImportSource:  bIS,
					FilterHeadersImportSource: fIS,
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
			prep: func() *HeadersImport {
				// Mock block import store.
				bIS := &mockHeaderImportSource{}
				bT := headerfs.Block
				bM := &HeaderMetadata{
					HeaderType:       bT,
					BitcoinChainType: wire.SimNet,
				}
				bIS.On("GetHeaderMetadata").Return(bM, nil)

				// Mock filter import store.
				fIS := &mockHeaderImportSource{}
				rFT := headerfs.RegularFilter
				fM := &HeaderMetadata{
					HeaderType:       rFT,
					BitcoinChainType: wire.SimNet,
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

				hImport := &HeadersImport{
					BlockHeadersImportSource:  bIS,
					FilterHeadersImportSource: fIS,
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
			prep: func() *HeadersImport {
				// Mock block import store.
				bIS := &mockHeaderImportSource{}
				bT := headerfs.Block
				bM := &HeaderMetadata{
					HeaderType:       bT,
					BitcoinChainType: wire.SimNet,
					StartHeight:      1,
				}
				bIS.On("GetHeaderMetadata").Return(bM, nil)
				bIS.On("GetURI").Return("/path/to/blocks")

				// Mock filter import store.
				fIS := &mockHeaderImportSource{}
				rFT := headerfs.RegularFilter
				fM := &HeaderMetadata{
					HeaderType:       rFT,
					BitcoinChainType: wire.SimNet,
					StartHeight:      3,
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

				hImport := &HeadersImport{
					BlockHeadersImportSource:  bIS,
					FilterHeadersImportSource: fIS,
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
			prep: func() *HeadersImport {
				// Mock block import store.
				bIS := &mockHeaderImportSource{}
				bT := headerfs.Block
				bM := &HeaderMetadata{
					HeaderType:       bT,
					BitcoinChainType: wire.SimNet,
					StartHeight:      1,
					HeadersCount:     3,
				}
				bIS.On("GetHeaderMetadata").Return(bM, nil)
				bIS.On("GetURI").Return("/path/to/blocks")

				// Mock filter import store.
				fIS := &mockHeaderImportSource{}
				rFT := headerfs.RegularFilter
				fM := &HeaderMetadata{
					HeaderType:       rFT,
					BitcoinChainType: wire.SimNet,
					StartHeight:      1,
					HeadersCount:     5,
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

				hImport := &HeadersImport{
					BlockHeadersImportSource:  bIS,
					FilterHeadersImportSource: fIS,
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
			prep: func() *HeadersImport {
				// Mock block import store.
				bIS := &mockHeaderImportSource{}
				bT := headerfs.Block
				bM := &HeaderMetadata{
					HeaderType:       bT,
					BitcoinChainType: wire.SimNet,
					StartHeight:      1,
					HeadersCount:     3,
				}
				bIS.On("GetHeaderMetadata").Return(bM, nil)

				// Mock filter import store.
				fIS := &mockHeaderImportSource{}
				rFT := headerfs.RegularFilter
				fM := &HeaderMetadata{
					HeaderType:       rFT,
					BitcoinChainType: wire.SimNet,
					StartHeight:      1,
					HeadersCount:     3,
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

				hImport := &HeadersImport{
					BlockHeadersImportSource:  bIS,
					FilterHeadersImportSource: fIS,
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
		hImport *HeadersImport
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
				hImport := &HeadersImport{
					BlockHeadersImportSource: bIS,
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
					&HeaderMetadata{}, nil,
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

				h := &HeadersImport{
					BlockHeadersImportSource: bIS,
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
					&HeaderMetadata{}, nil,
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

				h := &HeadersImport{
					BlockHeadersImportSource: bIS,
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
					&HeaderMetadata{}, nil,
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

				h := &HeadersImport{
					BlockHeadersImportSource: bIS,
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
				metadata := &HeaderMetadata{
					StartHeight: 2,
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

				h := &HeadersImport{
					BlockHeadersImportSource: bIS,
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
					blockHdrs[0],
					uint32(0),
				)
				if err != nil {
					return Prep{err: err}
				}

				// Mock block import source.
				bIS := &mockHeaderImportSource{}
				bIS.On("GetHeaderMetadata").Return(
					&HeaderMetadata{}, nil,
				)
				bIS.On("GetHeader", uint32(0)).Return(
					NewBlockHeader(), nil,
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

				h := &HeadersImport{
					BlockHeadersImportSource: bIS,
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
				bIS.On("GetHeaderMetadata").Return(
					&HeaderMetadata{}, nil,
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
					&HeaderMetadata{}, nil,
				)
				fIS.On("GetHeader", uint32(0)).Return(
					NewFilterHeader(), nil,
				)

				// Configure Import options.
				ops := &ImportOptions{
					TargetBlockHeaderStore:  bHS,
					TargetFilterHeaderStore: fHS,
				}

				h := &HeadersImport{
					BlockHeadersImportSource:  bIS,
					FilterHeadersImportSource: fIS,
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
				bIS.On("GetHeaderMetadata").Return(
					&HeaderMetadata{}, nil,
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
					&HeaderMetadata{}, nil,
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

				h := &HeadersImport{
					BlockHeadersImportSource:  bIS,
					FilterHeadersImportSource: fIS,
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
				metadata := &HeaderMetadata{
					StartHeight: 1,
				}
				bIS.On("GetHeaderMetadata").Return(
					metadata, nil,
				)
				bIS.On("GetHeader", uint32(0)).Return(
					NewBlockHeader(), nil,
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

				h := &HeadersImport{
					BlockHeadersImportSource: bIS,
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
					blockHdrs[0],
					uint32(0),
				)
				if err != nil {
					return Prep{err: err}
				}

				// Mock block import source.
				bIS := &mockHeaderImportSource{}
				metadata := &HeaderMetadata{
					StartHeight: 1,
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

				h := &HeadersImport{
					BlockHeadersImportSource: bIS,
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
				metadata := &HeaderMetadata{
					StartHeight: 1,
				}
				bIS.On("GetHeaderMetadata").Return(
					metadata, nil,
				)
				bIS.On("GetHeader", uint32(0)).Return(
					NewFilterHeader(), nil,
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

				h := &HeadersImport{
					BlockHeadersImportSource: bIS,
					options:                  ops,
				}

				return Prep{
					hImport: h,
				}
			},
			expectErr: true,
			expectErrMsg: "expected BlockHeader type, got " +
				"*chainimport.FilterHeader",
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
				metadata := &HeaderMetadata{
					StartHeight: 1,
				}
				bIS.On("GetHeaderMetadata").Return(
					metadata, nil,
				)
				bIS.On("GetHeader", uint32(0)).Return(
					NewBlockHeader(), nil,
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

				h := &HeadersImport{
					BlockHeadersImportSource: bIS,
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
				metadata := &HeaderMetadata{
					StartHeight: 1,
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

				h := &HeadersImport{
					BlockHeadersImportSource: bIS,
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
		hI  *HeadersImport
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
				hImport := &HeadersImport{
					BlockHeadersImportSource: bIS,
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
					&HeaderMetadata{
						StartHeight: uint32(1),
					}, nil,
				)
				importIndx := height - 1
				bIS.On("GetHeader", importIndx).Return(
					nil, errors.New("I/O read error"),
				)
				hImport := &HeadersImport{
					BlockHeadersImportSource: bIS,
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
					&HeaderMetadata{
						StartHeight: uint32(1),
					}, nil,
				)
				importIndx := height - 1
				bIS.On("GetHeader", importIndx).Return(
					NewFilterHeader(), nil,
				)
				hImport := &HeadersImport{
					BlockHeadersImportSource: bIS,
				}
				return Prep{
					hI: hImport,
				}
			},
			expectErr: true,
			expectErrMsg: "expected BlockHeader type, got " +
				"*chainimport.FilterHeader",
		},
		{
			name:   "ErrorOnGetBlockHeaderFromTargetStore",
			height: 101,
			prep: func(height uint32) Prep {
				// Mock block import store.
				bIS := &mockHeaderImportSource{}
				bIS.On("GetHeaderMetadata").Return(
					&HeaderMetadata{
						StartHeight: uint32(1),
					}, nil,
				)
				importIndx := height - 1
				bIS.On("GetHeader", importIndx).Return(
					NewBlockHeader(), nil,
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

				hImport := &HeadersImport{
					BlockHeadersImportSource: bIS,
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
					&HeaderMetadata{
						StartHeight: uint32(1),
					}, nil,
				)
				importIndx := height - 1
				bIS.On("GetHeader", importIndx).Return(
					NewBlockHeader(), nil,
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

				hImport := &HeadersImport{
					BlockHeadersImportSource: bIS,
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
					&HeaderMetadata{
						StartHeight: uint32(1),
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
					&HeaderMetadata{
						StartHeight: uint32(1),
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
				hImport := &HeadersImport{
					BlockHeadersImportSource:  bIS,
					FilterHeadersImportSource: fIS,
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
					&HeaderMetadata{
						StartHeight: uint32(1),
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
					&HeaderMetadata{
						StartHeight: uint32(1),
					}, nil,
				)
				importIndx = height - 1
				fIS.On("GetHeader", importIndx).Return(bH, nil)

				// Configure import options.
				ops := &ImportOptions{
					TargetBlockHeaderStore: bHS,
				}

				// Configure headers import.
				hImport := &HeadersImport{
					BlockHeadersImportSource:  bIS,
					FilterHeadersImportSource: fIS,
					options:                   ops,
				}

				return Prep{
					hI: hImport,
				}
			},
			expectErr: true,
			expectErrMsg: "expected FilterHeader type, got " +
				"*chainimport.BlockHeader",
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
					&HeaderMetadata{
						StartHeight: uint32(1),
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
					&HeaderMetadata{
						StartHeight: uint32(1),
					}, nil,
				)
				importIndx = height - 1
				fIS.On("GetHeader", importIndx).Return(
					NewFilterHeader(), nil,
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
				hImport := &HeadersImport{
					BlockHeadersImportSource:  bIS,
					FilterHeadersImportSource: fIS,
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
					&HeaderMetadata{
						StartHeight: uint32(1),
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
					&HeaderMetadata{
						StartHeight: uint32(1),
					}, nil,
				)
				importIndx = height - 1
				fIS.On("GetHeader", importIndx).Return(
					NewFilterHeader(), nil,
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
				hImport := &HeadersImport{
					BlockHeadersImportSource:  bIS,
					FilterHeadersImportSource: fIS,
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
					&HeaderMetadata{
						StartHeight: uint32(1),
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
					&HeaderMetadata{
						StartHeight: uint32(1),
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
				hImport := &HeadersImport{
					BlockHeadersImportSource:  bIS,
					FilterHeadersImportSource: fIS,
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

// mockHeaderImportSource mocks a header import source for testing import source
// interactions.
type mockHeaderImportSource struct {
	mock.Mock
	uri string
}

// Open opens the mock header import source.
func (m *mockHeaderImportSource) Open() error {
	args := m.Called()
	return args.Error(0)
}

// Close closes the mock header import source.
func (m *mockHeaderImportSource) Close() error {
	args := m.Called()
	return args.Error(0)
}

// GetHeaderMetadata gets header metadata from the mock header import source.
func (m *mockHeaderImportSource) GetHeaderMetadata() (*HeaderMetadata, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*HeaderMetadata), args.Error(1)
}

// Iterator returns a header iterator from the mock header import source.
func (m *mockHeaderImportSource) Iterator(start, end uint32) HeaderIterator {
	args := m.Called(start, end)
	return args.Get(0).(HeaderIterator)
}

// GetHeader gets a header by index from the mock header import source.
func (m *mockHeaderImportSource) GetHeader(index uint32) (Header, error) {
	args := m.Called(index)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(Header), args.Error(1)
}

// GetURI gets the URI from the mock header import source.
func (m *mockHeaderImportSource) GetURI() string {
	args := m.Called()
	return args.String(0)
}

// SetURI sets the URI for the mock header import source.
func (m *mockHeaderImportSource) SetURI(uri string) {
	m.Called(uri)
	m.uri = uri
}

// mockHeaderIterator mocks a header iterator for testing header iteration
// logic.
type mockHeaderIterator struct {
	mock.Mock
}

// Next returns the next header from the mock header iterator.
func (m *mockHeaderIterator) Next() (Header, bool, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Get(1).(bool), args.Error(2)
	}
	return args.Get(0).(Header), args.Get(1).(bool), args.Error(2)
}

// Close closes the mock header iterator.
func (m *mockHeaderIterator) Close() error {
	args := m.Called()
	return args.Error(0)
}

// mockHTTPClient mocks an HTTP client for testing HTTP header import source
// interactions.
type mockHTTPClient struct {
	mock.Mock
}

// Get returns a response from the mock HTTP client.
func (m *mockHTTPClient) Get(url string) (*http.Response, error) {
	args := m.Called(url)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*http.Response), args.Error(1)
}

// setupFileWithHdrs creates a temporary file with headers and returns the file,
// a cleanup function, and an error if any.
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

// constructBlkHdr constructs a block header from a hex string and height.
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

// constructFilterHdr constructs a filter header from a hex string and height.
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
