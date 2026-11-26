package neutrino

import (
	"fmt"
	"testing"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/btcutil/gcs"
	"github.com/btcsuite/btcd/btcutil/gcs/builder"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightninglabs/neutrino/cache/lru"
	"github.com/lightninglabs/neutrino/filterdb"
	"github.com/lightninglabs/neutrino/headerfs"
)

// mockFilterDB implements filterdb.FilterDatabase for testing.
type mockFilterDB struct {
	filters map[chainhash.Hash]*gcs.Filter
}

func newMockFilterDB() *mockFilterDB {
	return &mockFilterDB{
		filters: make(map[chainhash.Hash]*gcs.Filter),
	}
}

func (m *mockFilterDB) PutFilters(filters ...*filterdb.FilterData) error {
	for _, f := range filters {
		m.filters[*f.BlockHash] = f.Filter
	}
	return nil
}

func (m *mockFilterDB) FetchFilter(
	hash *chainhash.Hash, _ filterdb.FilterType,
) (*gcs.Filter, error) {

	f, ok := m.filters[*hash]
	if !ok {
		return nil, filterdb.ErrFilterNotFound
	}
	return f, nil
}

func (m *mockFilterDB) PurgeFilters(_ filterdb.FilterType) error {
	m.filters = make(map[chainhash.Hash]*gcs.Filter)
	return nil
}

// mockBlockHeaderStore implements headerfs.BlockHeaderStore for testing.
type mockBlockHeaderStore struct {
	headers map[uint32]*wire.BlockHeader
}

func newMockBlockHeaderStore() *mockBlockHeaderStore {
	return &mockBlockHeaderStore{
		headers: make(map[uint32]*wire.BlockHeader),
	}
}

func (m *mockBlockHeaderStore) FetchHeaderByHeight(
	height uint32,
) (*wire.BlockHeader, error) {

	h, ok := m.headers[height]
	if !ok {
		return nil, fmt.Errorf("block header not found at height %d",
			height)
	}
	return h, nil
}

func (m *mockBlockHeaderStore) ChainTip() (*wire.BlockHeader, uint32, error) {
	return nil, 0, nil
}

func (m *mockBlockHeaderStore) LatestBlockLocator() (
	blockchain.BlockLocator, error) {

	return nil, nil
}

func (m *mockBlockHeaderStore) FetchHeaderAncestors(
	_ uint32, _ *chainhash.Hash,
) ([]wire.BlockHeader, uint32, error) {

	return nil, 0, nil
}

func (m *mockBlockHeaderStore) HeightFromHash(
	_ *chainhash.Hash,
) (uint32, error) {

	return 0, nil
}

func (m *mockBlockHeaderStore) FetchHeader(
	_ *chainhash.Hash,
) (*wire.BlockHeader, uint32, error) {

	return nil, 0, nil
}

func (m *mockBlockHeaderStore) WriteHeaders(
	_ ...headerfs.BlockHeader,
) error {

	return nil
}

func (m *mockBlockHeaderStore) RollbackBlockHeaders(
	_ uint32,
) (*headerfs.BlockStamp, error) {

	return nil, nil
}

func (m *mockBlockHeaderStore) RollbackLastBlock() (
	*headerfs.BlockStamp, error) {

	return nil, nil
}

// mockFilterHeaderStore implements headerfs.FilterHeaderStore for testing.
type mockFilterHeaderStore struct {
	headers map[uint32]*chainhash.Hash
}

func newMockFilterHeaderStore() *mockFilterHeaderStore {
	return &mockFilterHeaderStore{
		headers: make(map[uint32]*chainhash.Hash),
	}
}

func (m *mockFilterHeaderStore) FetchHeaderByHeight(
	height uint32,
) (*chainhash.Hash, error) {

	h, ok := m.headers[height]
	if !ok {
		return nil, fmt.Errorf("filter header not found at height %d",
			height)
	}
	return h, nil
}

func (m *mockFilterHeaderStore) ChainTip() (
	*chainhash.Hash, uint32, error) {

	return nil, 0, nil
}

func (m *mockFilterHeaderStore) FetchHeader(
	_ *chainhash.Hash,
) (*chainhash.Hash, error) {

	return nil, nil
}

func (m *mockFilterHeaderStore) FetchHeaderAncestors(
	_ uint32, _ *chainhash.Hash,
) ([]chainhash.Hash, uint32, error) {

	return nil, 0, nil
}

func (m *mockFilterHeaderStore) WriteHeaders(
	_ ...headerfs.FilterHeader,
) error {

	return nil
}

func (m *mockFilterHeaderStore) RollbackLastBlock(
	_ *chainhash.Hash,
) (*headerfs.BlockStamp, error) {

	return nil, nil
}

// buildTestFilter creates a GCS filter containing the given data entries and
// returns the filter, its raw bytes, and the filter header hash computed from
// the given previous header.
func buildTestFilter(
	t *testing.T, blockHash *chainhash.Hash,
	prevHeader chainhash.Hash, entries [][]byte,
) (*gcs.Filter, []byte, chainhash.Hash) {

	t.Helper()

	key := builder.DeriveKey(blockHash)
	filter, err := gcs.BuildGCSFilter(
		builder.DefaultP, builder.DefaultM, key, entries,
	)
	if err != nil {
		t.Fatalf("build filter: %v", err)
	}

	raw, err := filter.NBytes()
	if err != nil {
		t.Fatalf("filter NBytes: %v", err)
	}

	filterHeader, err := builder.MakeHeaderForFilter(filter, prevHeader)
	if err != nil {
		t.Fatalf("make filter header: %v", err)
	}

	return filter, raw, filterHeader
}

// setupTestChainService creates a ChainService with mock stores pre-populated
// with block headers and filter headers for the given height range.
func setupTestChainService(
	t *testing.T,
) (*ChainService, *mockFilterDB, []chainhash.Hash, [][]byte) {

	t.Helper()

	const numBlocks = 5

	blockStore := newMockBlockHeaderStore()
	filterHeaderStore := newMockFilterHeaderStore()
	filterDB := newMockFilterDB()

	cache := lru.NewCache[FilterCacheKey, *CacheableFilter](1 << 20)

	cs := &ChainService{
		FilterDB:         filterDB,
		BlockHeaders:     blockStore,
		RegFilterHeaders: filterHeaderStore,
		FilterCache:      cache,
	}

	// Build a chain of block headers and filters.
	var prevFilterHeader chainhash.Hash
	blockHashes := make([]chainhash.Hash, numBlocks)
	rawFilters := make([][]byte, numBlocks)

	for i := range numBlocks {
		bh := &wire.BlockHeader{
			Nonce: uint32(i),
		}
		if i > 0 {
			prevHash := blockHashes[i-1]
			bh.PrevBlock = prevHash
		}
		blockHash := bh.BlockHash()
		blockHashes[i] = blockHash
		blockStore.headers[uint32(i)] = bh

		entries := [][]byte{
			[]byte("entry-" + string(rune('A'+i))),
		}

		_, raw, filterHeader := buildTestFilter(
			t, &blockHash, prevFilterHeader, entries,
		)
		rawFilters[i] = raw

		fhCopy := filterHeader
		filterHeaderStore.headers[uint32(i)] = &fhCopy
		prevFilterHeader = filterHeader
	}

	return cs, filterDB, blockHashes, rawFilters
}

func TestImportCFilter(t *testing.T) {
	cs, filterDB, blockHashes, rawFilters := setupTestChainService(t)

	// Import a single filter at height 0.
	err := cs.ImportCFilter(0, rawFilters[0])
	if err != nil {
		t.Fatalf("ImportCFilter: %v", err)
	}

	// Verify it was stored.
	f, err := filterDB.FetchFilter(
		&blockHashes[0], filterdb.RegularFilter,
	)
	if err != nil {
		t.Fatalf("FetchFilter: %v", err)
	}
	if f == nil {
		t.Fatal("filter should be stored")
	}
}

func TestImportCFilter_CachesFilter(t *testing.T) {
	cs, _, blockHashes, rawFilters := setupTestChainService(t)

	err := cs.ImportCFilter(0, rawFilters[0])
	if err != nil {
		t.Fatalf("ImportCFilter: %v", err)
	}

	cached, err := cs.getFilterFromCache(
		&blockHashes[0], filterdb.RegularFilter,
	)
	if err != nil {
		t.Fatalf("getFilterFromCache: %v", err)
	}
	if cached == nil {
		t.Fatal("filter should be cached")
	}
}

func TestImportCFilter_AlreadyExists(t *testing.T) {
	cs, _, _, rawFilters := setupTestChainService(t)

	// Import once.
	if err := cs.ImportCFilter(0, rawFilters[0]); err != nil {
		t.Fatalf("first import: %v", err)
	}

	// Import again - should be a no-op.
	if err := cs.ImportCFilter(0, rawFilters[0]); err != nil {
		t.Fatalf("second import should succeed: %v", err)
	}
}

func TestImportCFilter_InvalidFilter(t *testing.T) {
	cs, _, _, _ := setupTestChainService(t)

	err := cs.ImportCFilter(0, []byte{0xff, 0xff})
	if err == nil {
		t.Fatal("expected error for invalid filter data")
	}
}

func TestImportCFilter_VerificationFailed(t *testing.T) {
	cs, _, _, rawFilters := setupTestChainService(t)

	// Try to import filter for height 0 at height 1 (wrong filter).
	err := cs.ImportCFilter(1, rawFilters[0])
	if err == nil {
		t.Fatal("expected verification error")
	}
}

func TestImportCFilter_MissingBlockHeader(t *testing.T) {
	cs, _, _, _ := setupTestChainService(t)

	// Height 10 doesn't exist.
	err := cs.ImportCFilter(10, []byte{0x01, 0x00})
	if err == nil {
		t.Fatal("expected error for missing block header")
	}
}

func TestImportCFilters(t *testing.T) {
	cs, filterDB, blockHashes, rawFilters := setupTestChainService(t)

	imported, err := cs.ImportCFilters(0, rawFilters)
	if err != nil {
		t.Fatalf("ImportCFilters: %v", err)
	}
	if imported != 5 {
		t.Fatalf("expected 5 imported, got %d", imported)
	}

	// Verify all stored.
	for i := range 5 {
		f, err := filterDB.FetchFilter(
			&blockHashes[i], filterdb.RegularFilter,
		)
		if err != nil {
			t.Fatalf("FetchFilter at %d: %v", i, err)
		}
		if f == nil {
			t.Fatalf("filter at height %d should be stored", i)
		}
	}
}

func TestImportCFilters_CachesImportedFilters(t *testing.T) {
	cs, _, blockHashes, rawFilters := setupTestChainService(t)

	imported, err := cs.ImportCFilters(0, rawFilters)
	if err != nil {
		t.Fatalf("ImportCFilters: %v", err)
	}
	if imported != len(rawFilters) {
		t.Fatalf("expected %d imported, got %d",
			len(rawFilters), imported)
	}

	for i := range blockHashes {
		cached, err := cs.getFilterFromCache(
			&blockHashes[i], filterdb.RegularFilter,
		)
		if err != nil {
			t.Fatalf("getFilterFromCache at %d: %v", i, err)
		}
		if cached == nil {
			t.Fatalf("filter at height %d should be cached", i)
		}
	}
}

func TestImportCFilters_Empty(t *testing.T) {
	cs, _, _, _ := setupTestChainService(t)

	imported, err := cs.ImportCFilters(0, nil)
	if err != nil {
		t.Fatalf("ImportCFilters empty: %v", err)
	}
	if imported != 0 {
		t.Fatalf("expected 0, got %d", imported)
	}
}

func TestImportCFilters_SkipsExisting(t *testing.T) {
	cs, _, _, rawFilters := setupTestChainService(t)

	// Import first 3.
	_, err := cs.ImportCFilters(0, rawFilters[:3])
	if err != nil {
		t.Fatalf("first batch: %v", err)
	}

	// Import all 5 - first 3 should be skipped.
	imported, err := cs.ImportCFilters(0, rawFilters)
	if err != nil {
		t.Fatalf("second batch: %v", err)
	}
	if imported != 2 {
		t.Fatalf("expected 2 newly imported filters, got %d", imported)
	}
}

func TestImportCFilters_StopsOnError(t *testing.T) {
	cs, _, _, rawFilters := setupTestChainService(t)

	// Corrupt filter at index 2.
	bad := make([][]byte, len(rawFilters))
	copy(bad, rawFilters)
	bad[2] = []byte{0xff, 0xff}

	_, err := cs.ImportCFilters(0, bad)
	if err == nil {
		t.Fatal("expected error on corrupt filter")
	}
}
