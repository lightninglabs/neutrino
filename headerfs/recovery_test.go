package headerfs

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcwallet/walletdb"
	"github.com/stretchr/testify/require"
)

// errInjected is the error returned by the fault injecting test doubles.
var errInjected = errors.New("injected fault")

// faultyDB wraps a walletdb.DB and fails all read/write transactions while
// failUpdates is set, which lets a test make an index write fail at an exact
// point.
type faultyDB struct {
	walletdb.DB

	failUpdates bool
}

// Update implements walletdb.DB, failing if failUpdates is set.
func (f *faultyDB) Update(fn func(tx walletdb.ReadWriteTx) error,
	reset func()) error {

	if f.failUpdates {
		return errInjected
	}

	return f.DB.Update(fn, reset)
}

// newFaultyDB creates a new bdb instance in dir wrapped in a faultyDB.
func newFaultyDB(t *testing.T, dir string) *faultyDB {
	t.Helper()

	db, err := walletdb.Create(
		"bdb", filepath.Join(dir, "test.db"), true, time.Second*10,
		false,
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	return &faultyDB{DB: db}
}

// newFilterStoreWithChain creates a block header store holding three headers
// and a filter header store alongside it in dir, and returns the filter store
// together with a matching chain of three filter headers. The filter index
// resolves heights through the block header index, so both are required.
func newFilterStoreWithChain(t *testing.T, dir string,
	db walletdb.DB) (*filterHeaderStore, []FilterHeader) {

	t.Helper()

	blockStore, err := NewBlockHeaderStore(dir, db, &chaincfg.SimNetParams)
	require.NoError(t, err)
	blocks := createTestBlockHeaderChain(3)
	require.NoError(t, blockStore.WriteHeaders(blocks...))

	store, err := NewFilterHeaderStore(
		dir, db, RegularFilter, &chaincfg.SimNetParams, nil,
	)
	require.NoError(t, err)

	chain := createTestFilterHeaderChain(3)
	for i := range chain {
		chain[i].HeaderHash = blocks[i].BlockHash()
	}

	return store.(*filterHeaderStore), chain
}

// requireFileLen asserts that the flat file at path has exactly the length
// required to hold the headers up to and including the index tip.
func requireFileLen(t *testing.T, path string, tipHeight uint32,
	recordSize int64) {

	t.Helper()

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, (int64(tipHeight)+1)*recordSize, info.Size())
}

// TestAppendRawPartialWriteAfterReopen asserts that a partial write on a file
// freshly opened with O_APPEND, where the file offset is still zero, only
// removes the partially written bytes rather than the entire file.
func TestAppendRawPartialWriteAfterReopen(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "headers.bin")
	existing := make([]byte, 3*BlockHeaderSize)
	require.NoError(t, os.WriteFile(path, existing, 0644))

	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0644)
	require.NoError(t, err)
	t.Cleanup(func() { _ = file.Close() })

	// Write half of the record to the real file, then fail.
	mock := &mockFile{
		File: file,
		writeFn: func(p []byte, f File) (int, error) {
			n, _ := f.Write(p[:len(p)/2])
			return n, errInjected
		},
	}
	h := &headerStore{
		headerFile:  &headerFile{file: mock},
		headerIndex: &headerIndex{indexType: Block},
	}

	err = h.appendRaw(make([]byte, BlockHeaderSize))
	require.ErrorIs(t, err, errInjected)

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, int64(len(existing)), info.Size())
}

// TestWriteHeadersIndexFailure asserts that if the index write fails after the
// flat file was written, and even if syncing the file fails as well, no orphan
// bytes are left in the flat file, both before and after a reopen.
func TestWriteHeadersIndexFailure(t *testing.T) {
	t.Parallel()

	t.Run("block", func(t *testing.T) {
		dir := t.TempDir()
		db := newFaultyDB(t, dir)
		store, err := NewBlockHeaderStore(
			dir, db, &chaincfg.SimNetParams,
		)
		require.NoError(t, err)
		bhs := store.(*blockHeaderStore)

		chain := createTestBlockHeaderChain(3)
		require.NoError(t, bhs.WriteHeaders(chain[:2]...))

		bhs.file = &mockFile{
			File:   bhs.file.(*os.File),
			syncFn: func() error { return errInjected },
		}
		db.failUpdates = true
		require.Error(t, bhs.WriteHeaders(chain[2]))
		db.failUpdates = false

		path := filepath.Join(dir, "block_headers.bin")
		requireFileLen(t, path, 2, BlockHeaderSize)

		_, err = NewBlockHeaderStore(dir, db, &chaincfg.SimNetParams)
		require.NoError(t, err)
		requireFileLen(t, path, 2, BlockHeaderSize)
	})

	t.Run("filter", func(t *testing.T) {
		dir := t.TempDir()
		db := newFaultyDB(t, dir)
		fhs, chain := newFilterStoreWithChain(t, dir, db)
		require.NoError(t, fhs.WriteHeaders(chain[:2]...))

		fhs.file = &mockFile{
			File:   fhs.file.(*os.File),
			syncFn: func() error { return errInjected },
		}
		db.failUpdates = true
		err := fhs.WriteHeaders(chain[2])
		require.Error(t, err)
		db.failUpdates = false

		path := filepath.Join(dir, "reg_filter_headers.bin")
		requireFileLen(t, path, 2, RegularFilterHeaderSize)

		_, err = NewFilterHeaderStore(
			dir, db, RegularFilter, &chaincfg.SimNetParams, nil,
		)
		require.NoError(t, err)
		requireFileLen(t, path, 2, RegularFilterHeaderSize)
	})
}

// TestRollbackIndexFailure asserts that if the index update during a rollback
// fails, the flat file isn't truncated, so the file never ends up shorter than
// the index tip.
func TestRollbackIndexFailure(t *testing.T) {
	t.Parallel()

	t.Run("block", func(t *testing.T) {
		dir := t.TempDir()
		db := newFaultyDB(t, dir)
		store, err := NewBlockHeaderStore(
			dir, db, &chaincfg.SimNetParams,
		)
		require.NoError(t, err)
		bhs := store.(*blockHeaderStore)
		require.NoError(t, bhs.WriteHeaders(
			createTestBlockHeaderChain(3)...,
		))

		db.failUpdates = true
		_, err = bhs.RollbackBlockHeaders(2)
		require.Error(t, err)
		db.failUpdates = false

		path := filepath.Join(dir, "block_headers.bin")
		_, tip, err := bhs.ChainTip()
		require.NoError(t, err)
		requireFileLen(t, path, tip, BlockHeaderSize)

		_, err = NewBlockHeaderStore(dir, db, &chaincfg.SimNetParams)
		require.NoError(t, err)
		requireFileLen(t, path, tip, BlockHeaderSize)
	})

	t.Run("filter", func(t *testing.T) {
		dir := t.TempDir()
		db := newFaultyDB(t, dir)
		fhs, chain := newFilterStoreWithChain(t, dir, db)
		require.NoError(t, fhs.WriteHeaders(chain...))

		db.failUpdates = true
		_, err := fhs.RollbackLastBlock(&chain[1].HeaderHash)
		require.Error(t, err)
		db.failUpdates = false

		path := filepath.Join(dir, "reg_filter_headers.bin")
		_, tip, err := fhs.ChainTip()
		require.NoError(t, err)
		requireFileLen(t, path, tip, RegularFilterHeaderSize)
	})
}

// TestReopenReconcilesFile asserts that on startup, a trailing partial record
// is truncated away, and that a file shorter than the index tip is reported as
// an error rather than wrapping the height arithmetic.
func TestReopenReconcilesFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	db := newFaultyDB(t, dir)
	store, err := NewBlockHeaderStore(dir, db, &chaincfg.SimNetParams)
	require.NoError(t, err)
	require.NoError(t, store.WriteHeaders(createTestBlockHeaderChain(3)...))

	path := filepath.Join(dir, "block_headers.bin")
	_, tip, err := store.ChainTip()
	require.NoError(t, err)

	// Simulate a crash in the middle of a write by leaving a partial
	// record at the end of the file.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
	require.NoError(t, err)
	_, err = f.Write(make([]byte, BlockHeaderSize/2))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	_, err = NewBlockHeaderStore(dir, db, &chaincfg.SimNetParams)
	require.NoError(t, err)
	requireFileLen(t, path, tip, BlockHeaderSize)

	// Now cut the file short of the index tip.
	require.NoError(t, os.Truncate(path, int64(tip)*BlockHeaderSize))
	_, err = NewBlockHeaderStore(dir, db, &chaincfg.SimNetParams)
	require.ErrorContains(t, err, "below")
}
