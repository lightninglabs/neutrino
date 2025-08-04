package headerfs

import (
	"io"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcwallet/walletdb"
	"github.com/stretchr/testify/mock"
)

// MockBlockHeaderStore is a mock implementation of the BlockHeaderStore.
type MockBlockHeaderStore struct {
	mock.Mock
}

// ChainTip returns the current chain tip for the mock block header store.
func (m *MockBlockHeaderStore) ChainTip() (*wire.BlockHeader, uint32, error) {
	args := m.Called()
	return args.Get(0).(*wire.BlockHeader),
		args.Get(1).(uint32),
		args.Error(2)
}

// LatestBlockLocator returns the latest block locator for the mock block header
// store.
//
//nolint:lll
func (m *MockBlockHeaderStore) LatestBlockLocator() (blockchain.BlockLocator, error) {
	args := m.Called()
	return args.Get(0).(blockchain.BlockLocator), args.Error(1)
}

// FetchHeaderByHeight fetches a block header by height for the mock block
// header store.
func (m *MockBlockHeaderStore) FetchHeaderByHeight(
	height uint32) (*wire.BlockHeader, error) {

	args := m.Called(height)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*wire.BlockHeader), args.Error(1)
}

// FetchHeaderAncestors fetches block header ancestors for the mock block header
// store.
func (m *MockBlockHeaderStore) FetchHeaderAncestors(numHeaders uint32,
	stopHash *chainhash.Hash) ([]wire.BlockHeader, uint32, error) {

	args := m.Called(numHeaders, stopHash)
	return args.Get(0).([]wire.BlockHeader),
		args.Get(1).(uint32),
		args.Error(2)
}

// HeightFromHash returns the height from a hash for the mock block header
// store.
func (m *MockBlockHeaderStore) HeightFromHash(
	hash *chainhash.Hash) (uint32, error) {

	args := m.Called(hash)
	return args.Get(0).(uint32), args.Error(1)
}

// FetchHeader fetches a block header by hash for the mock block header store.
func (m *MockBlockHeaderStore) FetchHeader(
	hash *chainhash.Hash) (*wire.BlockHeader, uint32, error) {

	args := m.Called(hash)
	if args.Get(0) == nil {
		return nil, args.Get(1).(uint32), args.Error(2)
	}
	return args.Get(0).(*wire.BlockHeader),
		args.Get(1).(uint32),
		args.Error(2)
}

// WriteHeaders writes block headers to the mock block header store.
func (m *MockBlockHeaderStore) WriteHeaders(hdrs ...BlockHeader) error {
	args := m.Called(hdrs)
	return args.Error(0)
}

// RollbackBlockHeaders rolls back block headers in the mock block header store.
func (m *MockBlockHeaderStore) RollbackBlockHeaders(
	numHeaders uint32) (*BlockStamp, error) {

	args := m.Called(numHeaders)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*BlockStamp), args.Error(1)
}

// RollbackLastBlock rolls back the last block in the mock block header store.
func (m *MockBlockHeaderStore) RollbackLastBlock() (*BlockStamp, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*BlockStamp), args.Error(1)
}

// MockFilterHeaderStore is a mock implementation of the FilterHeaderStore.
type MockFilterHeaderStore struct {
	mock.Mock
}

// ChainTip returns the current chain tip for the mock filter header store.
func (m *MockFilterHeaderStore) ChainTip() (*chainhash.Hash, uint32, error) {
	args := m.Called()
	return args.Get(0).(*chainhash.Hash),
		args.Get(1).(uint32),
		args.Error(2)
}

// FetchHeader fetches a filter header by hash for the mock filter header store.
func (m *MockFilterHeaderStore) FetchHeader(
	hash *chainhash.Hash) (*chainhash.Hash, error) {

	args := m.Called(hash)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*chainhash.Hash), args.Error(1)
}

// FetchHeaderAncestors fetches filter header ancestors for the mock filter
// header store.
func (m *MockFilterHeaderStore) FetchHeaderAncestors(numHeaders uint32,
	stopHash *chainhash.Hash) ([]chainhash.Hash, uint32, error) {

	args := m.Called(numHeaders, stopHash)
	return args.Get(0).([]chainhash.Hash),
		args.Get(1).(uint32),
		args.Error(2)
}

// FetchHeaderByHeight fetches a filter header by height for the mock filter
// header store.
func (m *MockFilterHeaderStore) FetchHeaderByHeight(
	height uint32) (*chainhash.Hash, error) {

	args := m.Called(height)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*chainhash.Hash), args.Error(1)
}

// WriteHeaders writes filter headers to the mock filter header store.
func (m *MockFilterHeaderStore) WriteHeaders(headers ...FilterHeader) error {
	args := m.Called(headers)
	return args.Error(0)
}

// RollbackLastBlock rolls back the last block in the mock filter header store.
func (m *MockFilterHeaderStore) RollbackLastBlock(
	newTip *chainhash.Hash) (*BlockStamp, error) {

	args := m.Called(newTip)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*BlockStamp), args.Error(1)
}

// MockWalletDB is a mock implementation of walletdb.DB for testing.
type MockWalletDB struct {
	mock.Mock
}

// Update implements the walletdb.DB interface.
func (m *MockWalletDB) Update(fn func(tx walletdb.ReadWriteTx) error) error {
	args := m.Called(fn)
	return args.Error(0)
}

// View implements the walletdb.DB interface.
func (m *MockWalletDB) View(fn func(tx walletdb.ReadTx) error) error {
	args := m.Called(fn)
	return args.Error(0)
}

// BeginReadWriteTx implements the walletdb.DB interface.
func (m *MockWalletDB) BeginReadWriteTx() (walletdb.ReadWriteTx, error) {
	args := m.Called()

	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(walletdb.ReadWriteTx), args.Error(1)
}

// BeginReadTx implements the walletdb.DB interface.
func (m *MockWalletDB) BeginReadTx() (walletdb.ReadTx, error) {
	args := m.Called()

	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(walletdb.ReadTx), args.Error(1)
}

// Copy implements the walletdb.DB interface.
func (m *MockWalletDB) Copy(w io.Writer) error {
	args := m.Called(w)
	return args.Error(0)
}

// Close implements the walletdb.DB interface.
func (m *MockWalletDB) Close() error {
	args := m.Called()
	return args.Error(0)
}
