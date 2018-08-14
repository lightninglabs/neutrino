package headerfs

import (
	"fmt"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcwallet/waddrmgr"
)

// mockBlockHeaderStore is an implementation of the BlockHeaderStore backed by
// a simple map.
type mockBlockHeaderStore struct {
	headers map[chainhash.Hash]wire.BlockHeader
}

// A compile-time check to ensure the mockBlockHeaderStore adheres to the
// BlockHeaderStore interface.
var _ BlockHeaderStore = (*mockBlockHeaderStore)(nil)

// NewMockBlockHeaderStore ...
func NewMockBlockHeaderStore() BlockHeaderStore {
	return &mockBlockHeaderStore{
		headers: make(map[chainhash.Hash]wire.BlockHeader),
	}
}

func (m *mockBlockHeaderStore) ChainTip() (*wire.BlockHeader,
	uint32, error) {
	return nil, 0, nil

}

func (m *mockBlockHeaderStore) LatestBlockLocator() (
	blockchain.BlockLocator, error) {
	return nil, nil
}

func (m *mockBlockHeaderStore) FetchHeaderByHeight(height uint32) (
	*wire.BlockHeader, error) {

	return nil, nil
}

func (m *mockBlockHeaderStore) FetchHeaderAncestors(uint32,
	*chainhash.Hash) ([]wire.BlockHeader, uint32, error) {

	return nil, 0, nil
}

func (m *mockBlockHeaderStore) HeightFromHash(*chainhash.Hash) (uint32, error) {
	return 0, nil

}

func (m *mockBlockHeaderStore) FetchHeader(h *chainhash.Hash) (
	*wire.BlockHeader, uint32, error) {
	if header, ok := m.headers[*h]; ok {
		return &header, 0, nil
	}
	return nil, 0, fmt.Errorf("not found")
}

func (m *mockBlockHeaderStore) WriteHeaders(headers ...BlockHeader) error {
	for _, h := range headers {
		m.headers[h.BlockHash()] = *h.BlockHeader
	}
	return nil
}

func (m *mockBlockHeaderStore) RollbackLastBlock() (*waddrmgr.BlockStamp,
	error) {
	return nil, nil
}
