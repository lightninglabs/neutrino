package rescan

import (
	"database/sql"
	"errors"
	"fmt"
	"io"

	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/headerfs"
)

// HeaderStoreError is a failed read of the header store. Like the legacy
// rescan loop, the actor ends the rescan on one instead of retrying it.
//
// A read that finds no header is not one: a reorg that rolls the header
// store back can make a hash or a height disappear between two reads, and
// the disconnect notifications that follow let the scan recover. The flat
// file store reports any failed read as a missing header, so only a read
// past its end, or a sql store's missing row, counts as one.
type HeaderStoreError struct {
	Err error
}

// Error returns the read error.
func (e *HeaderStoreError) Error() string {
	return fmt.Sprintf("header store read failed: %v", e.Err)
}

// Unwrap returns the read error.
func (e *HeaderStoreError) Unwrap() error {
	return e.Err
}

// storeErr wraps a header store read error in a HeaderStoreError, unless it
// is nil or reports a header that is not there.
func storeErr(err error) error {
	var notFound *headerfs.ErrHeaderNotFound
	switch {
	case err == nil,
		errors.Is(err, headerfs.ErrHashNotFound),
		errors.Is(err, headerfs.ErrHeightNotFound):

		return err

	case errors.As(err, &notFound) && (errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, sql.ErrNoRows)):

		return err
	}

	return &HeaderStoreError{Err: err}
}

// bestBlock reads the best block from the header store.
func (e *Environment) bestBlock() (*headerfs.BlockStamp, error) {
	best, err := e.Chain.BestBlock()
	return best, storeErr(err)
}

// headerByHeight reads the active header at height from the header store.
func (e *Environment) headerByHeight(height uint32) (*wire.BlockHeader,
	error) {

	header, err := e.Chain.GetBlockHeaderByHeight(height)
	return header, storeErr(err)
}

// header reads the header with the given hash, and its height, from the
// header store.
func (e *Environment) header(hash *chainhash.Hash) (*wire.BlockHeader, uint32,
	error) {

	header, height, err := e.Chain.GetBlockHeader(hash)
	return header, height, storeErr(err)
}
