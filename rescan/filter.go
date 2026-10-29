package rescan

import (
	"context"
	"fmt"

	"github.com/btcsuite/btcd/address/v2"
	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/btcutil/v2/gcs"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/txscript/v2"
	"github.com/lightninglabs/neutrino/filtermatch"
	"github.com/lightninglabs/neutrino/headerfs"
)

// WatchState holds the mutable watch list state that is carried within FSM
// state structs. This groups the watch-related fields that need to travel
// together during state transitions.
type WatchState struct {
	// Addrs is the set of addresses being watched for incoming payments.
	Addrs []address.Address

	// Inputs is the set of outpoints being watched for spends.
	Inputs []filtermatch.InputWithScript

	// List holds the serialized pkScripts used for GCS filter matching.
	// This is the union of scripts derived from Addrs and Inputs.
	List [][]byte

	// addrScripts indexes payment scripts for matching transaction outputs.
	addrScripts map[string]struct{}
}

// Clone returns a deep copy of the WatchState so that state transitions
// don't share underlying slice memory.
func (ws *WatchState) Clone() WatchState {
	clone := WatchState{
		Addrs:  make([]address.Address, len(ws.Addrs)),
		Inputs: copyInputsWithScripts(ws.Inputs),
		List:   make([][]byte, len(ws.List)),
	}
	copy(clone.Addrs, ws.Addrs)
	for i, script := range ws.List {
		clone.List[i] = append([]byte(nil), script...)
	}
	if ws.addrScripts != nil {
		clone.addrScripts = make(
			map[string]struct{},
			len(ws.addrScripts),
		)
		for script := range ws.addrScripts {
			clone.addrScripts[script] = struct{}{}
		}
	}

	return clone
}

// ApplyUpdate adds the addresses and inputs from an AddWatchAddrsEvent
// to the watch state. It converts addresses to pkScripts and appends
// everything to the appropriate slices. It returns a new WatchState and leaves
// the original unchanged, or returns the original and an error if an address
// has no payment script.
func (ws *WatchState) ApplyUpdate(
	event *AddWatchAddrsEvent) (WatchState, error) {

	newWS := ws.Clone()
	if err := newWS.cacheAddrScripts(); err != nil {
		return *ws, err
	}

	newWS.Addrs = append(newWS.Addrs, event.Addrs...)
	newWS.Inputs = append(newWS.Inputs, event.Inputs...)

	for _, addr := range event.Addrs {
		script, err := txscript.PayToAddrScript(addr)
		if err != nil {
			return *ws, err
		}

		newWS.List = append(newWS.List, script)
		newWS.addrScripts[string(script)] = struct{}{}
	}

	for _, input := range event.Inputs {
		newWS.List = append(newWS.List, input.PkScript)
	}

	return newWS, nil
}

// cacheAddrScripts builds the output lookup once for this watch state.
func (ws *WatchState) cacheAddrScripts() error {
	if ws.addrScripts != nil {
		return nil
	}
	ws.addrScripts = make(map[string]struct{}, len(ws.Addrs))
	for _, addr := range ws.Addrs {
		script, err := txscript.PayToAddrScript(addr)
		if err != nil {
			ws.addrScripts = nil
			return err
		}
		ws.addrScripts[string(script)] = struct{}{}
	}
	return nil
}

// FetchAndMatchFilter fetches the compact filter for the given block hash
// and checks it against the watch list. Returns the match result and the
// filter (for potential later use in block extraction).
func FetchAndMatchFilter(ctx context.Context, chain ChainSource,
	blockHash *chainhash.Hash, watchList [][]byte,
	batch bool) (bool, *gcs.Filter, error) {

	filter, err := chain.GetCFilter(ctx, *blockHash, batch)
	if err != nil {
		return false, nil, err
	}

	// If the filter is empty, no match is possible.
	if filter == nil || filter.N() == 0 {
		return false, nil, nil
	}

	matched, err := filtermatch.MatchFilter(filter, blockHash, watchList)

	return matched, filter, err
}

// ExtractBlockMatches fetches the target block from the network and filters
// out any relevant transactions. It also updates the provided WatchState
// with new inputs discovered from matching outputs (so future spends are
// also watched). Returns the relevant transactions and the updated watch
// state.
func ExtractBlockMatches(ctx context.Context, chain ChainSource,
	curStamp *headerfs.BlockStamp, filter *gcs.Filter,
	ws WatchState) ([]*btcutil.Tx, WatchState, error) {

	ws = ws.Clone()
	block, err := chain.GetBlock(ctx, curStamp.Hash)
	if err != nil {
		return nil, ws, err
	}
	if block == nil {
		return nil, ws, fmt.Errorf("couldn't get block %d (%s) from "+
			"network", curStamp.Height, curStamp.Hash)
	}

	if err := ws.cacheAddrScripts(); err != nil {
		return nil, ws, err
	}

	// Verify the filter against the downloaded block.
	_, err = filtermatch.VerifyBasicBlockFilter(filter, block)
	if err != nil {
		return nil, ws, fmt.Errorf("error verifying filter against "+
			"block %d (%s), possibly invalid filter from peer: %v",
			curStamp.Height, curStamp.Hash, err)
	}

	relevantTxs := make([]*btcutil.Tx, 0, len(block.Transactions()))
	for _, tx := range block.Transactions() {
		relevant := filtermatch.SpendsWatchedInput(tx, ws.Inputs)

		// Even though the transaction may already be known as
		// relevant, we need to check its outputs anyway, as each one
		// that pays a watched address becomes a watched input.
		paid := filtermatch.PaysWatchedScript(tx, ws.addrScripts)
		for _, input := range paid {
			ws.Inputs = append(ws.Inputs, input)
			ws.List = append(ws.List, input.PkScript)
		}

		if relevant || len(paid) > 0 {
			relevantTxs = append(relevantTxs, tx)
		}
	}

	return relevantTxs, ws, nil
}
