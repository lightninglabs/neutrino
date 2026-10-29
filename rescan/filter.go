package rescan

import (
	"bytes"
	"context"
	"fmt"

	"github.com/btcsuite/btcd/address/v2"
	"github.com/btcsuite/btcd/btcjson"
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
//
// The watch list only grows, and a state's successor extends it in place:
// ApplyUpdate and ExtractBlockMatches append to the slices they were given
// rather than copy them, so an update costs O(1) amortized however long the
// list is. That is safe because a state's slices are read only up to their
// own lengths, which an append past them never changes, and because a state
// has at most one successor that lives on: the FSM commits one transition
// per event, and a transition that fails appends nothing it keeps. A watch
// state handed outside the FSM must be a Clone, which owns its memory.
type WatchState struct {
	// Addrs is the set of addresses being watched for incoming payments.
	Addrs []address.Address

	// Inputs is the set of outpoints being watched for spends.
	Inputs []filtermatch.InputWithScript

	// List holds the serialized pkScripts used for GCS filter matching.
	// This is the union of scripts derived from Addrs and Inputs.
	List [][]byte

	// addrList holds the payment script of each of Addrs, index for
	// index. It grows with Addrs.
	addrList [][]byte

	// addrScripts maps a payment script to the index in addrList where
	// it was first added. Successors share and extend it, so an entry
	// counts for a state only if its index is within that state's
	// addrList and addrList holds the same script there. A nil map is
	// built lazily from addrList.
	addrScripts map[string]int
}

// Clone returns a deep copy of the WatchState that shares no memory with it.
// Every slice of the copy has its length as its capacity, so appending to the
// copy can never write into the original. The copy builds its own script
// lookup on first use.
func (ws *WatchState) Clone() WatchState {
	clone := WatchState{
		Addrs:    append([]address.Address(nil), ws.Addrs...),
		Inputs:   copyInputsWithScripts(ws.Inputs),
		List:     copyScripts(ws.List),
		addrList: copyScripts(ws.addrList),
	}
	clone.Addrs = clone.Addrs[:len(clone.Addrs):len(clone.Addrs)]

	return clone
}

// copyScripts returns a deep copy of scripts whose capacity is its length.
func copyScripts(scripts [][]byte) [][]byte {
	if scripts == nil {
		return nil
	}

	copied := make([][]byte, len(scripts))
	for i, script := range scripts {
		copied[i] = append([]byte(nil), script...)
	}

	return copied
}

// ApplyUpdate adds the addresses and inputs from an AddWatchAddrsEvent
// to the watch state. It converts addresses to pkScripts and appends
// everything to the appropriate slices. It returns the extended WatchState,
// whose slices may share memory with the original's but leave everything the
// original reads unchanged, or returns the original and an error if an
// address has no payment script, in which case nothing was appended.
func (ws *WatchState) ApplyUpdate(
	event *AddWatchAddrsEvent) (WatchState, error) {

	// Convert every address first, so a bad one fails the update before
	// anything is appended.
	scripts := make([][]byte, len(event.Addrs))
	for i, addr := range event.Addrs {
		script, err := txscript.PayToAddrScript(addr)
		if err != nil {
			return *ws, err
		}
		scripts[i] = script
	}

	newWS := *ws
	newWS.cacheAddrScripts()

	newWS.Addrs = append(newWS.Addrs, event.Addrs...)
	newWS.Inputs = append(newWS.Inputs, event.Inputs...)

	for _, script := range scripts {
		newWS.List = append(newWS.List, script)
		newWS.addAddrScript(script)
	}

	for _, input := range event.Inputs {
		newWS.List = append(newWS.List, input.PkScript)
	}

	return newWS, nil
}

// cacheAddrScripts builds the payment script lookup if this watch state has
// none yet.
func (ws *WatchState) cacheAddrScripts() {
	if ws.addrScripts != nil {
		return
	}

	ws.addrScripts = make(map[string]int, len(ws.addrList))
	for i, script := range ws.addrList {
		if _, ok := ws.addrScripts[string(script)]; !ok {
			ws.addrScripts[string(script)] = i
		}
	}
}

// addAddrScript appends the payment script of an address just appended to
// Addrs, and indexes it unless this state already watches it.
func (ws *WatchState) addAddrScript(script []byte) {
	if !ws.watchesScript(script) {
		ws.addrScripts[string(script)] = len(ws.addrList)
	}
	ws.addrList = append(ws.addrList, script)
}

// watchesScript reports whether script is the payment script of one of this
// state's watched addresses. An entry another successor added, past this
// state's addrList or for a different script at that index, does not count.
func (ws *WatchState) watchesScript(script []byte) bool {
	ws.cacheAddrScripts()

	i, ok := ws.addrScripts[string(script)]
	return ok && i < len(ws.addrList) &&
		bytes.Equal(ws.addrList[i], script)
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
// also watched). Returns the relevant transactions, why each matched, and
// the updated watch state.
func ExtractBlockMatches(ctx context.Context, chain ChainSource,
	curStamp *headerfs.BlockStamp, filter *gcs.Filter,
	ws WatchState) ([]*btcutil.Tx, []TxMatch, WatchState, error) {

	block, err := chain.GetBlock(ctx, curStamp.Hash)
	if err != nil {
		return nil, nil, ws, err
	}
	if block == nil {
		return nil, nil, ws, fmt.Errorf("couldn't get block %d (%s) "+
			"from network", curStamp.Height, curStamp.Hash)
	}

	// Verify the filter against the downloaded block.
	_, err = filtermatch.VerifyBasicBlockFilter(filter, block)
	if err != nil {
		return nil, nil, ws, fmt.Errorf("error verifying filter "+
			"against block %d (%s), possibly invalid filter from "+
			"peer: %v", curStamp.Height, curStamp.Hash, err)
	}

	// The per-transaction callbacks get the same block details the
	// legacy rescan built, including the height the block was fetched
	// with.
	blockDetails := btcjson.BlockDetails{
		Height: block.Height(),
		Hash:   block.Hash().String(),
		Time:   block.MsgBlock().Header.Timestamp.Unix(),
	}

	relevantTxs := make([]*btcutil.Tx, 0, len(block.Transactions()))
	var matches []TxMatch
	for txIdx, tx := range block.Transactions() {
		spends := filtermatch.SpendsWatchedInput(tx, ws.Inputs)

		// Even though the transaction may already be known as
		// relevant, we need to check its outputs anyway, as each one
		// that pays a watched address becomes a watched input. The
		// spend check runs first, so a transaction never matches as
		// a spend of its own outputs.
		paid := filtermatch.PaysWatchedScript(tx, ws.watchesScript)
		for _, input := range paid {
			ws.Inputs = append(ws.Inputs, input)
			ws.List = append(ws.List, input.PkScript)
		}

		if !spends && len(paid) == 0 {
			continue
		}

		details := blockDetails
		details.Index = txIdx
		relevantTxs = append(relevantTxs, tx)
		matches = append(matches, TxMatch{
			Spends:  spends,
			Pays:    len(paid) > 0,
			Details: details,
		})
	}

	return relevantTxs, matches, ws, nil
}
