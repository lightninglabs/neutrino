package rescan

import (
	"bytes"
	"fmt"

	"github.com/btcsuite/btcd/address/v2"

	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/btcutil/v2/gcs"
	"github.com/btcsuite/btcd/btcutil/v2/gcs/builder"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/txscript/v2"
	"github.com/btcsuite/btcd/wire/v2"
	neutrino "github.com/lightninglabs/neutrino"
	"github.com/lightninglabs/neutrino/headerfs"
)

// WatchState holds the mutable watch list state that is carried within FSM
// state structs. This groups the watch-related fields that need to travel
// together during state transitions.
type WatchState struct {
	// Addrs is the set of addresses being watched for incoming payments.
	Addrs []address.Address

	// Inputs is the set of outpoints being watched for spends.
	Inputs []neutrino.InputWithScript

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
		Addrs: make([]address.Address, len(ws.Addrs)),
		Inputs: make(
			[]neutrino.InputWithScript,
			len(ws.Inputs),
		),
		List: make([][]byte, len(ws.List)),
	}
	copy(clone.Addrs, ws.Addrs)
	clone.Inputs = copyInputsWithScripts(ws.Inputs)
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
// everything to the appropriate slices. Returns a new WatchState — the
// original is not modified.
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

// MatchBlockFilter checks whether the block filter matches any items in the
// watch list. If this returns false, the block is certainly not interesting
// to us.
func MatchBlockFilter(
	filter *gcs.Filter,
	blockHash *chainhash.Hash,
	watchList [][]byte,
) (bool, error) {

	key := builder.DeriveKey(blockHash)
	matched, err := filter.MatchAny(key, watchList)
	if err != nil {
		return false, err
	}

	return matched, nil
}

// FetchAndMatchFilter fetches the compact filter for the given block hash
// and checks it against the watch list. Returns the match result and the
// filter (for potential later use in block extraction). Uses optimistic
// batch queries for performance during historical catch-up.
func FetchAndMatchFilter(
	chain neutrino.ChainSource,
	blockHash *chainhash.Hash,
	watchList [][]byte,
	queryOpts ...neutrino.QueryOption,
) (bool, *gcs.Filter, error) {

	opts := append(
		[]neutrino.QueryOption{
			neutrino.OptimisticBatch(),
		},
		queryOpts...)

	filter, err := chain.GetCFilter(
		*blockHash, wire.GCSFilterRegular, opts...,
	)
	if err != nil {
		return false, nil, err
	}

	// If the filter is empty, no match is possible.
	if filter.N() == 0 {
		return false, nil, nil
	}

	matched, err := MatchBlockFilter(
		filter,
		blockHash,
		watchList,
	)

	return matched, filter, err
}

// ExtractBlockMatches fetches the target block from the network and filters
// out any relevant transactions. It also updates the provided WatchState
// with new inputs discovered from matching outputs (so future spends are
// also watched). Returns the relevant transactions and the updated watch
// state.
func ExtractBlockMatches(
	chain neutrino.ChainSource,
	curStamp *headerfs.BlockStamp,
	filter *gcs.Filter,
	ws WatchState,
	queryOpts ...neutrino.QueryOption,
) ([]*btcutil.Tx, WatchState, error) {

	ws = ws.Clone()
	block, err := chain.GetBlock(curStamp.Hash, queryOpts...)
	if err != nil {
		return nil, ws, err
	}
	if block == nil {
		return nil, ws, fmt.Errorf(
			"couldn't get block %d (%s) from "+
				"network",
			curStamp.Height,
			curStamp.Hash,
		)
	}

	if err := ws.cacheAddrScripts(); err != nil {
		return nil, ws, err
	}

	// Verify the filter against the downloaded block.
	_, err = neutrino.VerifyBasicBlockFilter(filter, block)
	if err != nil {
		return nil, ws, fmt.Errorf(
			"error verifying filter against "+
				"block %d (%s), possibly invalid "+
				"filter from peer: %v",
			curStamp.Height,
			curStamp.Hash,
			err,
		)
	}

	relevantTxs := make(
		[]*btcutil.Tx,
		0,
		len(block.Transactions()),
	)
	for _, tx := range block.Transactions() {
		var relevant bool

		if SpendsWatchedInput(tx, ws.Inputs) {
			relevant = true
		}

		// Even though the transaction may already be known as
		// relevant, we need to call paysWatchedScripts anyway as it
		// discovers new inputs to watch.
		pays, newInputs := paysWatchedScripts(
			tx,
			ws.addrScripts,
		)

		if pays {
			relevant = true
		}

		// Add any new inputs discovered from matching outputs.
		if len(newInputs) > 0 {
			ws.Inputs = append(
				ws.Inputs,
				newInputs...)
			for _, input := range newInputs {
				ws.List = append(
					ws.List,
					input.PkScript,
				)
			}
		}

		if relevant {
			relevantTxs = append(relevantTxs, tx)
		}
	}

	return relevantTxs, ws, nil
}

// SpendsWatchedInput returns whether the transaction spends any of the
// watched inputs.
func SpendsWatchedInput(tx *btcutil.Tx,
	watchInputs []neutrino.InputWithScript) bool {

	// zeroOutPoint indicates we should match on the output script being
	// spent rather than the outpoint itself.
	var zeroOutPoint wire.OutPoint

	for _, in := range tx.MsgTx().TxIn {
		for _, input := range watchInputs {
			switch {
			case input.OutPoint == zeroOutPoint:
				pkScript, err := txscript.ComputePkScript(
					in.SignatureScript,
					in.Witness,
				)
				if err != nil {
					continue
				}

				if bytes.Equal(
					pkScript.Script(),
					input.PkScript,
				) {

					return true
				}

			case in.PreviousOutPoint == input.OutPoint:
				return true
			}
		}
	}

	return false
}

// paysWatchedScripts returns whether the transaction has an output paying to
// one of the watched scripts. It also returns an InputWithScript for each
// matching output, so that future spends of these outputs are also watched.
func paysWatchedScripts(
	tx *btcutil.Tx,
	scripts map[string]struct{},
) (bool, []neutrino.InputWithScript) {

	var newInputs []neutrino.InputWithScript
	for outIdx, out := range tx.MsgTx().TxOut {
		if _, ok := scripts[string(out.PkScript)]; !ok {
			continue
		}
		newInputs = append(
			newInputs,
			neutrino.InputWithScript{
				PkScript: out.PkScript,
				OutPoint: wire.OutPoint{
					Hash:  *tx.Hash(),
					Index: uint32(outIdx),
				},
			},
		)
	}
	return len(newInputs) > 0, newInputs
}
