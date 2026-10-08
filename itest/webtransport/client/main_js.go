//go:build js && wasm

package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"syscall/js"
	"time"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino"
	"github.com/lightninglabs/neutrino/internal/memstore"
	"github.com/lightninglabs/neutrino/wasmtransport"
)

const (
	operationTimeout = 60 * time.Second
	pollInterval     = 100 * time.Millisecond
)

type syncTarget struct {
	Height           int32  `json:"height"`
	BlockHash        string `json:"block_hash"`
	FilterHeaderHash string `json:"filter_header_hash"`
	FilterData       string `json:"filter_data"`
}

type browserSyncResult struct {
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
	Phase  string `json:"phase,omitempty"`

	PeerCount          int    `json:"peer_count,omitempty"`
	BlockHeaderHeight  uint32 `json:"block_header_height,omitempty"`
	BlockHeaderHash    string `json:"block_header_hash,omitempty"`
	FilterHeaderHeight uint32 `json:"filter_header_height,omitempty"`
	FilterHeaderHash   string `json:"filter_header_hash,omitempty"`
	FilterData         string `json:"filter_data,omitempty"`
	BestBlockHeight    int32  `json:"best_block_height,omitempty"`
	BestBlockHash      string `json:"best_block_hash,omitempty"`
}

type promiseResult struct {
	value js.Value
	err   error
}

func main() {
	if err := run(); err != nil {
		result := browserSyncResult{
			Status: "error",
			Phase:  "runtime",
			Error:  err.Error(),
		}
		if reportErr := report(result); reportErr != nil {
			js.Global().Get("console").Call(
				"error", err.Error()+"; report result: "+
					reportErr.Error(),
			)
		}
	}
}

func run() error {
	query := js.Global().Get("URLSearchParams").New(
		js.Global().Get("location").Get("search"),
	)
	endpoint, err := requiredQuery(query, "endpoint")
	if err != nil {
		return err
	}
	certificateHashHex, err := requiredQuery(query, "cert_hash")
	if err != nil {
		return err
	}
	peerAddress, err := requiredQuery(query, "peer_address")
	if err != nil {
		return err
	}
	certificateHash, err := hex.DecodeString(certificateHashHex)
	if err != nil {
		return fmt.Errorf("decode certificate hash: %w", err)
	}

	remote, err := net.ResolveTCPAddr("tcp", peerAddress)
	if err != nil {
		return fmt.Errorf("resolve configured Bitcoin peer: %w", err)
	}
	endpoints := wasmtransport.EndpointMap{
		remote.String(): {
			URL:                     endpoint,
			ServerCertificateHashes: [][]byte{certificateHash},
		},
	}
	dialer, err := wasmtransport.NewDialer(wasmtransport.DialerConfig{
		ResolveEndpoint:  endpoints.Resolve,
		HandshakeTimeout: 15 * time.Second,
	})
	if err != nil {
		return fmt.Errorf("create WebTransport dialer: %w", err)
	}

	params := chaincfg.SimNetParams
	memoryStores, err := memstore.New(&params)
	if err != nil {
		return fmt.Errorf("create in-memory ChainService stores: %w", err)
	}

	neutrino.UserAgentName = "neutrino-wasm-itest"
	neutrino.UserAgentVersion = "0.0.1"
	neutrino.MaxPeers = 1
	neutrino.TargetOutbound = 1
	service, err := neutrino.NewChainService(neutrino.Config{
		ChainParams:  params,
		ConnectPeers: []string{remote.String()},
		Dialer:       dialer,
		Stores: &neutrino.ChainServiceStores{
			FilterDB:         memoryStores.FilterDB,
			BlockHeaders:     memoryStores.BlockHeaders,
			RegFilterHeaders: memoryStores.RegFilterHeaders,
			BanStore:         memoryStores.BanStore,
		},
	})
	if err != nil {
		return fmt.Errorf("create ChainService: %w", err)
	}
	if err := service.Start(context.Background()); err != nil {
		return fmt.Errorf("start ChainService: %w", err)
	}
	defer service.Stop()

	initialTarget, err := fetchTarget()
	if err != nil {
		return fmt.Errorf("fetch initial btcd target: %w", err)
	}
	initialResult, err := waitForSync(
		service, initialTarget, operationTimeout,
	)
	if err != nil {
		return fmt.Errorf("initial sync: %w", err)
	}
	initialResult.Status = "ok"
	initialResult.Phase = "initial"
	if err := report(initialResult); err != nil {
		return fmt.Errorf("report initial sync: %w", err)
	}

	advancedTarget, err := waitForHigherTarget(
		initialTarget.Height, operationTimeout,
	)
	if err != nil {
		return err
	}
	advancedResult, err := waitForSync(
		service, advancedTarget, operationTimeout,
	)
	if err != nil {
		return fmt.Errorf("advanced sync: %w", err)
	}
	advancedResult.Status = "ok"
	advancedResult.Phase = "advanced"
	if err := report(advancedResult); err != nil {
		return fmt.Errorf("report advanced sync: %w", err)
	}

	return nil
}

func requiredQuery(query js.Value, key string) (string, error) {
	value := query.Call("get", key)
	if value.IsNull() || value.String() == "" {
		return "", fmt.Errorf("missing %s query parameter", key)
	}

	return value.String(), nil
}

func waitForHigherTarget(currentHeight int32,
	timeout time.Duration) (syncTarget, error) {

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		target, err := fetchTarget()
		if err != nil {
			return syncTarget{}, fmt.Errorf("fetch advanced btcd target: %w", err)
		}
		if target.Height > currentHeight {
			return target, nil
		}
		time.Sleep(pollInterval)
	}

	return syncTarget{}, fmt.Errorf(
		"timed out waiting for btcd target above height %d", currentHeight,
	)
}

func waitForSync(service *neutrino.ChainService, target syncTarget,
	timeout time.Duration) (browserSyncResult, error) {

	deadline := time.Now().Add(timeout)
	var last browserSyncResult
	for time.Now().Before(deadline) {
		blockHeader, blockHeight, err := service.BlockHeaders.ChainTip()
		if err != nil {
			return browserSyncResult{}, fmt.Errorf(
				"read block-header tip: %w", err,
			)
		}
		filterHeader, filterHeight, err :=
			service.RegFilterHeaders.ChainTip()
		if err != nil {
			return browserSyncResult{}, fmt.Errorf(
				"read filter-header tip: %w", err,
			)
		}
		bestBlock, err := service.BestBlock()
		if err != nil {
			return browserSyncResult{}, fmt.Errorf("read BestBlock: %w", err)
		}

		last = browserSyncResult{
			PeerCount:          len(service.Peers()),
			BlockHeaderHeight:  blockHeight,
			BlockHeaderHash:    blockHeader.BlockHash().String(),
			FilterHeaderHeight: filterHeight,
			FilterHeaderHash:   filterHeader.String(),
			BestBlockHeight:    bestBlock.Height,
			BestBlockHash:      bestBlock.Hash.String(),
		}
		if last.PeerCount > 0 &&
			last.BlockHeaderHeight == uint32(target.Height) &&
			last.FilterHeaderHeight == uint32(target.Height) &&
			last.BestBlockHeight == target.Height &&
			last.BlockHeaderHash == target.BlockHash &&
			last.BestBlockHash == target.BlockHash &&
			last.FilterHeaderHash == target.FilterHeaderHash {

			blockHash, err := chainhash.NewHashFromStr(target.BlockHash)
			if err != nil {
				return browserSyncResult{}, fmt.Errorf(
					"parse target block hash: %w", err,
				)
			}
			filter, err := service.GetCFilter(
				*blockHash, wire.GCSFilterRegular,
			)
			if err != nil {
				return browserSyncResult{}, fmt.Errorf(
					"fetch target compact filter: %w", err,
				)
			}
			filterData, err := filter.NBytes()
			if err != nil {
				return browserSyncResult{}, fmt.Errorf(
					"encode target compact filter: %w", err,
				)
			}
			last.FilterData = hex.EncodeToString(filterData)
			if last.FilterData != target.FilterData {
				return browserSyncResult{}, fmt.Errorf(
					"compact filter mismatch for %s", target.BlockHash,
				)
			}

			return last, nil
		}

		time.Sleep(pollInterval)
	}

	return browserSyncResult{}, fmt.Errorf(
		"timed out waiting for height=%d block=%s filter=%s; "+
			"last snapshot: peers=%d block=%d/%s filter=%d/%s best=%d/%s",
		target.Height, target.BlockHash, target.FilterHeaderHash,
		last.PeerCount, last.BlockHeaderHeight, last.BlockHeaderHash,
		last.FilterHeaderHeight, last.FilterHeaderHash,
		last.BestBlockHeight, last.BestBlockHash,
	)
}

func fetchTarget() (syncTarget, error) {
	options := js.Global().Get("Object").New()
	options.Set("cache", "no-store")
	response, err := await(
		js.Global().Call("fetch", "/target", options),
		10*time.Second,
	)
	if err != nil {
		return syncTarget{}, err
	}
	if !response.Get("ok").Bool() {
		return syncTarget{}, fmt.Errorf(
			"target request returned HTTP %d", response.Get("status").Int(),
		)
	}
	value, err := await(response.Call("json"), 10*time.Second)
	if err != nil {
		return syncTarget{}, err
	}

	return syncTarget{
		Height:           int32(value.Get("height").Int()),
		BlockHash:        value.Get("block_hash").String(),
		FilterHeaderHash: value.Get("filter_header_hash").String(),
		FilterData:       value.Get("filter_data").String(),
	}, nil
}

func report(result browserSyncResult) error {
	body, err := json.Marshal(result)
	if err != nil {
		return err
	}
	status := js.Global().Get("document").Call("getElementById", "status")
	status.Set("textContent", string(body))

	headers := js.Global().Get("Object").New()
	headers.Set("Content-Type", "application/json")
	options := js.Global().Get("Object").New()
	options.Set("method", "POST")
	options.Set("headers", headers)
	options.Set("body", string(body))
	response, err := await(
		js.Global().Call("fetch", "/result", options), 10*time.Second,
	)
	if err != nil {
		return err
	}
	if !response.Get("ok").Bool() {
		return fmt.Errorf(
			"result request returned HTTP %d", response.Get("status").Int(),
		)
	}

	return nil
}

func await(promise js.Value, timeout time.Duration) (js.Value, error) {
	results := make(chan promiseResult, 1)
	var resolve, reject js.Func
	resolve = js.FuncOf(func(_ js.Value, args []js.Value) any {
		value := js.Undefined()
		if len(args) > 0 {
			value = args[0]
		}
		results <- promiseResult{value: value}
		resolve.Release()
		reject.Release()
		return nil
	})
	reject = js.FuncOf(func(_ js.Value, args []js.Value) any {
		message := "JavaScript promise rejected"
		if len(args) > 0 {
			value := args[0]
			if value.Type() == js.TypeObject &&
				value.Get("message").Type() == js.TypeString {

				message = value.Get("message").String()
			}
		}
		results <- promiseResult{err: fmt.Errorf("%s", message)}
		resolve.Release()
		reject.Release()
		return nil
	})
	promise.Call("then", resolve, reject)

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case result := <-results:
		return result.value, result.err
	case <-timer.C:
		return js.Undefined(), fmt.Errorf("JavaScript promise timed out")
	}
}
