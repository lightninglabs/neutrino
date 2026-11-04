// The blockdn example runs a persistent Neutrino chain service using HTTPS
// instead of Bitcoin P2P connections.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino"
	"github.com/lightninglabs/neutrino/blockdn"
	"github.com/lightninglabs/neutrino/sqldb"
	sqldbv2 "github.com/lightningnetwork/lnd/sqldb/v2"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() (runErr error) {
	network := flag.String(
		"network",
		"signet",
		"Bitcoin network: mainnet, signet, testnet3, testnet4",
	)
	baseURL := flag.String(
		"url",
		"",
		"block-dn URL; defaults to the network's public instance",
	)
	dataDir := flag.String(
		"datadir",
		"neutrino-data",
		"data directory (network subdirectory is added)",
	)
	witness := flag.String(
		"witness-url",
		"",
		"optional independent block-dn server to cross-check",
	)
	poll := flag.Duration("poll", 15*time.Second, "tip polling interval")
	once := flag.Bool("once", false, "exit after catching up to the server")
	verifyTip := flag.Bool(
		"verify-tip", false,
		"fetch and validate the tip compact filter and full block after sync",
	)
	timeout := flag.Duration(
		"timeout",
		0,
		"stop after this duration; zero runs until interrupted",
	)
	flag.Parse()
	params, err := networkParams(*network)
	if err != nil {
		return err
	}
	if *baseURL == "" {
		*baseURL = defaultURL(*network)
	}
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()
	if *timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *timeout)
		defer cancel()
	}
	// Fail immediately on a wrong-network server rather than creating state
	// or waiting indefinitely for the background retry loop.
	client, err := blockdn.NewClient(*baseURL, nil)
	if err != nil {
		return err
	}
	client = client.ForNetwork(*params.GenesisHash)
	status, err := client.Status(ctx)
	if err != nil {
		return err
	}
	if status.GenesisHash != *params.GenesisHash {
		return fmt.Errorf(
			"%s serves genesis %s; %s requires %s; use -url",
			*baseURL,
			status.GenesisHash,
			*network,
			params.GenesisHash,
		)
	}
	var witnesses []string
	if *witness != "" {
		witnesses = []string{*witness}
	}
	dir := filepath.Join(*dataDir, *network)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	service, err := neutrino.NewChainService(neutrino.Config{
		DataDir:       dir,
		ChainParams:   params,
		PersistToDisk: true,
		SQLConfig: &sqldb.Config{
			Backend: sqldb.BackendSqlite,
			Sqlite:  &sqldbv2.SqliteConfig{},
		},
		BlockDN: &neutrino.BlockDNConfig{
			BaseURL:      *baseURL,
			PollInterval: *poll,
			WitnessURLs:  witnesses,
		},
		// These guards catch accidental P2P use if the example changes.
		Dialer: func(net.Addr) (net.Conn, error) {
			return nil, neutrino.ErrP2PDisabled
		},
		NameResolver: func(string) ([]net.IP, error) {
			return nil, neutrino.ErrP2PDisabled
		},
	})
	if err != nil {
		return err
	}
	defer func() {
		if err := service.Stop(); runErr == nil {
			runErr = err
		}
	}()
	if err := service.Start(ctx); err != nil {
		return err
	}
	log.Printf(
		"syncing %s from %s; SQLite: %s",
		*network,
		*baseURL,
		filepath.Join(dir, sqldb.DefaultSqliteFilename),
	)
	log.Print(
		"server supplies tip freshness and filter commitments; " +
			"Bitcoin P2P is disabled",
	)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var lastError string
	var verifiedHash chainhash.Hash
	for {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				return ctx.Err()
			}
			return nil
		case <-ticker.C:
			progress, err := service.BlockDNStatus()
			if err != nil {
				return err
			}
			log.Printf(
				"headers=%d target=%d current=%t hash=%s",
				progress.Height,
				progress.TargetHeight,
				progress.Current,
				progress.Hash,
			)
			if progress.LastError != nil &&
				progress.LastError.Error() != lastError {
				lastError = progress.LastError.Error()
				log.Printf("sync retry: %s", lastError)
			}
			if progress.Current {
				lastError = ""
				if *verifyTip && verifiedHash != progress.Hash {
					_, err := service.GetCFilter(
						progress.Hash, wire.GCSFilterRegular,
					)
					if err != nil {
						return fmt.Errorf("tip filter: %w", err)
					}
					block, err := service.GetBlock(progress.Hash)
					if err != nil {
						return fmt.Errorf("tip block: %w", err)
					}
					verifiedHash = progress.Hash
					log.Printf(
						"verified tip filter and block: "+
							"height=%d hash=%s transactions=%d",
						progress.Height, progress.Hash,
						len(block.Transactions()),
					)
				}
				if *once {
					return nil
				}
			}
		}
	}
}

func networkParams(network string) (chaincfg.Params, error) {
	switch network {
	case "mainnet":
		return chaincfg.MainNetParams, nil
	case "signet":
		return chaincfg.SigNetParams, nil
	case "testnet3":
		return chaincfg.TestNet3Params, nil
	case "testnet4":
		return chaincfg.TestNet4Params, nil
	default:
		return chaincfg.Params{}, fmt.Errorf(
			"unknown network %q; "+
				"use mainnet, signet, testnet3 or testnet4",
			network,
		)
	}
}

func defaultURL(network string) string {
	if network == "mainnet" {
		return "https://block-dn.org"
	}
	return "https://" + network + ".block-dn.org"
}
