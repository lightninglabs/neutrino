module github.com/lightninglabs/neutrino

require (
	github.com/btcsuite/btcd v0.23.5-0.20230711222809-7faa9b266231
	github.com/btcsuite/btcd/btcec/v2 v2.1.3
	github.com/btcsuite/btcd/btcutil v1.1.1
	github.com/btcsuite/btcd/chaincfg/chainhash v1.0.1
	github.com/btcsuite/btclog v0.0.0-20170628155309-84c8d2346e9f
	github.com/btcsuite/btcwallet/wallet/txauthor v1.2.3
	github.com/btcsuite/btcwallet/walletdb v1.4.0
	github.com/btcsuite/btcwallet/wtxmgr v1.5.0
	github.com/davecgh/go-spew v1.1.1
	github.com/lightninglabs/neutrino/cache v1.1.0
	github.com/lightningnetwork/lnd/queue v1.0.1
	github.com/linden/tempdb v0.0.0-20240218031655-83bc03e79f51
	github.com/stretchr/testify v1.8.1
)

require (
	github.com/aead/siphash v1.0.1 // indirect
	github.com/btcsuite/btcwallet/wallet/txrules v1.2.0 // indirect
	github.com/btcsuite/btcwallet/wallet/txsizes v1.1.0 // indirect
	github.com/btcsuite/go-socks v0.0.0-20170105172521-4720035b7bfd // indirect
	github.com/btcsuite/websocket v0.0.0-20150119174127-31079b680792 // indirect
	github.com/decred/dcrd/crypto/blake256 v1.0.0 // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.0.1 // indirect
	github.com/decred/dcrd/lru v1.0.0 // indirect
	github.com/kkdai/bstream v0.0.0-20161212061736-f391b8402d23 // indirect
	github.com/lightningnetwork/lnd/clock v1.0.1 // indirect
	github.com/lightningnetwork/lnd/ticker v1.0.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/crypto v0.1.0 // indirect
	golang.org/x/sys v0.1.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

go 1.21.2

toolchain go1.21.5
