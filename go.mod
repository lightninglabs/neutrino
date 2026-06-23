module github.com/lightninglabs/neutrino

require (
	github.com/btcsuite/btcd v0.26.0
	github.com/btcsuite/btcd/address/v2 v2.0.0
	github.com/btcsuite/btcd/btcec/v2 v2.5.0
	github.com/btcsuite/btcd/btcutil/v2 v2.0.0
	github.com/btcsuite/btcd/chaincfg/v2 v2.0.0
	github.com/btcsuite/btcd/chainhash/v2 v2.0.0
	github.com/btcsuite/btcd/txscript/v2 v2.0.0
	github.com/btcsuite/btcd/wire/v2 v2.0.0
	github.com/btcsuite/btclog v1.0.0
	github.com/btcsuite/btcwallet/wallet/txauthor v1.4.0
	github.com/btcsuite/btcwallet/walletdb v1.6.0
	github.com/btcsuite/btcwallet/wtxmgr v1.6.0
	github.com/davecgh/go-spew v1.1.1
	github.com/lightninglabs/neutrino/cache v1.1.3
	github.com/lightningnetwork/lnd/queue v1.0.1
	github.com/stretchr/testify v1.10.0
	golang.org/x/exp v0.0.0-20250811191247-51f88131bc50
	pgregory.net/rapid v1.2.0
)

require (
	github.com/aead/siphash v1.0.1 // indirect
	github.com/btcsuite/btcd/v2transport v1.0.1 // indirect
	github.com/btcsuite/btcwallet v0.16.18 // indirect
	github.com/btcsuite/btcwallet/wallet/txrules v1.3.0 // indirect
	github.com/btcsuite/btcwallet/wallet/txsizes v1.3.0 // indirect
	github.com/btcsuite/go-socks v0.0.0-20170105172521-4720035b7bfd // indirect
	github.com/btcsuite/websocket v0.0.0-20150119174127-31079b680792 // indirect
	github.com/decred/dcrd/crypto/blake256 v1.1.0 // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.4.0 // indirect
	github.com/decred/dcrd/lru v1.1.3 // indirect
	github.com/kcalvinalvin/anet v0.0.0-20251112173137-d8ddc1f6dbee // indirect
	github.com/kkdai/bstream v1.0.0 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/lightningnetwork/lnd/clock v1.0.1 // indirect
	github.com/lightningnetwork/lnd/fn/v2 v2.0.8 // indirect
	github.com/lightningnetwork/lnd/ticker v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	go.etcd.io/bbolt v1.3.11 // indirect
	golang.org/x/crypto v0.41.0 // indirect
	golang.org/x/sync v0.16.0 // indirect
	golang.org/x/sys v0.35.0 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

go 1.25.11

replace github.com/lightninglabs/neutrino/cache => ./cache
