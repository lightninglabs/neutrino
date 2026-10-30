//go:build !js

package sqldb

// The legacy import opens the old neutrino.db through walletdb's "bdb"
// driver, which is backed by bbolt. bbolt does not build for js/wasm, so the
// driver is only registered on other platforms. On js/wasm, opening a legacy
// database fails with walletdb's unknown driver error instead.
import _ "github.com/btcsuite/btcwallet/walletdb/bdb"
