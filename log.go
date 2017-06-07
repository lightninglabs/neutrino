package neutrino

import (
	"github.com/btcsuite/btclog"
	"github.com/roasbeef/btcd/addrmgr"
	"github.com/roasbeef/btcd/blockchain"
	"github.com/roasbeef/btcd/peer"
	"github.com/roasbeef/btcd/txscript"
)

// log is a logger that is initialized with no output filters.  This
// means the package will not perform any logging by default until the caller
// requests it.
var log btclog.Logger

// The default amount of logging is none.
func init() {
	DisableLog()
}

// DisableLog disables all library log output.  Logging output is disabled
// by default until either UseLogger or SetLogWriter are called.
func DisableLog() {
	log = btclog.Disabled
}

// UseLogger uses a specified Logger to output package logging info.
// This should be used in preference to SetLogWriter if the caller is also
// using btclog.
func UseLogger(logger btclog.Logger) {
	log = logger
	blockchain.UseLogger(logger)
	txscript.UseLogger(logger)
	peer.UseLogger(logger)
	addrmgr.UseLogger(logger)
}
