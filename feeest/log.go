package feeest

import "github.com/btcsuite/btclog"

var log btclog.Logger

func init() {
	DisableLog()
}

// DisableLog disables all library log output.
func DisableLog() {
	UseLogger(btclog.Disabled)
}

// UseLogger uses a specified Logger to output package logging info.
func UseLogger(logger btclog.Logger) {
	log = logger
}
