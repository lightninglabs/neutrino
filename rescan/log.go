package rescan

import (
	"github.com/btcsuite/btclog"
)

// log is a logger that is initialized with no output filters. This means the
// package will not perform any logging by default without the calling code
// registering a logger.
var log btclog.Logger

// The default amount of logging is none.
func init() {
	DisableLog()
}

// DisableLog disables all library log output. Logging output is disabled by
// default until UseLogger is called.
func DisableLog() {
	UseLogger(btclog.Disabled)
}

// UseLogger uses a specified Logger to output package logging info.
func UseLogger(logger btclog.Logger) {
	log = logger
}
