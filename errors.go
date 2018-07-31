package neutrino

import "errors"

var (
	// ErrShuttingDown signals that neutrino received a shutdown request.
	ErrShuttingDown = errors.New("neutrino shutting down")
)
