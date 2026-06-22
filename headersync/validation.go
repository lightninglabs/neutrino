package headersync

import (
	"errors"
	"fmt"

	"github.com/btcsuite/btcd/wire"
)

var (
	// ErrNoHeaders is returned when a peer response contains no headers for
	// a range that is expected to make progress.
	ErrNoHeaders = errors.New("no headers in range response")

	// ErrRangeStartMismatch is returned when the first returned header does
	// not connect to the range start anchor.
	ErrRangeStartMismatch = errors.New("header range start mismatch")

	// ErrRangeDisconnected is returned when the response contains an
	// internal gap.
	ErrRangeDisconnected = errors.New("header range disconnected")

	// ErrRangeStopMismatch is returned when the last returned header does
	// not match the range stop anchor.
	ErrRangeStopMismatch = errors.New("header range stop mismatch")

	// ErrTooManyHeaders is returned when a peer sends more headers than the
	// request can safely account for.
	ErrTooManyHeaders = errors.New("too many headers in response")

	// ErrRangeOvershoot is returned when a response would advance past the
	// requested stop height.
	ErrRangeOvershoot = errors.New("header range overshoot")
)

// AnchorResponse is the structurally validated result of one anchor discovery
// response. A response only becomes trusted if it lands exactly on the requested
// stop hash; otherwise it is an untrusted discovered intermediate anchor that
// needs independent confirmation.
type AnchorResponse struct {
	Height      uint32
	Hash        Hash
	Trusted     bool
	HeaderCount int
}

// ValidateHeaderRange checks the structural properties needed before a range
// can be staged as complete. Contextual proof-of-work and median-time checks
// remain the block manager's responsibility.
func ValidateHeaderRange(rng HeaderRange, headers []*wire.BlockHeader) error {
	if len(headers) == 0 {
		return ErrNoHeaders
	}

	if headers[0].PrevBlock != rng.StartHash {
		return fmt.Errorf("%w: range=%d start_height=%d",
			ErrRangeStartMismatch, rng.ID, rng.StartHeight)
	}

	for i := 1; i < len(headers); i++ {
		prevHash := headers[i-1].BlockHash()
		if headers[i].PrevBlock != prevHash {
			return fmt.Errorf("%w: range=%d index=%d",
				ErrRangeDisconnected, rng.ID, i)
		}
	}

	finalHash := headers[len(headers)-1].BlockHash()
	if finalHash != rng.StopHash {
		return fmt.Errorf("%w: range=%d stop_height=%d",
			ErrRangeStopMismatch, rng.ID, rng.StopHeight)
	}

	return nil
}

// ValidateAnchorResponse checks the structural properties of an anchor
// discovery response. It verifies that the response starts at the requested
// locator hash, is internally connected, stays within the requested span, and
// only treats the stop boundary as trusted when the final header hash matches
// the requested stop hash.
func ValidateAnchorResponse(request AnchorRequest,
	headers []*wire.BlockHeader, maxHeaders uint32) (AnchorResponse, error) {

	if len(headers) == 0 {
		return AnchorResponse{}, ErrNoHeaders
	}
	if maxHeaders > 0 && uint32(len(headers)) > maxHeaders {
		return AnchorResponse{}, fmt.Errorf("%w: got=%d max=%d",
			ErrTooManyHeaders, len(headers), maxHeaders)
	}

	if headers[0].PrevBlock != request.StartHash {
		return AnchorResponse{}, fmt.Errorf("%w: start_height=%d",
			ErrRangeStartMismatch, request.StartHeight)
	}

	for i := 1; i < len(headers); i++ {
		prevHash := headers[i-1].BlockHash()
		if headers[i].PrevBlock != prevHash {
			return AnchorResponse{}, fmt.Errorf("%w: index=%d",
				ErrRangeDisconnected, i)
		}
	}

	finalHeight := request.StartHeight + uint32(len(headers))
	trustedStop := request.StopHash != (Hash{})
	if finalHeight > request.StopHeight && trustedStop {
		return AnchorResponse{}, fmt.Errorf("%w: final=%d stop=%d",
			ErrRangeOvershoot, finalHeight, request.StopHeight)
	}

	finalHash := headers[len(headers)-1].BlockHash()
	trusted := false
	if finalHeight == request.StopHeight && trustedStop {
		if finalHash != request.StopHash {
			return AnchorResponse{}, fmt.Errorf(
				"%w: stop_height=%d", ErrRangeStopMismatch,
				request.StopHeight,
			)
		}

		trusted = true
	}

	return AnchorResponse{
		Height:      finalHeight,
		Hash:        finalHash,
		Trusted:     trusted,
		HeaderCount: len(headers),
	}, nil
}
