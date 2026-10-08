package chainsync

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/btcsuite/btcd/chaincfg/v2"
)

// ErrInvalidRetargetParams is returned when the chain parameters cannot be
// used for difficulty retarget validation.
var ErrInvalidRetargetParams = errors.New("invalid retarget parameters")

// Retarget contains the derived values used by header difficulty validation.
// Timespans are expressed in whole seconds so that live synchronization and
// header import always agree on the retarget interval.
type Retarget struct {
	// BlocksPerRetarget is the number of blocks between difficulty
	// retargets.
	BlocksPerRetarget int32

	// MinRetargetTimespan is the smallest actual timespan, in seconds, a
	// retarget window is clamped to.
	MinRetargetTimespan int64

	// MaxRetargetTimespan is the largest actual timespan, in seconds, a
	// retarget window is clamped to.
	MaxRetargetTimespan int64
}

// NewRetarget validates the retarget related chain parameters and derives the
// values used by header difficulty validation using integer seconds.
func NewRetarget(params chaincfg.Params) (Retarget, error) {
	if err := validateParams(params); err != nil {
		return Retarget{}, err
	}

	timespan := int64(params.TargetTimespan / time.Second)
	interval := int64(params.TargetTimePerBlock / time.Second)
	factor := params.RetargetAdjustmentFactor

	return Retarget{
		BlocksPerRetarget:   int32(timespan / interval),
		MinRetargetTimespan: timespan / factor,
		MaxRetargetTimespan: timespan * factor,
	}, nil
}

// validateParams checks the retarget parameters and makes sure the values
// derived from them cannot overflow the fields of Retarget.
func validateParams(params chaincfg.Params) error {
	interval := params.TargetTimePerBlock
	timespan := params.TargetTimespan
	factor := params.RetargetAdjustmentFactor

	if interval < time.Second {
		return fmt.Errorf("%w: block interval %v is below one second",
			ErrInvalidRetargetParams, interval)
	}

	// Both sync paths derive the retarget interval in whole seconds, so a
	// fractional interval would be truncated inconsistently.
	if interval%time.Second != 0 {
		return fmt.Errorf("%w: block interval %v is not whole seconds",
			ErrInvalidRetargetParams, interval)
	}

	if timespan < time.Second {
		return fmt.Errorf("%w: timespan %v is below one second",
			ErrInvalidRetargetParams, timespan)
	}

	if timespan%time.Second != 0 {
		return fmt.Errorf("%w: timespan %v is not whole seconds",
			ErrInvalidRetargetParams, timespan)
	}

	if interval > timespan {
		return fmt.Errorf("%w: block interval %v exceeds timespan %v",
			ErrInvalidRetargetParams, interval, timespan)
	}

	if factor <= 0 {
		return fmt.Errorf("%w: adjustment factor %d is not positive",
			ErrInvalidRetargetParams, factor)
	}

	// The derived values are stored as int32 and int64 respectively, so
	// make sure the validated inputs cannot overflow them.
	seconds := int64(timespan / time.Second)
	blocks := seconds / int64(interval/time.Second)
	if blocks > math.MaxInt32 {
		return fmt.Errorf("%w: %d blocks per retarget exceeds int32",
			ErrInvalidRetargetParams, blocks)
	}

	if seconds/factor == 0 {
		return fmt.Errorf("%w: minimum retarget timespan is zero",
			ErrInvalidRetargetParams)
	}

	if seconds > math.MaxInt64/factor {
		return fmt.Errorf("%w: maximum retarget timespan overflows",
			ErrInvalidRetargetParams)
	}

	return nil
}
