package neutrino

import "github.com/lightninglabs/neutrino/headersync"

// handleDriverEvent hands one event to the header sync driver and executes
// the effects it returns, in order. The block manager is the driver's
// executor: it owns peer I/O, timers and the chain store, and reports the
// outcome of an effect that can fail back as a further event. It must be
// called from the block handler's goroutine, which serializes the driver.
func (b *blockManager) handleDriverEvent(
	event headersync.DriverEvent) headersync.DriverResult {

	ctx, cancel := b.headerSyncOpCtx()
	defer cancel()

	result, err := b.headerSync.Driver().Handle(ctx, event)
	if err != nil {
		log.Warnf("headersync driver failed to handle %T: %v", event,
			err)
	}
	for _, effect := range result.Effects {
		b.executeDriverEffect(effect)
	}

	return result
}

// executeDriverEffect performs one driver effect.
func (b *blockManager) executeDriverEffect(effect headersync.DriverEffect) {
	switch effect := effect.(type) {
	case headersync.ArmCooldownWake:
		b.armHeaderCooldownWake(effect.Until)

	default:
		log.Warnf("headersync driver emitted unknown effect %T", effect)
	}
}
