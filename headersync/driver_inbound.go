package headersync

// Inbound carries a message from a peer or an expired deadline that the driver
// does not interpret. It exists so the driver, and not the executor, decides
// when the message may run: the executor handles it by reading the header
// stores, and a chain store mutation in flight holds their locks. While one is
// pending the driver defers the message, and otherwise it hands the message
// straight back to the executor as a Deliver effect.
type Inbound struct {
	// Message is the executor's message, opaque to the driver.
	Message any
}

func (Inbound) driverEvent() {}

// Deliver asks the executor to handle an inbound message now. The executor
// reports what the message did to the driver as further events.
type Deliver struct {
	// Message is the executor's message, as it came in with Inbound.
	Message any
}

func (Deliver) driverEffect() {}
