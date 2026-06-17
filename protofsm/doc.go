// Package protofsm contains neutrino's isolated copy of the core lnd
// protofsm state machine.
//
// The upstream lnd package also includes daemon adapters for peer sends,
// transaction broadcasts, and chain notifications. Those adapters depend on
// lnd root-module packages, so this local copy intentionally keeps only the
// deterministic state transition engine. Neutrino callers should model network
// and chain side effects as explicit outbox messages and let the owning actor
// dispatch them through the actor system.
package protofsm
