module github.com/ltcsuite/neutrino

require (
	github.com/btcsuite/btclog v0.0.0-20170628155309-84c8d2346e9f
	github.com/btcsuite/goleveldb v1.0.0 // indirect
	github.com/davecgh/go-spew v1.1.1
	github.com/lightningnetwork/lnd/queue v1.0.1
	github.com/ltcsuite/ltcd v0.0.0-20190519120615-e27ee083f08f
	github.com/ltcsuite/ltcutil v0.0.0
	github.com/ltcsuite/ltcwallet v0.0.0-20190105125346-3fa612e326e5
)

replace github.com/ltcsuite/ltcwallet => ../../ltcsuite/ltcwallet

replace github.com/ltcsuite/ltcutil => ../../ltcsuite/ltcutil

replace github.com/ltcsuite/ltcd => ../../ltcsuite/ltcd
