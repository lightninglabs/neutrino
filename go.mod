module github.com/lightninglabs/neutrino

require (
	github.com/boltdb/bolt v1.3.1 // indirect
	github.com/btcsuite/btcd v0.0.0-20180824064422-7d2daa5bfef28c5e282571bc06416516936115ee
	github.com/btcsuite/btclog v0.0.0-20170628155309-84c8d2346e9f
	github.com/btcsuite/btcutil v0.0.0-20180706230648-ab6388e0c60ae4834a1f57511e20c17b5f78be4b
	github.com/btcsuite/btcwallet v0.0.0-20180904010540-284e2e0e696e33d5be388f7f3d9a26db703e0c06
	github.com/davecgh/go-spew v1.1.1
	go.etcd.io/bbolt v1.3.0 // indirect
)

replace github.com/btcsuite/btcd => github.com/wpaulino/btcd v0.0.0-20190112023042-f6eae62a7710

replace github.com/btcsuite/btcwallet => github.com/wpaulino/btcwallet v0.0.0-20190112033209-311af2782058
