package chainsync

import (
	"fmt"
	"sort"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
)

// ErrCheckpointMismatch is returned if given filter headers don't pass our
// control check.
var ErrCheckpointMismatch = fmt.Errorf("checkpoint doesn't match")

// filterHeaderCheckpoints holds a mapping from heights to filter headers for
// various heights. We use them to check whether peers are serving us the
// expected filter headers.
var filterHeaderCheckpoints = map[wire.BitcoinNet]map[uint32]*chainhash.Hash{
	// Mainnet filter header checkpoints.
	chaincfg.MainNetParams.Net: {
		100000: hashFromStr("f28cbc1ab369eb01b7b5fe8bf59763abb73a31471fe404a26a06be4153aa7fa5"),
		200000: hashFromStr("e5031471732f4fbfe7a25f6a03acc1413300d5c56ae8e06b95046b8e4c0f32b3"),
		300000: hashFromStr("1bd50220fcdde929ca3143c91d2dd9a9bfedb38c452ba98dbb51e719bff8aa5b"),
		400000: hashFromStr("5d973ab1f1c569c70deec1c1a8fb2e317a260f1656edb3b262c65f78ef192e3a"),
		500000: hashFromStr("5d16ca293c9bdc0a9bc279b63f99fb661be38b095a59a44200a807caaa631a3c"),
		600000: hashFromStr("bde0854d0b2f4386a860462547140e0c6817f5b4b2ab515ef70e204e377598f8"),
		660000: hashFromStr("08312375fabc082b17fa8ee88443feb350c19a34bb7483f94f7478fa4ad33032"),
	},

	// Testnet filter header checkpoints.
	chaincfg.TestNet3Params.Net: {
		100000:  hashFromStr("97c0633f14625627fcd133250ad8cc525937e776b5f3fd272b06d02c58b65a1c"),
		200000:  hashFromStr("51aa817e5abe3acdcf103616b1a5736caf84bc3773a7286e9081108ecc38cc87"),
		400000:  hashFromStr("4aab9b3d4312cd85cfcd48a08b36c4402bfdc1e8395dcf4236c3029dfa837c48"),
		600000:  hashFromStr("713d9c9198e2dba0739e85aab6875cb951c36297b95a2d51131aa6919753b55d"),
		800000:  hashFromStr("0dafdff27269a70293c120b14b1f5e9a72a5e8688098cfc6140b9d64f8325b99"),
		1000000: hashFromStr("c2043fa2f6eb5f8f8d2c5584f743187f36302ed86b62c302e31155f378da9c5f"),
		1400000: hashFromStr("f9ae1750483d4c8ce82512616b1ded932886af46decb8d3e575907930542d9b3"),
		1500000: hashFromStr("dc0cfa13daf09df9b8dbe7532f75ebdb4255860b295016b2ca4b789394bc5090"),
		1800000: hashFromStr("67083b2d5dfc9ca1415bffa14e43a5bbe595e2e8b7ffbcc7a4ea78fa069a9c8d"),
		1900000: hashFromStr("96a31467f9edcaa3297770bc6cdf66926d5d17dfad70cb0cac285bfe9075c494"),
	},
}

// ControlCFHeader controls the given filter header against our list of
// checkpoints. It returns ErrCheckpointMismatch if we have a checkpoint at the
// given height, and it doesn't match.
func ControlCFHeader(params chaincfg.Params, fType wire.FilterType,
	height uint32, filterHeader *chainhash.Hash) error {

	if fType != wire.GCSFilterRegular {
		return fmt.Errorf("unsupported filter type %v", fType)
	}

	control, ok := filterHeaderCheckpoints[params.Net]
	if !ok {
		return nil
	}

	hash, ok := control[height]
	if !ok {
		return nil
	}

	if *filterHeader != *hash {
		return ErrCheckpointMismatch
	}

	return nil
}

// FetchHardCodedFilterHeaderCheckpts fetches the hardcoded filter header
// checkpoint for the passed bitcoin netwwork.
func FetchHardCodedFilterHeaderCheckpts(net wire.BitcoinNet) map[uint32]*chainhash.Hash {

	return filterHeaderCheckpoints[net]
}

// FetchHardCodedFilterHeaderCheckHeight fetches the hardcoded filter header
// checkpoint heights for the passed bitcoin netwwork.
func FetchHardCodedFilterHdrCheckptHeight(net wire.BitcoinNet) []uint32 {
	heights := make([]uint32, 0, len(filterHeaderCheckpoints[net]))

	for height, _ := range filterHeaderCheckpoints[net] {
		heights = append(heights, height)
	}
	sort.Slice(heights, func(i, j int) bool {
		return heights[j] > heights[i]
	})

	return heights
}

// hashFromStr makes a chainhash.Hash from a valid hex string. If the string is
// invalid, a nil pointer will be returned.
func hashFromStr(hexStr string) *chainhash.Hash {
	hash, _ := chainhash.NewHashFromStr(hexStr)
	return hash
}
