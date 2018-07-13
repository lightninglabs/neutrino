package neutrino

import (
	"io/ioutil"
	"os"
	"sort"
	"testing"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil/gcs/builder"
	"github.com/btcsuite/btcwallet/walletdb"
	"github.com/lightninglabs/neutrino/headerfs"
)

func decodeHashNoError(str string) *chainhash.Hash {
	hash, err := chainhash.NewHashFromStr(str)
	if err != nil {
		panic("Got error decoding hash: " + err.Error())
	}
	return hash
}

type cfCheckptTestCase struct {
	name           string
	checkpoints    map[string][]*chainhash.Hash
	storepoints    []*chainhash.Hash
	storeAddHeight int
	heightDiff     int
}

type checkCFHTestCase struct {
	name     string
	headers  map[string]*wire.MsgCFHeaders
	idx      int
	mismatch bool
}

type resolveCFHTestCase struct {
	name     string
	block    *wire.MsgBlock
	idx      int
	headers  map[string]*wire.MsgCFHeaders
	badPeers []string
}

var (
	checkpoints1 = []*chainhash.Hash{
		decodeHashNoError("01234567890abcdeffedcba09f76543210"),
	}
	checkpoints2 = []*chainhash.Hash{
		decodeHashNoError("01234567890abcdeffedcba09f76543210"),
		decodeHashNoError("fedcba09f7654321001234567890abcdef"),
	}
	checkpoints3 = []*chainhash.Hash{
		decodeHashNoError("fedcba09f7654321001234567890abcdef"),
	}
	checkpoints4 = []*chainhash.Hash{
		decodeHashNoError("fedcba09f7654321001234567890abcdef"),
		decodeHashNoError("01234567890abcdeffedcba09f76543210"),
	}
	checkpoints5 = []*chainhash.Hash{
		decodeHashNoError("fedcba09f7654321001234567890abcdef"),
		decodeHashNoError("fedcba09f7654321001234567890abcdef"),
	}

	block = &wire.MsgBlock{}

	headers1 = &wire.MsgCFHeaders{
		FilterHashes: []*chainhash.Hash{
			decodeHashNoError("01234567890abcdeffedcba09f76543210"),
			decodeHashNoError("fedcba09f7654321001234567890abcdef"),
		},
	}
	headers2 = &wire.MsgCFHeaders{
		FilterHashes: []*chainhash.Hash{
			decodeHashNoError("01234567890abcdeffedcba09f76543210"),
		},
	}
	headers3 = &wire.MsgCFHeaders{
		FilterHashes: []*chainhash.Hash{
			decodeHashNoError("fedcba09f7654321001234567890abcdef"),
			decodeHashNoError("01234567890abcdeffedcba09f76543210"),
		},
	}
	headers4 = func() *wire.MsgCFHeaders {
		cfh := &wire.MsgCFHeaders{
			FilterHashes: []*chainhash.Hash{
				decodeHashNoError("fedcba09f7654321001234567890abcdef"),
			},
		}
		filter, _ := builder.BuildExtFilter(block)
		filterHash, _ := builder.GetFilterHash(filter)
		cfh.FilterHashes = append(cfh.FilterHashes, &filterHash)
		return cfh
	}()

	cfCheckptTestCases = []*cfCheckptTestCase{
		{
			name: "all match 1",
			checkpoints: map[string][]*chainhash.Hash{
				"1": checkpoints1,
				"2": checkpoints1,
			},
			storepoints:    checkpoints1,
			storeAddHeight: 0,
			heightDiff:     -1,
		},
		{
			name: "all match 2",
			checkpoints: map[string][]*chainhash.Hash{
				"1": checkpoints2,
				"2": checkpoints2,
			},
			storepoints:    checkpoints2,
			storeAddHeight: 0,
			heightDiff:     -1,
		},
		{
			name: "all match 3",
			checkpoints: map[string][]*chainhash.Hash{
				"1": checkpoints1,
				"2": checkpoints2,
			},
			storepoints:    checkpoints1,
			storeAddHeight: 0,
			heightDiff:     -1,
		},
		{
			name: "mismatch 1",
			checkpoints: map[string][]*chainhash.Hash{
				"1": checkpoints4,
				"2": checkpoints2,
			},
			storepoints:    checkpoints2,
			storeAddHeight: 0,
			heightDiff:     0,
		},
		{
			name: "mismatch 2",
			checkpoints: map[string][]*chainhash.Hash{
				"1": checkpoints4,
				"2": checkpoints2,
			},
			storepoints:    checkpoints4,
			storeAddHeight: 0,
			heightDiff:     0,
		},
		{
			name: "mismatch 3",
			checkpoints: map[string][]*chainhash.Hash{
				"1": checkpoints4,
				"2": checkpoints2,
			},
			storepoints:    checkpoints1,
			storeAddHeight: 0,
			heightDiff:     0,
		},
		{
			name: "mismatch 4",
			checkpoints: map[string][]*chainhash.Hash{
				"1": checkpoints4,
				"2": checkpoints2,
			},
			storepoints:    checkpoints3,
			storeAddHeight: 0,
			heightDiff:     0,
		},
		{
			name: "mismatch 5",
			checkpoints: map[string][]*chainhash.Hash{
				"1": checkpoints4,
				"2": checkpoints5,
			},
			storepoints:    checkpoints4,
			storeAddHeight: 0,
			heightDiff:     1,
		},
		{
			name: "mismatch 6",
			checkpoints: map[string][]*chainhash.Hash{
				"1": checkpoints2,
				"2": checkpoints4,
			},
			storepoints:    checkpoints3,
			storeAddHeight: 0,
			heightDiff:     0,
		},
	}

	checkCFHTestCases = []*checkCFHTestCase{
		{
			name: "match 1",
			headers: map[string]*wire.MsgCFHeaders{
				"a": headers1,
				"b": headers1,
			},
			idx:      0,
			mismatch: false,
		},
		{
			name: "match 2",
			headers: map[string]*wire.MsgCFHeaders{
				"a": headers1,
				"b": headers2,
			},
			idx:      0,
			mismatch: false,
		},
		{
			name: "match 3",
			headers: map[string]*wire.MsgCFHeaders{
				"a": headers1,
				"b": headers2,
			},
			idx:      1,
			mismatch: false,
		},
		{
			name: "match 4",
			headers: map[string]*wire.MsgCFHeaders{
				"a": headers2,
				"b": headers3,
			},
			idx:      1,
			mismatch: false,
		},
		{
			name: "mismatch 1",
			headers: map[string]*wire.MsgCFHeaders{
				"a": headers1,
				"b": headers3,
			},
			idx:      0,
			mismatch: true,
		},
		{
			name: "mismatch 2",
			headers: map[string]*wire.MsgCFHeaders{
				"a": headers1,
				"b": headers3,
			},
			idx:      1,
			mismatch: true,
		},
		{
			name: "mismatch 3",
			headers: map[string]*wire.MsgCFHeaders{
				"a": headers2,
				"b": headers3,
			},
			idx:      0,
			mismatch: true,
		},
	}

	resolveCFHTestCases = []*resolveCFHTestCase{
		{
			name:  "all bad 1",
			block: block,
			headers: map[string]*wire.MsgCFHeaders{
				"a": headers1,
				"b": headers1,
			},
			idx:      0,
			badPeers: []string{"a", "b"},
		},
		{
			name:  "all bad 2",
			block: block,
			headers: map[string]*wire.MsgCFHeaders{
				"a": headers2,
				"b": headers2,
			},
			idx:      0,
			badPeers: []string{"a", "b"},
		},
		{
			name:  "all bad 3",
			block: block,
			headers: map[string]*wire.MsgCFHeaders{
				"a": headers2,
				"b": headers2,
			},
			idx:      0,
			badPeers: []string{"a", "b"},
		},
		{
			name:  "all bad 4",
			block: block,
			headers: map[string]*wire.MsgCFHeaders{
				"a": headers1,
				"b": headers3,
			},
			idx:      0,
			badPeers: []string{"a", "b"},
		},
		{
			name:  "all bad 5",
			block: block,
			headers: map[string]*wire.MsgCFHeaders{
				"a": headers1,
				"b": headers3,
			},
			idx:      1,
			badPeers: []string{"a", "b"},
		},
		{
			name:  "one good",
			block: block,
			headers: map[string]*wire.MsgCFHeaders{
				"a": headers4,
				"b": headers3,
				"c": headers1,
			},
			idx:      1,
			badPeers: []string{"b", "c"},
		},
		{
			name:  "all good",
			block: block,
			headers: map[string]*wire.MsgCFHeaders{
				"a": headers4,
				"b": headers4,
			},
			idx:      1,
			badPeers: []string{},
		},
	}
)

func heightToHeader(height uint32) *wire.BlockHeader {
	header := &wire.BlockHeader{Nonce: height}
	return header
}

func runCheckCFCheckptSanityTestCase(t *testing.T, testCase *cfCheckptTestCase) {
	tempDir, err := ioutil.TempDir("", "neutrino")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %s", err)
	}
	defer os.RemoveAll(tempDir)
	db, err := walletdb.Create("bdb", tempDir+"/weks.db")
	if err != nil {
		t.Fatalf("Error opening DB: %s", err)
	}
	defer db.Close()
	hdrStore, err := headerfs.NewBlockHeaderStore(tempDir, db,
		&chaincfg.SimNetParams)
	if err != nil {
		t.Fatalf("Error creating block header store: %s", err)
	}
	cfStore, err := headerfs.NewFilterHeaderStore(tempDir, db,
		headerfs.ExtendedFilter, &chaincfg.SimNetParams)
	if err != nil {
		t.Fatalf("Error creating filter header store: %s", err)
	}
	var (
		height uint32
		header *wire.BlockHeader
	)
	for i, point := range testCase.storepoints {
		cfBatch := make([]headerfs.FilterHeader, 0, wire.CFCheckptInterval)
		hdrBatch := make([]headerfs.BlockHeader, 0, wire.CFCheckptInterval)
		startHeader := 0
		if i == 0 {
			startHeader++
		}
		for j := startHeader; j < wire.CFCheckptInterval-1; j++ {
			height = uint32(i*wire.CFCheckptInterval + j)
			header = heightToHeader(height)
			hdrBatch = append(hdrBatch, headerfs.BlockHeader{
				BlockHeader: header,
				Height:      height,
			})
			cfBatch = append(cfBatch, headerfs.FilterHeader{
				FilterHash: zeroHash,
				HeaderHash: header.BlockHash(),
				Height:     height,
			})
		}
		height = uint32((i+1)*wire.CFCheckptInterval - 1)
		header = heightToHeader(height)
		hdrBatch = append(hdrBatch, headerfs.BlockHeader{
			BlockHeader: header,
			Height:      height,
		})
		cfBatch = append(cfBatch, headerfs.FilterHeader{
			FilterHash: *point,
			HeaderHash: header.BlockHash(),
			Height:     height,
		})
		err = hdrStore.WriteHeaders(hdrBatch...)
		if err != nil {
			t.Fatalf("Error writing batch of cfheaders: %s", err)
		}
		err = cfStore.WriteHeaders(cfBatch...)
		if err != nil {
			t.Fatalf("Error writing batch of cfheaders: %s", err)
		}
	}
	for i := 0; i < testCase.storeAddHeight; i++ {
		height = uint32(len(testCase.storepoints)*
			wire.CFCheckptInterval + i)
		header = heightToHeader(height)
		err = hdrStore.WriteHeaders(headerfs.BlockHeader{
			BlockHeader: header,
			Height:      height,
		})
		if err != nil {
			t.Fatalf("Error writing single block header: %s", err)
		}
		err = cfStore.WriteHeaders(headerfs.FilterHeader{
			FilterHash: zeroHash,
			HeaderHash: zeroHash,
			Height:     height,
		})
		if err != nil {
			t.Fatalf("Error writing single cfheader: %s", err)
		}
	}
	heightDiff, err := checkCFCheckptSanity(testCase.checkpoints, cfStore)
	if err != nil {
		t.Fatalf("Error from checkCFCheckptSanity: %s", err)
	}
	if heightDiff != testCase.heightDiff {
		t.Fatalf("Height difference mismatch. Expected: %d, got: %d",
			testCase.heightDiff, heightDiff)
	}
}

func TestCheckCFCheckptSanity(t *testing.T) {
	for _, testCase := range cfCheckptTestCases {
		t.Run(testCase.name, func(t *testing.T) {
			runCheckCFCheckptSanityTestCase(t, testCase)
		})
	}
}

func TestCheckForCFHeadersMismatch(t *testing.T) {
	for _, testCase := range checkCFHTestCases {
		t.Run(testCase.name, func(t *testing.T) {
			mismatch := checkForCFHeaderMismatch(testCase.headers,
				testCase.idx)
			if mismatch != testCase.mismatch {
				t.Fatalf("Wrong mismatch detected. Expected: "+
					"%t, got: %t", testCase.mismatch,
					mismatch)
			}
		})
	}
}

func TestResolveCFHeadersMismatch(t *testing.T) {
	for _, testCase := range resolveCFHTestCases {
		t.Run(testCase.name, func(t *testing.T) {
			badPeers, err := resolveCFHeaderMismatch(block,
				wire.GCSFilterExtended, testCase.idx,
				testCase.headers)
			if err != nil {
				t.Fatalf("Couldn't resolve cfheader "+
					"mismatch: %v", err)
			}
			if len(badPeers) != len(testCase.badPeers) {
				t.Fatalf("Banned wrong peers.\nExpected: "+
					"%#v\nGot: %#v", testCase.badPeers,
					badPeers)
			}
			sort.Strings(badPeers)
			for i := 0; i < len(badPeers); i++ {
				if badPeers[i] != testCase.badPeers[i] {
					t.Fatalf("Banned wrong peers.\n"+
						"Expected: %#v\nGot: %#v",
						testCase.badPeers, badPeers)
				}
			}
		})
	}
}
