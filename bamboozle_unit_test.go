package neutrino

import (
	"io/ioutil"
	"os"
	"sort"
	"testing"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil/gcs"
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
	name        string
	block       *wire.MsgBlock
	idx         int
	peerFilters map[string]*gcs.Filter
	badPeers    []string
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

	script1 = []byte{
		0x41, // OP_DATA_65
		0x04, 0xd6, 0x4b, 0xdf, 0xd0, 0x9e, 0xb1, 0xc5,
		0xfe, 0x29, 0x5a, 0xbd, 0xeb, 0x1d, 0xca, 0x42,
		0x81, 0xbe, 0x98, 0x8e, 0x2d, 0xa0, 0xb6, 0xc1,
		0xc6, 0xa5, 0x9d, 0xc2, 0x26, 0xc2, 0x86, 0x24,
		0xe1, 0x81, 0x75, 0xe8, 0x51, 0xc9, 0x6b, 0x97,
		0x3d, 0x81, 0xb0, 0x1c, 0xc3, 0x1f, 0x04, 0x78,
		0x34, 0xbc, 0x06, 0xd6, 0xd6, 0xed, 0xf6, 0x20,
		0xd1, 0x84, 0x24, 0x1a, 0x6a, 0xed, 0x8b, 0x63,
		0xa6, // 65-byte signature
		0xac, // OP_CHECKSIG
	}
	script2 = []byte{
		0x00, // Version 0 witness program
		0x14, // OP_DATA_20
		0x9d, 0xda, 0xc6, 0xf3, 0x9d, 0x51, 0xe0, 0x39,
		0x8e, 0x53, 0x2a, 0x22, 0xc4, 0x1b, 0xa1, 0x89,
		0x40, 0x6a, 0x85, 0x23, // 20-byte pub key hash
	}
	script3 = []byte{
		0x6a, // OP_RETURN
		0x24, // OP_PUSH_DATA_36
		0xaa, 0x21, 0xa9, 0xed, 0x26, 0xe6, 0xdd, 0xfa,
		0x3c, 0xc5, 0x1e, 0x27, 0x61, 0xba, 0xf6, 0xea,
		0xc4, 0x54, 0xea, 0x11, 0x6d, 0xa3, 0x8f, 0xfb,
		0x3f, 0xc4, 0x45, 0x05, 0xf2, 0x16, 0x10, 0xe5,
		0x5b, 0x4c, 0x6f, 0x4d,
	}

	// For the purpose of the cfheader mismatch test, we actually only need
	// to have the scripts of each transaction present.
	block = &wire.MsgBlock{
		Transactions: []*wire.MsgTx{
			{
				TxOut: []*wire.TxOut{
					{
						PkScript: script1,
					},
				},
			},
			{
				TxOut: []*wire.TxOut{
					{
						PkScript: script2,
					},
				},
			},
			{
				TxOut: []*wire.TxOut{
					{
						PkScript: script3,
					},
				},
			},
		},
	}
	correctFilter, _ = builder.BuildBasicFilter(block, nil)

	fakeFilter1, _ = gcs.FromBytes(2, builder.DefaultP, builder.DefaultM, []byte{
		0x30, 0x43, 0x02, 0x1f, 0x4d, 0x23, 0x81, 0xdc,
		0x97, 0xf1, 0x82, 0xab, 0xd8, 0x18, 0x5f, 0x51,
		0x75, 0x30, 0x18, 0x52, 0x32, 0x12, 0xf5, 0xdd,
		0xc0, 0x7c, 0xc4, 0xe6, 0x3a, 0x8d, 0xc0, 0x36,
		0x58, 0xda, 0x19, 0x02, 0x20, 0x60, 0x8b, 0x5c,
		0x4d, 0x92, 0xb8, 0x6b, 0x6d, 0xe7, 0xd7, 0x8e,
		0xf2, 0x3a, 0x2f, 0xa7, 0x35, 0xbc, 0xb5, 0x9b,
		0x91, 0x4a, 0x48, 0xb0, 0xe1, 0x87, 0xc5, 0xe7,
		0x56, 0x9a, 0x18, 0x19, 0x70, 0x01,
	})
	fakeFilter2, _ = gcs.FromBytes(2, builder.DefaultP, builder.DefaultM, []byte{
		0x03, 0x07, 0xea, 0xd0, 0x84, 0x80, 0x7e, 0xb7,
		0x63, 0x46, 0xdf, 0x69, 0x77, 0x00, 0x0c, 0x89,
		0x39, 0x2f, 0x45, 0xc7, 0x64, 0x25, 0xb2, 0x61,
		0x81, 0xf5, 0x21, 0xd7, 0xf3, 0x70, 0x06, 0x6a,
		0x8f,
	})

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
		filter, _ := builder.BuildBasicFilter(block, nil)
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
			peerFilters: map[string]*gcs.Filter{
				"a": fakeFilter1,
				"b": fakeFilter1,
			},
			idx:      0,
			badPeers: []string{"a", "b"},
		},
		{
			name:  "all bad 2",
			block: block,
			peerFilters: map[string]*gcs.Filter{
				"a": fakeFilter2,
				"b": fakeFilter2,
			},
			idx:      0,
			badPeers: []string{"a", "b"},
		},
		{
			name:  "all bad 3",
			block: block,
			peerFilters: map[string]*gcs.Filter{
				"a": fakeFilter2,
				"b": fakeFilter2,
			},
			idx:      0,
			badPeers: []string{"a", "b"},
		},
		{
			name:  "all bad 4",
			block: block,
			peerFilters: map[string]*gcs.Filter{
				"a": fakeFilter1,
				"b": fakeFilter2,
			},
			idx:      0,
			badPeers: []string{"a", "b"},
		},
		{
			name:  "all bad 5",
			block: block,
			peerFilters: map[string]*gcs.Filter{
				"a": fakeFilter2,
				"b": fakeFilter1,
			},
			idx:      1,
			badPeers: []string{"a", "b"},
		},
		{
			name:  "one good",
			block: block,
			peerFilters: map[string]*gcs.Filter{
				"a": correctFilter,
				"b": fakeFilter1,
				"c": fakeFilter2,
			},
			idx:      1,
			badPeers: []string{"b", "c"},
		},
		{
			name:  "all good",
			block: block,
			peerFilters: map[string]*gcs.Filter{
				"a": correctFilter,
				"b": correctFilter,
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

	hdrStore, err := headerfs.NewBlockHeaderStore(
		tempDir, db, &chaincfg.SimNetParams,
	)
	if err != nil {
		t.Fatalf("Error creating block header store: %s", err)
	}

	cfStore, err := headerfs.NewFilterHeaderStore(
		tempDir, db, headerfs.RegularFilter, &chaincfg.SimNetParams,
	)
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

		for j := 1; j < wire.CFCheckptInterval; j++ {
			height := uint32(i*wire.CFCheckptInterval + j)
			header := heightToHeader(height)

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

		height := uint32((i + 1) * wire.CFCheckptInterval)

		header := heightToHeader(height)
		hdrBatch = append(hdrBatch, headerfs.BlockHeader{
			BlockHeader: header,
			Height:      height,
		})

		cfBatch = append(cfBatch, headerfs.FilterHeader{
			FilterHash: *point,
			HeaderHash: header.BlockHash(),
			Height:     height,
		})

		if err = hdrStore.WriteHeaders(hdrBatch...); err != nil {
			t.Fatalf("Error writing batch of headers: %s", err)
		}

		if err = cfStore.WriteHeaders(cfBatch...); err != nil {
			t.Fatalf("Error writing batch of cfheaders: %s", err)
		}
	}

	for i := 0; i < testCase.storeAddHeight; i++ {
		height = uint32(len(testCase.storepoints)*
			wire.CFCheckptInterval + i)
		header = heightToHeader(height)

		if err = hdrStore.WriteHeaders(headerfs.BlockHeader{
			BlockHeader: header,
			Height:      height,
		}); err != nil {
			t.Fatalf("Error writing single block header: %s", err)
		}

		if err = cfStore.WriteHeaders(headerfs.FilterHeader{
			FilterHash: zeroHash,
			HeaderHash: zeroHash,
			Height:     height,
		}); err != nil {
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
	t.Parallel()

	for _, testCase := range cfCheckptTestCases {
		t.Run(testCase.name, func(t *testing.T) {
			runCheckCFCheckptSanityTestCase(t, testCase)
		})
	}
}

func TestCheckForCFHeadersMismatch(t *testing.T) {
	t.Parallel()

	for _, testCase := range checkCFHTestCases {
		t.Run(testCase.name, func(t *testing.T) {
			mismatch := checkForCFHeaderMismatch(
				testCase.headers, testCase.idx,
			)
			if mismatch != testCase.mismatch {
				t.Fatalf("Wrong mismatch detected. Expected: "+
					"%t, got: %t", testCase.mismatch,
					mismatch)
			}
		})
	}
}

func TestResolveCFHeadersMismatch(t *testing.T) {
	t.Parallel()

	for _, testCase := range resolveCFHTestCases {
		t.Run(testCase.name, func(t *testing.T) {
			badPeers, err := resolveCFHeaderMismatch(
				block, wire.GCSFilterRegular, testCase.peerFilters,
			)
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
