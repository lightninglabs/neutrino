package neutrino

import (
	"crypto/sha256"
	"testing"

	"github.com/btcsuite/btcd/btcec"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil"
	"github.com/btcsuite/btcutil/gcs"
	"github.com/btcsuite/btcutil/gcs/builder"
	"github.com/stretchr/testify/require"
)

var (
	chainParams = &chaincfg.RegressionNetParams

	dummySignaturePush = []byte{
		txscript.OP_DATA_72,
		0x30, 0x45, 0x02, 0x21, 0x00, 0xad, 0x08, 0x51,
		0xc6, 0x9d, 0xd7, 0x56, 0xb4, 0x51, 0x90, 0xb5,
		0xa8, 0xe9, 0x7c, 0xb4, 0xac, 0x3c, 0x2b, 0x0f,
		0xa2, 0xf2, 0xaa, 0xe2, 0x3a, 0xed, 0x6c, 0xa9,
		0x7a, 0xb3, 0x3b, 0xf8, 0x83, 0x02, 0x20, 0x0b,
		0x24, 0x85, 0x93, 0xab, 0xc1, 0x25, 0x95, 0x12,
		0x79, 0x3e, 0x7d, 0xea, 0x61, 0x03, 0x6c, 0x60,
		0x17, 0x75, 0xeb, 0xb2, 0x36, 0x40, 0xa0, 0x12,
		0x0b, 0x0d, 0xba, 0x2c, 0x34, 0xb7, 0x90, 0x01,
	}
)

// TestVerifyBlockFilter tests that a filter is correctly inspected for validity
// against a downloaded block.
func TestVerifyBlockFilter(t *testing.T) {
	privKey, err := btcec.NewPrivateKey(btcec.S256())
	require.NoError(t, err)

	pubKey := privKey.PubKey()
	pubKey2 := incrementKey(pubKey)

	// We'll create an initial block with just one TX that spends to all
	// currently known standard output types (P2PKH, P2SH, NP2WKH, P2WKH and
	// P2WSH). All those outputs are then spent in the next block in a
	// single TX that references all of them.
	prevTx := &wire.MsgTx{
		Version: 2,
		TxIn:    []*wire.TxIn{},
		TxOut: []*wire.TxOut{{
			Value:    999,
			PkScript: makeP2PK(t, pubKey),
		}, {
			Value:    999,
			PkScript: makeP2PKH(t, pubKey),
		}, {
			Value:    999,
			PkScript: makeP2SH(t, pubKey),
		}, {
			Value:    999,
			PkScript: makeNP2WKH(t, pubKey),
		}, {
			Value:    999,
			PkScript: makeP2WKH(t, pubKey),
		}, {
			Value:    999,
			PkScript: makeP2WSH(t, pubKey),
		}},
	}
	prevBlock := &wire.MsgBlock{
		Header: wire.BlockHeader{
			PrevBlock:  [32]byte{1, 2, 3},
			MerkleRoot: [32]byte{3, 2, 1},
		},
		Transactions: []*wire.MsgTx{
			{}, // Fake coinbase TX.
			prevTx,
		},
	}

	// The spend TX is the transaction that has an input to spend each of
	// the different output types we created in the previous block/TX.
	spendTx := &wire.MsgTx{
		Version: 2,
		TxIn: []*wire.TxIn{
			spendP2PK(t, pubKey, prevTx, 0),
			spendP2PKH(t, pubKey, prevTx, 1),
			spendP2SH(t, pubKey, prevTx, 2),
			spendNP2WKH(t, pubKey, prevTx, 3),
			spendP2WKH(t, pubKey, prevTx, 4),
			spendP2WSH(t, pubKey, prevTx, 5),
		},
		TxOut: []*wire.TxOut{{
			Value:    999,
			PkScript: makeP2WKH(t, pubKey2),
		}, {
			Value:    999,
			PkScript: makeP2WSH(t, pubKey2),
		}, {
			Value:    999,
			PkScript: []byte{txscript.OP_RETURN},
		}},
	}
	spendBlock := &wire.MsgBlock{
		Header: wire.BlockHeader{
			PrevBlock:  prevBlock.BlockHash(),
			MerkleRoot: [32]byte{3, 2, 1},
		},
		Transactions: []*wire.MsgTx{
			{}, // Fake coinbase TX.
			spendTx,
		},
	}

	// We now create a filter from our block that is fully valid and
	// contains all the entries we require according to BIP-158.
	utxoSet := []*wire.MsgTx{prevTx}
	validFilter := filterFromBlock(t, utxoSet, spendBlock, true)
	b := btcutil.NewBlock(spendBlock)

	opReturnValid, err := VerifyBasicBlockFilter(validFilter, b)
	require.NoError(t, err)
	require.Equal(t, 1, opReturnValid)
}

func filterFromBlock(t *testing.T, utxoSet []*wire.MsgTx,
	block *wire.MsgBlock, withInputPrevOut bool) *gcs.Filter {

	var filterContent [][]byte
	for idx, tx := range block.Transactions {
		// Skip coinbase transaction.
		if idx == 0 {
			continue
		}

		// Add all output pk scripts. Normally we'd need to filter out
		// any OP_RETURNs but for the test we want to make sure they're
		// counted correctly so we leave them in.
		for _, out := range tx.TxOut {
			filterContent = append(filterContent, out.PkScript)
		}

		// To create an invalid filter we just skip the pk scripts of
		// the spent outputs.
		if !withInputPrevOut {
			continue
		}

		// Add all previous output scripts of all transactions.
		for _, in := range tx.TxIn {
			utxo := locateUtxo(t, utxoSet, in)
			filterContent = append(filterContent, utxo.PkScript)
		}
	}

	blockHash := block.BlockHash()
	key := builder.DeriveKey(&blockHash)
	filter, err := gcs.BuildGCSFilter(
		builder.DefaultP, builder.DefaultM, key, filterContent,
	)
	require.NoError(t, err)

	return filter
}

func locateUtxo(t *testing.T, utxoSet []*wire.MsgTx, in *wire.TxIn) *wire.TxOut {
	for _, utxo := range utxoSet {
		if utxo.TxHash() == in.PreviousOutPoint.Hash {
			return utxo.TxOut[in.PreviousOutPoint.Index]
		}
	}

	require.Fail(t, "utxo for outpoint %v not found", in.PreviousOutPoint)
	return nil
}

func spendP2PK(_ *testing.T, _ *btcec.PublicKey, prevTx *wire.MsgTx,
	idx uint32) *wire.TxIn {

	return &wire.TxIn{
		PreviousOutPoint: wire.OutPoint{
			Hash:  prevTx.TxHash(),
			Index: idx,
		},
		SignatureScript: dummySignaturePush,
	}
}

func spendP2PKH(_ *testing.T, pubKey *btcec.PublicKey, prevTx *wire.MsgTx,
	idx uint32) *wire.TxIn {

	return &wire.TxIn{
		PreviousOutPoint: wire.OutPoint{
			Hash:  prevTx.TxHash(),
			Index: idx,
		},
		SignatureScript: append(
			dummySignaturePush, pubKey.SerializeCompressed()...,
		),
	}
}

func spendP2SH(t *testing.T, pubKey *btcec.PublicKey, prevTx *wire.MsgTx,
	idx uint32) *wire.TxIn {

	return &wire.TxIn{
		PreviousOutPoint: wire.OutPoint{
			Hash:  prevTx.TxHash(),
			Index: idx,
		},
		SignatureScript: append(
			dummySignaturePush, scriptP2PKH(t, pubKey)...,
		),
	}
}

func spendNP2WKH(t *testing.T, pubKey *btcec.PublicKey, prevTx *wire.MsgTx,
	idx uint32) *wire.TxIn {

	pkHash := btcutil.Hash160(pubKey.SerializeCompressed())

	witAddr, err := btcutil.NewAddressWitnessPubKeyHash(pkHash, chainParams)
	require.NoError(t, err)

	witnessProgram, err := txscript.PayToAddrScript(witAddr)
	require.NoError(t, err)

	return &wire.TxIn{
		PreviousOutPoint: wire.OutPoint{
			Hash:  prevTx.TxHash(),
			Index: idx,
		},
		SignatureScript: witAddr.ScriptAddress(),
		Witness:         [][]byte{dummySignaturePush, witnessProgram},
	}
}

func spendP2WKH(_ *testing.T, pubKey *btcec.PublicKey, prevTx *wire.MsgTx,
	idx uint32) *wire.TxIn {

	return &wire.TxIn{
		PreviousOutPoint: wire.OutPoint{
			Hash:  prevTx.TxHash(),
			Index: idx,
		},
		Witness: [][]byte{
			dummySignaturePush, pubKey.SerializeCompressed(),
		},
	}
}

func spendP2WSH(t *testing.T, pubKey *btcec.PublicKey, prevTx *wire.MsgTx,
	idx uint32) *wire.TxIn {

	script := scriptP2PKH(t, pubKey)

	return &wire.TxIn{
		PreviousOutPoint: wire.OutPoint{
			Hash:  prevTx.TxHash(),
			Index: idx,
		},
		Witness: [][]byte{dummySignaturePush, script},
	}
}

func makeP2PK(t *testing.T, pubKey *btcec.PublicKey) []byte {
	addr, err := btcutil.NewAddressPubKey(
		pubKey.SerializeCompressed(), chainParams,
	)
	require.NoError(t, err)

	pkScript, err := txscript.PayToAddrScript(addr)
	require.NoError(t, err)
	return pkScript
}

func makeP2PKH(t *testing.T, pubKey *btcec.PublicKey) []byte {
	pkHash := btcutil.Hash160(pubKey.SerializeCompressed())
	addr, err := btcutil.NewAddressPubKeyHash(pkHash, chainParams)
	require.NoError(t, err)

	pkScript, err := txscript.PayToAddrScript(addr)
	require.NoError(t, err)
	return pkScript
}

func makeP2SH(t *testing.T, pubKey *btcec.PublicKey) []byte {
	script := scriptP2PKH(t, pubKey)

	addr, err := btcutil.NewAddressScriptHash(script, chainParams)
	require.NoError(t, err)

	pkScript, err := txscript.PayToAddrScript(addr)
	require.NoError(t, err)
	return pkScript
}

func makeNP2WKH(t *testing.T, pubKey *btcec.PublicKey) []byte {
	pkHash := btcutil.Hash160(pubKey.SerializeCompressed())

	witAddr, err := btcutil.NewAddressWitnessPubKeyHash(pkHash, chainParams)
	require.NoError(t, err)

	witnessProgram, err := txscript.PayToAddrScript(witAddr)
	require.NoError(t, err)

	addr, err := btcutil.NewAddressScriptHash(witnessProgram, chainParams)
	require.NoError(t, err)

	pkScript, err := txscript.PayToAddrScript(addr)
	require.NoError(t, err)
	return pkScript
}

func makeP2WKH(t *testing.T, pubKey *btcec.PublicKey) []byte {
	pkHash := btcutil.Hash160(pubKey.SerializeCompressed())

	addr, err := btcutil.NewAddressWitnessPubKeyHash(pkHash, chainParams)
	require.NoError(t, err)

	pkScript, err := txscript.PayToAddrScript(addr)
	require.NoError(t, err)
	return pkScript
}

func makeP2WSH(t *testing.T, pubKey *btcec.PublicKey) []byte {
	witnessScript := scriptP2PKH(t, pubKey)
	scriptHash := sha256.Sum256(witnessScript)

	addr, err := btcutil.NewAddressWitnessScriptHash(
		scriptHash[:], chainParams,
	)
	require.NoError(t, err)

	pkScript, err := txscript.PayToAddrScript(addr)
	require.NoError(t, err)
	return pkScript
}

func scriptP2PKH(t *testing.T, pubKey *btcec.PublicKey) []byte {
	// A simple P2PKH script but wrapped as P2SH.
	// <pubKey> OP_CHECKSIG
	b := txscript.NewScriptBuilder()

	b.AddData(pubKey.SerializeCompressed())
	b.AddOp(txscript.OP_CHECKSIG)

	script, err := b.Script()
	require.NoError(t, err)

	return script
}

func incrementKey(key *btcec.PublicKey) *btcec.PublicKey {
	curveParams := key.Curve.Params()
	newX, newY := key.Curve.Add(key.X, key.Y, curveParams.Gx, curveParams.Gy)
	return &btcec.PublicKey{
		X:     newX,
		Y:     newY,
		Curve: btcec.S256(),
	}
}
