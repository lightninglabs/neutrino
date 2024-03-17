package sideload

import (
	"bytes"
	"io"
	"os"
	"sync"
	"testing"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/integration/rpctest"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/tlv"
	"github.com/stretchr/testify/require"
)

func GenerateEncodedBinaryReader(t *testing.T, c *TestCfg,
	blkHdrsByte []byte) io.ReadSeeker {

	encodedOsFile, err := os.CreateTemp("", "temp")
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, encodedOsFile.Close())

		require.NoError(t, os.Remove(encodedOsFile.Name()))
	})

	require.NoError(
		t, tlv.WriteVarInt(encodedOsFile, uint64(c.DataType),
			&[8]byte{}),
	)

	require.NoError(
		t, tlv.WriteVarInt(encodedOsFile, c.StartHeight, &[8]byte{}),
	)

	require.NoError(
		t, tlv.WriteVarInt(encodedOsFile, c.EndHeight, &[8]byte{}),
	)

	require.NoError(
		t, tlv.WriteVarInt(encodedOsFile, uint64(c.Net), &[8]byte{}),
	)

	lengthofWrittenBytes, err := encodedOsFile.Write(blkHdrsByte)
	require.NoError(t, err)
	require.Equal(t, lengthofWrittenBytes, len(blkHdrsByte))

	// Reset to the beginning of the file.
	_, err = encodedOsFile.Seek(0, io.SeekStart)
	require.NoError(t, err)

	return encodedOsFile
}

var headerBufPool = sync.Pool{
	New: func() interface{} { return new(bytes.Buffer) },
}

func GenerateValidBlockHeaders(numHeaders uint32, harness *rpctest.Harness,
	t *testing.T) []byte {

	// Generate 200 valid blocks that we then feed to the block manager.
	blockHashes, err := harness.Client.Generate(numHeaders)
	require.NoError(t, err)

	return serializeHeaders(blockHashes, harness, t)
}

// GenerateInValidBlockHeaders produces a slice of invalid block headers
// between the specified startHeight and endHeight. These headers are made
// invalid by creating a gap in the blockchain: headers up to
// lastValidHeaderHeight are generated and considered valid, then a set of
// headers are generated and discarded (not added to the result), followed by
// another set of headers that are added to the result. This gap results in
// a discontinuity making the entire sequence invalid. The function utilizes
// a test harness for generating and fetching block headers.
func GenerateInValidBlockHeaders(startHeight, endHeight,
	lastValidHeaderHeight uint32, harness *rpctest.Harness,
	t *testing.T) []byte {

	numHeaders := endHeight - startHeight

	// The lastValidHeaderHeight must be within the fetch
	if lastValidHeaderHeight > endHeight-startHeight {
		t.Fatalf("unable to generate invalid headers")
	}

	totalBlockHashes := make([]*chainhash.Hash, 0, numHeaders)
	blockHashes, err := harness.Client.Generate(
		lastValidHeaderHeight - startHeight,
	)
	require.NoError(t, err)

	totalBlockHashes = append(totalBlockHashes, blockHashes...)

	_, err = harness.Client.Generate(10)
	require.NoError(t, err)

	blockHashes, err = harness.Client.Generate(
		endHeight - lastValidHeaderHeight,
	)
	require.NoError(t, err)

	totalBlockHashes = append(totalBlockHashes, blockHashes...)

	return serializeHeaders(totalBlockHashes, harness, t)
}

func serializeHeaders(blockHashes []*chainhash.Hash,
	harness *rpctest.Harness, t *testing.T) []byte {

	// First, we'll grab a buffer from the write buffer pool so we can
	// reduce our total number of allocations, and also write the headers
	// in a single swoop.
	headerBuf := headerBufPool.Get().(*bytes.Buffer)
	headerBuf.Reset()
	defer headerBufPool.Put(headerBuf)

	for i := range blockHashes {
		hdr, err := harness.Client.GetBlockHeader(blockHashes[i])
		require.NoError(t, err)

		err = hdr.Serialize(headerBuf)
		require.NoError(t, err)
	}

	return headerBuf.Bytes()
}

// TestCfg defines a struct for configuring tests that involve generating
// or working with headers. It includes fields for specifying the
// start and end heights of the headers, the network type, and the data type
// of the headers.
type TestCfg struct {
	StartHeight uint64
	EndHeight   uint64
	Net         wire.BitcoinNet
	DataType    dataType
}
