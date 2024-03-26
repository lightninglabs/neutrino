package sideload

import (
	"testing"

	"github.com/btcsuite/btcd/wire"
	"github.com/stretchr/testify/require"
)

type TestLoaderSourceFunc func(*testing.T, *TestCfg) LoaderSource[*wire.
	BlockHeader]

func TestLoaderSource(t *testing.T) {
	sourceFuncs := []TestLoaderSourceFunc{
		testNewBinaryBlkHdrLoader,
	}

	test := &TestCfg{
		StartHeight: 2,
		EndHeight:   10,
		Net:         wire.TestNet3,
		DataType:    BlockHeaders,
	}

	for _, sourceFunc := range sourceFuncs {
		source := sourceFunc(t, test)

		require.Equal(t, test.StartHeight, uint64(source.StartHeight()))

		require.Equal(t, test.EndHeight, uint64(source.EndHeight()))

		require.Equal(t, test.Net, source.HeadersChain())

		// Fetch all headers from the reader.
		fetchSize := uint32(test.EndHeight - test.StartHeight)
		headers, err := source.FetchHeaders(fetchSize)
		require.NoError(t, err)

		// Store first header in the variable,
		// we would use it for testing later.
		firstHeader := headers[0]

		// We should obtain a length of headers equal to the number
		// of headers requested.
		require.Len(t, headers, int(fetchSize))

		headers, err = source.FetchHeaders(fetchSize)
		require.NoError(t, err)

		// We should not be able to fetch more headers as our
		// reader's seeker is at its end.
		require.Len(t, headers, 0)

		// We expect an error when setting the reader to fetch a
		// header at a height that we do not have.
		err = source.SetHeight(0)
		require.Error(t, err)

		// Now we have set the reader at a header height which it has,
		// We should be able to fetch more headers.
		// Starting from height 3 upwards.
		err = source.SetHeight(2)
		require.NoError(t, err)

		fetchSize = uint32(test.EndHeight - 3)
		headers, err = source.FetchHeaders(fetchSize)
		require.NoError(t, err)

		require.Len(t, headers, int(fetchSize))

		// Setting it at -1, enable to read all (
		// endHeight - startHeight) header.
		err = source.SetHeight(-1)
		require.NoError(t, err)

		fetchSize = uint32(test.EndHeight - test.StartHeight)
		headers, err = source.FetchHeaders(fetchSize)
		require.NoError(t, err)

		require.Len(t, headers, int(fetchSize))

		// Since we set the reader to its first height,
		// the first header we got previously should be the same as
		// the one that we have now.
		require.Equal(t, firstHeader, headers[0])
	}
}
