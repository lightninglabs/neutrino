package headersync

import (
	"errors"
	"testing"

	"github.com/btcsuite/btcd/wire"
	"github.com/stretchr/testify/require"
)

func TestValidateHeaderRange(t *testing.T) {
	start := testHash(100)
	headers := makeTestHeaders(start, 3)
	stop := headers[len(headers)-1].BlockHash()
	rng := HeaderRange{
		ID:          1,
		StartHeight: 0,
		StartHash:   start,
		StopHeight:  3,
		StopHash:    stop,
	}

	require.NoError(t, ValidateHeaderRange(rng, headers))
}

func TestValidateHeaderRangeRejectsStartMismatch(t *testing.T) {
	headers := makeTestHeaders(testHash(101), 1)
	rng := HeaderRange{
		ID:        1,
		StartHash: testHash(100),
		StopHash:  headers[0].BlockHash(),
	}

	err := ValidateHeaderRange(rng, headers)
	require.True(t, errors.Is(err, ErrRangeStartMismatch))
}

func TestValidateHeaderRangeRejectsDisconnectedHeaders(t *testing.T) {
	start := testHash(100)
	headers := makeTestHeaders(start, 2)
	headers[1].PrevBlock = testHash(200)
	rng := HeaderRange{
		ID:        1,
		StartHash: start,
		StopHash:  headers[1].BlockHash(),
	}

	err := ValidateHeaderRange(rng, headers)
	require.True(t, errors.Is(err, ErrRangeDisconnected))
}

func TestValidateHeaderRangeRejectsStopMismatch(t *testing.T) {
	start := testHash(100)
	headers := makeTestHeaders(start, 1)
	rng := HeaderRange{
		ID:        1,
		StartHash: start,
		StopHash:  testHash(201),
	}

	err := ValidateHeaderRange(rng, headers)
	require.True(t, errors.Is(err, ErrRangeStopMismatch))
}

func TestValidateAnchorResponse(t *testing.T) {
	start := testHash(100)
	headers := makeTestHeaders(start, 2)
	request := AnchorRequest{
		StartHeight: 10,
		StartHash:   start,
		StopHeight:  20,
		StopHash:    testHash(200),
	}

	response, err := ValidateAnchorResponse(request, headers, 2000)
	require.NoError(t, err)
	require.Equal(t, uint32(12), response.Height)
	require.Equal(t, headers[1].BlockHash(), response.Hash)
	require.False(t, response.Trusted)
	require.Equal(t, 2, response.HeaderCount)
}

func TestValidateAnchorResponseTrustsStopHash(t *testing.T) {
	start := testHash(100)
	headers := makeTestHeaders(start, 2)
	request := AnchorRequest{
		StartHeight: 10,
		StartHash:   start,
		StopHeight:  12,
		StopHash:    headers[1].BlockHash(),
	}

	response, err := ValidateAnchorResponse(request, headers, 2000)
	require.NoError(t, err)
	require.True(t, response.Trusted)
}

func TestValidateAnchorResponseWithZeroStopHash(t *testing.T) {
	start := testHash(100)
	headers := makeTestHeaders(start, 2)
	request := AnchorRequest{
		StartHeight: 10,
		StartHash:   start,
		StopHeight:  12,
	}

	response, err := ValidateAnchorResponse(request, headers, 2000)
	require.NoError(t, err)
	require.EqualValues(t, 12, response.Height)
	require.Equal(t, headers[1].BlockHash(), response.Hash)
	require.False(t, response.Trusted)
}

func TestValidateAnchorResponseRejectsOvershoot(t *testing.T) {
	start := testHash(100)
	headers := makeTestHeaders(start, 3)
	request := AnchorRequest{
		StartHeight: 10,
		StartHash:   start,
		StopHeight:  12,
		StopHash:    headers[1].BlockHash(),
	}

	_, err := ValidateAnchorResponse(request, headers, 2000)
	require.True(t, errors.Is(err, ErrRangeOvershoot))
}

func TestValidateAnchorResponseRejectsTooManyHeaders(t *testing.T) {
	start := testHash(100)
	headers := makeTestHeaders(start, 2)
	request := AnchorRequest{
		StartHeight: 10,
		StartHash:   start,
		StopHeight:  12,
		StopHash:    headers[1].BlockHash(),
	}

	_, err := ValidateAnchorResponse(request, headers, 1)
	require.True(t, errors.Is(err, ErrTooManyHeaders))
}

func makeTestHeaders(start Hash, count int) []*wire.BlockHeader {
	headers := make([]*wire.BlockHeader, 0, count)
	prev := start
	for i := 0; i < count; i++ {
		header := &wire.BlockHeader{
			Version:   int32(i + 1),
			PrevBlock: prev,
			Bits:      uint32(i + 1),
			Nonce:     uint32(i + 1),
		}
		headers = append(headers, header)
		prev = header.BlockHash()
	}

	return headers
}
