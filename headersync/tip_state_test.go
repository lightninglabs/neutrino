package headersync

import (
	"testing"

	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/headerlist"
	"github.com/stretchr/testify/require"
)

// TestValidationChain checks that the overlay reads its own nodes first, then
// the base, and only pushes to its own side.
func TestValidationChain(t *testing.T) {
	t.Parallel()

	base := headerlist.NewBoundedMemoryChain(4)
	base.ResetHeaderState(headerlist.Node{
		Height: 10,
		Header: wire.BlockHeader{Nonce: 10},
	})
	baseTip := base.PushBack(headerlist.Node{
		Height: 11,
		Header: wire.BlockHeader{Nonce: 11},
	})

	validationChain := NewValidationChain(base, *baseTip)
	pendingTip := validationChain.PushBack(headerlist.Node{
		Height: 12,
		Header: wire.BlockHeader{Nonce: 12},
	})

	require.Equal(t, int32(12), validationChain.Back().Height)
	require.Equal(t, uint32(12), validationChain.Back().Header.Nonce)
	require.Equal(t, pendingTip, validationChain.Back())

	pending, ok := validationChain.AtHeight(12)
	require.True(t, ok)
	require.Equal(t, uint32(12), pending.Header.Nonce)

	cached, ok := validationChain.AtHeight(10)
	require.True(t, ok)
	require.Equal(t, uint32(10), cached.Header.Nonce)

	_, ok = validationChain.AtHeight(9)
	require.False(t, ok)
}
