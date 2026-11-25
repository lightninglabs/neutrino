package neutrino

import (
	"io"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestConnSetGroups checks how a connSet counts the dials and connections of
// an outbound group, and keeps a group to one dial or connection when asked.
func TestConnSetGroups(t *testing.T) {
	const group = "1.2"

	var cs connSet

	// A dial that needs the group to itself gets it, and the next one
	// is refused while the first is counted. One that doesn't is
	// counted alongside.
	require.True(t, cs.startDial(group, true))
	require.False(t, cs.startDial(group, true))
	require.True(t, cs.startDial(group, false))
	require.Equal(t, 2, cs.groupCount(group))

	// A failed dial is no longer counted.
	_, err := cs.track(group, nil, net.ErrClosed)
	require.ErrorIs(t, err, net.ErrClosed)
	require.Equal(t, 1, cs.groupCount(group))

	// A connection is counted until it's closed, once however many
	// times it's closed.
	local, remote := net.Pipe()
	t.Cleanup(func() { _ = remote.Close() })
	conn, err := cs.track(group, local, nil)
	require.NoError(t, err)
	require.Equal(t, 1, cs.groupCount(group))
	require.NoError(t, conn.Close())
	_ = conn.Close()
	require.Zero(t, cs.groupCount(group))
	require.True(t, cs.startDial(group, true))

	// Once the set is closed, a connection made is closed right away.
	cs.closeAll()
	local, remote = net.Pipe()
	t.Cleanup(func() { _ = remote.Close() })
	_, err = cs.track(group, local, nil)
	require.ErrorIs(t, err, ErrShuttingDown)
	require.Zero(t, cs.groupCount(group))
	_, err = local.Write([]byte{0})
	require.ErrorIs(t, err, io.ErrClosedPipe)
}
