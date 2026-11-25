package filtermatch

import (
	"context"
	"errors"
	"testing"

	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/btcutil/v2/gcs"
	"github.com/btcsuite/btcd/btcutil/v2/gcs/builder"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/stretchr/testify/require"
)

func TestBasicMatcher(t *testing.T) {
	t.Parallel()

	block := btcutil.NewBlock(wire.NewMsgBlock(&wire.BlockHeader{}))
	hash := *block.Hash()
	scripts := [][]byte{{0x51}}
	empty, err := builder.BuildBasicFilter(block.MsgBlock(), nil)
	require.NoError(t, err)

	key := builder.DeriveKey(&hash)
	hit, err := gcs.BuildGCSFilter(
		builder.DefaultP, builder.DefaultM, key, scripts,
	)
	require.NoError(t, err)
	fetchErr := errors.New("lookup failed")
	tests := []struct {
		name   string
		filter *gcs.Filter
		err    error
		body   *btcutil.Block
		fetch  bool
		fail   bool
	}{
		{name: "missing filter", fail: true},
		{name: "empty filter", filter: empty},
		{name: "failed lookup", err: fetchErr, fail: true},
		{name: "missing body", filter: hit, fetch: true, fail: true},
		{name: "wrong body", filter: hit, fetch: true, fail: true,
			body: btcutil.NewBlock(wire.NewMsgBlock(
				&wire.BlockHeader{Nonce: 1},
			))},
		{name: "checked body", filter: hit, fetch: true, body: block},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			source := func(_ context.Context, h chainhash.Hash,
				batch bool) (*gcs.Filter, error) {

				require.Equal(t, hash, h)
				require.True(t, batch)
				return tc.filter, tc.err
			}
			matcher := NewBlockMatcher(BasicFilterSource(source))
			fetched := false
			body, err := FetchMatchingBlock(
				t.Context(), matcher, hash, scripts, true,
				func(_ context.Context, h chainhash.Hash) (
					*btcutil.Block, error) {

					fetched = true
					require.Equal(t, hash, h)
					return tc.body, nil
				},
			)
			require.Equal(t, tc.fetch, fetched)
			if tc.fail {
				require.Error(t, err)
				require.Nil(t, body)
				if tc.err != nil {
					require.ErrorIs(t, err, tc.err)
				}
				return
			}
			require.NoError(t, err)
			require.Same(t, tc.body, body)
		})
	}
}
