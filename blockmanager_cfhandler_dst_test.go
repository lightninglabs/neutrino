package neutrino

import (
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/banman"
	"github.com/lightninglabs/neutrino/blockntfns"
	"github.com/lightninglabs/neutrino/headerfs"
	querysyncwm "github.com/lightninglabs/neutrino/querysync/workmanager"
	"github.com/lightninglabs/neutrino/sqldb"
	"github.com/stretchr/testify/require"
)

var errCFHeaderRead = errors.New("filter tip read unavailable")

// cfHeaderLostAckStore commits a write, loses its acknowledgement and fails
// the first attempt to determine whether it committed.
type cfHeaderLostAckStore struct {
	headerfs.FilterHeaderStore
	failWrite bool
	failRead  bool
}

func (s *cfHeaderLostAckStore) WriteHeaders(
	headers ...headerfs.FilterHeader) error {

	if err := s.FilterHeaderStore.WriteHeaders(headers...); err != nil {
		return err
	}
	if s.failWrite {
		s.failWrite = false
		s.failRead = true
		return errDSTWrite
	}
	return nil
}

func (s *cfHeaderLostAckStore) ChainTip() (*chainhash.Hash, uint32, error) {
	if s.failRead {
		s.failRead = false
		return nil, 0, errCFHeaderRead
	}
	return s.FilterHeaderStore.ChainTip()
}

// TestCFHeaderCommittedReadFailureHeals requires the next normal sync pass
// to recover publication after both a commit acknowledgement and read fail.
func TestCFHeaderCommittedReadFailureHeals(t *testing.T) {
	for _, backend := range []string{"memory", "sql"} {
		for _, suffix := range []bool{false, true} {
			name := backend + "/next_sync"
			if suffix {
				name = backend + "/suffix_commit"
			}
			t.Run(name, func(t *testing.T) {
				// External database startup must use real time.
				var db *sqldb.Backend
				if backend == "sql" {
					db = sqldb.NewTestBackend(t)
				}
				synctest.Test(t, func(t *testing.T) {
					closeTestBackendInBubble(t, db)
					testCFHeaderReadFailure(
						t,
						suffix,
						db,
					)
				})
			})
		}
	}
}

// closeTestBackendInBubble keeps connection timers in their owning bubble.
func closeTestBackendInBubble(t *testing.T, db *sqldb.Backend) {
	t.Helper()
	if db != nil {
		t.Cleanup(func() {
			require.NoError(t, db.Close())
		})
	}
}

// testCFHeaderReadFailure runs one committed-write recovery schedule.
func testCFHeaderReadFailure(
	t *testing.T, suffix bool, db *sqldb.Backend,
) {

	params := &chaincfg.SimNetParams
	var blocks headerfs.BlockHeaderStore
	var underlying headerfs.FilterHeaderStore
	if db != nil {
		var err error
		blocks, err = headerfs.NewSQLBlockHeaderStore(
			t.Context(),
			db.HeaderTxer,
			params,
		)
		require.NoError(t, err)
		underlying, err = headerfs.NewSQLFilterHeaderStore(
			t.Context(),
			db.HeaderTxer,
			headerfs.RegularFilter,
			params,
			nil,
		)
		require.NoError(t, err)
	} else {
		blocks = newMemBlockHeaderStore(params)
		var err error
		underlying, err = newMemFilterHeaderStore(params)
		require.NoError(t, err)
	}
	fixture := dstChains()
	numHeaders := 3
	if suffix {
		numHeaders = 5
	}
	headers := make(
		[]headerfs.BlockHeader,
		numHeaders,
	)
	for i := range headers {
		headers[i] = headerfs.BlockHeader{
			BlockHeader: fixture.honest[i+1],
			Height: uint32(
				i + 1,
			),
		}
	}
	require.NoError(
		t,
		blocks.WriteHeaders(headers...),
	)
	store := &cfHeaderLostAckStore{
		FilterHeaderStore: underlying, failWrite: true,
	}
	bm := newCFHeaderTestManager(
		t,
		blocks,
		store,
	)
	notifications := make(
		chan blockntfns.BlockNtfn,
		10,
	)
	go func() {
		for {
			select {
			case n := <-bm.blockNtfnChan:
				notifications <- n
			case <-bm.quit:
				return
			}
		}
	}()
	bm.wg.Add(1)
	go bm.blockHandler()
	defer func() { require.NoError(t, bm.Stop()) }()
	tip, _, err := underlying.ChainTip()
	require.NoError(t, err)
	hashes := make([]chainhash.Hash, 3)
	msg := &wire.MsgCFHeaders{
		StopHash: headers[2].BlockHash(), PrevFilterHeader: *tip,
	}
	for i := range hashes {
		hashes[i] = chainhash.DoubleHashH(
			[]byte{byte(i + 1)},
		)
		msg.FilterHashes = append(
			msg.FilterHashes,
			&hashes[i],
		)
	}
	_, _, err = bm.submitCFHeaders(
		[]*wire.MsgCFHeaders{msg},
	)
	require.ErrorIs(t, err, errDSTWrite)
	require.ErrorIs(t, err, errCFHeaderRead)

	// This is cfHandler's next normal iteration. Equal durable heights
	// must not hide unpublished committed work.
	if suffix {
		previous, _, err := underlying.ChainTip()
		require.NoError(t, err)
		next := &wire.MsgCFHeaders{
			StopHash:         headers[4].BlockHash(),
			PrevFilterHeader: *previous,
		}
		for height := byte(4); height <= 5; height++ {
			hash := chainhash.DoubleHashH(
				[]byte{height},
			)
			next.FilterHashes = append(
				next.FilterHashes,
				&hash,
			)
		}
		_, _, err = bm.submitCFHeaders(
			[]*wire.MsgCFHeaders{
				next,
			},
		)
		require.NoError(t, err)
	} else {
		require.NoError(t, bm.getUncheckpointedCFHeaders(
			store, wire.GCSFilterRegular,
		))
	}
	synctest.Wait()
	bm.newFilterHeadersMtx.RLock()
	cachedHeight, cachedHash := bm.filterHeaderTip, bm.filterHeaderTipHash
	bm.newFilterHeadersMtx.RUnlock()
	require.Equal(
		t,
		uint32(len(headers)),
		cachedHeight,
	)
	require.Equal(
		t,
		headers[len(headers)-1].BlockHash(),
		cachedHash,
	)
	require.Len(
		t,
		notifications,
		len(headers),
	)
	for i := range headers {
		n := <-notifications
		_, ok := n.(*blockntfns.Connected)
		require.True(t, ok)
		require.Equal(
			t,
			uint32(i+1),
			n.Height(),
		)
		header := n.Header()
		require.Equal(
			t,
			headers[i].BlockHash(),
			header.BlockHash(),
		)
	}
	require.NoError(
		t,
		bm.getUncheckpointedCFHeaders(
			store,
			wire.GCSFilterRegular,
		),
	)
	synctest.Wait()
	require.Empty(t, notifications)
}

// cfHeaderBlockTipGate holds a completed tip read while another producer
// attempts a reorg. A coherent snapshot keeps the block owner here.
type cfHeaderBlockTipGate struct {
	headerfs.BlockHeaderStore
	armed   bool
	entered chan struct{}
	release chan struct{}
}

func (s *cfHeaderBlockTipGate) ChainTip() (*wire.BlockHeader, uint32, error) {
	header, height, err := s.BlockHeaderStore.ChainTip()
	if s.armed {
		s.armed = false
		close(s.entered)
		<-s.release
	}
	return header, height, err
}

// TestCFHeaderReorgDoesNotBanHonestPeer requires network responses to be
// checked against the predecessor belonging to their requested branch.
func TestCFHeaderReorgDoesNotBanHonestPeer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		params := chaincfg.SimNetParams
		params.Checkpoints = nil
		fixture := dstChains()
		blocks := newMemBlockHeaderStore(&params)
		filters, err := newMemFilterHeaderStore(&params)
		require.NoError(t, err)
		genesisFilter, _, err := filters.ChainTip()
		require.NoError(t, err)
		const oldHeight = dstChainHeight + dstTipExtra
		var headers []headerfs.BlockHeader
		for height := uint32(1); height <= oldHeight; height++ {
			headers = append(headers, headerfs.BlockHeader{
				BlockHeader: fixture.honest[height],
				Height:      height,
			})
		}
		require.NoError(t, blocks.WriteHeaders(headers...))

		// Derive each branch from immutable block hashes so a truthful
		// replacement response cannot share A's prefix.
		filterHashes := make(map[chainhash.Hash]chainhash.Hash)
		filterHeaders := make(map[chainhash.Hash]chainhash.Hash)
		filterHeaders[fixture.honest[0].BlockHash()] = *genesisFilter
		for _, branch := range [][]*wire.BlockHeader{
			fixture.honest[:oldHeight+1], fixture.reorg,
		} {
			previous := *genesisFilter
			for _, block := range branch[1:] {
				hash := block.BlockHash()
				filter := chainhash.DoubleHashH(hash[:])
				previous = chainhash.DoubleHashH(
					append(filter[:], previous[:]...),
				)
				filterHashes[hash] = filter
				filterHeaders[hash] = previous
			}
		}
		const filterHeight = oldHeight - 1
		for height := uint32(1); height <= filterHeight; height++ {
			hash := fixture.honest[height].BlockHash()
			require.NoError(
				t,
				filters.WriteHeaders(headerfs.FilterHeader{
					Height:     height,
					HeaderHash: hash,
					FilterHash: filterHeaders[hash],
				}),
			)
		}
		require.NotEqual(t,
			filterHeaders[fixture.honest[filterHeight].BlockHash()],
			filterHeaders[fixture.reorg[filterHeight].BlockHash()],
		)
		gate := &cfHeaderBlockTipGate{
			BlockHeaderStore: blocks, entered: make(chan struct{}),
			release: make(chan struct{}),
		}
		bm := newCFHeaderTestManager(t, gate, filters)
		bm.cfg.ChainParams.Checkpoints = nil
		bm.cfg.TimeSource = dstMedianTime{
			MedianTimeSource: bm.cfg.TimeSource,
			offset: time.Until(
				params.GenesisBlock.Header.Timestamp.Add(
					time.Hour,
				),
			),
		}
		var banned []string
		bm.cfg.BanPeer = func(peer string, _ banman.Reason) error {
			banned = append(banned, peer)
			return nil
		}
		reorgStop := fixture.reorg[len(fixture.reorg)-1].BlockHash()
		bm.cfg.Broadcaster = broadcastFunc(
			func(b *querysyncwm.Broadcast,
				options ...querysyncwm.QueryOption) error {

				request, ok := b.Msg.(*wire.MsgGetCFHeaders)
				require.True(t, ok)
				branch := fixture.honest[:oldHeight+1]
				if request.StopHash == reorgStop {
					branch = fixture.reorg
				}
				require.Equal(
					t,
					branch[len(branch)-1].BlockHash(),
					request.StopHash,
				)
				predecessor := branch[request.StartHeight-1]
				prevHash := predecessor.BlockHash()
				prevFilter := filterHeaders[prevHash]
				response := &wire.MsgCFHeaders{
					FilterType:       request.FilterType,
					StopHash:         request.StopHash,
					PrevFilterHeader: prevFilter,
				}
				selected := branch[request.StartHeight:]
				for _, block := range selected {
					hash := block.BlockHash()
					filter := filterHashes[hash]
					response.FilterHashes = append(
						response.FilterHashes,
						&filter,
					)
				}
				return newScriptedBroadcaster(
					map[string][]wire.Message{
						"honest": {response},
					},
				).Broadcast(b, options...)
			},
		)
		peer := newTestServerPeer(
			t,
			"127.0.0.1:18444",
			int32(oldHeight+1),
		)
		bm.syncPeer = peer
		go func() {
			for {
				select {
				case <-bm.blockNtfnChan:
				case <-bm.quit:
					return
				}
			}
		}()
		bm.wg.Add(1)
		go bm.blockHandler()
		defer func() { require.NoError(t, bm.Stop()) }()
		gate.armed = true
		done := make(chan error, 1)
		go func() {
			done <- bm.getUncheckpointedCFHeaders(
				filters, wire.GCSFilterRegular,
			)
		}()
		<-gate.entered
		reorg := wire.NewMsgHeaders()
		for _, header := range fixture.reorg[dstReorgFork+1:] {
			require.NoError(t, reorg.AddBlockHeader(header))
		}
		bm.QueueHeaders(reorg, peer)
		synctest.Wait()
		close(gate.release)
		// A stale batch may be rejected and retried. Peer immunity
		// must hold whether commit or reorg is processed first.
		<-done
		_, _, err = bm.submitCFHeaders(nil)
		require.ErrorContains(t, err, "no cfheaders")
		synctest.Wait()
		require.Empty(
			t,
			banned,
			"truthful fork responses must never be banned",
		)
		_, filterTip, err := filters.ChainTip()
		require.NoError(t, err)
		require.Equal(t, uint32(dstReorgFork), filterTip)
	})
}
