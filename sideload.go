package neutrino

import (
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightninglabs/neutrino/chaindataloader"
	"github.com/lightninglabs/neutrino/chainsync"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/lightninglabs/neutrino/headerlist"
)

const (
	sideloadDataChunkSize int32 = 2000
)

// sideloader a struct that contains all dependencies required to fetch and
// process sideloaded headers.
type sideloader struct {
	// reader is the sideload source.
	blkHdrReader chaindataloader.Reader

	cfHdrReader chaindataloader.Reader
	// blkHdrProcessor contains dependies required to verify and store block
	// headers.
	blkHdrProcessor *blkHdrProcessor

	// blkHdrSkipVerify indicates if processing the headers require verification.
	blkHdrSkipVerify bool

	fHdrSkipVerify bool

	// fHdrNextCheckpointIdx indicates the index of the next filter header
	// checkpoint height relative to the current filter tip.
	fHdrNextCheckpointIdx int

	// fHdrStore is the store for filter headers.
	fHdrStore *headerfs.FilterHeaderStore

	// filterCheckpoints is the map of hardcoded filter checkpoints to their
	//hash.
	filterCheckpoints map[uint32]*chainhash.Hash

	// filterCheckptHeights is the slice of hardcoded filter checkpoint heights.
	filterCheckptHeights []uint32

	// blkHdrCurHeight is the height of the block header's store tip.
	blkHdrCurHeight int32

	// fHdrCurHeight is the height of the filter header's store tip.
	fHdrCurHeight int32

	// fHdrCurHash is the filter hash of the filter header's store tip.
	fHdrCurHash *chainhash.Hash

	// fHdrNextCheckptHeight is the height of the next filter header next
	// checkpoint's height relative to the current filter header tip.
	fHdrNextCheckptHeight int32

	// fHdrLastHeight is the last height to which we can sideload the filter
	// headers to. This restriction occurs because the filter header tip
	// cannot surpass the block header tip as it depends on it.
	fHdrLastHeight uint32
}

// processSideloadedCfHeader verifies and stores the passed cfheaders.
// It returns the current height and next checkpoint after this process to be
// used by the called. It returns an error where necessary.
func (h *sideloader) processSideloadedCfHeader(
	headers []*chainhash.Hash) (*chaindataloader.ProcessHdrResp, error) {

	msgCfHeader := &wire.MsgCFHeaders{
		FilterType:       fType,
		StopHash:         *headers[len(headers)-1],
		PrevFilterHeader: *h.fHdrCurHash,
		FilterHashes:     headers,
	}

	// We only verify, if this is set to false.
	if !h.fHdrSkipVerify {

		if !verifyCheckpoint(h.fHdrCurHash, headers[len(headers)-1],
			msgCfHeader) {

			log.Warnf("Filter headers failed verification at height  - %v",
				h.fHdrCurHeight)
			return nil, nil
		}
		h.fHdrNextCheckpointIdx = h.fHdrNextCheckpointIdx + 1
		h.fHdrNextCheckptHeight = int32(h.filterCheckptHeights[h.
			fHdrNextCheckpointIdx])

	}

	_, _, _, err := writeCfHeaders(msgCfHeader, h.fHdrStore,
		h.blkHdrProcessor.store)

	if err != nil {
		return nil, err
	}

	h.fHdrCurHash = headers[len(headers)-1]
	h.fHdrCurHeight = h.fHdrCurHeight + int32(len(headers))

	// Check if we have reached the height that we are not to surpass.
	if h.fHdrCurHeight > int32(h.fHdrLastHeight) {
		log.Debugf("Completed filter header sideloading at height %v",
			h.fHdrLastHeight)
		return nil, nil
	}

	return &chaindataloader.ProcessHdrResp{
		CurHeight:         h.fHdrCurHeight,
		NextCheckptHeight: h.fHdrNextCheckptHeight,
	}, nil
}

// processSideLoadBlockHeader verifies and stores the passed block headers.
// It returns the current height and next checkpoint after this process to be
// used by the called. It returns an error where necessary.
func (h *sideloader) processSideLoadBlockHeader(headers []*wire.
	BlockHeader) (*chaindataloader.ProcessHdrResp, error) {

	if !areHeadersConnected(headers) {
		log.Warnf("Headers received don't connect")
		return nil, nil
	}

	headersArray := make([]headerfs.BlockHeader, 0, len(headers))

	var (
		finalHeight          int32
		nextCheckpointHeight int32
	)

	for _, header := range headers {
		var (
			node     *headerlist.Node
			prevNode *headerlist.Node
		)
		// Ensure there is a previous header to compare against.
		prevNodeEl := h.blkHdrProcessor.headerList.Back()
		if prevNodeEl == nil {
			log.Warnf("side load - Header list does not contain a " +
				"previous element as expected -- exiting side load")

			return nil, nil
		}

		node = &headerlist.Node{Header: *header}
		prevNode = prevNodeEl
		node.Height = prevNode.Height + 1

		if !h.blkHdrSkipVerify {

			valid, err := h.blkHdrProcessor.verifyBlockHeader(header, *prevNode)
			if err != nil || !valid {
				log.Debugf("Side Load- Did not pass verification at "+
					"height %v-- ", node.Height)

				return nil, nil
			}

			// Verify checkpoint only if verification is enabled.
			if h.blkHdrProcessor.nextCheckpt != nil &&
				node.Height == h.blkHdrProcessor.nextCheckpt.Height {

				nodeHash := node.Header.BlockHash()
				if nodeHash.IsEqual(h.blkHdrProcessor.nextCheckpt.Hash) {
					// Update nextCheckpoint  to give more accurate info
					// about tip of DB.
					h.blkHdrProcessor.nextCheckpt = h.blkHdrProcessor.
						findNextHeaderCheckpoint(node.Height)

					log.Infof("Verified downloaded block "+
						"header against checkpoint at height "+
						"%d/hash %s", node.Height, nodeHash)
				} else {
					log.Warnf("Error at checkpoint while side loading "+
						"headers, exiting at height %d, hash %s",
						node.Height, nodeHash)
					return nil, nil
				}
			}
		}

		// convert header to  headerfs.Blockheader and add to an  array.
		headersArray = append(headersArray, headerfs.BlockHeader{
			BlockHeader: header,
			Height:      uint32(node.Height),
		})
		h.blkHdrProcessor.headerList.PushBack(*node)
		finalHeight = node.Height
		nextCheckpointHeight = h.blkHdrProcessor.nextCheckpt.Height
	}

	// handle error
	err := h.blkHdrProcessor.store.WriteHeaders(headersArray...)

	if err != nil {
		return nil, err
	}

	return &chaindataloader.ProcessHdrResp{
		CurHeight:         finalHeight,
		NextCheckptHeight: nextCheckpointHeight,
	}, nil
}

func prepBlkHdrSideload(c *SideLoadOpt,
	hdrProcessor *blkHdrProcessor, s *sideloader) (*sideloader, error) {

	s.blkHdrProcessor = hdrProcessor

	reader, err := chaindataloader.NewBlockHeaderReader(&chaindataloader.
		BlkHdrReaderConfig{
		ReaderConfig: chaindataloader.ReaderConfig{
			SourceType: c.SourceType,
			Reader:     c.Reader,
		},
		ProcessBlkHeader: s.processSideLoadBlockHeader,
	})

	if err != nil {
		return nil, err
	}

	// If headers contained in the side load source are for a different chain
	// network return immediately.
	if reader.HeadersChain() != hdrProcessor.chainParams.Net {
		log.Errorf("headers from side load file are of network %v "+
			"and not %v as expected"+
			"-- skipping side loading", reader.HeadersChain(), hdrProcessor.
			chainParams.Net)

		return nil, nil
	}

	// Initialize the next checkpoint based on the current tipHeight.
	tipHeader, tipHeight, err := hdrProcessor.store.ChainTip()
	if err != nil {
		return nil, err
	}
	s.blkHdrSkipVerify = c.SkipVerify
	if !s.blkHdrSkipVerify {
		nextCheckpt := hdrProcessor.findNextHeaderCheckpoint(int32(tipHeight))

		if nextCheckpt == nil {
			log.Debugf("block header tip already past checkpoint cannot" +
				" verify sideload, set blkHdrSkipVerify to true if you want to " +
				"sideload anyway ---exiting sideload")

			return nil, nil
		}

		if reader.EndHeight() < uint32(nextCheckpt.Height) {
			log.Debugf("sideload endHeight, %v less than next checkpoint "+
				"tipHeight, %v cannot verify sideload, "+
				"set blkHdrSkipVerify to true if you "+
				"want to sideload anyway ---exiting sideload",
				reader.EndHeight(), nextCheckpt.Height)

			return nil, nil
		}
		s.blkHdrProcessor.nextCheckpt = nextCheckpt
		s.blkHdrCurHeight = int32(tipHeight)
	}

	if reader.EndHeight() <= tipHeight || reader.StartHeight() > tipHeight {

		//	Log error!!!!!!!!!!!!
		return nil, nil

	}

	s.blkHdrProcessor.headerList.ResetHeaderState(headerlist.Node{
		Header: *tipHeader,
		Height: int32(tipHeight),
	})
	s.blkHdrReader = reader
	return s, err
}

func prepFilterHdrSideload(c *SideLoadOpt, s *sideloader) (*sideloader, error) {

	fTipHash, fTip, err := s.fHdrStore.ChainTip()

	if err != nil {
		return nil, err
	}

	// Filter headers should not be fetched beyond the block header's tip.
	s.fHdrCurHeight = int32(fTip)
	s.fHdrCurHash = fTipHash
	s.fHdrSkipVerify = c.SkipVerify

	var nextCheckptHeight uint32
	if !s.fHdrSkipVerify {
		s.filterCheckptHeights = chainsync.FetchHardCodedFilterHdrCheckptHeight(
			s.blkHdrProcessor.chainParams.Net)

		s.filterCheckpoints = chainsync.FetchHardCodedFilterHeaderCheckpts(
			s.blkHdrProcessor.chainParams.Net)

		var nextCheckptIdx int
		for i := 0; i < len(s.filterCheckptHeights); i++ {

			if s.filterCheckptHeights[i] > fTip {
				nextCheckptIdx = i
				nextCheckptHeight = s.filterCheckptHeights[i]
				break
			}
		}

		if nextCheckptHeight == 0 {
			log.Debug("filter tip is already past checkpoint, " +
				"cannot verify and sideload")
			return nil, nil
		}
		s.fHdrNextCheckpointIdx = nextCheckptIdx
		s.fHdrNextCheckptHeight = int32(nextCheckptHeight)

	}

	reader, err := chaindataloader.NewFilterHeaderReader(&chaindataloader.
		FilterHdrReaderConfig{
		ReaderConfig: chaindataloader.ReaderConfig{
			SourceType: c.SourceType,
			Reader:     c.Reader,
		},
		ProcessCfHeader: s.processSideloadedCfHeader,
		FilterType:      fType,
	})

	if err != nil {
		return nil, err
	}

	// If headers contained in the side load source are for a different chain
	// network return immediately.
	if reader.HeadersChain() != s.blkHdrProcessor.chainParams.Net {
		log.Error("headers from side load file are of network %v "+
			"and so incompatible with neutrino's current bitcoin network "+
			"-- skipping side loading", reader.HeadersChain())

		return nil, nil
	}

	if reader.EndHeight() <= fTip || (!s.blkHdrSkipVerify &&
		reader.EndHeight() < nextCheckptHeight) || reader.StartHeight() > fTip {

		log.Warnf("Unable to fetch cfheaders")
		return nil, nil

	}

	s.cfHdrReader = reader

	return s, nil
}

// sideLoadHeaders is the general ingestion loop for side loading headers.
func (s *sideloader) sideLoadHeaders(curHeight, nextCheckptHeight int32,
	blkhdr bool) error {

	var (
		reader chaindataloader.Reader
		verify bool
	)

	if blkhdr {
		reader = s.blkHdrReader
		verify = s.blkHdrSkipVerify

	} else {
		reader = s.cfHdrReader
		verify = s.fHdrSkipVerify
	}

	err := reader.SetHeight(uint32(curHeight))
	if err != nil {
		return err
	}

	if !verify {
		log.Debugf("sideloading from height to %v to last checkpoint",
			curHeight)
	} else {
		log.Debugf("sideloading from height to %v, chunk size=%v, "+
			"to height=%v", curHeight, sideloadDataChunkSize, reader.
			EndHeight())
	}
	for int32(reader.EndHeight()) >= nextCheckptHeight {

		n := sideloadDataChunkSize

		if !verify {
			n = nextCheckptHeight - curHeight
		}

		resp, err := reader.Load(uint32(n))

		if err != nil {
			return err
		}

		// If the current height is unchanged it means no header was written in
		// the store and so we are done. `resp` should not be nil if we are
		// verifying headers as we need it for the next call, if it is nil we
		// stop sideloading.
		if (!verify && resp == nil) || resp.CurHeight == curHeight {
			log.Infof("Halted sideloading at curHeight - %v", curHeight)
			return nil
		}
		curHeight = resp.CurHeight
		nextCheckptHeight = resp.NextCheckptHeight

	}

	return nil
}
