package neutrino

import (
	"errors"
	"fmt"

	"github.com/btcsuite/btcd/btcutil/gcs"
	"github.com/btcsuite/btcd/btcutil/gcs/builder"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/lightninglabs/neutrino/filterdb"
)

// ImportCFilter verifies a raw compact filter for the given block height
// against locally-stored filter headers and persists it to the filter database.
// Filters already present in the database are silently skipped.
//
// The filter is verified by recomputing the filter header hash and comparing it
// against the stored filter header at the same height. This ensures that even
// filters obtained from untrusted sources (such as a CDN) are
// cryptographically validated before being persisted.
func (s *ChainService) ImportCFilter(height uint32, rawFilter []byte) error {
	blockHeader, err := s.BlockHeaders.FetchHeaderByHeight(height)
	if err != nil {
		return fmt.Errorf("fetch block header at height %d: %w",
			height, err)
	}
	blockHash := blockHeader.BlockHash()

	// Skip if already stored.
	existing, err := s.FilterDB.FetchFilter(
		&blockHash, filterdb.RegularFilter,
	)
	if err == nil && existing != nil {
		return nil
	}
	if err != nil && !errors.Is(err, filterdb.ErrFilterNotFound) {
		return fmt.Errorf("fetch existing filter at height %d: %w",
			height, err)
	}

	filter, err := gcs.FromNBytes(
		builder.DefaultP, builder.DefaultM, rawFilter,
	)
	if err != nil {
		return fmt.Errorf("parse filter at height %d: %w", height, err)
	}

	err = s.verifyCFilter(height, &blockHash, filter)
	if err != nil {
		return err
	}

	err = s.FilterDB.PutFilters(&filterdb.FilterData{
		Filter:    filter,
		BlockHash: &blockHash,
		Type:      filterdb.RegularFilter,
	})
	if err != nil {
		return fmt.Errorf("store filter at height %d: %w", height, err)
	}

	_, err = s.putFilterToCache(
		&blockHash, filterdb.RegularFilter, filter,
	)
	if err != nil {
		log.Warnf("Could not cache imported filter at height %d: %v",
			height, err)
	}

	return nil
}

// ImportCFilters verifies and persists raw compact filters for a contiguous
// height range [startHeight, startHeight+len(rawFilters)-1]. Each filter is
// verified against locally-stored filter headers. Filters already in the
// database are skipped. Returns the number of newly imported filters.
//
// Filters are written to the database in a single batch for efficiency.
func (s *ChainService) ImportCFilters(
	startHeight uint32, rawFilters [][]byte,
) (int, error) {

	if len(rawFilters) == 0 {
		return 0, nil
	}

	var batch []*filterdb.FilterData
	var batchHeights []uint32
	imported := 0

	for i, raw := range rawFilters {
		height := startHeight + uint32(i)

		blockHeader, err := s.BlockHeaders.FetchHeaderByHeight(height)
		if err != nil {
			return imported, fmt.Errorf(
				"fetch block header at height %d: %w",
				height, err,
			)
		}
		blockHash := blockHeader.BlockHash()

		// Skip if already stored.
		existing, err := s.FilterDB.FetchFilter(
			&blockHash, filterdb.RegularFilter,
		)
		if err == nil && existing != nil {
			continue
		}
		if err != nil && !errors.Is(err, filterdb.ErrFilterNotFound) {
			return imported, fmt.Errorf(
				"fetch existing filter at height %d: %w",
				height, err,
			)
		}

		filter, err := gcs.FromNBytes(
			builder.DefaultP, builder.DefaultM, raw,
		)
		if err != nil {
			return imported, fmt.Errorf(
				"parse filter at height %d: %w", height, err,
			)
		}

		err = s.verifyCFilter(height, &blockHash, filter)
		if err != nil {
			return imported, err
		}

		hashCopy := blockHash
		batch = append(batch, &filterdb.FilterData{
			Filter:    filter,
			BlockHash: &hashCopy,
			Type:      filterdb.RegularFilter,
		})
		batchHeights = append(batchHeights, height)
		imported++
	}

	if len(batch) > 0 {
		if err := s.FilterDB.PutFilters(batch...); err != nil {
			return 0, fmt.Errorf("store filters batch: %w", err)
		}

		for i, filterData := range batch {
			_, err := s.putFilterToCache(
				filterData.BlockHash, filterData.Type,
				filterData.Filter,
			)
			if err != nil {
				log.Warnf(
					"Could not cache imported filter "+
						"at height %d: %v",
					batchHeights[i], err,
				)
			}
		}
	}

	return imported, nil
}

// verifyCFilter checks that a parsed compact filter matches the locally-stored
// filter header at the given height. The verification recomputes the filter
// header from the filter and the previous filter header, then compares it with
// the stored value.
func (s *ChainService) verifyCFilter(height uint32, blockHash *chainhash.Hash,
	filter *gcs.Filter) error {

	curFilterHeader, err := s.RegFilterHeaders.FetchHeaderByHeight(height)
	if err != nil {
		return fmt.Errorf("fetch filter header at height %d: %w",
			height, err)
	}

	var prevFilterHeader chainhash.Hash
	if height > 0 {
		prev, err := s.RegFilterHeaders.FetchHeaderByHeight(height - 1)
		if err != nil {
			return fmt.Errorf(
				"fetch prev filter header at height %d: %w",
				height-1, err,
			)
		}
		prevFilterHeader = *prev
	}

	computed, err := builder.MakeHeaderForFilter(filter, prevFilterHeader)
	if err != nil {
		return fmt.Errorf("compute filter header at height %d: %w",
			height, err)
	}

	if computed != *curFilterHeader {
		return fmt.Errorf(
			"filter verification failed at height %d: "+
				"computed header %s != stored header %s "+
				"(block %s)",
			height, computed, *curFilterHeader, blockHash,
		)
	}

	return nil
}
