package memstore

import (
	"fmt"
	"sync"

	"github.com/btcsuite/btcd/btcutil/v2/gcs"
	"github.com/btcsuite/btcd/btcutil/v2/gcs/builder"
	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/lightninglabs/neutrino/filterdb"
)

type filterKey struct {
	blockHash chainhash.Hash
	typeID    filterdb.FilterType
}

// FilterStore is an in-memory implementation of filterdb.FilterDatabase.
type FilterStore struct {
	mu      sync.RWMutex
	filters map[filterKey][]byte
}

var _ filterdb.FilterDatabase = (*FilterStore)(nil)

// NewFilterStore returns a filter store initialized with the basic genesis
// filter for the target chain.
func NewFilterStore(params *chaincfg.Params) (*FilterStore, error) {
	if params == nil {
		return nil, fmt.Errorf("chain parameters are required")
	}

	genesisFilter, err := builder.BuildBasicFilter(
		params.GenesisBlock, nil,
	)
	if err != nil {
		return nil, err
	}
	genesisBytes, err := genesisFilter.NBytes()
	if err != nil {
		return nil, err
	}

	store := &FilterStore{
		filters: make(map[filterKey][]byte),
	}
	store.filters[filterKey{
		blockHash: *params.GenesisHash,
		typeID:    filterdb.RegularFilter,
	}] = genesisBytes

	return store, nil
}

// PutFilters stores a batch of compact filters atomically.
func (s *FilterStore) PutFilters(filters ...*filterdb.FilterData) error {
	type encodedFilter struct {
		key   filterKey
		bytes []byte
	}

	encoded := make([]encodedFilter, len(filters))
	for i, filter := range filters {
		if filter == nil || filter.BlockHash == nil {
			return fmt.Errorf("filter and block hash are required")
		}
		if filter.Type != filterdb.RegularFilter {
			return fmt.Errorf("unknown filter type: %v", filter.Type)
		}

		var filterBytes []byte
		if filter.Filter != nil {
			var err error
			filterBytes, err = filter.Filter.NBytes()
			if err != nil {
				return err
			}
		}

		encoded[i] = encodedFilter{
			key: filterKey{
				blockHash: *filter.BlockHash,
				typeID:    filter.Type,
			},
			bytes: filterBytes,
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, filter := range encoded {
		s.filters[filter.key] = filter.bytes
	}

	return nil
}

// FetchFilter returns the compact filter for a block hash and filter type.
func (s *FilterStore) FetchFilter(blockHash *chainhash.Hash,
	filterType filterdb.FilterType) (*gcs.Filter, error) {

	if filterType != filterdb.RegularFilter {
		return nil, fmt.Errorf("unknown filter type: %v", filterType)
	}

	s.mu.RLock()
	filterBytes, ok := s.filters[filterKey{
		blockHash: *blockHash,
		typeID:    filterType,
	}]
	if ok {
		filterBytes = append([]byte(nil), filterBytes...)
	}
	s.mu.RUnlock()

	if !ok {
		return nil, filterdb.ErrFilterNotFound
	}
	if len(filterBytes) == 0 {
		return nil, nil
	}

	return gcs.FromNBytes(
		builder.DefaultP, builder.DefaultM, filterBytes,
	)
}

// PurgeFilters removes all filters of the requested type.
func (s *FilterStore) PurgeFilters(filterType filterdb.FilterType) error {
	if filterType != filterdb.RegularFilter {
		return fmt.Errorf("unknown filter type: %v", filterType)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for key := range s.filters {
		if key.typeID == filterType {
			delete(s.filters, key)
		}
	}

	return nil
}
