package memstore

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/lightninglabs/neutrino/banman"
)

type banRecord struct {
	reason     banman.Reason
	expiration time.Time
}

// BanStore is an in-memory implementation of banman.Store.
type BanStore struct {
	mu      sync.Mutex
	records map[string]banRecord
}

var _ banman.Store = (*BanStore)(nil)

// NewBanStore returns an empty ban store.
func NewBanStore() *BanStore {
	return &BanStore{
		records: make(map[string]banRecord),
	}
}

func ipNetKey(ipNet *net.IPNet) (string, error) {
	if ipNet == nil || ipNet.IP == nil {
		return "", fmt.Errorf("IP network is required")
	}
	ones, bits := ipNet.Mask.Size()
	if ones < 0 || bits == 0 {
		return "", fmt.Errorf("invalid IP network mask")
	}

	return ipNet.String(), nil
}

// BanIPNet records a ban for an IP network.
func (s *BanStore) BanIPNet(ipNet *net.IPNet, reason banman.Reason,
	duration time.Duration) error {

	key, err := ipNetKey(ipNet)
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.records[key] = banRecord{
		reason:     reason,
		expiration: time.Now().Add(duration),
	}
	s.mu.Unlock()

	return nil
}

// Status returns the current ban status for an IP network and removes expired
// records.
func (s *BanStore) Status(ipNet *net.IPNet) (banman.Status, error) {
	key, err := ipNetKey(ipNet)
	if err != nil {
		return banman.Status{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.records[key]
	if !ok {
		return banman.Status{}, nil
	}
	if !time.Now().Before(record.expiration) {
		delete(s.records, key)
		return banman.Status{}, nil
	}

	return banman.Status{
		Banned:     true,
		Reason:     record.reason,
		Expiration: record.expiration,
	}, nil
}

// UnbanIPNet removes a ban for an IP network.
func (s *BanStore) UnbanIPNet(ipNet *net.IPNet) error {
	key, err := ipNetKey(ipNet)
	if err != nil {
		return err
	}

	s.mu.Lock()
	delete(s.records, key)
	s.mu.Unlock()

	return nil
}
