package banman

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	sqldbv2 "github.com/lightningnetwork/lnd/sqldb/v2"

	"github.com/lightninglabs/neutrino/sqldb"
	"github.com/lightninglabs/neutrino/sqldb/sqlc"
)

// SQLBanStore is a SQL-backed implementation of the Store interface. It
// stores peer bans in the banned_peers table provisioned by the neutrino SQL
// migrations.
type SQLBanStore struct {
	db sqldb.BanTx

	// mtx serialises access from concurrent callers so that observable
	// semantics match the legacy walletdb-backed banStore: a Status call
	// immediately following a BanIPNet always sees the new record.
	mtx sync.Mutex
}

// A compile-time constraint to ensure SQLBanStore satisfies the Store
// interface.
var _ Store = (*SQLBanStore)(nil)

// NewSQLStore returns a SQLBanStore that uses the provided BatchedTx for all
// transactional access. The schema must already have been migrated by the
// owning sqldb.Backend.
func NewSQLStore(db sqldb.BanTx) (*SQLBanStore, error) {
	if db == nil {
		return nil, errors.New("nil ban tx executor")
	}

	return &SQLBanStore{db: db}, nil
}

// BanIPNet creates or refreshes a ban record for the given IP network. The
// duration is added to the current time to derive the absolute expiration
// stored in the database.
//
// NOTE: This method is part of the Store interface.
func (s *SQLBanStore) BanIPNet(ipNet *net.IPNet, reason Reason,
	duration time.Duration) error {

	s.mtx.Lock()
	defer s.mtx.Unlock()

	key, err := encodeIPNetKey(ipNet)
	if err != nil {
		return err
	}

	expiration := time.Now().Add(duration).UTC()

	ctx := context.Background()
	return s.db.ExecTx(
		ctx, sqldbv2.WriteTxOpt(),
		func(q sqldb.BanQueries) error {
			return q.UpsertBan(ctx, sqlc.UpsertBanParams{
				IpNet:      key,
				Expiration: expiration,
				Reason:     int32(reason),
			})
		}, sqldbv2.NoOpReset,
	)
}

// Status returns the current ban status for the given IP network. If a
// previously-recorded ban has expired, Status sweeps the row from the table
// in the same transaction and returns Banned: false.
//
// NOTE: This method is part of the Store interface.
func (s *SQLBanStore) Status(ipNet *net.IPNet) (Status, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	key, err := encodeIPNetKey(ipNet)
	if err != nil {
		return Status{}, err
	}

	var status Status

	ctx := context.Background()
	err = s.db.ExecTx(
		ctx, sqldbv2.WriteTxOpt(),
		func(q sqldb.BanQueries) error {
			row, err := q.GetBan(ctx, key)
			switch {
			case errors.Is(err, sql.ErrNoRows):
				status = Status{}
				return nil

			case err != nil:
				return err
			}

			expiration := row.Expiration
			if !time.Now().Before(expiration) {
				// The ban has expired; remove the row in the
				// same tx so the next caller observes a clean
				// state.
				if delErr := q.DeleteBan(ctx, key); delErr != nil {
					return delErr
				}

				status = Status{}
				return nil
			}

			status = Status{
				Banned:     true,
				Reason:     Reason(row.Reason),
				Expiration: expiration,
			}
			return nil
		}, sqldbv2.NoOpReset,
	)
	if err != nil {
		return Status{}, err
	}

	return status, nil
}

// UnbanIPNet removes the ban record for the given IP network. It is a no-op
// when no record exists for the network.
//
// NOTE: This method is part of the Store interface.
func (s *SQLBanStore) UnbanIPNet(ipNet *net.IPNet) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	key, err := encodeIPNetKey(ipNet)
	if err != nil {
		return err
	}

	ctx := context.Background()
	return s.db.ExecTx(
		ctx, sqldbv2.WriteTxOpt(),
		func(q sqldb.BanQueries) error {
			return q.DeleteBan(ctx, key)
		}, sqldbv2.NoOpReset,
	)
}

// PurgeExpired sweeps every ban whose expiration has elapsed and returns the
// number of rows removed. It is intended to be called opportunistically (for
// example, at neutrino startup) to bound the size of the banned_peers table.
func (s *SQLBanStore) PurgeExpired() (int64, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	var n int64
	ctx := context.Background()
	err := s.db.ExecTx(
		ctx, sqldbv2.WriteTxOpt(),
		func(q sqldb.BanQueries) error {
			rows, err := q.DeleteExpiredBans(ctx, time.Now().UTC())
			if err != nil {
				return err
			}
			n = rows
			return nil
		}, sqldbv2.NoOpReset,
	)
	return n, err
}

// encodeIPNetKey returns the canonical binary key used by both the legacy
// walletdb and SQL ban stores. The format is byte-identical to the legacy
// encoding so that the auto-migration can copy rows across without
// re-encoding.
func encodeIPNetKey(ipNet *net.IPNet) ([]byte, error) {
	if ipNet == nil {
		return nil, fmt.Errorf("nil IPNet")
	}

	var buf bytes.Buffer
	if err := encodeIPNet(&buf, ipNet); err != nil {
		return nil, fmt.Errorf("unable to encode %v: %w", ipNet, err)
	}
	return buf.Bytes(), nil
}
