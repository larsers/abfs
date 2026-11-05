// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gc

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/grpc/codes"
)

const (
	stateTable           = "GcState"
	DefaultStateName     = "default"
	DefaultLeaseDuration = 5 * time.Minute
	PhaseDryRun          = "dry-run"
	minLeaseDuration     = 3 * time.Second
	releaseTimeout       = 10 * time.Second
)

// ErrLeaseLost reports that another process took the lease over.
var ErrLeaseLost = errors.New("the GC lease was taken over by another process")

// errLeaseExpiring stops a run whose lease could not be renewed in time.
var errLeaseExpiring = errors.New("the GC lease could not be renewed before it expires")

// ErrLeaseHeld reports that another process holds a valid lease.
type ErrLeaseHeld struct { //nolint:errname // upstream names typed errors ErrX, as remote.ErrObjectLinkValidation
	Holder string
	Expiry time.Time
}

var _ error = (*ErrLeaseHeld)(nil)

func (e *ErrLeaseHeld) Error() string {
	return fmt.Sprintf("another garbage collector (%s) holds the lease until %s", e.Holder, e.Expiry.UTC().Format(time.RFC3339))
}

// Lease is the right to run a GC cycle, held in the GcState row of one
// garbage collector. Each new lease increments LeaseEpoch, and every write
// checks the holder and the epoch, so a process that lost its lease cannot
// write the state.
type Lease struct {
	client   *spanner.Client
	name     string
	holder   string
	duration time.Duration
	// Cycle and Epoch are the cycle number and the lease epoch this lease
	// recorded.
	Cycle int64
	Epoch int64
	// Start is the commit time of the transaction that took the lease.
	Start time.Time
	// taken is the local time just before the lease was taken, the base of the
	// watchdog.
	taken time.Time
}

// DefaultHolder names this process as a lease holder.
func DefaultHolder() string {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}
	return fmt.Sprintf("%s-%d-%d", host, os.Getpid(), time.Now().UnixNano())
}

// AcquireLease takes the lease of the garbage collector name for duration and
// starts a new cycle in the given phase: it increments the cycle number and
// the lease epoch and records the start time and grace. It fails with
// *ErrLeaseHeld while another holder's lease is valid.
func AcquireLease(ctx context.Context, client *spanner.Client, name, holder, phase string, duration, grace time.Duration) (*Lease, error) {
	if duration < minLeaseDuration {
		return nil, fmt.Errorf("lease duration %s is shorter than %s", duration, minLeaseDuration)
	}
	l := &Lease{client: client, name: name, holder: holder, duration: duration, taken: time.Now()}
	commit, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		now, err := spannerNow(ctx, txn)
		if err != nil {
			return err
		}
		var cycle, epoch int64
		var prevStart spanner.NullTime
		row, err := txn.ReadRow(ctx, stateTable, spanner.Key{name}, []string{"Cycle", "LeaseHolder", "LeaseEpoch", "LeaseExpiry", "CycleStartTime"})
		switch {
		case spanner.ErrCode(err) == codes.NotFound:
		case err != nil:
			return err
		default:
			var current spanner.NullString
			var expiry spanner.NullTime
			if err := row.Columns(&cycle, &current, &epoch, &expiry, &prevStart); err != nil {
				return err
			}
			if current.Valid && current.StringVal != holder && expiry.Valid && expiry.Time.After(now) {
				return &ErrLeaseHeld{Holder: current.StringVal, Expiry: expiry.Time}
			}
		}
		l.Cycle, l.Epoch = cycle+1, epoch+1
		return txn.BufferWrite([]*spanner.Mutation{spanner.InsertOrUpdateMap(stateTable, map[string]any{
			"Name":                   name,
			"Cycle":                  l.Cycle,
			"Phase":                  phase,
			"LeaseHolder":            holder,
			"LeaseEpoch":             l.Epoch,
			"LeaseExpiry":            now.Add(duration),
			"CycleStartTime":         now,
			"PreviousCycleStartTime": prevStart,
			"GraceSeconds":           int64(grace / time.Second),
			"UpdateTime":             spanner.CommitTimestamp,
		})})
	})
	if err != nil {
		var held *ErrLeaseHeld
		if errors.As(err, &held) {
			return nil, held
		}
		return nil, fmt.Errorf("failed to take the GC lease: %w", err)
	}
	l.Start = commit
	return l, nil
}

// RecordSnapshot records the time of the snapshot of the roots.
func (l *Lease) RecordSnapshot(ctx context.Context, snapshot time.Time) error {
	if err := l.update(ctx, func(time.Time) map[string]any {
		return map[string]any{"SnapshotTime": snapshot}
	}); err != nil {
		return fmt.Errorf("failed to record the GC snapshot time: %w", err)
	}
	return nil
}

// Release gives the lease up, so that the next run need not wait for it to
// expire.
func (l *Lease) Release() {
	ctx, cancel := context.WithTimeout(context.Background(), releaseTimeout)
	defer cancel()
	err := l.update(ctx, func(time.Time) map[string]any {
		return map[string]any{"LeaseHolder": spanner.NullString{}, "LeaseExpiry": spanner.NullTime{}}
	})
	if err != nil {
		log.Printf("Warning: failed to release the GC lease: %v", err)
	}
}

func (l *Lease) renew(ctx context.Context) error {
	return l.update(ctx, func(now time.Time) map[string]any {
		return map[string]any{"LeaseExpiry": now.Add(l.duration)}
	})
}

// update writes the columns that set returns, given the Spanner time, if the
// lease is still ours.
func (l *Lease) update(ctx context.Context, set func(now time.Time) map[string]any) error {
	_, err := l.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		row, err := txn.ReadRow(ctx, stateTable, spanner.Key{l.name}, []string{"LeaseHolder", "LeaseEpoch"})
		if spanner.ErrCode(err) == codes.NotFound {
			return ErrLeaseLost
		}
		if err != nil {
			return err
		}
		var holder spanner.NullString
		var epoch int64
		if err := row.Columns(&holder, &epoch); err != nil {
			return err
		}
		if !holder.Valid || holder.StringVal != l.holder || epoch != l.Epoch {
			return ErrLeaseLost
		}
		now, err := spannerNow(ctx, txn)
		if err != nil {
			return err
		}
		cols := set(now)
		cols["Name"] = l.name
		cols["UpdateTime"] = spanner.CommitTimestamp
		return txn.BufferWrite([]*spanner.Mutation{spanner.UpdateMap(stateTable, cols)})
	})
	if errors.Is(err, ErrLeaseLost) {
		return ErrLeaseLost
	}
	return err
}

// margin is how long before the lease expires the watchdog stops the run.
func (l *Lease) margin() time.Duration {
	return l.duration / 4
}

// Keep renews the lease in the background until stop is called. The returned
// context is canceled when the lease is lost, or when it is about to expire
// because renewals failed; context.Cause tells which. The watchdog runs on the
// local clock from the start of the last successful renewal, which is never
// later than the expiry Spanner recorded.
func (l *Lease) Keep(ctx context.Context) (_ context.Context, stop func()) {
	ctx, cancel := context.WithCancelCause(ctx)
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		deadline := l.taken.Add(l.duration - l.margin())
		watchdog := time.NewTimer(time.Until(deadline))
		defer watchdog.Stop()
		ticker := time.NewTicker(l.duration / 3)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-watchdog.C:
				cancel(errLeaseExpiring)
				return
			case <-ticker.C:
				start := time.Now()
				rctx, rcancel := context.WithDeadline(ctx, deadline)
				err := l.renew(rctx)
				rcancel()
				switch {
				case err == nil:
					deadline = start.Add(l.duration - l.margin())
					watchdog.Reset(time.Until(deadline))
				case errors.Is(err, ErrLeaseLost):
					cancel(ErrLeaseLost)
					return
				default:
					log.Printf("Warning: failed to renew the GC lease: %v", err)
				}
			}
		}
	}()
	return ctx, func() {
		close(done)
		<-stopped
		cancel(nil)
	}
}

func spannerNow(ctx context.Context, txn *spanner.ReadWriteTransaction) (time.Time, error) {
	var now time.Time
	err := txn.Query(ctx, spanner.Statement{SQL: "SELECT CURRENT_TIMESTAMP()"}).Do(func(row *spanner.Row) error {
		return row.Columns(&now)
	})
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to read the Spanner time: %w", err)
	}
	return now, nil
}
