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
	"sync/atomic"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/grpc/codes"
)

const (
	stateTable           = "AbfsGcState"
	listsTable           = "AbfsGcLists"
	DefaultStateName     = "default"
	DefaultLeaseDuration = 5 * time.Minute
	PhaseDryRun          = "dry-run"
	PhaseCondemn         = "condemn"
	PhaseDelete          = "delete"
	PhaseUndo            = "undo"
	minLeaseDuration     = 3 * time.Second
	releaseTimeout       = 10 * time.Second
)

// Tests replace these.
var (
	renewRetry = 10 * time.Second
	renewLease = (*Lease).renew
	wallNow    = time.Now
)

// ErrLeaseLost reports that another process took the lease over.
var ErrLeaseLost = errors.New("the GC lease was taken over by another process")

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

// Lease is the right to run a GC cycle. Every write checks the holder and the
// epoch, so a process that lost its lease cannot write the state.
type Lease struct {
	client   *spanner.Client
	name     string
	holder   string
	duration time.Duration
	// Cycle and Epoch are the cycle number and the lease epoch.
	Cycle int64
	Epoch int64
	// Start is the commit time of the lease.
	Start time.Time
	// PreviousExpiry is the expiry of the lease taken over, zero if it was
	// released. A holder that stalled may still act until then.
	PreviousExpiry time.Time
	// MaxGrace is the largest grace of any condemn or delete lease.
	MaxGrace time.Duration
	// taken is the local time just before the lease was taken.
	taken time.Time
	// expiry is the expiry Spanner recorded, in Unix nanoseconds.
	expiry atomic.Int64
}

// DefaultHolder names this process as a lease holder.
func DefaultHolder() string {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}
	return fmt.Sprintf("%s-%d-%d", host, os.Getpid(), time.Now().UnixNano())
}

// AcquireLease takes the lease of the garbage collector name and starts a new
// cycle in phase. It fails with *ErrLeaseHeld while another lease is valid.
func AcquireLease(ctx context.Context, client *spanner.Client, name, holder, phase string, duration, grace time.Duration) (*Lease, error) {
	if duration < minLeaseDuration {
		return nil, fmt.Errorf("lease duration %s is shorter than %s", duration, minLeaseDuration)
	}
	l := &Lease{client: client, name: name, holder: holder, duration: duration, taken: time.Now()}
	var leaseExpiry time.Time
	commit, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		now, err := spannerNow(ctx, txn)
		if err != nil {
			return err
		}
		leaseExpiry = now.Add(duration)
		var cycle, epoch int64
		var prevStart, expiry spanner.NullTime
		var maxGrace spanner.NullInt64
		row, err := txn.ReadRow(ctx, stateTable, spanner.Key{name}, []string{"Cycle", "LeaseHolder", "LeaseEpoch", "LeaseExpiry", "CycleStartTime", "MaxGraceSeconds"})
		switch {
		case spanner.ErrCode(err) == codes.NotFound:
		case err != nil:
			return err
		default:
			var current spanner.NullString
			if err := row.Columns(&cycle, &current, &epoch, &expiry, &prevStart, &maxGrace); err != nil {
				return err
			}
			if current.Valid && current.StringVal != holder && expiry.Valid && expiry.Time.After(now) {
				return &ErrLeaseHeld{Holder: current.StringVal, Expiry: expiry.Time}
			}
		}
		l.Cycle, l.Epoch = cycle+1, epoch+1
		l.PreviousExpiry = time.Time{}
		if expiry.Valid {
			l.PreviousExpiry = expiry.Time
		}
		l.MaxGrace = time.Duration(maxGrace.Int64) * time.Second
		cols := map[string]any{
			"Name":                   name,
			"Cycle":                  l.Cycle,
			"Phase":                  phase,
			"LeaseHolder":            holder,
			"LeaseEpoch":             l.Epoch,
			"LeaseExpiry":            leaseExpiry,
			"PreviousLeaseExpiry":    expiry,
			"CycleStartTime":         now,
			"PreviousCycleStartTime": prevStart,
			"GraceSeconds":           int64(grace / time.Second),
			"UpdateTime":             spanner.CommitTimestamp,
		}
		if phase == PhaseCondemn || phase == PhaseDelete {
			l.MaxGrace = max(l.MaxGrace, grace.Truncate(time.Second))
			cols["MaxGraceSeconds"] = int64(l.MaxGrace / time.Second)
		}
		return txn.BufferWrite([]*spanner.Mutation{spanner.InsertOrUpdateMap(stateTable, cols)})
	})
	if err != nil {
		var held *ErrLeaseHeld
		if errors.As(err, &held) {
			return nil, held
		}
		return nil, fmt.Errorf("failed to take the GC lease: %w", err)
	}
	l.Start = commit
	l.expiry.Store(leaseExpiry.UnixNano())
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

// ListState is the condemned list that AbfsGcState records, for reports.
type ListState struct {
	// Location is empty when no condemn cycle published a list.
	Location string
	// SnapshotTime is that of the cycle that wrote the list.
	SnapshotTime time.Time
	Complete     bool
	CompleteTime time.Time
}

// ReadList returns the condemned list recorded in the state of the lease.
func (l *Lease) ReadList(ctx context.Context) (ListState, error) {
	row, err := l.client.Single().ReadRow(ctx, stateTable, spanner.Key{l.name}, []string{"ListLocation", "ListSnapshotTime", "ListComplete", "ListCompleteTime"})
	if err != nil {
		return ListState{}, fmt.Errorf("failed to read the GC list state: %w", err)
	}
	var location spanner.NullString
	var snapshot, completeTime spanner.NullTime
	var complete spanner.NullBool
	if err := row.Columns(&location, &snapshot, &complete, &completeTime); err != nil {
		return ListState{}, fmt.Errorf("failed to read the GC list state: %w", err)
	}
	return ListState{Location: location.StringVal, SnapshotTime: snapshot.Time, Complete: complete.Valid && complete.Bool, CompleteTime: completeTime.Time}, nil
}

// ListRecord is a row of AbfsGcLists. A marker of its cycle counts for a delete
// only once the record is complete.
type ListRecord struct {
	Cycle        int64
	SnapshotTime time.Time
	Location     string
	// CompleteTime is zero while the list is not complete.
	CompleteTime time.Time
	CacheTTL     time.Duration
	Markers      int64
}

// Complete reports whether every marker of the list was written.
func (r *ListRecord) Complete() bool {
	return !r.CompleteTime.IsZero()
}

var listColumns = []string{"Cycle", "SnapshotTime", "Location", "CompleteTime", "CacheTTLSeconds", "Markers"}

// ReadListRecords returns the AbfsGcLists rows of the lease, by cycle.
func (l *Lease) ReadListRecords(ctx context.Context) (map[int64]*ListRecord, error) {
	records := make(map[int64]*ListRecord)
	err := l.client.Single().Read(ctx, listsTable, spanner.Key{l.name}.AsPrefix(), listColumns).Do(func(row *spanner.Row) error {
		var r ListRecord
		var location spanner.NullString
		var complete spanner.NullTime
		var ttl, markers spanner.NullInt64
		if err := row.Columns(&r.Cycle, &r.SnapshotTime, &location, &complete, &ttl, &markers); err != nil {
			return err
		}
		r.Location, r.CompleteTime, r.Markers = location.StringVal, complete.Time, markers.Int64
		r.CacheTTL = time.Duration(ttl.Int64) * time.Second
		records[r.Cycle] = &r
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to read the GC list records (Spanner table %s, which 'abfs server gcp init --gc' creates): %w", listsTable, err)
	}
	return records, nil
}

// RecordList records the condemned list of this cycle as not complete.
func (l *Lease) RecordList(ctx context.Context, location string, snapshot time.Time) error {
	insert := spanner.Insert(listsTable, []string{"Name", "Cycle", "SnapshotTime", "Location"}, []any{l.name, l.Cycle, snapshot, location})
	if err := l.updateWith(ctx, func(time.Time) map[string]any {
		return map[string]any{"ListLocation": location, "ListSnapshotTime": snapshot, "ListComplete": false, "ListCompleteTime": spanner.NullTime{}}
	}, insert); err != nil {
		return fmt.Errorf("failed to record the GC condemned list: %w", err)
	}
	return nil
}

// CompleteList records that every marker of the list of this cycle was written.
func (l *Lease) CompleteList(ctx context.Context, ttl time.Duration, markers int64) error {
	complete := spanner.Update(listsTable, []string{"Name", "Cycle", "CompleteTime", "CacheTTLSeconds", "Markers"},
		[]any{l.name, l.Cycle, spanner.CommitTimestamp, int64((ttl + time.Second - 1) / time.Second), markers})
	if err := l.updateWith(ctx, func(time.Time) map[string]any {
		return map[string]any{"ListComplete": true, "ListCompleteTime": spanner.CommitTimestamp}
	}, complete); err != nil {
		return fmt.Errorf("failed to record the GC condemned list as complete: %w", err)
	}
	return nil
}

// PruneListRecords deletes the rows of AbfsGcLists of cycles.
func (l *Lease) PruneListRecords(ctx context.Context, cycles []int64) error {
	if len(cycles) == 0 {
		return nil
	}
	muts := make([]*spanner.Mutation, len(cycles))
	for i, c := range cycles {
		muts[i] = spanner.Delete(listsTable, spanner.Key{l.name, c})
	}
	if err := l.updateWith(ctx, func(time.Time) map[string]any { return map[string]any{} }, muts...); err != nil {
		return fmt.Errorf("failed to delete old GC list records: %w", err)
	}
	return nil
}

// ClearListRecords deletes every AbfsGcLists row of the lease.
func (l *Lease) ClearListRecords(ctx context.Context) error {
	if err := l.updateWith(ctx, func(time.Time) map[string]any { return map[string]any{} }, spanner.Delete(listsTable, spanner.Key{l.name}.AsPrefix())); err != nil {
		return fmt.Errorf("failed to delete the GC list records: %w", err)
	}
	return nil
}

// Release gives the lease up so the next run need not wait for it to expire.
func (l *Lease) Release() {
	ctx, cancel := context.WithTimeout(context.Background(), releaseTimeout)
	defer cancel()
	err := l.update(ctx, func(time.Time) map[string]any {
		return map[string]any{"LeaseHolder": spanner.NullString{}, "LeaseExpiry": spanner.NullTime{}, "Phase": spanner.NullString{}}
	})
	if err != nil {
		log.Printf("Warning: failed to release the GC lease: %v", err)
	}
}

func (l *Lease) renew(ctx context.Context) error {
	var expiry time.Time
	err := l.update(ctx, func(now time.Time) map[string]any {
		expiry = now.Add(l.duration)
		return map[string]any{"LeaseExpiry": expiry}
	})
	if err == nil {
		l.expiry.Store(expiry.UnixNano())
	}
	return err
}

// checkWrite uses the wall clock, not the monotonic one, so a process that was
// paused stops writing before another holder can take the lease over.
func (l *Lease) checkWrite(d time.Duration) error {
	if wallNow().Add(2*d + clockSkewMargin).After(time.Unix(0, l.expiry.Load())) {
		return errLeaseExpiring
	}
	return nil
}

func (l *Lease) update(ctx context.Context, set func(now time.Time) map[string]any) error {
	return l.updateWith(ctx, set)
}

func (l *Lease) updateWith(ctx context.Context, set func(now time.Time) map[string]any, extra ...*spanner.Mutation) error {
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
		return txn.BufferWrite(append([]*spanner.Mutation{spanner.UpdateMap(stateTable, cols)}, extra...))
	})
	if errors.Is(err, ErrLeaseLost) {
		return ErrLeaseLost
	}
	return err
}

func (l *Lease) margin() time.Duration {
	return l.duration / 4
}

// Keep renews the lease until stop is called. The returned context is canceled
// when the lease is lost or about to expire; context.Cause tells which.
func (l *Lease) Keep(ctx context.Context) (_ context.Context, stop func()) {
	ctx, cancel := context.WithCancelCause(ctx)
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		deadline := l.taken.Add(l.duration - l.margin())
		watchdog := time.NewTimer(time.Until(deadline))
		defer watchdog.Stop()
		next := time.NewTimer(l.duration / 3)
		defer next.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-watchdog.C:
				cancel(errLeaseExpiring)
				return
			case <-next.C:
				// The watchdog counts from before the renewal, never after Spanner's expiry.
				start := time.Now()
				rctx, rcancel := context.WithDeadline(ctx, deadline)
				err := renewLease(l, rctx)
				rcancel()
				switch {
				case err == nil:
					deadline = start.Add(l.duration - l.margin())
					watchdog.Reset(time.Until(deadline))
					next.Reset(l.duration / 3)
				case errors.Is(err, ErrLeaseLost):
					cancel(ErrLeaseLost)
					return
				default:
					log.Printf("Warning: failed to renew the GC lease: %v", err)
					next.Reset(min(renewRetry, l.duration/3))
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
