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
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/bigtable"

	"github.com/google/abfs/binding"
	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/objects/gitobjects"
)

// tooNewBatch is the number of keys of one point read of the objects written
// during the consistency scan, and tooNewReadsInFlight the number of batches
// checked at once. tooNewMaxWait bounds how long the scan waits for the
// checks, summed over its key ranges, which bounds how much longer a delete
// cycle runs than a condemn cycle; past it the scan stops checking, the reads
// in flight are cancelled and the cycle refuses its deletes. tooNewWaitSlice
// is the longest single wait.
const (
	tooNewBatch         = 100
	tooNewReadsInFlight = 8
	tooNewMaxWait       = 15 * time.Minute
	tooNewWaitSlice     = time.Second
)

// clientWrittenTypes are the object types that clients write into their
// local caches, which may hold the only copy of such an object.
var clientWrittenTypes = []string{string(entities.ObjectTypeBlob), string(gitobjects.ObjectTypeDir), string(binding.ObjectTypeEvalRequest), string(binding.ObjectTypeEvalResponse)}

// DeleteOpts sets a delete cycle of the garbage collector.
type DeleteOpts struct {
	CondemnOpts
	// Types lists the object types the cycle may delete; empty allows every
	// type.
	Types []string
	// AcceptMountsWithoutKeepAlive deletes objects of the types clients write
	// while live mounts run without the GC keep-alive, whose caches may hold
	// the only copy of such an object. The check then only warns.
	AcceptMountsWithoutKeepAlive bool
	// DeleteRate is the maximum number of objects deleted per second; zero
	// means no limit.
	DeleteRate float64
	// DeleteWorkers is the number of conditional deletes in flight, split
	// evenly over the key ranges of the scan; at least one per range.
	DeleteWorkers int
	// ChunkRate is the maximum number of chunks deleted per second; zero
	// means no limit.
	ChunkRate float64
	// StartDelay pauses the cycle before the delete step, for tests that use
	// an object in between.
	StartDelay time.Duration
}

// DefaultDeleteOpts returns the default options; the replica counts have no
// default.
func DefaultDeleteOpts() DeleteOpts {
	return DeleteOpts{CondemnOpts: DefaultCondemnOpts(), DeleteRate: DefaultDeleteRate, DeleteWorkers: DefaultDeleteWorkers, ChunkRate: DefaultChunkRate}
}

// Validate checks the options.
func (o *DeleteOpts) Validate() error {
	if err := o.CondemnOpts.Validate(); err != nil {
		return err
	}
	for _, t := range o.Types {
		if _, ok := typeIndex[t]; !ok {
			return fmt.Errorf("invalid object type %q to delete: the known types are %s", t, strings.Join(typeNames, ", "))
		}
	}
	switch {
	case o.DeleteRate < 0:
		return fmt.Errorf("invalid delete rate %g", o.DeleteRate)
	case o.ChunkRate < 0:
		return fmt.Errorf("invalid chunk delete rate %g", o.ChunkRate)
	case o.DeleteWorkers <= 0:
		return fmt.Errorf("invalid number of delete workers %d", o.DeleteWorkers)
	case o.DeleteWorkers < o.Shards:
		return fmt.Errorf("%d delete workers are fewer than the %d key ranges, each of which keeps at least one delete in flight", o.DeleteWorkers, o.Shards)
	case o.StartDelay < 0:
		return fmt.Errorf("invalid delete start delay %s", o.StartDelay)
	}
	return nil
}

// allows reports whether the cycle may delete objects of type t.
func (o *DeleteOpts) allows(t string) bool {
	return len(o.Types) == 0 || slices.Contains(o.Types, t)
}

// allowsClientWrittenTypes reports whether the cycle may delete objects of a
// type that clients write locally.
func (o *DeleteOpts) allowsClientWrittenTypes() bool {
	return slices.ContainsFunc(clientWrittenTypes, o.allows)
}

// DeleteReport reports the delete step of a cycle.
type DeleteReport struct {
	// Refused lists the reasons why the cycle did not delete. It then
	// condemned as a condemn cycle does.
	Refused []string `json:"refused,omitempty"`
	// Warnings lists the checks that the options turned into warnings.
	Warnings []string `json:"warnings,omitempty"`
	// TooNew reports the objects the consistency scan found without a gc row
	// because they were written during it.
	TooNew *TooNewReport `json:"too_new,omitempty"`
	// Plan reports what the cycle would delete; dry runs and condemn cycles
	// report only it. PlanError is why the cycle could not read the list
	// records, so it reports no plan.
	Plan      *DeletePlanReport `json:"plan,omitempty"`
	PlanError string            `json:"plan_error,omitempty"`
	// Step reports the delete step, when it ran.
	Step *DeleteStepReport `json:"step,omitempty"`
	// Chunks reports the chunk step, which runs in every delete cycle.
	Chunks *ChunkStepReport `json:"chunks,omitempty"`
	// Refs reports the removal of the refs past their retention, which runs
	// in every delete cycle that may condemn.
	Refs *RefRemovalReport `json:"refs,omitempty"`
	// RecordsRemoved counts the flagged delete records of older cycles
	// deleted.
	RecordsRemoved int64 `json:"records_removed"`
}

// deleteEvidence is what deleteBlockers judges, gathered by the cycle before
// its delete step.
type deleteEvidence struct {
	// snapshot is the snapshot of this cycle, and holds the records of the
	// condemned lists.
	snapshot time.Time
	holds    *listHolds
	servers  *serverEvidence
	instance instanceEvidence
	// refRemovalErr is the error of the removal of expired refs.
	refRemovalErr error
	// tooNew is the check of the objects written during the consistency
	// scan, tooNewErr why it failed.
	tooNew    *TooNewReport
	tooNewErr error
}

// newDeleteEvidence returns the evidence that the lease and the dry run res
// hold. The cycle adds the rest.
func newDeleteEvidence(lease *Lease, res *Result) *deleteEvidence {
	return &deleteEvidence{snapshot: lease.Start, holds: res.holds, servers: res.servers}
}

// deleteBlockers returns the reasons in the report r and the evidence ev why
// the cycle must not delete, and the warnings of the checks that o turns into
// warnings. Each condition closes a case in which a delete could remove an
// object that is still in use; the server read just before the delete step
// is recheckServers. Which keys the cycle may delete, the hold of their
// markers, is the plan's.
func deleteBlockers(r *Report, ev *deleteEvidence, o *DeleteOpts) (reasons, warnings []string) {
	add := func(format string, args ...any) {
		reasons = append(reasons, fmt.Sprintf(format, args...))
	}

	if ev.holds == nil || ev.holds.err != nil {
		add("the GC list records could not be read, so no marker is known to be complete")
	}
	if ev.servers == nil {
		add("the GC servers were not read")
	} else {
		reasons = append(reasons, windowBlockers(ev.servers, ev.snapshot, &o.DryRunOpts)...)
	}
	reasons = append(reasons, instanceBlockers(&ev.instance)...)
	if ev.refRemovalErr != nil {
		add("the removal of expired refs failed: %v", ev.refRemovalErr)
	}

	// A client cache may hold the only copy of an object it wrote, which only
	// the keep-alive and the local write check keep recent.
	if o.allowsClientWrittenTypes() {
		m := &r.LiveMounts
		if m.WithoutKeepAlive > 0 {
			msg := fmt.Sprintf("%d live mounts run without the GC keep-alive, so their caches may hold the only copy of a blob, tree or eval object they wrote%s", m.WithoutKeepAlive, mountSamples(m, "without_keepalive"))
			if o.AcceptMountsWithoutKeepAlive {
				warnings = append(warnings, msg)
			} else {
				add("%s; set gc-keepalive in their config, or pass --accept-mounts-without-keepalive", msg)
			}
		}
		if m.WithoutLocalWriteCheck > 0 {
			add("%d live mounts with the GC keep-alive run with a cacheman that does not keep locally written content until a server confirms it%s", m.WithoutLocalWriteCheck, mountSamples(m, "without_local_write_check"))
		}
		if m.TouchIntervalTooLong > 0 {
			add("%d live mounts touch their objects less often than the grace allows%s", m.TouchIntervalTooLong, mountSamples(m, "touch_interval_too_long"))
		}
	}

	// A server without --gc writes no gc row, so a new object of one has none
	// even after the scan.
	switch {
	case ev.tooNew != nil && ev.tooNew.WithoutGCRow > 0:
		add("%d objects without a gc row were written during the consistency scan, for example %s: a server without --gc is running", ev.tooNew.WithoutGCRow, strings.Join(ev.tooNew.Samples, ", "))
	case ev.tooNewErr != nil:
		add("cannot read the gc rows of the objects written during the consistency scan: %v", ev.tooNewErr)
	case r.Consistency.TooNew > 0 && ev.tooNew == nil:
		add("the gc rows of the %d objects written during the consistency scan were not checked", r.Consistency.TooNew)
	case ev.tooNew != nil && ev.tooNew.Keys != r.Consistency.TooNew:
		add("only %d of the %d objects written during the consistency scan were checked for a gc row: the checks fell behind the writes", ev.tooNew.Keys, r.Consistency.TooNew)
	}
	return reasons, warnings
}

// mountSamples describes the first samples of a count of m.
func mountSamples(m *MountCounts, count string) string {
	samples := m.Samples[count]
	if len(samples) == 0 {
		return ""
	}
	var parts []string
	for _, s := range samples[:min(len(samples), 3)] {
		parts = append(parts, fmt.Sprintf("details %s of %s@%s", s.Details, s.User, s.Host))
	}
	return ", for example " + strings.Join(parts, ", ")
}

// recheckServers reads the GC servers again just before the delete step and
// returns the reasons why the cycle must not delete: the evidence of the
// first read must hold again on this read, and the touch interval of the live
// mounts in r must still fit the servers that run now when o allows types
// that clients write. It fails only when ctx ends.
func recheckServers(ctx context.Context, lease *Lease, r *Report, o *DeleteOpts) ([]string, error) {
	return checkServersAgain(ctx, func(ctx context.Context) (*serverEvidence, error) {
		return readServerEvidence(ctx, lease.client)
	}, r, o)
}

// checkServersAgain is recheckServers with the read of read.
func checkServersAgain(ctx context.Context, read func(ctx context.Context) (*serverEvidence, error), r *Report, o *DeleteOpts) ([]string, error) {
	e, err := read(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil, err
		}
		return []string{fmt.Sprintf("the second read of the GC servers failed: %v", err)}, nil
	}
	reasons := slices.Concat(windowBlockers(e, r.SnapshotTime, &o.DryRunOpts), liveBlockers(e, &o.CondemnOpts))
	if o.allowsClientWrittenTypes() && r.LiveMounts.longestTouch > 0 {
		if !keepAliveTouchFits(r.LiveMounts.longestTouch, e.keepAliveTouchLimit(r.SnapshotTime, o.Grace)) {
			reasons = append(reasons, fmt.Sprintf("live mounts touch their objects every %s, too seldom for the grace %s less the longest touch interval and cache TTL of the servers that run now", r.LiveMounts.longestTouch, o.Grace))
		}
	}
	return reasons, nil
}

// TooNewReport reports the objects that the consistency scan found without a
// gc row because they were written during it, read again while it ran.
type TooNewReport struct {
	// Keys counts the keys checked.
	Keys int64 `json:"keys"`
	// NotHashes counts keys that are not object hashes, which have no gc row.
	NotHashes int64 `json:"not_hashes"`
	// Gone counts objects whose row is gone again.
	Gone int64 `json:"gone"`
	// WithoutGCRow counts objects that still have no gc row, with up to
	// maxMissingKept keys in Samples. A server without --gc wrote them.
	WithoutGCRow int64    `json:"without_gc_row"`
	Samples      []string `json:"samples,omitempty"`
}

// tooNewCheck checks the objects that the consistency scan finds without a gc
// row because they were written during it, in batches while the scan runs, so
// that it holds counts and the batches in flight, not every key.
//
// It reads the gc row of a key first. A server with --gc writes the gc row of
// an object before its objects row, and the scan has read that objects row,
// so the gc row of such an object was written before the check reads it.
// Only the collector that holds the lease removes objects rows and gc rows,
// in its delete step and its chunk step, which both run after the scan and
// the check (a backfill replaces the edges of a row in one mutation). So a
// gc row that is missing was never written: a writer without --gc put the
// object. The objects row is read only for those keys, so that an object
// removed since the scan counts as gone, not as one without a gc row.
//
// A gc row can also be present for an object that a writer without --gc put:
// a backfill takes no lease and writes the rows of such objects, so one that
// reaches a key between the scan and the check hides that writer for that
// key. A server with --gc does not write the gc row of an object that exists,
// and an undo takes the lease.
type tooNewCheck struct {
	objects, gcTable *bigtable.Table
	budget           writeBudget
	// maxWait bounds the time that the scan waits for room in batches,
	// summed over its key ranges.
	maxWait time.Duration
	batches chan []string
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	// stopped is set when a read failed or the wait is used up; the check
	// then takes and reads no more batches. cut is set when the wait is used
	// up: the reads in flight are cancelled, and their errors ignored, since
	// the check is incomplete and refuses the deletes anyway.
	stopped atomic.Bool
	cut     atomic.Bool
	// mu guards waited, rep and err.
	mu     sync.Mutex
	waited time.Duration
	rep    TooNewReport
	err    error
}

// newTooNewCheck returns a check that reads with bt, each attempt within the
// budget b.
func newTooNewCheck(bt *bigtable.Client, b writeBudget) *tooNewCheck {
	return &tooNewCheck{objects: bt.Open(objectsTable), gcTable: bt.Open(TableName), budget: b, maxWait: tooNewMaxWait}
}

// start starts the workers, which check tooNewReadsInFlight batches at once.
func (c *tooNewCheck) start(ctx context.Context) {
	ctx, c.cancel = context.WithCancel(ctx)
	c.batches = make(chan []string, tooNewReadsInFlight)
	for range tooNewReadsInFlight {
		c.wg.Go(func() {
			for batch := range c.batches {
				if c.stopped.Load() {
					continue
				}
				if err := c.check(ctx, batch); err != nil {
					c.mu.Lock()
					if c.err == nil && !c.cut.Load() {
						c.err = err
					}
					c.mu.Unlock()
					c.stopped.Store(true)
				}
			}
		})
	}
}

// send queues a batch of keys and reports whether it did. It waits for room
// until ctx ends or the scan has waited maxWait in all, which stops the check
// and cancels its reads; it does not queue once the check stopped. The check
// owns a queued batch.
func (c *tooNewCheck) send(ctx context.Context, batch []string) bool {
	if c.stopped.Load() {
		return false
	}
	select {
	case c.batches <- batch:
		return true
	default:
	}
	// Each wait is at most one slice long and counted when it ends, so that
	// the ranges that wait at once share the budget.
	for {
		c.mu.Lock()
		left := c.maxWait - c.waited
		c.mu.Unlock()
		if left <= 0 {
			c.cut.Store(true)
			c.stopped.Store(true)
			c.cancel()
			return false
		}
		start := time.Now()
		t := time.NewTimer(min(left, tooNewWaitSlice))
		var queued, done bool
		select {
		case c.batches <- batch:
			queued = true
		case <-ctx.Done():
			done = true
		case <-t.C:
		}
		t.Stop()
		c.mu.Lock()
		c.waited += time.Since(start)
		c.mu.Unlock()
		if queued || done || c.stopped.Load() {
			return queued
		}
	}
}

// wait waits for the checks of the batches sent and returns the report of
// the batches checked, and the first error of a read before the wait was used
// up; after an error the batches still queued are dropped.
func (c *tooNewCheck) wait() (*TooNewReport, error) {
	close(c.batches)
	c.wg.Wait()
	c.cancel()
	rep := c.rep
	slices.Sort(rep.Samples)
	return &rep, c.err
}

// check reads the gc row of each key of batch, and the objects row of the
// keys without one. Each read is retried, each attempt within the budget.
func (c *tooNewCheck) check(ctx context.Context, batch []string) error {
	objectsFilter := bigtable.RowFilter(bigtable.ChainFilters(bigtable.FamilyFilter(objectsMetaFamily), bigtable.LatestNFilter(1), bigtable.StripValueFilter()))
	gcFilter := bigtable.RowFilter(bigtable.ChainFilters(bigtable.FamilyFilter(EdgesFamily), bigtable.ColumnFilter(LinksColumn), bigtable.LatestNFilter(1), bigtable.StripValueFilter()))
	var rep TooNewReport
	var hashes []string
	for _, k := range batch {
		if entities.IsHash(k) {
			hashes = append(hashes, k)
		} else {
			rep.NotHashes++
		}
	}
	var unlinked []string
	if len(hashes) > 0 {
		var linked map[string]bool
		err := retryWith(ctx, firstRetryWait, c.budget, func(actx context.Context) error {
			linked = make(map[string]bool, len(hashes))
			return c.gcTable.ReadRows(actx, bigtable.RowList(hashes), func(row bigtable.Row) bool {
				linked[row.Key()] = true
				return true
			}, gcFilter)
		})
		if err != nil {
			return fmt.Errorf("failed to read the gc rows of the objects written during the scan: %w", err)
		}
		for _, k := range hashes {
			if !linked[k] {
				unlinked = append(unlinked, k)
			}
		}
	}
	var missing []string
	if len(unlinked) > 0 {
		var present map[string]bool
		err := retryWith(ctx, firstRetryWait, c.budget, func(actx context.Context) error {
			present = make(map[string]bool, len(unlinked))
			return c.objects.ReadRows(actx, bigtable.RowList(unlinked), func(row bigtable.Row) bool {
				present[row.Key()] = len(row[objectsMetaFamily]) > 0
				return true
			}, objectsFilter)
		})
		if err != nil {
			return fmt.Errorf("failed to read the objects written during the scan: %w", err)
		}
		for _, k := range unlinked {
			if present[k] {
				missing = append(missing, k)
			} else {
				rep.Gone++
			}
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rep.Keys += int64(len(batch))
	c.rep.NotHashes += rep.NotHashes
	c.rep.Gone += rep.Gone
	c.rep.WithoutGCRow += int64(len(missing))
	c.rep.Samples = append(c.rep.Samples, missing[:min(len(missing), maxMissingKept-len(c.rep.Samples))]...)
	return nil
}
