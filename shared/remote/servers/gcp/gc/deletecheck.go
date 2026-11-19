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

// tooNewMaxWait bounds the total wait of the scan for the checks; past it the
// cycle refuses its deletes.
const (
	tooNewBatch         = 100
	tooNewReadsInFlight = 8
	tooNewMaxWait       = 15 * time.Minute
	tooNewWaitSlice     = time.Second
)

// clientWrittenTypes are the types of which a client cache may hold the only copy.
var clientWrittenTypes = []string{string(entities.ObjectTypeBlob), string(gitobjects.ObjectTypeDir), string(binding.ObjectTypeEvalRequest), string(binding.ObjectTypeEvalResponse)}

// DeleteOpts sets a delete cycle of the garbage collector.
type DeleteOpts struct {
	CondemnOpts
	// Types lists the object types the cycle may delete; empty allows all.
	Types []string
	// AcceptMountsWithoutKeepAlive turns the keep-alive check into a warning.
	AcceptMountsWithoutKeepAlive bool
	// DeleteRate and ChunkRate are deletes per second; zero means no limit.
	DeleteRate    float64
	DeleteWorkers int
	ChunkRate     float64
	// StartDelay pauses the cycle before the delete step, for tests.
	StartDelay time.Duration
}

// DefaultDeleteOpts returns the default options.
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

func (o *DeleteOpts) allows(t string) bool {
	return len(o.Types) == 0 || slices.Contains(o.Types, t)
}

func (o *DeleteOpts) allowsClientWrittenTypes() bool {
	return slices.ContainsFunc(clientWrittenTypes, o.allows)
}

// DeleteReport reports the delete step of a cycle.
type DeleteReport struct {
	// Refused lists why the cycle did not delete; it then only condemned.
	Refused   []string          `json:"refused,omitempty"`
	Warnings  []string          `json:"warnings,omitempty"`
	TooNew    *TooNewReport     `json:"too_new,omitempty"`
	Plan      *DeletePlanReport `json:"plan,omitempty"`
	PlanError string            `json:"plan_error,omitempty"`
	Step      *DeleteStepReport `json:"step,omitempty"`
	Chunks    *ChunkStepReport  `json:"chunks,omitempty"`
	Refs      *RefRemovalReport `json:"refs,omitempty"`
	// RecordsRemoved counts the flagged delete records of older cycles removed.
	RecordsRemoved int64 `json:"records_removed"`
}

// deleteEvidence is what deleteBlockers judges.
type deleteEvidence struct {
	snapshot      time.Time
	holds         *listHolds
	servers       *serverEvidence
	instance      instanceEvidence
	refRemovalErr error
	tooNew        *TooNewReport
	tooNewErr     error
}

func newDeleteEvidence(lease *Lease, res *Result) *deleteEvidence {
	return &deleteEvidence{snapshot: lease.Start, holds: res.holds, servers: res.servers}
}

// deleteBlockers returns why the cycle must not delete, and the checks that o
// turns into warnings. Each reason closes a case in which a delete could
// remove an object still in use.
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

	// A client cache may hold the only copy of an object it wrote.
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

	// Only a server without --gc writes an object without a gc row.
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
// returns why the cycle must not delete. It fails only when ctx ends.
func recheckServers(ctx context.Context, lease *Lease, r *Report, o *DeleteOpts) ([]string, error) {
	return checkServersAgain(ctx, func(ctx context.Context) (*serverEvidence, error) {
		return readServerEvidence(ctx, lease.client)
	}, r, o)
}

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
// gc row because they were written during it.
type TooNewReport struct {
	Keys      int64 `json:"keys"`
	NotHashes int64 `json:"not_hashes"`
	Gone      int64 `json:"gone"`
	// WithoutGCRow counts objects that a server without --gc wrote.
	WithoutGCRow int64    `json:"without_gc_row"`
	Samples      []string `json:"samples,omitempty"`
}

// tooNewCheck checks, while the scan runs, the objects that it finds without
// a gc row because they were written during it. A server with --gc writes the
// gc row before the objects row, and only the lease holder removes rows, so a
// missing gc row was never written.
type tooNewCheck struct {
	objects, gcTable *bigtable.Table
	budget           writeBudget
	maxWait          time.Duration
	batches          chan []string
	cancel           context.CancelFunc
	wg               sync.WaitGroup
	stopped          atomic.Bool
	// cut is set when the wait is used up; errors of the cancelled reads are
	// ignored, since the incomplete check refuses the deletes anyway.
	cut atomic.Bool
	// mu guards waited, rep and err.
	mu     sync.Mutex
	waited time.Duration
	rep    TooNewReport
	err    error
}

func newTooNewCheck(bt *bigtable.Client, b writeBudget) *tooNewCheck {
	return &tooNewCheck{objects: bt.Open(objectsTable), gcTable: bt.Open(TableName), budget: b, maxWait: tooNewMaxWait}
}

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

// send queues a batch and reports whether it did. Once the scan has waited
// maxWait in all, it stops the check and cancels its reads.
func (c *tooNewCheck) send(ctx context.Context, batch []string) bool {
	if c.stopped.Load() {
		return false
	}
	select {
	case c.batches <- batch:
		return true
	default:
	}
	// Waits are counted per slice so that ranges waiting at once share the budget.
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

// wait returns the report of the batches checked and the first read error.
func (c *tooNewCheck) wait() (*TooNewReport, error) {
	close(c.batches)
	c.wg.Wait()
	c.cancel()
	rep := c.rep
	slices.Sort(rep.Samples)
	return &rep, c.err
}

// check reads the objects row only of keys without a gc row, so that an
// object removed since the scan counts as gone.
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
