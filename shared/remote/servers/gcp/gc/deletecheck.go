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
	"time"

	"cloud.google.com/go/bigtable"

	"github.com/google/abfs/binding"
	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/objects/gitobjects"
)

// statsWaitMargin is how much longer than its heartbeat interval the
// collector waits for a server to write a heartbeat whose touch stats were
// read after the snapshot, and statsPollInterval how often it reads them;
// tests change them.
var (
	statsWaitMargin   = time.Minute
	statsPollInterval = 10 * time.Second
)

// tooNewBatch is the number of keys of one point read of the objects written
// during the consistency scan.
const tooNewBatch = 100

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
	// NoBackupCheck deletes without a backup of the tables taken after the
	// list was complete.
	NoBackupCheck bool
	// DeleteRate is the maximum number of objects deleted per second; zero
	// means no limit.
	DeleteRate float64
	// DeleteWorkers is the number of conditional deletes in flight, split
	// evenly over the key ranges of the scan.
	DeleteWorkers int
	// ChunkDelay is how long after its record the chunks of a deleted object
	// are deleted. It must exceed the lifetime of the signed URLs of chunks
	// that a server hands out.
	ChunkDelay time.Duration
	// ChunkRate is the maximum number of chunks deleted per second; zero
	// means no limit.
	ChunkRate float64
	// AllowNoSoftDelete deletes bucket chunks although the bucket keeps
	// deleted objects for less than seven days, or not at all, so that they
	// cannot be restored.
	AllowNoSoftDelete bool
	// StartDelay pauses the cycle before the delete step, for tests that use
	// an object in between.
	StartDelay time.Duration
}

// DefaultDeleteOpts returns the default options; the replica counts have no
// default.
func DefaultDeleteOpts() DeleteOpts {
	return DeleteOpts{CondemnOpts: DefaultCondemnOpts(), DeleteRate: DefaultDeleteRate, DeleteWorkers: DefaultDeleteWorkers, ChunkDelay: DefaultChunkDelay, ChunkRate: DefaultChunkRate}
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
	case o.ChunkDelay <= signedURLLifetime:
		return fmt.Errorf("chunk delay %s is not longer than the %s lifetime of the signed URLs of chunks", o.ChunkDelay, signedURLLifetime)
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
	// Plan reports what the cycle would delete from the list before; dry runs
	// and condemn cycles report only it.
	Plan *DeletePlanReport `json:"plan,omitempty"`
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
	// snapshot is the snapshot of this cycle, and previousCondemnCycle the
	// cycle of the condemn or delete lease before its own.
	snapshot             time.Time
	previousCondemnCycle int64
	// list is the list GcState names, listHeader its header, nil when it
	// could not be read, and listErr why.
	list       ListState
	listHeader *listHeader
	listErr    string
	servers    *serverEvidence
	instance   instanceEvidence
	// refRemovalErr is the error of the removal of expired refs.
	refRemovalErr error
	// tooNew is the result of checkTooNew, tooNewErr why it failed.
	tooNew    *TooNewReport
	tooNewErr error
}

// newDeleteEvidence returns the evidence that the lease, the previous list
// read by c and the dry run res hold. The cycle adds the rest.
func newDeleteEvidence(lease *Lease, c *condemner, res *Result) *deleteEvidence {
	ev := &deleteEvidence{
		snapshot:             lease.Start,
		previousCondemnCycle: lease.PreviousCondemnCycle,
		list:                 c.prev.state,
		listErr:              c.prev.err,
		servers:              res.servers,
	}
	if c.prev.list != nil {
		ev.listHeader = c.prev.list.header
	}
	return ev
}

// deleteBlockers returns the reasons in the report r and the evidence ev why
// the cycle must not delete from the list before it, and the warnings of the
// checks that o turns into warnings. Each condition closes a case in which a
// delete could remove an object that is still in use; the server read just
// before the delete step is waitForServerStats.
func deleteBlockers(r *Report, ev *deleteEvidence, o *DeleteOpts) (reasons, warnings []string) {
	add := func(format string, args ...any) {
		reasons = append(reasons, fmt.Sprintf(format, args...))
	}

	// The list is the one the last condemn or delete cycle wrote, and all its
	// markers were written before its completion time.
	l := ev.list
	switch {
	case l.Location == "":
		add("no condemn cycle published a list to delete from")
	case ev.listErr != "" || ev.listHeader == nil:
		add("the list %s could not be read: %s", l.Location, ev.listErr)
	case !l.Complete:
		add("the list %s is not complete: the cycle that wrote it stopped before it wrote every marker", l.Location)
	case !l.CompleteTime.After(l.SnapshotTime):
		add("the list %s has no completion time after its snapshot: a collector of an older version wrote it", l.Location)
	case ev.listHeader.cycle != ev.previousCondemnCycle:
		add("the list %s is of cycle %d, but cycle %d was the last condemn or delete cycle and did not complete its own list", l.Location, ev.listHeader.cycle, ev.previousCondemnCycle)
	}

	if ev.servers == nil {
		add("the GC servers were not read")
	} else {
		start := windowStart(ev.snapshot, o.Grace)
		// A server may answer from information read before the last marker
		// landed until the largest cache TTL has passed; the reference or
		// touch such a use makes lands within the larger of the two write
		// bounds, and the mark of this cycle must see it.
		ttl := ev.servers.longestCacheTTL(start)
		bound := ttl + max(ValidationWriteBound, TouchDrainBound) + clockSkewMargin
		if !l.CompleteTime.IsZero() && ev.snapshot.Before(l.CompleteTime.Add(bound)) {
			add("the snapshot %s is earlier than the completion of the list at %s plus the largest server cache TTL %s, the write bound %s and the clock skew margin %s: a server may still use information read before the markers; run the cycle at %s or later",
				formatTime(ev.snapshot), formatTime(l.CompleteTime), ttl, max(ValidationWriteBound, TouchDrainBound), clockSkewMargin, formatTime(l.CompleteTime.Add(bound)))
		}
		reasons = append(reasons, windowBlockers(ev.servers, ev.snapshot, &o.DryRunOpts)...)
	}
	// An object first written or used after the snapshot of the list must
	// still be recent in this mark.
	if ev.listHeader != nil {
		prev := l.SnapshotTime
		if ev.listHeader.snapshot.Before(prev) {
			prev = ev.listHeader.snapshot
		}
		if limit := prev.Add(o.Grace - clockSkewMargin - ValidationWriteBound); !ev.snapshot.Before(limit) {
			add("the snapshot %s is not earlier than the snapshot of the list at %s plus the grace %s less the clock skew margin and the write bound: objects used after that list was taken may no longer be recent; this cycle condemns again",
				formatTime(ev.snapshot), formatTime(prev), o.Grace)
		}
	}

	reasons = append(reasons, instanceBlockers(&ev.instance, l.CompleteTime)...)
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
	case r.Consistency.TooNew > maxTooNew:
		add("%d objects written during the consistency scan had no gc row, more than %d to check again", r.Consistency.TooNew, maxTooNew)
	case ev.tooNewErr != nil:
		add("cannot read the gc rows of the objects written during the consistency scan: %v", ev.tooNewErr)
	case r.Consistency.TooNew > 0 && ev.tooNew == nil:
		add("the gc rows of the %d objects written during the consistency scan were not checked", r.Consistency.TooNew)
	case ev.tooNew != nil && ev.tooNew.WithoutGCRow > 0:
		add("%d objects without a gc row were written during the consistency scan, for example %s: a server without --gc is running", ev.tooNew.WithoutGCRow, strings.Join(ev.tooNew.Samples, ", "))
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

// waitForServerStats reads the GC servers again just before the delete step
// and returns the reasons why the cycle must not delete. Every live server
// must have written a heartbeat whose touch stats it read after the snapshot,
// so that a touch it lost since the first read shows; the collector waits up
// to the server's heartbeat interval plus statsWaitMargin for one. The
// evidence of the first read must hold again on this read, and the touch
// interval of the live mounts in r must still fit the servers that run now
// when o allows types that clients write. It fails only when ctx ends.
func waitForServerStats(ctx context.Context, lease *Lease, r *Report, o *DeleteOpts) ([]string, error) {
	return pollServerStats(ctx, func(ctx context.Context) (*serverEvidence, error) {
		return readServerEvidence(ctx, lease.client, lease.name)
	}, r, o)
}

// pollServerStats is waitForServerStats with the reads of read.
func pollServerStats(ctx context.Context, read func(ctx context.Context) (*serverEvidence, error), r *Report, o *DeleteOpts) ([]string, error) {
	start := time.Now()
	for {
		e, err := read(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil, err
			}
			return []string{fmt.Sprintf("the second read of the GC servers failed: %v", err)}, nil
		}
		reasons := slices.Concat(windowBlockers(e, r.SnapshotTime, &o.DryRunOpts), liveBlockers(e, &o.CondemnOpts))
		if o.allowsClientWrittenTypes() && r.LiveMounts.longestTouch > 0 {
			limit := o.Grace - e.longestTouchInterval(windowStart(r.SnapshotTime, o.Grace)) - clockSkewMargin
			if !keepAliveTouchFits(r.LiveMounts.longestTouch, limit) {
				reasons = append(reasons, fmt.Sprintf("live mounts touch their objects every %s, too seldom for the grace %s less the longest touch interval of the servers that run now", r.LiveMounts.longestTouch, o.Grace))
			}
		}
		if len(reasons) > 0 {
			return reasons, nil
		}
		var late []string
		waiting := false
		for _, s := range e.inWindow(windowStart(r.SnapshotTime, o.Grace)) {
			if !s.StoppedAt.IsZero() || !s.StatsTime.Before(r.SnapshotTime.Add(clockSkewMargin)) {
				continue
			}
			waiting = true
			interval := s.HeartbeatInterval
			if interval <= 0 {
				interval = MaxHeartbeatInterval
			}
			if time.Since(start) > interval+statsWaitMargin {
				late = append(late, fmt.Sprintf("server %s wrote no heartbeat with touch stats read after the snapshot within %s", s.ID, interval+statsWaitMargin))
			}
		}
		if len(late) > 0 || !waiting {
			return late, nil
		}
		select {
		case <-time.After(statsPollInterval):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// TooNewReport reports the objects that the consistency scan found without a
// gc row because they were written during it, read again after the scan.
type TooNewReport struct {
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

// checkTooNew reads the objects row and then the gc row of each key, so that
// an object written in between, gc row first, is never found without one.
func checkTooNew(ctx context.Context, bt *bigtable.Client, keys []string) (*TooNewReport, error) {
	rep := &TooNewReport{Keys: int64(len(keys))}
	objects, gcTable := bt.Open(objectsTable), bt.Open(TableName)
	objectsFilter := bigtable.RowFilter(bigtable.ChainFilters(bigtable.FamilyFilter(objectsMetaFamily), bigtable.LatestNFilter(1), bigtable.StripValueFilter()))
	gcFilter := bigtable.RowFilter(bigtable.ChainFilters(bigtable.FamilyFilter(EdgesFamily), bigtable.ColumnFilter(LinksColumn), bigtable.LatestNFilter(1), bigtable.StripValueFilter()))
	var hashes []string
	for _, k := range keys {
		if entities.IsHash(k) {
			hashes = append(hashes, k)
		} else {
			rep.NotHashes++
		}
	}
	for lo := 0; lo < len(hashes); lo += tooNewBatch {
		batch := hashes[lo:min(lo+tooNewBatch, len(hashes))]
		present := make(map[string]bool, len(batch))
		err := objects.ReadRows(ctx, bigtable.RowList(batch), func(row bigtable.Row) bool {
			present[row.Key()] = len(row[objectsMetaFamily]) > 0
			return true
		}, objectsFilter)
		if err != nil {
			return nil, fmt.Errorf("failed to read the objects written during the scan: %w", err)
		}
		var still []string
		for _, k := range batch {
			if present[k] {
				still = append(still, k)
			} else {
				rep.Gone++
			}
		}
		if len(still) == 0 {
			continue
		}
		linked := make(map[string]bool, len(still))
		err = gcTable.ReadRows(ctx, bigtable.RowList(still), func(row bigtable.Row) bool {
			linked[row.Key()] = true
			return true
		}, gcFilter)
		if err != nil {
			return nil, fmt.Errorf("failed to read the gc rows of the objects written during the scan: %w", err)
		}
		for _, k := range still {
			if linked[k] {
				continue
			}
			rep.WithoutGCRow++
			if len(rep.Samples) < maxMissingKept {
				rep.Samples = append(rep.Samples, k)
			}
		}
	}
	return rep, nil
}
