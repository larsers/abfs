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
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/spanner"
	"cloud.google.com/go/storage"

	"github.com/google/abfs/shared/remote"
)

const (
	DefaultCondemnRate  = 5000
	DefaultKeepLists    = 7
	DefaultCondemnBatch = 2000
)

// CondemnOpts sets a condemn cycle of the garbage collector.
type CondemnOpts struct {
	DryRunOpts
	// ExpectedReplicas is the number of distinct server hosts that must
	// write a GC heartbeat. A fleet whose size changes, such as an autoscaled
	// one, sets MinReplicas instead, and MaxReplicas when it has a bound; zero
	// means no upper bound. The evidence of each server comes from its own
	// heartbeat rows, so the count only notices servers that write none.
	ExpectedReplicas int
	MinReplicas      int
	MaxReplicas      int
	// Rate is the maximum number of rows marked per second; zero means no
	// limit.
	Rate float64
	// KeepLists is the number of condemned lists kept in the bucket.
	KeepLists int
	// BatchSize is the number of rows of one bulk write; zero uses
	// DefaultCondemnBatch.
	BatchSize int
}

// DefaultCondemnOpts returns the default options; the replica counts have no
// default.
func DefaultCondemnOpts() CondemnOpts {
	return CondemnOpts{DryRunOpts: DefaultDryRunOpts(), Rate: DefaultCondemnRate, KeepLists: DefaultKeepLists}
}

// Validate checks the options.
func (o *CondemnOpts) Validate() error {
	if err := o.DryRunOpts.Validate(); err != nil {
		return err
	}
	switch {
	case !o.Consistency:
		return errors.New("a condemn cycle needs the consistency check")
	case o.ExpectedReplicas < 0 || o.MinReplicas < 0 || o.MaxReplicas < 0:
		return fmt.Errorf("invalid expected replicas %d, min replicas %d or max replicas %d: they cannot be negative", o.ExpectedReplicas, o.MinReplicas, o.MaxReplicas)
	case o.ExpectedReplicas == 0 && o.MinReplicas == 0:
		return fmt.Errorf("invalid expected replicas %d: a condemn cycle needs the number of server hosts that run with GC, or the smallest number with min replicas", o.ExpectedReplicas)
	case o.ExpectedReplicas > 0 && (o.MinReplicas > 0 || o.MaxReplicas > 0):
		return errors.New("expected replicas excludes min and max replicas")
	case o.MaxReplicas > 0 && o.MaxReplicas < o.MinReplicas:
		return fmt.Errorf("invalid max replicas %d: it is below min replicas %d", o.MaxReplicas, o.MinReplicas)
	case o.KeepLists <= 0:
		return fmt.Errorf("invalid number of kept lists %d", o.KeepLists)
	case o.Rate < 0 || o.BatchSize < 0:
		return fmt.Errorf("invalid rate %g or batch size %d", o.Rate, o.BatchSize)
	}
	return nil
}

// CondemnReport reports the condemn step of a cycle.
type CondemnReport struct {
	ExpectedReplicas int `json:"expected_replicas,omitempty"`
	MinReplicas      int `json:"min_replicas,omitempty"`
	MaxReplicas      int `json:"max_replicas,omitempty"`
	// Refused lists the reasons why the cycle did not condemn. It then wrote
	// no marker and no list.
	Refused []string `json:"refused,omitempty"`
	// Cleared counts the markers of live objects the cycle deleted, and
	// LiveList names the object that lists them.
	Cleared  int64  `json:"cleared"`
	LiveList string `json:"live_list,omitempty"`
	// List is the condemned list of the cycle in the server bucket.
	List string `json:"list,omitempty"`
	// Condemned counts the objects marked, per type in Types, and their
	// size: the unreachable objects without a marker or with the marker of a
	// list without a complete record. Kept counts the unreachable objects
	// whose recorded marker the cycle kept.
	Condemned      int64            `json:"condemned"`
	CondemnedBytes int64            `json:"condemned_bytes"`
	Types          map[string]int64 `json:"types,omitempty"`
	Kept           int64            `json:"kept"`
	// CacheTTLHours is the largest cache TTL of the servers that the list
	// record holds. NotCompleted lists why the cycle did not complete its
	// list; a later cycle then writes its markers again.
	CacheTTLHours float64  `json:"cache_ttl_hours"`
	NotCompleted  []string `json:"not_completed,omitempty"`
	// ListsRemoved counts the list objects of older cycles deleted, and
	// RecordsRemoved the rows of AbfsGcLists of lists no object holds the marker
	// of.
	ListsRemoved   int64 `json:"lists_removed"`
	RecordsRemoved int64 `json:"records_removed"`
	// RecordsCleared reports that the cycle found objects without a gc row,
	// which only servers without --gc write, and deleted every row of
	// AbfsGcLists before any other step, so that every marker is written again
	// and held anew.
	RecordsCleared bool `json:"records_cleared,omitempty"`
}

// ErrCondemnRefused reports that a condemn cycle found its preconditions
// broken and wrote no marker and no list.
type ErrCondemnRefused struct { //nolint:errname // upstream names typed errors ErrX, as remote.ErrObjectLinkValidation
	Reasons []string
}

var _ error = (*ErrCondemnRefused)(nil)

func (e *ErrCondemnRefused) Error() string {
	return "the GC cycle refused to condemn: " + strings.Join(e.Reasons, "; ")
}

// Condemn runs one garbage collection cycle that condemns: the steps of a dry
// run, then, unless condemnBlockers finds a reason to refuse, it clears the
// markers of the live objects, publishes the list of the unreachable objects
// that hold no marker of a complete list in bucket, sets their marker
// abfsgc:condemned to the cycle number and completes the list in AbfsGcLists. A
// marker of a complete list is never written again. It deletes nothing. A
// refused cycle returns its result with an *ErrCondemnRefused.
func Condemn(ctx context.Context, sp *spanner.Client, bt *bigtable.Client, bucket *storage.BucketHandle, objs remote.RemoteProvider, opts *CondemnOpts) (*Result, error) {
	o := *opts
	if err := o.Validate(); err != nil {
		return nil, err
	}
	if o.BatchSize == 0 {
		o.BatchSize = DefaultCondemnBatch
	}
	c := &condemner{opts: &o, bt: bt, bucket: bucket}
	return runCycle(ctx, sp, bt, bucket, objs, &o.DryRunOpts, c)
}

// blockingRootClasses are the root classes in which a root without a gc row
// stops a condemn: what such a root keeps is not known.
var blockingRootClasses = []string{
	string(RefSource), string(RefProjectStatus), string(RefPushedPacks), string(RefConfig), string(RefACL), string(RefWorkspace), string(RefSnapshot), string(RefMount), RootPermanent,
}

// condemnBlockers returns the reasons in the report r why the cycle must not
// condemn: a server that could answer for a condemned object without
// rescuing it, or a mark that may have missed what a root keeps.
func condemnBlockers(r *Report, o *CondemnOpts) []string {
	var reasons []string
	add := func(format string, args ...any) {
		reasons = append(reasons, fmt.Sprintf(format, args...))
	}
	for _, w := range r.Replicas.Warnings {
		add("%s", w)
	}
	reasons = append(reasons, replicaCountProblems(&r.Replicas, o)...)
	switch {
	case !r.Consistency.Ran:
		add("the consistency check did not run")
	case r.Consistency.MissingGCRows > 0:
		add("%d objects have no gc row; run gc backfill", r.Consistency.MissingGCRows)
	}
	if r.Objects.OutdatedRows > 0 {
		add("%d gc rows have outdated edge rules; run gc backfill", r.Objects.OutdatedRows)
	}
	if r.Objects.NewerRows > 0 {
		add("%d gc rows have newer edge rules than this collector", r.Objects.NewerRows)
	}
	if r.LiveMounts.Errors > 0 {
		add("the pins of %d live mounts could not be read", r.LiveMounts.Errors)
	}
	if r.Workspaces.Errors > 0 {
		add("the pins of %d workspaces could not be read", r.Workspaces.Errors)
	}
	if r.Snapshots.Errors > 0 {
		add("the pins of %d snapshots could not be read", r.Snapshots.Errors)
	}
	for _, class := range blockingRootClasses {
		if c := r.Roots[class]; c != nil && c.Dangling > 0 {
			add("%d roots of class %s have no gc row", c.Dangling, class)
		}
	}
	return reasons
}

// replicaCountProblems checks the number of hosts with a live heartbeat row
// against the expected number or range, which notices servers that write no
// row.
func replicaCountProblems(rep *ReplicaReport, o *CondemnOpts) []string {
	switch {
	case o.ExpectedReplicas > 0 && rep.Hosts != int64(o.ExpectedReplicas):
		return []string{fmt.Sprintf("%d server hosts write a GC heartbeat, but %d are expected", rep.Hosts, o.ExpectedReplicas)}
	case o.ExpectedReplicas == 0 && rep.Hosts < int64(o.MinReplicas):
		return []string{fmt.Sprintf("%d server hosts write a GC heartbeat, but at least %d are expected", rep.Hosts, o.MinReplicas)}
	case o.ExpectedReplicas == 0 && o.MaxReplicas > 0 && rep.Hosts > int64(o.MaxReplicas):
		return []string{fmt.Sprintf("%d server hosts write a GC heartbeat, but at most %d are expected", rep.Hosts, o.MaxReplicas)}
	}
	return nil
}

// condemner runs the condemn step of a cycle.
type condemner struct {
	opts   *CondemnOpts
	bt     *bigtable.Client
	bucket *storage.BucketHandle
	budget writeBudget
	// prev is the list of the last condemn cycle, for the report.
	prev previousList
	// del runs the delete steps of a delete cycle, nil in a condemn cycle.
	del *deleter
}

func (c *condemner) run(ctx context.Context, lease *Lease, res *Result) error {
	start := time.Now()
	r := res.Report
	rep := &CondemnReport{ExpectedReplicas: c.opts.ExpectedReplicas, MinReplicas: c.opts.MinReplicas, MaxReplicas: c.opts.MaxReplicas}
	r.Condemn = rep
	c.budget = writeBudget{deadline: c.opts.WriteDeadline, lease: lease}
	defer func() {
		r.Seconds.Condemn = time.Since(start).Seconds()
		r.Seconds.Total += r.Seconds.Condemn
	}()
	if err := c.clearRecords(ctx, lease, res); err != nil {
		return err
	}
	if rep.Refused = condemnBlockers(r, c.opts); len(rep.Refused) > 0 {
		for _, reason := range rep.Refused {
			log.Printf("Warning: GC cycle %d does not condemn: %s", r.Cycle, reason)
		}
		return &ErrCondemnRefused{Reasons: rep.Refused}
	}
	if c.del != nil {
		if err := c.del.check(ctx, lease, c, res); err != nil {
			return err
		}
	}

	// The markers of live keys are cleared before the delete step, so that
	// the targets of a root that came back get a full hold when they are
	// condemned again.
	cleared, err := c.clearMarked(ctx, lease, res, rep)
	if err != nil {
		return err
	}
	r.PreviousList = c.reportPrevious(res, cleared)
	nodes := slices.Clone(res.unreachable)
	if c.del != nil {
		deleted, err := c.del.run(ctx, lease, res)
		if err != nil {
			return err
		}
		// A marker written to a deleted or gone row would make a row of its
		// own.
		if deleted != nil {
			nodes = slices.DeleteFunc(nodes, func(n sizedNode) bool { return deleted.has(n.id) })
		}
	}
	// A recorded marker is never written again: its value is what a later
	// delete of the key matches. A marker without a complete record may be
	// of a run that never completed, and is replaced.
	before := len(nodes)
	nodes = slices.DeleteFunc(nodes, func(n sizedNode) bool {
		return n.marker != 0 && res.holds.recorded(res.markers[n.marker-1])
	})
	rep.Kept = int64(before - len(nodes))
	if cycles := res.holds.prunable(r.Cycle, res.markers); len(cycles) > 0 {
		if err := lease.PruneListRecords(ctx, cycles); err != nil {
			if stops(ctx, err) {
				return err
			}
			log.Printf("Warning: %v", err)
		} else {
			rep.RecordsRemoved = int64(len(cycles))
		}
	}

	// The list comes before any marker, and its record too, so that every
	// marker a crashed run leaves is of a list without a complete record,
	// which a later cycle writes again.
	name := listObjectName(c.opts.StateName, r.Cycle)
	h := &listHeader{cycle: r.Cycle, snapshot: r.SnapshotTime, holder: c.opts.Holder, epoch: lease.Epoch, count: int64(len(nodes)), types: typeNames}
	next := 0
	err = writeList(ctx, c.budget, c.bucket, name, h, func() (listRecord, bool) {
		if next == len(nodes) {
			return listRecord{}, false
		}
		n := nodes[next]
		next++
		return listRecord{key: res.graph.key(n.id), typ: res.graph.types[n.id], size: n.size}, true
	})
	if err != nil {
		return err
	}
	rep.List = name
	if err := lease.RecordList(ctx, name, r.SnapshotTime); err != nil {
		return err
	}
	rep.Types = make(map[string]int64)
	for _, n := range nodes {
		rep.CondemnedBytes += n.size
		rep.Types[typeNames[res.graph.types[n.id]]]++
	}
	if err := c.mark(ctx, res.graph, nodes, r.Cycle); err != nil {
		return err
	}
	rep.Condemned = int64(len(nodes))
	// Every marker batch returned, so every attempt applied or died before
	// this read. Each server that held information from before a marker
	// landed has a row in the window of the cycle.
	servers, err := readServerEvidence(ctx, lease.client)
	if err != nil {
		return err
	}
	ttl, problems := servers.listCacheTTL(r.SnapshotTime, c.opts.Grace)
	if len(problems) > 0 {
		rep.NotCompleted = problems
		return fmt.Errorf("GC cycle %d wrote its markers but does not complete its list, so a later cycle writes them again: %s", r.Cycle, strings.Join(problems, "; "))
	}
	rep.CacheTTLHours = ttl.Hours()
	if err := lease.CompleteList(ctx, ttl, rep.Condemned); err != nil {
		return err
	}
	if rep.ListsRemoved, err = pruneLists(ctx, c.budget, c.bucket, c.opts.StateName, c.opts.KeepLists); err != nil {
		log.Printf("Warning: failed to remove old condemned lists: %v", err)
	}
	if c.del != nil {
		if c.del.rep.RecordsRemoved, err = pruneRecords(ctx, c.budget, c.bucket, c.opts.StateName, c.opts.KeepLists); err != nil {
			log.Printf("Warning: failed to remove old delete records: %v", err)
		}
		if c.del.rep.Refs.RecordsRemoved, err = pruneRefRecords(ctx, c.budget, c.bucket, c.opts.StateName, c.opts.KeepLists); err != nil {
			log.Printf("Warning: failed to remove old ref records: %v", err)
		}
	}
	log.Printf("GC cycle %d condemned %d objects of %d bytes, listed in %s; %d keep their markers", r.Cycle, rep.Condemned, rep.CondemnedBytes, name, rep.Kept)
	return nil
}

// clearRecords deletes every list record when the scan found objects without
// a gc row: servers without --gc ran, which do not rescue, so an object used
// then may hold a marker whose hold has ended. Without a record no marker is
// eligible, and the condemn writes each one again with a hold of its own.
// It runs before the checks, since a cycle that finds such objects before
// the backfill refuses, and none is left after it.
func (c *condemner) clearRecords(ctx context.Context, lease *Lease, res *Result) error {
	r := res.Report
	n := r.Consistency.MissingGCRows
	if res.tooNew != nil {
		n += res.tooNew.WithoutGCRow
	}
	if n == 0 {
		return nil
	}
	if err := lease.ClearListRecords(ctx); err != nil {
		return err
	}
	res.holds, res.plan = &listHolds{}, nil
	if r.Delete != nil {
		r.Delete.Plan = nil
	}
	r.Condemn.RecordsCleared = true
	log.Printf("Warning: GC cycle %d found %d objects without a gc row, which a server without --gc wrote; it deleted the records of every condemned list, so that every marker is written again and held anew", r.Cycle, n)
	return nil
}

// markerValue returns the value of the condemn marker of cycle.
func markerValue(cycle int64) []byte {
	return binary.BigEndian.AppendUint64(nil, uint64(cycle))
}

// mark sets the condemn marker of the objects nodes of g to cycle, in key
// order. Only rows the consistency scan saw are written, so the write never
// creates an objects row, unless the row was deleted since. The mutation
// deletes the cells of the column first, so the row holds exactly the new
// number whatever cells a run without a complete list left.
func (c *condemner) mark(ctx context.Context, g *graph, nodes []sizedNode, cycle int64) error {
	table := c.bt.Open(objectsTable)
	lim := newLimiter(c.opts.Rate)
	value := markerValue(cycle)
	rows := make([]string, 0, c.opts.BatchSize)
	for lo := 0; lo < len(nodes); lo += c.opts.BatchSize {
		rows = rows[:0]
		for _, n := range nodes[lo:min(lo+c.opts.BatchSize, len(nodes))] {
			rows = append(rows, g.key(n.id).String())
		}
		if err := lim.wait(ctx, len(rows)); err != nil {
			return err
		}
		mut := bigtable.NewMutation()
		mut.DeleteCellsInColumn(ObjectsFamily, CondemnedColumn)
		mut.Set(ObjectsFamily, CondemnedColumn, bigtable.Now(), value)
		if err := applyBulk(ctx, c.budget, table, rows, mut); err != nil {
			return fmt.Errorf("failed to write the condemn markers of cycle %d: %w", cycle, err)
		}
	}
	return nil
}

// applyBulk applies mut to rows and retries the rows that fail with a
// transient error, with the waits of EdgeWriter, each attempt within the
// budget b. It fails on the first row that fails otherwise.
func applyBulk(ctx context.Context, b writeBudget, table *bigtable.Table, rows []string, mut *bigtable.Mutation) error {
	wait := firstRetryWait
	for attempt := 1; ; attempt++ {
		muts := make([]*bigtable.Mutation, len(rows))
		for i := range muts {
			muts[i] = mut
		}
		actx, cancel, err := b.attempt(ctx)
		if err != nil {
			return err
		}
		errs, err := table.ApplyBulk(actx, rows, muts)
		cancel()
		var retry []string
		var last error
		if err != nil {
			if !retryable(ctx, err) {
				return err
			}
			retry, last = rows, err
		}
		for i, e := range errs {
			switch {
			case e == nil:
			case !retryable(ctx, e):
				return fmt.Errorf("failed to write row %s: %w", rows[i], e)
			default:
				retry, last = append(retry, rows[i]), e
			}
		}
		if len(retry) == 0 {
			return nil
		}
		if attempt == writeAttempts {
			return fmt.Errorf("%d rows failed after %d attempts: %w", len(retry), attempt, last)
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return ctx.Err()
		}
		wait = min(2*wait, maxRetryWait)
		rows = retry
	}
}
