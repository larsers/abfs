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
	"maps"
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
	// List is the condemned list of the cycle in the server bucket.
	List string `json:"list,omitempty"`
	// Condemned counts the objects marked, per type in Types, and their
	// size.
	Condemned      int64            `json:"condemned"`
	CondemnedBytes int64            `json:"condemned_bytes"`
	Types          map[string]int64 `json:"types,omitempty"`
	// ListsRemoved counts the list objects of older cycles deleted.
	ListsRemoved int64 `json:"lists_removed"`
}

// ErrCondemnRefused reports that a condemn cycle found its preconditions
// broken and wrote nothing.
type ErrCondemnRefused struct { //nolint:errname // upstream names typed errors ErrX, as remote.ErrObjectLinkValidation
	Reasons []string
}

var _ error = (*ErrCondemnRefused)(nil)

func (e *ErrCondemnRefused) Error() string {
	return "the GC cycle refused to condemn: " + strings.Join(e.Reasons, "; ")
}

// Condemn runs one garbage collection cycle that condemns: the steps of a dry
// run, then, unless condemnBlockers finds a reason to refuse, it publishes
// the list of the unreachable objects in bucket and sets their marker
// g:condemned to the cycle number. It deletes nothing. A refused cycle
// returns its result with an *ErrCondemnRefused.
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
	string(RefSource), string(RefProjectStatus), string(RefPushedPacks), string(RefWorkspace), string(RefSnapshot), string(RefMount), RootPermanent,
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
// against the expected number or range, and that each has one live row.
func replicaCountProblems(rep *ReplicaReport, o *CondemnOpts) []string {
	var problems []string
	add := func(format string, args ...any) {
		problems = append(problems, fmt.Sprintf(format, args...))
	}
	switch {
	case o.ExpectedReplicas > 0 && rep.Hosts != int64(o.ExpectedReplicas):
		add("%d server hosts write a GC heartbeat, but %d are expected", rep.Hosts, o.ExpectedReplicas)
	case o.ExpectedReplicas == 0 && rep.Hosts < int64(o.MinReplicas):
		add("%d server hosts write a GC heartbeat, but at least %d are expected", rep.Hosts, o.MinReplicas)
	case o.ExpectedReplicas == 0 && o.MaxReplicas > 0 && rep.Hosts > int64(o.MaxReplicas):
		add("%d server hosts write a GC heartbeat, but at most %d are expected", rep.Hosts, o.MaxReplicas)
	}
	for _, host := range slices.Sorted(maps.Keys(rep.RowsPerHost)) {
		if n := rep.RowsPerHost[host]; n > 1 {
			add("host %s has %d live GC heartbeat rows: a server there stopped without removing its row, and the touches it held may be lost", host, n)
		}
	}
	return problems
}

// condemner runs the condemn step of a cycle.
type condemner struct {
	opts   *CondemnOpts
	bt     *bigtable.Client
	bucket *storage.BucketHandle
	budget writeBudget
	// prev is the list of the last condemn cycle.
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

	// The markers of the previous list's live keys are cleared before this
	// cycle's list replaces it in GcState, so that a crash in between leaves
	// the previous list in place for the next cycle to clear again.
	var err error
	if r.PreviousList, err = c.clearLive(ctx, lease, res); err != nil {
		return err
	}
	nodes := res.unreachable
	if c.del != nil {
		deleted, err := c.del.run(ctx, lease, res)
		if err != nil {
			return err
		}
		// A marker written to a deleted or gone row would make a row of its
		// own.
		if deleted != nil {
			nodes = slices.DeleteFunc(slices.Clone(nodes), func(n sizedNode) bool { return deleted.has(n.id) })
		}
	}

	// The list comes before any marker, so that every marker a crashed run
	// leaves is in a list the next cycle reads.
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
	if err := lease.CompleteList(ctx); err != nil {
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
	log.Printf("GC cycle %d condemned %d objects of %d bytes, listed in %s", r.Cycle, rep.Condemned, rep.CondemnedBytes, name)
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
// number whatever cells an earlier cycle left.
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
