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
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/spanner"
	"cloud.google.com/go/storage"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
)

const (
	DefaultUndoWorkers = 16
	// undoMetadata raises the metageneration of a bucket chunk, so that a
	// chunk step that read the object before cannot delete it.
	undoMetadata          = "abfs-undo"
	maxNotRestoredSamples = 20
	// chunkUndoColumn gets a new cell so that the chunk step keeps the chunk
	// row; readers of a chunk read only data:content.
	chunkUndoColumn = "undo"
)

// fenceSleep waits for d unless ctx ends first; tests change it.
var fenceSleep = func(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// UndoOpts sets a run of Undo.
type UndoOpts struct {
	// Cycle and Through are the first and the last delete cycle to undo;
	// Through zero means Cycle.
	Cycle         int64
	Through       int64
	Workers       int
	LeaseDuration time.Duration
	WriteDeadline time.Duration
	Holder        string
	StateName     string
}

// DefaultUndoOpts returns the default options.
func DefaultUndoOpts() UndoOpts {
	return UndoOpts{Workers: DefaultUndoWorkers, LeaseDuration: DefaultLeaseDuration, WriteDeadline: DefaultWriteDeadline}
}

// Validate checks the options.
func (o *UndoOpts) Validate() error {
	switch {
	case o.Cycle < 1:
		return fmt.Errorf("invalid cycle %d to undo", o.Cycle)
	case o.Through != 0 && o.Through < o.Cycle:
		return fmt.Errorf("the last cycle to undo, %d, is before the first, %d", o.Through, o.Cycle)
	case o.Workers < 1:
		return fmt.Errorf("invalid number of workers %d", o.Workers)
	case o.LeaseDuration <= 0 || o.WriteDeadline <= 0:
		return fmt.Errorf("invalid lease duration %s or write deadline %s: they must be positive", o.LeaseDuration, o.WriteDeadline)
	case 2*o.WriteDeadline > o.LeaseDuration/4:
		return fmt.Errorf("write deadline %s is too long for the lease %s: two attempts must fit into a quarter of it", o.WriteDeadline, o.LeaseDuration)
	}
	return nil
}

// UndoReport reports a run of Undo.
type UndoReport struct {
	// Cycle is the cycle of the lease of the run.
	Cycle            int64   `json:"cycle"`
	FirstCycle       int64   `json:"first_cycle"`
	LastCycle        int64   `json:"last_cycle"`
	FenceWaitSeconds float64 `json:"fence_wait_seconds"`
	Records          int64   `json:"records"`
	// Objects counts the objects of the records; the counts after Present
	// were not restored.
	Objects           int64    `json:"objects"`
	Restored          int64    `json:"restored"`
	Present           int64    `json:"present"`
	MissingChunks     int64    `json:"missing_chunks"`
	WithoutGCRow      int64    `json:"without_gc_row,omitempty"`
	InvalidContent    int64    `json:"invalid_content,omitempty"`
	BlockedByChild    int64    `json:"blocked_by_child"`
	NotRestoredSample []string `json:"not_restored_sample,omitempty"`
	// RefRecords lists the refs those cycles removed; the run restores no ref.
	RefRecords           []string `json:"ref_records,omitempty"`
	ChunkRowsWritten     int64    `json:"chunk_rows_written"`
	BucketObjectsStamped int64    `json:"bucket_objects_stamped"`
	MapRows              int64    `json:"map_rows"`
	MapRowsRestored      int64    `json:"map_rows_restored"`
	MapRowsPresent       int64    `json:"map_rows_present"`
	MapRowsBlocked       int64    `json:"map_rows_blocked"`
	Seconds              float64  `json:"seconds"`
}

// NotRestored returns the number of objects and map rows not restored.
func (r *UndoReport) NotRestored() int64 {
	return r.MissingChunks + r.WithoutGCRow + r.InvalidContent + r.BlockedByChild + r.MapRowsBlocked
}

// Undo writes back the objects and map rows that the delete cycles from
// opts.Cycle to opts.Through deleted, from their delete records in bucket.
func Undo(ctx context.Context, sp *spanner.Client, bt *bigtable.Client, bucket *storage.BucketHandle, opts *UndoOpts) (*UndoReport, error) {
	o := *opts
	if err := o.Validate(); err != nil {
		return nil, err
	}
	if o.Through == 0 {
		o.Through = o.Cycle
	}
	if o.Holder == "" {
		o.Holder = DefaultHolder()
	}
	if o.StateName == "" {
		o.StateName = DefaultStateName
	}
	u := newUndoer(bt, bucket, &o)
	// A check before the lease refuses a bad range without using up a cycle.
	if err := u.listCycles(ctx); err != nil {
		return nil, err
	}
	lease, err := AcquireLease(ctx, sp, o.StateName, o.Holder, PhaseUndo, o.LeaseDuration, 0)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	runCtx, stop := lease.Keep(ctx)
	defer stop()
	if err := u.prepare(runCtx); err != nil {
		return nil, err
	}
	rep, err := u.run(runCtx, lease)
	if err != nil && runCtx.Err() != nil && ctx.Err() == nil {
		err = fmt.Errorf("the GC undo stopped: %w", context.Cause(runCtx))
	}
	if err != nil && rep.ChunkRowsWritten+rep.BucketObjectsStamped > 0 {
		err = fmt.Errorf("%w; run the undo again until it succeeds, or the chunks it kept for objects it did not finish stay in the store", err)
	}
	return rep, err
}

type undoer struct {
	opts                           *UndoOpts
	bucket                         *storage.BucketHandle
	objects, gcTable, maps, chunks *bigtable.Table
	reads                          writeBudget
	writes                         writeBudget
	rep                            *UndoReport
	records                        map[int64][]recordName
	// deleted holds the newest cycle whose records name each key.
	deletedOnce sync.Once
	deletedErr  error
	deleted     map[Hash]int64
	recordedAt  map[int64]time.Time
	pruned      []int64
	// mu guards rep, notRestored and present.
	mu          sync.Mutex
	notRestored map[Hash]bool
	present     map[Hash]bool
}

func newUndoer(bt *bigtable.Client, bucket *storage.BucketHandle, o *UndoOpts) *undoer {
	return &undoer{
		opts: o, bucket: bucket,
		objects: bt.Open(objectsTable), gcTable: bt.Open(TableName), maps: bt.Open(mapsTableName), chunks: bt.Open(chunksTableName),
		reads:       writeBudget{deadline: o.WriteDeadline},
		rep:         &UndoReport{FirstCycle: o.Cycle, LastCycle: o.Through},
		notRestored: make(map[Hash]bool),
		present:     make(map[Hash]bool),
	}
}

func (u *undoer) prepare(ctx context.Context) error {
	if err := u.listCycles(ctx); err != nil {
		return err
	}
	for cycle := u.opts.Cycle; cycle <= u.opts.Through; cycle++ {
		if len(u.records[cycle]) == 0 {
			log.Printf("GC cycle %d has no delete records: it deleted nothing", cycle)
		}
	}
	return u.listRefRecords(ctx)
}

func (u *undoer) listCycles(ctx context.Context) error {
	u.records = make(map[int64][]recordName)
	found := false
	for cycle := u.opts.Cycle; cycle <= u.opts.Through; cycle++ {
		names, err := u.listRecords(ctx, cycle)
		if err != nil {
			return err
		}
		found = found || len(names) > 0
		u.records[cycle] = names
	}
	if !found {
		return fmt.Errorf("found no delete records of the cycles %d to %d under gs://%s/%s: they deleted nothing, or the bucket or the GC state is not the one of the deletes", u.opts.Cycle, u.opts.Through, u.bucket.BucketName(), recordPrefixOf(u.opts.StateName))
	}
	return nil
}

// listRefRecords starts at the cycle before the first one, because a delete
// cycle deletes what the refs that the cycle before removed reached.
func (u *undoer) listRefRecords(ctx context.Context) error {
	prefix := refRecordPrefixOf(u.opts.StateName)
	it := u.bucket.Objects(ctx, &storage.Query{Prefix: prefix})
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to list the ref records under %s: %w", prefix, err)
		}
		cycle, err := strconv.ParseInt(strings.TrimPrefix(attrs.Name, prefix), 10, 64)
		if err == nil && cycle >= u.opts.Cycle-1 && cycle <= u.opts.Through {
			u.rep.RefRecords = append(u.rep.RefRecords, attrs.Name)
		}
	}
	for _, name := range u.rep.RefRecords {
		log.Printf("GC undo restores no refs; the ref record gs://%s/%s lists refs that were removed, which test-client put-ref can put back", u.bucket.BucketName(), name)
	}
	return nil
}

func (u *undoer) run(ctx context.Context, lease *Lease) (*UndoReport, error) {
	start := time.Now()
	r := u.rep
	r.Cycle = lease.Cycle
	defer func() { r.Seconds = time.Since(start).Seconds() }()
	u.writes = writeBudget{deadline: u.opts.WriteDeadline, lease: lease}
	wait, err := waitFence(ctx, lease, u.opts.WriteDeadline)
	r.FenceWaitSeconds = wait.Seconds()
	if err != nil {
		return r, err
	}
	for cycle := u.opts.Through; cycle >= u.opts.Cycle; cycle-- {
		if err := u.cycle(ctx, cycle); err != nil {
			return r, err
		}
	}
	log.Printf("GC undo of cycles %d to %d restored %d objects and %d map rows; %d objects and %d map rows were present, %d objects and map rows were not restored",
		u.opts.Cycle, u.opts.Through, r.Restored, r.MapRowsRestored, r.Present, r.MapRowsPresent, r.NotRestored())
	return r, nil
}

// waitFence waits until no write of a collector that lost the lease can
// land: it starts none later than two attempts and the margin before expiry.
func waitFence(ctx context.Context, lease *Lease, d time.Duration) (time.Duration, error) {
	base := lease.PreviousExpiry
	if base.IsZero() {
		base = lease.Start
	}
	fence := base.Add(2*d + clockSkewMargin)
	start := wallNow()
	if fence.After(start) {
		log.Printf("GC undo waits until %s, when a collector that lost the lease has stopped writing", fence.UTC().Format(time.RFC3339))
	}
	for {
		now := wallNow()
		if !fence.After(now) {
			return now.Sub(start), nil
		}
		if err := fenceSleep(ctx, fence.Sub(now)); err != nil {
			return now.Sub(start), err
		}
	}
}

type recordName struct {
	name              string
	cycle             int64
	created           time.Time
	level, rng, batch int
}

// parseRecordName parses <cycle>/<level>-<range>-<batch> or <cycle>/pruned.
func parseRecordName(prefix string, attrs *storage.ObjectAttrs) (n recordName, pruned, ok bool) {
	c, rest, found := strings.Cut(strings.TrimPrefix(attrs.Name, prefix), "/")
	cycle, err := strconv.ParseInt(c, 10, 64)
	if !found || err != nil {
		return n, false, false
	}
	n = recordName{name: attrs.Name, cycle: cycle, created: attrs.Created}
	if rest == prunedMarker {
		return n, true, true
	}
	parts := strings.Split(rest, "-")
	if len(parts) != 3 {
		return n, false, false
	}
	for i, p := range []*int{&n.level, &n.rng, &n.batch} {
		if *p, err = strconv.Atoi(parts[i]); err != nil {
			return n, false, false
		}
	}
	return n, false, true
}

// listRecords lists the delete records of cycle, deepest level first.
func (u *undoer) listRecords(ctx context.Context, cycle int64) ([]recordName, error) {
	prefix := recordPrefixOf(u.opts.StateName)
	var names []recordName
	it := u.bucket.Objects(ctx, &storage.Query{Prefix: prefix + strconv.FormatInt(cycle, 10) + "/"})
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list the delete records of cycle %d under %s: %w", cycle, prefix, err)
		}
		n, pruned, ok := parseRecordName(prefix, attrs)
		switch {
		case !ok:
			return nil, fmt.Errorf("unexpected object %s among the delete records", attrs.Name)
		case pruned:
			return nil, fmt.Errorf("the delete records of cycle %d were removed at %s, so the deletes of that cycle cannot be undone", cycle, formatTime(attrs.Created))
		}
		names = append(names, n)
	}
	slices.SortFunc(names, func(a, b recordName) int {
		return cmp.Or(cmp.Compare(b.level, a.level), cmp.Compare(a.rng, b.rng), cmp.Compare(a.batch, b.batch))
	})
	return names, nil
}

func (u *undoer) loadDeleted(ctx context.Context) error {
	u.deletedOnce.Do(func() {
		u.deletedErr = u.readDeleted(ctx)
		if u.deletedErr != nil {
			u.deletedErr = fmt.Errorf("failed to read the keys of the delete records: %w", u.deletedErr)
		}
	})
	return u.deletedErr
}

func (u *undoer) readDeleted(ctx context.Context) error {
	prefix := recordPrefixOf(u.opts.StateName)
	var names []recordName
	u.recordedAt = make(map[int64]time.Time)
	it := u.bucket.Objects(ctx, &storage.Query{Prefix: prefix})
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return err
		}
		n, pruned, ok := parseRecordName(prefix, attrs)
		switch {
		case !ok:
		case pruned:
			u.pruned = append(u.pruned, n.cycle)
		default:
			names = append(names, n)
			if n.created.After(u.recordedAt[n.cycle]) {
				u.recordedAt[n.cycle] = n.created
			}
		}
	}
	u.deleted = make(map[Hash]int64)
	var mu sync.Mutex
	eg, ectx := errgroup.WithContext(ctx)
	eg.SetLimit(u.opts.Workers)
	for _, n := range names {
		eg.Go(func() error {
			var entries []recordEntry
			err := retryWith(ectx, firstRetryWait, u.reads, func(actx context.Context) error {
				var err error
				_, entries, err = readRecordKeys(actx, u.bucket, n.name)
				return err
			})
			if err != nil {
				return err
			}
			mu.Lock()
			defer mu.Unlock()
			for _, e := range entries {
				if cycle, ok := u.deleted[e.key]; e.kind == recordObject && (!ok || cycle < n.cycle) {
					u.deleted[e.key] = n.cycle
				}
			}
			return nil
		})
	}
	return eg.Wait()
}

// cycle restores the keys of a record in parallel: the records of one level
// hold no parent of each other's keys.
func (u *undoer) cycle(ctx context.Context, cycle int64) error {
	for _, n := range u.records[cycle] {
		var h *recordHeader
		var entries []recordEntry
		err := retryWith(ctx, firstRetryWait, u.reads, func(actx context.Context) error {
			var err error
			h, entries, err = readRecord(actx, u.bucket, n.name)
			return err
		})
		if err != nil {
			return err
		}
		if h.cycle != cycle {
			return fmt.Errorf("the delete record %s holds the deletes of cycle %d", n.name, h.cycle)
		}
		u.rep.Records++
		eg, ectx := errgroup.WithContext(ctx)
		eg.SetLimit(u.opts.Workers)
		for i := range entries {
			e := &entries[i]
			eg.Go(func() error {
				if e.kind == recordMapRow {
					return u.mapRow(ectx, e.key, e.mapTo)
				}
				return u.object(ectx, e)
			})
		}
		if err := eg.Wait(); err != nil {
			return err
		}
	}
	return nil
}

func (u *undoer) readRow(ctx context.Context, table *bigtable.Table, row string, filter bigtable.Filter) (bigtable.Row, error) {
	var res bigtable.Row
	err := retryWith(ctx, firstRetryWait, u.reads, func(actx context.Context) error {
		var err error
		res, err = table.ReadRow(actx, row, bigtable.RowFilter(filter))
		return err
	})
	return res, err
}

func (u *undoer) apply(ctx context.Context, table *bigtable.Table, row string, mut *bigtable.Mutation) error {
	return retryWith(ctx, firstRetryWait, u.writes, func(actx context.Context) error {
		return table.Apply(actx, row, mut)
	})
}

func presentPredicate(family string) bigtable.Filter {
	return bigtable.ChainFilters(bigtable.FamilyFilter(family), bigtable.LatestNFilter(1), bigtable.StripValueFilter())
}

func (u *undoer) notRestoredReason(key Hash, n *int64, reason string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	*n++
	u.notRestored[key] = true
	if len(u.rep.NotRestoredSample) < maxNotRestoredSamples {
		u.rep.NotRestoredSample = append(u.rep.NotRestoredSample, key.String()+": "+reason)
	}
}

func (u *undoer) count(n *int64) {
	u.mu.Lock()
	*n++
	u.mu.Unlock()
}

func (u *undoer) object(ctx context.Context, e *recordEntry) error {
	key := e.key
	row := key.String()
	u.count(&u.rep.Objects)
	live, err := u.readRow(ctx, u.objects, row, presentPredicate(objectsMetaFamily))
	if err != nil {
		return fmt.Errorf("failed to read the objects row of %s: %w", key, err)
	}
	if len(live) > 0 {
		u.count(&u.rep.Present)
		u.setPresent(key)
		return nil
	}
	links, ok := e.edge(LinksColumn)
	if !ok {
		u.notRestoredReason(key, &u.rep.WithoutGCRow, "had no gc row when it was deleted")
		return nil
	}
	children, err := UnpackEdges(links)
	if err != nil {
		u.notRestoredReason(key, &u.rep.InvalidContent, err.Error())
		return nil
	}
	u.mu.Lock()
	blocked := slices.ContainsFunc(children, func(c Hash) bool { return u.notRestored[c] })
	u.mu.Unlock()
	if blocked {
		u.notRestoredReason(key, &u.rep.BlockedByChild, "links an object that was not restored")
		return nil
	}
	if reason, err := u.goneChild(ctx, children); err != nil {
		return err
	} else if reason != "" {
		u.notRestoredReason(key, &u.rep.BlockedByChild, reason)
		return nil
	}

	var chunks []recordedChunk
	if content := e.content(); len(content) > 0 {
		if err := json.Unmarshal(content, &chunks); err != nil {
			u.notRestoredReason(key, &u.rep.InvalidContent, "invalid content descriptor: "+err.Error())
			return nil
		}
	}
	var chunkRows []string
	var bucketObjects []*storage.ObjectAttrs
	var missing []string
	for _, c := range chunks {
		switch c.Depot {
		case inlineDepot:
		case bigtableDepot:
			cr, err := u.readRow(ctx, u.chunks, c.ID, presentPredicate(objectsDataFamily))
			switch {
			case err != nil:
				return fmt.Errorf("failed to read the chunk row %s: %w", c.ID, err)
			case len(cr) == 0:
				missing = append(missing, c.ID)
			default:
				chunkRows = append(chunkRows, c.ID)
			}
		case bucketDepot:
			var attrs *storage.ObjectAttrs
			err := retryWith(ctx, firstRetryWait, u.reads, func(actx context.Context) error {
				var err error
				attrs, err = u.bucket.Object(c.ID).Attrs(actx)
				return err
			})
			switch {
			case errors.Is(err, storage.ErrObjectNotExist):
				missing = append(missing, c.ID)
			case err != nil:
				return fmt.Errorf("failed to read the bucket chunk %s: %w", c.ID, err)
			default:
				bucketObjects = append(bucketObjects, attrs)
			}
		default:
			u.notRestoredReason(key, &u.rep.InvalidContent, "a chunk of the unknown depot "+c.Depot)
			return nil
		}
	}
	if len(missing) > 0 {
		for _, name := range missing {
			log.Printf("GC undo cannot restore %s: its chunk %s is gone", key, name)
		}
		u.notRestoredReason(key, &u.rep.MissingChunks, fmt.Sprintf("the chunk %s is gone: the chunk step deletes the chunks of a deleted object %s after its delete", missing[0], undoWindow))
		return nil
	}

	for _, attrs := range bucketObjects {
		metadata := maps.Clone(attrs.Metadata)
		if metadata == nil {
			metadata = make(map[string]string)
		}
		metadata[undoMetadata] = strconv.FormatInt(u.rep.Cycle, 10)
		err := retryWith(ctx, firstRetryWait, u.writes, func(actx context.Context) error {
			_, err := u.bucket.Object(attrs.Name).Update(actx, storage.ObjectAttrsToUpdate{Metadata: metadata})
			return err
		})
		if err != nil {
			return fmt.Errorf("failed to keep the bucket chunk %s of %s: %w", attrs.Name, key, err)
		}
		u.count(&u.rep.BucketObjectsStamped)
	}
	for _, id := range chunkRows {
		mut := bigtable.NewMutation()
		mut.Set(objectsDataFamily, chunkUndoColumn, bigtable.ServerTime, nil)
		if err := u.apply(ctx, u.chunks, id, mut); err != nil {
			return fmt.Errorf("failed to keep the chunk row %s of %s: %w", id, key, err)
		}
		u.count(&u.rep.ChunkRowsWritten)
	}
	if err := u.apply(ctx, u.gcTable, row, restoredGCRow(e)); err != nil {
		return fmt.Errorf("failed to write the gc row of %s: %w", key, err)
	}
	// Drop a marker that a stale condemn left in the same window.
	objectRow := bigtable.NewMutation()
	objectRow.DeleteCellsInColumn(ObjectsFamily, CondemnedColumn)
	for _, c := range e.cells {
		objectRow.Set(c.family, c.column, c.ts, c.value)
	}
	if err := u.apply(ctx, u.objects, row, objectRow); err != nil {
		return fmt.Errorf("failed to write the objects row of %s: %w", key, err)
	}
	u.count(&u.rep.Restored)
	u.setPresent(key)
	return nil
}

// restoredGCRow writes the edges with the server time so that the delete
// step and the chunk step treat the row as written again.
func restoredGCRow(e *recordEntry) *bigtable.Mutation {
	mut := bigtable.NewMutation()
	for _, c := range e.edges {
		mut.Set(EdgesFamily, c.column, bigtable.ServerTime, c.value)
	}
	mut.Set(TouchFamily, TouchColumn, bigtable.ServerTime, nil)
	return mut
}

func (u *undoer) setPresent(key Hash) {
	u.mu.Lock()
	u.present[key] = true
	u.mu.Unlock()
}

// goneChild returns why an object must not be restored yet, or "". A child
// that no record names, when no cycle was pruned, was absent when its parent
// was written, and blocks nothing.
func (u *undoer) goneChild(ctx context.Context, children []Hash) (string, error) {
	var unseen []Hash
	u.mu.Lock()
	for _, c := range children {
		if !u.present[c] {
			unseen = append(unseen, c)
		}
	}
	u.mu.Unlock()
	if len(unseen) == 0 {
		return "", nil
	}
	live, err := readObjectRows(ctx, u.objects, u.reads, hashRows(unseen))
	if err != nil {
		return "", fmt.Errorf("failed to read the objects rows of the children: %w", err)
	}
	var absent []Hash
	u.mu.Lock()
	for _, c := range unseen {
		if live[c.String()].present {
			u.present[c] = true
		} else {
			absent = append(absent, c)
		}
	}
	u.mu.Unlock()
	if len(absent) == 0 {
		return "", nil
	}
	if err := u.loadDeleted(ctx); err != nil {
		return "", err
	}
	for _, c := range absent {
		cycle, ok := u.deleted[c]
		switch {
		case ok && cycle >= u.opts.Cycle && cycle <= u.opts.Through:
			return fmt.Sprintf("links %s, which cycle %d deleted and this run had not restored yet; run the undo again", c, cycle), nil
		case ok && !u.recordedAt[cycle].Add(undoWindow).After(wallNow()):
			return fmt.Sprintf("links %s, which cycle %d deleted more than %s ago, so an undo of that cycle cannot restore it unless its content is inline", c, cycle, undoWindow), nil
		case ok:
			return fmt.Sprintf("links %s, which cycle %d deleted; undo cycle %d too", c, cycle, cycle), nil
		case len(u.pruned) > 0:
			return fmt.Sprintf("links %s, which is absent and may have been deleted by a cycle whose records were removed, such as cycle %d; an undo cannot tell", c, slices.Max(u.pruned)), nil
		}
	}
	return "", nil
}

func hashRows(keys []Hash) []string {
	rows := make([]string, len(keys))
	for i, k := range keys {
		rows[i] = k.String()
	}
	return rows
}

// mapRow restores the map row with a new time, so that the next cycle does
// not expire it again.
func (u *undoer) mapRow(ctx context.Context, from, to Hash) error {
	row := from.String() + "#" + to.String()
	u.count(&u.rep.MapRows)
	u.mu.Lock()
	blocked := u.notRestored[from] || u.notRestored[to]
	u.mu.Unlock()
	if blocked {
		u.count(&u.rep.MapRowsBlocked)
		return nil
	}
	reason, err := u.goneChild(ctx, []Hash{from, to})
	if err != nil {
		return err
	}
	if reason != "" {
		log.Printf("GC undo does not restore the map row %s: it %s", row, reason)
		u.count(&u.rep.MapRowsBlocked)
		return nil
	}
	live, err := u.readRow(ctx, u.maps, row, presentPredicate(mapsFamily))
	if err != nil {
		return fmt.Errorf("failed to read the map row %s: %w", row, err)
	}
	if len(live) > 0 {
		u.count(&u.rep.MapRowsPresent)
		return nil
	}
	mut := bigtable.NewMutation()
	mut.Set(mapsFamily, "", bigtable.ServerTime, []byte("1"))
	if err := u.apply(ctx, u.maps, row, mut); err != nil {
		return fmt.Errorf("failed to write the map row %s: %w", row, err)
	}
	u.count(&u.rep.MapRowsRestored)
	return nil
}
