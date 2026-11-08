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
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"maps"
	"regexp"
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
	// undoMetadata is set on each bucket chunk that an undo keeps, to the
	// cycle of the undo. It raises the metageneration of the object, so that
	// a chunk step that read the object before cannot delete it.
	undoMetadata = "abfs-undo"
	// maxNotRestoredSamples is the number of objects not restored that the
	// report names.
	maxNotRestoredSamples = 20
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

var tableSuffix = regexp.MustCompile(`^[-_.a-zA-Z0-9]+$`)

// UndoOpts sets a run of Undo.
type UndoOpts struct {
	// Cycle and Through are the first and the last delete cycle whose deletes
	// the run undoes; Through zero means Cycle.
	Cycle   int64
	Through int64
	// FromSuffix names the restored tables, <table><FromSuffix> for the
	// tables objects, gc, maps and chunks, restored from backups taken
	// before Cycle.
	FromSuffix string
	// Workers is the number of keys of one delete record restored in
	// parallel.
	Workers int
	// LeaseDuration is the lease the run holds and renews.
	LeaseDuration time.Duration
	// WriteDeadline is the deadline of one attempt of a write call.
	WriteDeadline time.Duration
	// Holder names this process in the lease; empty uses DefaultHolder.
	Holder string
	// StateName is the GcState row; empty uses DefaultStateName.
	StateName string
	// NoSourceCheck restores from tables without checking that they were
	// restored from backups of the live tables taken before Cycle.
	NoSourceCheck bool
}

// DefaultUndoOpts returns the default options; the cycles and the suffix
// have no default.
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
	case !tableSuffix.MatchString(o.FromSuffix):
		return fmt.Errorf("invalid suffix %q of the restored tables", o.FromSuffix)
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
	// Cycle is the cycle of the lease of the run; FirstCycle and LastCycle
	// the delete cycles it undid.
	Cycle      int64 `json:"cycle"`
	FirstCycle int64 `json:"first_cycle"`
	LastCycle  int64 `json:"last_cycle"`
	// FenceWaitSeconds is how long the run waited after it took the lease,
	// so that a collector that had lost it stopped writing first.
	FenceWaitSeconds float64 `json:"fence_wait_seconds"`
	Records          int64   `json:"records"`
	// Objects counts the objects of the records. Restored were written back;
	// Present had a row and were left as they are. The others were not
	// restored: MissingFromBackup lack a row or chunk row in the restored
	// tables, WaitingForBucketObjects lack a bucket chunk, listed in
	// MissingBucketObjects, InvalidContent have a descriptor that does not
	// parse or names an unknown depot, and BlockedByChild link an object
	// that was not restored.
	Objects                 int64    `json:"objects"`
	Restored                int64    `json:"restored"`
	Present                 int64    `json:"present"`
	MissingFromBackup       int64    `json:"missing_from_backup"`
	WaitingForBucketObjects int64    `json:"waiting_for_bucket_objects"`
	MissingBucketObjects    []string `json:"missing_bucket_objects,omitempty"`
	InvalidContent          int64    `json:"invalid_content,omitempty"`
	BlockedByChild          int64    `json:"blocked_by_child"`
	NotRestoredSample       []string `json:"not_restored_sample,omitempty"`
	// RefRecords names the ref records of the cycle before Cycle to
	// LastCycle, which list the refs those cycles removed; the run restores
	// no ref.
	RefRecords []string `json:"ref_records,omitempty"`
	// ChunkRowsWritten and BucketObjectsStamped count the chunks kept for the
	// restored objects.
	ChunkRowsWritten     int64 `json:"chunk_rows_written"`
	BucketObjectsStamped int64 `json:"bucket_objects_stamped"`
	// MapRows counts the map rows of the records: restored, present, missing
	// from the restored table, or blocked by an end that was not restored.
	MapRows         int64   `json:"map_rows"`
	MapRowsRestored int64   `json:"map_rows_restored"`
	MapRowsPresent  int64   `json:"map_rows_present"`
	MapRowsMissing  int64   `json:"map_rows_missing"`
	MapRowsBlocked  int64   `json:"map_rows_blocked"`
	Seconds         float64 `json:"seconds"`
}

// NotRestored returns the number of objects and map rows the run could not
// restore.
func (r *UndoReport) NotRestored() int64 {
	return r.MissingFromBackup + r.WaitingForBucketObjects + r.InvalidContent + r.BlockedByChild + r.MapRowsMissing + r.MapRowsBlocked
}

// Undo writes back the objects and map rows that the delete cycles from
// opts.Cycle to opts.Through deleted, as their delete records in bucket name
// them, from the tables restored from backups taken before opts.Cycle, which
// sources checks unless opts.NoSourceCheck is set. It fails before any write
// when a cycle of the range has records no more. It takes the lease in the
// phase undo and waits before its first write until a collector that had
// lost the lease stopped writing. It restores the newest cycle first and in
// a cycle the deepest level first, so that the children of an object are
// back before it. Per object whose row is absent: the bucket chunks of its
// descriptor, which must exist, get a metadata key; its chunk rows are
// written with a new timestamp; its gc row is written with a new timestamp
// and a touch, so that it is recent for the grace and looks to the chunk
// step like a row written again; and its objects row comes last, without
// the condemn marker. A map row comes back with a new time after its two
// ends. An object or map row that is present stays as it is. An object that
// lacks a bucket chunk, whose restored row holds another descriptor than its
// record, or that links an object deleted since the backup and not restored
// is not restored, nor is anything that links it; running Undo again after
// the chunks were restored from soft delete, or with more cycles, restores
// them.
func Undo(ctx context.Context, sp *spanner.Client, bt *bigtable.Client, bucket *storage.BucketHandle, sources TableSources, opts *UndoOpts) (*UndoReport, error) {
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
	if o.NoSourceCheck {
		sources = nil
	} else if sources == nil {
		return nil, errors.New("the undo needs the sources of the restored tables, or NoSourceCheck")
	}
	u := newUndoer(bt, bucket, &o)
	if err := u.prepare(ctx, sources); err != nil {
		return nil, err
	}
	lease, err := AcquireLease(ctx, sp, o.StateName, o.Holder, PhaseUndo, o.LeaseDuration, 0)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	runCtx, stop := lease.Keep(ctx)
	defer stop()
	rep, err := u.run(runCtx, lease)
	if err != nil && runCtx.Err() != nil && ctx.Err() == nil {
		err = fmt.Errorf("the GC undo stopped: %w", context.Cause(runCtx))
	}
	if err != nil && rep.ChunkRowsWritten+rep.BucketObjectsStamped > 0 {
		// The chunk step keeps the chunks an undo wrote or stamped, also of an
		// object whose row the undo did not write.
		err = fmt.Errorf("%w; run the undo again until it succeeds, or the chunks it kept for objects it did not finish stay in the store", err)
	}
	return rep, err
}

// undoer restores the keys of delete records.
type undoer struct {
	opts   *UndoOpts
	bucket *storage.BucketHandle
	// The live tables and the restored ones.
	objects, gcTable, maps, chunks                            *bigtable.Table
	fromObjects, fromGC, fromMaps, fromChunks                 *bigtable.Table
	fromObjectsName, fromGCName, fromMapsName, fromChunksName string
	reads                                                     writeBudget
	writes                                                    writeBudget
	rep                                                       *UndoReport
	// records holds the delete records of each cycle of the run.
	records map[int64][]recordName
	// mu guards rep, notRestored, the keys that were not restored, and
	// present, the keys that are present or were restored.
	mu          sync.Mutex
	notRestored map[Hash]bool
	present     map[Hash]bool
}

func newUndoer(bt *bigtable.Client, bucket *storage.BucketHandle, o *UndoOpts) *undoer {
	u := &undoer{
		opts: o, bucket: bucket,
		objects: bt.Open(objectsTable), gcTable: bt.Open(TableName), maps: bt.Open(mapsTableName), chunks: bt.Open(chunksTableName),
		fromObjectsName: objectsTable + o.FromSuffix, fromGCName: TableName + o.FromSuffix,
		fromMapsName: mapsTableName + o.FromSuffix, fromChunksName: chunksTableName + o.FromSuffix,
		reads:       writeBudget{deadline: o.WriteDeadline},
		rep:         &UndoReport{FirstCycle: o.Cycle, LastCycle: o.Through},
		notRestored: make(map[Hash]bool),
		present:     make(map[Hash]bool),
	}
	u.fromObjects, u.fromGC = bt.Open(u.fromObjectsName), bt.Open(u.fromGCName)
	u.fromMaps, u.fromChunks = bt.Open(u.fromMapsName), bt.Open(u.fromChunksName)
	return u
}

// checkTables checks that the restored tables can be read.
func (u *undoer) checkTables(ctx context.Context) error {
	for _, t := range []struct {
		name  string
		table *bigtable.Table
	}{{u.fromObjectsName, u.fromObjects}, {u.fromGCName, u.fromGC}, {u.fromMapsName, u.fromMaps}, {u.fromChunksName, u.fromChunks}} {
		if _, err := u.readRow(ctx, t.table, "abfs-setup-check", bigtable.StripValueFilter()); err != nil {
			return fmt.Errorf("cannot read the restored Bigtable table %s: %w", t.name, err)
		}
	}
	return nil
}

// prepare checks the restored tables, lists the delete records of the
// cycles and, unless sources is nil, checks that each restored table comes
// from a backup of its live table that ended before the first record of the
// cycles was written. It fails when no cycle has a record, or a cycle has
// records no more.
func (u *undoer) prepare(ctx context.Context, sources TableSources) error {
	if err := u.checkTables(ctx); err != nil {
		return err
	}
	u.records = make(map[int64][]recordName)
	var first time.Time
	for cycle := u.opts.Cycle; cycle <= u.opts.Through; cycle++ {
		names, err := u.listRecords(ctx, cycle)
		if err != nil {
			return err
		}
		if len(names) == 0 {
			log.Printf("GC cycle %d has no delete records: it deleted nothing", cycle)
		}
		for _, n := range names {
			if first.IsZero() || n.created.Before(first) {
				first = n.created
			}
		}
		u.records[cycle] = names
	}
	if first.IsZero() {
		return fmt.Errorf("found no delete records of the cycles %d to %d under gs://%s/%s: they deleted nothing, or the bucket or the GC state is not the one of the deletes", u.opts.Cycle, u.opts.Through, u.bucket.BucketName(), recordPrefixOf(u.opts.StateName))
	}
	if sources != nil {
		if err := u.checkSources(ctx, sources, first); err != nil {
			return err
		}
	}
	return u.listRefRecords(ctx)
}

// checkSources checks that each restored table was restored from a backup
// of its live table that ended before first, when the first delete record of
// the cycles was written, so that it holds every object the cycles deleted.
func (u *undoer) checkSources(ctx context.Context, sources TableSources, first time.Time) error {
	for _, t := range []struct{ live, restored string }{
		{objectsTable, u.fromObjectsName}, {TableName, u.fromGCName}, {mapsTableName, u.fromMapsName}, {chunksTableName, u.fromChunksName},
	} {
		b, err := sources.RestoredFrom(ctx, t.restored)
		switch {
		case err != nil:
			return fmt.Errorf("cannot read where the table %s was restored from%s: %w", t.restored, permissionHint(err, "bigtable.tables.get"), err)
		case b == nil:
			return fmt.Errorf("the table %s was not restored from a backup; restore it from a backup of %s taken before cycle %d, or pass --no-source-check", t.restored, t.live, u.opts.Cycle)
		case b.SourceTable != t.live:
			return fmt.Errorf("the table %s was restored from the backup %s of the table %s, not of %s", t.restored, b.Backup, b.SourceTable, t.live)
		case !b.End.Before(first):
			return fmt.Errorf("the table %s was restored from the backup %s, which ended at %s, not before the first delete record of cycle %d was written at %s; restore a backup taken before that cycle", t.restored, b.Backup, formatTime(b.End), u.opts.Cycle, formatTime(first))
		}
	}
	return nil
}

// listRefRecords names in the report the ref records of the cycle before
// the first one of the run to the last one: a delete cycle deletes what the
// refs that the cycle before removed reached, and the run restores no ref.
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

// run waits for the fence of lease and then restores the records of the
// cycles, newest first.
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

// waitFence waits until the expiry of the lease that lease took over, or
// when that lease was released the time lease was taken, plus two write
// attempts and the clock skew margin. A collector that lost its lease starts
// no write attempt later than two attempts and the margin before its expiry,
// and every lease before the one taken over expired before lease was taken,
// so no write of theirs lands after the wait. It returns how long it waited.
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

// recordName is a delete record of a cycle and its level, range and batch.
type recordName struct {
	name              string
	created           time.Time
	level, rng, batch int
}

// listRecords lists the delete records of cycle, deepest level first. It
// fails when the records of the cycle were pruned.
func (u *undoer) listRecords(ctx context.Context, cycle int64) ([]recordName, error) {
	prefix := recordPrefixOf(u.opts.StateName) + strconv.FormatInt(cycle, 10) + "/"
	var names []recordName
	it := u.bucket.Objects(ctx, &storage.Query{Prefix: prefix})
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list the delete records under %s: %w", prefix, err)
		}
		if strings.TrimPrefix(attrs.Name, prefix) == prunedMarker {
			return nil, fmt.Errorf("the delete records of cycle %d were removed at %s, so the deletes of that cycle cannot be undone", cycle, formatTime(attrs.Created))
		}
		n := recordName{created: attrs.Created}
		parts := strings.Split(strings.TrimPrefix(attrs.Name, prefix), "-")
		if len(parts) == 3 {
			n.level, err = strconv.Atoi(parts[0])
			if err == nil {
				n.rng, err = strconv.Atoi(parts[1])
			}
			if err == nil {
				n.batch, err = strconv.Atoi(parts[2])
			}
		}
		if len(parts) != 3 || err != nil {
			return nil, fmt.Errorf("unexpected object %s among the delete records", attrs.Name)
		}
		n.name = attrs.Name
		names = append(names, n)
	}
	slices.SortFunc(names, func(a, b recordName) int {
		return cmp.Or(cmp.Compare(b.level, a.level), cmp.Compare(a.rng, b.rng), cmp.Compare(a.batch, b.batch))
	})
	return names, nil
}

// cycle restores the keys of the records of cycle, one record at a time.
// The records of one level hold no parent of each other's keys, so the keys
// of a record are restored in parallel.
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

// readRow reads row of table with filter, nil when it does not exist.
func (u *undoer) readRow(ctx context.Context, table *bigtable.Table, row string, filter bigtable.Filter) (bigtable.Row, error) {
	var res bigtable.Row
	err := retryWith(ctx, firstRetryWait, u.reads, func(actx context.Context) error {
		var err error
		res, err = table.ReadRow(actx, row, bigtable.RowFilter(filter))
		return err
	})
	return res, err
}

// apply applies mut to row of table within the write budget of the lease.
func (u *undoer) apply(ctx context.Context, table *bigtable.Table, row string, mut *bigtable.Mutation) error {
	return retryWith(ctx, firstRetryWait, u.writes, func(actx context.Context) error {
		return table.Apply(actx, row, mut)
	})
}

// copyCells returns a mutation that sets the cells of row, with their
// timestamps, or with the server time when now is set.
func copyCells(row bigtable.Row, now bool) *bigtable.Mutation {
	return copyCellsTo(bigtable.NewMutation(), row, now)
}

// copyCellsTo adds to mut the sets of copyCells and returns it.
func copyCellsTo(mut *bigtable.Mutation, row bigtable.Row, now bool) *bigtable.Mutation {
	for _, family := range slices.Sorted(maps.Keys(row)) {
		for _, item := range row[family] {
			ts := item.Timestamp
			if now {
				ts = bigtable.ServerTime
			}
			mut.Set(family, strings.TrimPrefix(item.Column, family+":"), ts, item.Value)
		}
	}
	return mut
}

// presentPredicate matches a row with a cell of family.
func presentPredicate(family string) bigtable.Filter {
	return bigtable.ChainFilters(bigtable.FamilyFilter(family), bigtable.LatestNFilter(1), bigtable.StripValueFilter())
}

// notRestoredReason counts n and records key as not restored, so that no
// parent of it is restored either.
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

// object restores the objects row of the record entry e and what it needs,
// unless the row is present. It fails only when a read or write fails after
// its retries.
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
	latest := bigtable.LatestNFilter(1)
	from, err := u.readRow(ctx, u.fromObjects, row, bigtable.ChainFilters(bigtable.FamilyFilter(objectsMetaFamily+"|"+objectsDataFamily), latest))
	if err != nil {
		return fmt.Errorf("failed to read the objects row of %s from %s: %w", key, u.fromObjectsName, err)
	}
	if len(from[objectsMetaFamily]) == 0 {
		u.notRestoredReason(key, &u.rep.MissingFromBackup, "no objects row in "+u.fromObjectsName)
		return nil
	}
	var content []byte
	for _, item := range from[objectsDataFamily] {
		if item.Column == objectsDataFamily+":"+objectsContentColumn {
			content = item.Value
		}
	}
	if !bytes.Equal(recordedContent(content), e.content) {
		// The object was written again after the backup, with other chunks;
		// the chunk step may have deleted those of the backup.
		u.notRestoredReason(key, &u.rep.MissingFromBackup, "the row in "+u.fromObjectsName+" holds another content descriptor than the delete record: the backup is older than the last write of the object")
		return nil
	}
	fromGC, err := u.readRow(ctx, u.fromGC, row, latest)
	if err != nil {
		return fmt.Errorf("failed to read the gc row of %s from %s: %w", key, u.fromGCName, err)
	}
	var links []byte
	hasLinks := false
	for _, item := range fromGC[EdgesFamily] {
		if item.Column == EdgesFamily+":"+LinksColumn {
			links, hasLinks = item.Value, true
		}
	}
	if !hasLinks {
		u.notRestoredReason(key, &u.rep.MissingFromBackup, "no gc row in "+u.fromGCName)
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
	if gone, err := u.goneChild(ctx, children); err != nil {
		return err
	} else if !gone.IsZero() {
		u.notRestoredReason(key, &u.rep.BlockedByChild, "links "+gone.String()+", which is absent but in "+u.fromObjectsName+"; undo the cycle that deleted it too")
		return nil
	}

	var chunks []recordedChunk
	if len(content) > 0 {
		if err := json.Unmarshal(content, &chunks); err != nil {
			u.notRestoredReason(key, &u.rep.InvalidContent, "invalid content descriptor: "+err.Error())
			return nil
		}
	}
	var chunkRows []bigtable.Row
	var bucketObjects []*storage.ObjectAttrs
	var missing []string
	for _, c := range chunks {
		switch c.Depot {
		case inlineDepot:
		case bigtableDepot:
			cr, err := u.readRow(ctx, u.fromChunks, c.ID, latest)
			if err != nil {
				return fmt.Errorf("failed to read the chunk row %s from %s: %w", c.ID, u.fromChunksName, err)
			}
			if len(cr) == 0 {
				u.notRestoredReason(key, &u.rep.MissingFromBackup, "no chunk row "+c.ID+" in "+u.fromChunksName)
				return nil
			}
			chunkRows = append(chunkRows, cr)
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
			log.Printf("GC undo waits for the bucket chunk %s of %s; restore it from soft delete and run the undo again", name, key)
		}
		u.mu.Lock()
		u.rep.MissingBucketObjects = append(u.rep.MissingBucketObjects, missing...)
		u.mu.Unlock()
		u.notRestoredReason(key, &u.rep.WaitingForBucketObjects, "waits for bucket chunks")
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
	for _, cr := range chunkRows {
		if err := u.apply(ctx, u.chunks, cr.Key(), copyCells(cr, true)); err != nil {
			return fmt.Errorf("failed to write the chunk row %s of %s: %w", cr.Key(), key, err)
		}
		u.count(&u.rep.ChunkRowsWritten)
	}
	if err := u.apply(ctx, u.gcTable, row, restoredGCRow(fromGC)); err != nil {
		return fmt.Errorf("failed to write the gc row of %s: %w", key, err)
	}
	// A condemn marker in the row, as a stale condemn leaves in a row deleted
	// in the same window, goes.
	objectRow := bigtable.NewMutation()
	objectRow.DeleteCellsInColumn(ObjectsFamily, CondemnedColumn)
	if err := u.apply(ctx, u.objects, row, copyCellsTo(objectRow, from, false)); err != nil {
		return fmt.Errorf("failed to write the objects row of %s: %w", key, err)
	}
	u.count(&u.rep.Restored)
	u.setPresent(key)
	return nil
}

// restoredGCRow returns the mutation that writes the gc row from back: its
// edges with the server time, as a put writes them, so that the delete step
// and the chunk step, which keep a gc row whose e:links cell is newer than
// the grace start of their record, keep it, and a touch.
func restoredGCRow(from bigtable.Row) *bigtable.Mutation {
	mut := copyCells(bigtable.Row{EdgesFamily: from[EdgesFamily]}, true)
	mut.Set(TouchFamily, TouchColumn, bigtable.ServerTime, nil)
	return mut
}

func (u *undoer) setPresent(key Hash) {
	u.mu.Lock()
	u.present[key] = true
	u.mu.Unlock()
}

// goneChild returns a child of children, that the run neither restored nor
// found present, whose objects row is absent from the live table but present
// in the restored one, so deleted since the backup; the zero hash when there
// is none. A child absent from both was absent when its parent was backed up.
func (u *undoer) goneChild(ctx context.Context, children []Hash) (Hash, error) {
	var unseen []Hash
	u.mu.Lock()
	for _, c := range children {
		if !u.present[c] {
			unseen = append(unseen, c)
		}
	}
	u.mu.Unlock()
	if len(unseen) == 0 {
		return Hash{}, nil
	}
	live, err := readObjectRows(ctx, u.objects, u.reads, hashRows(unseen))
	if err != nil {
		return Hash{}, fmt.Errorf("failed to read the objects rows of the children: %w", err)
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
		return Hash{}, nil
	}
	backedUp, err := readObjectRows(ctx, u.fromObjects, u.reads, hashRows(absent))
	if err != nil {
		return Hash{}, fmt.Errorf("failed to read the objects rows of the children from %s: %w", u.fromObjectsName, err)
	}
	for _, c := range absent {
		if backedUp[c.String()].present {
			return c, nil
		}
	}
	return Hash{}, nil
}

func hashRows(keys []Hash) []string {
	rows := make([]string, len(keys))
	for i, k := range keys {
		rows[i] = k.String()
	}
	return rows
}

// mapRow restores the map row from#to with a new timestamp, so that it is
// not expired again by the next cycle, unless it is present or an end was
// not restored.
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
	live, err := u.readRow(ctx, u.maps, row, presentPredicate(mapsFamily))
	if err != nil {
		return fmt.Errorf("failed to read the map row %s: %w", row, err)
	}
	if len(live) > 0 {
		u.count(&u.rep.MapRowsPresent)
		return nil
	}
	fromRow, err := u.readRow(ctx, u.fromMaps, row, bigtable.ChainFilters(bigtable.FamilyFilter(mapsFamily), bigtable.LatestNFilter(1)))
	if err != nil {
		return fmt.Errorf("failed to read the map row %s from %s: %w", row, u.fromMapsName, err)
	}
	if len(fromRow) == 0 {
		u.count(&u.rep.MapRowsMissing)
		return nil
	}
	if err := u.apply(ctx, u.maps, row, copyCells(fromRow, true)); err != nil {
		return fmt.Errorf("failed to write the map row %s: %w", row, err)
	}
	u.count(&u.rep.MapRowsRestored)
	return nil
}
