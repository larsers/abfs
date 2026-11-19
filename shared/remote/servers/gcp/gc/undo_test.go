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
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

const undoCycle = 9

// undoEvent is a write the undo sent: a Bigtable row of a table, or the
// metadata of a bucket object, at the wall-clock time of the collector.
type undoEvent struct {
	table, key string
	at         time.Time
}

// undoFixture is a chunkFixture that keeps the record entry of each object
// it deletes, and records every Bigtable write and bucket metadata update.
// Its Bigtable has only the live tables.
type undoFixture struct {
	*chunkFixture
	// bucket is the bucket of the undo, whose metadata updates are recorded.
	bucket  *storage.BucketHandle
	entries map[Hash]recordEntry
	mu      sync.Mutex
	events  []undoEvent
}

func newUndoFixture(t *testing.T) *undoFixture {
	t.Helper()
	ctx := context.Background()
	f := &undoFixture{entries: make(map[Hash]recordEntry)}
	bt := newHooksTestBigtable(t, grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		switch m := req.(type) {
		case *bigtablepb.MutateRowRequest:
			f.add(m.GetTableName(), string(m.GetRowKey()))
		case *bigtablepb.CheckAndMutateRowRequest:
			f.add(m.GetTableName(), string(m.GetRowKey()))
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}))
	cf := &chunkFixture{stepFixture: &stepFixture{t: t, client: bt, bucket: newTestBucket(t), now: time.Now(), shards: 1}, lease: &Lease{Epoch: 3}, checks: &fakeInstance{}}
	cf.lease.expiry.Store(time.Now().Add(3 * undoWindow).UnixNano())
	cf.clock.Store(time.Now().UnixNano())
	orig := wallNow
	wallNow = func() time.Time { return time.Unix(0, cf.clock.Load()) }
	t.Cleanup(func() { wallNow = orig })
	f.chunkFixture = cf

	client, err := storage.NewGRPCClient(ctx, option.WithGRPCDialOption(grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if strings.HasSuffix(method, "/UpdateObject") {
			m := req.(proto.Message).ProtoReflect()
			obj := m.Get(m.Descriptor().Fields().ByName("object")).Message()
			f.add("bucket", obj.Get(obj.Descriptor().Fields().ByName("name")).String())
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	})))
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	f.bucket = client.Bucket(cf.stepFixture.bucket.BucketName())
	return f
}

func (f *undoFixture) add(table, key string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, undoEvent{table: table[strings.LastIndex(table, "/")+1:], key: key, at: wallNow()})
}

// takeEvents returns the events recorded so far and forgets them.
func (f *undoFixture) takeEvents() []undoEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	events := f.events
	f.events = nil
	return events
}

// read keeps the record entry of key as the delete step reads it.
func (f *undoFixture) read(key Hash) {
	f.t.Helper()
	s := &step{objects: f.client.Open(objectsTable), gcTable: f.client.Open(TableName), reads: writeBudget{deadline: DefaultWriteDeadline}}
	objects, gcRows, err := s.readDeleteRows(context.Background(), []string{key.String()})
	require.NoError(f.t, err)
	f.entries[key] = objectEntry(key, objects[key.String()], gcRows[key.String()])
}

// deleted writes the object key with a chunk row and a bucket chunk and the
// gc edges to its children, and reads and deletes its objects and gc rows as
// a delete step does. It returns the names of the two chunks.
func (f *undoFixture) deleted(key Hash, children ...Hash) (row, obj string) {
	f.t.Helper()
	f.objectWith(key, "tree", children, markerValue(stepListCycle), true)
	f.content(key, "u")
	f.read(key)
	f.deleteRow(objectsTable, key)
	f.deleteRow(TableName, key)
	return chunkNames(key, "u")
}

// recordOf writes a delete record of cycle for level with the entry of each
// key, and a map row entry from#to for each pair of maps.
func (f *undoFixture) recordOf(cycle int64, level int, keys []Hash, maps ...[2]Hash) {
	f.t.Helper()
	var entries []recordEntry
	for _, k := range keys {
		e, ok := f.entries[k]
		require.True(f.t, ok, "%s was not read", k)
		entries = append(entries, e)
	}
	for _, m := range maps {
		entries = append(entries, recordEntry{kind: recordMapRow, key: m[0], mapTo: m[1]})
	}
	h := &recordHeader{cycle: cycle, runID: runID("collector", 3), gcRowsAfter: f.now.Add(-DefaultGrace)}
	_, err := writeRecord(context.Background(), writeBudget{deadline: DefaultWriteDeadline}, f.stepFixture.bucket, recordObjectName(DefaultStateName, cycle, level, 0, 0), h, entries)
	require.NoError(f.t, err)
}

// undoLease is a lease of cycle undoCycle taken an hour ago over a lease
// that was released, so that its fence has passed.
func undoLease() *Lease {
	l := &Lease{Cycle: undoCycle, Epoch: 4, Start: wallNow().Add(-time.Hour)}
	l.expiry.Store(wallNow().Add(time.Hour).UnixNano())
	return l
}

// undo runs an undo of the cycles first to last under lease.
func (f *undoFixture) undo(lease *Lease, first, last int64) *UndoReport {
	f.t.Helper()
	rep, err := f.undoWith(lease, first, last)
	require.NoError(f.t, err)
	return rep
}

func (f *undoFixture) undoWith(lease *Lease, first, last int64) (*UndoReport, error) {
	f.t.Helper()
	o := DefaultUndoOpts()
	o.Cycle, o.Through, o.StateName = first, last, DefaultStateName
	require.NoError(f.t, o.Validate())
	u := newUndoer(f.client, f.bucket, &o)
	if err := u.prepare(context.Background()); err != nil {
		return nil, err
	}
	return u.run(context.Background(), lease)
}

func (f *undoFixture) cells(table, row string) bigtable.Row {
	f.t.Helper()
	r, err := f.client.Open(table).ReadRow(context.Background(), row)
	require.NoError(f.t, err)
	return r
}

// cellTimes returns the timestamp of each cell of row by column.
func cellTimes(row bigtable.Row) map[string]bigtable.Timestamp {
	times := make(map[string]bigtable.Timestamp)
	for _, items := range row {
		for _, item := range items {
			times[item.Column] = max(times[item.Column], item.Timestamp)
		}
	}
	return times
}

// cellValues returns the value of each cell of row by column.
func cellValues(row bigtable.Row) map[string]string {
	values := make(map[string]string)
	for _, items := range row {
		for _, item := range items {
			values[item.Column] = string(item.Value)
		}
	}
	return values
}

// Two attempts of the write deadline must fit into a quarter of the lease,
// and a run needs a worker.
func TestUndoOpts_Validate(t *testing.T) {
	o := DefaultUndoOpts()
	o.Cycle = 5
	o.LeaseDuration = 4 * time.Minute
	o.WriteDeadline = 30 * time.Second
	require.NoError(t, o.Validate())
	o.WriteDeadline = 31 * time.Second
	require.ErrorContains(t, o.Validate(), "write deadline 31s is too long for the lease 4m0s")
	o.WriteDeadline = 30 * time.Second
	o.Workers = 0
	require.ErrorContains(t, o.Validate(), "invalid number of workers 0")
}

// A delete step records the objects and gc rows it deletes, and an undo of
// the cycle writes them back from the records alone: the objects row with
// the cells and times it had, without the marker, and the gc row with every
// edge column, so that the next scan reads it as before.
func TestUndo_FromRecordsOnly(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newUndoFixture(t)
	h := testHashes(2)
	tree, blob := h[0], h[1]
	f.object(tree, "tree", []Hash{blob})
	f.descriptor(tree, []byte(`[{"size":3,"depot":"inline","id":"YWJj"}]`))
	f.object(blob, "blob", nil)
	f.content(blob, "u")
	objects := make(map[Hash]bigtable.Row)
	gcRows := make(map[Hash]bigtable.Row)
	latest := bigtable.RowFilter(bigtable.LatestNFilter(1))
	for _, k := range h {
		var err error
		objects[k], err = f.client.Open(objectsTable).ReadRow(ctx, k.String(), latest)
		require.NoError(err)
		gcRows[k] = f.cells(TableName, k.String())
		delete(objects[k], ObjectsFamily)
	}
	res := f.mark(nil)
	d := f.deleter(f.lease, nil)
	_, err := d.deleteStep(ctx, f.lease, res)
	require.NoError(err)
	require.Equal(int64(2), d.rep.Step.Deleted)
	require.Positive(d.rep.Step.RecordBytes)
	for _, k := range h {
		require.False(f.rowExists(objectsTable, k.String()), k)
		require.False(f.rowExists(TableName, k.String()), k)
	}

	rep := f.undo(undoLease(), stepCycle, stepCycle)
	require.Equal(int64(2), rep.Restored)
	require.Zero(rep.NotRestored())
	for _, k := range h {
		got := f.cells(objectsTable, k.String())
		require.Equal(cellValues(objects[k]), cellValues(got), k)
		require.Equal(cellTimes(objects[k]), cellTimes(got), k)
		edges := f.cells(TableName, k.String())
		delete(edges, TouchFamily)
		require.Equal(cellValues(gcRows[k]), cellValues(edges), k)
	}
	g, stats, err := scanNodes(ctx, f.client.Open(TableName), f.bounds(), f.now.Add(-DefaultGrace))
	require.NoError(err)
	require.Zero(stats.Outdated)
	require.Equal(2, g.size())
}

// A batch whose entries exceed the byte limit of a record goes into several
// records, numbered on over the batches of a range so that no name repeats,
// and the undo reads every one.
func TestUndo_RecordSplitAtTheByteCap(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newUndoFixture(t)
	h := testHashes(3)
	for _, k := range h {
		f.object(k, "blob", nil)
		f.descriptor(k, []byte(`[{"size":3,"depot":"inline","id":"YWJj"}]`))
	}
	orig := maxRecordBytes
	maxRecordBytes = 1
	t.Cleanup(func() { maxRecordBytes = orig })
	res := f.mark(nil)
	d := f.deleter(f.lease, func(o *DeleteOpts) { o.BatchSize = 2 })
	_, err := d.deleteStep(ctx, f.lease, res)
	require.NoError(err)
	require.Equal(int64(3), d.rep.Step.Deleted)
	require.Equal(int64(3), d.rep.Step.RecordsWritten)
	for batch := range 3 {
		_, entries, err := readRecord(ctx, f.bucket, recordObjectName(DefaultStateName, stepCycle, 1, 0, batch))
		require.NoError(err)
		require.Len(entries, 1)
	}

	rep := f.undo(undoLease(), stepCycle, stepCycle)
	require.Equal(int64(3), rep.Records)
	require.Equal(int64(3), rep.Restored)
	for _, k := range h {
		require.True(f.rowExists(objectsTable, k.String()), k)
	}
}

// An undo restores the newest cycle of its range first and in each cycle
// the deepest level first, so that every child is back before its parent
// and a map row after its ends. Per key it keeps the bucket chunk, writes a
// new cell to the chunk row, the gc row with a new time and a touch, and
// the objects row last, with the times of the record and without the
// condemn marker. A map row comes back with a new time. A cycle after the
// range is not undone.
func TestUndo_RestoresInOrder(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newUndoFixture(t)
	h := testHashes(7)
	a, b, c, d, from, to, later := h[0], h[1], h[2], h[3], h[4], h[5], h[6]
	f.object(from, "eval-request", nil)
	f.object(to, "eval-response", nil)
	mapKey := from.String() + "#" + to.String()

	chunksOf := make(map[Hash][2]string)
	for _, k := range []struct{ key, child Hash }{{a, b}, {b, Hash{}}, {c, d}, {d, Hash{}}, {later, Hash{}}} {
		var children []Hash
		if !k.child.IsZero() {
			children = []Hash{k.child}
		}
		row, obj := f.deleted(k.key, children...)
		chunksOf[k.key] = [2]string{row, obj}
	}
	f.recordOf(5, 1, []Hash{a})
	f.recordOf(5, 2, []Hash{b})
	f.recordOf(6, 0, nil, [2]Hash{from, to})
	f.recordOf(6, 1, []Hash{c})
	f.recordOf(6, 2, []Hash{d})
	f.recordOf(7, 1, []Hash{later})
	f.takeEvents()
	before := bigtable.Now()

	rep := f.undo(undoLease(), 5, 6)
	events := f.takeEvents()
	var rows []string
	for _, e := range events {
		if e.table == objectsTableName || e.table == mapsTableName {
			rows = append(rows, e.key)
		}
	}
	require.Equal([]string{d.String(), c.String(), mapKey, b.String(), a.String()}, rows)
	for _, k := range []Hash{a, b, c, d} {
		order := slices.IndexFunc(events, func(e undoEvent) bool { return e.table == "bucket" && e.key == chunksOf[k][1] })
		for _, want := range []undoEvent{{table: chunksTableName, key: chunksOf[k][0]}, {table: TableName, key: k.String()}, {table: objectsTableName, key: k.String()}} {
			next := slices.IndexFunc(events, func(e undoEvent) bool { return e.table == want.table && e.key == want.key })
			require.Greater(next, order, "%s of %s", want.table, k)
			order = next
		}
	}
	require.Equal(int64(5), rep.Records)
	require.Equal(int64(4), rep.Objects)
	require.Equal(int64(4), rep.Restored)
	require.Equal(int64(4), rep.ChunkRowsWritten)
	require.Equal(int64(4), rep.BucketObjectsStamped)
	require.Equal(int64(1), rep.MapRowsRestored)
	require.Zero(rep.NotRestored())

	// The objects row keeps the times of the record and has no marker; the
	// gc row is new, as after a put, and so is its touch.
	object := f.cells(objectsTableName, d.String())
	require.Empty(object[ObjectsFamily])
	for _, cell := range f.entries[d].cells {
		require.Equal(cell.ts, cellTimes(object)[cell.family+":"+cell.column], cell.column)
	}
	times := cellTimes(f.cells(TableName, d.String()))
	for _, col := range []string{LinksColumn, TypeColumn, VersionColumn} {
		require.GreaterOrEqual(times[EdgesFamily+":"+col], before, col)
	}
	require.GreaterOrEqual(times[TouchFamily+":"+TouchColumn], before)
	// The chunk row keeps its content and gets a new cell, the map row is
	// new, and the bucket chunk carries the cycle of the undo.
	chunkTimes := cellTimes(f.cells(chunksTableName, chunksOf[d][0]))
	require.Less(chunkTimes[objectsDataFamily+":"+objectsContentColumn], before)
	require.GreaterOrEqual(chunkTimes[objectsDataFamily+":"+chunkUndoColumn], before)
	mapRow := f.cells(mapsTableName, mapKey)
	require.GreaterOrEqual(cellTimes(mapRow)[mapsFamily+":"], before)
	require.Equal("1", cellValues(mapRow)[mapsFamily+":"])
	attrs, err := f.bucket.Object(chunksOf[d][1]).Attrs(ctx)
	require.NoError(err)
	require.Equal(fmt.Sprint(undoCycle), attrs.Metadata[undoMetadata])
	require.False(f.rowExists(objectsTableName, later.String()))
}

// The undo writes nothing before the expiry of the lease it took over plus
// two write attempts and the clock skew margin, or, when that lease was
// released, before the time it took its own lease plus the same, since a
// holder that lost an older lease may still be writing.
func TestUndo_WaitsForTheFence(t *testing.T) {
	tests := []struct {
		name     string
		previous time.Duration
		start    time.Duration
		released bool
	}{
		{name: "lease taken over", previous: 0, start: -time.Minute},
		{name: "lease released", start: 0, released: true},
		{name: "lease expired long ago", previous: -time.Hour, start: -time.Minute},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			f := newUndoFixture(t)
			key := testHashes(1)[0]
			f.deleted(key)
			f.recordOf(5, 1, []Hash{key})
			orig := fenceSleep
			fenceSleep = func(_ context.Context, d time.Duration) error {
				f.clock.Add(int64(min(d, 7*time.Second)))
				return nil
			}
			t.Cleanup(func() { fenceSleep = orig })
			f.takeEvents()

			now := wallNow()
			lease := &Lease{Cycle: undoCycle, Epoch: 4, Start: now.Add(tt.start)}
			if !tt.released {
				lease.PreviousExpiry = now.Add(tt.previous)
			}
			lease.expiry.Store(now.Add(time.Hour).UnixNano())
			fence := lease.PreviousExpiry.Add(2*DefaultWriteDeadline + clockSkewMargin)
			if tt.released {
				fence = lease.Start.Add(2*DefaultWriteDeadline + clockSkewMargin)
			}
			rep := f.undo(lease, 5, 5)
			require.Equal(int64(1), rep.Restored)
			events := f.takeEvents()
			require.NotEmpty(events)
			for _, e := range events {
				require.False(e.at.Before(fence), "%s %s written %s before the fence", e.table, e.key, fence.Sub(e.at))
			}
			require.InDelta(max(fence.Sub(now), 0).Seconds(), rep.FenceWaitSeconds, 7)
		})
	}
}

// An object or map row that is present when the undo reaches it stays as it
// is: none of its chunks, its gc row or its row is written, and it is
// counted.
func TestUndo_SkipsPresentKeys(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newUndoFixture(t)
	h := testHashes(4)
	present, from, to, gone := h[0], h[1], h[2], h[3]
	f.object(from, "eval-request", nil)
	f.object(to, "eval-response", nil)
	f.mapRow(from, to, f.now.Add(-1000*time.Hour))
	_, obj := f.deleted(gone)
	f.object(present, "blob", nil)
	f.read(present)
	f.recordOf(5, 0, nil, [2]Hash{from, to})
	f.recordOf(5, 1, []Hash{present, gone})
	mapKey := from.String() + "#" + to.String()
	mapTime := cellTimes(f.cells(mapsTableName, mapKey))
	f.takeEvents()

	rep := f.undo(undoLease(), 5, 5)
	require.Equal(int64(1), rep.Present)
	require.Equal(int64(1), rep.Restored)
	require.Equal(int64(1), rep.MapRowsPresent)
	require.Zero(rep.MapRowsRestored)
	for _, e := range f.takeEvents() {
		require.NotContains([]string{present.String(), mapKey}, e.key, "%s written", e.table)
		require.NotContains(e.key, present.String()+"-", "chunk %s written", e.key)
	}
	require.Equal(mapTime, cellTimes(f.cells(mapsTableName, mapKey)))
	attrs, err := f.bucket.Object(obj).Attrs(ctx)
	require.NoError(err)
	require.Equal(fmt.Sprint(undoCycle), attrs.Metadata[undoMetadata])
}

// An object whose bucket chunk or chunk row is gone, as after the chunk step,
// is not restored, nor is its gc row or any chunk, and neither is any object
// or map row that links it; the report names the chunk. Nor is an object
// that had no gc row. Other objects are restored.
func TestUndo_MissingChunkStopsTheKey(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newUndoFixture(t)
	h := testHashes(6)
	child, parent, other, to, rowless, noGCRow := h[0], h[1], h[2], h[3], h[4], h[5]
	f.object(to, "eval-response", nil)
	childRow, childObj := f.deleted(child)
	f.deleted(parent, child)
	f.deleted(other)
	rowlessRow, _ := f.deleted(rowless)
	f.object(noGCRow, "blob", nil)
	f.deleteRow(TableName, noGCRow)
	f.read(noGCRow)
	f.deleteRow(objectsTable, noGCRow)
	mapKey := child.String() + "#" + to.String()
	require.NoError(f.bucket.Object(childObj).Delete(ctx))
	mut := bigtable.NewMutation()
	mut.DeleteRow()
	require.NoError(f.client.Open(chunksTableName).Apply(ctx, rowlessRow, mut))
	f.recordOf(5, 0, nil, [2]Hash{child, to})
	f.recordOf(5, 1, []Hash{parent, other, noGCRow})
	f.recordOf(5, 2, []Hash{child, rowless})

	rep := f.undo(undoLease(), 5, 5)
	require.Equal(int64(2), rep.MissingChunks)
	require.Equal(int64(1), rep.WithoutGCRow)
	require.Equal(int64(1), rep.BlockedByChild)
	require.Equal(int64(1), rep.MapRowsBlocked)
	require.Equal(int64(1), rep.Restored)
	require.Equal(int64(5), rep.NotRestored())
	require.True(slices.ContainsFunc(rep.NotRestoredSample, func(s string) bool {
		return strings.HasPrefix(s, child.String()+": the chunk "+childObj+" is gone")
	}), rep.NotRestoredSample)
	for _, k := range []Hash{child, parent, rowless, noGCRow} {
		require.False(f.rowExists(objectsTableName, k.String()), k)
		require.False(f.rowExists(TableName, k.String()), k)
	}
	require.Empty(cellTimes(f.cells(chunksTableName, childRow))[objectsDataFamily+":"+chunkUndoColumn])
	require.False(f.rowExists(mapsTableName, mapKey))
	require.True(f.rowExists(objectsTableName, other.String()))
}

// A chunk step that read the attributes of a bucket chunk and found the row
// of its object absent before an undo wrote them back deletes neither the
// bucket chunk, whose metageneration the undo raised, nor the chunk row,
// which the undo gave a cell after the record, nor the gc row, whose edges
// the undo wrote after the grace start of the record. A chunk step after a
// finished undo finds the row and keeps every chunk.
func TestUndo_ChunkStepKeepsRestoredChunks(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	var f *undoFixture
	h := testHashes(2)
	raced, after := h[0], h[1]
	racedRow, racedObj := chunkNames(raced, "u")
	var once atomic.Bool
	var undone *UndoReport
	intercept := grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if strings.HasSuffix(method, "/DeleteObject") && !once.Swap(true) {
			undone = f.undo(undoLease(), stepCycle, stepCycle)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	})
	f = newUndoFixture(t)
	client, err := storage.NewGRPCClient(ctx, option.WithGRPCDialOption(intercept))
	require.NoError(err)
	t.Cleanup(func() { _ = client.Close() })

	// The bucket chunk comes first, so the chunk step deletes it before the
	// chunk row.
	f.object(raced, "blob", nil)
	f.chunkRow(racedRow, f.now.Add(-1000*time.Hour))
	f.bucketObject(racedObj)
	f.descriptor(raced, fmt.Appendf(nil, `[{"size":3,"depot":"bucket","id":"%s"},{"size":3,"depot":"bigtable","id":"%s"}]`, racedObj, racedRow))
	f.read(raced)
	f.deleteRow(objectsTable, raced)
	f.deleteRow(TableName, raced)
	afterRow, afterObj := f.deleted(after)
	f.recordOf(stepCycle, 1, []Hash{raced})
	f.recordOf(stepListCycle, 1, []Hash{after})
	require.Equal(int64(1), f.undo(undoLease(), stepListCycle, stepListCycle).Restored)
	f.later()

	d := f.deleter(f.lease, nil)
	d.bucket = client.Bucket(f.bucket.BucketName())
	require.NoError(d.deleteChunks(ctx, stepCycle+1))
	rep := d.rep.Chunks
	require.NotNil(undone)
	require.Equal(int64(1), undone.Restored)
	require.Equal(int64(1), rep.RowAbsent)
	require.Equal(int64(1), rep.KeptRowPresent)
	require.Equal(int64(2), rep.RestoredAfterRecord)
	require.Zero(rep.ChunkRowsDeleted + rep.BucketObjectsDeleted)
	for _, name := range []string{racedRow, afterRow} {
		require.True(f.rowExists(chunksTableName, name), name)
	}
	for _, name := range []string{racedObj, afterObj} {
		require.True(f.objectExists(name), name)
	}
	require.True(f.rowExists(objectsTableName, raced.String()))
	require.True(f.rowExists(TableName, raced.String()))
	require.Equal(int64(1), rep.GCRowsKept)
	require.Zero(rep.GCRowsDeleted)
}

// A parent whose child is absent and not restored by the run is not
// restored when a delete record of a cycle outside the run names the child,
// before the first cycle or after the last, so that no restored object links
// a missing one; the reason says which cycle to undo too, and once that
// cycle is past the undo window, that an undo may not restore it. A child
// that no record names, absent when the parent was written, and a present
// child block nothing. Undoing the cycles of the children too restores
// every parent, a parent deleted before its child after a second run.
func TestUndo_BlocksAParentOfAGoneChild(t *testing.T) {
	require := require.New(t)
	f := newUndoFixture(t)
	h := testHashes(8)
	parentBefore, childBefore, parentAfter, childAfter := h[0], h[1], h[2], h[3]
	withMissing, missing, withPresent, present := h[4], h[5], h[6], h[7]
	f.object(present, "blob", nil)
	f.deleted(childBefore)
	f.deleted(childAfter)
	f.deleted(parentBefore, childBefore)
	f.deleted(parentAfter, childAfter)
	f.deleted(withMissing, missing)
	f.deleted(withPresent, present)
	f.recordOf(4, 2, []Hash{childBefore})
	f.recordOf(5, 1, []Hash{parentBefore, parentAfter, withMissing, withPresent})
	f.recordOf(6, 2, []Hash{childAfter})

	rep := f.undo(undoLease(), 5, 5)
	require.Equal(int64(2), rep.BlockedByChild)
	require.Equal(int64(2), rep.Restored)
	require.ElementsMatch([]string{
		parentBefore.String() + ": links " + childBefore.String() + ", which cycle 4 deleted; undo cycle 4 too",
		parentAfter.String() + ": links " + childAfter.String() + ", which cycle 6 deleted; undo cycle 6 too",
	}, rep.NotRestoredSample)
	require.True(f.rowExists(objectsTableName, withMissing.String()))
	require.True(f.rowExists(objectsTableName, withPresent.String()))

	// Past the undo window the chunks of the child may be gone.
	f.clock.Add(int64(undoWindow + time.Minute))
	rep = f.undo(undoLease(), 5, 5)
	require.Equal(int64(2), rep.BlockedByChild)
	require.Contains(rep.NotRestoredSample[0], "deleted more than 168h0m0s ago, so an undo of that cycle cannot restore it unless its content is inline")

	// The child of cycle 4 comes back after its parent of cycle 5.
	rep = f.undo(undoLease(), 4, 6)
	require.Equal(int64(1), rep.BlockedByChild)
	require.Equal([]string{parentBefore.String() + ": links " + childBefore.String() + ", which cycle 4 deleted and this run had not restored yet; run the undo again"}, rep.NotRestoredSample)
	rep = f.undo(undoLease(), 4, 6)
	require.Zero(rep.NotRestored())
	require.Equal(int64(1), rep.Restored)
	require.True(f.rowExists(objectsTableName, parentBefore.String()))
	require.True(f.rowExists(objectsTableName, parentAfter.String()))
}

// A map row whose end a cycle outside the run deleted is not restored, so
// that it names no absent object; an undo that covers that cycle restores
// both.
func TestUndo_MapRowOfAnEndDeletedLater(t *testing.T) {
	require := require.New(t)
	f := newUndoFixture(t)
	h := testHashes(2)
	from, to := h[0], h[1]
	f.object(from, "blob", nil)
	f.deleted(to)
	f.recordOf(5, 0, nil, [2]Hash{from, to})
	f.recordOf(6, 1, []Hash{to})
	row := from.String() + "#" + to.String()

	rep := f.undo(undoLease(), 5, 5)
	require.Equal(int64(1), rep.MapRowsBlocked)
	require.Zero(rep.MapRowsRestored)
	require.False(f.rowExists(mapsTableName, row))

	rep = f.undo(undoLease(), 5, 6)
	require.Zero(rep.NotRestored())
	require.Equal(int64(1), rep.MapRowsRestored)
	require.True(f.rowExists(mapsTableName, row))
	require.True(f.rowExists(objectsTableName, to.String()))
}

// When the records of a cycle outside the run were pruned, a child that is
// absent, not restored by the run and named by no record may have been
// deleted by that cycle, so its parent is not restored.
func TestUndo_PrunedCycleBlocksAbsentChildren(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newUndoFixture(t)
	h := testHashes(2)
	parent, missing := h[0], h[1]
	f.deleted(parent, missing)
	f.recordOf(5, 1, []Hash{parent})
	require.NoError(writePrunedMarker(ctx, writeBudget{deadline: DefaultWriteDeadline}, f.stepFixture.bucket, DefaultStateName, 3))

	rep := f.undo(undoLease(), 5, 5)
	require.Equal(int64(1), rep.BlockedByChild)
	require.Equal([]string{parent.String() + ": links " + missing.String() + ", which is absent and may have been deleted by a cycle whose records were removed, such as cycle 3; an undo cannot tell"}, rep.NotRestoredSample)
	require.False(f.rowExists(objectsTableName, parent.String()))
}

// An objects row that holds only a condemn marker, as a stale condemn leaves
// in a row deleted in the same window, comes back without the marker.
func TestUndo_ClearsAStaleMarker(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newUndoFixture(t)
	key := testHashes(1)[0]
	f.deleted(key)
	marker := bigtable.NewMutation()
	marker.Set(ObjectsFamily, CondemnedColumn, bigtable.Now(), markerValue(stepCycle))
	require.NoError(f.client.Open(objectsTableName).Apply(ctx, key.String(), marker))
	f.recordOf(5, 1, []Hash{key})

	rep := f.undo(undoLease(), 5, 5)
	require.Equal(int64(1), rep.Restored)
	row := f.cells(objectsTableName, key.String())
	require.NotEmpty(row[objectsMetaFamily])
	require.Empty(row[ObjectsFamily])
}

// An undo of a cycle whose records were pruned fails before it writes, and
// so does an undo of cycles that left no record, or of a record of format
// version 1; a cycle of the range without records and without the marker
// deleted nothing and is skipped.
func TestUndo_FailsWithoutRecords(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newUndoFixture(t)
	key := testHashes(1)[0]
	f.deleted(key)
	f.recordOf(5, 1, []Hash{key})
	b := writeBudget{deadline: DefaultWriteDeadline}
	require.NoError(writePrunedMarker(ctx, b, f.stepFixture.bucket, DefaultStateName, 3))
	old := f.stepFixture.bucket.Object(recordObjectName(DefaultStateName, 8, 1, 0, 0)).NewWriter(ctx)
	_, err := old.Write(binary.BigEndian.AppendUint64(binary.BigEndian.AppendUint32([]byte(recordMagic), 1), 8))
	require.NoError(err)
	require.NoError(old.Close())
	f.takeEvents()

	_, err = f.undoWith(undoLease(), 3, 5)
	require.ErrorContains(err, "the delete records of cycle 3 were removed")
	_, err = f.undoWith(undoLease(), 6, 7)
	require.ErrorContains(err, "found no delete records of the cycles 6 to 7")
	_, err = f.undoWith(undoLease(), 8, 8)
	require.ErrorContains(err, "unsupported delete record format version 1")
	require.Empty(f.takeEvents())
	rep, err := f.undoWith(undoLease(), 4, 6)
	require.NoError(err)
	require.Equal(int64(1), rep.Restored)
}

// No undo write starts once the wall clock is within two write deadlines and
// the clock skew margin of the expiry of the lease, and the undo then stops:
// neither the stamp of a bucket chunk nor a row write.
func TestUndo_StopsBeforeLeaseExpiry(t *testing.T) {
	for _, inline := range []bool{false, true} {
		t.Run(fmt.Sprintf("inline %v", inline), func(t *testing.T) {
			require := require.New(t)
			f := newUndoFixture(t)
			key := testHashes(1)[0]
			if inline {
				f.object(key, "blob", nil)
				f.descriptor(key, []byte(`[{"size":3,"depot":"inline","id":"YWJj"}]`))
				f.read(key)
				f.deleteRow(objectsTable, key)
				f.deleteRow(TableName, key)
			} else {
				_, obj := f.deleted(key)
				t.Cleanup(func() {
					attrs, err := f.bucket.Object(obj).Attrs(context.Background())
					require.NoError(err)
					require.Empty(attrs.Metadata[undoMetadata])
				})
			}
			f.recordOf(5, 1, []Hash{key})
			f.takeEvents()
			lease := undoLease()
			lease.expiry.Store(wallNow().Add(2*DefaultWriteDeadline + clockSkewMargin - time.Second).UnixNano())

			_, err := f.undoWith(lease, 5, 5)
			require.ErrorIs(err, errLeaseExpiring)
			require.Empty(f.takeEvents())
			require.False(f.rowExists(TableName, key.String()))
			require.False(f.rowExists(objectsTableName, key.String()))
		})
	}
}

// A record that holds the deletes of another cycle than its name stops the
// undo before it restores its keys.
func TestUndo_RecordOfAnotherCycle(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newUndoFixture(t)
	key := testHashes(1)[0]
	f.deleted(key)
	h := &recordHeader{cycle: 6, runID: runID("collector", 3), gcRowsAfter: f.now.Add(-DefaultGrace)}
	_, err := writeRecord(ctx, writeBudget{deadline: DefaultWriteDeadline}, f.stepFixture.bucket, recordObjectName(DefaultStateName, 5, 1, 0, 0), h, []recordEntry{f.entries[key]})
	require.NoError(err)
	f.takeEvents()

	_, err = f.undoWith(undoLease(), 5, 5)
	require.ErrorContains(err, "holds the deletes of cycle 6")
	require.Empty(f.takeEvents())
	require.False(f.rowExists(objectsTableName, key.String()))
}

// The undo restores no refs, and names the ref records of the cycle before
// its range to its last cycle, which list the refs whose removal let those
// cycles delete.
func TestUndo_NamesTheRefRecords(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newUndoFixture(t)
	key := testHashes(1)[0]
	f.deleted(key)
	f.recordOf(5, 1, []Hash{key})
	for _, cycle := range []int64{3, 4, 6, 7} {
		w := f.stepFixture.bucket.Object(refRecordName(DefaultStateName, cycle)).NewWriter(ctx)
		require.NoError(w.Close())
	}
	rep := f.undo(undoLease(), 5, 6)
	require.Equal([]string{refRecordName(DefaultStateName, 4), refRecordName(DefaultStateName, 6)}, rep.RefRecords)
}
