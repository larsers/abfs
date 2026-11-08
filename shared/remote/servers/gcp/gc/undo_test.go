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
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"cloud.google.com/go/bigtable/bttest"
	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

const (
	undoSuffix = "-r"
	undoCycle  = 9
)

// undoEvent is a write the undo sent: a Bigtable row of a table, or the
// metadata of a bucket object, at the wall-clock time of the collector.
type undoEvent struct {
	table, key string
	at         time.Time
}

// undoFixture is a chunkFixture whose Bigtable also has the restored tables,
// and which records every Bigtable write and bucket metadata update.
type undoFixture struct {
	*chunkFixture
	// bucket is the bucket of the undo, whose metadata updates are recorded.
	bucket *storage.BucketHandle
	mu     sync.Mutex
	events []undoEvent
}

func newUndoFixture(t *testing.T) *undoFixture {
	t.Helper()
	ctx := context.Background()
	f := &undoFixture{}
	bt := newUndoTestBigtable(t, grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		switch m := req.(type) {
		case *bigtablepb.MutateRowRequest:
			f.add(m.GetTableName(), string(m.GetRowKey()))
		case *bigtablepb.CheckAndMutateRowRequest:
			f.add(m.GetTableName(), string(m.GetRowKey()))
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}))
	cf := &chunkFixture{stepFixture: &stepFixture{t: t, client: bt, bucket: newTestBucket(t), now: time.Now(), shards: 1}, lease: &Lease{Epoch: 3}, checks: &fakeInstance{}}
	_, err := cf.bucket.Update(ctx, storage.BucketAttrsToUpdate{SoftDeletePolicy: &storage.SoftDeletePolicy{RetentionDuration: minSoftDeleteRetention}})
	require.NoError(t, err)
	cf.lease.expiry.Store(time.Now().Add(100 * time.Hour).UnixNano())
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

// newUndoTestBigtable starts an in-process Bigtable with the tables of the
// server, and the same tables restored under undoSuffix, and returns a client
// for it that dials with opts.
func newUndoTestBigtable(t *testing.T, opts ...grpc.DialOption) *bigtable.Client {
	t.Helper()
	ctx := context.Background()
	srv, err := bttest.NewServer("localhost:0")
	require.NoError(t, err)
	t.Cleanup(srv.Close)
	conn, err := grpc.NewClient(srv.Addr, append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, opts...)...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	adm, err := bigtable.NewAdminClient(ctx, "test-proj", "test-bt", option.WithGRPCConn(conn))
	require.NoError(t, err)
	tables := map[string][]string{
		objectsTableName: {"meta", "data", ObjectsFamily},
		mapsTableName:    {mapsFamily},
		TableName:        {EdgesFamily, TouchFamily},
		chunksTableName:  {objectsDataFamily},
	}
	for table, families := range tables {
		for _, name := range []string{table, table + undoSuffix} {
			require.NoError(t, adm.CreateTable(ctx, name))
			for _, f := range families {
				require.NoError(t, adm.CreateColumnFamily(ctx, name, f))
			}
		}
	}
	client, err := bigtable.NewClient(ctx, "test-proj", "test-bt", option.WithGRPCConn(conn))
	require.NoError(t, err)
	return client
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

// backup copies every row of the live tables to the restored ones, cell
// times included, as a restore of backups taken now would.
func (f *undoFixture) backup() {
	f.t.Helper()
	ctx := context.Background()
	for _, table := range []string{objectsTableName, TableName, mapsTableName, chunksTableName} {
		var rows []bigtable.Row
		require.NoError(f.t, f.client.Open(table).ReadRows(ctx, bigtable.InfiniteRange(""), func(row bigtable.Row) bool {
			rows = append(rows, row)
			return true
		}))
		for _, row := range rows {
			require.NoError(f.t, f.client.Open(table+undoSuffix).Apply(ctx, row.Key(), copyCells(row, false)))
		}
	}
}

// deleted writes the object key with a chunk row and a bucket chunk and the
// gc edges to its children, backs it up, and deletes its objects and gc rows
// as a delete step does. It returns the names of the two chunks.
func (f *undoFixture) deleted(key Hash, children ...Hash) (row, obj string) {
	f.t.Helper()
	f.objectWith(key, "tree", children, markerValue(stepListCycle), true)
	f.content(key, "u")
	f.backup()
	f.deleteRow(objectsTable, key)
	f.deleteRow(TableName, key)
	return chunkNames(key, "u")
}

// recordOf writes a delete record of cycle for level with one entry per key,
// with its descriptor in the backup, and a map row entry from#to for each
// pair of maps.
func (f *undoFixture) recordOf(cycle int64, level int, keys []Hash, maps ...[2]Hash) {
	f.t.Helper()
	var entries []recordEntry
	for _, k := range keys {
		var content []byte
		for _, item := range f.cells(objectsTableName+undoSuffix, k.String())[objectsDataFamily] {
			content = item.Value
		}
		entries = append(entries, recordEntry{kind: recordObject, key: k, content: recordedContent(content)})
	}
	for _, m := range maps {
		entries = append(entries, recordEntry{kind: recordMapRow, key: m[0], mapTo: m[1]})
	}
	h := &recordHeader{cycle: cycle, runID: runID("collector", 3), gcRowsAfter: f.now.Add(-DefaultGrace)}
	require.NoError(f.t, writeRecord(context.Background(), writeBudget{deadline: DefaultWriteDeadline}, f.stepFixture.bucket, recordObjectName(DefaultStateName, cycle, level, 0, 0), h, entries))
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
	o := DefaultUndoOpts()
	o.Cycle, o.Through, o.FromSuffix, o.StateName = first, last, undoSuffix, DefaultStateName
	require.NoError(f.t, o.Validate())
	u := newUndoer(f.client, f.bucket, &o)
	require.NoError(f.t, u.checkTables(context.Background()))
	rep, err := u.run(context.Background(), lease)
	require.NoError(f.t, err)
	return rep
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

// An undo restores the newest cycle of its range first and in each cycle
// the deepest level first, so that every child is back before its parent
// and a map row after its ends. Per key it keeps the bucket chunk, writes
// the chunk row with a new time, the gc row with the times of the backup
// and a touch, and the objects row last, without the condemn marker. A map
// row comes back with a new time. A cycle after the range is not undone.
func TestUndo_RestoresInOrder(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newUndoFixture(t)
	h := testHashes(7)
	a, b, c, d, from, to, later := h[0], h[1], h[2], h[3], h[4], h[5], h[6]
	f.object(from, "eval-request", nil)
	f.object(to, "eval-response", nil)
	f.mapRow(from, to, f.now.Add(-1000*time.Hour))
	f.backup()
	mapKey := from.String() + "#" + to.String()
	mut := bigtable.NewMutation()
	mut.DeleteRow()
	require.NoError(f.client.Open(mapsTableName).Apply(ctx, mapKey, mut))

	chunksOf := make(map[Hash][2]string)
	for _, k := range []struct{ key, child Hash }{{a, b}, {b, Hash{}}, {c, d}, {d, Hash{}}, {later, Hash{}}} {
		var children []Hash
		if !k.child.IsZero() {
			children = []Hash{k.child}
		}
		row, obj := f.deleted(k.key, children...)
		chunksOf[k.key] = [2]string{row, obj}
		mut := bigtable.NewMutation()
		mut.DeleteRow()
		require.NoError(f.client.Open(chunksTableName).Apply(ctx, row, mut))
	}
	f.recordOf(5, 1, []Hash{a})
	f.recordOf(5, 2, []Hash{b})
	f.recordOf(6, 0, nil, [2]Hash{from, to})
	f.recordOf(6, 1, []Hash{c})
	f.recordOf(6, 2, []Hash{d})
	f.recordOf(7, 1, []Hash{later})
	backupObject := f.cells(objectsTableName+undoSuffix, d.String())
	backupGC := f.cells(TableName+undoSuffix, d.String())
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

	// The objects row and the gc row keep the times of the backup; the
	// marker is gone and the touch is new.
	object := f.cells(objectsTableName, d.String())
	require.Empty(object[ObjectsFamily])
	require.NotEmpty(backupObject[ObjectsFamily])
	delete(backupObject, ObjectsFamily)
	require.Equal(cellTimes(backupObject), cellTimes(object))
	gcRow := f.cells(TableName, d.String())
	times := cellTimes(gcRow)
	for col, ts := range cellTimes(backupGC) {
		require.Equal(ts, times[col], col)
	}
	require.GreaterOrEqual(times[TouchFamily+":"+TouchColumn], before)
	// The chunk row and the map row are new, the bucket chunk carries the
	// cycle of the undo.
	require.GreaterOrEqual(cellTimes(f.cells(chunksTableName, chunksOf[d][0]))[objectsDataFamily+":"+objectsContentColumn], before)
	require.GreaterOrEqual(cellTimes(f.cells(mapsTableName, mapKey))[mapsFamily+":"], before)
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
	// Written again after the delete, with other chunks.
	f.object(present, "blob", nil)
	f.backup()
	f.content(present, "v")
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

// An object whose bucket chunk is gone is not restored, nor is its chunk
// row or gc row, and neither is any object or map row that links it; the
// report names the chunk. Other objects are restored. Once the chunk is
// back, as from soft delete, the next undo restores the rest.
func TestUndo_MissingBucketChunkStopsTheKey(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newUndoFixture(t)
	h := testHashes(4)
	child, parent, other, to := h[0], h[1], h[2], h[3]
	f.object(to, "eval-response", nil)
	f.mapRow(child, to, f.now.Add(-1000*time.Hour))
	childRow, childObj := f.deleted(child)
	f.deleted(parent, child)
	f.deleted(other)
	mapKey := child.String() + "#" + to.String()
	mut := bigtable.NewMutation()
	mut.DeleteRow()
	require.NoError(f.client.Open(mapsTableName).Apply(ctx, mapKey, mut))
	require.NoError(f.client.Open(chunksTableName).Apply(ctx, childRow, mut))
	require.NoError(f.bucket.Object(childObj).Delete(ctx))
	f.recordOf(5, 0, nil, [2]Hash{child, to})
	f.recordOf(5, 1, []Hash{parent, other})
	f.recordOf(5, 2, []Hash{child})

	rep := f.undo(undoLease(), 5, 5)
	require.Equal([]string{childObj}, rep.MissingBucketObjects)
	require.Equal(int64(1), rep.WaitingForBucketObjects)
	require.Equal(int64(1), rep.BlockedByChild)
	require.Equal(int64(1), rep.MapRowsBlocked)
	require.Equal(int64(1), rep.Restored)
	require.Equal(int64(3), rep.NotRestored())
	for _, k := range []Hash{child, parent} {
		require.False(f.rowExists(objectsTableName, k.String()), k)
		require.False(f.rowExists(TableName, k.String()), k)
	}
	require.False(f.rowExists(chunksTableName, childRow))
	require.False(f.rowExists(mapsTableName, mapKey))
	require.True(f.rowExists(objectsTableName, other.String()))

	f.bucketObject(childObj)
	rep = f.undo(undoLease(), 5, 5)
	require.Equal(int64(2), rep.Restored)
	require.Equal(int64(1), rep.Present)
	require.Equal(int64(1), rep.MapRowsRestored)
	require.Zero(rep.NotRestored())
	for _, k := range []Hash{child, parent} {
		require.True(f.rowExists(objectsTableName, k.String()), k)
	}
	require.True(f.rowExists(chunksTableName, childRow))
}

// A chunk step that read the attributes of a bucket chunk and found the row
// of its object absent before an undo wrote them back deletes neither the
// bucket chunk, whose metageneration the undo raised, nor the chunk row,
// which the undo wrote after the record. A chunk step after a finished undo
// finds the row and keeps every chunk.
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
	desc := f.descriptor(raced, fmt.Appendf(nil, `[{"size":3,"depot":"bucket","id":"%s"},{"size":3,"depot":"bigtable","id":"%s"}]`, racedObj, racedRow))
	afterRow, afterObj := f.deleted(after)
	f.backup()
	f.deleteRow(objectsTable, raced)
	f.deleteRow(TableName, raced)
	f.record(0, time.Time{}, []Hash{raced}, map[Hash][]byte{raced: desc})
	f.recordOf(stepListCycle, 1, []Hash{after})
	require.Equal(int64(1), f.undo(undoLease(), stepListCycle, stepListCycle).Restored)
	f.later()

	d := f.deleter(f.lease, noBackupCheck)
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
}
