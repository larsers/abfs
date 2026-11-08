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
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// chunkFixture is a stepFixture whose objects have a chunk row and a bucket
// chunk each, in a bucket that keeps deleted objects for seven days, with a
// wall clock that the test moves past the chunk delay.
type chunkFixture struct {
	*stepFixture
	lease  *Lease
	clock  atomic.Int64
	checks *fakeInstance
}

func newChunkFixture(t *testing.T, opts ...grpc.DialOption) *chunkFixture {
	t.Helper()
	f := &chunkFixture{stepFixture: newStepFixture(t, 1, opts...), lease: &Lease{Epoch: 3}, checks: &fakeInstance{}}
	_, err := f.bucket.Update(context.Background(), storage.BucketAttrsToUpdate{SoftDeletePolicy: &storage.SoftDeletePolicy{RetentionDuration: minSoftDeleteRetention}})
	require.NoError(t, err)
	f.lease.expiry.Store(time.Now().Add(100 * time.Hour).UnixNano())
	f.clock.Store(time.Now().UnixNano())
	orig := wallNow
	wallNow = func() time.Time { return time.Unix(0, f.clock.Load()) }
	t.Cleanup(func() { wallNow = orig })
	return f
}

// later moves the wall clock past the chunk delay of every record written so
// far.
func (f *chunkFixture) later() {
	f.clock.Add(int64(DefaultChunkDelay + time.Minute))
}

// chunked writes the object key with the chunks named by uuid.
func (f *chunkFixture) chunked(key Hash, uuid string) []byte {
	f.t.Helper()
	f.object(key, "blob", nil)
	return f.content(key, uuid)
}

// content writes a chunk row and a bucket chunk named by key and uuid, long
// ago, and sets their descriptor in the objects row of key.
func (f *chunkFixture) content(key Hash, uuid string) []byte {
	f.t.Helper()
	row, obj := chunkNames(key, uuid)
	f.chunkRow(row, f.now.Add(-1000*time.Hour))
	f.bucketObject(obj)
	return f.descriptor(key, fmt.Appendf(nil, `[{"size":3,"depot":"bigtable","id":"%s"},{"size":3,"depot":"bucket","id":"%s"}]`, row, obj))
}

func chunkNames(key Hash, uuid string) (row, obj string) {
	return key.String() + "-" + uuid + "-0", key.String() + "-" + uuid + "-1"
}

func (f *chunkFixture) descriptor(key Hash, desc []byte) []byte {
	f.t.Helper()
	mut := bigtable.NewMutation()
	mut.DeleteCellsInColumn(objectsDataFamily, objectsContentColumn)
	mut.Set(objectsDataFamily, objectsContentColumn, bigtable.Now(), desc)
	require.NoError(f.t, f.client.Open(objectsTable).Apply(context.Background(), key.String(), mut))
	return desc
}

// putRow writes the objects row of key again with the descriptor desc, as a
// put of an object without a row does.
func (f *chunkFixture) putRow(key Hash, desc []byte) {
	f.t.Helper()
	mut := bigtable.NewMutation()
	mut.Set(objectsMetaFamily, "type", bigtable.Now(), []byte("blob"))
	mut.Set(objectsDataFamily, objectsContentColumn, bigtable.Now(), desc)
	require.NoError(f.t, f.client.Open(objectsTable).Apply(context.Background(), key.String(), mut))
}

func (f *chunkFixture) chunkRow(id string, at time.Time) {
	f.t.Helper()
	mut := bigtable.NewMutation()
	mut.Set(objectsDataFamily, objectsContentColumn, bigtable.Time(at), []byte("abc"))
	require.NoError(f.t, f.client.Open(chunksTableName).Apply(context.Background(), id, mut))
}

func (f *chunkFixture) bucketObject(name string) {
	f.t.Helper()
	w := f.bucket.Object(name).NewWriter(context.Background())
	_, err := w.Write([]byte("abc"))
	require.NoError(f.t, err)
	require.NoError(f.t, w.Close())
}

func (f *chunkFixture) objectExists(name string) bool {
	f.t.Helper()
	_, err := f.bucket.Object(name).Attrs(context.Background())
	if errors.Is(err, storage.ErrObjectNotExist) {
		return false
	}
	require.NoError(f.t, err)
	return true
}

func (f *chunkFixture) deleteRow(table string, key Hash) {
	f.t.Helper()
	mut := bigtable.NewMutation()
	mut.DeleteRow()
	require.NoError(f.t, f.client.Open(table).Apply(context.Background(), key.String(), mut))
}

// links writes a newer e:links cell to the gc row of key, as a put does.
func (f *chunkFixture) links(key Hash) {
	f.t.Helper()
	mut := bigtable.NewMutation()
	mut.Set(EdgesFamily, LinksColumn, bigtable.Now(), PackEdges(nil))
	require.NoError(f.t, f.client.Open(TableName).Apply(context.Background(), key.String(), mut))
}

// record writes a delete record of cycle 5 for keys with the descriptors in
// descs, the list it deleted from completed at listComplete, as the delete
// step writes it before its deletes.
func (f *chunkFixture) record(batch int, listComplete time.Time, keys []Hash, descs map[Hash][]byte) string {
	f.t.Helper()
	entries := make([]recordEntry, len(keys))
	for i, k := range keys {
		entries[i] = recordEntry{kind: recordObject, key: k, content: recordedContent(descs[k])}
	}
	h := &recordHeader{cycle: stepCycle, runID: runID("collector", 3), gcRowsAfter: f.now.Add(-DefaultGrace), listComplete: listComplete}
	name := recordObjectName(DefaultStateName, stepCycle, 1, 0, batch)
	require.NoError(f.t, writeRecord(context.Background(), writeBudget{deadline: DefaultWriteDeadline}, f.bucket, name, h, entries))
	return name
}

func (f *chunkFixture) flagOf(name string) string {
	f.t.Helper()
	attrs, err := f.bucket.Object(name).Attrs(context.Background())
	require.NoError(f.t, err)
	return attrs.Metadata[chunksDoneMetadata]
}

// chunkStep runs the chunk step of cycle 6.
func (f *chunkFixture) chunkStep(change func(o *DeleteOpts)) (*ChunkStepReport, error) {
	f.t.Helper()
	d := f.deleter(f.lease, change)
	d.checks = f.checks
	err := d.deleteChunks(context.Background(), stepCycle+1)
	return d.rep.Chunks, err
}

func (f *chunkFixture) chunkStepOK(change func(o *DeleteOpts)) *ChunkStepReport {
	f.t.Helper()
	rep, err := f.chunkStep(change)
	require.NoError(f.t, err)
	return rep
}

func noBackupCheck(o *DeleteOpts) { o.NoBackupCheck = true }

// The chunks of a recorded object go only when its row is gone or holds
// other chunks: a row with the recorded descriptor, as after a crash between
// the record and the delete, keeps its chunks and its gc row. A row gone, as
// after a crash between the objects row and the gc row delete, loses its
// chunks and its gc row, unless a put wrote the gc row again since the grace
// start. A row written again keeps its new chunks, also one that shares a
// name with the recorded ones. The record is flagged once every key is done.
func TestChunkStep_RowCheck(t *testing.T) {
	require := require.New(t)
	f := newChunkFixture(t)
	h := testHashes(5)
	present, gone, reput, putting, inline := h[0], h[1], h[2], h[3], h[4]
	descs := make(map[Hash][]byte)
	for _, k := range h[:4] {
		descs[k] = f.chunked(k, "u")
	}
	f.object(inline, "blob", nil)
	descs[inline] = f.descriptor(inline, []byte(`[{"size":3,"depot":"inline","id":"YWJj"}]`))
	name := f.record(0, time.Time{}, h, descs)

	f.deleteRow(objectsTable, gone)
	f.deleteRow(objectsTable, reput)
	_, sharedObj := chunkNames(reput, "u")
	newRow, _ := chunkNames(reput, "v")
	f.chunkRow(newRow, time.Now())
	f.links(reput)
	f.putRow(reput, fmt.Appendf(nil, `[{"size":3,"depot":"bigtable","id":"%s"},{"size":3,"depot":"bucket","id":"%s"}]`, newRow, sharedObj))
	f.deleteRow(objectsTable, putting)
	f.links(putting)
	f.deleteRow(objectsTable, inline)
	f.later()

	rep := f.chunkStepOK(noBackupCheck)
	require.Empty(rep.Skipped)
	for _, k := range []Hash{present, gone, putting} {
		row, obj := chunkNames(k, "u")
		require.Equal(k == present, f.rowExists(chunksTableName, row), k)
		require.Equal(k == present, f.objectExists(obj), k)
	}
	oldRow, _ := chunkNames(reput, "u")
	require.False(f.rowExists(chunksTableName, oldRow))
	require.True(f.rowExists(chunksTableName, newRow))
	require.True(f.objectExists(sharedObj))
	require.True(f.rowExists(objectsTable, present.String()))
	for _, k := range []Hash{present, reput, putting} {
		require.True(f.rowExists(TableName, k.String()), k)
	}
	for _, k := range []Hash{gone, inline} {
		require.False(f.rowExists(TableName, k.String()), k)
	}
	require.Equal(int64(5), rep.Keys)
	require.Equal(int64(1), rep.KeptRowPresent)
	require.Equal(int64(1), rep.RePutAfterDelete)
	require.Equal(int64(3), rep.RowAbsent)
	require.Equal(int64(3), rep.ChunkRowsDeleted)
	require.Equal(int64(2), rep.BucketObjectsDeleted)
	require.Equal(int64(2), rep.GCRowsDeleted)
	require.Equal(int64(1), rep.GCRowsKept)
	require.Zero(rep.Errors + rep.RestoredAfterRecord)
	require.Equal(int64(1), rep.RecordsDone)
	require.Zero(rep.Unflagged)
	require.Equal("6", f.flagOf(name))
	attrs, err := f.bucket.Object(name).Attrs(context.Background())
	require.NoError(err)
	require.Equal(runID("collector", 3), attrs.Metadata[runIDMetadata])

	// A flagged record is not read again.
	rep = f.chunkStepOK(noBackupCheck)
	require.Zero(rep.Records)
}

// A chunk written or changed at or after its record stays: a bucket chunk
// written again, as by a restore from soft delete, or written again between
// the read of its generation and the delete, and a chunk row written again,
// as by an undo. The keys are done and the record is flagged.
func TestChunkStep_RestoredAfterRecord(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	h := testHashes(3)
	rewritten, newerRow, raced := h[0], h[1], h[2]
	_, racedObj := chunkNames(raced, "u")
	var f *chunkFixture
	var once atomic.Bool
	type deleteRequest interface{ GetObject() string }
	intercept := grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if r, ok := req.(deleteRequest); ok && strings.HasSuffix(method, "/DeleteObject") && r.GetObject() == racedObj && !once.Swap(true) {
			f.bucketObject(racedObj)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	})
	f = newChunkFixture(t)
	client, err := storage.NewGRPCClient(ctx, option.WithGRPCDialOption(intercept))
	require.NoError(err)
	t.Cleanup(func() { _ = client.Close() })
	attrs, err := f.bucket.Attrs(ctx)
	require.NoError(err)

	descs := make(map[Hash][]byte)
	for _, k := range h {
		descs[k] = f.chunked(k, "u")
		f.deleteRow(objectsTable, k)
	}
	f.record(0, time.Time{}, h, descs)
	_, obj := chunkNames(rewritten, "u")
	f.bucketObject(obj)
	row, _ := chunkNames(newerRow, "u")
	f.chunkRow(row, time.Now())
	f.later()

	d := f.deleter(f.lease, noBackupCheck)
	d.bucket = client.Bucket(attrs.Name)
	require.NoError(d.deleteChunks(ctx, stepCycle+1))
	rep := d.rep.Chunks
	require.True(f.objectExists(obj))
	require.True(f.rowExists(chunksTableName, row))
	require.True(f.objectExists(racedObj))
	require.Equal(int64(3), rep.RestoredAfterRecord)
	require.Equal(int64(2), rep.ChunkRowsDeleted)
	require.Equal(int64(1), rep.BucketObjectsDeleted)
	require.Equal(int64(1), rep.RecordsDone)
}

// No record is read before the chunk delay has passed since it was
// created, so that a signed URL of a chunk handed out before the delete
// still works.
func TestChunkStep_WaitsForTheChunkDelay(t *testing.T) {
	require := require.New(t)
	f := newChunkFixture(t)
	key := testHashes(1)[0]
	descs := map[Hash][]byte{key: f.chunked(key, "u")}
	f.deleteRow(objectsTable, key)
	name := f.record(0, time.Time{}, []Hash{key}, descs)
	attrs, err := f.bucket.Object(name).Attrs(context.Background())
	require.NoError(err)
	row, obj := chunkNames(key, "u")

	f.clock.Store(attrs.Created.Add(DefaultChunkDelay - time.Second).UnixNano())
	rep := f.chunkStepOK(noBackupCheck)
	require.Equal(int64(1), rep.RecordsWaiting)
	require.Zero(rep.Records)
	require.Equal(int64(1), rep.Unflagged)
	require.InDelta((DefaultChunkDelay - time.Second).Seconds(), rep.OldestUnflaggedSeconds, 1)
	require.True(f.rowExists(chunksTableName, row))
	require.True(f.objectExists(obj))

	f.clock.Store(attrs.Created.Add(DefaultChunkDelay).UnixNano())
	rep = f.chunkStepOK(noBackupCheck)
	require.Equal(int64(1), rep.RecordsDone)
	require.False(f.rowExists(chunksTableName, row))
	require.False(f.objectExists(obj))
}

// The step deletes nothing from a bucket whose settings would make a delete
// permanent or keep the bytes: soft delete shorter than seven days, unless
// the operator allows it, Object Versioning or a retention policy.
func TestChunkStep_BucketSettings(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	week := &storage.SoftDeletePolicy{RetentionDuration: minSoftDeleteRetention}
	tests := []struct {
		name  string
		attrs storage.BucketAttrs
		allow bool
		want  string
	}{
		{"seven days of soft delete", storage.BucketAttrs{SoftDeletePolicy: week}, false, ""},
		{"no soft delete", storage.BucketAttrs{}, false, "keeps deleted objects for 0s in soft delete"},
		{"six days of soft delete", storage.BucketAttrs{SoftDeletePolicy: &storage.SoftDeletePolicy{RetentionDuration: 6 * 24 * time.Hour}}, false, "keeps deleted objects for 144h0m0s"},
		{"no soft delete, allowed", storage.BucketAttrs{}, true, ""},
		{"versioning", storage.BucketAttrs{SoftDeletePolicy: week, VersioningEnabled: true}, true, "Object Versioning on"},
		{"retention policy", storage.BucketAttrs{SoftDeletePolicy: week, RetentionPolicy: &storage.RetentionPolicy{RetentionPeriod: time.Hour}}, true, "retention policy of 1h0m0s"},
		{"default event-based hold", storage.BucketAttrs{SoftDeletePolicy: week, DefaultEventBasedHold: true}, true, "event-based hold on each new object"},
		{"object retention", storage.BucketAttrs{SoftDeletePolicy: week, ObjectRetentionMode: "Enabled"}, true, "object retention Enabled"},
	}
	for _, tt := range tests {
		got := bucketProblems(&tt.attrs, tt.allow)
		if tt.want == "" {
			require.Empty(got, tt.name)
			continue
		}
		require.Len(got, 1, tt.name)
		require.Contains(got[0], tt.want, tt.name)
	}

	// A new bucket of the emulator has no soft delete; the emulator cannot
	// turn Object Versioning off again.
	setup := func(versioning bool) (*chunkFixture, string, string) {
		f := newChunkFixture(t)
		f.bucket = newTestBucket(t)
		_, err := f.bucket.Update(ctx, storage.BucketAttrsToUpdate{VersioningEnabled: versioning})
		require.NoError(err)
		key := testHashes(1)[0]
		descs := map[Hash][]byte{key: f.chunked(key, "u")}
		f.deleteRow(objectsTable, key)
		name := f.record(0, time.Time{}, []Hash{key}, descs)
		f.later()
		_, obj := chunkNames(key, "u")
		return f, name, obj
	}
	allow := func(o *DeleteOpts) { o.NoBackupCheck, o.AllowNoSoftDelete = true, true }
	for _, step := range []struct {
		change     func(o *DeleteOpts)
		versioning bool
	}{
		{noBackupCheck, false},
		{allow, true},
	} {
		f, name, obj := setup(step.versioning)
		rep := f.chunkStepOK(step.change)
		require.Len(rep.Skipped, 1)
		require.Zero(rep.Records)
		require.Equal(int64(1), rep.Unflagged)
		require.True(f.objectExists(obj))
		require.Empty(f.flagOf(name))
	}
	f, name, obj := setup(false)
	rep := f.chunkStepOK(allow)
	require.Empty(rep.Skipped)
	require.Equal("6", f.flagOf(name))
	require.False(f.objectExists(obj))
}

// Chunk rows have no soft delete, so a record is processed only when a ready
// backup of the chunks table that has not expired started after the list it
// deleted from was complete; the operator can skip the check.
func TestChunkStep_BackupCheck(t *testing.T) {
	require := require.New(t)
	f := newChunkFixture(t)
	key := testHashes(1)[0]
	descs := map[Hash][]byte{key: f.chunked(key, "u")}
	f.deleteRow(objectsTable, key)
	listComplete := time.Now().Add(-time.Hour)
	name := f.record(0, listComplete, []Hash{key}, descs)
	f.later()
	row, _ := chunkNames(key, "u")
	after := listComplete.Add(time.Minute)
	refused := []struct {
		name    string
		backups []Backup
	}{
		{"no backup", nil},
		{"before the list", []Backup{{Table: chunksTableName, Start: listComplete.Add(-time.Minute), Ready: true}}},
		{"not ready", []Backup{{Table: chunksTableName, Start: after}}},
		{"expired", []Backup{{Table: chunksTableName, Start: after, Ready: true, Expire: time.Unix(0, f.clock.Load()).Add(-time.Minute)}}},
		{"another table", []Backup{{Table: objectsTable, Start: after, Ready: true}}},
	}
	for _, tt := range refused {
		f.checks.backups = tt.backups
		rep := f.chunkStepOK(nil)
		require.Empty(rep.Skipped, tt.name)
		require.Equal(int64(1), rep.RecordsWithoutBackup, tt.name)
		require.Equal(int64(1), rep.Unflagged, tt.name)
		require.True(f.rowExists(chunksTableName, row), tt.name)
	}
	f.checks.backups = []Backup{{Table: chunksTableName, Start: after, Ready: true, Expire: time.Unix(0, f.clock.Load()).Add(time.Hour)}}
	rep := f.chunkStepOK(nil)
	require.Equal(int64(1), rep.RecordsDone)
	require.Equal("6", f.flagOf(name))
	require.False(f.rowExists(chunksTableName, row))
	require.True(f.checks.backupsCalled)
}

// A key the step cannot finish leaves its record without the flag, and the
// next step reads it again: a chunk of a depot the step does not know, or a
// bucket delete refused by an object hold or a retention policy.
func TestChunkStep_KeepsUnfinishedRecords(t *testing.T) {
	require := require.New(t)
	tests := []struct {
		err  error
		want bucketOutcome
	}{
		{nil, bucketDeleted},
		{storage.ErrObjectNotExist, bucketGone},
		{status.Error(codes.FailedPrecondition, "generation"), bucketPreconditionFailed},
		{&googleapi.Error{Code: http.StatusPreconditionFailed}, bucketPreconditionFailed},
		{&googleapi.Error{Code: http.StatusForbidden, Message: "retentionPolicyNotMet"}, bucketRefused},
		{status.Error(codes.PermissionDenied, "object hold"), bucketRefused},
		{status.Error(codes.Internal, "injected"), bucketFailed},
	}
	for _, tt := range tests {
		require.Equal(tt.want, bucketDeleteOutcome(tt.err), "%v", tt.err)
	}

	f := newChunkFixture(t)
	key := testHashes(1)[0]
	f.object(key, "blob", nil)
	descs := map[Hash][]byte{key: f.descriptor(key, []byte(`[{"size":3,"depot":"mem","id":"x"}]`))}
	f.deleteRow(objectsTable, key)
	name := f.record(0, time.Time{}, []Hash{key}, descs)
	f.later()
	for range 2 {
		rep := f.chunkStepOK(noBackupCheck)
		require.Equal(int64(1), rep.Records)
		require.Equal(int64(1), rep.UnknownDepot)
		require.Zero(rep.RecordsDone)
		require.Equal(int64(1), rep.Unflagged)
		require.Empty(f.flagOf(name))
	}
}

// No chunk delete starts once the wall clock is within two write deadlines
// and the clock skew margin of the expiry of the lease, and the step then
// stops the cycle: neither a chunk row delete nor a bucket chunk delete.
func TestChunkStep_StopsBeforeLeaseExpiry(t *testing.T) {
	for _, bucketOnly := range []bool{false, true} {
		t.Run(fmt.Sprintf("bucket only %v", bucketOnly), func(t *testing.T) {
			require := require.New(t)
			f := newChunkFixture(t)
			key := testHashes(1)[0]
			descs := map[Hash][]byte{key: f.chunked(key, "u")}
			row, obj := chunkNames(key, "u")
			if bucketOnly {
				descs[key] = f.descriptor(key, fmt.Appendf(nil, `[{"size":3,"depot":"bucket","id":"%s"}]`, obj))
			}
			f.deleteRow(objectsTable, key)
			name := f.record(0, time.Time{}, []Hash{key}, descs)
			f.later()
			f.lease.expiry.Store(f.clock.Load() + int64(2*DefaultWriteDeadline+clockSkewMargin-time.Second))
			_, err := f.chunkStep(noBackupCheck)
			require.ErrorIs(err, errLeaseExpiring)
			require.True(f.rowExists(chunksTableName, row))
			require.True(f.objectExists(obj))
			require.Empty(f.flagOf(name))
		})
	}
}

// A bucket chunk under a hold or a retention is not deleted, and one whose
// delete the bucket refuses stays: a refusal answered as a failed
// precondition while the generation and metageneration are the ones read,
// or as a permission error. Each counts as refused and leaves its record
// without the flag, so that the next step tries it again; once the refusal
// is gone, the chunk is deleted and the record flagged. The release of a
// hold changes the object after its record, which keeps it.
func TestChunkStep_HeldChunks(t *testing.T) {
	now := time.Now()
	for _, tt := range []struct {
		attrs storage.ObjectAttrs
		want  bool
	}{
		{storage.ObjectAttrs{}, false},
		{storage.ObjectAttrs{EventBasedHold: true}, true},
		{storage.ObjectAttrs{TemporaryHold: true}, true},
		{storage.ObjectAttrs{Retention: &storage.ObjectRetention{RetainUntil: now.Add(time.Hour)}}, true},
		{storage.ObjectAttrs{Retention: &storage.ObjectRetention{RetainUntil: now.Add(-time.Hour)}}, false},
		{storage.ObjectAttrs{RetentionExpirationTime: now.Add(time.Hour)}, true},
		{storage.ObjectAttrs{RetentionExpirationTime: now.Add(-time.Hour)}, false},
	} {
		require.Equal(t, tt.want, held(&tt.attrs, now), "%+v", tt.attrs)
	}

	answers := map[string]error{
		"failed precondition": status.Error(codes.FailedPrecondition, "object under hold"),
		"permission denied":   status.Error(codes.PermissionDenied, "object under hold"),
	}
	for _, name := range []string{"temporary hold", "failed precondition", "permission denied"} {
		t.Run(name, func(t *testing.T) {
			require := require.New(t)
			ctx := context.Background()
			var refuse atomic.Bool
			var deletes atomic.Int64
			intercept := grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				if strings.HasSuffix(method, "/DeleteObject") {
					deletes.Add(1)
					if refuse.Load() && answers[name] != nil {
						return answers[name]
					}
				}
				return invoker(ctx, method, req, reply, cc, opts...)
			})
			f := newChunkFixture(t)
			client, err := storage.NewGRPCClient(ctx, option.WithGRPCDialOption(intercept))
			require.NoError(err)
			t.Cleanup(func() { _ = client.Close() })
			key := testHashes(1)[0]
			_, obj := chunkNames(key, "u")
			f.object(key, "blob", nil)
			f.bucketObject(obj)
			descs := map[Hash][]byte{key: f.descriptor(key, fmt.Appendf(nil, `[{"size":3,"depot":"bucket","id":"%s"}]`, obj))}
			f.deleteRow(objectsTable, key)
			if name == "temporary hold" {
				_, err := f.bucket.Object(obj).Update(ctx, storage.ObjectAttrsToUpdate{TemporaryHold: true})
				require.NoError(err)
			}
			record := f.record(0, time.Time{}, []Hash{key}, descs)
			f.later()
			step := func() *ChunkStepReport {
				d := f.deleter(f.lease, noBackupCheck)
				d.bucket = client.Bucket(f.bucket.BucketName())
				require.NoError(d.deleteChunks(ctx, stepCycle+1))
				return d.rep.Chunks
			}

			refuse.Store(true)
			rep := step()
			require.Equal(int64(1), rep.DeleteRefused)
			require.Zero(rep.RestoredAfterRecord + rep.BucketObjectsDeleted + rep.RecordsDone)
			require.Equal(int64(1), rep.Unflagged)
			require.Empty(f.flagOf(record))
			require.True(f.objectExists(obj))
			if name == "temporary hold" {
				require.Zero(deletes.Load())
				_, err := f.bucket.Object(obj).Update(ctx, storage.ObjectAttrsToUpdate{TemporaryHold: false})
				require.NoError(err)
			}

			// The release of a hold changed the chunk after its record, which
			// is no restore.
			refuse.Store(false)
			rep = step()
			require.Equal(int64(1), rep.RecordsDone)
			require.Zero(rep.RestoredAfterRecord)
			require.Equal(int64(1), rep.BucketObjectsDeleted)
			require.False(f.objectExists(obj))
		})
	}
}

// Only a new write, a restore from soft delete or the mark of an undo keeps
// a bucket chunk for good. Another change of its metadata after the record,
// as a label or a hold placed between the read and the delete, is no
// restore: the chunk stays while it is held, and a later step deletes it.
func TestChunkStep_MetadataChangesAreNoRestore(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	h := testHashes(3)
	undone, labeled, raced := h[0], h[1], h[2]
	_, undoneObj := chunkNames(undone, "u")
	_, labeledObj := chunkNames(labeled, "u")
	_, racedObj := chunkNames(raced, "u")
	var f *chunkFixture
	var once atomic.Bool
	var holdErr error
	type deleteRequest interface{ GetObject() string }
	intercept := grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if r, ok := req.(deleteRequest); ok && strings.HasSuffix(method, "/DeleteObject") && r.GetObject() == racedObj && !once.Swap(true) {
			_, holdErr = f.bucket.Object(racedObj).Update(ctx, storage.ObjectAttrsToUpdate{TemporaryHold: true})
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	})
	f = newChunkFixture(t)
	client, err := storage.NewGRPCClient(ctx, option.WithGRPCDialOption(intercept))
	require.NoError(err)
	t.Cleanup(func() { _ = client.Close() })
	descs := make(map[Hash][]byte)
	for _, k := range h {
		_, obj := chunkNames(k, "u")
		f.object(k, "blob", nil)
		f.bucketObject(obj)
		descs[k] = f.descriptor(k, fmt.Appendf(nil, `[{"size":3,"depot":"bucket","id":"%s"}]`, obj))
		f.deleteRow(objectsTable, k)
	}
	record := f.record(0, time.Time{}, h, descs)
	for obj, metadata := range map[string]map[string]string{undoneObj: {undoMetadata: "5"}, labeledObj: {"label": "x"}} {
		_, err := f.bucket.Object(obj).Update(ctx, storage.ObjectAttrsToUpdate{Metadata: metadata})
		require.NoError(err)
	}
	f.later()
	step := func() *ChunkStepReport {
		d := f.deleter(f.lease, noBackupCheck)
		d.bucket = client.Bucket(f.bucket.BucketName())
		require.NoError(d.deleteChunks(ctx, stepCycle+1))
		return d.rep.Chunks
	}

	rep := step()
	require.NoError(holdErr)
	require.Equal(int64(1), rep.RestoredAfterRecord)
	require.Equal(int64(1), rep.BucketObjectsDeleted)
	require.Equal(int64(1), rep.DeleteRefused)
	require.Empty(f.flagOf(record))
	require.True(f.objectExists(undoneObj))
	require.False(f.objectExists(labeledObj))
	require.True(f.objectExists(racedObj))

	_, err = f.bucket.Object(racedObj).Update(ctx, storage.ObjectAttrsToUpdate{TemporaryHold: false})
	require.NoError(err)
	rep = step()
	require.Equal(int64(1), rep.RecordsDone)
	require.Equal(int64(1), rep.RestoredAfterRecord)
	require.Equal(int64(1), rep.BucketObjectsDeleted)
	require.True(f.objectExists(undoneObj))
	require.False(f.objectExists(racedObj))
}

// The chunk step after a delete step deletes the chunks of the deleted keys
// only. A key rescued between the read of the delete step and its delete
// keeps its chunks and its gc row, so that the next scan finds no object
// without a gc row, and its record is flagged; a key whose marker had
// changed before the read is in no record.
func TestDeleteStep_ChunksOfDeletedKeysOnly(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	h := testHashes(3)
	deleted, rescued, changed := h[0], h[1], h[2]
	var f *chunkFixture
	var once atomic.Bool
	intercept := grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		m, ok := req.(*bigtablepb.CheckAndMutateRowRequest)
		if ok && strings.HasSuffix(m.GetTableName(), "/"+objectsTable) && string(m.GetRowKey()) == rescued.String() && !once.Swap(true) {
			// The rescue writes its touch, then clears the marker.
			touch := bigtable.NewMutation()
			touch.Set(TouchFamily, "touch", bigtable.Now(), nil)
			require.NoError(f.client.Open(TableName).Apply(ctx, rescued.String(), touch))
			unmark := bigtable.NewMutation()
			unmark.DeleteCellsInColumn(ObjectsFamily, CondemnedColumn)
			require.NoError(f.client.Open(objectsTable).Apply(ctx, rescued.String(), unmark))
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	})
	f = newChunkFixture(t, intercept)
	for _, k := range h {
		f.chunked(k, "u")
	}
	marker := bigtable.NewMutation()
	marker.Set(ObjectsFamily, CondemnedColumn, bigtable.Now(), markerValue(stepListCycle+1))
	require.NoError(f.client.Open(objectsTable).Apply(ctx, changed.String(), marker))
	res := f.mark(nil)
	d := f.deleter(f.lease, nil)
	_, err := d.deleteStep(ctx, f.lease, res)
	require.NoError(err)
	step := d.rep.Step
	require.Equal(int64(1), step.Deleted)
	require.Equal(int64(2), step.NotMatched)
	f.later()

	rep := f.chunkStepOK(noBackupCheck)
	require.Equal(step.RecordsWritten, rep.RecordsDone)
	require.Equal(int64(2), rep.Keys)
	require.Equal(int64(1), rep.KeptRowPresent)
	require.Equal(int64(1), rep.RowAbsent)
	for _, k := range h {
		row, obj := chunkNames(k, "u")
		require.Equal(k != deleted, f.rowExists(chunksTableName, row), k)
		require.Equal(k != deleted, f.objectExists(obj), k)
		require.Equal(k != deleted, f.rowExists(TableName, k.String()), k)
	}
	g, _, err := scanNodes(ctx, f.client.Open(TableName), res.bounds, f.now.Add(-DefaultGrace))
	require.NoError(err)
	scan, err := checkConsistency(ctx, f.client.Open(objectsTable), res.bounds, g, newBitmap(g.size()), time.Now(), false)
	require.NoError(err)
	require.Zero(scan.report.MissingGCRows)
}

// laterBy moves the wall clock d ahead for the rest of the test.
func laterBy(t *testing.T, d time.Duration) {
	orig := wallNow
	wallNow = func() time.Time { return orig().Add(d) }
	t.Cleanup(func() { wallNow = orig })
}

// leaseAt is a lease that does not expire within an hour of the wall clock.
func leaseAt() *Lease {
	l := &Lease{Epoch: 3}
	l.expiry.Store(wallNow().Add(time.Hour).UnixNano())
	return l
}

// Pruning deletes the records of the cycles beyond the newest keep ones
// whose records are all flagged and older than the soft delete retention of
// the bucket, and at least seven days, so that an undo finds them while it
// can restore what they name. A record without the flag, whose chunks may
// still be to delete, keeps every record of its cycle. Before it deletes the
// records of a cycle, it writes the pruned marker of the cycle, which it
// keeps, and every write is within the write budget.
func TestPruneRecords(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	bucket := newTestBucket(t)
	write := func(name string, flagged bool) {
		w := bucket.Object(name).NewWriter(ctx)
		if flagged {
			w.Metadata = map[string]string{chunksDoneMetadata: "9"}
		}
		require.NoError(w.Close())
	}
	type record struct {
		name          string
		flagged, kept bool
	}
	records := []record{
		{"gc/deleted/default/3/0-0-0", true, true},
		{"gc/deleted/default/3/1-0-0", false, true},
		{"gc/deleted/default/4/1-2-0", true, false},
		{"gc/deleted/default/4/1-3-0", true, false},
		{"gc/deleted/default/5/1-0-0", true, false},
		{"gc/deleted/default/7/1-0-0", true, true},
		{"gc/deleted/default/8/1-0-0", true, true},
		{"gc/deleted/default/notes", true, true},
		{"gc/deleted/other/1/1-0-0", true, true},
	}
	for _, r := range records {
		write(r.name, r.flagged)
	}
	markers := []string{prunedMarkerName(DefaultStateName, 4), prunedMarkerName(DefaultStateName, 5)}
	prune := func(lease *Lease) (int64, error) {
		return pruneRecords(ctx, writeBudget{deadline: DefaultWriteDeadline, lease: lease}, bucket, DefaultStateName, 2)
	}
	requireRecords := func(pruned bool) {
		for _, r := range records {
			_, err := bucket.Object(r.name).Attrs(ctx)
			if r.kept || !pruned {
				require.NoError(err, r.name)
			} else {
				require.ErrorIs(err, storage.ErrObjectNotExist, r.name)
			}
		}
		for _, m := range markers {
			attrs, err := bucket.Object(m).Attrs(ctx)
			if !pruned {
				require.ErrorIs(err, storage.ErrObjectNotExist, m)
				continue
			}
			require.NoError(err, m)
			require.NotEmpty(attrs.Metadata[chunksDoneMetadata], m)
		}
	}

	// Younger than seven days.
	removed, err := prune(leaseAt())
	require.NoError(err)
	require.Zero(removed)
	requireRecords(false)

	laterBy(t, undoWindow+time.Hour)
	expiring := &Lease{}
	expiring.expiry.Store(wallNow().Add(2*DefaultWriteDeadline + clockSkewMargin - time.Second).UnixNano())
	removed, err = prune(expiring)
	require.ErrorIs(err, errLeaseExpiring)
	require.Zero(removed)
	requireRecords(false)

	// Younger than the soft delete retention of the bucket.
	_, err = bucket.Update(ctx, storage.BucketAttrsToUpdate{SoftDeletePolicy: &storage.SoftDeletePolicy{RetentionDuration: undoWindow + 2*time.Hour}})
	require.NoError(err)
	removed, err = prune(leaseAt())
	require.NoError(err)
	require.Zero(removed)
	requireRecords(false)

	laterBy(t, 2*time.Hour)
	removed, err = prune(leaseAt())
	require.NoError(err)
	require.Equal(int64(3), removed)
	requireRecords(true)
	removed, err = prune(leaseAt())
	require.NoError(err)
	require.Zero(removed)
	requireRecords(true)
}

// The retention of a delete record counts from its flag, when its chunks
// entered soft delete, not from its write: a record whose chunk step ran days
// late stays while an undo can still restore its chunks.
func TestPruneRecords_RetentionCountsFromTheFlag(t *testing.T) {
	require := require.New(t)
	now := time.Now()
	oldest := now.Add(-undoWindow)
	old := now.Add(-undoWindow - 24*time.Hour)
	flagged := map[string]string{chunksDoneMetadata: "9"}
	for _, tt := range []struct {
		name  string
		attrs storage.ObjectAttrs
		want  bool
	}{
		{"flagged now", storage.ObjectAttrs{Created: old, Updated: now, Metadata: flagged}, false},
		{"flagged long ago", storage.ObjectAttrs{Created: old, Updated: old, Metadata: flagged}, true},
		{"unflagged", storage.ObjectAttrs{Created: old, Updated: old}, false},
	} {
		require.Equal(tt.want, flaggedBefore(&tt.attrs, oldest), tt.name)
	}

	// The flag write moves the update time of the record.
	ctx := context.Background()
	bucket := newTestBucket(t)
	obj := bucket.Object("gc/deleted/default/3/0-0-0")
	require.NoError(obj.NewWriter(ctx).Close())
	time.Sleep(10 * time.Millisecond)
	attrs, err := obj.Update(ctx, storage.ObjectAttrsToUpdate{Metadata: flagged})
	require.NoError(err)
	require.True(attrs.Updated.After(attrs.Created), "updated %s, created %s", attrs.Updated, attrs.Created)
}

// The chunk delay must exceed the lifetime of a signed URL of a chunk by
// twice the clock skew margin: the servers date the URLs by their clocks and
// the step judges the age of a record by its own.
func TestDeleteOpts_ChunkDelayCoversClockSkew(t *testing.T) {
	for _, tt := range []struct {
		delay time.Duration
		ok    bool
	}{
		{signedURLLifetime + time.Second, false},
		{signedURLLifetime + 2*clockSkewMargin - time.Second, false},
		{signedURLLifetime + 2*clockSkewMargin, true},
	} {
		o := DefaultDeleteOpts()
		o.MinReplicas, o.ChunkDelay = 2, tt.delay
		err := o.Validate()
		if tt.ok {
			require.NoError(t, err, tt.delay)
		} else {
			require.ErrorContains(t, err, "by twice the clock skew margin", tt.delay)
		}
	}
}
