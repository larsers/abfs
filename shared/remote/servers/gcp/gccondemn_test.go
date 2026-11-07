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

package gcp

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"cloud.google.com/go/spanner"
	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/objects"
	"github.com/google/abfs/shared/remote/servers/gcp/gc"
)

// condemnFixture is a gcFixture with a bucket for the condemned lists and the
// empty objects that every server writes at start.
type condemnFixture struct {
	*gcFixture
	bucket string
}

func newCondemnFixture(t *testing.T) *condemnFixture {
	t.Helper()
	f := newGCFixture(t)
	bucket := newEmulatorBucket(t)
	client, err := storage.NewGRPCClient(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	f.s.storage = client
	f.put(entities.EmptyFileHash, []byte("blob 0\x00"), f.old)
	f.put(entities.EmptyDirHash, []byte("tree 0\x00"), f.old)
	return &condemnFixture{gcFixture: f, bucket: bucket}
}

func (f *condemnFixture) heartbeat(id string) {
	f.t.Helper()
	require.NoError(f.t, gc.WriteHeartbeat(context.Background(), f.s.casdb.SpannerClient(), &gc.Replica{
		ID: id, Version: gc.ExtractorVersion, GC: true, TouchInterval: 36 * time.Hour, CacheTTL: 6 * time.Hour,
	}))
}

func (f *condemnFixture) opts() *gc.CondemnOpts {
	o := gc.DefaultCondemnOpts()
	o.Shards = 3
	o.ExpectedReplicas = 1
	o.Rate = 0
	o.BatchSize = 2
	return &o
}

func (f *condemnFixture) condemn(o *gc.CondemnOpts) (*gc.Result, error) {
	f.t.Helper()
	if o == nil {
		o = f.opts()
	}
	return f.s.CondemnGC(context.Background(), f.bucket, o)
}

// markerCells returns the values of every cell of g:condemned of key, all
// versions.
func (f *condemnFixture) markerCells(key entities.Hash) [][]byte {
	f.t.Helper()
	row, err := f.s.casdb.BTClient().Open("objects").ReadRow(context.Background(), key.String(), bigtable.RowFilter(bigtable.FamilyFilter(gc.ObjectsFamily)))
	require.NoError(f.t, err)
	var values [][]byte
	for _, item := range row[gc.ObjectsFamily] {
		values = append(values, item.Value)
	}
	return values
}

func marker(cycle byte) []byte {
	return []byte{0, 0, 0, 0, 0, 0, 0, cycle}
}

func (f *condemnFixture) listState() (location spanner.NullString, complete spanner.NullBool) {
	f.t.Helper()
	row, err := f.s.casdb.SpannerClient().Single().ReadRow(context.Background(), "GcState", spanner.Key{gc.DefaultStateName}, []string{"ListLocation", "ListComplete"})
	require.NoError(f.t, err)
	require.NoError(f.t, row.Columns(&location, &complete))
	return location, complete
}

func (f *condemnFixture) listNames() []string {
	f.t.Helper()
	var names []string
	it := f.s.storage.Bucket(f.bucket).Objects(context.Background(), &storage.Query{Prefix: "gc/"})
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return names
		}
		require.NoError(f.t, err)
		names = append(names, attrs.Name)
	}
}

// A condemn cycle marks exactly the unreachable objects that have an object
// row with its number, and never creates an object row for a gc row without
// one. The next cycle rewrites the markers of keys still unreachable to its
// own number, leaving one cell, so an old number cannot match later.
func TestCondemnGC_MarksUnreachableObjects(t *testing.T) {
	require := require.New(t)
	f := newCondemnFixture(t)
	old := f.old
	f.heartbeat("srv-0-aa")
	keptBlob := f.blob("kept file", old)
	keptTree := f.tree(old, keptBlob)
	kept := f.commit(keptTree, "kept", old)
	f.ref("platform/a", "refs/heads/main", kept, old)
	goneBlob := f.blob("gone file", old)
	goneTree := f.tree(old, goneBlob)
	gone := f.commit(goneTree, "gone", old)
	recent := f.blob("recent file", f.now)
	ghost := objects.HashBlobBytes([]byte("upload failed after its gc row"))
	f.gcRow(ghost, "blob", nil, old)

	res, err := f.condemn(nil)
	require.NoError(err)
	r := res.Report
	require.Equal(gc.PhaseCondemn, r.Phase)
	require.Empty(r.Condemn.Refused)
	require.Equal(int64(3), r.Condemn.Condemned)
	require.Equal(map[string]int64{"blob": 1, "tree": 1, "commit": 1}, r.Condemn.Types)
	require.Positive(r.Condemn.CondemnedBytes)
	for _, k := range []entities.Hash{gone, goneTree, goneBlob} {
		require.Equal([][]byte{marker(1)}, f.markerCells(k), k)
	}
	for _, k := range []entities.Hash{kept, keptTree, keptBlob, recent} {
		require.Empty(f.markerCells(k), k)
	}
	row, err := f.s.casdb.BTClient().Open("objects").ReadRow(context.Background(), ghost.String())
	require.NoError(err)
	require.Empty(row)

	require.Equal("gc/condemned/default/1", r.Condemn.List)
	require.Equal([]string{"gc/condemned/default/1"}, f.listNames())
	location, complete := f.listState()
	require.Equal("gc/condemned/default/1", location.StringVal)
	require.True(complete.Bool)

	res, err = f.condemn(nil)
	require.NoError(err)
	require.Equal(int64(3), res.Report.Condemn.Condemned)
	for _, k := range []entities.Hash{gone, goneTree, goneBlob} {
		require.Equal([][]byte{marker(2)}, f.markerCells(k), k)
	}
	require.Equal([]string{"gc/condemned/default/1", "gc/condemned/default/2", "gc/condemned/default/2.live"}, f.listNames())
}

// A refused cycle writes no marker, no list and no list state, and still
// returns its report.
func TestCondemnGC_RefusedRunWritesNothing(t *testing.T) {
	require := require.New(t)
	f := newCondemnFixture(t)
	f.blob("unreachable file", f.old)
	// No server host writes a heartbeat.

	res, err := f.condemn(nil)
	var refused *gc.ErrCondemnRefused
	require.ErrorAs(err, &refused)
	require.NotNil(res)
	require.Equal(refused.Reasons, res.Report.Condemn.Refused)
	require.Contains(refused.Reasons[0], "0 server hosts write a GC heartbeat, but 1 are expected")
	require.Equal(int64(1), res.Report.Objects.Unreachable)

	var cells int
	err = f.s.casdb.BTClient().Open("objects").ReadRows(context.Background(), bigtable.InfiniteRange(""), func(row bigtable.Row) bool {
		cells += len(row[gc.ObjectsFamily])
		return true
	}, bigtable.RowFilter(bigtable.FamilyFilter(gc.ObjectsFamily)))
	require.NoError(err)
	require.Zero(cells)
	require.Empty(f.listNames())
	location, _ := f.listState()
	require.False(location.Valid)
}

// The marker a condemn cycle writes is the value the server's rescue
// matches: a read through Headers or GetObject clears it.
func TestCondemnGC_ServerRescuesCondemnedObjects(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newCondemnFixture(t)
	f.heartbeat("srv-0-aa")
	blob := f.blob("condemned file", f.old)
	tree := f.tree(f.old, f.blob("other condemned file", f.old))
	_, err := f.condemn(nil)
	require.NoError(err)
	require.Equal([][]byte{marker(1)}, f.markerCells(blob))

	require.NoError(f.s.enableGCHooks(&NewServerOpts{GCTouchInterval: time.Hour}))
	t.Cleanup(f.s.stopGCHooks)
	f.s.forget(blob)
	f.s.forget(tree)
	hdrs, err := f.s.Headers(ctx, []entities.Hash{blob})
	require.NoError(err)
	require.Equal(entities.ObjectTypeBlob, hdrs[0].Type)
	require.NoError(readObject(f.s, tree))
	require.Empty(f.markerCells(blob))
	require.Empty(f.markerCells(tree))
	require.Equal(int64(1), rescuesOf(f.s, pathHeaders))
	require.Equal(int64(1), rescuesOf(f.s, pathGetObject))
}

// A cycle that cannot publish its list writes no marker, so that every
// marker is in a list that a later cycle reads. Here the list name is taken
// by another run.
func TestCondemnGC_NoMarkerWithoutList(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newCondemnFixture(t)
	f.heartbeat("srv-0-aa")
	blob := f.blob("unreachable file", f.old)
	w := f.s.storage.Bucket(f.bucket).Object("gc/condemned/default/1").NewWriter(ctx)
	w.Metadata = map[string]string{"abfs-gc-run": "another-collector#9"}
	require.NoError(w.Close())

	_, err := f.condemn(nil)
	require.ErrorContains(err, `written by the GC run "another-collector#9"`)
	require.Empty(f.markerCells(blob))
	location, _ := f.listState()
	require.False(location.Valid)
}

// A cycle clears the markers of the previous list's live keys before its own
// list replaces the previous one in GcState. A cycle that stops at its list
// write therefore leaves no marker on a live key, and the previous list stays
// in GcState for the next cycle, which leaves no marker of cycle 1.
func TestCondemnGC_ClearsLiveKeysBeforeItsList(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newCondemnFixture(t)
	f.heartbeat("srv-0-aa")
	live := f.blob("reached again", f.old)
	still := f.blob("still unreachable", f.old)
	_, err := f.condemn(nil)
	require.NoError(err)
	require.Equal([][]byte{marker(1)}, f.markerCells(live))

	f.ref("platform/a", "refs/heads/main", f.tree(f.old, live), f.old)
	w := f.s.storage.Bucket(f.bucket).Object("gc/condemned/default/2").NewWriter(ctx)
	w.Metadata = map[string]string{"abfs-gc-run": "another-collector#9"}
	require.NoError(w.Close())
	_, err = f.condemn(nil)
	require.ErrorContains(err, `written by the GC run "another-collector#9"`)
	require.Empty(f.markerCells(live))
	require.Equal([][]byte{marker(1)}, f.markerCells(still))
	location, complete := f.listState()
	require.Equal("gc/condemned/default/1", location.StringVal)
	require.True(complete.Bool)

	res, err := f.condemn(nil)
	require.NoError(err)
	require.Equal("gc/condemned/default/1", res.Report.PreviousList.Location)
	f.requireNoMarker(1)
	require.Equal([][]byte{marker(3)}, f.markerCells(still))
}

// A previous list that is gone does not stop the next cycle: the report says
// so, and the cycle condemns and rewrites the markers of what is still
// unreachable.
func TestCondemnGC_MissingPreviousList(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newCondemnFixture(t)
	f.heartbeat("srv-0-aa")
	k := f.blob("unreachable file", f.old)
	_, err := f.condemn(nil)
	require.NoError(err)
	require.NoError(f.s.storage.Bucket(f.bucket).Object("gc/condemned/default/1").Delete(ctx))

	res, err := f.condemn(nil)
	require.NoError(err)
	prev := res.Report.PreviousList
	require.Equal("gc/condemned/default/1", prev.Location)
	require.Equal("not found", prev.Error)
	require.Equal(int64(1), res.Report.Condemn.Condemned)
	require.Equal([][]byte{marker(2)}, f.markerCells(k))
	location, _ := f.listState()
	require.Equal("gc/condemned/default/2", location.StringVal)
}

// liveLine is a line of a live list.
type liveLine struct {
	Key       string     `json:"key"`
	Type      string     `json:"type"`
	Root      *gc.Root   `json:"root"`
	TouchTime *time.Time `json:"touch_time"`
	Marker    *int64     `json:"marker"`
	Rescued   bool       `json:"rescued"`
}

func (f *condemnFixture) liveLines(name string) map[string]liveLine {
	f.t.Helper()
	r, err := f.s.storage.Bucket(f.bucket).Object(name).NewReader(context.Background())
	require.NoError(f.t, err)
	defer r.Close()
	zr, err := gzip.NewReader(r)
	require.NoError(f.t, err)
	lines := make(map[string]liveLine)
	dec := json.NewDecoder(zr)
	for dec.More() {
		var l liveLine
		require.NoError(f.t, dec.Decode(&l))
		lines[l.Key] = l
	}
	return lines
}

// requireNoMarker checks that no objects row holds the marker of cycle.
func (f *condemnFixture) requireNoMarker(cycle byte) {
	f.t.Helper()
	err := f.s.casdb.BTClient().Open("objects").ReadRows(context.Background(), bigtable.InfiniteRange(""), func(row bigtable.Row) bool {
		for _, item := range row[gc.ObjectsFamily] {
			require.NotEqual(f.t, marker(cycle), item.Value, row.Key())
		}
		return true
	}, bigtable.RowFilter(bigtable.FamilyFilter(gc.ObjectsFamily)))
	require.NoError(f.t, err)
}

// The next condemn cycle clears the markers of the previous list's keys that
// are live again and lists every such key with the root that keeps it: a
// key a ref reaches again under source with the ref, a key touched since
// under recent with its touch time, and a key a server read under recent as
// rescued.
func TestCondemnGC_ListsLiveKeysOfPreviousList(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newCondemnFixture(t)
	f.heartbeat("srv-0-aa")
	reRooted := f.blob("re-rooted file", f.old)
	touched := f.blob("touched file", f.old)
	rescued := f.blob("rescued file", f.old)
	still := f.blob("still unreachable file", f.old)
	_, err := f.condemn(nil)
	require.NoError(err)

	refTime := f.now.Add(-time.Minute)
	f.ref("platform/b", "refs/heads/again", reRooted, refTime)
	touchTime := f.now.Add(-2 * time.Minute)
	f.touch(touched, touchTime)
	require.NoError(f.s.enableGCHooks(&NewServerOpts{}))
	t.Cleanup(f.s.stopGCHooks)
	f.s.forget(rescued)
	_, err = f.s.Headers(ctx, []entities.Hash{rescued})
	require.NoError(err)
	require.Empty(f.markerCells(rescued))

	res, err := f.condemn(nil)
	require.NoError(err)
	for _, k := range []entities.Hash{reRooted, touched, rescued} {
		require.Empty(f.markerCells(k), k)
	}
	require.Equal([][]byte{marker(2)}, f.markerCells(still))

	prev := res.Report.PreviousList
	require.Equal("gc/condemned/default/1", prev.Location)
	require.Equal(int64(1), prev.Cycle)
	require.True(prev.Complete)
	require.Equal(int64(4), prev.Keys)
	require.Equal(int64(2), prev.Cleared)
	require.Equal("gc/condemned/default/2.live", prev.LiveList)
	blobs := prev.Types["blob"]
	require.Equal(int64(1), blobs.StillUnreachable)
	require.Equal(map[string]int64{"source": 1, "recent": 2}, blobs.Live)
	require.Equal(int64(1), blobs.Rescued)
	require.Equal([]string{reRooted.String()}, blobs.Samples["source"])

	lines := f.liveLines(prev.LiveList)
	require.Len(lines, 3)
	ref := lines[reRooted.String()]
	require.Equal("source", ref.Root.Class)
	require.Equal("platform/b", ref.Root.Project)
	require.Equal("refs/heads/again", ref.Root.Ref)
	require.WithinDuration(refTime, *ref.Root.UpdateTime, time.Millisecond)
	require.Equal(int64(1), *ref.Marker)
	require.False(ref.Rescued)
	tl := lines[touched.String()]
	require.Equal("recent", tl.Root.Class)
	require.WithinDuration(touchTime, *tl.TouchTime, time.Millisecond)
	require.False(tl.Rescued)
	rl := lines[rescued.String()]
	require.Equal("recent", rl.Root.Class)
	require.Nil(rl.Marker)
	require.True(rl.Rescued)
	require.Equal("blob", rl.Type)
}

// A cycle that stops after half its marker batches leaves markers of its
// number on part of its list. The next cycle clears those of keys that are
// live again and rewrites the rest, so no marker of the stopped cycle is
// left for an exact-number delete to match.
func TestCondemnGC_CrashMidwayLeavesNoOldMarker(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newCondemnFixture(t)
	f.heartbeat("srv-0-aa")
	var keys []entities.Hash
	for i := range 4 {
		keys = append(keys, f.blob(fmt.Sprintf("file %d", i), f.old))
	}
	slices.SortFunc(keys, func(a, b entities.Hash) int { return strings.Compare(a.String(), b.String()) })

	// A Bigtable client whose second bulk write fails, as if the collector
	// stopped there.
	var mutates atomic.Int32
	stop := grpc.WithStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		if strings.HasSuffix(method, "/MutateRows") && mutates.Add(1) > 1 {
			return nil, status.Error(codes.PermissionDenied, "stopped by the test")
		}
		return streamer(ctx, desc, cc, method, opts...)
	})
	conn, err := grpc.NewClient(os.Getenv("BIGTABLE_EMULATOR_HOST"), grpc.WithTransportCredentials(insecure.NewCredentials()), stop)
	require.NoError(err)
	bt, err := bigtable.NewClient(ctx, emulatorProject, emulatorBigtable, option.WithGRPCConn(conn))
	require.NoError(err)
	defer bt.Close()
	_, err = gc.Condemn(ctx, f.s.casdb.SpannerClient(), bt, f.s.storage.Bucket(f.bucket), f.s, f.opts())
	require.ErrorContains(err, "stopped by the test")
	require.Equal([][]byte{marker(1)}, f.markerCells(keys[0]))
	require.Equal([][]byte{marker(1)}, f.markerCells(keys[1]))
	require.Empty(f.markerCells(keys[2]))
	location, complete := f.listState()
	require.Equal("gc/condemned/default/1", location.StringVal)
	require.False(complete.Bool)

	// One key of each batch is reached again.
	f.ref("platform/a", "refs/heads/main", f.tree(f.old, keys[0], keys[2]), f.old)
	res, err := f.condemn(nil)
	require.NoError(err)
	f.requireNoMarker(1)
	require.Empty(f.markerCells(keys[0]))
	require.Empty(f.markerCells(keys[2]))
	require.Equal([][]byte{marker(2)}, f.markerCells(keys[1]))
	require.Equal([][]byte{marker(2)}, f.markerCells(keys[3]))
	prev := res.Report.PreviousList
	require.False(prev.Complete)
	require.Equal(int64(1), prev.Cleared)
	lines := f.liveLines(prev.LiveList)
	require.Len(lines, 2)
	// The marker of a key the stopped cycle never reached is missing, which is
	// no rescue.
	require.False(lines[keys[2].String()].Rescued)
	require.Equal(int64(1), *lines[keys[0].String()].Marker)
}

// newBigtableClientWith returns a client of the emulator whose unary calls
// pass through intercept.
func newBigtableClientWith(t *testing.T, intercept grpc.UnaryClientInterceptor) *bigtable.Client {
	t.Helper()
	conn, err := grpc.NewClient(os.Getenv("BIGTABLE_EMULATOR_HOST"), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithUnaryInterceptor(intercept))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	bt, err := bigtable.NewClient(context.Background(), emulatorProject, emulatorBigtable, option.WithGRPCConn(conn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = bt.Close() })
	return bt
}

// A rescue whose touch fails fails the call and leaves the marker. Once a
// rescue touched the key, a marker of the same cycle set again, as by a
// retried or late condemn write, does not make the key unreachable: the next
// cycle finds it live through recent and clears the marker.
func TestCondemnGC_ReappliedMarkerAfterRescue(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newCondemnFixture(t)
	f.heartbeat("srv-0-aa")
	k := f.blob("used after its condemn", f.old)
	_, err := f.condemn(nil)
	require.NoError(err)
	require.Equal([][]byte{marker(1)}, f.markerCells(k))

	// No toucher, so the only touch is the rescue's.
	require.NoError(f.s.enableGCHooks(&NewServerOpts{}))
	t.Cleanup(f.s.stopGCHooks)
	rescue := f.s.gcHooks.rescue
	failTouches := newBigtableClientWith(t, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if m, ok := req.(*bigtablepb.MutateRowRequest); ok && strings.HasSuffix(m.GetTableName(), "/tables/"+gc.TableName) {
			return status.Error(codes.PermissionDenied, "touch refused by the test")
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	})
	f.s.gcHooks.rescue = gc.NewRescuer(failTouches).Rescue
	f.s.forget(k)
	_, err = f.s.Headers(ctx, []entities.Hash{k})
	require.ErrorContains(err, "touch refused by the test")
	require.Equal([][]byte{marker(1)}, f.markerCells(k))
	require.False(objectTouched(t, f.s, k))

	f.s.gcHooks.rescue = rescue
	_, err = f.s.Headers(ctx, []entities.Hash{k})
	require.NoError(err)
	require.Empty(f.markerCells(k))
	mut := bigtable.NewMutation()
	mut.Set(gc.ObjectsFamily, gc.CondemnedColumn, bigtable.Now(), marker(1))
	require.NoError(f.s.casdb.BTClient().Open("objects").Apply(ctx, k.String(), mut))

	res, err := f.condemn(nil)
	require.NoError(err)
	require.Empty(f.markerCells(k))
	line := f.liveLines(res.Report.PreviousList.LiveList)[k.String()]
	require.Equal("recent", line.Root.Class)
	require.Equal(int64(1), *line.Marker)
}
