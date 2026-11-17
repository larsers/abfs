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
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
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
	f.bucket = bucket
	return &condemnFixture{gcFixture: f, bucket: bucket}
}

func (f *condemnFixture) heartbeat(id string) {
	f.t.Helper()
	s := &gc.Server{ID: id, Version: gc.ExtractorVersion, GC: true, TouchInterval: 36 * time.Hour, CacheTTL: 6 * time.Hour, HeartbeatInterval: 5 * time.Minute, LinkValidation: true}
	require.NoError(f.t, gc.WriteServer(context.Background(), f.s.casdb.SpannerClient(), s, false, false))
}

// listRecord returns the row of AbfsGcLists of cycle, nil when there is none.
func (f *condemnFixture) listRecord(cycle int64) *gc.ListRecord {
	f.t.Helper()
	row, err := f.s.casdb.SpannerClient().Single().ReadRow(context.Background(), "AbfsGcLists", spanner.Key{gc.DefaultStateName, cycle}, []string{"Cycle", "Location", "CompleteTime", "CacheTTLSeconds", "Markers"})
	if spanner.ErrCode(err) == codes.NotFound {
		return nil
	}
	require.NoError(f.t, err)
	r := &gc.ListRecord{}
	var location spanner.NullString
	var complete spanner.NullTime
	var ttl, markers spanner.NullInt64
	require.NoError(f.t, row.Columns(&r.Cycle, &location, &complete, &ttl, &markers))
	r.Location, r.CompleteTime, r.CacheTTL, r.Markers = location.StringVal, complete.Time, time.Duration(ttl.Int64)*time.Second, markers.Int64
	return r
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

// markerCells returns the values of every cell of abfsgc:condemned of key, all
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
	row, err := f.s.casdb.SpannerClient().Single().ReadRow(context.Background(), "AbfsGcState", spanner.Key{gc.DefaultStateName}, []string{"ListLocation", "ListComplete"})
	require.NoError(f.t, err)
	require.NoError(f.t, row.Columns(&location, &complete))
	return location, complete
}

func (f *condemnFixture) listNames() []string {
	f.t.Helper()
	var names []string
	it := f.s.storage.Bucket(f.bucket).Objects(context.Background(), &storage.Query{Prefix: "abfsgc/"})
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
// row with its number, never creates an object row for a gc row without one,
// and completes its list in AbfsGcLists with the largest cache TTL of the
// servers. The next cycle keeps the markers of keys still unreachable: a
// recorded marker is never written again.
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

	require.Equal("abfsgc/condemned/default/1", r.Condemn.List)
	require.Equal([]string{"abfsgc/condemned/default/1"}, f.listNames())
	location, complete := f.listState()
	require.Equal("abfsgc/condemned/default/1", location.StringVal)
	require.True(complete.Bool)
	rec := f.listRecord(1)
	require.Equal("abfsgc/condemned/default/1", rec.Location)
	require.True(rec.Complete())
	require.Equal(6*time.Hour, rec.CacheTTL)
	require.Equal(int64(3), rec.Markers)
	require.Equal(6.0, r.Condemn.CacheTTLHours)

	res, err = f.condemn(nil)
	require.NoError(err)
	require.Zero(res.Report.Condemn.Condemned)
	require.Equal(int64(3), res.Report.Condemn.Kept)
	for _, k := range []entities.Hash{gone, goneTree, goneBlob} {
		require.Equal([][]byte{marker(1)}, f.markerCells(k), k)
	}
	require.Equal([]string{"abfsgc/condemned/default/1", "abfsgc/condemned/default/2"}, f.listNames())
	require.Zero(f.listRecord(2).Markers)
	require.Equal(int64(3), res.Report.Delete.Plan.Held)
	require.Zero(res.Report.Delete.Plan.Candidates)
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
	require.Contains(refused.Reasons[0], "no server that may have run within the grace wrote a GC heartbeat")
	require.Contains(refused.Reasons[1], "0 server hosts write a GC heartbeat, but 1 are expected")
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
	require.Nil(f.listRecord(1))
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

// A cycle that cannot publish its list writes no marker and no record. Here
// the list name is taken by another run.
func TestCondemnGC_NoMarkerWithoutList(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newCondemnFixture(t)
	f.heartbeat("srv-0-aa")
	blob := f.blob("unreachable file", f.old)
	w := f.s.storage.Bucket(f.bucket).Object("abfsgc/condemned/default/1").NewWriter(ctx)
	w.Metadata = map[string]string{"abfs-gc-run": "another-collector#9"}
	require.NoError(w.Close())

	_, err := f.condemn(nil)
	require.ErrorContains(err, `written by the GC run "another-collector#9"`)
	require.Empty(f.markerCells(blob))
	location, _ := f.listState()
	require.False(location.Valid)
	require.Nil(f.listRecord(1))
}

// A cycle clears the markers of the live keys that its scan read before it
// writes its list, so a cycle that stops at its list write leaves no marker
// on a live key. The marker of a key still unreachable, of a complete list,
// stays through every later cycle.
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
	w := f.s.storage.Bucket(f.bucket).Object("abfsgc/condemned/default/2").NewWriter(ctx)
	w.Metadata = map[string]string{"abfs-gc-run": "another-collector#9"}
	require.NoError(w.Close())
	_, err = f.condemn(nil)
	require.ErrorContains(err, `written by the GC run "another-collector#9"`)
	require.Empty(f.markerCells(live))
	require.Equal([][]byte{marker(1)}, f.markerCells(still))
	location, complete := f.listState()
	require.Equal("abfsgc/condemned/default/1", location.StringVal)
	require.True(complete.Bool)
	require.Nil(f.listRecord(2))

	res, err := f.condemn(nil)
	require.NoError(err)
	require.Equal("abfsgc/condemned/default/1", res.Report.PreviousList.Location)
	require.Zero(res.Report.Condemn.Cleared)
	require.Equal([][]byte{marker(1)}, f.markerCells(still))
}

// The list in AbfsGcState serves only the report: a cycle runs on when it is gone
// or cannot be read, and the markers of what is still unreachable stay.
func TestCondemnGC_MissingPreviousList(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newCondemnFixture(t)
	f.heartbeat("srv-0-aa")
	k := f.blob("unreachable file", f.old)
	_, err := f.condemn(nil)
	require.NoError(err)
	require.NoError(f.s.storage.Bucket(f.bucket).Object("abfsgc/condemned/default/1").Delete(ctx))

	res, err := f.condemn(nil)
	require.NoError(err)
	prev := res.Report.PreviousList
	require.Equal("abfsgc/condemned/default/1", prev.Location)
	require.Equal("not found", prev.Error)
	require.Zero(res.Report.Condemn.Condemned)
	require.Equal([][]byte{marker(1)}, f.markerCells(k))
	location, _ := f.listState()
	require.Equal("abfsgc/condemned/default/2", location.StringVal)

	w := f.s.storage.Bucket(f.bucket).Object("abfsgc/condemned/default/2").NewWriter(ctx)
	_, err = w.Write([]byte("not a list"))
	require.NoError(err)
	require.NoError(w.Close())
	res, err = f.condemn(nil)
	require.NoError(err)
	require.NotEmpty(res.Report.PreviousList.Error)
	require.Equal([][]byte{marker(1)}, f.markerCells(k))
}

// A dry run plans from the markers its scan read and the list records: a key
// of a complete list is held until the hold after the completion has passed,
// and then a candidate. It writes nothing.
func TestDryRunGC_PlansFromMarkers(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newCondemnFixture(t)
	f.heartbeat("srv-0-aa")
	k := f.blob("unreachable file", f.old)
	_, err := f.condemn(nil)
	require.NoError(err)
	o := gc.DefaultDryRunOpts()
	o.Shards = 3
	res, err := f.s.DryRunGC(ctx, f.bucket, &o)
	require.NoError(err)
	plan := res.Report.Delete.Plan
	require.Zero(plan.Candidates)
	require.Equal(int64(1), plan.Held)
	require.Len(plan.Lists, 1)
	require.Equal(int64(1), plan.Lists[0].Cycle)
	require.Equal(88.0, plan.Lists[0].HoldHours)
	require.False(plan.Lists[0].Eligible)

	ageList(t, f.s, 1, 89*time.Hour)
	res, err = f.s.DryRunGC(ctx, f.bucket, &o)
	require.NoError(err)
	require.Equal(int64(1), res.Report.Delete.Plan.Candidates)
	require.Equal(int64(1), res.Report.Delete.Plan.WouldDelete)
	require.Equal([][]byte{marker(1)}, f.markerCells(k))
}

// ageList moves the completion of the list of cycle back by d.
func ageList(t *testing.T, s *GCPServer, cycle int64, d time.Duration) {
	t.Helper()
	ctx := context.Background()
	sp := s.casdb.SpannerClient()
	row, err := sp.Single().ReadRow(ctx, "AbfsGcLists", spanner.Key{gc.DefaultStateName, cycle}, []string{"CompleteTime"})
	require.NoError(t, err)
	var complete time.Time
	require.NoError(t, row.Columns(&complete))
	_, err = sp.Apply(ctx, []*spanner.Mutation{spanner.Update("AbfsGcLists", []string{"Name", "Cycle", "CompleteTime"}, []any{gc.DefaultStateName, cycle, complete.Add(-d)})})
	require.NoError(t, err)
}

// liveLine is a line of a live list.
type liveLine struct {
	Key    string   `json:"key"`
	Type   string   `json:"type"`
	Root   *gc.Root `json:"root"`
	Marker int64    `json:"marker"`
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

// The next condemn cycle clears the markers of the keys that are live again
// and lists every such key with the root that keeps it: a key a ref reaches
// again under source with the ref, a key touched since under recent. A key a
// server read has no marker left, and counts as rescued in the report of the
// previous list.
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
	f.touch(touched, f.now.Add(-2*time.Minute))
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
	require.Equal([][]byte{marker(1)}, f.markerCells(still))

	prev := res.Report.PreviousList
	require.Equal("abfsgc/condemned/default/1", prev.Location)
	require.Equal(int64(1), prev.Cycle)
	require.True(prev.Complete)
	require.Equal(int64(4), prev.Keys)
	cr := res.Report.Condemn
	require.Equal(int64(2), cr.Cleared)
	require.Equal("abfsgc/condemned/default/2.live", cr.LiveList)
	require.Equal(int64(2), res.Report.Consistency.MarkedLive)
	blobs := prev.Types["blob"]
	require.Equal(int64(1), blobs.StillUnreachable)
	require.Equal(map[string]int64{"source": 1, "recent": 2}, blobs.Live)
	require.Equal(int64(1), blobs.Rescued)
	require.Equal([]string{reRooted.String()}, blobs.Samples["source"])

	lines := f.liveLines(cr.LiveList)
	require.Len(lines, 2)
	ref := lines[reRooted.String()]
	require.Equal("source", ref.Root.Class)
	require.Equal("platform/b", ref.Root.Project)
	require.Equal("refs/heads/again", ref.Root.Ref)
	require.WithinDuration(refTime, *ref.Root.UpdateTime, time.Millisecond)
	require.Equal(int64(1), ref.Marker)
	tl := lines[touched.String()]
	require.Equal("recent", tl.Root.Class)
	require.Equal("blob", tl.Type)
}

// A cycle that stops after half its marker batches leaves markers of its
// number on part of its list and a record that is not complete. The next
// cycle clears those of keys that are live again and writes the rest again
// with its own number, so no marker of the stopped cycle is left.
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
	require.Equal("abfsgc/condemned/default/1", location.StringVal)
	require.False(complete.Bool)
	require.False(f.listRecord(1).Complete())

	// One key of each batch is reached again.
	f.ref("platform/a", "refs/heads/main", f.tree(f.old, keys[0], keys[2]), f.old)
	res, err := f.condemn(nil)
	require.NoError(err)
	f.requireNoMarker(1)
	require.Empty(f.markerCells(keys[0]))
	require.Empty(f.markerCells(keys[2]))
	require.Equal([][]byte{marker(2)}, f.markerCells(keys[1]))
	require.Equal([][]byte{marker(2)}, f.markerCells(keys[3]))
	require.Equal(int64(2), res.Report.Condemn.Condemned)
	require.False(res.Report.PreviousList.Complete)
	require.Equal(int64(1), res.Report.Condemn.Cleared)
	lines := f.liveLines(res.Report.Condemn.LiveList)
	require.Len(lines, 1)
	require.Equal(int64(1), lines[keys[0].String()].Marker)
	require.Equal(int64(1), res.Report.Delete.Plan.Unrecorded)

	// The record of the stopped cycle goes once no row holds its marker, in a
	// cycle after the next one.
	require.NotNil(f.listRecord(1))
	res, err = f.condemn(nil)
	require.NoError(err)
	require.Nil(f.listRecord(1))
	require.NotNil(f.listRecord(2))
	require.Equal(int64(1), res.Report.Condemn.RecordsRemoved)
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
	line := f.liveLines(res.Report.Condemn.LiveList)[k.String()]
	require.Equal("recent", line.Root.Class)
	require.Equal(int64(1), line.Marker)
}

// The largest cache TTL that a list records is read after its markers were
// written: a server row that appears while the cycle writes them counts,
// although the first read of the cycle did not see it.
func TestCondemnGC_ListTTLReadAfterMarkers(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newCondemnFixture(t)
	f.heartbeat("srv-0-aa")
	key := f.blob("unreachable file", f.old)
	var marks atomic.Int32
	bt := newInterceptedClient(t, grpc.WithStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		cs, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			return nil, err
		}
		return &mutateLog{ClientStream: cs, add: func(e string) {
			if e == "mark" && marks.Add(1) == 1 {
				s := &gc.Server{ID: "srv-0-bb", Version: gc.ExtractorVersion, GC: true, TouchInterval: 36 * time.Hour, CacheTTL: 10 * time.Hour, HeartbeatInterval: 5 * time.Minute, LinkValidation: true}
				require.NoError(gc.WriteServer(ctx, f.s.casdb.SpannerClient(), s, false, false))
			}
		}}, nil
	}))
	res, err := gc.Condemn(ctx, f.s.casdb.SpannerClient(), bt, f.s.storage.Bucket(f.bucket), f.s, f.opts())
	require.NoError(err)
	require.Equal(int32(1), marks.Load())
	require.Equal(6.0, res.Report.Settings.CacheTTLHours)
	require.Equal(10.0, res.Report.Condemn.CacheTTLHours)
	require.Equal(10*time.Hour, f.listRecord(1).CacheTTL)
	require.Equal([][]byte{marker(1)}, f.markerCells(key))
}

// failingListRead fails every read of the Spanner table AbfsGcLists.
type failingListRead struct {
	grpc.ClientStream
	failed bool
}

func (r *failingListRead) SendMsg(m any) error {
	if req, ok := m.(*spannerpb.ReadRequest); ok && req.GetTable() == "AbfsGcLists" {
		r.failed = true
	}
	return r.ClientStream.SendMsg(m)
}

func (r *failingListRead) RecvMsg(m any) error {
	if r.failed {
		return status.Error(codes.PermissionDenied, "read refused by the test")
	}
	return r.ClientStream.RecvMsg(m)
}

// A condemn that cannot read the list records counts every marker as
// recorded: it keeps a marker without a record as it is, marks only keys
// without one, and removes no record, though the record of a list that no
// key holds the marker of would be removed after a read.
func TestCondemnGC_UnreadableListRecords(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newCondemnFixture(t)
	f.heartbeat("srv-0-aa")
	dropped := f.blob("deleted after its condemn", f.old)
	_, err := f.condemn(nil)
	require.NoError(err)
	for _, table := range []string{"objects", gc.TableName} {
		mut := bigtable.NewMutation()
		mut.DeleteRow()
		require.NoError(f.s.casdb.BTClient().Open(table).Apply(ctx, dropped.String(), mut))
	}
	recorded := f.blob("recorded marker", f.old)
	_, err = f.condemn(nil)
	require.NoError(err)
	require.Equal([][]byte{marker(2)}, f.markerCells(recorded))
	unrecorded := f.blob("marker without a record", f.old)
	mut := bigtable.NewMutation()
	mut.Set(gc.ObjectsFamily, gc.CondemnedColumn, bigtable.Now(), marker(9))
	require.NoError(f.s.casdb.BTClient().Open("objects").Apply(ctx, unrecorded.String(), mut))
	fresh := f.blob("no marker", f.old)

	sp, err := spanner.NewClient(ctx, f.s.casdb.SpannerClient().DatabaseName(), option.WithGRPCDialOption(grpc.WithStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		cs, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			return nil, err
		}
		return &failingListRead{ClientStream: cs}, nil
	})))
	require.NoError(err)
	t.Cleanup(sp.Close)
	res, err := gc.Condemn(ctx, sp, f.s.casdb.BTClient(), f.s.storage.Bucket(f.bucket), f.s, f.opts())
	require.NoError(err)
	require.Contains(res.Report.Delete.PlanError, "read refused by the test")
	require.Equal(int64(1), res.Report.Condemn.Condemned)
	require.Equal(int64(2), res.Report.Condemn.Kept)
	require.Zero(res.Report.Condemn.RecordsRemoved)
	require.Equal([][]byte{marker(2)}, f.markerCells(recorded))
	require.Equal([][]byte{marker(9)}, f.markerCells(unrecorded))
	require.Equal([][]byte{marker(3)}, f.markerCells(fresh))
	require.NotNil(f.listRecord(1))
	require.True(f.listRecord(3).Complete())

	// The same cycle with the records read removes the record of the first
	// list and writes the marker without a record again.
	res, err = f.condemn(nil)
	require.NoError(err)
	require.Equal(int64(1), res.Report.Condemn.RecordsRemoved)
	require.Nil(f.listRecord(1))
	require.Equal([][]byte{marker(4)}, f.markerCells(unrecorded))
}

// A condemn or delete cycle on a store without AbfsGcLists fails before it
// writes anything, with an error that names the init that creates the
// table; a dry run reports the error.
func TestCondemnGC_MissingListTable(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newDeleteFixture(t)
	f.blob("unreachable file", f.old)
	dc, err := database.NewDatabaseAdminClient(ctx)
	require.NoError(err)
	defer dc.Close()
	op, err := dc.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{Database: f.s.casdb.SpannerClient().DatabaseName(), Statements: []string{"DROP TABLE AbfsGcLists"}})
	require.NoError(err)
	require.NoError(op.Wait(ctx))

	o := f.deleteOpts().DryRunOpts
	res, err := gc.DryRun(ctx, f.s.casdb.SpannerClient(), f.s.casdb.BTClient(), f.s.storage.Bucket(f.bucket), f.s, &o)
	require.NoError(err)
	require.Contains(res.Report.Delete.PlanError, "init --gc")
	_, err = f.condemn(nil)
	require.ErrorContains(err, "init --gc")
	_, err = f.deleteWith(f.s.casdb.BTClient(), nil)
	require.ErrorContains(err, "init --gc")
	require.Empty(f.listNames())
	location, _ := f.listState()
	require.False(location.Valid)
}
