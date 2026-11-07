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
	"context"
	"errors"
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/spanner"
	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"

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
	require.Equal([]string{"gc/condemned/default/1", "gc/condemned/default/2"}, f.listNames())
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
	require.Equal(int64(1), f.s.gcHooks.rescues[pathHeaders].Load())
	require.Equal(int64(1), f.s.gcHooks.rescues[pathGetObject].Load())
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
