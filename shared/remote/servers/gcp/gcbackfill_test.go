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
	"fmt"
	"slices"
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	"github.com/stretchr/testify/require"

	"github.com/google/abfs/binding"
	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/objects"
	"github.com/google/abfs/shared/objects/gitobjects"
	"github.com/google/abfs/shared/remote/servers/gcp/casdb"
	"github.com/google/abfs/shared/remote/servers/gcp/depot"
	"github.com/google/abfs/shared/remote/servers/gcp/gc"
)

// backfillFixture holds objects that a server without gc rows added.
type backfillFixture struct {
	s       *GCPServer
	blobs   []entities.Hash
	tree    entities.Hash
	bigTree entities.Hash
	info    entities.Hash
	keys    []entities.Hash
}

func newBackfillFixture(t *testing.T) *backfillFixture {
	t.Helper()
	require := require.New(t)
	s, _ := createTestServerWithCASDB(t)
	createGCTable(t)
	// Small chunks, so that the big tree is read from several.
	s.depot = depot.NewMasterDepot([]depot.Depot{depot.NewMemoryDepot("small", 64), depot.NewMemoryDepot("large", 1<<20)})

	f := &backfillFixture{s: s}
	for i := range 20 {
		data := fmt.Sprintf("file %d", i)
		key := objects.HashBlobBytes([]byte(data))
		require.NoError(putRaw(s, key, fmt.Appendf(nil, "blob %d\x00%s", len(data), data)))
		f.blobs = append(f.blobs, key)
	}
	var data []byte
	f.tree, data = treeOf(f.blobs[0])
	require.NoError(putRaw(s, f.tree, data))
	f.bigTree, data = treeOf(f.blobs...)
	require.Greater(len(data), 64)
	require.NoError(putRaw(s, f.bigTree, data))
	_, content, err := s.casdb.GetObjectData(context.Background(), f.bigTree)
	require.NoError(err)
	require.Contains(content.Content, `"depot":"large"`)
	info := &binding.BuildInfo{
		Version:        binding.CurrentBuildInfoVersion,
		BuildStartData: binding.BuildStartData{RootBegin: f.tree.String()},
		RootEnd:        f.bigTree.String(),
		NinjaLog:       "malformed",
	}
	require.NoError(info.Encode())
	require.NoError(putRaw(s, info.Hash, info.Encoded))
	f.info = info.Hash

	f.keys = append(slices.Clone(f.blobs), f.tree, f.bigTree, f.info)
	for _, key := range f.keys {
		require.Nil(readGCRow(t, s, key))
	}
	return f
}

func TestBackfillGC_WritesMissingRows(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newBackfillFixture(t)

	stats, err := f.s.BackfillGC(ctx, &gc.BackfillOpts{Shards: 3, BatchSize: 4})
	require.NoError(err)
	require.True(stats.Done)
	require.Equal(int64(len(f.keys)), stats.Scanned)
	require.Zero(stats.Current)
	require.Equal(int64(1), stats.Malformed)
	require.Equal(map[string]*gc.TypeCounts{
		"blob":       {Written: 20},
		"tree":       {Written: 2, Edges: 21},
		"build-info": {Written: 1, Edges: 2},
	}, stats.Types)

	wantBig := slices.Clone(f.blobs)
	slices.SortFunc(wantBig, func(a, b entities.Hash) int { return slices.Compare(a[:], b[:]) })
	require.Equal(wantBig, readGCRow(t, f.s, f.bigTree).Edges)
	require.ElementsMatch([]entities.Hash{f.tree, f.bigTree}, readGCRow(t, f.s, f.info).Edges)
	blobRow := readGCRow(t, f.s, f.blobs[0])
	require.Equal("blob", blobRow.Type)
	require.Empty(blobRow.Edges)
	// The rows have the time of the object rows, not of the backfill.
	for _, key := range f.keys {
		require.Equal(objectRowTime(t, f.s, key), readGCRow(t, f.s, key).Written, key)
	}

	// Every row is current now.
	stats, err = f.s.BackfillGC(ctx, &gc.BackfillOpts{})
	require.NoError(err)
	require.Equal(int64(len(f.keys)), stats.Current)
	require.Empty(stats.Types)
}

func TestBackfillGC_DryRunWritesNothing(t *testing.T) {
	require := require.New(t)
	f := newBackfillFixture(t)

	stats, err := f.s.BackfillGC(context.Background(), &gc.BackfillOpts{DryRun: true, Rate: 10000})
	require.NoError(err)
	require.Equal(int64(20), stats.Types["blob"].Written)
	require.Equal(int64(21), stats.Types["tree"].Edges)
	for _, key := range f.keys {
		require.Nil(readGCRow(t, f.s, key))
	}
}

func TestBackfillGC_RewritesOlderVersions(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newBackfillFixture(t)
	_, err := f.s.BackfillGC(ctx, &gc.BackfillOpts{})
	require.NoError(err)

	mut := bigtable.NewMutation()
	mut.Set(gc.EdgesFamily, gc.VersionColumn, bigtable.Now(), make([]byte, 8))
	require.NoError(f.s.casdb.BTClient().Open(gc.TableName).Apply(ctx, f.tree.String(), mut))
	require.Zero(readGCRow(t, f.s, f.tree).Version)

	stats, err := f.s.BackfillGC(ctx, &gc.BackfillOpts{})
	require.NoError(err)
	require.Equal(map[string]*gc.TypeCounts{"tree": {Written: 1, Edges: 1}}, stats.Types)
	// The cells of the older row are newer than the object row; they are
	// replaced, not shadowing the new ones.
	row := readGCRow(t, f.s, f.tree)
	require.Equal(int64(gc.ExtractorVersion), row.Version)
	require.Equal(objectRowTime(t, f.s, f.tree), row.Written)
}

// After an upgrade that changed the edge rules of deep trees only, a backfill
// rewrites the deep tree rows and leaves the others as they are.
func TestBackfillGC_RewritesOnlyOutdatedTypes(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	s, _ := createTestServerWithCASDB(t)
	createGCTable(t)
	blob := objects.HashBlobBytes([]byte("file"))
	require.NoError(putRaw(s, blob, []byte("blob 4\x00file")))
	tree, data := treeOf(blob)
	require.NoError(putRaw(s, tree, data))
	chunk := &gitobjects.DeepTreeObject{DeepTree: entities.DeepTree{Entries: []*entities.DeepEntry{
		{Mode: entities.FileModeDir, Hash: tree},
		{Name: "f0", Mode: entities.FileModeRegular, Hash: blob, Size: 4},
	}}}
	require.NoError(chunk.Encode())
	require.NoError(putRaw(s, chunk.Hash, chunk.Encoded))
	top := &gitobjects.DeepTreeObject{DeepTree: entities.DeepTree{Entries: []*entities.DeepEntry{
		{Mode: entities.FileModeDir, Hash: tree},
		{Name: "big", Mode: entities.FileModeDeepTree, Hash: chunk.Hash},
	}}}
	require.NoError(top.Encode())
	require.NoError(putRaw(s, top.Hash, top.Encoded))

	// The rows a server of version 1 wrote, after the object rows.
	v1 := bigtable.Now()
	writeGCRow(t, s, blob, "blob", nil, v1, 1)
	writeGCRow(t, s, tree, "tree", []entities.Hash{blob}, v1, 1)
	writeGCRow(t, s, chunk.Hash, "deeptree", nil, v1, 1)
	writeGCRow(t, s, top.Hash, "deeptree", nil, v1, 1)
	treeRow := readGCRow(t, s, tree)

	stats, err := s.BackfillGC(ctx, &gc.BackfillOpts{})
	require.NoError(err)
	require.Equal(int64(2), stats.Current)
	require.Equal(map[string]*gc.TypeCounts{"deeptree": {Written: 2, Edges: 1}}, stats.Types)
	row := readGCRow(t, s, top.Hash)
	require.Equal(int64(gc.ExtractorVersion), row.Version)
	require.Equal([]entities.Hash{chunk.Hash}, row.Edges)
	require.Equal(objectRowTime(t, s, top.Hash), row.Written)
	require.Equal(int64(gc.ExtractorVersion), readGCRow(t, s, chunk.Hash).Version)
	require.Equal(treeRow, readGCRow(t, s, tree))

	stats, err = s.BackfillGC(ctx, &gc.BackfillOpts{})
	require.NoError(err)
	require.Equal(int64(4), stats.Current)
	require.Empty(stats.Types)
}

// objectRowTime returns the time of the meta:type cell of the object row of
// key.
func objectRowTime(t *testing.T, s *GCPServer, key entities.Hash) time.Time {
	t.Helper()
	ts, ok := touched(t, s, key.String(), "objects", "meta", "type")
	require.True(t, ok)
	return ts.Time()
}

func TestBackfillGC_KeyRange(t *testing.T) {
	require := require.New(t)
	f := newBackfillFixture(t)
	keys := make([]string, len(f.keys))
	for i, k := range f.keys {
		keys[i] = k.String()
	}
	slices.Sort(keys)
	start, end := keys[5], keys[15]

	stats, err := f.s.BackfillGC(context.Background(), &gc.BackfillOpts{Start: start, End: end, Shards: 4, BatchSize: 3})
	require.NoError(err)
	require.True(stats.Done)
	require.Equal(int64(10), stats.Scanned)
	for i, k := range keys {
		row := readGCRow(t, f.s, entities.NewHash(k))
		if i >= 5 && i < 15 {
			require.NotNil(row, i)
		} else {
			require.Nil(row, i)
		}
	}
}

func TestBackfillGC_StopsWithResumeKey(t *testing.T) {
	require := require.New(t)
	f := newBackfillFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	stats, err := f.s.BackfillGC(ctx, &gc.BackfillOpts{Start: "4", Shards: 2})
	require.Error(err)
	require.False(stats.Done)
	require.Equal("4", stats.ResumeKey)
}

func TestBackfillGC_CountsUnreadableObjects(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newBackfillFixture(t)
	// An object row whose content is gone.
	lost := objects.HashBlobBytes([]byte("lost tree"))
	require.NoError(f.s.casdb.AddObject(ctx, &casdb.ObjectHeader{Id: lost, Type: "tree", Size: 28}, &casdb.ObjectContent{
		Encoding: "raw",
		Content:  `[{"size":35,"depot":"small","id":"gone"}]`,
	}))

	stats, err := f.s.BackfillGC(ctx, &gc.BackfillOpts{})
	require.ErrorContains(err, "1 objects")
	require.True(stats.Done)
	require.Equal(&gc.TypeCounts{Written: 2, Edges: 21, Failed: 1}, stats.Types["tree"])
	require.Nil(readGCRow(t, f.s, lost))
	require.NotNil(readGCRow(t, f.s, f.bigTree))
}

// The backfill reads objects through a server that leaves condemn markers in
// place.
func TestBackfillGC_ReadsLeaveCondemnMarkers(t *testing.T) {
	require := require.New(t)
	f := newBackfillFixture(t)
	adm, err := bigtable.NewAdminClient(context.Background(), "test-proj", "test-bt")
	require.NoError(err)
	defer adm.Close()
	require.NoError(adm.CreateColumnFamily(context.Background(), "objects", gc.ObjectsFamily))
	require.NoError(f.s.enableGCHooks(&NewServerOpts{GCReadOnly: true, GCTouchInterval: time.Hour}))
	t.Cleanup(f.s.stopGCHooks)
	condemnObject(t, f.s, f.bigTree, 3)

	_, err = f.s.BackfillGC(context.Background(), &gc.BackfillOpts{})
	require.NoError(err)
	require.NotNil(readGCRow(t, f.s, f.bigTree))
	require.True(isCondemned(t, f.s, f.bigTree))
	require.False(objectTouched(t, f.s, f.bigTree))
}
