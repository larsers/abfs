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
	"crypto/sha1"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/google/abfs/binding"
	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/remote/servers/gcp/gc"
)

type treeEntry struct {
	mode, name string
	hash       entities.Hash
}

// namedTree stores a tree with the entries, which must be in git order.
func (f *gcFixture) namedTree(entries ...treeEntry) entities.Hash {
	var body []byte
	for _, e := range entries {
		body = append(body, fmt.Appendf(nil, "%s %s\x00", e.mode, e.name)...)
		body = append(body, e.hash[:]...)
	}
	data := append(fmt.Appendf(nil, "tree %d\x00", len(body)), body...)
	return f.put(entities.Hash(sha1.Sum(data)), data, f.old)
}

// A key the mark misses because a gc row lacks an edge is in the content of
// a mount's checkout; the walk finds it by content, with the path of the
// first project in manifest order that holds it, even once it is deleted, and
// tells it from content the client wrote that no checkout holds. Its reads
// leave the markers in place and touch nothing.
func TestGCWalk_FindsKeysByContent(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newGCFixture(t)
	lib := f.blob("int lib;", f.old)
	readme := f.blob("readme", f.old)
	src := f.namedTree(treeEntry{"100644", "lib.c", lib})
	// The gc rows of src and otherRoot miss their edges to lib.
	f.gcRow(src, "tree", nil, f.old)
	root := f.namedTree(treeEntry{"100644", "README", readme}, treeEntry{"40000", "src", src})
	commit := f.commit(root, "pinned", f.old)
	otherRoot := f.namedTree(treeEntry{"100644", "lib.c", lib})
	f.gcRow(otherRoot, "tree", nil, f.old)
	other := f.commit(otherRoot, "other", f.old)
	details, pinned := f.mount("refs/abfs/mount/user/u/1", []string{commit.String(), other.String()}, f.now.Add(-time.Hour))
	written := f.blob("hostname of the build", f.old)

	res := f.run(nil)
	requireReachable(t, res, true, commit, root, src, readme)
	requireReachable(t, res, false, lib, written)
	deleteObjectRow(t, f.s, lib)
	f.s.forget(lib)
	read := []entities.Hash{details, pinned, commit, root, src}
	for _, h := range read {
		condemnObject(t, f.s, h, 3)
	}
	require.NoError(f.s.enableGCHooks(&NewServerOpts{GCReadOnly: true, GCTouchInterval: time.Hour}))
	t.Cleanup(f.s.stopGCHooks)

	got, err := f.s.WalkGC(ctx, &gc.WalkOpts{MountDetails: []entities.Hash{details}, Keys: []entities.Hash{lib, written, src, commit, pinned, lib}, Workers: 2})
	require.NoError(err)
	require.Equal([]gc.WalkKey{
		{Key: lib, Reached: true, Type: gc.WalkBlob, MountDetails: details, Commit: commit, Path: "p0/src/lib.c"},
		{Key: written},
		{Key: src, Reached: true, Type: gc.WalkTree, MountDetails: details, Commit: commit, Path: "p0/src"},
		{Key: commit, Reached: true, Type: gc.WalkCommit, MountDetails: details, Commit: commit, Path: "p0"},
		{Key: pinned, Reached: true, Type: gc.WalkPinnedManifest, MountDetails: details},
	}, got.Keys)
	require.Equal(int64(0), got.Incomplete, got.IncompleteSamples)
	require.Equal(int64(2), got.Revisions)
	require.Equal(int64(3), got.Trees)
	require.Equal(int64(3), got.Entries)
	for _, h := range read {
		require.True(isCondemned(t, f.s, h), h)
		require.False(objectTouched(t, f.s, h), h)
	}
}

// What the walk cannot read or follow makes it incomplete, so that a key it
// did not reach is not taken as outside the checkouts.
func TestGCWalk_CountsWhatItCannotFollow(t *testing.T) {
	require := require.New(t)
	f := newGCFixture(t)
	lost := f.blob("under a missing tree", f.old)
	sub := f.namedTree(treeEntry{"100644", "f", lost})
	commit := f.commit(f.namedTree(treeEntry{"40000", "sub", sub}), "pinned", f.old)
	details, _ := f.mountDetails([]string{commit.String(), "main"})
	unpinned := f.object(&binding.MountDetails{Version: binding.CurrentMountDetailsVersion, MetaRefPrefix: "user/u"}, f.old)
	deleteObjectRow(t, f.s, sub)
	f.s.forget(sub)

	got, err := f.s.WalkGC(context.Background(), &gc.WalkOpts{MountDetails: []entities.Hash{details, unpinned}, Keys: []entities.Hash{lost}, Workers: 1})
	require.NoError(err)
	require.False(got.Keys[0].Reached)
	require.Equal(int64(3), got.Incomplete, got.IncompleteSamples)
	require.Len(got.IncompleteSamples, 3)
	require.Contains(got.IncompleteSamples[0], `project p1 has revision "main"`)
	require.Contains(got.IncompleteSamples[1], "name no pinned manifest")
	require.Contains(got.IncompleteSamples[2], fmt.Sprintf("tree %s at p0/sub of commit %s", sub, commit))
}

// The walk refuses a server whose reads rescue or touch, which would keep
// the objects it is asked about.
func TestGCWalk_RefusesAServerThatRescues(t *testing.T) {
	require := require.New(t)
	s := newGCHooksServer(t, &NewServerOpts{GCTouchInterval: time.Hour})
	key := putBlob(t, s, "hello")
	_, err := s.WalkGC(context.Background(), &gc.WalkOpts{MountDetails: []entities.Hash{key}, Keys: []entities.Hash{key}})
	require.ErrorContains(err, "neither rescues nor touches")
	s.gcHooks.rescue = nil
	_, err = s.WalkGC(context.Background(), &gc.WalkOpts{MountDetails: []entities.Hash{key}, Keys: []entities.Hash{key}})
	require.ErrorContains(err, "neither rescues nor touches")
}
