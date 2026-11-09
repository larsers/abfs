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
	require.NoError(got.Err())
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
	require.Contains(got.IncompleteSamples[2], fmt.Sprintf("tree %s at p0/sub of revision %s", sub, commit))
	require.EqualError(got.Err(), "the walk is incomplete: 3 mount details, revisions or objects could not be read or followed")
}

// A mount whose workspace differs from its commit pins the workspace's tree
// in an annotation and mounts that tree. The walk follows it too and tells
// what it found there by type.
func TestGCWalk_FollowsTheAnnotatedTree(t *testing.T) {
	require := require.New(t)
	f := newGCFixture(t)
	committed := f.blob("committed", f.old)
	edited := f.blob("edited in the parent workspace", f.old)
	commit := f.commit(f.namedTree(treeEntry{"100644", "f", committed}), "pinned", f.old)
	sub := f.namedTree(treeEntry{"100644", "f", edited})
	wsTree := f.namedTree(treeEntry{"100644", "f", committed}, treeEntry{"40000", "sub", sub})
	manifest := "<manifest>\n<remote name=\"origin\" fetch=\"https://example.com\"/>\n<default revision=\"main\" remote=\"origin\"/>\n" +
		fmt.Sprintf("<project name=\"p0\" path=\"p0\" revision=\"%s\">\n<annotaion name=\"abfs.dir.hash.tag\" value=\"%s\"/>\n</project>\n", commit, wsTree) +
		fmt.Sprintf("<project name=\"p1\" path=\"p1\" revision=\"%s\">\n<annotaion name=\"abfs.dir.hash.tag\" value=\"main\"/>\n</project>\n</manifest>\n", commit)
	pinned := f.blob(manifest, f.old)
	details := f.object(&binding.MountDetails{Version: binding.CurrentMountDetailsVersion, MetaRefPrefix: "user/u", PinnedManifestHash: pinned}, f.old)

	got, err := f.s.WalkGC(context.Background(), &gc.WalkOpts{MountDetails: []entities.Hash{details}, Keys: []entities.Hash{committed, edited, wsTree}, Workers: 1})
	require.NoError(err)
	require.Equal([]gc.WalkKey{
		{Key: committed, Reached: true, Type: gc.WalkBlob, MountDetails: details, Commit: commit, Path: "p0/f"},
		{Key: edited, Reached: true, Type: gc.WalkDirHash + gc.WalkBlob, MountDetails: details, Commit: commit, Path: "p0/sub/f"},
		{Key: wsTree, Reached: true, Type: gc.WalkDirHash + gc.WalkTree, MountDetails: details, Commit: commit, Path: "p0"},
	}, got.Keys)
	require.Equal(int64(2), got.Revisions)
	require.Equal(int64(1), got.Incomplete, got.IncompleteSamples)
	require.Contains(got.IncompleteSamples[0], `project p1 has abfs.dir.hash.tag "main", which is not a hash`)
}

// A manifest can pin a project at an annotated tag. The walk follows the tag,
// and a tag of a tag, to the commit and walks the commit's tree.
func TestGCWalk_FollowsAnnotatedTags(t *testing.T) {
	require := require.New(t)
	f := newGCFixture(t)
	file := f.blob("tagged", f.old)
	commit := f.commit(f.namedTree(treeEntry{"100644", "f", file}), "release", f.old)
	tag := f.tag(commit, "commit", "v1")
	outer := f.tag(tag, "tag", "v1-signed")
	details, _ := f.mountDetails([]string{outer.String()})

	got, err := f.s.WalkGC(context.Background(), &gc.WalkOpts{MountDetails: []entities.Hash{details}, Keys: []entities.Hash{file, commit, tag}, Workers: 1})
	require.NoError(err)
	require.Equal(int64(0), got.Incomplete, got.IncompleteSamples)
	require.Equal([]gc.WalkKey{
		{Key: file, Reached: true, Type: gc.WalkBlob, MountDetails: details, Commit: outer, Path: "p0/f"},
		{Key: commit, Reached: true, Type: gc.WalkCommit, MountDetails: details, Commit: outer, Path: "p0"},
		{Key: tag, Reached: true, Type: gc.WalkTag, MountDetails: details, Commit: outer, Path: "p0"},
	}, got.Keys)
}

// The walk follows at most 10 nested tags, and only to a commit. A pinned
// revision or a tag target that is neither a commit nor a tag, or that cannot
// be read, leaves the walk incomplete and is reported by what pinned it, not
// as a commit.
func TestGCWalk_TagChainLimits(t *testing.T) {
	f := newGCFixture(t)
	file := f.blob("tagged", f.old)
	tree := f.namedTree(treeEntry{"100644", "f", file})
	commit := f.commit(tree, "release", f.old)
	chain := func(n int) entities.Hash {
		h, typ := commit, "commit"
		for i := range n {
			h, typ = f.tag(h, typ, fmt.Sprintf("v%d-%d", n, i)), "tag"
		}
		return h
	}
	missing := entities.NewHash("0123456789abcdef0123456789abcdef01234567")
	treeTag := f.tag(tree, "tree", "tree-tag")
	tests := []struct {
		name     string
		revision entities.Hash
		key      entities.Hash
		typ      string
		sample   string
	}{
		{name: "10 nested tags", revision: chain(10), key: file, typ: gc.WalkBlob},
		{name: "11 nested tags", revision: chain(11), key: commit, sample: "more than 10 nested tags"},
		{name: "tag of a tree", revision: treeTag, key: tree, typ: gc.WalkTagTarget, sample: "tag-target " + tree.String() + " at p0 of revision " + treeTag.String() + ": unexpected object *gitobjects.Tree"},
		{name: "tree pinned", revision: tree, key: tree, typ: gc.WalkRevision, sample: "revision " + tree.String() + " at p0 of revision " + tree.String() + ": unexpected object *gitobjects.Tree"},
		{name: "missing revision", revision: missing, key: missing, typ: gc.WalkRevision, sample: "revision " + missing.String()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			details, _ := f.mountDetails([]string{tt.revision.String()})
			got, err := f.s.WalkGC(context.Background(), &gc.WalkOpts{MountDetails: []entities.Hash{details}, Keys: []entities.Hash{tt.key}, Workers: 1})
			require.NoError(err)
			if tt.sample == "" {
				require.Equal(int64(0), got.Incomplete, got.IncompleteSamples)
			} else {
				require.Equal(int64(1), got.Incomplete, got.IncompleteSamples)
				require.Contains(got.IncompleteSamples[0], tt.sample)
			}
			want := gc.WalkKey{Key: tt.key}
			if tt.typ != "" {
				want = gc.WalkKey{Key: tt.key, Reached: true, Type: tt.typ, MountDetails: details, Commit: tt.revision, Path: "p0"}
				if tt.typ == gc.WalkBlob {
					want.Path = "p0/f"
				}
			}
			require.Equal([]gc.WalkKey{want}, got.Keys)
		})
	}
}

// tag stores an annotated tag of target, an object of type typ.
func (f *gcFixture) tag(target entities.Hash, typ, name string) entities.Hash {
	body := fmt.Appendf(nil, "object %s\ntype %s\ntag %s\ntagger A <a@example.com> 0 +0000\n\n%s\n", target, typ, name, name)
	data := append(fmt.Appendf(nil, "tag %d\x00", len(body)), body...)
	return f.put(entities.Hash(sha1.Sum(data)), data, f.old)
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
