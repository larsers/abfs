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

	"github.com/go-git/go-git/v5/plumbing/filemode"
	"golang.org/x/sync/errgroup"

	"github.com/google/abfs/binding"
	"github.com/google/abfs/manifest"
	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/objects/gitobjects"
	"github.com/google/abfs/shared/remote"
)

// DefaultWalkWorkers is the number of pinned revisions Walk walks in parallel.
const DefaultWalkWorkers = 16

// What a walk found a key as.
const (
	WalkMountDetails   = "mount-details"
	WalkManifest       = "manifest"
	WalkPinnedManifest = "pinned-manifest"
	WalkCommit         = "commit"
	WalkTree           = "tree"
	WalkBlob           = "blob"
	WalkSubmodule      = "submodule"
	// WalkDirHash prefixes the type of what the walk found below the tree a
	// pinned manifest names for a project in place of its commit's tree.
	WalkDirHash = "dir-hash-"
	// dirHashAnnotation is repo.ABFSDirHashTag: a pinned manifest of a mount
	// whose workspace differs from its commit names the workspace's tree in
	// this project annotation, and the mount uses that tree.
	dirHashAnnotation = "abfs.dir.hash.tag"
)

// WalkOpts are the options of Walk.
type WalkOpts struct {
	// MountDetails are the mount details whose pinned revisions are walked.
	MountDetails []Hash
	// Keys are the object keys to look for.
	Keys []Hash
	// Workers is the number of pinned revisions walked in parallel.
	Workers int
}

// WalkKey tells whether a key is in the closure of the pinned revisions, and
// how the walk reached it first.
type WalkKey struct {
	Key     Hash
	Reached bool
	// Type is what the walk found the key as, one of the Walk constants.
	Type string
	// MountDetails and Commit are the mount details and the pinned revision
	// whose walk reached the key first; Commit is zero for the mount details
	// and their manifests.
	MountDetails Hash
	Commit       Hash
	// Path is the project path and, below it, the path in the project's tree.
	Path string
}

// WalkResult is the result of Walk.
type WalkResult struct {
	// Keys has one entry per distinct key of WalkOpts.Keys, in their order.
	Keys            []WalkKey
	MountDetails    int64
	PinnedManifests int64
	Revisions       int64
	Trees           int64
	// Entries counts the file and submodule entries of the trees read. Their
	// objects are not read.
	Entries int64
	// Incomplete counts what the walk could not read or follow: mount details
	// without a pinned manifest, revisions that are not hashes, and objects
	// that are missing, cannot be read or have an unexpected type. With any,
	// a key outside the closure may still be in it. IncompleteSamples
	// describes up to maxSamples of them.
	Incomplete        int64
	IncompleteSamples []string
}

// walkRevision is a project revision of a pinned manifest, or the tree its
// annotation names in place of the commit's tree.
type walkRevision struct {
	details, commit Hash
	path            string
	dirHash         Hash
}

// walkHit is the first path of a revision's walk to a key.
type walkHit struct {
	typ, path string
}

// revisionWalk is the walk of one revision.
type revisionWalk struct {
	rev        *walkRevision
	hits       map[int]walkHit
	trees      int64
	entries    int64
	incomplete []string
}

type walker struct {
	objs remote.RemoteProvider
	want map[Hash]int
	res  *WalkResult
}

// Walk reads each mount details object and its pinned manifest, and walks
// the commit and the trees of every pinned revision from object content, as
// a client's checkout of the mount sees them. It reports, for each key,
// whether the walk reached it and the first path that did. It does not read
// the gc table, so a gap in the edges the collector extracted does not hide
// a key from it, and it reads trees only to list their entries, so a blob
// need not exist to be found. objs must neither rescue nor touch what it
// reads.
func Walk(ctx context.Context, objs remote.RemoteProvider, o *WalkOpts) (*WalkResult, error) {
	w := &walker{objs: objs, want: make(map[Hash]int, len(o.Keys)), res: &WalkResult{}}
	for _, k := range o.Keys {
		if _, ok := w.want[k]; !ok {
			w.want[k] = len(w.res.Keys)
			w.res.Keys = append(w.res.Keys, WalkKey{Key: k})
		}
	}
	revs, err := w.pinnedRevisions(ctx, o.MountDetails)
	if err != nil {
		return nil, err
	}
	walks := make([]revisionWalk, len(revs))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(max(1, o.Workers))
	for i := range revs {
		walks[i] = revisionWalk{rev: &revs[i], hits: make(map[int]walkHit)}
		g.Go(func() error {
			return w.walkCommit(gctx, &walks[i])
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	// Revisions are merged in manifest order, so the first path does not
	// depend on which walk finished first.
	for _, rw := range walks {
		w.res.Trees += rw.trees
		w.res.Entries += rw.entries
		for _, s := range rw.incomplete {
			w.incomplete(s)
		}
		for i, hit := range rw.hits {
			w.reach(i, hit.typ, rw.rev.details, rw.rev.commit, hit.path)
		}
	}
	return w.res, nil
}

// Err fails when the walk is incomplete: a key it did not reach may then be in
// the closure.
func (r *WalkResult) Err() error {
	if r.Incomplete > 0 {
		return fmt.Errorf("the walk is incomplete: %d mount details, revisions or objects could not be read or followed", r.Incomplete)
	}
	return nil
}

func (w *walker) incomplete(s string) {
	w.res.Incomplete++
	if len(w.res.IncompleteSamples) < maxSamples {
		w.res.IncompleteSamples = append(w.res.IncompleteSamples, s)
	}
}

// reach records the first way to the key with the index i.
func (w *walker) reach(i int, typ string, details, commit Hash, path string) {
	k := &w.res.Keys[i]
	if !k.Reached {
		*k = WalkKey{Key: k.Key, Reached: true, Type: typ, MountDetails: details, Commit: commit, Path: path}
	}
}

func (w *walker) reachKey(key Hash, typ string, details Hash) {
	if i, ok := w.want[key]; ok {
		w.reach(i, typ, details, Hash{}, "")
	}
}

// pinnedRevisions reads the mount details and their pinned manifests, as the
// pin reader of a cycle does, and returns each distinct revision once, with
// the project path it was found at first.
func (w *walker) pinnedRevisions(ctx context.Context, mounts []Hash) ([]walkRevision, error) {
	var revs []walkRevision
	seenDetails := make(map[Hash]bool, len(mounts))
	seenManifests := make(map[Hash]bool)
	seenRevs := make(map[Hash]bool)
	seenDirHashes := make(map[Hash]bool)
	for _, h := range mounts {
		if seenDetails[h] {
			continue
		}
		seenDetails[h] = true
		w.reachKey(h, WalkMountDetails, h)
		_, obj, err := remote.DecodeObject(ctx, w.objs, h)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			w.incomplete(fmt.Sprintf("mount details %s: %v", h, err))
			continue
		}
		md, ok := obj.(*binding.MountDetails)
		if !ok {
			w.incomplete(fmt.Sprintf("mount details %s: unexpected object %T", h, obj))
			continue
		}
		w.res.MountDetails++
		if !md.ManifestHash.IsZero() {
			w.reachKey(md.ManifestHash, WalkManifest, h)
		}
		pinned := md.PinnedManifestHash
		if pinned.IsZero() {
			w.incomplete(fmt.Sprintf("mount details %s name no pinned manifest", h))
			continue
		}
		w.reachKey(pinned, WalkPinnedManifest, h)
		if seenManifests[pinned] {
			continue
		}
		seenManifests[pinned] = true
		projects, err := manifestProjects(ctx, w.objs, pinned)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			w.incomplete(fmt.Sprintf("pinned manifest %s of mount details %s: %v", pinned, h, err))
			continue
		}
		w.res.PinnedManifests++
		for _, p := range projects {
			if !entities.IsHash(p.revision) {
				w.incomplete(fmt.Sprintf("pinned manifest %s: project %s has revision %q, which is not a hash", pinned, p.path, p.revision))
				continue
			}
			rev := entities.NewHash(p.revision)
			if !seenRevs[rev] {
				seenRevs[rev] = true
				revs = append(revs, walkRevision{details: h, commit: rev, path: p.path})
			}
			if p.dirHash == "" {
				continue
			}
			if !entities.IsHash(p.dirHash) {
				w.incomplete(fmt.Sprintf("pinned manifest %s: project %s has %s %q, which is not a hash", pinned, p.path, dirHashAnnotation, p.dirHash))
				continue
			}
			if dh := entities.NewHash(p.dirHash); !seenDirHashes[dh] {
				seenDirHashes[dh] = true
				revs = append(revs, walkRevision{details: h, commit: rev, path: p.path, dirHash: dh})
			}
		}
	}
	w.res.Revisions = int64(len(revs))
	return revs, nil
}

type manifestProject struct {
	path, revision, dirHash string
}

// manifestProjects returns the projects of a manifest with their revisions,
// in the order of manifestRevisions.
func manifestProjects(ctx context.Context, objs remote.RemoteProvider, h Hash) ([]manifestProject, error) {
	content, err := readBlob(ctx, objs, h)
	if err != nil {
		return nil, err
	}
	m, err := manifest.Parse(content)
	if err != nil {
		return nil, fmt.Errorf("failed to parse the manifest: %w", err)
	}
	var projects []manifestProject
	for _, p := range m.Project {
		mp := manifestProject{path: p.GetPath(), revision: m.ProjectRevision(p)}
		for _, a := range p.Annotation {
			if a.Name == dirHashAnnotation {
				mp.dirHash = a.Value
				break
			}
		}
		projects = append(projects, mp)
	}
	for _, p := range m.ExtendProject {
		if p.Revision != "" {
			path := p.Path
			if path == "" {
				path = p.Name
			}
			projects = append(projects, manifestProject{path: path, revision: p.Revision})
		}
	}
	if m.SuperProject != nil {
		projects = append(projects, manifestProject{path: m.SuperProject.GetPath(), revision: m.ProjectRevision(&m.SuperProject.Project)})
	}
	return projects, nil
}

func (rw *revisionWalk) hit(w *walker, key Hash, typ, path string) {
	if !rw.rev.dirHash.IsZero() {
		typ = WalkDirHash + typ
	}
	if i, ok := w.want[key]; ok {
		if _, found := rw.hits[i]; !found {
			rw.hits[i] = walkHit{typ: typ, path: path}
		}
	}
}

// failed records an object of the walk that could not be read. It returns
// the context's error once the walk is cancelled.
func (rw *revisionWalk) failed(ctx context.Context, what string, h Hash, path string, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	rw.incomplete = append(rw.incomplete, fmt.Sprintf("%s %s at %s of commit %s: %v", what, h, path, rw.rev.commit, err))
	return nil
}

func (w *walker) walkCommit(ctx context.Context, rw *revisionWalk) error {
	rev := rw.rev
	if !rev.dirHash.IsZero() {
		return w.walkTree(ctx, rw, rev.dirHash, rev.path, make(map[Hash]bool))
	}
	rw.hit(w, rev.commit, WalkCommit, rev.path)
	_, obj, err := remote.DecodeObject(ctx, w.objs, rev.commit)
	if err != nil {
		return rw.failed(ctx, "commit", rev.commit, rev.path, err)
	}
	c, ok := obj.(*gitobjects.Commit)
	if !ok {
		return rw.failed(ctx, "commit", rev.commit, rev.path, fmt.Errorf("unexpected object %T", obj))
	}
	// The first link of a commit is its tree; the parents are history, which
	// a checkout does not hold.
	return w.walkTree(ctx, rw, c.Links()[0], rev.path, make(map[Hash]bool))
}

func (w *walker) walkTree(ctx context.Context, rw *revisionWalk, h Hash, path string, seen map[Hash]bool) error {
	if seen[h] {
		return nil
	}
	seen[h] = true
	rw.hit(w, h, WalkTree, path)
	_, obj, err := remote.DecodeObject(ctx, w.objs, h)
	if err != nil {
		return rw.failed(ctx, "tree", h, path, err)
	}
	t, ok := obj.(*gitobjects.Tree)
	if !ok {
		return rw.failed(ctx, "tree", h, path, fmt.Errorf("unexpected object %T", obj))
	}
	rw.trees++
	for _, ent := range t.GitTree().Entries {
		p, eh := path+"/"+ent.Name, Hash(ent.Hash)
		switch ent.Mode {
		case filemode.Dir:
			if err := w.walkTree(ctx, rw, eh, p, seen); err != nil {
				return err
			}
		case filemode.Submodule:
			// A commit of another repository, which its own project pins.
			rw.entries++
			rw.hit(w, eh, WalkSubmodule, p)
		default:
			rw.entries++
			rw.hit(w, eh, WalkBlob, p)
		}
	}
	return nil
}
