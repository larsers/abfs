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
	"github.com/google/abfs/repo"
	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/objects/gitobjects"
	"github.com/google/abfs/shared/remote"
)

// DefaultWalkWorkers is the number of pinned revisions Walk walks in parallel.
const DefaultWalkWorkers = 16

const maxTagChain = 10

// What a walk found a key as.
const (
	WalkMountDetails   = "mount-details"
	WalkManifest       = "manifest"
	WalkPinnedManifest = "pinned-manifest"
	WalkCommit         = "commit"
	WalkTag            = "tag"
	WalkTree           = "tree"
	WalkBlob           = "blob"
	WalkSubmodule      = "submodule"
	// WalkRevision and WalkTagTarget could not be read or are no commit or tag.
	WalkRevision  = "revision"
	WalkTagTarget = "tag-target"
	// WalkDirHash prefixes what the walk found below a pinned dir-hash tree.
	WalkDirHash = "dir-hash-"
)

// WalkOpts are the options of Walk.
type WalkOpts struct {
	MountDetails []Hash
	// Keys are the object keys to look for.
	Keys []Hash
	// Workers of zero uses DefaultWalkWorkers.
	Workers int
}

// WalkKey tells whether a key is in the closure and how the walk reached it first.
type WalkKey struct {
	Key     Hash
	Reached bool
	// Type is one of the Walk constants.
	Type string
	// Commit is zero for the mount details and their manifests.
	MountDetails Hash
	Commit       Hash
	// Path is the project path and the path in its tree.
	Path string
}

// WalkResult is the result of Walk.
type WalkResult struct {
	Keys            []WalkKey
	MountDetails    int64
	PinnedManifests int64
	Revisions       int64
	Trees           int64
	// Entries counts tree entries; their objects are not read.
	Entries int64
	// With any Incomplete, a key outside the closure may still be in it.
	Incomplete        int64
	IncompleteSamples []string
}

type walkRevision struct {
	details, commit Hash
	path            string
	dirHash         Hash
}

type walkHit struct {
	typ, path string
}

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

// Walk walks every pinned revision of the mount details from object content and
// reports whether it reached each key. It does not read the abfsgc table, so a
// gap in the extracted edges does not hide a key. objs must neither rescue nor
// touch what it reads.
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
	workers := o.Workers
	if workers <= 0 {
		workers = DefaultWalkWorkers
	}
	g.SetLimit(workers)
	for i := range revs {
		walks[i] = revisionWalk{rev: &revs[i], hits: make(map[int]walkHit)}
		g.Go(func() error {
			return w.walkCommit(gctx, &walks[i])
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	// Merge in manifest order so the first path does not depend on timing.
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

// Err fails when the walk is incomplete.
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
				w.incomplete(fmt.Sprintf("pinned manifest %s: project %s has %s %q, which is not a hash", pinned, p.path, repo.ABFSDirHashTag, p.dirHash))
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

func (rw *revisionWalk) failed(ctx context.Context, what string, h Hash, path string, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	rw.incomplete = append(rw.incomplete, fmt.Sprintf("%s %s at %s of revision %s: %v", what, h, path, rw.rev.commit, err))
	return nil
}

func (w *walker) walkCommit(ctx context.Context, rw *revisionWalk) error {
	rev := rw.rev
	if !rev.dirHash.IsZero() {
		return w.walkTree(ctx, rw, rev.dirHash, rev.path, make(map[Hash]bool))
	}
	// A pinned revision can be an annotated tag; the checkout holds its commit.
	h, what := rev.commit, WalkRevision
	for range maxTagChain + 1 {
		_, obj, err := remote.DecodeObject(ctx, w.objs, h)
		if err != nil {
			rw.hit(w, h, what, rev.path)
			return rw.failed(ctx, what, h, rev.path, err)
		}
		switch o := obj.(type) {
		case *gitobjects.Commit:
			rw.hit(w, h, WalkCommit, rev.path)
			// The parents are history, which a checkout does not hold.
			return w.walkTree(ctx, rw, o.Links()[0], rev.path, make(map[Hash]bool))
		case *gitobjects.Tag:
			rw.hit(w, h, WalkTag, rev.path)
			h, what = o.Links()[0], WalkTagTarget
		default:
			rw.hit(w, h, what, rev.path)
			return rw.failed(ctx, what, h, rev.path, fmt.Errorf("unexpected object %T", obj))
		}
	}
	return rw.failed(ctx, "tag", rev.commit, rev.path, fmt.Errorf("more than %d nested tags", maxTagChain))
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
