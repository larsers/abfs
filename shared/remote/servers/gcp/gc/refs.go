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
	"log"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"

	"github.com/google/abfs/binding"
	"github.com/google/abfs/manifest"
	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/objects"
	"github.com/google/abfs/shared/remote"
)

// RefClass is the retention class of a ref.
type RefClass string

// The ref classes. Refs under refs/abfs/ count only in the project
// abfs-meta, where mounts, builds and workspaces write them; elsewhere they
// are source refs.
const (
	RefSource        RefClass = "source"
	RefProjectStatus RefClass = "project-status"
	RefPushedPacks   RefClass = "pushed-packs"
	RefConfig        RefClass = "config"
	RefACL           RefClass = "acl"
	RefWorkspace     RefClass = "workspace"
	RefMount         RefClass = "mount"
	RefUnmount       RefClass = "unmount"
	RefBuild         RefClass = "build"
	RefBuildStart    RefClass = "build-start"
	RefOtherMeta     RefClass = "other-meta"
)

// metaRefClasses maps the prefixes of the refs that clients write under
// refs/abfs/ to their classes.
var metaRefClasses = []struct {
	prefix string
	class  RefClass
}{
	{"refs/abfs/ws/", RefWorkspace},
	{"refs/abfs/mount/", RefMount},
	{"refs/abfs/unmount/", RefUnmount},
	{"refs/abfs/build/", RefBuild},
	{"refs/abfs/build-start/", RefBuildStart},
}

const (
	configRef = "config"
	aclRef    = "acl"
)

func classifyRef(project, ref string) RefClass {
	switch ref {
	case binding.ProjectStatusRef:
		return RefProjectStatus
	case binding.PacksRefs:
		return RefPushedPacks
	case configRef:
		return RefConfig
	case aclRef:
		return RefACL
	}
	if project != entities.ABFSMetaProject || !strings.HasPrefix(ref, "refs/abfs/") {
		return RefSource
	}
	for _, c := range metaRefClasses {
		if strings.HasPrefix(ref, c.prefix) {
			return c.class
		}
	}
	return RefOtherMeta
}

// RefCounts counts the refs of one class in the snapshot.
type RefCounts struct {
	Refs int64 `json:"refs"`
	// Null counts the tombstones: rows whose target is NULL.
	Null int64 `json:"null"`
	// Roots counts the refs whose targets are roots.
	Roots int64 `json:"roots"`
	// Expired counts the refs that retention would tombstone.
	Expired int64 `json:"expired"`
	// Malformed counts targets that are not hashes.
	Malformed int64 `json:"malformed,omitempty"`
}

// refSnapshot holds the refs read at the snapshot time.
type refSnapshot struct {
	counts map[RefClass]*RefCounts
	roots  []refRoot
	// mounts holds the targets of the live mount refs.
	mounts []Hash
}

type refRoot struct {
	class  RefClass
	target Hash
}

// readRefs reads every ref at the snapshot time and sorts them by retention.
func readRefs(ctx context.Context, client *spanner.Client, o *DryRunOpts, snapshot time.Time) (*refSnapshot, error) {
	txn := client.ReadOnlyTransaction().WithTimestampBound(spanner.ReadTimestamp(snapshot))
	defer txn.Close()
	stmt := spanner.Statement{SQL: `SELECT p.Project, r.Ref, r.UpdateTime, r.Target FROM Refs AS r JOIN Projects AS p ON p.ProjectId = r.ProjectId`}
	snap := &refSnapshot{counts: make(map[RefClass]*RefCounts)}
	err := txn.QueryWithOptions(ctx, stmt, spanner.QueryOptions{Priority: spannerpb.RequestOptions_PRIORITY_LOW}).Do(func(row *spanner.Row) error {
		var project, ref string
		var updated time.Time
		var target spanner.NullString
		if err := row.Columns(&project, &ref, &updated, &target); err != nil {
			return err
		}
		class := classifyRef(project, ref)
		c := snap.counts[class]
		if c == nil {
			c = &RefCounts{}
			snap.counts[class] = c
		}
		c.Refs++
		if !target.Valid {
			c.Null++
			return nil
		}
		if o.refRetention(class, updated, snapshot) {
			c.Expired++
			return nil
		}
		if !entities.IsHash(target.StringVal) {
			c.Malformed++
			return nil
		}
		c.Roots++
		h := entities.NewHash(target.StringVal)
		snap.roots = append(snap.roots, refRoot{class: class, target: h})
		if class == RefMount {
			snap.mounts = append(snap.mounts, h)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to read the refs at %s: %w", snapshot.UTC().Format(time.RFC3339Nano), err)
	}
	return snap, nil
}

// refRetention tells whether retention would tombstone a ref of class c last
// written at updated. The target of every other ref is a root.
func (o *DryRunOpts) refRetention(c RefClass, updated, snapshot time.Time) bool {
	switch c {
	case RefMount:
		return !updated.After(snapshot.Add(-o.MountIdle))
	case RefBuild, RefBuildStart, RefUnmount:
		// An unmount ref keeps its unmount details and the mount details
		// they link, so that no ref names a missing object.
		return !updated.After(snapshot.Add(-o.BuildRetention))
	}
	return false
}

// MountCounts reports the pinned manifests of the live mounts.
type MountCounts struct {
	Live int64 `json:"live"`
	// WithoutPinnedManifest counts mount details that name no pinned manifest.
	WithoutPinnedManifest int64 `json:"without_pinned_manifest"`
	// PinnedManifests counts the distinct pinned manifests parsed.
	PinnedManifests int64 `json:"pinned_manifests"`
	// Revisions counts the distinct project revisions that are object keys.
	Revisions int64 `json:"revisions"`
	// IgnoredRevisions counts revisions that are not hashes, such as branch
	// names.
	IgnoredRevisions int64 `json:"ignored_revisions"`
	// Errors counts mount details or pinned manifests that could not be read
	// or parsed; their revisions are not roots.
	Errors int64 `json:"errors"`
}

// pinnedRevisions reads the mount details of each live mount and the pinned
// manifest it names, and returns the project revisions that are object keys:
// the commits the mounts read, which the mount details do not link.
func pinnedRevisions(ctx context.Context, objs remote.RemoteProvider, mounts []Hash) ([]Hash, MountCounts, error) {
	counts := MountCounts{Live: int64(len(mounts))}
	seenDetails := make(map[Hash]bool, len(mounts))
	seenManifests := make(map[Hash]bool)
	seenRevisions := make(map[Hash]bool)
	var revisions []Hash
	for _, h := range mounts {
		if seenDetails[h] {
			continue
		}
		seenDetails[h] = true
		pinned, err := pinnedManifestOf(ctx, objs, h)
		if err != nil {
			if ctx.Err() != nil {
				return nil, counts, ctx.Err()
			}
			log.Printf("Warning: failed to read the mount details %s of a live mount: %v", h, err)
			counts.Errors++
			continue
		}
		if pinned.IsZero() {
			counts.WithoutPinnedManifest++
			continue
		}
		if seenManifests[pinned] {
			continue
		}
		seenManifests[pinned] = true
		revs, ignored, err := manifestRevisions(ctx, objs, pinned)
		if err != nil {
			if ctx.Err() != nil {
				return nil, counts, ctx.Err()
			}
			log.Printf("Warning: failed to read the pinned manifest %s of a live mount: %v", pinned, err)
			counts.Errors++
			continue
		}
		counts.PinnedManifests++
		counts.IgnoredRevisions += int64(ignored)
		for _, r := range revs {
			if !seenRevisions[r] {
				seenRevisions[r] = true
				revisions = append(revisions, r)
			}
		}
	}
	counts.Revisions = int64(len(revisions))
	return revisions, counts, nil
}

func pinnedManifestOf(ctx context.Context, objs remote.RemoteProvider, h Hash) (Hash, error) {
	_, obj, err := remote.DecodeObject(ctx, objs, h)
	if err != nil {
		return Hash{}, err
	}
	md, ok := obj.(*binding.MountDetails)
	if !ok {
		return Hash{}, fmt.Errorf("unexpected object %T", obj)
	}
	return md.PinnedManifestHash, nil
}

// manifestRevisions returns the revisions of a manifest that are hashes and
// counts the others.
func manifestRevisions(ctx context.Context, objs remote.RemoteProvider, h Hash) (revs []Hash, ignored int, err error) {
	content, err := readBlob(ctx, objs, h)
	if err != nil {
		return nil, 0, err
	}
	m, err := manifest.Parse(content)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to parse the manifest: %w", err)
	}
	add := func(rev string) {
		if entities.IsHash(rev) {
			revs = append(revs, entities.NewHash(rev))
		} else {
			ignored++
		}
	}
	for _, p := range m.Project {
		add(m.ProjectRevision(p))
	}
	for _, p := range m.ExtendProject {
		if p.Revision != "" {
			add(p.Revision)
		}
	}
	if m.SuperProject != nil {
		add(m.ProjectRevision(&m.SuperProject.Project))
	}
	return revs, ignored, nil
}

func readBlob(ctx context.Context, objs remote.RemoteProvider, h Hash) (_ []byte, retErr error) {
	reader, err := objs.GetObject(ctx, h)
	if err != nil {
		return nil, err
	}
	defer entities.CloseErrCheck(reader, &retErr)
	raw, err := remote.DecodeToRaw(reader)
	if err != nil {
		return nil, err
	}
	defer entities.CloseErrCheck(raw, &retErr)
	return objects.ReadBlob(raw)
}
