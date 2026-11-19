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
	"github.com/google/abfs/repo"
	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/objects"
	"github.com/google/abfs/shared/remote"
)

// RefClass is the retention class of a ref.
type RefClass string

// Refs under refs/abfs/ have a class of their own only in abfs-meta.
const (
	RefSource        RefClass = "source"
	RefProjectStatus RefClass = "project-status"
	RefPushedPacks   RefClass = "pushed-packs"
	RefConfig        RefClass = "config"
	RefACL           RefClass = "acl"
	RefWorkspace     RefClass = "workspace"
	RefSnapshot      RefClass = "snapshot"
	RefMount         RefClass = "mount"
	RefUnmount       RefClass = "unmount"
	RefBuild         RefClass = "build"
	RefBuildStart    RefClass = "build-start"
	RefOtherMeta     RefClass = "other-meta"
)

var metaRefClasses = []struct {
	prefix string
	class  RefClass
}{
	{"refs/abfs/ws/", RefWorkspace},
	{"refs/abfs/snapshot/", RefSnapshot},
	{mountRefPrefix, RefMount},
	{unmountRefPrefix, RefUnmount},
	{"refs/abfs/build/", RefBuild},
	{"refs/abfs/build-start/", RefBuildStart},
}

const (
	configRef = "config"
	aclRef    = "acl"

	// A mount ref and its unmount ref share the name after the prefix.
	mountRefPrefix   = "refs/abfs/mount/"
	unmountRefPrefix = "refs/abfs/unmount/"
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
	Null      int64 `json:"null"`
	Roots     int64 `json:"roots"`
	Expired   int64 `json:"expired"`
	Malformed int64 `json:"malformed,omitempty"`
	// Ended counts mount refs with an unmount ref; they pin nothing.
	Ended int64 `json:"ended,omitempty"`
}

type refSnapshot struct {
	counts     map[RefClass]*RefCounts
	roots      []refRoot
	mounts     []Hash
	workspaces []Hash
	snapshots  []Hash
	expired    []expiredRef
}

type refRoot struct {
	class        RefClass
	target       Hash
	project, ref string
	updated      time.Time
}

type refRow struct {
	projectID          int64
	host, project, ref string
	updated            time.Time
	target             spanner.NullString
}

type mountKey struct {
	projectID int64
	name      string
}

// readRefs reads every ref at the snapshot time and sorts them by retention.
// A mount ref with an unmount ref is ended: it pins nothing and goes with the
// unmount ref.
func readRefs(ctx context.Context, client *spanner.Client, o *DryRunOpts, snapshot time.Time) (*refSnapshot, error) {
	txn := client.ReadOnlyTransaction().WithTimestampBound(spanner.ReadTimestamp(snapshot))
	defer txn.Close()
	stmt := spanner.Statement{SQL: `SELECT r.ProjectId, p.Host, p.Project, r.Ref, r.UpdateTime, r.Target FROM Refs AS r JOIN Projects AS p ON p.ProjectId = r.ProjectId`}
	snap := &refSnapshot{counts: make(map[RefClass]*RefCounts)}
	var mounts []refRow
	unmounted := make(map[mountKey]time.Time)
	err := txn.QueryWithOptions(ctx, stmt, spanner.QueryOptions{Priority: spannerpb.RequestOptions_PRIORITY_LOW}).Do(func(row *spanner.Row) error {
		var r refRow
		if err := row.Columns(&r.projectID, &r.host, &r.project, &r.ref, &r.updated, &r.target); err != nil {
			return err
		}
		class := classifyRef(r.project, r.ref)
		switch class {
		case RefMount:
			mounts = append(mounts, r)
			return nil
		case RefUnmount:
			if r.target.Valid {
				unmounted[mountKey{r.projectID, strings.TrimPrefix(r.ref, unmountRefPrefix)}] = r.updated
			}
		}
		snap.add(class, &r, o.refRetention(class, r.updated, snapshot), false)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to read the refs at %s: %w", snapshot.UTC().Format(time.RFC3339Nano), err)
	}
	// Expired mount refs go first, so a removal that stops halfway leaves no
	// mount ref live again.
	others := snap.expired
	snap.expired = nil
	for i := range mounts {
		r := &mounts[i]
		expired := o.refRetention(RefMount, r.updated, snapshot)
		unmountedAt, ended := unmounted[mountKey{r.projectID, strings.TrimPrefix(r.ref, mountRefPrefix)}]
		if ended && o.refRetention(RefUnmount, unmountedAt, snapshot) {
			expired = true
		}
		snap.add(RefMount, r, expired, ended)
	}
	snap.expired = append(snap.expired, others...)
	return snap, nil
}

func (s *refSnapshot) add(class RefClass, r *refRow, expired, ended bool) {
	c := s.counts[class]
	if c == nil {
		c = &RefCounts{}
		s.counts[class] = c
	}
	c.Refs++
	if !r.target.Valid {
		c.Null++
		return
	}
	if expired {
		c.Expired++
		s.expired = append(s.expired, expiredRef{class: class, projectID: r.projectID, host: r.host, project: r.project, ref: r.ref, updated: r.updated, target: r.target.StringVal})
		return
	}
	if !entities.IsHash(r.target.StringVal) {
		c.Malformed++
		return
	}
	c.Roots++
	h := entities.NewHash(r.target.StringVal)
	s.roots = append(s.roots, refRoot{class: class, target: h, project: r.project, ref: r.ref, updated: r.updated})
	switch {
	case class == RefMount && ended:
		c.Ended++
	case class == RefMount:
		s.mounts = append(s.mounts, h)
	case class == RefWorkspace:
		s.workspaces = append(s.workspaces, h)
	case class == RefSnapshot:
		s.snapshots = append(s.snapshots, h)
	}
}

// refRetention tells whether retention would tombstone a ref of class c last
// written at updated.
func (o *DryRunOpts) refRetention(c RefClass, updated, snapshot time.Time) bool {
	switch c {
	case RefMount:
		return !updated.After(snapshot.Add(-o.MountIdle))
	case RefBuild, RefBuildStart, RefUnmount:
		// An unmount ref keeps the mount details it links, so no ref names a
		// missing object.
		return !updated.After(snapshot.Add(-o.BuildRetention))
	case RefSnapshot:
		return !updated.After(snapshot.Add(-o.SnapshotRetention))
	}
	return false
}

// MountCounts reports the pinned manifests of the live mounts, or of the
// mounts that the workspace snapshots were taken from.
type MountCounts struct {
	Live                  int64 `json:"live"`
	WithoutPinnedManifest int64 `json:"without_pinned_manifest"`
	WithoutKeepAlive      int64 `json:"without_keepalive"`
	// WithoutLocalWriteCheck counts mounts whose cacheman may evict the only
	// copy of locally written content before a server confirms it.
	WithoutLocalWriteCheck int64 `json:"without_local_write_check"`
	// TouchIntervalTooLong counts mounts whose touch interval may let a
	// touched object stop being recent before the next touch.
	TouchIntervalTooLong int64                    `json:"touch_interval_too_long"`
	PinnedManifests      int64                    `json:"pinned_manifests"`
	Revisions            int64                    `json:"revisions"`
	IgnoredRevisions     int64                    `json:"ignored_revisions"`
	DanglingRevisions    int64                    `json:"dangling_revisions"`
	NotFound             int64                    `json:"not_found"`
	Errors               int64                    `json:"errors"`
	Samples              map[string][]MountSample `json:"samples,omitempty"`
	longestTouch         time.Duration
}

// MountSample names the mount details, and the workspace snapshot, behind a
// count of MountCounts.
type MountSample struct {
	Details   string `json:"details,omitempty"`
	Workspace string `json:"workspace,omitempty"`
	Host      string `json:"host,omitempty"`
	User      string `json:"user,omitempty"`
}

const maxSamples = 20

func (c *MountCounts) sample(count string, src *pinSource) {
	if c.Samples == nil {
		c.Samples = make(map[string][]MountSample)
	}
	if len(c.Samples[count]) < maxSamples {
		c.Samples[count] = append(c.Samples[count], src.sample())
	}
}

// pinSource is the mount details that pinned a revision first.
type pinSource struct {
	details, workspace Hash
	host, user         string
	snapshotRef        bool
}

func (p *pinSource) sample() MountSample {
	var s MountSample
	if !p.details.IsZero() {
		s.Details = p.details.String()
	}
	if !p.workspace.IsZero() {
		s.Workspace = p.workspace.String()
	}
	s.Host, s.User = p.host, p.user
	return s
}

type pinnedRevision struct {
	hash Hash
	src  *pinSource
}

// pinReader reads the pinned manifests of mounts and workspaces.
type pinReader struct {
	objs      remote.RemoteProvider
	details   map[Hash]*binding.MountDetails
	manifests map[Hash][]Hash
	ignored   map[Hash]int
	seen      map[Hash]bool
	revisions []pinnedRevision

	touchLimit time.Duration
}

type pinCounts struct {
	mounts, workspaces, snapshots MountCounts
}

func (c *pinCounts) counts(src *pinSource) *MountCounts {
	switch {
	case src.workspace.IsZero():
		return &c.mounts
	case src.snapshotRef:
		return &c.snapshots
	}
	return &c.workspaces
}

// pinnedRevisions returns the project revisions that the pinned manifests of
// live mounts and workspace snapshots name, which no object links.
func pinnedRevisions(ctx context.Context, objs remote.RemoteProvider, refs *refSnapshot, touchLimit time.Duration) ([]pinnedRevision, pinCounts, error) {
	p := &pinReader{
		objs:      objs,
		details:   make(map[Hash]*binding.MountDetails),
		manifests: make(map[Hash][]Hash),
		ignored:   make(map[Hash]int),
		seen:      make(map[Hash]bool),

		touchLimit: touchLimit,
	}
	var counts pinCounts
	counts.mounts.Live = int64(len(refs.mounts))
	seenDetails := make(map[Hash]bool, len(refs.mounts))
	seenManifests := make(map[Hash]bool)
	for _, h := range refs.mounts {
		if err := p.mount(ctx, &counts.mounts, seenDetails, seenManifests, &pinSource{details: h}); err != nil {
			return nil, counts, err
		}
	}
	if err := p.snapshots(ctx, &counts.workspaces, refs.workspaces, false); err != nil {
		return nil, counts, err
	}
	if err := p.snapshots(ctx, &counts.snapshots, refs.snapshots, true); err != nil {
		return nil, counts, err
	}
	for _, r := range p.revisions {
		counts.counts(r.src).Revisions++
	}
	return p.revisions, counts, nil
}

func (p *pinReader) snapshots(ctx context.Context, c *MountCounts, snapshots []Hash, snapshotRef bool) error {
	c.Live = int64(len(snapshots))
	seenDetails := make(map[Hash]bool, len(snapshots))
	seenManifests := make(map[Hash]bool)
	seenSnapshots := make(map[Hash]bool, len(snapshots))
	for _, h := range snapshots {
		if seenSnapshots[h] {
			continue
		}
		seenSnapshots[h] = true
		src := &pinSource{workspace: h, snapshotRef: snapshotRef}
		details, err := wsMountDetails(ctx, p.objs, h)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			p.failed(c, src, "workspace snapshot", err)
			continue
		}
		if details.IsZero() {
			c.WithoutPinnedManifest++
			c.sample("without_pinned_manifest", src)
			continue
		}
		src.details = details
		if err := p.mount(ctx, c, seenDetails, seenManifests, src); err != nil {
			return err
		}
	}
	return nil
}

func (p *pinReader) failed(c *MountCounts, src *pinSource, what string, err error) {
	if entities.IsNotExist(err) {
		c.NotFound++
		c.sample("not_found", src)
		return
	}
	log.Printf("Warning: failed to read the %s of %s: %v", what, src.describe(), err)
	c.Errors++
	c.sample("errors", src)
}

func (p *pinSource) describe() string {
	if p.workspace.IsZero() {
		return "the live mount with details " + p.details.String()
	}
	return "the workspace snapshot " + p.workspace.String()
}

func (p *pinReader) mount(ctx context.Context, c *MountCounts, seenDetails, seenManifests map[Hash]bool, src *pinSource) error {
	if seenDetails[src.details] {
		return nil
	}
	seenDetails[src.details] = true
	md, err := p.mountDetails(ctx, src.details)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		p.failed(c, src, "mount details", err)
		return nil
	}
	src.host, src.user = md.HostName, md.UserName
	if cfg := md.Config; cfg == nil || !cfg.GCKeepAlive || cfg.GCKeepAliveRefInterval <= 0 || cfg.GCKeepAliveTouchInterval <= 0 {
		c.WithoutKeepAlive++
		c.sample("without_keepalive", src)
	} else {
		if !md.GCLocalWriteCheck {
			c.WithoutLocalWriteCheck++
			c.sample("without_local_write_check", src)
		}
		c.longestTouch = max(c.longestTouch, cfg.GCKeepAliveTouchInterval)
		if !keepAliveTouchFits(cfg.GCKeepAliveTouchInterval, p.touchLimit) {
			c.TouchIntervalTooLong++
			c.sample("touch_interval_too_long", src)
		}
	}
	pinned := md.PinnedManifestHash
	if pinned.IsZero() {
		c.WithoutPinnedManifest++
		c.sample("without_pinned_manifest", src)
		return nil
	}
	if seenManifests[pinned] {
		return nil
	}
	seenManifests[pinned] = true
	revs, ignored, err := p.manifest(ctx, pinned)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		p.failed(c, src, "pinned manifest "+pinned.String(), err)
		return nil
	}
	c.PinnedManifests++
	c.IgnoredRevisions += int64(ignored)
	if ignored > 0 {
		c.sample("ignored_revisions", src)
	}
	for _, r := range revs {
		if !p.seen[r] {
			p.seen[r] = true
			p.revisions = append(p.revisions, pinnedRevision{hash: r, src: src})
		}
	}
	return nil
}

// keepAliveTouchFits allows the keep-alive to run a tenth late.
func keepAliveTouchFits(interval, limit time.Duration) bool {
	return interval+interval/10 < limit
}

func (p *pinReader) mountDetails(ctx context.Context, h Hash) (*binding.MountDetails, error) {
	if md, ok := p.details[h]; ok {
		return md, nil
	}
	_, obj, err := remote.DecodeObject(ctx, p.objs, h)
	if err != nil {
		return nil, err
	}
	md, ok := obj.(*binding.MountDetails)
	if !ok {
		return nil, fmt.Errorf("unexpected object %T", obj)
	}
	p.details[h] = md
	return md, nil
}

func (p *pinReader) manifest(ctx context.Context, h Hash) ([]Hash, int, error) {
	if revs, ok := p.manifests[h]; ok {
		return revs, p.ignored[h], nil
	}
	revs, ignored, err := manifestRevisions(ctx, p.objs, h)
	if err != nil {
		return nil, 0, err
	}
	p.manifests[h], p.ignored[h] = revs, ignored
	return revs, ignored, nil
}

func wsMountDetails(ctx context.Context, objs remote.RemoteProvider, h Hash) (Hash, error) {
	_, obj, err := remote.DecodeObject(ctx, objs, h)
	if err != nil {
		return Hash{}, err
	}
	ws, ok := obj.(*binding.WSSnapshot)
	if !ok {
		return Hash{}, fmt.Errorf("unexpected object %T", obj)
	}
	return ws.MountDetails, nil
}

func manifestRevisions(ctx context.Context, objs remote.RemoteProvider, h Hash) (revs []Hash, ignored int, err error) {
	projects, err := manifestProjects(ctx, objs, h)
	if err != nil {
		return nil, 0, err
	}
	add := func(rev string) {
		if entities.IsHash(rev) {
			revs = append(revs, entities.NewHash(rev))
		} else {
			ignored++
		}
	}
	for _, p := range projects {
		add(p.revision)
		if p.dirHash != "" {
			add(p.dirHash)
		}
	}
	return revs, ignored, nil
}

type manifestProject struct {
	path, revision, dirHash string
}

// manifestProjects also returns the workspace tree that a mount uses in place
// of its commit's tree.
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
			if a.Name == repo.ABFSDirHashTag {
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
