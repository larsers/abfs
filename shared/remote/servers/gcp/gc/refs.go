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
	// Expired counts the refs past their retention, which a delete cycle
	// removes.
	Expired int64 `json:"expired"`
	// Malformed counts targets that are not hashes.
	Malformed int64 `json:"malformed,omitempty"`
}

// refSnapshot holds the refs read at the snapshot time.
type refSnapshot struct {
	counts map[RefClass]*RefCounts
	roots  []refRoot
	// mounts and workspaces hold the targets of the live mount refs and of
	// the workspace refs.
	mounts     []Hash
	workspaces []Hash
	// expired holds the refs past their retention, which a delete cycle
	// removes.
	expired []expiredRef
}

type refRoot struct {
	class        RefClass
	target       Hash
	project, ref string
	updated      time.Time
}

// readRefs reads every ref at the snapshot time and sorts them by retention.
func readRefs(ctx context.Context, client *spanner.Client, o *DryRunOpts, snapshot time.Time) (*refSnapshot, error) {
	txn := client.ReadOnlyTransaction().WithTimestampBound(spanner.ReadTimestamp(snapshot))
	defer txn.Close()
	stmt := spanner.Statement{SQL: `SELECT r.ProjectId, p.Host, p.Project, r.Ref, r.UpdateTime, r.Target FROM Refs AS r JOIN Projects AS p ON p.ProjectId = r.ProjectId`}
	snap := &refSnapshot{counts: make(map[RefClass]*RefCounts)}
	err := txn.QueryWithOptions(ctx, stmt, spanner.QueryOptions{Priority: spannerpb.RequestOptions_PRIORITY_LOW}).Do(func(row *spanner.Row) error {
		var projectID int64
		var host, project, ref string
		var updated time.Time
		var target spanner.NullString
		if err := row.Columns(&projectID, &host, &project, &ref, &updated, &target); err != nil {
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
			snap.expired = append(snap.expired, expiredRef{class: class, projectID: projectID, host: host, project: project, ref: ref, updated: updated, target: target.StringVal})
			return nil
		}
		if !entities.IsHash(target.StringVal) {
			c.Malformed++
			return nil
		}
		c.Roots++
		h := entities.NewHash(target.StringVal)
		snap.roots = append(snap.roots, refRoot{class: class, target: h, project: project, ref: ref, updated: updated})
		switch class {
		case RefMount:
			snap.mounts = append(snap.mounts, h)
		case RefWorkspace:
			snap.workspaces = append(snap.workspaces, h)
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

// MountCounts reports the pinned manifests of the live mounts, or of the
// mounts that the workspace snapshots were taken from.
type MountCounts struct {
	// Live counts the live mount refs, or the workspace refs.
	Live int64 `json:"live"`
	// WithoutPinnedManifest counts mount details that name no pinned manifest,
	// and workspace snapshots that name no mount details.
	WithoutPinnedManifest int64 `json:"without_pinned_manifest"`
	// WithoutKeepAlive counts mount details of clients that ran without the GC
	// keep-alive or with one of its intervals zero.
	WithoutKeepAlive int64 `json:"without_keepalive"`
	// WithoutLocalWriteCheck counts mount details with the keep-alive whose
	// cacheman does not keep locally written content until a server confirms
	// it, so that it may evict the only copy of an object a delete removes.
	WithoutLocalWriteCheck int64 `json:"without_local_write_check"`
	// TouchIntervalTooLong counts mount details with the keep-alive whose
	// touch interval, plus the tenth the keep-alive may be late, is not below
	// the grace less the longest server touch interval and the clock skew
	// margin: an object the mount touched may stop being recent before the
	// next touch.
	TouchIntervalTooLong int64 `json:"touch_interval_too_long"`
	// PinnedManifests counts the distinct pinned manifests parsed.
	PinnedManifests int64 `json:"pinned_manifests"`
	// Revisions counts the distinct project revisions that are object keys,
	// each counted where it was found first.
	Revisions int64 `json:"revisions"`
	// IgnoredRevisions counts revisions that are not hashes, such as branch
	// names.
	IgnoredRevisions int64 `json:"ignored_revisions"`
	// DanglingRevisions counts the revisions counted here that have no gc row.
	DanglingRevisions int64 `json:"dangling_revisions"`
	// NotFound counts workspace snapshots, mount details or pinned manifests
	// that do not exist, so they pin nothing.
	NotFound int64 `json:"not_found"`
	// Errors counts the ones that could not be read or parsed; their
	// revisions are not roots.
	Errors int64 `json:"errors"`
	// Samples lists up to maxSamples mount details for each count above, by
	// the count's name.
	Samples map[string][]MountSample `json:"samples,omitempty"`
	// longestTouch is the longest touch interval of the mount details with
	// the keep-alive, which a delete checks again against the servers that
	// run just before it.
	longestTouch time.Duration
}

// MountSample names the mount details, and the workspace snapshot, behind a
// count of MountCounts.
type MountSample struct {
	Details   string `json:"details,omitempty"`
	Workspace string `json:"workspace,omitempty"`
	Host      string `json:"host,omitempty"`
	User      string `json:"user,omitempty"`
}

// maxSamples is the number of examples the report keeps per count.
const maxSamples = 20

func (c *MountCounts) sample(count string, src *pinSource) {
	if c.Samples == nil {
		c.Samples = make(map[string][]MountSample)
	}
	if len(c.Samples[count]) < maxSamples {
		c.Samples[count] = append(c.Samples[count], src.sample())
	}
}

// pinSource is the mount details that pinned a revision first, and the
// workspace snapshot that named them.
type pinSource struct {
	details, workspace Hash
	host, user         string
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

// pinnedRevision is a project revision of a pinned manifest that is an object
// key.
type pinnedRevision struct {
	hash Hash
	src  *pinSource
}

// pinReader reads the pinned manifests of mounts and workspaces. Each object
// is read once, and each revision is kept once, with the first source.
type pinReader struct {
	objs      remote.RemoteProvider
	details   map[Hash]*binding.MountDetails
	manifests map[Hash][]Hash
	ignored   map[Hash]int
	seen      map[Hash]bool
	revisions []pinnedRevision

	// touchLimit is the bound of TouchIntervalTooLong.
	touchLimit time.Duration
}

// pinnedRevisions reads the mount details of each live mount and of the mount
// behind each workspace snapshot, and the pinned manifests they name. It
// returns the project revisions that are object keys: the commits the mounts
// and workspaces are based on, which neither links. A mount with the
// keep-alive whose touch interval is not safely below touchLimit is counted
// in TouchIntervalTooLong.
func pinnedRevisions(ctx context.Context, objs remote.RemoteProvider, mounts, workspaces []Hash, touchLimit time.Duration) ([]pinnedRevision, MountCounts, MountCounts, error) {
	p := &pinReader{
		objs:      objs,
		details:   make(map[Hash]*binding.MountDetails),
		manifests: make(map[Hash][]Hash),
		ignored:   make(map[Hash]int),
		seen:      make(map[Hash]bool),

		touchLimit: touchLimit,
	}
	mountCounts := MountCounts{Live: int64(len(mounts))}
	seenDetails := make(map[Hash]bool, len(mounts))
	seenManifests := make(map[Hash]bool)
	for _, h := range mounts {
		if err := p.mount(ctx, &mountCounts, seenDetails, seenManifests, &pinSource{details: h}); err != nil {
			return nil, mountCounts, MountCounts{}, err
		}
	}
	wsCounts := MountCounts{Live: int64(len(workspaces))}
	seenDetails = make(map[Hash]bool, len(workspaces))
	seenManifests = make(map[Hash]bool)
	seenSnapshots := make(map[Hash]bool, len(workspaces))
	for _, h := range workspaces {
		if seenSnapshots[h] {
			continue
		}
		seenSnapshots[h] = true
		src := &pinSource{workspace: h}
		details, err := wsMountDetails(ctx, objs, h)
		if err != nil {
			if ctx.Err() != nil {
				return nil, mountCounts, wsCounts, ctx.Err()
			}
			p.failed(&wsCounts, src, "workspace snapshot", err)
			continue
		}
		if details.IsZero() {
			wsCounts.WithoutPinnedManifest++
			wsCounts.sample("without_pinned_manifest", src)
			continue
		}
		src.details = details
		if err := p.mount(ctx, &wsCounts, seenDetails, seenManifests, src); err != nil {
			return nil, mountCounts, wsCounts, err
		}
	}
	for _, r := range p.revisions {
		if r.src.workspace.IsZero() {
			mountCounts.Revisions++
		} else {
			wsCounts.Revisions++
		}
	}
	return p.revisions, mountCounts, wsCounts, nil
}

// failed counts an object of src that could not be read: a missing one in
// NotFound, any other failure in Errors.
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

// mount reads the mount details of src and its pinned manifest.
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

// keepAliveTouchFits reports whether the keep-alive of a mount that touches
// every interval, up to a tenth late, touches its objects again before the
// last touch stops keeping them recent, which it does for limit.
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

// wsMountDetails returns the mount details that the workspace snapshot h was
// taken from.
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
