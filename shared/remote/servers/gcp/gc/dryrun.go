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
	"runtime"
	"slices"
	"time"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/spanner"
	"cloud.google.com/go/storage"
	"golang.org/x/sync/errgroup"

	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/instance"
	"github.com/google/abfs/shared/remote"
)

const (
	DefaultGrace          = 72 * time.Hour
	DefaultMountIdle      = 336 * time.Hour
	DefaultBuildRetention = 720 * time.Hour
	DefaultMapRetention   = 720 * time.Hour
	DefaultLongestPush    = 3 * time.Hour
	DefaultScanShards     = 16

	// DefaultSnapshotRetention keeps a workspace snapshot as long as a build.
	DefaultSnapshotRetention = 720 * time.Hour

	// minMountIdle is the shortest mount idle time: four times the interval
	// at which clients with the GC keep-alive rewrite their mount ref, so
	// that a live mount does not lose its ref between two rewrites.
	minMountIdle = 4 * instance.GCKeepAliveRefInterval
)

// DryRunOpts sets a dry run of the garbage collector.
type DryRunOpts struct {
	// Grace keeps every object written or touched within it before the
	// snapshot, and everything such an object reaches.
	Grace time.Duration
	// MountIdle is how long a mount ref stays a root after its last write.
	// An ended mount ref, one with an unmount ref, keeps only its mount
	// details and goes earlier when its unmount ref passes BuildRetention.
	MountIdle time.Duration
	// BuildRetention is how long build, build-start and unmount refs stay
	// roots after their last write. An ended mount ref goes with its unmount
	// ref.
	BuildRetention time.Duration
	// SnapshotRetention is how long a snapshot ref stays a root after its
	// last write.
	SnapshotRetention time.Duration
	// MapRetention is how long an eval map row stays a root after its last
	// write or use.
	MapRetention time.Duration
	// LongestPush is the longest push or build upload. Grace must cover it
	// and the largest cache TTL of the servers (see graceProblem).
	LongestPush time.Duration
	// Consistency checks that every object has a gc row, which reads the
	// meta family of every object row. The command always sets it; tests
	// turn it off.
	Consistency bool
	// Shards is the number of key ranges scanned in parallel.
	Shards int
	// LeaseDuration is the lease the run holds and renews.
	LeaseDuration time.Duration
	// WriteDeadline is the deadline of one attempt of a Bigtable write call
	// of the run; a streamed Cloud Storage object gets it plus its size at
	// 8 MiB/s. Two attempts must fit into a quarter of the lease, the margin
	// at which the run stops.
	WriteDeadline time.Duration
	// Holder names this process in the lease; empty uses DefaultHolder.
	Holder string
	// StateName is the GcState row; empty uses DefaultStateName.
	StateName string
	// Explain lists keys whose state and first root the report explains.
	Explain []Hash
}

// DefaultDryRunOpts returns the default options.
func DefaultDryRunOpts() DryRunOpts {
	return DryRunOpts{
		Grace:             DefaultGrace,
		MountIdle:         DefaultMountIdle,
		BuildRetention:    DefaultBuildRetention,
		SnapshotRetention: DefaultSnapshotRetention,
		MapRetention:      DefaultMapRetention,
		LongestPush:       DefaultLongestPush,
		Consistency:       true,
		Shards:            DefaultScanShards,
		LeaseDuration:     DefaultLeaseDuration,
		WriteDeadline:     DefaultWriteDeadline,
	}
}

// Validate checks the options. The grace must be longer than the longest
// push; the cycle checks it against the cache TTL of the servers too (see
// graceProblem).
func (o *DryRunOpts) Validate() error {
	for _, d := range []struct {
		name  string
		value time.Duration
	}{
		{"grace", o.Grace}, {"mount idle time", o.MountIdle}, {"build retention", o.BuildRetention},
		{"snapshot retention", o.SnapshotRetention},
		{"map retention", o.MapRetention}, {"lease duration", o.LeaseDuration},
		{"write deadline", o.WriteDeadline},
	} {
		if d.value <= 0 {
			return fmt.Errorf("invalid %s %s: it must be positive", d.name, d.value)
		}
	}
	if o.LongestPush < 0 {
		return fmt.Errorf("invalid longest push %s: it cannot be negative", o.LongestPush)
	}
	if o.Grace <= o.LongestPush {
		return fmt.Errorf("grace %s is not longer than the longest push %s", o.Grace, o.LongestPush)
	}
	if o.MountIdle < minMountIdle {
		return fmt.Errorf("mount idle time %s is shorter than %s, four times the interval at which clients rewrite their mount ref", o.MountIdle, minMountIdle)
	}
	if o.Shards <= 0 {
		return fmt.Errorf("invalid number of shards %d", o.Shards)
	}
	if 2*o.WriteDeadline > o.LeaseDuration/4 {
		return fmt.Errorf("write deadline %s is too long for the lease %s: two attempts must fit into a quarter of it", o.WriteDeadline, o.LeaseDuration)
	}
	return nil
}

// Report is the result of a dry run.
type Report struct {
	Cycle        int64     `json:"cycle"`
	Phase        string    `json:"phase"`
	Holder       string    `json:"holder"`
	StartTime    time.Time `json:"start_time"`
	SnapshotTime time.Time `json:"snapshot_time"`
	Settings     Settings  `json:"settings"`
	// Refs counts the refs per class at the snapshot.
	Refs map[RefClass]*RefCounts `json:"refs"`
	// RefsToTombstone counts the refs past their retention.
	RefsToTombstone int64       `json:"refs_to_tombstone"`
	LiveMounts      MountCounts `json:"live_mounts"`
	// Workspaces reports the pinned manifests of the mounts that the
	// workspace snapshots were taken from, and Snapshots those of the
	// snapshots that snapshot refs name.
	Workspaces MountCounts `json:"workspaces"`
	Snapshots  MountCounts `json:"snapshots"`
	// Replicas reports the server replicas and their settings.
	Replicas ReplicaReport `json:"replicas"`
	// AppProfile reports the Bigtable app profile of the command, which the
	// command checks before the cycle.
	AppProfile *ProfileReport `json:"app_profile,omitempty"`
	// Roots counts the roots per class; a root without a gc row is dangling.
	Roots       map[string]*RootCounts  `json:"roots"`
	Objects     ObjectCounts            `json:"objects"`
	Types       map[string]*TypeReport  `json:"types"`
	Edges       EdgeCounts              `json:"edges"`
	Maps        map[MapClass]*MapCounts `json:"maps"`
	Consistency ConsistencyReport       `json:"consistency"`
	// Condemn reports the condemn step of a condemn cycle, and PreviousList
	// what it found of the keys of the list before.
	Condemn      *CondemnReport      `json:"condemn,omitempty"`
	PreviousList *PreviousListReport `json:"previous_list,omitempty"`
	// Delete reports the delete step of a delete cycle, and in the other
	// modes what one would delete from the list of the cycle before.
	Delete *DeleteReport `json:"delete,omitempty"`
	// Explain explains the keys of DryRunOpts.Explain.
	Explain []KeyReport  `json:"explain,omitempty"`
	Memory  MemoryReport `json:"memory"`
	Seconds Timings      `json:"seconds"`
}

// Settings records the options of the run.
type Settings struct {
	GraceHours             float64 `json:"grace_hours"`
	MountIdleHours         float64 `json:"mount_idle_hours"`
	BuildRetentionHours    float64 `json:"build_retention_hours"`
	SnapshotRetentionHours float64 `json:"snapshot_retention_hours"`
	MapRetentionHours      float64 `json:"map_retention_hours"`
	// CacheTTLHours is the largest cache TTL of the servers that may have
	// run within the grace, which the grace check used; zero when no server
	// recorded one.
	CacheTTLHours    float64 `json:"cache_ttl_hours"`
	LongestPushHours float64 `json:"longest_push_hours"`
	Shards           int     `json:"shards"`
}

// RootCounts counts the roots of one class.
type RootCounts struct {
	Roots    int64 `json:"roots"`
	Dangling int64 `json:"dangling"`
}

// The root classes besides the ref classes.
const (
	RootPinnedRevisions = "pinned-revisions"
	RootRecent          = "recent"
	rootMapPrefix       = "map-"
	// RootPermanent holds the empty file and the empty directory. Every server
	// writes them at start and link validation needs them, but clients do not
	// push them, so nothing may remove them.
	RootPermanent = "permanent"
)

// ObjectCounts counts the objects of the table gc.
type ObjectCounts struct {
	// Rows counts the gc rows, one per object.
	Rows        int64 `json:"rows"`
	Reachable   int64 `json:"reachable"`
	Unreachable int64 `json:"unreachable"`
	// Recent counts objects written or touched within the grace.
	Recent int64 `json:"recent"`
	// TouchOnly counts rows with a touch and no edges, for objects that have
	// no gc row yet.
	TouchOnly int64 `json:"touch_only"`
	// OutdatedRows counts rows written by an older edge extractor than their
	// type requires, which a backfill rewrites; NewerRows counts rows of a
	// newer one.
	OutdatedRows int64 `json:"outdated_rows"`
	NewerRows    int64 `json:"newer_rows"`
	// ReachableBytes and UnreachableBytes sum the object sizes (meta:size),
	// read by the consistency pass; without it they are not known.
	ReachableBytes   *int64 `json:"reachable_bytes,omitempty"`
	UnreachableBytes *int64 `json:"unreachable_bytes,omitempty"`
}

// MemoryReport reports the memory of the run.
type MemoryReport struct {
	// PeakHeapBytes is the largest heap seen between the steps.
	PeakHeapBytes uint64 `json:"peak_heap_bytes"`
	// PeakSysBytes is the largest memory obtained from the system.
	PeakSysBytes uint64 `json:"peak_sys_bytes"`
	KeysBytes    int64  `json:"keys_bytes"`
	EdgesBytes   int64  `json:"edges_bytes"`
	MapRowsBytes int64  `json:"map_rows_bytes"`
}

// Timings records the seconds each step took.
type Timings struct {
	Refs        float64 `json:"refs"`
	Mounts      float64 `json:"mounts"`
	Scan        float64 `json:"scan"`
	Mark        float64 `json:"mark"`
	Consistency float64 `json:"consistency"`
	Condemn     float64 `json:"condemn,omitempty"`
	Total       float64 `json:"total"`
}

// Result holds the report and the mark of a dry run.
type Result struct {
	Report *Report
	graph  *graph
	live   bitmap
	// first holds the first root of each tracked node, resolved by roots.
	first map[uint32]rootRef
	roots *rootTable
	// unreachable lists, in key order, the unreachable nodes that the
	// consistency scan found an objects row for: the ones a condemn marks.
	// tooNew is the check, in a delete cycle, of the objects that scan found
	// without a gc row because they were written during it, and tooNewErr why
	// it failed.
	unreachable []sizedNode
	tooNew      *TooNewReport
	tooNewErr   error
	// seen holds the nodes that the consistency scan found an objects row
	// for.
	seen bitmap
	// servers is the server evidence read at the start of the cycle.
	servers *serverEvidence
	// bounds are the key ranges of the scans, and maps the rows of the table
	// maps.
	bounds []string
	maps   *mapScan
	// plan is the plan of a delete from the list of the cycle before, nil
	// when there is none.
	plan *deletePlan
	// expiredRefs are the refs past their retention at the snapshot.
	expiredRefs []expiredRef
}

// firstRoot returns the root whose walk first reached the tracked node id, or
// nil when id is not tracked or not reachable.
func (r *Result) firstRoot(id uint32) *Root {
	ref, ok := r.first[id]
	if !ok {
		return nil
	}
	return r.roots.describe(ref)
}

// Reachable reports whether the mark found h reachable, and whether h has a gc
// row at all.
func (r *Result) Reachable(h Hash) (reachable, found bool) {
	id := r.graph.find(h)
	if id == noNode {
		return false, false
	}
	return r.live.has(id), true
}

// DryRun runs one garbage collection cycle that only reports: it takes the
// lease, reads the roots at one snapshot, marks the objects of the table gc
// and counts what a collection would remove. It writes nothing but the
// GcState row and deletes the GcReplicas rows not seen for a day. objs reads
// the workspace snapshots, mount details and pinned manifests of live mounts
// and workspaces. With bucket, which holds the condemned lists, the report
// also counts what a delete cycle would remove from the last list, or says
// in Report.Delete why it could not read it.
func DryRun(ctx context.Context, sp *spanner.Client, bt *bigtable.Client, bucket *storage.BucketHandle, objs remote.RemoteProvider, opts *DryRunOpts) (*Result, error) {
	o := *opts
	if err := o.Validate(); err != nil {
		return nil, err
	}
	return runCycle(ctx, sp, bt, bucket, objs, &o, nil)
}

// runCycle runs one cycle: a dry run when c is nil, else a condemn cycle
// whose steps c runs on the mark, under the same lease, and a delete cycle
// when c has a deleter.
func runCycle(ctx context.Context, sp *spanner.Client, bt *bigtable.Client, bucket *storage.BucketHandle, objs remote.RemoteProvider, o *DryRunOpts, c *condemner) (*Result, error) {
	if o.Holder == "" {
		o.Holder = DefaultHolder()
	}
	if o.StateName == "" {
		o.StateName = DefaultStateName
	}
	phase := PhaseDryRun
	allows := func(string) bool { return true }
	switch {
	case c != nil && c.del != nil:
		phase = PhaseDelete
		allows = c.del.opts.allows
	case c != nil:
		phase = PhaseCondemn
	}
	start := time.Now()
	lease, err := AcquireLease(ctx, sp, o.StateName, o.Holder, phase, o.LeaseDuration, o.Grace)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	runCtx, stop := lease.Keep(ctx)
	defer stop()

	d := &dryRun{opts: o, lease: lease, sp: sp, bt: bt, objs: objs, start: start, phase: phase, keepUnreachable: c != nil}
	var res *Result
	var prev previousList
	var planErr error
	switch {
	case c != nil:
		err = c.readPrevious(runCtx, lease, d)
		prev = c.prev
	case bucket != nil:
		// A dry run reads the list only for the counts of what a delete
		// would remove, so it reports a failed read and runs on.
		if prev, planErr = readPreviousList(runCtx, lease, bucket); planErr != nil {
			if runCtx.Err() != nil {
				err = planErr
			} else {
				log.Printf("Warning: the GC dry run does not count what a delete would remove: %v", planErr)
			}
		}
		d.keepUnreachable = prev.list != nil
	}
	if err == nil {
		res, err = d.run(runCtx)
	}
	if err == nil && prev.list != nil && res.seen != nil {
		res.plan = planDeletes(res, prev.list, res.Report.SnapshotTime.Add(-o.MapRetention).UnixMicro(), allows)
		res.plan.report.List = prev.state.Location
		res.Report.Delete = &DeleteReport{Plan: res.plan.report}
	}
	if err == nil && planErr != nil {
		res.Report.Delete = &DeleteReport{PlanError: planErr.Error()}
	}
	if err == nil && c != nil {
		err = c.run(runCtx, lease, res)
	}
	if err != nil && runCtx.Err() != nil && ctx.Err() == nil {
		return nil, fmt.Errorf("the GC %s cycle stopped: %w", phase, context.Cause(runCtx))
	}
	return res, err
}

type dryRun struct {
	opts   *DryRunOpts
	lease  *Lease
	sp     *spanner.Client
	bt     *bigtable.Client
	objs   remote.RemoteProvider
	start  time.Time
	report *Report
	// perType counts the edges per type index.
	perType []int64
	// outdated counts the outdated rows per type index.
	outdated []int64
	// tracked lists keys whose first root the mark records, besides those of
	// the Explain option.
	tracked []Hash
	phase   string
	// keepUnreachable keeps the unreachable objects that have an objects row,
	// for a condemn.
	keepUnreachable bool
}

func (d *dryRun) run(ctx context.Context) (*Result, error) {
	o := d.opts
	snapshot := d.lease.Start
	r := &Report{
		Cycle:        d.lease.Cycle,
		Phase:        d.phase,
		Holder:       o.Holder,
		StartTime:    d.start,
		SnapshotTime: snapshot,
		Settings: Settings{
			GraceHours:             o.Grace.Hours(),
			MountIdleHours:         o.MountIdle.Hours(),
			BuildRetentionHours:    o.BuildRetention.Hours(),
			SnapshotRetentionHours: o.SnapshotRetention.Hours(),
			MapRetentionHours:      o.MapRetention.Hours(),
			LongestPushHours:       o.LongestPush.Hours(),
			Shards:                 o.Shards,
		},
		Roots: make(map[string]*RootCounts),
		Types: make(map[string]*TypeReport),
	}
	d.report = r
	if err := d.lease.RecordSnapshot(ctx, snapshot); err != nil {
		return nil, err
	}
	servers, err := readServerEvidence(ctx, d.sp, o.StateName)
	if err != nil {
		return nil, err
	}
	fromServers, stopped := serverReplicas(servers.servers)
	r.Replicas = checkReplicas(slices.Concat(servers.replicas, fromServers), snapshot, o)
	r.Replicas.Stopped = stopped
	start := windowStart(snapshot, o.Grace)
	longest, _ := servers.graceTTL(start)
	r.Settings.CacheTTLHours = longest.CacheTTL.Hours()
	if p := servers.graceProblem(start, o); p != "" {
		r.Replicas.Warnings = append(r.Replicas.Warnings, p)
	}
	if !servers.legacyGone.IsZero() {
		r.Replicas.LegacyServersGoneTime = &servers.legacyGone
	}
	if loss, lost := servers.mapRowLoss(snapshot, o.Grace); lost {
		r.Replicas.MapRowLossTime = &loss.at
	}
	for _, w := range r.Replicas.Warnings {
		log.Printf("Warning: %s", w)
	}
	if r.Replicas.Removed, err = DeleteStaleReplicas(ctx, d.sp, snapshot.Add(-staleReplicaAge)); err != nil {
		return nil, err
	}
	// A row of GcServers is evidence for every delete whose window holds it,
	// so it stays for the largest grace of any condemn or delete cycle, and
	// for the time before the window in which a map row loss still counts.
	keep := max(staleReplicaAge, max(d.lease.MaxGrace, o.Grace)+time.Hour+clientMapReuse+keepAlivePassAllowance)
	if r.Replicas.ServersRemoved, err = PruneServers(ctx, d.sp, snapshot.Add(-keep)); err != nil {
		return nil, err
	}

	step := time.Now()
	refs, err := readRefs(ctx, d.sp, o, snapshot)
	if err != nil {
		return nil, err
	}
	r.Refs = refs.counts
	for _, c := range refs.counts {
		r.RefsToTombstone += c.Expired
	}
	r.Seconds.Refs = time.Since(step).Seconds()
	d.sampleMemory()

	step = time.Now()
	revisions, pins, err := pinnedRevisions(ctx, d.objs, refs, servers.keepAliveTouchLimit(snapshot, o.Grace))
	if err != nil {
		return nil, err
	}
	r.Seconds.Mounts = time.Since(step).Seconds()

	step = time.Now()
	bounds, err := splitRange("", "", o.Shards)
	if err != nil {
		return nil, err
	}
	bounds = prefixBounds(bounds)
	scanStart := time.Now()
	gcTable := d.bt.Open(TableName)
	g, nodes, err := scanNodes(ctx, gcTable, bounds, snapshot.Add(-o.Grace))
	if err != nil {
		return nil, err
	}
	d.outdated = nodes.OutdatedTypes
	d.sampleMemory()
	var maps *mapScan
	eg, ectx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		var err error
		r.Edges, d.perType, err = scanEdges(ectx, gcTable, bounds, g)
		return err
	})
	eg.Go(func() error {
		var err error
		maps, err = scanMaps(ectx, d.bt.Open(mapsTableName), bounds, g)
		return err
	})
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	r.Seconds.Scan = time.Since(step).Seconds()
	d.sampleMemory()

	step = time.Now()
	// The roots are walked one at a time in a fixed order of classes, so that
	// a tracked node is attributed to the first root, in that order, that
	// reaches it.
	m := newMarker(g, g.size())
	for _, keys := range [][]Hash{d.tracked, o.Explain} {
		ids := make([]uint32, len(keys))
		for i, h := range keys {
			ids[i] = g.find(h)
		}
		m.track(ids)
	}
	sortRefRoots(refs.roots)
	pinnedAt := slices.IndexFunc(refs.roots, func(r refRoot) bool { return refClassRank(r.class) > refClassRank(pinnedAfter) })
	if pinnedAt < 0 {
		pinnedAt = len(refs.roots)
	}
	for i, root := range refs.roots[:pinnedAt] {
		d.addRoot(m, g, string(root.class), root.target, rootRef{kind: rootOfRef, i: i})
	}
	for i, rev := range revisions {
		if d.addRoot(m, g, RootPinnedRevisions, rev.hash, rootRef{kind: rootPinned, i: i}) {
			continue
		}
		c := pins.counts(rev.src)
		c.DanglingRevisions++
		c.sample("dangling_revisions", rev.src)
	}
	r.LiveMounts, r.Workspaces, r.Snapshots = pins.mounts, pins.workspaces, pins.snapshots
	for i, root := range refs.roots[pinnedAt:] {
		d.addRoot(m, g, string(root.class), root.target, rootRef{kind: rootOfRef, i: pinnedAt + i})
	}
	for _, h := range []Hash{entities.EmptyFileHash, entities.EmptyDirHash} {
		d.addRoot(m, g, RootPermanent, h, rootRef{kind: rootPermanentObject, i: int(g.find(h))})
	}
	recent := d.root(RootRecent)
	for id := range uint32(g.size()) {
		if g.recent.has(id) {
			m.addRoot(id, rootRef{kind: rootRecentObject, i: int(id)})
			recent.Roots++
		}
	}
	r.Maps = m.markMaps(g, maps.rows, snapshot.Add(-o.MapRetention).UnixMicro())
	for class, mc := range r.Maps {
		d.root(rootMapPrefix + string(class)).Roots = mc.Live
	}
	if maps.malformed > 0 {
		r.Maps[MapMalformed] = &MapCounts{Rows: maps.malformed}
	}
	r.Seconds.Mark = time.Since(step).Seconds()
	d.sampleMemory()

	r.Objects = ObjectCounts{
		Rows:         int64(g.size()),
		Reachable:    m.live.count(),
		Recent:       recent.Roots,
		TouchOnly:    nodes.TouchOnly,
		OutdatedRows: nodes.Outdated,
		NewerRows:    nodes.Newer,
	}
	r.Objects.Unreachable = r.Objects.Rows - r.Objects.Reachable
	d.countTypes(g, m.live)

	res := &Result{Report: r, graph: g, live: m.live, first: m.first, roots: &rootTable{g: g, refs: refs.roots, revisions: revisions}, servers: servers, bounds: bounds, maps: maps, expiredRefs: refs.expired}
	if o.Consistency {
		step = time.Now()
		var check *tooNewCheck
		if d.phase == PhaseDelete {
			check = newTooNewCheck(d.bt, writeBudget{deadline: o.WriteDeadline})
		}
		scan, err := checkConsistency(ctx, d.bt.Open(objectsTable), bounds, g, m.live, scanStart.Add(-clockSkewMargin), d.keepUnreachable, check)
		if err != nil {
			return nil, err
		}
		r.Consistency = *scan.report
		r.Objects.ReachableBytes = &scan.sizes.reachable
		r.Objects.UnreachableBytes = &scan.sizes.unreachable
		res.unreachable, res.seen = scan.unreachable, scan.seen
		res.tooNew, res.tooNewErr = scan.tooNew, scan.tooNewErr
		r.Seconds.Consistency = time.Since(step).Seconds()
	}
	if len(o.Explain) > 0 {
		if r.Explain, err = res.explainKeys(ctx, d.bt, o.Explain); err != nil {
			return nil, err
		}
	}
	d.sampleMemory()
	r.Memory.KeysBytes = g.keysBytes()
	for _, s := range g.segs {
		r.Memory.EdgesBytes += s.bytes()
	}
	for _, rows := range maps.rows {
		r.Memory.MapRowsBytes += int64(cap(rows)) * 16
	}
	r.Seconds.Total = time.Since(d.start).Seconds()
	logSummary(r)
	return res, nil
}

func (d *dryRun) root(class string) *RootCounts {
	c := d.report.Roots[class]
	if c == nil {
		c = &RootCounts{}
		d.report.Roots[class] = c
	}
	return c
}

// addRoot walks the root h of class, which ref names, and reports whether h
// has a gc row.
func (d *dryRun) addRoot(m *marker, g *graph, class string, h Hash, ref rootRef) bool {
	c := d.root(class)
	id := g.find(h)
	if id == noNode {
		c.Dangling++
		return false
	}
	c.Roots++
	m.addRoot(id, ref)
	return true
}

// TypeReport counts the objects and edges of one type.
type TypeReport struct {
	Objects     int64 `json:"objects"`
	Reachable   int64 `json:"reachable"`
	Unreachable int64 `json:"unreachable"`
	Edges       int64 `json:"edges"`
	// OutdatedRows counts the rows that a backfill rewrites.
	OutdatedRows int64 `json:"outdated_rows"`
	// UnreachableSample lists the first unreachable keys of the type.
	UnreachableSample []string `json:"unreachable_sample,omitempty"`
}

func (d *dryRun) countTypes(g *graph, live bitmap) {
	counts := make([]TypeReport, len(typeNames))
	for id, t := range g.types {
		c := &counts[t]
		c.Objects++
		if live.has(uint32(id)) {
			c.Reachable++
			continue
		}
		c.Unreachable++
		if len(c.UnreachableSample) < maxSamples {
			c.UnreachableSample = append(c.UnreachableSample, g.key(uint32(id)).String())
		}
	}
	for t, n := range d.perType {
		counts[t].Edges = n
	}
	for t, n := range d.outdated {
		counts[t].OutdatedRows = n
	}
	for t := range counts {
		if counts[t].Objects > 0 {
			d.report.Types[typeNames[t]] = &counts[t]
		}
	}
}

func (d *dryRun) sampleMemory() {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	d.report.Memory.PeakHeapBytes = max(d.report.Memory.PeakHeapBytes, ms.HeapAlloc)
	d.report.Memory.PeakSysBytes = max(d.report.Memory.PeakSysBytes, ms.Sys)
}

func logSummary(r *Report) {
	var mapsExpired int64
	for _, mc := range r.Maps {
		mapsExpired += mc.Expired
	}
	var danglingRoots int64
	for _, c := range r.Roots {
		danglingRoots += c.Dangling
	}
	missing := "not checked"
	if r.Consistency.Ran {
		missing = fmt.Sprint(r.Consistency.MissingGCRows)
	}
	log.Printf("GC %s cycle %d: %d objects, %d reachable, %d unreachable, %d dangling edges, %d dangling roots, %d refs and %d map rows to expire, missing gc rows %s, peak heap %d MiB, %.0f s",
		r.Phase, r.Cycle, r.Objects.Rows, r.Objects.Reachable, r.Objects.Unreachable, r.Edges.Dangling, danglingRoots,
		r.RefsToTombstone, mapsExpired, missing, r.Memory.PeakHeapBytes>>20, r.Seconds.Total)
}
