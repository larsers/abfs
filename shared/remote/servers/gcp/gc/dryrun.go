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
	"time"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/spanner"
	"golang.org/x/sync/errgroup"

	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/remote"
)

const (
	DefaultGrace          = 72 * time.Hour
	DefaultMountIdle      = 336 * time.Hour
	DefaultBuildRetention = 720 * time.Hour
	DefaultMapRetention   = 720 * time.Hour
	DefaultCycle          = 24 * time.Hour
	DefaultCacheTTL       = 6 * time.Hour
	DefaultLongestPush    = 3 * time.Hour
	DefaultScanShards     = 16
)

// DryRunOpts sets a dry run of the garbage collector.
type DryRunOpts struct {
	// Grace keeps every object written or touched within it before the
	// snapshot, and everything such an object reaches.
	Grace time.Duration
	// MountIdle is how long a mount ref stays a root after its last write.
	MountIdle time.Duration
	// BuildRetention is how long build and build-start refs stay roots, and
	// unmount refs are kept, after their last write.
	BuildRetention time.Duration
	// MapRetention is how long an eval map row stays a root after its last
	// write or use.
	MapRetention time.Duration
	// Cycle, CacheTTL and LongestPush are the cycle period, the TTL of the
	// servers' header and content caches, and the longest push; Grace must
	// cover two cycles, one TTL and one push.
	Cycle       time.Duration
	CacheTTL    time.Duration
	LongestPush time.Duration
	// Consistency checks that every object has a gc row, which reads the
	// meta family of every object row.
	Consistency bool
	// Shards is the number of key ranges scanned in parallel.
	Shards int
	// LeaseDuration is the lease the run holds and renews.
	LeaseDuration time.Duration
	// Holder names this process in the lease; empty uses DefaultHolder.
	Holder string
	// StateName is the GcState row; empty uses DefaultStateName.
	StateName string
}

// DefaultDryRunOpts returns the default options.
func DefaultDryRunOpts() DryRunOpts {
	return DryRunOpts{
		Grace:          DefaultGrace,
		MountIdle:      DefaultMountIdle,
		BuildRetention: DefaultBuildRetention,
		MapRetention:   DefaultMapRetention,
		Cycle:          DefaultCycle,
		CacheTTL:       DefaultCacheTTL,
		LongestPush:    DefaultLongestPush,
		Consistency:    true,
		Shards:         DefaultScanShards,
		LeaseDuration:  DefaultLeaseDuration,
	}
}

// Validate checks the timing rule: a touch is written at most once per half
// grace, so the grace must exceed two cycles, plus one cache TTL for the
// servers to see a condemn marker and one push for uploads whose root comes
// last.
func (o *DryRunOpts) Validate() error {
	for _, d := range []struct {
		name  string
		value time.Duration
	}{
		{"grace", o.Grace}, {"mount idle time", o.MountIdle}, {"build retention", o.BuildRetention},
		{"map retention", o.MapRetention}, {"cycle", o.Cycle}, {"lease duration", o.LeaseDuration},
	} {
		if d.value <= 0 {
			return fmt.Errorf("invalid %s %s: it must be positive", d.name, d.value)
		}
	}
	if o.CacheTTL < 0 || o.LongestPush < 0 {
		return fmt.Errorf("invalid cache TTL %s or longest push %s: they cannot be negative", o.CacheTTL, o.LongestPush)
	}
	if need := 2*o.Cycle + o.CacheTTL + o.LongestPush; o.Grace < need {
		return fmt.Errorf("grace %s is shorter than 2 x cycle (%s) + cache TTL (%s) + longest push (%s) = %s", o.Grace, o.Cycle, o.CacheTTL, o.LongestPush, need)
	}
	if o.Shards <= 0 {
		return fmt.Errorf("invalid number of shards %d", o.Shards)
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
	// RefsToTombstone counts the refs that retention would tombstone.
	RefsToTombstone int64       `json:"refs_to_tombstone"`
	LiveMounts      MountCounts `json:"live_mounts"`
	// Roots counts the roots per class; a root without a gc row is dangling.
	Roots       map[string]*RootCounts  `json:"roots"`
	Objects     ObjectCounts            `json:"objects"`
	Types       map[string]*TypeReport  `json:"types"`
	Edges       EdgeCounts              `json:"edges"`
	Maps        map[MapClass]*MapCounts `json:"maps"`
	Consistency ConsistencyReport       `json:"consistency"`
	Memory      MemoryReport            `json:"memory"`
	Seconds     Timings                 `json:"seconds"`
}

// Settings records the options of the run.
type Settings struct {
	GraceHours          float64 `json:"grace_hours"`
	MountIdleHours      float64 `json:"mount_idle_hours"`
	BuildRetentionHours float64 `json:"build_retention_hours"`
	MapRetentionHours   float64 `json:"map_retention_hours"`
	CycleHours          float64 `json:"cycle_hours"`
	CacheTTLHours       float64 `json:"cache_ttl_hours"`
	LongestPushHours    float64 `json:"longest_push_hours"`
	Shards              int     `json:"shards"`
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
	// OutdatedRows and NewerRows count rows written by an older or a newer
	// edge extractor; a backfill rewrites the older ones.
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
	Total       float64 `json:"total"`
}

// Result holds the report and the mark of a dry run.
type Result struct {
	Report *Report
	graph  *graph
	live   bitmap
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
// GcState row. objs reads the mount details and pinned manifests of live
// mounts.
func DryRun(ctx context.Context, sp *spanner.Client, bt *bigtable.Client, objs remote.RemoteProvider, opts *DryRunOpts) (*Result, error) {
	o := *opts
	if err := o.Validate(); err != nil {
		return nil, err
	}
	if o.Holder == "" {
		o.Holder = DefaultHolder()
	}
	if o.StateName == "" {
		o.StateName = DefaultStateName
	}
	start := time.Now()
	lease, err := AcquireLease(ctx, sp, o.StateName, o.Holder, PhaseDryRun, o.LeaseDuration, o.Grace)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	runCtx, stop := lease.Keep(ctx)
	defer stop()

	d := &dryRun{opts: &o, lease: lease, sp: sp, bt: bt, objs: objs, start: start}
	res, err := d.run(runCtx)
	if err != nil && runCtx.Err() != nil && ctx.Err() == nil {
		return nil, fmt.Errorf("the GC dry run stopped: %w", context.Cause(runCtx))
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
}

func (d *dryRun) run(ctx context.Context) (*Result, error) {
	o := d.opts
	snapshot := d.lease.Start
	r := &Report{
		Cycle:        d.lease.Cycle,
		Phase:        PhaseDryRun,
		Holder:       o.Holder,
		StartTime:    d.start,
		SnapshotTime: snapshot,
		Settings: Settings{
			GraceHours:          o.Grace.Hours(),
			MountIdleHours:      o.MountIdle.Hours(),
			BuildRetentionHours: o.BuildRetention.Hours(),
			MapRetentionHours:   o.MapRetention.Hours(),
			CycleHours:          o.Cycle.Hours(),
			CacheTTLHours:       o.CacheTTL.Hours(),
			LongestPushHours:    o.LongestPush.Hours(),
			Shards:              o.Shards,
		},
		Roots: make(map[string]*RootCounts),
		Types: make(map[string]*TypeReport),
	}
	d.report = r
	if err := d.lease.RecordSnapshot(ctx, snapshot); err != nil {
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
	revisions, mounts, err := pinnedRevisions(ctx, d.objs, refs.mounts)
	if err != nil {
		return nil, err
	}
	r.LiveMounts = mounts
	r.Seconds.Mounts = time.Since(step).Seconds()

	step = time.Now()
	bounds, err := splitRange("", "", o.Shards)
	if err != nil {
		return nil, err
	}
	scanStart := time.Now()
	gcTable := d.bt.Open(TableName)
	g, nodes, err := scanNodes(ctx, gcTable, bounds, snapshot.Add(-o.Grace))
	if err != nil {
		return nil, err
	}
	d.sampleMemory()
	var mapRows []mapRow
	var malformedMaps int64
	eg, ectx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		var err error
		r.Edges, d.perType, err = scanEdges(ectx, gcTable, bounds, g)
		return err
	})
	eg.Go(func() error {
		var err error
		mapRows, malformedMaps, err = scanMaps(ectx, d.bt.Open(mapsTableName), bounds, g)
		return err
	})
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	r.Seconds.Scan = time.Since(step).Seconds()
	d.sampleMemory()

	step = time.Now()
	m := newMarker(g, len(g.keys))
	for _, root := range refs.roots {
		d.addRoot(m, g, string(root.class), root.target)
	}
	for _, h := range revisions {
		d.addRoot(m, g, RootPinnedRevisions, h)
	}
	recent := d.root(RootRecent)
	for id := range uint32(len(g.keys)) {
		if g.recent.has(id) {
			m.add(id)
			recent.Roots++
		}
	}
	m.walk()
	r.Maps = m.markMaps(g, mapRows, snapshot.Add(-o.MapRetention).UnixMicro())
	for class, mc := range r.Maps {
		d.root(rootMapPrefix + string(class)).Roots = mc.Live
	}
	if malformedMaps > 0 {
		r.Maps[MapMalformed] = &MapCounts{Rows: malformedMaps}
	}
	r.Seconds.Mark = time.Since(step).Seconds()
	d.sampleMemory()

	r.Objects = ObjectCounts{
		Rows:         int64(len(g.keys)),
		Reachable:    m.live.count(),
		Recent:       recent.Roots,
		TouchOnly:    nodes.TouchOnly,
		OutdatedRows: nodes.Outdated,
		NewerRows:    nodes.Newer,
	}
	r.Objects.Unreachable = r.Objects.Rows - r.Objects.Reachable
	d.countTypes(g, m.live)

	if o.Consistency {
		step = time.Now()
		cr, sizes, err := checkConsistency(ctx, d.bt.Open(objectsTable), bounds, g, m.live, scanStart.Add(-clockSkewMargin))
		if err != nil {
			return nil, err
		}
		r.Consistency = *cr
		r.Objects.ReachableBytes = &sizes.reachable
		r.Objects.UnreachableBytes = &sizes.unreachable
		r.Seconds.Consistency = time.Since(step).Seconds()
	}
	d.sampleMemory()
	r.Memory.KeysBytes = int64(len(g.keys)) * entities.HashSize
	for _, s := range g.segs {
		r.Memory.EdgesBytes += int64(len(s.edges))*4 + int64(len(s.offsets))*8
	}
	r.Memory.MapRowsBytes = int64(len(mapRows)) * 16
	r.Seconds.Total = time.Since(d.start).Seconds()
	logSummary(r)
	return &Result{Report: r, graph: g, live: m.live}, nil
}

func (d *dryRun) root(class string) *RootCounts {
	c := d.report.Roots[class]
	if c == nil {
		c = &RootCounts{}
		d.report.Roots[class] = c
	}
	return c
}

func (d *dryRun) addRoot(m *marker, g *graph, class string, h Hash) {
	c := d.root(class)
	id := g.find(h)
	if id == noNode {
		c.Dangling++
		return
	}
	c.Roots++
	m.add(id)
}

// TypeReport counts the objects and edges of one type.
type TypeReport struct {
	Objects     int64 `json:"objects"`
	Reachable   int64 `json:"reachable"`
	Unreachable int64 `json:"unreachable"`
	Edges       int64 `json:"edges"`
}

func (d *dryRun) countTypes(g *graph, live bitmap) {
	counts := make([]TypeReport, len(typeNames))
	for id, t := range g.types {
		c := &counts[t]
		c.Objects++
		if live.has(uint32(id)) {
			c.Reachable++
		} else {
			c.Unreachable++
		}
	}
	for t, n := range d.perType {
		counts[t].Edges = n
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
	log.Printf("GC dry run of cycle %d: %d objects, %d reachable, %d unreachable, %d dangling edges, %d dangling roots, %d refs and %d map rows to expire, missing gc rows %s, peak heap %d MiB, %.0f s",
		r.Cycle, r.Objects.Rows, r.Objects.Reachable, r.Objects.Unreachable, r.Edges.Dangling, danglingRoots,
		r.RefsToTombstone, mapsExpired, missing, r.Memory.PeakHeapBytes>>20, r.Seconds.Total)
}
