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

package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"maps"
	"os"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/pflag"

	"github.com/google/abfs/shared/cmd"
	"github.com/google/abfs/shared/cmd/args"
	"github.com/google/abfs/shared/cmd/clierr"
	"github.com/google/abfs/shared/cmd/root"
	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/metrics"
	"github.com/google/abfs/shared/remote/servers/gcp"
	"github.com/google/abfs/shared/remote/servers/gcp/gc"
)

const (
	defaultBackfillRate = 2000
	appProfileUsage     = "The GCP Bigtable App Profile ID (empty uses the instance default app profile). Give the GC commands their own profile with single-cluster routing, single-row transactions and priority low; the command checks it at the start and warns otherwise"
	clientMetricsUsage  = "Export the built-in metrics of the Spanner, Bigtable and Cloud Storage client libraries to Cloud Monitoring, which show per-attempt latencies and retries per app profile"
)

type gcpGCCmd struct {
	*root.RootCmd
}

func (c *gcpGCCmd) Spec() cmd.CmdSpec {
	return cmd.CmdSpec{
		Name:  "gc",
		Usage: "<subcommand>",
		Desc:  `Garbage collection of a GCP-backed ABFS server.`,
		Subcommands: []cmd.ABFSCmd{
			&gcpGCBackfillCmd{RootCmd: c.RootCmd},
			&gcpGCRunCmd{RootCmd: c.RootCmd},
			&gcpGCUndoCmd{RootCmd: c.RootCmd},
			&gcpGCWalkCmd{RootCmd: c.RootCmd},
		},
	}
}

func (c *gcpGCCmd) RegisterFlags(fl *pflag.FlagSet) {}

func (c *gcpGCCmd) Exec(ctx context.Context, fl *pflag.FlagSet) error {
	return clierr.NoSubcommandErr(fl)
}

type gcServer struct {
	opts gcp.NewServerOpts
}

func (s *gcServer) registerFlags(fl *pflag.FlagSet, appProfileUsage string) {
	registerStoreFlags(fl, &s.opts.ProjectID, &s.opts.InstanceID, &s.opts.DatabaseID, &s.opts.BtInstance, &s.opts.BucketName)
	fl.StringVar(&s.opts.BtAppProfile, "bt-app-profile", "", appProfileUsage)
	fl.BoolVar(&s.opts.ClientMetrics, "gcp-client-metrics", false, clientMetricsUsage)
}

func (s *gcServer) setOpts() {
	s.opts.GC = true
	s.opts.GCReadOnly = true
	s.opts.MaxCachedHeaders = 1
	s.opts.MaxCachedContents = 1
	s.opts.SmallPools = true
}

func (s *gcServer) check() error {
	return requireStoreFlags(s.opts.ProjectID, s.opts.InstanceID, s.opts.BucketName)
}

// open creates a read-only server: a command's reads must not rescue or count
// as a use, or a command would keep what it reads.
func (s *gcServer) open(ctx context.Context) (*gcp.GCPServer, error) {
	s.setOpts()
	server, err := gcp.New(ctx, &s.opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create the ABFS GCP server: %w", err)
	}
	return server, nil
}

type gcpGCBackfillCmd struct {
	*root.RootCmd
	server gcServer
	opts   gc.BackfillOpts
}

func (c *gcpGCBackfillCmd) Spec() cmd.CmdSpec {
	return cmd.CmdSpec{
		Name:  "backfill",
		Usage: "--project <project> --instance <instance> --bucket <bucket> [--db <db>] [--bt-instance <instance>] [--start <key>] [--shards <n>] [--rate <n>] [--dry-run]",
		Desc: `Write the GC edges of existing objects to the Bigtable table abfsgc.
Scans the objects table and writes the gc row of every object whose row is missing or
was written by an older edge extractor, reading and decoding every such object that is
not a blob. Objects that a server with --gc added have their rows already. The scan can
stop at any time; run it again with --start set to the printed resume key. Needs the
resources of 'abfs server gcp init --gc'.`,
	}
}

func (c *gcpGCBackfillCmd) RegisterFlags(fl *pflag.FlagSet) {
	c.server.registerFlags(fl, appProfileUsage)
	fl.StringVar(&c.opts.Start, "start", "", "The first object key to scan, a lowercase hexadecimal hash or prefix (default: the start of the table)")
	fl.IntVar(&c.opts.Shards, "shards", gc.DefaultBackfillShards, "The number of key ranges scanned in parallel")
	fl.Float64Var(&c.opts.Rate, "rate", defaultBackfillRate, "The maximum number of object rows scanned per second (0 means no limit)")
	fl.BoolVar(&c.opts.DryRun, "dry-run", false, "Count the gc rows to write and write none")
}

func (c *gcpGCBackfillCmd) Exec(ctx context.Context, fl *pflag.FlagSet) error {
	args.EnsureNoArgs(fl)
	if err := c.server.check(); err != nil {
		return err
	}
	checkAppProfile(ctx, &c.server.opts)
	server, err := c.server.open(ctx)
	if err != nil {
		return err
	}
	defer closeServer(server)
	stats, err := server.BackfillGC(ctx, &c.opts)
	if stats != nil {
		printBackfillStats(stats, c.opts.DryRun)
	}
	return err
}

func printBackfillStats(stats *gc.BackfillStats, dryRun bool) {
	fmt.Printf("Scanned %d objects, %d with current gc rows, %d malformed hashes skipped.\n", stats.Scanned, stats.Current, stats.Malformed)
	written := "WRITTEN"
	if dryRun {
		written = "TO WRITE"
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "TYPE\t%s\tEDGES\tFAILED\n", written)
	for _, typ := range slices.Sorted(maps.Keys(stats.Types)) {
		t := stats.Types[typ]
		fmt.Fprintf(w, "%s\t%d\t%d\t%d\n", typ, t.Written, t.Edges, t.Failed)
	}
	_ = w.Flush()
	switch {
	case stats.Done:
	case stats.ResumeKey == "":
		fmt.Println("Stopped early; run again to continue.")
	default:
		fmt.Printf("Stopped early; continue with --start %s\n", stats.ResumeKey)
	}
}

type gcpGCRunCmd struct {
	*root.RootCmd
	server  gcServer
	opts    gc.DeleteOpts
	dryRun  bool
	condemn bool
	delete  bool
	report  string
	explain []string
}

func (c *gcpGCRunCmd) Spec() cmd.CmdSpec {
	return cmd.CmdSpec{
		Name:  "run",
		Usage: "(--dry-run | (--condemn | --delete) (--expected-replicas <n> | --min-replicas <n> [--max-replicas <n>])) --project <project> --instance <instance> --bucket <bucket> [--db <db>] [--bt-instance <instance>] [--report <file>] [--explain <key>[,<key>]]",
		Desc: `Run one garbage collection cycle and report it as JSON. Both modes take the GC lease,
read the refs at one snapshot, scan the abfsgc and maps tables and mark what the roots, the
retention rules and the grace keep. The cycle also scans the meta family of the objects
table to check that every object has a gc row.

--dry-run reports what a collection would remove and writes nothing but the garbage
collector state in Spanner. --condemn also clears the markers of live objects and marks the
unreachable objects that hold no marker of a complete list with the cycle number, after it
wrote their list to the bucket and its row to the Spanner table AbfsGcLists; once every marker
is written it completes that row. A marker of a complete list is never written again. It
deletes nothing, and servers with --gc clear the marker of any marked object they read. It
refuses, and writes no marker and no list, unless every check of the cycle passed:
--expected-replicas distinct server hosts, or at least --min-replicas and at most
--max-replicas, write a GC heartbeat, with settings that fit the timing rules; --grace
covers the largest cache TTL of the servers plus --longest-push; no object lacks a gc row;
and no pin or root could not be read. An object without a gc row shows that servers ran
without --gc, so the cycle first deletes every row of AbfsGcLists, and later cycles write each
marker again with a hold of its own. --explain adds, per key, the root that keeps it.

--delete runs a condemn cycle that first deletes the unreachable objects whose marker is of
a complete list whose hold has ended: the grace plus 16h after the list was completed, or
longer when the cache TTL recorded with the list and the keep-alive interval of the live
mounts need it. It deletes the map rows that name them first, then each object after every
parent of it, each batch recorded in the bucket under abfsgc/deleted/ before its deletes, each
delete conditional on the marker the cycle read. It deletes only when every delete check
passed, and else only condemns and lists the reasons in the report: the list records are
readable; no server that may have run within the grace ran without link validation or with
an older version of the GC rules; the Bigtable instance has one cluster and the app profile
accepts conditional writes; and no live mount can hold the only copy of content it wrote. A
dry run and a condemn cycle report what a delete would remove. Each record holds the rows
it deletes, so 'gc undo' can write them back. The chunks of deleted objects are removed 7
days after the record of their batch, unless the row of the object is back; that step
deletes no chunk while the bucket has Object Versioning on, a retention policy, a default
event-based hold or object retention.
Needs the resources of 'abfs server gcp init --gc'.

Exit status: 0 when nothing was refused, stopped or skipped; 1 when the cycle failed; 3
when it refused to condemn; 4 when it refused to delete; 5 when a worker error stopped its
delete step; 6 when the chunk step deleted no chunks of a delete record past the undo
window, could not list the records, or left a record without its flag a day past the undo
window. Of several, the lowest. A dry run exits 0 or 1.`,
	}
}

func (c *gcpGCRunCmd) RegisterFlags(fl *pflag.FlagSet) {
	c.opts = gc.DefaultDeleteOpts()
	c.server.registerFlags(fl, appProfileUsage)
	fl.BoolVar(&c.dryRun, "dry-run", false, "Report what a collection would remove and change nothing")
	fl.BoolVar(&c.condemn, "condemn", false, "Mark the unreachable objects as condemned and delete nothing")
	fl.BoolVar(&c.delete, "delete", false, "Delete the unreachable objects whose condemn marker has held long enough, then condemn as --condemn does")
	fl.IntVar(&c.opts.ExpectedReplicas, "expected-replicas", 0, "With --condemn or --delete, the number of distinct server hosts that must run with --gc, for a fleet of fixed size (this or --min-replicas is required)")
	fl.IntVar(&c.opts.MinReplicas, "min-replicas", 0, "With --condemn or --delete, the smallest number of distinct server hosts that must run with --gc, for a fleet whose size changes: the autoscaler minimum less the servers a disruption budget lets stop at once")
	fl.IntVar(&c.opts.MaxReplicas, "max-replicas", 0, "With --min-replicas, the largest number of distinct server hosts with --gc (0 means no limit)")
	fl.StringSliceVar(&c.opts.Types, "delete-types", nil, "In delete mode, the object types, separated by commas, that the cycle may delete (default: every type)")
	fl.BoolVar(&c.opts.AcceptMountsWithoutKeepAlive, "accept-mounts-without-keepalive", false, "In delete mode, delete blobs, trees and eval objects while live mounts run without gc-keepalive, whose caches may hold the only copy of content they wrote; the check then only warns")
	fl.DurationVar(&c.opts.StartDelay, "delete-start-delay", 0, "In delete mode, a pause before the delete step, for tests")
	if err := fl.MarkHidden("delete-start-delay"); err != nil {
		panic(err)
	}
	fl.StringVar(&c.report, "report", "", "Write the JSON report to this file instead of stdout")
	fl.DurationVar(&c.opts.Grace, "grace", gc.DefaultGrace, "Keep objects written or touched within this time before the snapshot, and what they reach")
	fl.DurationVar(&c.opts.MountIdle, "mount-idle", gc.DefaultMountIdle, "Count a mount as live, keeping its ref and the commits it was pinned to, while it wrote its mount ref within this time and has no unmount ref; delete mode removes older mount refs. At least 1h, four times the interval at which clients rewrite their mount ref")
	fl.DurationVar(&c.opts.BuildRetention, "build-retention", gc.DefaultBuildRetention, "Keep what build and unmount refs reach for this time after they were written; delete mode removes older ones. A mount ref with an unmount ref keeps only its mount details, until it goes with that unmount ref or at --mount-idle, whichever comes first")
	fl.DurationVar(&c.opts.SnapshotRetention, "snapshot-retention", gc.DefaultSnapshotRetention, "Keep what the ref of a workspace snapshot reaches, with the commits its mount was pinned to, for this time after the ref was written; delete mode removes older ones")
	fl.DurationVar(&c.opts.MapRetention, "map-retention", gc.DefaultMapRetention, "Keep eval map results written or used within this time")
	fl.DurationVar(&c.opts.LongestPush, "longest-push", gc.DefaultLongestPush, "The longest push or build upload, for the check of --grace")
	fl.StringSliceVar(&c.explain, "explain", nil, "Object keys, separated by commas, whose gc row, reachability, first root, times and condemn marker the report explains")
}

var deleteOnlyFlags = []string{
	"delete-types", "accept-mounts-without-keepalive", "delete-start-delay",
}

func (c *gcpGCRunCmd) checkOpts(fl *pflag.FlagSet) error {
	modes := 0
	for _, m := range []bool{c.dryRun, c.condemn, c.delete} {
		if m {
			modes++
		}
	}
	if modes != 1 {
		return errors.New("pass one of --dry-run, --condemn and --delete")
	}
	if !c.delete {
		var set []string
		for _, name := range deleteOnlyFlags {
			if fl.Changed(name) {
				set = append(set, "--"+name)
			}
		}
		if len(set) > 0 {
			return fmt.Errorf("only delete mode takes %s", strings.Join(set, ", "))
		}
	}
	var err error
	switch {
	case c.delete:
		err = c.opts.Validate()
	case c.condemn:
		err = c.opts.CondemnOpts.Validate()
	default:
		err = c.opts.DryRunOpts.Validate()
	}
	if err != nil {
		return err
	}
	c.opts.Explain, err = parseKeys("--explain", c.explain)
	return err
}

func (c *gcpGCRunCmd) Exec(ctx context.Context, fl *pflag.FlagSet) error {
	args.EnsureNoArgs(fl)
	m := newCycleMetrics(metrics.GetProvider())
	r, err := c.run(ctx, fl)
	return m.endCycle(c.mode(), r, err)
}

func (m *cycleMetrics) endCycle(mode string, r *gc.Report, err error) error {
	result, code := cycleExit(r, err)
	m.record(mode, result, r, time.Now())
	if code == 0 || code == 1 {
		return err
	}
	return clierr.CLIErrWithExitCode(code, "%s", outcomeMessage(r, err))
}

func (c *gcpGCRunCmd) mode() string {
	switch {
	case c.delete:
		return gc.PhaseDelete
	case c.condemn:
		return gc.PhaseCondemn
	case c.dryRun:
		return gc.PhaseDryRun
	}
	return ""
}

// outcomeExitCodes are the exit codes of the outcomes of a cycle; a cycle
// that failed exits 1. Of several outcomes, Report.Outcome returns the one
// with the lowest code.
var outcomeExitCodes = map[string]int{
	gc.OutcomeOK:             0,
	gc.OutcomeCondemnRefused: 3,
	gc.OutcomeDeleteRefused:  4,
	gc.OutcomeDeleteStopped:  5,
	gc.OutcomeChunksSkipped:  6,
}

// A cycle without a report failed, whatever its outcome.
func cycleExit(r *gc.Report, err error) (string, int) {
	var refused *gc.ErrCondemnRefused
	switch {
	case r == nil:
		return "error", 1
	case errors.As(err, &refused):
		return gc.OutcomeCondemnRefused, outcomeExitCodes[gc.OutcomeCondemnRefused]
	case err != nil:
		return "error", 1
	}
	outcome := r.Outcome()
	return outcome, outcomeExitCodes[outcome]
}

func outcomeMessage(r *gc.Report, err error) string {
	if err != nil {
		return err.Error()
	}
	outcome := r.Outcome()
	switch outcome {
	case gc.OutcomeCondemnRefused:
		return fmt.Sprintf("the GC cycle %d refused to condemn: %s", r.Cycle, strings.Join(r.Condemn.Refused, "; "))
	case gc.OutcomeDeleteRefused:
		return fmt.Sprintf("the GC cycle %d refused to delete and only condemned: %s", r.Cycle, strings.Join(r.Delete.Refused, "; "))
	case gc.OutcomeDeleteStopped:
		return fmt.Sprintf("the GC cycle %d stopped its delete step: %s", r.Cycle, r.Delete.Step.Error)
	case gc.OutcomeChunksSkipped:
		c := r.Delete.Chunks
		switch {
		case c.ListFailed:
			return fmt.Sprintf("the GC cycle %d could not list the delete records and deleted no chunks: %s", r.Cycle, strings.Join(c.Skipped, "; "))
		case c.RecordsSkipped > 0:
			return fmt.Sprintf("the GC cycle %d deleted no chunks of %d delete records past the undo window: %s", r.Cycle, c.RecordsSkipped, strings.Join(c.Skipped, "; "))
		default:
			return fmt.Sprintf("the GC cycle %d left %d delete records without the chunks-done flag a day past the undo window: %d unreadable records, %d errors, %d bucket deletes refused, %d chunks of an unknown depot", r.Cycle, c.Unflagged, c.RecordErrors, c.Errors, c.DeleteRefused, c.UnknownDepot)
		}
	}
	return fmt.Sprintf("the GC cycle %d ended with outcome %s", r.Cycle, outcome)
}

// Each run is a new series, so these are gauges: a counter would have one
// point, in which a rate sees no increase.
type cycleMetrics struct {
	end    metrics.Gauge
	keys   metrics.Gauge
	chunks metrics.Gauge
}

func newCycleMetrics(p metrics.Provider) *cycleMetrics {
	return &cycleMetrics{
		end:    p.NewGauge("abfs_gc_cycle_end_timestamp_seconds", "Time at which a GC cycle ended, in seconds since the epoch, by mode and result: ok, the outcome that set the exit code, or error.", []string{"mode", "result"}),
		keys:   p.NewGauge("abfs_gc_cycle_keys", "Objects that a GC cycle condemned or deleted, by mode and action.", []string{"mode", "action"}),
		chunks: p.NewGauge("abfs_gc_cycle_chunks_deleted", "Chunks that the chunk step of a GC delete cycle deleted, by depot.", []string{"depot"}),
	}
}

// record sets zero counts for the steps the mode does not run; r may be nil.
func (m *cycleMetrics) record(mode, result string, r *gc.Report, end time.Time) {
	m.end.Set(float64(end.Unix()), mode, result)
	if r == nil || mode == gc.PhaseDryRun {
		return
	}
	var condemned int64
	if r.Condemn != nil {
		condemned = r.Condemn.Condemned
	}
	m.keys.Set(float64(condemned), mode, "condemned")
	if mode != gc.PhaseDelete {
		return
	}
	var deleted, rows, objects int64
	if d := r.Delete; d != nil {
		if d.Step != nil {
			deleted = d.Step.Deleted
		}
		if d.Chunks != nil {
			rows, objects = d.Chunks.ChunkRowsDeleted, d.Chunks.BucketObjectsDeleted
		}
	}
	m.keys.Set(float64(deleted), mode, "deleted")
	m.chunks.Set(float64(rows), "bigtable")
	m.chunks.Set(float64(objects), "bucket")
}

// run returns nil when the cycle stopped before its report was written.
func (c *gcpGCRunCmd) run(ctx context.Context, fl *pflag.FlagSet) (*gc.Report, error) {
	if err := c.checkOpts(fl); err != nil {
		return nil, err
	}
	if err := c.server.check(); err != nil {
		return nil, err
	}
	profile := checkAppProfile(ctx, &c.server.opts)
	if c.delete && len(profile.Problems) > 0 {
		return nil, fmt.Errorf("the Bigtable app profile %s does not fit a delete cycle: %s", profile.ID, strings.Join(profile.Problems, "; "))
	}
	server, err := c.server.open(ctx)
	if err != nil {
		return nil, err
	}
	defer closeServer(server)
	o := &c.server.opts
	var res *gc.Result
	switch {
	case c.delete:
		res, err = server.DeleteGC(ctx, o.BucketName, server.GCInstanceChecks(o.ProjectID, o.BtInstance), &c.opts)
	case c.condemn:
		res, err = server.CondemnGC(ctx, o.BucketName, &c.opts.CondemnOpts)
	default:
		res, err = server.DryRunGC(ctx, &c.opts.DryRunOpts)
	}
	if res == nil {
		return nil, err
	}
	res.Report.AppProfile = profile
	if werr := writeReport(c.report, res.Report); werr != nil {
		return nil, errors.Join(err, werr)
	}
	return res.Report, err
}

type gcpGCUndoCmd struct {
	*root.RootCmd
	server gcServer
	opts   gc.UndoOpts
}

func (c *gcpGCUndoCmd) Spec() cmd.CmdSpec {
	return cmd.CmdSpec{
		Name:  "undo",
		Usage: "--cycle <n> [--through <m>] --project <project> --instance <instance> --bucket <bucket> [--db <db>] [--bt-instance <instance>]",
		Desc: `Write back the objects and map rows that the delete cycles n to m deleted, from the
delete records of those cycles in the bucket, which hold the rows they deleted. Restores
the newest cycle first and the children of an object before it. For each object whose row
is absent: its chunks must exist, its bucket chunks get the metadata abfs-undo and its
chunk rows a new cell, its gc row is written again with a touch, so that it is recent for
the grace, and its objects row comes last, without the condemn marker. Map rows come back
with a new time. Objects and map rows that are present stay as they are. The chunk step
deletes the chunks of a deleted object 7 days after its delete; an object whose chunks are
gone is not restored, nor is anything that links it, and the command names the chunks.
Takes the GC lease, in the phase undo, and waits before its first write until a collector
that lost the lease has stopped writing. Fails when something was not restored.`,
	}
}

func (c *gcpGCUndoCmd) RegisterFlags(fl *pflag.FlagSet) {
	c.opts = gc.DefaultUndoOpts()
	c.server.registerFlags(fl, appProfileUsage)
	fl.Int64Var(&c.opts.Cycle, "cycle", 0, "The first delete cycle whose deletes are undone (required)")
	fl.Int64Var(&c.opts.Through, "through", 0, "The last delete cycle whose deletes are undone (default: --cycle)")
}

func (c *gcpGCUndoCmd) checkOpts() error {
	if c.opts.Cycle == 0 {
		return errors.New("pass --cycle")
	}
	return c.opts.Validate()
}

func (c *gcpGCUndoCmd) Exec(ctx context.Context, fl *pflag.FlagSet) error {
	args.EnsureNoArgs(fl)
	if err := c.checkOpts(); err != nil {
		return err
	}
	if err := c.server.check(); err != nil {
		return err
	}
	checkAppProfile(ctx, &c.server.opts)
	server, err := c.server.open(ctx)
	if err != nil {
		return err
	}
	defer closeServer(server)
	rep, err := server.UndoGC(ctx, c.server.opts.BucketName, &c.opts)
	if rep != nil {
		printUndo(rep)
	}
	if err != nil {
		return err
	}
	if n := rep.NotRestored(); n > 0 {
		return fmt.Errorf("%d objects and map rows were not restored", n)
	}
	return nil
}

func printUndo(rep *gc.UndoReport) {
	fmt.Printf("Read %d delete records of cycles %d to %d after a wait of %.0f s.\n", rep.Records, rep.FirstCycle, rep.LastCycle, rep.FenceWaitSeconds)
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "\tRESTORED\tPRESENT\tMISSING CHUNKS\tNO GC ROW\tINVALID\tBLOCKED")
	fmt.Fprintf(w, "objects\t%d\t%d\t%d\t%d\t%d\t%d\n", rep.Restored, rep.Present, rep.MissingChunks, rep.WithoutGCRow, rep.InvalidContent, rep.BlockedByChild)
	fmt.Fprintf(w, "map rows\t%d\t%d\t-\t-\t-\t%d\n", rep.MapRowsRestored, rep.MapRowsPresent, rep.MapRowsBlocked)
	_ = w.Flush()
	for _, s := range rep.NotRestoredSample {
		fmt.Printf("  not restored: %s\n", s)
	}
}

type gcpGCWalkCmd struct {
	*root.RootCmd
	server  gcServer
	opts    gc.WalkOpts
	details []string
	keys    []string
}

func (c *gcpGCWalkCmd) Spec() cmd.CmdSpec {
	return cmd.CmdSpec{
		Name:  "walk",
		Usage: "--mount-details <hash>[,<hash>] --key <hash>[,<hash>] --project <project> --instance <instance> --bucket <bucket> [--db <db>] [--bt-instance <instance>]",
		Desc: `Tell whether keys are in the content of the checkouts of mounts. Reads each mount
details object and its pinned manifest, and walks the commit and the trees of every
pinned revision from object content, without the abfsgc table, so a gap in the GC edges
does not hide a key. Prints, per key, whether the walk reached it and the first path
that did. It reads trees only to list their entries, so a blob need not exist to be
found. Its reads rescue and touch nothing; it takes no lease and writes nothing. Fails
when the walk is incomplete: mount details without a pinned manifest, revisions that
are not hashes, or objects it could not read; a key outside may then be inside.`,
	}
}

func (c *gcpGCWalkCmd) RegisterFlags(fl *pflag.FlagSet) {
	c.server.registerFlags(fl, "The GCP Bigtable App Profile ID (empty uses the instance default app profile)")
	fl.StringSliceVar(&c.details, "mount-details", nil, "The mount details, separated by commas, whose pinned revisions are walked: the targets of the mount refs of a client (required)")
	fl.StringSliceVar(&c.keys, "key", nil, "The object keys, separated by commas, to look for (required)")
}

func (c *gcpGCWalkCmd) checkOpts() error {
	if len(c.details) == 0 || len(c.keys) == 0 {
		return errors.New("pass --mount-details and --key")
	}
	var err error
	if c.opts.MountDetails, err = parseKeys("--mount-details", c.details); err != nil {
		return err
	}
	c.opts.Keys, err = parseKeys("--key", c.keys)
	return err
}

func parseKeys(flag string, keys []string) ([]entities.Hash, error) {
	hashes := make([]entities.Hash, 0, len(keys))
	for _, k := range keys {
		if !entities.IsHash(k) {
			return nil, fmt.Errorf("invalid key %q for %s", k, flag)
		}
		hashes = append(hashes, entities.NewHash(k))
	}
	return hashes, nil
}

func (c *gcpGCWalkCmd) Exec(ctx context.Context, fl *pflag.FlagSet) error {
	args.EnsureNoArgs(fl)
	if err := c.checkOpts(); err != nil {
		return err
	}
	if err := c.server.check(); err != nil {
		return err
	}
	server, err := c.server.open(ctx)
	if err != nil {
		return err
	}
	defer closeServer(server)
	res, err := server.WalkGC(ctx, &c.opts)
	if err != nil {
		return err
	}
	printWalk(res)
	return res.Err()
}

func printWalk(res *gc.WalkResult) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "KEY\tCLOSURE\tTYPE\tPATH\tCOMMIT\tMOUNT DETAILS")
	for _, k := range res.Keys {
		if !k.Reached {
			fmt.Fprintf(w, "%s\toutside\t-\t-\t-\t-\n", k.Key)
			continue
		}
		path, commit := "-", "-"
		if k.Path != "" {
			path = k.Path
		}
		if !k.Commit.IsZero() {
			commit = k.Commit.String()
		}
		fmt.Fprintf(w, "%s\tinside\t%s\t%s\t%s\t%s\n", k.Key, k.Type, path, commit, k.MountDetails)
	}
	_ = w.Flush()
	fmt.Printf("Walked %d mount details, %d pinned manifests, %d revisions, %d trees and %d entries of files and submodules.\n",
		res.MountDetails, res.PinnedManifests, res.Revisions, res.Trees, res.Entries)
	if res.Incomplete > 0 {
		fmt.Printf("Incomplete, so a key outside may be inside: %d mount details, revisions or objects could not be read or followed, such as:\n", res.Incomplete)
		for _, s := range res.IncompleteSamples {
			fmt.Printf("  %s\n", s)
		}
	}
}

func checkAppProfile(ctx context.Context, o *gcp.NewServerOpts) *gc.ProfileReport {
	rep := gc.ReadAppProfile(ctx, o.ProjectID, o.BtInstance, o.BtAppProfile)
	for _, p := range rep.Problems {
		log.Printf("Warning: Bigtable app profile %s: %s", rep.ID, p)
	}
	if rep.Fix != "" {
		log.Printf("Warning: %s", rep.Fix)
	}
	return rep
}

func writeReport(file string, report *gc.Report) error {
	// One line is one log entry where the logs of a container are collected.
	data, err := json.Marshal(report)
	if err != nil {
		return fmt.Errorf("failed to encode the GC report: %w", err)
	}
	data = append(data, '\n')
	if file == "" {
		_, err = os.Stdout.Write(data)
		return err
	}
	if err := os.WriteFile(file, data, 0o644); err != nil {
		return fmt.Errorf("failed to write the GC report: %w", err)
	}
	return nil
}
