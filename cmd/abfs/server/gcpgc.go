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

	"github.com/spf13/pflag"

	"github.com/google/abfs/shared/cmd"
	"github.com/google/abfs/shared/cmd/args"
	"github.com/google/abfs/shared/cmd/clierr"
	"github.com/google/abfs/shared/cmd/root"
	"github.com/google/abfs/shared/entities"
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

type gcpGCBackfillCmd struct {
	*root.RootCmd
	server gcp.NewServerOpts
	opts   gc.BackfillOpts
}

func (c *gcpGCBackfillCmd) Spec() cmd.CmdSpec {
	return cmd.CmdSpec{
		Name:  "backfill",
		Usage: "--project <project> --instance <instance> --bucket <bucket> [--db <db>] [--bt-instance <instance>] [--start <key>] [--end <key>] [--shards <n>] [--rate <n>] [--dry-run]",
		Desc: `Write the GC edges of existing objects to the Bigtable table gc.
Scans the objects table and writes the gc row of every object whose row is missing or
was written by an older edge extractor, reading and decoding every such object that is
not a blob. Objects that a server with --gc added have their rows already. The scan can
stop at any time; run it again with --start set to the printed resume key. Needs the
resources of 'abfs server gcp init --gc'.`,
	}
}

func (c *gcpGCBackfillCmd) RegisterFlags(fl *pflag.FlagSet) {
	fl.StringVar(&c.server.ProjectID, "project", "", "The GCP Project which contains the databases (required)")
	fl.StringVar(&c.server.InstanceID, "instance", "", "The GCP Spanner Database Instance (required)")
	fl.StringVar(&c.server.DatabaseID, "db", defaultDatabase, "The GCP Spanner Database Name")
	fl.StringVar(&c.server.BtInstance, "bt-instance", defaultBtInstance, "The GCP Bigtable Database Instance")
	fl.StringVar(&c.server.BtAppProfile, "bt-app-profile", "", appProfileUsage)
	fl.StringVar(&c.server.BucketName, "bucket", "", "The GCS Bucket Name (required)")
	fl.StringVar(&c.opts.Start, "start", "", "The first object key to scan, a lowercase hexadecimal hash or prefix (default: the start of the table)")
	fl.StringVar(&c.opts.End, "end", "", "The object key to stop before (default: the end of the table)")
	fl.IntVar(&c.opts.Shards, "shards", gc.DefaultBackfillShards, "The number of key ranges scanned in parallel")
	fl.IntVar(&c.opts.BatchSize, "batch-size", gc.DefaultBackfillBatchSize, "The number of object rows read per request")
	fl.Float64Var(&c.opts.Rate, "rate", defaultBackfillRate, "The maximum number of object rows scanned per second (0 means no limit)")
	fl.BoolVar(&c.opts.DryRun, "dry-run", false, "Count the gc rows to write and write none")
	fl.DurationVar(&c.opts.WriteDeadline, "write-deadline", gc.DefaultWriteDeadline, "The deadline of one attempt of a gc row write, the client library's retries included")
	fl.BoolVar(&c.server.ClientMetrics, "gcp-client-metrics", true, clientMetricsUsage)
}

func (c *gcpGCBackfillCmd) Exec(ctx context.Context, fl *pflag.FlagSet) error {
	args.EnsureNoArgs(fl)
	if err := requireFlags(map[string]string{"--project": c.server.ProjectID, "--instance": c.server.InstanceID, "--bucket": c.server.BucketName}); err != nil {
		return err
	}
	// The scan reads each object once, so caching them only costs memory.
	// Its reads must not rescue condemned objects or count as a use.
	c.server.GC = true
	c.server.GCReadOnly = true
	c.server.MaxCachedHeaders = 1
	c.server.MaxCachedContents = 1
	checkAppProfile(ctx, &c.server)
	server, err := gcp.New(ctx, &c.server)
	if err != nil {
		return fmt.Errorf("failed to create the ABFS GCP server: %w", err)
	}
	defer func() {
		if err := server.Close(); err != nil {
			log.Printf("Warning: error closing ABFS server: %v", err)
		}
	}()
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
	server  gcp.NewServerOpts
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
read the refs at one snapshot, scan the gc and maps tables and mark what the roots, the
retention rules and the grace keep. With --consistency (required for --condemn) the cycle
also scans the meta family of the objects table to check that every object has a gc row.

--dry-run reports what a collection would remove and writes nothing but the garbage
collector state in Spanner. --condemn also marks the unreachable objects with the cycle
number, after it wrote their list to the bucket; it deletes nothing, and servers with --gc
clear the marker of any marked object they read. It refuses, and writes nothing, unless
every check of the cycle passed: --expected-replicas distinct server hosts, or at least
--min-replicas and at most --max-replicas, write a GC heartbeat, one live row each, with
settings that fit the timing rules; no object lacks a gc row; and no pin or root could not
be read. --explain adds, per key, the root that keeps it.

--delete runs a condemn cycle that first deletes the objects of the list of the condemn or
delete cycle before that are still unreachable and still hold its marker: the map rows that
name them first, then each object after every parent of it, each batch recorded in the
bucket under gc/deleted/ before its deletes, each delete conditional on the marker. It
deletes only when every delete check passed, and else only condemns and lists the reasons
in the report: the list is complete and the cycle runs in the window its timings allow; no
server that may have run within the grace crashed, lost touches or ran without link
validation; the Bigtable instance has one cluster and the app profile accepts conditional
writes; ready backups of the tables exist (unless --no-backup-check); and no live mount can
hold the only copy of content it wrote. A dry run and a condemn cycle report what a delete
would remove. The chunks of deleted objects are removed one cycle later, at least
--chunk-delay after the record of their batch, unless the row of the object is back; that
step deletes nothing while the bucket keeps deleted objects for less than 7 days (unless
--allow-no-soft-delete), has Object Versioning on or a retention policy, and it keeps a
record until a ready backup of the chunks table started after its list (unless
--no-backup-check).
Needs the resources of 'abfs server gcp init --gc'.`,
	}
}

func (c *gcpGCRunCmd) RegisterFlags(fl *pflag.FlagSet) {
	c.opts = gc.DefaultDeleteOpts()
	fl.StringVar(&c.server.ProjectID, "project", "", "The GCP Project which contains the databases (required)")
	fl.StringVar(&c.server.InstanceID, "instance", "", "The GCP Spanner Database Instance (required)")
	fl.StringVar(&c.server.DatabaseID, "db", defaultDatabase, "The GCP Spanner Database Name")
	fl.StringVar(&c.server.BtInstance, "bt-instance", defaultBtInstance, "The GCP Bigtable Database Instance")
	fl.StringVar(&c.server.BtAppProfile, "bt-app-profile", "", appProfileUsage)
	fl.BoolVar(&c.server.ClientMetrics, "gcp-client-metrics", true, clientMetricsUsage)
	fl.StringVar(&c.server.BucketName, "bucket", "", "The GCS Bucket Name (required)")
	fl.BoolVar(&c.dryRun, "dry-run", false, "Report what a collection would remove and change nothing")
	fl.BoolVar(&c.condemn, "condemn", false, "Mark the unreachable objects as condemned and delete nothing")
	fl.BoolVar(&c.delete, "delete", false, "Delete the objects condemned by the cycle before that are still unreachable, then condemn as --condemn does")
	fl.IntVar(&c.opts.ExpectedReplicas, "expected-replicas", 0, "With --condemn or --delete, the number of distinct server hosts that must run with --gc, for a fleet of fixed size (this or --min-replicas is required)")
	fl.IntVar(&c.opts.MinReplicas, "min-replicas", 0, "With --condemn or --delete, the smallest number of distinct server hosts that must run with --gc, for a fleet whose size changes: the autoscaler minimum less the servers a disruption budget lets stop at once")
	fl.IntVar(&c.opts.MaxReplicas, "max-replicas", 0, "With --min-replicas, the largest number of distinct server hosts with --gc (0 means no limit)")
	fl.StringSliceVar(&c.opts.Types, "delete-types", nil, "In delete mode, the object types, separated by commas, that the cycle may delete (default: every type)")
	fl.BoolVar(&c.opts.AcceptMountsWithoutKeepAlive, "accept-mounts-without-keepalive", false, "In delete mode, delete blobs, trees and eval objects while live mounts run without gc-keepalive, whose caches may hold the only copy of content they wrote; the check then only warns")
	fl.BoolVar(&c.opts.NoBackupCheck, "no-backup-check", false, "In delete mode, delete without ready backups of the tables objects, gc, maps and chunks taken after the list was complete")
	fl.Float64Var(&c.opts.DeleteRate, "delete-rate", gc.DefaultDeleteRate, "In delete mode, the maximum number of objects deleted per second (0 means no limit)")
	fl.IntVar(&c.opts.DeleteWorkers, "delete-workers", gc.DefaultDeleteWorkers, "In delete mode, the number of conditional deletes in flight, split evenly over the --shards key ranges; at least --shards")
	fl.DurationVar(&c.opts.ChunkDelay, "chunk-delay", gc.DefaultChunkDelay, "In delete mode, how long after the record of a deleted object its chunks are deleted, at the earliest in the next cycle; at least 12 min, the 10 min lifetime of signed chunk URLs plus 1 min of clock skew on each side")
	fl.Float64Var(&c.opts.ChunkRate, "chunk-rate", gc.DefaultChunkRate, "In delete mode, the maximum number of chunks of deleted objects deleted per second (0 means no limit)")
	fl.BoolVar(&c.opts.AllowNoSoftDelete, "allow-no-soft-delete", false, "In delete mode, delete the bucket chunks of deleted objects although the bucket keeps deleted objects for less than 7 days, so that they cannot be restored")
	fl.DurationVar(&c.opts.StartDelay, "delete-start-delay", 0, "In delete mode, a pause before the delete step, for tests")
	if err := fl.MarkHidden("delete-start-delay"); err != nil {
		panic(err)
	}
	fl.Float64Var(&c.opts.Rate, "condemn-rate", gc.DefaultCondemnRate, "With --condemn or --delete, the maximum number of objects marked per second (0 means no limit)")
	fl.IntVar(&c.opts.KeepLists, "keep-lists", gc.DefaultKeepLists, "With --condemn or --delete, the number of condemned lists of the newest cycles kept in the bucket")
	fl.StringVar(&c.report, "report", "", "Write the JSON report to this file instead of stdout")
	fl.DurationVar(&c.opts.Grace, "grace", gc.DefaultGrace, "Keep objects written or touched within this time before the snapshot, and what they reach")
	fl.DurationVar(&c.opts.MountIdle, "mount-idle", gc.DefaultMountIdle, "Keep what a mount ref reaches while the mount wrote its ref within this time; delete mode removes older mount refs")
	fl.DurationVar(&c.opts.BuildRetention, "build-retention", gc.DefaultBuildRetention, "Keep what build and unmount refs reach for this time after they were written; delete mode removes older ones")
	fl.DurationVar(&c.opts.SnapshotRetention, "snapshot-retention", gc.DefaultSnapshotRetention, "Keep what the ref of a workspace snapshot reaches, with the commits its mount was pinned to, for this time after the ref was written; delete mode removes older ones")
	fl.DurationVar(&c.opts.MapRetention, "map-retention", gc.DefaultMapRetention, "Keep eval map results written or used within this time")
	fl.DurationVar(&c.opts.Cycle, "cycle", gc.DefaultCycle, "The period between garbage collection cycles, for the check of --grace")
	fl.DurationVar(&c.opts.CacheTTL, "cache-ttl", gc.DefaultCacheTTL, "The TTL of the servers' header and content caches, for the check of --grace")
	fl.DurationVar(&c.opts.LongestPush, "longest-push", gc.DefaultLongestPush, "The longest push or build upload, for the check of --grace")
	fl.BoolVar(&c.opts.Consistency, "consistency", true, "Check that every object has a gc row, and sum object sizes, with a scan of the meta family of the objects table")
	fl.IntVar(&c.opts.Shards, "shards", gc.DefaultScanShards, "The number of key ranges scanned in parallel")
	fl.DurationVar(&c.opts.LeaseDuration, "lease", gc.DefaultLeaseDuration, "The duration of the GC lease, renewed while the cycle runs")
	fl.DurationVar(&c.opts.WriteDeadline, "write-deadline", gc.DefaultWriteDeadline, "The deadline of one attempt of a Bigtable write, the client library's retries included; a list object gets it plus its size at 8 MiB/s. At most an eighth of --lease")
	fl.StringSliceVar(&c.explain, "explain", nil, "Object keys, separated by commas, whose gc row, reachability, first root, times and condemn marker the report explains")
}

// deleteOnlyFlags are the flags that only delete mode reads.
var deleteOnlyFlags = []string{
	"delete-types", "accept-mounts-without-keepalive", "no-backup-check", "delete-rate", "delete-workers",
	"chunk-delay", "chunk-rate", "allow-no-soft-delete", "delete-start-delay",
}

// checkOpts checks the mode flags set in fl and fills the keys to explain.
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
	c.opts.Explain = nil
	for _, k := range c.explain {
		if !entities.IsHash(k) {
			return fmt.Errorf("invalid key %q for --explain", k)
		}
		c.opts.Explain = append(c.opts.Explain, entities.NewHash(k))
	}
	return nil
}

func (c *gcpGCRunCmd) Exec(ctx context.Context, fl *pflag.FlagSet) error {
	args.EnsureNoArgs(fl)
	if err := c.checkOpts(fl); err != nil {
		return err
	}
	if err := requireFlags(map[string]string{"--project": c.server.ProjectID, "--instance": c.server.InstanceID, "--bucket": c.server.BucketName}); err != nil {
		return err
	}
	// The run reads only the workspace snapshots, mount details and pinned
	// manifests, once each, and must not rescue them.
	c.server.GC = true
	c.server.GCReadOnly = true
	c.server.MaxCachedHeaders = 1
	c.server.MaxCachedContents = 1
	profile := checkAppProfile(ctx, &c.server)
	if c.delete && len(profile.Problems) > 0 {
		return fmt.Errorf("the Bigtable app profile %s does not fit a delete cycle: %s", profile.ID, strings.Join(profile.Problems, "; "))
	}
	server, err := gcp.New(ctx, &c.server)
	if err != nil {
		return fmt.Errorf("failed to create the ABFS GCP server: %w", err)
	}
	defer func() {
		if err := server.Close(); err != nil {
			log.Printf("Warning: error closing ABFS server: %v", err)
		}
	}()
	var res *gc.Result
	switch {
	case c.delete:
		res, err = server.DeleteGC(ctx, c.server.BucketName, server.GCInstanceChecks(c.server.ProjectID, c.server.BtInstance), &c.opts)
	case c.condemn:
		res, err = server.CondemnGC(ctx, c.server.BucketName, &c.opts.CondemnOpts)
	default:
		res, err = server.DryRunGC(ctx, c.server.BucketName, &c.opts.DryRunOpts)
	}
	if res != nil {
		res.Report.AppProfile = profile
		if werr := writeReport(c.report, res.Report); werr != nil {
			return errors.Join(err, werr)
		}
	}
	return err
}

type gcpGCUndoCmd struct {
	*root.RootCmd
	server gcp.NewServerOpts
	opts   gc.UndoOpts
}

func (c *gcpGCUndoCmd) Spec() cmd.CmdSpec {
	return cmd.CmdSpec{
		Name:  "undo",
		Usage: "--cycle <n> [--through <m>] --from-suffix <suffix> --project <project> --instance <instance> --bucket <bucket> [--db <db>] [--bt-instance <instance>]",
		Desc: `Write back the objects and map rows that the delete cycles n to m deleted, from the
tables objects, gc, maps and chunks restored as <table><suffix> from backups taken before
cycle n, and the delete records of those cycles in the bucket. Restores the newest cycle
first and the children of an object before it. For each object whose row is absent: its
chunks in the bucket must exist and get the metadata abfs-undo, its chunk rows and its gc
row are written again with a touch, so that it is recent for the grace, and its objects
row comes last, without the condemn marker. Map rows come back with a new time. Objects
and map rows that are present stay as they are. An object whose chunks are gone from the
bucket is not restored, nor is anything that links it: the command prints the chunk
names; restore them from soft delete and run the command again. Takes the GC lease, in
the phase undo, and waits before its first write until a collector that lost the lease
has stopped writing. Fails when something was not restored.`,
	}
}

func (c *gcpGCUndoCmd) RegisterFlags(fl *pflag.FlagSet) {
	c.opts = gc.DefaultUndoOpts()
	fl.StringVar(&c.server.ProjectID, "project", "", "The GCP Project which contains the databases (required)")
	fl.StringVar(&c.server.InstanceID, "instance", "", "The GCP Spanner Database Instance (required)")
	fl.StringVar(&c.server.DatabaseID, "db", defaultDatabase, "The GCP Spanner Database Name")
	fl.StringVar(&c.server.BtInstance, "bt-instance", defaultBtInstance, "The GCP Bigtable Database Instance")
	fl.StringVar(&c.server.BtAppProfile, "bt-app-profile", "", appProfileUsage)
	fl.BoolVar(&c.server.ClientMetrics, "gcp-client-metrics", true, clientMetricsUsage)
	fl.StringVar(&c.server.BucketName, "bucket", "", "The GCS Bucket Name (required)")
	fl.Int64Var(&c.opts.Cycle, "cycle", 0, "The first delete cycle whose deletes are undone (required)")
	fl.Int64Var(&c.opts.Through, "through", 0, "The last delete cycle whose deletes are undone (default: --cycle)")
	fl.StringVar(&c.opts.FromSuffix, "from-suffix", "", "The suffix of the names of the restored tables, as -r for objects-r, gc-r, maps-r and chunks-r (required)")
	fl.IntVar(&c.opts.Workers, "workers", gc.DefaultUndoWorkers, "The number of objects of one delete record restored in parallel")
	fl.BoolVar(&c.opts.NoSourceCheck, "no-source-check", false, "Restore from tables that are not restored from backups of the live tables taken before --cycle")
	fl.DurationVar(&c.opts.LeaseDuration, "lease", gc.DefaultLeaseDuration, "The duration of the GC lease, renewed while the undo runs")
	fl.DurationVar(&c.opts.WriteDeadline, "write-deadline", gc.DefaultWriteDeadline, "The deadline of one attempt of a write, the client library's retries included. At most an eighth of --lease")
}

func (c *gcpGCUndoCmd) checkOpts() error {
	if c.opts.Cycle == 0 || c.opts.FromSuffix == "" {
		return errors.New("pass --cycle and --from-suffix")
	}
	return c.opts.Validate()
}

func (c *gcpGCUndoCmd) Exec(ctx context.Context, fl *pflag.FlagSet) error {
	args.EnsureNoArgs(fl)
	if err := c.checkOpts(); err != nil {
		return err
	}
	if err := requireFlags(map[string]string{"--project": c.server.ProjectID, "--instance": c.server.InstanceID, "--bucket": c.server.BucketName}); err != nil {
		return err
	}
	c.server.GC = true
	c.server.GCReadOnly = true
	checkAppProfile(ctx, &c.server)
	server, err := gcp.New(ctx, &c.server)
	if err != nil {
		return fmt.Errorf("failed to create the ABFS GCP server: %w", err)
	}
	defer func() {
		if err := server.Close(); err != nil {
			log.Printf("Warning: error closing ABFS server: %v", err)
		}
	}()
	rep, err := server.UndoGC(ctx, c.server.BucketName, server.GCTableSources(c.server.ProjectID, c.server.BtInstance), &c.opts)
	if rep != nil {
		printUndo(rep, c.server.BucketName)
	}
	if err != nil {
		return err
	}
	if n := rep.NotRestored(); n > 0 {
		return fmt.Errorf("%d objects and map rows were not restored", n)
	}
	return nil
}

func printUndo(rep *gc.UndoReport, bucket string) {
	if len(rep.MissingBucketObjects) > 0 {
		fmt.Printf("Waiting for %d bucket chunks of %d objects; restore them from soft delete and run gc undo again:\n", len(rep.MissingBucketObjects), rep.WaitingForBucketObjects)
		for _, name := range rep.MissingBucketObjects {
			fmt.Printf("  gs://%s/%s\n", bucket, name)
		}
	}
	fmt.Printf("Read %d delete records of cycles %d to %d after a wait of %.0f s.\n", rep.Records, rep.FirstCycle, rep.LastCycle, rep.FenceWaitSeconds)
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "\tRESTORED\tPRESENT\tMISSING FROM BACKUP\tWAITING FOR CHUNKS\tINVALID\tBLOCKED")
	fmt.Fprintf(w, "objects\t%d\t%d\t%d\t%d\t%d\t%d\n", rep.Restored, rep.Present, rep.MissingFromBackup, rep.WaitingForBucketObjects, rep.InvalidContent, rep.BlockedByChild)
	fmt.Fprintf(w, "map rows\t%d\t%d\t%d\t-\t-\t%d\n", rep.MapRowsRestored, rep.MapRowsPresent, rep.MapRowsMissing, rep.MapRowsBlocked)
	_ = w.Flush()
	for _, s := range rep.NotRestoredSample {
		fmt.Printf("  not restored: %s\n", s)
	}
}

type gcpGCWalkCmd struct {
	*root.RootCmd
	server  gcp.NewServerOpts
	opts    gc.WalkOpts
	details []string
	keys    []string
}

func (c *gcpGCWalkCmd) Spec() cmd.CmdSpec {
	return cmd.CmdSpec{
		Name:  "walk",
		Usage: "--mount-details <hash>[,<hash>] --key <hash>[,<hash>] --project <project> --instance <instance> --bucket <bucket> [--db <db>] [--bt-instance <instance>] [--workers <n>]",
		Desc: `Tell whether keys are in the content of the checkouts of mounts. Reads each mount
details object and its pinned manifest, and walks the commit and the trees of every
pinned revision from object content, without the gc table, so a gap in the GC edges
does not hide a key. Prints, per key, whether the walk reached it and the first path
that did. It reads trees only to list their entries, so a blob need not exist to be
found. Its reads rescue and touch nothing; it takes no lease and writes nothing. Fails
when the walk is incomplete: mount details without a pinned manifest, revisions that
are not hashes, or objects it could not read; a key outside may then be inside.`,
	}
}

func (c *gcpGCWalkCmd) RegisterFlags(fl *pflag.FlagSet) {
	fl.StringVar(&c.server.ProjectID, "project", "", "The GCP Project which contains the databases (required)")
	fl.StringVar(&c.server.InstanceID, "instance", "", "The GCP Spanner Database Instance (required)")
	fl.StringVar(&c.server.DatabaseID, "db", defaultDatabase, "The GCP Spanner Database Name")
	fl.StringVar(&c.server.BtInstance, "bt-instance", defaultBtInstance, "The GCP Bigtable Database Instance")
	fl.StringVar(&c.server.BtAppProfile, "bt-app-profile", "", "The GCP Bigtable App Profile ID (empty uses the instance default app profile)")
	fl.StringVar(&c.server.BucketName, "bucket", "", "The GCS Bucket Name (required)")
	fl.StringSliceVar(&c.details, "mount-details", nil, "The mount details, separated by commas, whose pinned revisions are walked: the targets of the mount refs of a client (required)")
	fl.StringSliceVar(&c.keys, "key", nil, "The object keys, separated by commas, to look for (required)")
	fl.IntVar(&c.opts.Workers, "workers", gc.DefaultWalkWorkers, "The number of pinned revisions walked in parallel")
}

func (c *gcpGCWalkCmd) checkOpts() error {
	if len(c.details) == 0 || len(c.keys) == 0 {
		return errors.New("pass --mount-details and --key")
	}
	if c.opts.Workers < 1 {
		return fmt.Errorf("invalid --workers %d: it must be at least 1", c.opts.Workers)
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
	if err := requireFlags(map[string]string{"--project": c.server.ProjectID, "--instance": c.server.InstanceID, "--bucket": c.server.BucketName}); err != nil {
		return err
	}
	// Its reads must not rescue condemned objects or count as a use, or the
	// walk would keep what it is asked about.
	c.server.GC = true
	c.server.GCReadOnly = true
	server, err := gcp.New(ctx, &c.server)
	if err != nil {
		return fmt.Errorf("failed to create the ABFS GCP server: %w", err)
	}
	defer func() {
		if err := server.Close(); err != nil {
			log.Printf("Warning: error closing ABFS server: %v", err)
		}
	}()
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

// checkAppProfile reads the Bigtable app profile of a GC command and warns
// about each setting the collector needs that it lacks.
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
	data, err := json.MarshalIndent(report, "", "  ")
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
