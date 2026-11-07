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

const defaultBackfillRate = 2000

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
	fl.StringVar(&c.server.BtAppProfile, "bt-app-profile", "", "The GCP Bigtable App Profile ID (empty uses the instance default app profile)")
	fl.StringVar(&c.server.BucketName, "bucket", "", "The GCS Bucket Name (required)")
	fl.StringVar(&c.opts.Start, "start", "", "The first object key to scan, a lowercase hexadecimal hash or prefix (default: the start of the table)")
	fl.StringVar(&c.opts.End, "end", "", "The object key to stop before (default: the end of the table)")
	fl.IntVar(&c.opts.Shards, "shards", gc.DefaultBackfillShards, "The number of key ranges scanned in parallel")
	fl.IntVar(&c.opts.BatchSize, "batch-size", gc.DefaultBackfillBatchSize, "The number of object rows read per request")
	fl.Float64Var(&c.opts.Rate, "rate", defaultBackfillRate, "The maximum number of object rows scanned per second (0 means no limit)")
	fl.BoolVar(&c.opts.DryRun, "dry-run", false, "Count the gc rows to write and write none")
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
	opts    gc.CondemnOpts
	dryRun  bool
	condemn bool
	report  string
	explain []string
}

func (c *gcpGCRunCmd) Spec() cmd.CmdSpec {
	return cmd.CmdSpec{
		Name:  "run",
		Usage: "(--dry-run | --condemn --expected-replicas <n>) --project <project> --instance <instance> --bucket <bucket> [--db <db>] [--bt-instance <instance>] [--report <file>] [--explain <key>[,<key>]]",
		Desc: `Run one garbage collection cycle and report it as JSON. Both modes take the GC lease,
read the refs at one snapshot, scan the gc and maps tables and mark what the roots, the
retention rules and the grace keep. With --consistency (required for --condemn) the cycle
also scans the meta family of the objects table to check that every object has a gc row.

--dry-run reports what a collection would remove and writes nothing but the garbage
collector state in Spanner. --condemn also marks the unreachable objects with the cycle
number, after it wrote their list to the bucket; it deletes nothing, and servers with --gc
clear the marker of any marked object they read. It refuses, and writes nothing, unless
every check of the cycle passed: --expected-replicas distinct server hosts write a GC
heartbeat, one live row each, with settings that fit the timing rules; no object lacks a gc
row; and no pin or root could not be read. --explain adds, per key, the root that keeps it.
Needs the resources of 'abfs server gcp init --gc'.`,
	}
}

func (c *gcpGCRunCmd) RegisterFlags(fl *pflag.FlagSet) {
	c.opts = gc.DefaultCondemnOpts()
	fl.StringVar(&c.server.ProjectID, "project", "", "The GCP Project which contains the databases (required)")
	fl.StringVar(&c.server.InstanceID, "instance", "", "The GCP Spanner Database Instance (required)")
	fl.StringVar(&c.server.DatabaseID, "db", defaultDatabase, "The GCP Spanner Database Name")
	fl.StringVar(&c.server.BtInstance, "bt-instance", defaultBtInstance, "The GCP Bigtable Database Instance")
	fl.StringVar(&c.server.BtAppProfile, "bt-app-profile", "", "The GCP Bigtable App Profile ID for the scans, best one with low priority and single-cluster routing (empty uses the instance default app profile)")
	fl.StringVar(&c.server.BucketName, "bucket", "", "The GCS Bucket Name (required)")
	fl.BoolVar(&c.dryRun, "dry-run", false, "Report what a collection would remove and change nothing")
	fl.BoolVar(&c.condemn, "condemn", false, "Mark the unreachable objects as condemned and delete nothing")
	fl.IntVar(&c.opts.ExpectedReplicas, "expected-replicas", 0, "With --condemn, the number of distinct server hosts that must run with --gc (required)")
	fl.Float64Var(&c.opts.Rate, "condemn-rate", gc.DefaultCondemnRate, "With --condemn, the maximum number of objects marked per second (0 means no limit)")
	fl.IntVar(&c.opts.KeepLists, "keep-lists", gc.DefaultKeepLists, "With --condemn, the number of condemned lists of the newest cycles kept in the bucket")
	fl.StringVar(&c.report, "report", "", "Write the JSON report to this file instead of stdout")
	fl.DurationVar(&c.opts.Grace, "grace", gc.DefaultGrace, "Keep objects written or touched within this time before the snapshot, and what they reach")
	fl.DurationVar(&c.opts.MountIdle, "mount-idle", gc.DefaultMountIdle, "Keep what a mount ref reaches while the mount wrote its ref within this time")
	fl.DurationVar(&c.opts.BuildRetention, "build-retention", gc.DefaultBuildRetention, "Keep what build and unmount refs reach for this time after they were written")
	fl.DurationVar(&c.opts.MapRetention, "map-retention", gc.DefaultMapRetention, "Keep eval map results written or used within this time")
	fl.DurationVar(&c.opts.Cycle, "cycle", gc.DefaultCycle, "The period between garbage collection cycles, for the check of --grace")
	fl.DurationVar(&c.opts.CacheTTL, "cache-ttl", gc.DefaultCacheTTL, "The TTL of the servers' header and content caches, for the check of --grace")
	fl.DurationVar(&c.opts.LongestPush, "longest-push", gc.DefaultLongestPush, "The longest push or build upload, for the check of --grace")
	fl.BoolVar(&c.opts.Consistency, "consistency", true, "Check that every object has a gc row, and sum object sizes, with a scan of the meta family of the objects table")
	fl.IntVar(&c.opts.Shards, "shards", gc.DefaultScanShards, "The number of key ranges scanned in parallel")
	fl.DurationVar(&c.opts.LeaseDuration, "lease", gc.DefaultLeaseDuration, "The duration of the GC lease, renewed while the cycle runs")
	fl.StringSliceVar(&c.explain, "explain", nil, "Object keys, separated by commas, whose gc row, reachability, first root, times and condemn marker the report explains")
}

// checkOpts checks the mode flags and fills the keys to explain.
func (c *gcpGCRunCmd) checkOpts() error {
	if c.dryRun == c.condemn {
		return errors.New("pass one of --dry-run and --condemn")
	}
	var err error
	if c.condemn {
		err = c.opts.Validate()
	} else {
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
	if err := c.checkOpts(); err != nil {
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
	if c.condemn {
		res, err = server.CondemnGC(ctx, c.server.BucketName, &c.opts)
	} else {
		res, err = server.DryRunGC(ctx, &c.opts.DryRunOpts)
	}
	if res != nil {
		if werr := writeReport(c.report, res.Report); werr != nil {
			return errors.Join(err, werr)
		}
	}
	return err
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
