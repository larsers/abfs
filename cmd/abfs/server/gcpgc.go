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
not a blob. Objects that a server with --gc-edges added have their rows already. The scan
can stop at any time; run it again with --start set to the printed resume key.`,
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
