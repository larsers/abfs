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
	"slices"
	"strings"
	"time"

	"github.com/spf13/pflag"

	"github.com/google/abfs/shared/cmd"
	"github.com/google/abfs/shared/cmd/args"
	"github.com/google/abfs/shared/cmd/root"
	"github.com/google/abfs/shared/remote/servers/gcp"
)

const (
	defaultDatabase              = "cas"
	defaultBtInstance            = "objects"
	defaultMaxCachedHeaders      = gcp.DefaultMaxCachedHeaders
	defaultMaxCachedContents     = gcp.DefaultMaxCachedContents
	defaultLinksContentThreshold = gcp.DefaultLinksContentThreshold
	defaultBtConnPool            = gcp.DefaultBtConnPool
)

type gcpCmd struct {
	*serverCmd
	project               string
	instance              string
	database              string
	btInstance            string
	btAppProfile          string
	btConnPool            int
	bucket                string
	maxCachedHeaders      int
	maxCachedContents     int
	clientMetrics         bool
	linksContent          bool
	linksContentThreshold int64
	gc                    bool
	cacheTTL              time.Duration
	gcTouchInterval       time.Duration
}

func (c *gcpCmd) Spec() cmd.CmdSpec {
	return cmd.CmdSpec{
		Name:    "gcp",
		Usage:   "[-p <port>] --project <project> --instance <instance> --bucket <bucket> [--db <db>] [--bt-instance <instance>]",
		Aliases: []string{},
		Desc:    `Run a GCP-backed ABFS server.`,
		Subcommands: []cmd.ABFSCmd{
			&gcpInitCmd{RootCmd: c.RootCmd},
			&gcpGCCmd{RootCmd: c.RootCmd},
		},
	}
}

func (c *gcpCmd) RegisterFlags(fl *pflag.FlagSet) {
	fl.StringVar(&c.project, "project", "", "The GCP Project which contains the databases (required)")
	fl.StringVar(&c.instance, "instance", "", "The GCP Spanner Database Instance (required)")
	fl.StringVar(&c.database, "db", defaultDatabase, "The GCP Spanner Database Name")
	fl.StringVar(&c.btInstance, "bt-instance", defaultBtInstance, "The GCP Bigtable Database Instance")
	fl.StringVar(&c.btAppProfile, "bt-app-profile", "", "The GCP Bigtable App Profile ID (empty uses the instance default app profile)")
	fl.IntVar(&c.btConnPool, "bt-conn-pool", defaultBtConnPool, fmt.Sprintf("Number of gRPC connections in the Bigtable client pool. A non-positive value uses %d. Avoid excessively high values (fd/memory pressure).", gcp.DefaultBtConnPool))
	fl.StringVar(&c.bucket, "bucket", "", "The GCS Bucket Name (required)")
	fl.IntVarP(&c.maxCachedHeaders, "headers-cache", "", defaultMaxCachedHeaders, "The maximum number of cached objects' headers")
	fl.IntVarP(&c.maxCachedContents, "contents-cache", "", defaultMaxCachedContents, "The maximum number of cached objects' content")
	fl.BoolVar(&c.clientMetrics, "gcp-client-metrics", false, clientMetricsUsage)
	fl.BoolVar(&c.linksContent, "links-content", false, "Return signed Cloud Storage URLs instead of streaming objects larger than --links-content-threshold, so that clients download them from Cloud Storage directly. The server's service account must be able to sign URLs (iam.serviceAccounts.signBlob on itself)")
	fl.Int64Var(&c.linksContentThreshold, "links-content-threshold", defaultLinksContentThreshold, "The size in bytes above which objects are returned as links when --links-content is set")
	fl.BoolVar(&c.gc, "gc", false, "Run the server side of the garbage collector: write the GC edges of new objects to the Bigtable table gc, rescue condemned objects that are used, apply --cache-ttl and --gc-touch-interval, and write a heartbeat to the Spanner table GcServers every 5m, which the garbage collector reads. The server does not start without its first heartbeat and answers Unavailable while its last successful one is older than 10m30s. Needs the resources of 'abfs server gcp init --gc'")
	fl.DurationVar(&c.cacheTTL, "cache-ttl", gcp.DefaultCacheTTL, "With --gc, how long cached object headers and content are used before they are read again from Bigtable; above 0. The garbage collector's --grace must cover it and its --longest-push")
	fl.DurationVar(&c.gcTouchInterval, "gc-touch-interval", gcp.DefaultGCTouchInterval, "With --gc, length of the period in which the server records at most one use of each object and map row for the garbage collector; above 0 and at most half the garbage collector's --grace")
}

func (c *gcpCmd) Exec(ctx context.Context, fl *pflag.FlagSet) error {
	args.EnsureNoArgs(fl)
	return c.run(ctx)
}

func (c *gcpCmd) validate() error {
	return requireFlags(map[string]string{"--project": c.project, "--instance": c.instance, "--bucket": c.bucket})
}

func (c *gcpCmd) run(ctx context.Context) error {
	if err := c.validate(); err != nil {
		return err
	}
	server, err := gcp.New(ctx, &gcp.NewServerOpts{
		ProjectID:             c.project,
		InstanceID:            c.instance,
		DatabaseID:            c.database,
		BtInstance:            c.btInstance,
		BtAppProfile:          c.btAppProfile,
		BtConnPool:            c.btConnPool,
		BucketName:            c.bucket,
		MaxCachedHeaders:      c.maxCachedHeaders,
		MaxCachedContents:     c.maxCachedContents,
		NoLinkValidation:      c.noLinkValidation,
		NoHashValidation:      c.noHashValidation,
		ClientMetrics:         c.clientMetrics,
		LinksContent:          c.linksContent,
		LinksContentThreshold: c.linksContentThreshold,
		GC:                    c.gc,
		CacheTTL:              c.cacheTTL,
		GCTouchInterval:       c.gcTouchInterval,
	})
	if err != nil {
		return fmt.Errorf("failed to create the ABFS GCP server: %w", err)
	}
	defer func() {
		if err := server.Close(); err != nil {
			log.Printf("WARNING: error closing ABFS server: %v", err)
		}
	}()
	return c.Serve(ctx, gcp.NewFilterServer(server, nil))
}

type gcpInitCmd struct {
	*root.RootCmd
	opts gcp.SetupOpts
}

func (c *gcpInitCmd) Spec() cmd.CmdSpec {
	return cmd.CmdSpec{
		Name:  "init",
		Usage: "--project <project> --instance <instance> --bucket <bucket> [--db <db>] [--bt-instance <instance>] [--gc]",
		Desc: `Prepare the Spanner and Bigtable instances and the bucket of a GCP-backed ABFS server.
Creates the Spanner database, its schema and the Bigtable tables when they are missing and
checks that the bucket exists. The instances and the bucket must exist. Existing data is
kept, so it is safe to run again, for example after an upgrade. With --gc it also creates
the tables of the garbage collector.`,
	}
}

func (c *gcpInitCmd) RegisterFlags(fl *pflag.FlagSet) {
	fl.StringVar(&c.opts.ProjectID, "project", "", "The GCP Project which contains the databases (required)")
	fl.StringVar(&c.opts.InstanceID, "instance", "", "The GCP Spanner Database Instance (required)")
	fl.StringVar(&c.opts.DatabaseID, "db", defaultDatabase, "The GCP Spanner Database Name")
	fl.StringVar(&c.opts.BtInstance, "bt-instance", defaultBtInstance, "The GCP Bigtable Database Instance")
	fl.StringVar(&c.opts.BucketName, "bucket", "", "The GCS Bucket Name (required)")
	fl.BoolVar(&c.opts.GC, "gc", false, "Also create the garbage collector resources: the Bigtable table gc, the column family g of the table objects and the Spanner tables GcState, GcReplicas and GcServers")
}

func (c *gcpInitCmd) Exec(ctx context.Context, fl *pflag.FlagSet) error {
	args.EnsureNoArgs(fl)
	if err := requireFlags(map[string]string{"--project": c.opts.ProjectID, "--instance": c.opts.InstanceID, "--bucket": c.opts.BucketName}); err != nil {
		return err
	}
	if err := gcp.Setup(ctx, &c.opts); err != nil {
		return fmt.Errorf("failed to set up the GCP server resources: %w", err)
	}
	fmt.Println("The GCP server resources are ready.")
	return nil
}

func requireFlags(flags map[string]string) error {
	var missing []string
	for name, value := range flags {
		if value == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		slices.Sort(missing)
		return fmt.Errorf("missing required flag(s): %s", strings.Join(missing, ", "))
	}
	return nil
}

// StandaloneGCPServerCmd provides a standalone command runner for the GCP server with flat flags.
type StandaloneGCPServerCmd struct {
	serverCmd *serverCmd
	gcpCmd    *gcpCmd
}

func NewStandaloneGCPServerCmd(rootCmd *root.RootCmd) *StandaloneGCPServerCmd {
	sc := &serverCmd{RootCmd: rootCmd}
	gc := &gcpCmd{serverCmd: sc}
	return &StandaloneGCPServerCmd{
		serverCmd: sc,
		gcpCmd:    gc,
	}
}

func (s *StandaloneGCPServerCmd) RegisterFlags(fl *pflag.FlagSet) {
	s.serverCmd.RegisterFlags(fl)
	s.gcpCmd.RegisterFlags(fl)
}

func (s *StandaloneGCPServerCmd) Run(ctx context.Context) error {
	return s.gcpCmd.run(ctx)
}
