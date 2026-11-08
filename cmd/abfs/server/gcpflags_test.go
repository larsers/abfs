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
	"strings"
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"

	"github.com/google/abfs/shared/cmd/root"
	"github.com/google/abfs/shared/remote/servers/gcp"
	"github.com/google/abfs/shared/remote/servers/gcp/gc"
)

func TestGCPFlags_GCHooks(t *testing.T) {
	tests := []struct {
		args           []string
		gc             bool
		ttl, touch, hb time.Duration
	}{
		{nil, false, gcp.DefaultCacheTTL, gcp.DefaultGCTouchInterval, gcp.DefaultGCHeartbeatInterval},
		{[]string{"--gc", "--cache-ttl=0", "--gc-touch-interval=0", "--gc-heartbeat-interval=1m"}, true, 0, 0, time.Minute},
	}
	for _, tt := range tests {
		cmd := NewStandaloneGCPServerCmd(root.NewRootCommand())
		fl := pflag.NewFlagSet("abfs-server", pflag.ContinueOnError)
		cmd.RegisterFlags(fl)
		require.NoError(t, fl.Parse(tt.args))
		require.Equal(t, tt.gc, cmd.gcpCmd.gc)
		require.Equal(t, tt.ttl, cmd.gcpCmd.cacheTTL)
		require.Equal(t, tt.touch, cmd.gcpCmd.gcTouchInterval)
		require.Equal(t, tt.hb, cmd.gcpCmd.gcHeartbeatInterval)
	}
}

func TestGCPInitFlags_GC(t *testing.T) {
	for _, tt := range []struct {
		args []string
		gc   bool
	}{{nil, false}, {[]string{"--gc"}, true}} {
		cmd := &gcpInitCmd{RootCmd: root.NewRootCommand()}
		fl := pflag.NewFlagSet("init", pflag.ContinueOnError)
		cmd.RegisterFlags(fl)
		require.NoError(t, fl.Parse(tt.args))
		require.Equal(t, tt.gc, cmd.opts.GC)
	}
}

// gc run takes exactly one mode, and --condemn and --delete need the expected
// number or range of server hosts and the consistency check.
func TestGCPGCRunFlags_Modes(t *testing.T) {
	tests := []struct {
		args    []string
		wantErr string
	}{
		{nil, "pass one of --dry-run, --condemn and --delete"},
		{[]string{"--dry-run", "--condemn", "--expected-replicas=3"}, "pass one of --dry-run, --condemn and --delete"},
		{[]string{"--dry-run"}, ""},
		{[]string{"--condemn"}, "invalid expected replicas 0"},
		{[]string{"--condemn", "--expected-replicas=3", "--consistency=false"}, "needs the consistency check"},
		{[]string{"--condemn", "--expected-replicas=3", "--keep-lists=0"}, "invalid number of kept lists"},
		{[]string{"--dry-run", "--explain=nothex"}, `invalid key "nothex"`},
		{[]string{"--dry-run", "--write-deadline=38s"}, "two attempts must fit into a quarter of it"},
		{[]string{"--dry-run", "--write-deadline=37s"}, ""},
		{[]string{"--condemn", "--expected-replicas=3", "--condemn-rate=100", "--explain=" + strings.Repeat("ab", 20)}, ""},
		// An autoscaled fleet gives a range instead of the exact count.
		{[]string{"--condemn", "--min-replicas=2"}, ""},
		{[]string{"--condemn", "--min-replicas=2", "--max-replicas=11"}, ""},
		{[]string{"--condemn", "--max-replicas=11"}, "invalid expected replicas 0"},
		{[]string{"--condemn", "--min-replicas=3", "--max-replicas=2"}, "is below min replicas 3"},
		{[]string{"--condemn", "--expected-replicas=3", "--min-replicas=2"}, "expected replicas excludes min and max replicas"},
		{[]string{"--condemn", "--expected-replicas=3", "--max-replicas=4"}, "expected replicas excludes min and max replicas"},
		{[]string{"--condemn", "--min-replicas=-1"}, "cannot be negative"},
		// The checks of delete mode have no effect in the other modes.
		{[]string{"--condemn", "--min-replicas=2", "--no-backup-check"}, "apply only to delete mode"},
		{[]string{"--dry-run", "--accept-mounts-without-keepalive"}, "apply only to delete mode"},
		{[]string{"--dry-run", "--delete-types=commit"}, "apply only to delete mode"},
		{[]string{"--condemn", "--min-replicas=2", "--delete-start-delay=5m"}, "apply only to delete mode"},
		{[]string{"--condemn", "--min-replicas=2", "--allow-no-soft-delete"}, "apply only to delete mode"},
		// Delete mode implies the condemn and takes its checks.
		{[]string{"--delete"}, "invalid expected replicas 0"},
		{[]string{"--delete", "--condemn", "--min-replicas=2"}, "pass one of --dry-run, --condemn and --delete"},
		{[]string{"--delete", "--dry-run", "--min-replicas=2"}, "pass one of --dry-run, --condemn and --delete"},
		{[]string{"--delete", "--min-replicas=2"}, ""},
		{[]string{"--delete", "--min-replicas=2", "--delete-types=commit,blob", "--no-backup-check", "--accept-mounts-without-keepalive", "--delete-start-delay=5m"}, ""},
		{[]string{"--delete", "--min-replicas=2", "--delete-types=commits"}, `invalid object type "commits"`},
		{[]string{"--delete", "--min-replicas=2", "--chunk-delay=10m"}, "not longer than the 10m0s lifetime of the signed URLs"},
		{[]string{"--delete", "--min-replicas=2", "--delete-workers=0"}, "invalid number of delete workers 0"},
		{[]string{"--delete", "--min-replicas=2", "--delete-rate=-1"}, "invalid delete rate"},
		{[]string{"--delete", "--min-replicas=2", "--chunk-rate=-1"}, "invalid chunk delete rate"},
		{[]string{"--delete", "--min-replicas=2", "--chunk-rate=0", "--allow-no-soft-delete"}, ""},
		{[]string{"--delete", "--expected-replicas=3", "--consistency=false"}, "needs the consistency check"},
	}
	for _, tt := range tests {
		c := &gcpGCRunCmd{RootCmd: root.NewRootCommand()}
		fl := pflag.NewFlagSet("run", pflag.ContinueOnError)
		c.RegisterFlags(fl)
		require.NoError(t, fl.Parse(tt.args))
		err := c.checkOpts()
		if tt.wantErr != "" {
			require.ErrorContains(t, err, tt.wantErr, tt.args)
			continue
		}
		require.NoError(t, err, tt.args)
	}
	c := &gcpGCRunCmd{RootCmd: root.NewRootCommand()}
	fl := pflag.NewFlagSet("run", pflag.ContinueOnError)
	c.RegisterFlags(fl)
	require.NoError(t, fl.Parse([]string{"--condemn", "--expected-replicas=3"}))
	require.Equal(t, 3, c.opts.ExpectedReplicas)
	require.Equal(t, float64(gc.DefaultCondemnRate), c.opts.Rate)
	require.Equal(t, gc.DefaultKeepLists, c.opts.KeepLists)
	require.Equal(t, float64(2000), c.opts.DeleteRate)
	require.Equal(t, 32, c.opts.DeleteWorkers)
	require.Equal(t, time.Hour, c.opts.ChunkDelay)
	require.Equal(t, float64(500), c.opts.ChunkRate)
	require.False(t, c.opts.AllowNoSoftDelete)
	require.Zero(t, c.opts.StartDelay)
}

// The GC commands export the client library metrics by default, which show
// the per-attempt latencies and retries of their app profile; the server
// does not.
func TestGCPGCFlags_ClientMetrics(t *testing.T) {
	run := &gcpGCRunCmd{RootCmd: root.NewRootCommand()}
	backfill := &gcpGCBackfillCmd{RootCmd: root.NewRootCommand()}
	undo := &gcpGCUndoCmd{RootCmd: root.NewRootCommand()}
	server := NewStandaloneGCPServerCmd(root.NewRootCommand())
	for _, tt := range []struct {
		register func(*pflag.FlagSet)
		got      *bool
		want     bool
	}{
		{run.RegisterFlags, &run.server.ClientMetrics, true},
		{backfill.RegisterFlags, &backfill.server.ClientMetrics, true},
		{undo.RegisterFlags, &undo.server.ClientMetrics, true},
		{server.RegisterFlags, &server.gcpCmd.clientMetrics, false},
	} {
		fl := pflag.NewFlagSet("gc", pflag.ContinueOnError)
		tt.register(fl)
		require.NoError(t, fl.Parse(nil))
		require.Equal(t, tt.want, *tt.got)
	}
}

// gc undo needs the first cycle and the suffix of the restored tables, takes
// a range of cycles in order, and bounds its write attempts by the lease as
// gc run does.
func TestGCPGCUndoFlags(t *testing.T) {
	tests := []struct {
		args    []string
		wantErr string
	}{
		{nil, "pass --cycle and --from-suffix"},
		{[]string{"--cycle=2"}, "pass --cycle and --from-suffix"},
		{[]string{"--from-suffix=-r"}, "pass --cycle and --from-suffix"},
		{[]string{"--cycle=2", "--from-suffix=-r"}, ""},
		{[]string{"--cycle=2", "--through=4", "--from-suffix=-r"}, ""},
		{[]string{"--cycle=3", "--through=2", "--from-suffix=-r"}, "the last cycle to undo, 2, is before the first, 3"},
		{[]string{"--cycle=-1", "--from-suffix=-r"}, "invalid cycle -1"},
		{[]string{"--cycle=2", "--from-suffix=a/b"}, `invalid suffix "a/b"`},
		{[]string{"--cycle=2", "--from-suffix=-r", "--workers=0"}, "invalid number of workers 0"},
		{[]string{"--cycle=2", "--from-suffix=-r", "--write-deadline=40s"}, "write deadline 40s is too long for the lease 5m0s"},
	}
	for _, tt := range tests {
		c := &gcpGCUndoCmd{RootCmd: root.NewRootCommand()}
		fl := pflag.NewFlagSet("undo", pflag.ContinueOnError)
		c.RegisterFlags(fl)
		require.NoError(t, fl.Parse(tt.args))
		err := c.checkOpts()
		if tt.wantErr != "" {
			require.ErrorContains(t, err, tt.wantErr, tt.args)
			continue
		}
		require.NoError(t, err, tt.args)
	}
}
