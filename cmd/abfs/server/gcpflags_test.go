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
		args       []string
		gc         bool
		ttl, touch time.Duration
	}{
		{nil, false, gcp.DefaultCacheTTL, gcp.DefaultGCTouchInterval},
		{[]string{"--gc", "--cache-ttl=5m", "--gc-touch-interval=30m"}, true, 5 * time.Minute, 30 * time.Minute},
	}
	for _, tt := range tests {
		cmd := NewStandaloneGCPServerCmd(root.NewRootCommand())
		fl := pflag.NewFlagSet("abfs-server", pflag.ContinueOnError)
		cmd.RegisterFlags(fl)
		require.NoError(t, fl.Parse(tt.args))
		require.Equal(t, tt.gc, cmd.gcpCmd.gc)
		require.Equal(t, tt.ttl, cmd.gcpCmd.cacheTTL)
		require.Equal(t, tt.touch, cmd.gcpCmd.gcTouchInterval)
	}
	// The heartbeat interval is fixed.
	cmd := NewStandaloneGCPServerCmd(root.NewRootCommand())
	fl := pflag.NewFlagSet("abfs-server", pflag.ContinueOnError)
	cmd.RegisterFlags(fl)
	require.ErrorContains(t, fl.Parse([]string{"--gc-heartbeat-interval=1m"}), "unknown flag: --gc-heartbeat-interval")
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
// number or range of server hosts. The other modes reject the flags only
// delete mode reads.
func TestGCPGCRunFlags_Modes(t *testing.T) {
	tests := []struct {
		args    []string
		wantErr string
	}{
		{nil, "pass one of --dry-run, --condemn and --delete"},
		{[]string{"--dry-run", "--condemn", "--expected-replicas=3"}, "pass one of --dry-run, --condemn and --delete"},
		{[]string{"--dry-run"}, ""},
		{[]string{"--condemn"}, "invalid expected replicas 0"},
		{[]string{"--dry-run", "--explain=nothex"}, `invalid key "nothex"`},
		{[]string{"--condemn", "--expected-replicas=3", "--explain=" + strings.Repeat("ab", 20)}, ""},
		// The grace must exceed the longest push, and the mount idle time
		// four rewrites of a mount ref.
		{[]string{"--dry-run", "--grace=20m", "--longest-push=20m"}, "grace 20m0s is not longer than the longest push 20m0s"},
		{[]string{"--dry-run", "--grace=90m", "--longest-push=20m"}, ""},
		{[]string{"--dry-run", "--mount-idle=30m"}, "mount idle time 30m0s is shorter than 1h0m0s"},
		{[]string{"--dry-run", "--mount-idle=1h"}, ""},
		// An autoscaled fleet gives a range instead of the exact count.
		{[]string{"--condemn", "--min-replicas=2"}, ""},
		{[]string{"--condemn", "--min-replicas=2", "--max-replicas=11"}, ""},
		{[]string{"--condemn", "--max-replicas=11"}, "invalid expected replicas 0"},
		{[]string{"--condemn", "--min-replicas=3", "--max-replicas=2"}, "is below min replicas 3"},
		{[]string{"--condemn", "--expected-replicas=3", "--min-replicas=2"}, "expected replicas excludes min and max replicas"},
		{[]string{"--condemn", "--expected-replicas=3", "--max-replicas=4"}, "expected replicas excludes min and max replicas"},
		{[]string{"--condemn", "--min-replicas=-1"}, "cannot be negative"},
		// The flags of delete mode have no effect in the other modes.
		{[]string{"--condemn", "--min-replicas=2", "--no-backup-check"}, "only delete mode takes --no-backup-check"},
		{[]string{"--dry-run", "--accept-mounts-without-keepalive"}, "only delete mode takes --accept-mounts-without-keepalive"},
		{[]string{"--dry-run", "--delete-types=commit"}, "only delete mode takes --delete-types"},
		{[]string{"--condemn", "--min-replicas=2", "--delete-start-delay=5m"}, "only delete mode takes --delete-start-delay"},
		{[]string{"--condemn", "--min-replicas=2", "--allow-no-soft-delete"}, "only delete mode takes --allow-no-soft-delete"},
		// Delete mode implies the condemn and takes its checks.
		{[]string{"--delete"}, "invalid expected replicas 0"},
		{[]string{"--delete", "--condemn", "--min-replicas=2"}, "pass one of --dry-run, --condemn and --delete"},
		{[]string{"--delete", "--dry-run", "--min-replicas=2"}, "pass one of --dry-run, --condemn and --delete"},
		{[]string{"--delete", "--min-replicas=2"}, ""},
		{[]string{"--delete", "--min-replicas=2", "--delete-types=commit,blob", "--no-backup-check", "--accept-mounts-without-keepalive", "--delete-start-delay=5m"}, ""},
		{[]string{"--delete", "--min-replicas=2", "--delete-types=commits"}, `invalid object type "commits"`},
		{[]string{"--delete", "--min-replicas=2", "--chunk-delay=1h"}, "unknown flag: --chunk-delay"},
	}
	for _, tt := range tests {
		c := &gcpGCRunCmd{RootCmd: root.NewRootCommand()}
		fl := pflag.NewFlagSet("run", pflag.ContinueOnError)
		c.RegisterFlags(fl)
		err := fl.Parse(tt.args)
		if err == nil {
			err = c.checkOpts(fl)
		}
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
	require.Equal(t, 15*time.Minute, c.opts.ChunkDelay)
	require.Equal(t, float64(500), c.opts.ChunkRate)
	require.Equal(t, 16, c.opts.Shards)
	require.Equal(t, 5*time.Minute, c.opts.LeaseDuration)
	require.Equal(t, 30*time.Second, c.opts.WriteDeadline)
	require.True(t, c.opts.Consistency)
	require.False(t, c.opts.AllowNoSoftDelete)
	require.Zero(t, c.opts.StartDelay)
	// The pause before the delete step is for tests and not in the help.
	require.True(t, fl.Lookup("delete-start-delay").Hidden)
	require.NotContains(t, fl.FlagUsages(), "delete-start-delay")
}

// The server and every GC command take --gcp-client-metrics, off by default.
func TestGCPGCFlags_ClientMetrics(t *testing.T) {
	run := &gcpGCRunCmd{RootCmd: root.NewRootCommand()}
	backfill := &gcpGCBackfillCmd{RootCmd: root.NewRootCommand()}
	undo := &gcpGCUndoCmd{RootCmd: root.NewRootCommand()}
	walk := &gcpGCWalkCmd{RootCmd: root.NewRootCommand()}
	server := NewStandaloneGCPServerCmd(root.NewRootCommand())
	for _, tt := range []struct {
		register func(*pflag.FlagSet)
		got      *bool
	}{
		{run.RegisterFlags, &run.server.ClientMetrics},
		{backfill.RegisterFlags, &backfill.server.ClientMetrics},
		{undo.RegisterFlags, &undo.server.ClientMetrics},
		{walk.RegisterFlags, &walk.server.ClientMetrics},
		{server.RegisterFlags, &server.gcpCmd.clientMetrics},
	} {
		fl := pflag.NewFlagSet("gc", pflag.ContinueOnError)
		tt.register(fl)
		require.NoError(t, fl.Parse(nil))
		require.False(t, *tt.got)
		require.NoError(t, fl.Parse([]string{"--gcp-client-metrics"}))
		require.True(t, *tt.got)
	}
}

// gc undo needs the first cycle and the suffix of the restored tables, and
// takes a range of cycles in order. Its workers, lease and write deadline are
// fixed.
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
		require.Equal(t, gc.DefaultUndoWorkers, c.opts.Workers)
		require.Equal(t, gc.DefaultLeaseDuration, c.opts.LeaseDuration)
		require.Equal(t, gc.DefaultWriteDeadline, c.opts.WriteDeadline)
	}
}
