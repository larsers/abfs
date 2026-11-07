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

// gc run takes exactly one mode, and --condemn needs the expected number of
// server hosts and the consistency check.
func TestGCPGCRunFlags_Modes(t *testing.T) {
	tests := []struct {
		args    []string
		wantErr string
	}{
		{nil, "pass one of --dry-run and --condemn"},
		{[]string{"--dry-run", "--condemn", "--expected-replicas=3"}, "pass one of --dry-run and --condemn"},
		{[]string{"--dry-run"}, ""},
		{[]string{"--condemn"}, "invalid expected replicas 0"},
		{[]string{"--condemn", "--expected-replicas=3", "--consistency=false"}, "needs the consistency check"},
		{[]string{"--condemn", "--expected-replicas=3", "--keep-lists=0"}, "invalid number of kept lists"},
		{[]string{"--dry-run", "--explain=nothex"}, `invalid key "nothex"`},
		{[]string{"--condemn", "--expected-replicas=3", "--condemn-rate=100", "--explain=" + strings.Repeat("ab", 20)}, ""},
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
}
