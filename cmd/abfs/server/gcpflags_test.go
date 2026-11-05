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
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"

	"github.com/google/abfs/shared/cmd/root"
	"github.com/google/abfs/shared/remote/servers/gcp"
)

func TestGCPFlags_GCHooks(t *testing.T) {
	tests := []struct {
		args           []string
		ttl, touch, hb time.Duration
	}{
		{nil, gcp.DefaultCacheTTL, gcp.DefaultGCTouchInterval, gcp.DefaultGCHeartbeatInterval},
		{[]string{"--cache-ttl=0", "--gc-touch-interval=0", "--gc-heartbeat-interval=1m"}, 0, 0, time.Minute},
	}
	for _, tt := range tests {
		cmd := NewStandaloneGCPServerCmd(root.NewRootCommand())
		fl := pflag.NewFlagSet("abfs-server", pflag.ContinueOnError)
		cmd.RegisterFlags(fl)
		require.NoError(t, fl.Parse(tt.args))
		require.Equal(t, tt.ttl, cmd.gcpCmd.cacheTTL)
		require.Equal(t, tt.touch, cmd.gcpCmd.gcTouchInterval)
		require.Equal(t, tt.hb, cmd.gcpCmd.gcHeartbeatInterval)
	}
}
