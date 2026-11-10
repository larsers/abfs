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

package instance

import (
	"bytes"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestConfigGCKeepAlive(t *testing.T) {
	require := require.New(t)

	c := DefaultConfig(t.TempDir())
	require.False(c.GCKeepAlive)
	require.Equal(15*time.Minute, c.GCKeepAliveRefInterval)
	require.Equal(18*time.Hour, c.GCKeepAliveTouchInterval)

	var o ConfigOptional
	require.NoError(o.ApplyKeyValues([]string{"gc-keepalive=true", "gc-keepalive-touch-interval=6h"}))
	c.Apply(&o)
	require.True(c.GCKeepAlive)
	require.Equal(GCKeepAliveRefInterval, c.GCKeepAliveRefInterval)
	require.Equal(6*time.Hour, c.GCKeepAliveTouchInterval)

	// The ref interval is fixed, and the mount details record it.
	require.ErrorContains(o.ApplyKeyValues([]string{"gc-keepalive-ref-interval=2m"}), `invalid config key: "gc-keepalive-ref-interval"`)
	require.Equal(GCKeepAliveRefInterval, c.Clone().GCKeepAliveRefInterval)
}

// A deployment turns the keep-alive on for its clients in the central client
// configuration, which the clients resolve into their remote config.
func TestConfigGCKeepAlive_CentralConfig(t *testing.T) {
	require := require.New(t)

	var overrides ConfigRemoteOverrides
	require.NoError(yaml.Unmarshal([]byte("- select:\n  config:\n    gc-keepalive: true\n    gc-keepalive-touch-interval: 12h\n"), &overrides))
	remote := ResolveRemoteConfig(ClientInfo{User: "u", Class: "ci"}, &overrides)

	c := DefaultConfig(t.TempDir())
	c.Apply(remote)
	require.True(c.GCKeepAlive)
	require.Equal(12*time.Hour, c.GCKeepAliveTouchInterval)
	require.Equal(15*time.Minute, c.GCKeepAliveRefInterval)
}

// A config of an older client may still set gc-keepalive-ref-interval. It
// loads with its other keys, and the interval stays fixed.
func TestConfigGCKeepAlive_ObsoleteRefInterval(t *testing.T) {
	require := require.New(t)

	var logs bytes.Buffer
	log.SetOutput(&logs)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
	obsoleteLogged.Store(false)

	dir := t.TempDir()
	content := "remote-servers: [abfs-server:50051]\nauth-type: gcpsa\ngc-keepalive: true\ngc-keepalive-ref-interval: 2m\ngc-keepalive-touch-interval: 6h\n"
	require.NoError(os.WriteFile(filepath.Join(dir, "config"), []byte(content), 0o600))
	require.NoError(os.WriteFile(filepath.Join(dir, ".remote-config"), []byte("gc-keepalive-ref-interval: 5m\n"), 0o400))

	i := NewInstance("default", dir, dir, "abfs", nil, DefaultFlavor)
	require.Equal([]string{"abfs-server:50051"}, i.Config.RemoteServers)
	require.Equal(AuthTypeGCPSA, i.Config.AuthType)
	require.True(i.Config.GCKeepAlive)
	require.Equal(6*time.Hour, i.Config.GCKeepAliveTouchInterval)
	require.Equal(GCKeepAliveRefInterval, i.Config.GCKeepAliveRefInterval)
	require.Equal(1, strings.Count(logs.String(), "Ignoring config key gc-keepalive-ref-interval"), logs.String())

	// Writing the config back drops the key.
	var out bytes.Buffer
	require.NoError(i.LocalConfig.Encode(&out))
	require.NotContains(out.String(), "gc-keepalive-ref-interval")

	// The mount details record the fixed interval.
	details, err := json.Marshal(i.Config.Clone())
	require.NoError(err)
	require.Contains(string(details), `"GCKeepAliveRefInterval":900000000000`)

	// The central client configuration may set it as well.
	var overrides ConfigRemoteOverrides
	require.NoError(yaml.Unmarshal([]byte("- select:\n  config:\n    gc-keepalive: true\n    gc-keepalive-ref-interval: 2m\n"), &overrides))
	remote := ResolveRemoteConfig(ClientInfo{User: "u", Class: "ci"}, &overrides)
	out.Reset()
	require.NoError(remote.Encode(&out))
	require.Equal("gc-keepalive: true\n", out.String())
}
