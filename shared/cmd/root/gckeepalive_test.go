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

package root

import (
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"

	"github.com/google/abfs/shared/instance"
)

func TestConfigFromFlags_GCKeepAlive(t *testing.T) {
	require := require.New(t)

	fl := pflag.NewFlagSet("test", pflag.ContinueOnError)
	var overrides instance.ConfigOptional
	registerConfigFlags(fl, &overrides)

	require.NoError(fl.Parse([]string{"--gc-keepalive-ref-interval", "30m", "--gc-keepalive-touch-interval", "0"}))

	require.NotNil(overrides.GCKeepAliveRefInterval)
	require.Equal(30*time.Minute, *overrides.GCKeepAliveRefInterval)
	require.NotNil(overrides.GCKeepAliveTouchInterval)
	require.Zero(*overrides.GCKeepAliveTouchInterval)
}
