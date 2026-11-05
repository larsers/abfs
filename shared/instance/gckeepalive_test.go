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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConfigGCKeepAlive(t *testing.T) {
	require := require.New(t)

	c := DefaultConfig(t.TempDir())
	require.Equal(15*time.Minute, c.GCKeepAliveRefInterval)
	require.Equal(18*time.Hour, c.GCKeepAliveTouchInterval)

	var o ConfigOptional
	require.NoError(o.ApplyKeyValues([]string{"gc-keepalive-ref-interval=0s", "gc-keepalive-touch-interval=6h"}))
	c.Apply(&o)
	require.Zero(c.GCKeepAliveRefInterval)
	require.Equal(6*time.Hour, c.GCKeepAliveTouchInterval)
}
