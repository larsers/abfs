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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/google/abfs/shared/cacheman/protocol"
)

func TestCacheman_LocalWriteCheckFeature(t *testing.T) {
	ctx := context.Background()
	features := func(t *testing.T, c *CacheMan) []string {
		req := httptest.NewRequest("GET", "http://localhost/interface-version", nil)
		w := httptest.NewRecorder()
		c.httpHandler().ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code)
		var resp struct {
			Features []string `json:"features"`
		}
		require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
		return resp.Features
	}

	c := newCheckCacheman(t, newCheckRemote(), checkOpts{gcKeepAlive: false})
	require.Empty(t, features(t, c))

	c = newCheckCacheman(t, newCheckRemote(), checkOpts{gcKeepAlive: true})
	c.startLocalWriteCheck(ctx)
	require.Equal(t, []string{protocol.FeatureGCLocalWriteCheck}, features(t, c))

	// A reload that turns gc-keepalive off keeps the check until the next start.
	off := *c.config
	off.GCKeepAlive = false
	c.applyLocalWriteCheck(&off)
	require.Equal(t, []string{protocol.FeatureGCLocalWriteCheck}, features(t, c))
}
