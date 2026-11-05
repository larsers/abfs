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
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/instance"
	"github.com/google/abfs/shared/remote"
	"github.com/google/abfs/shared/remote/servers/inmemory"
)

func TestHandleRemoteHeaders(t *testing.T) {
	require := require.New(t)

	tempDir := t.TempDir()
	cacheDir := filepath.Join(tempDir, "cache")
	logDir := filepath.Join(tempDir, "log")
	require.NoError(os.MkdirAll(filepath.Join(cacheDir, "tmp"), 0755))
	require.NoError(os.MkdirAll(logDir, 0755))

	trueVal := true
	falseVal := false
	inst := instance.NewInstance("default", tempDir, tempDir, "abfs", &instance.ConfigOptional{
		CacheDir:    &cacheDir,
		LogDir:      &logDir,
		LocalOnly:   &trueVal,
		EnableIndex: &falseVal,
	}, instance.DefaultFlavor)
	defer func() {
		require.NoError(inst.Close())
	}()

	c, err := New(inst, nil)
	require.NoError(err)
	defer func() {
		require.NoError(c.Stop())
	}()
	handler := c.httpHandler()

	post := func(keys []entities.Hash) (int, []bool) {
		b, err := json.Marshal(keys)
		require.NoError(err)
		req := httptest.NewRequest("POST", "http://localhost/remote-headers", bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		var found []bool
		if w.Code == http.StatusOK {
			require.NoError(json.NewDecoder(w.Body).Decode(&found))
		}
		return w.Code, found
	}

	missing := entities.NewHash("deadbeef")

	// A local-only cacheman has no server to ask.
	code, _ := post([]entities.Hash{entities.EmptyDirHash})
	require.Equal(http.StatusServiceUnavailable, code)

	mockRemote := inmemory.New()
	require.NoError(mockRemote.PutObject(context.Background(), entities.EmptyDirHash, remote.NewReaderFromBytes(entities.EmptyDirContent)))
	c.config.LocalOnly = false
	c.abfsClient = mockRemote

	code, found := post([]entities.Hash{entities.EmptyDirHash, missing})
	require.Equal(http.StatusOK, code)
	require.Equal([]bool{true, false}, found)

	code, found = post(nil)
	require.Equal(http.StatusOK, code)
	require.Empty(found)
}
