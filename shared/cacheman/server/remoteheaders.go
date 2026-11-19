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
	"errors"
	"net/http"
	"time"

	"github.com/google/abfs/shared/entities"
)

// remoteHeadersTimeout is generous, since the server can take a while to rescue
// a batch of condemned objects.
var remoteHeadersTimeout = 10 * time.Minute

// handleRemoteHeaders returns for each key whether the remote server has it.
// Mounts use it to touch the objects they hold for the server GC.
func (c *CacheMan) handleRemoteHeaders(w http.ResponseWriter, r *http.Request) {
	var keys []entities.Hash
	if !c.ReadJSON(w, r, &keys) {
		return
	}
	abfsClient := c.remoteClient()
	if abfsClient == nil {
		c.ErrorJSON(w, r, errors.New("remote client not initialized"), http.StatusServiceUnavailable)
		return
	}
	if _, ok := abfsClient.(*localOnlyClient); ok {
		c.ErrorJSON(w, r, errors.New("cacheman is running in local-only mode"), http.StatusServiceUnavailable)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), remoteHeadersTimeout)
	defer cancel()
	hdrs, err := abfsClient.Headers(ctx, keys)
	if err != nil {
		c.ErrorJSON(w, r, err, http.StatusInternalServerError)
		return
	}
	found := make([]bool, len(hdrs))
	for i, hdr := range hdrs {
		found[i] = !hdr.IsEmpty()
	}
	if len(found) == len(keys) {
		c.checkRemoteHeaders(ctx, keys, found)
	}
	c.WriteJSON(w, r, found)
}

// remoteClient returns the ABFS client, which a config reload can replace.
func (c *CacheMan) remoteClient() ABFSClient {
	c.abfsClientLock.RLock()
	defer c.abfsClientLock.RUnlock()
	return c.abfsClient
}

func (c *CacheMan) setABFSClient(abfsClient ABFSClient) {
	c.abfsClientLock.Lock()
	defer c.abfsClientLock.Unlock()
	c.abfsClient = abfsClient
}
