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
	"errors"
	"net/http"

	"github.com/google/abfs/shared/entities"
)

// handleRemoteHeaders asks the remote server for the headers of the given keys
// and returns how many of them the server has. Mounts use it to touch the
// objects they hold, so the server GC sees them as in use.
func (c *CacheMan) handleRemoteHeaders(w http.ResponseWriter, r *http.Request) {
	var keys []entities.Hash
	if !c.ReadJSON(w, r, &keys) {
		return
	}
	if c.abfsClient == nil {
		c.ErrorJSON(w, r, errors.New("remote client not initialized"), http.StatusServiceUnavailable)
		return
	}
	if _, ok := c.abfsClient.(*localOnlyClient); ok {
		c.ErrorJSON(w, r, errors.New("cacheman is running in local-only mode"), http.StatusServiceUnavailable)
		return
	}

	hdrs, err := c.abfsClient.Headers(r.Context(), keys)
	if err != nil {
		c.ErrorJSON(w, r, err, http.StatusInternalServerError)
		return
	}
	found := 0
	for _, hdr := range hdrs {
		if !hdr.IsEmpty() {
			found++
		}
	}
	c.WriteJSON(w, r, found)
}
