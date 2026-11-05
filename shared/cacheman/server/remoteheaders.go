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

// remoteHeadersTimeout bounds one request. A batch of condemned objects takes
// the server a while to rescue, so it is generous; it only keeps a server that
// does not answer from holding the keep-alive forever.
var remoteHeadersTimeout = 10 * time.Minute

// handleRemoteHeaders asks the remote server for the headers of the given keys
// and returns for each key whether the server has it. Mounts use it to touch
// the objects they hold, so the server GC sees them as in use.
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

	ctx, cancel := context.WithTimeout(r.Context(), remoteHeadersTimeout)
	defer cancel()
	hdrs, err := c.abfsClient.Headers(ctx, keys)
	if err != nil {
		c.ErrorJSON(w, r, err, http.StatusInternalServerError)
		return
	}
	found := make([]bool, len(hdrs))
	for i, hdr := range hdrs {
		found[i] = !hdr.IsEmpty()
	}
	c.WriteJSON(w, r, found)
}
