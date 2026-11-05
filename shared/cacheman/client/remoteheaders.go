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

package client

import (
	"github.com/google/abfs/cas/cas/providers/cacheman"
	"github.com/google/abfs/shared/entities"
)

// RemoteHeaders asks the remote server, through cacheman, for the headers of
// keys and returns how many of them the server has.
func (c *HttpClient) RemoteHeaders(keys []entities.Hash) (int, error) {
	var found int
	if err := c.postJSON("/remote-headers", keys, &found); err != nil {
		return 0, err
	}
	return found, nil
}

// RemoteHeaders asks the remote server, through cacheman, for the headers of
// keys and returns how many of them the server has.
func (c *Client) RemoteHeaders(keys []entities.Hash) (int, error) {
	if c.client.IsClosing() {
		return 0, cacheman.ErrClosedClient
	}
	return c.httpClient.RemoteHeaders(keys)
}
