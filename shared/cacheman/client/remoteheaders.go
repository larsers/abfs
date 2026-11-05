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
	"fmt"

	"github.com/google/abfs/cas/cas/providers/cacheman"
	"github.com/google/abfs/shared/entities"
)

// RemoteHeaders asks the remote server, through cacheman, for the headers of
// keys and returns for each key whether the server has it.
func (c *HttpClient) RemoteHeaders(keys []entities.Hash) ([]bool, error) {
	var found []bool
	if err := c.postJSON("/remote-headers", keys, &found); err != nil {
		return nil, err
	}
	if len(found) != len(keys) {
		return nil, fmt.Errorf("cacheman answered %d remote headers for %d keys", len(found), len(keys))
	}
	return found, nil
}

// RemoteHeaders asks the remote server, through cacheman, for the headers of
// keys and returns for each key whether the server has it.
func (c *Client) RemoteHeaders(keys []entities.Hash) ([]bool, error) {
	if c.client.IsClosing() {
		return nil, cacheman.ErrClosedClient
	}
	return c.httpClient.RemoteHeaders(keys)
}
