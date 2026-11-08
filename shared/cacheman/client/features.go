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
)

// Features returns the optional features cacheman implements. An older cacheman lists none.
func (c *HttpClient) Features() ([]string, error) {
	var resp struct {
		Features []string `json:"features"`
	}
	if err := c.getJSON("/interface-version", &resp); err != nil {
		return nil, err
	}
	return resp.Features, nil
}

// Features returns the optional features cacheman implements. An older cacheman lists none.
func (c *Client) Features() ([]string, error) {
	if c.client.IsClosing() {
		return nil, cacheman.ErrClosedClient
	}
	return c.httpClient.Features()
}
