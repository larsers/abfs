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

package gc

import (
	"crypto/rand"
	"encoding/hex"
	"os"
)

// NewReplicaID returns an ID for the heartbeat of this server process: the
// host name and a random suffix, so that a restarted process gets a new row
// and the row of a process that stopped without recording its stop stays.
func NewReplicaID() string {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return host + "-" + hex.EncodeToString(b)
}
