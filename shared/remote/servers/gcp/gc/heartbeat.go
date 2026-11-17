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
	"time"
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

// Replica is what checkReplicas checks of the heartbeat row of a server
// process in GcServers.
type Replica struct {
	ID string
	// Version is the version of the GC rules the process follows, which it
	// also writes into the table gc, 0 when it writes none.
	Version int64
	// GC reports whether the process runs the garbage collector hooks.
	GC bool
	// TouchInterval and CacheTTL are the process's touch period and cache
	// TTL; zero means it writes no touches or keeps cache entries without a
	// TTL. HeartbeatInterval is the time between two of its heartbeats.
	TouchInterval     time.Duration
	CacheTTL          time.Duration
	HeartbeatInterval time.Duration
	// LastSeen is the commit time of its last heartbeat.
	LastSeen time.Time
}
