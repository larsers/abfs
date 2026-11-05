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
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"

	"cloud.google.com/go/spanner"
)

const replicasTable = "GcReplicas"

// NewReplicaID returns an ID for the heartbeat of this server process: the
// host name and a random suffix, so that a restarted process gets a new row.
func NewReplicaID() string {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return host + "-" + hex.EncodeToString(b)
}

// WriteHeartbeat records in the Spanner table GcReplicas that the server
// replica replicaID is running and writes gc rows of the given edge extractor
// version, 0 when it writes none.
func WriteHeartbeat(ctx context.Context, client *spanner.Client, replicaID string, version int64) error {
	m := spanner.InsertOrUpdate(replicasTable, []string{"ReplicaId", "Version", "LastSeen"},
		[]any{replicaID, version, spanner.CommitTimestamp})
	if _, err := client.Apply(ctx, []*spanner.Mutation{m}); err != nil {
		return fmt.Errorf("failed to write the heartbeat of replica %s: %w", replicaID, err)
	}
	return nil
}
