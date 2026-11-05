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
	"time"

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

// Replica is the heartbeat row of a server replica in GcReplicas.
type Replica struct {
	ID string
	// Version is the GC edge extractor version the replica writes into the
	// table gc, 0 when it writes none.
	Version int64
	// GC reports whether the replica runs the garbage collector hooks.
	GC bool
	// TouchInterval and CacheTTL are the replica's touch period and cache TTL;
	// zero means it writes no touches or keeps cache entries without a TTL.
	TouchInterval time.Duration
	CacheTTL      time.Duration
	// LastSeen is the commit time of its last heartbeat; WriteHeartbeat
	// ignores it.
	LastSeen time.Time
}

var replicaColumns = []string{"ReplicaId", "Version", "GcEnabled", "TouchIntervalSeconds", "CacheTTLSeconds", "LastSeen"}

// WriteHeartbeat records in the Spanner table GcReplicas that the server
// replica r is running, with its settings.
func WriteHeartbeat(ctx context.Context, client *spanner.Client, r *Replica) error {
	m := spanner.InsertOrUpdate(replicasTable, replicaColumns,
		[]any{r.ID, r.Version, r.GC, int64(r.TouchInterval / time.Second), int64(r.CacheTTL / time.Second), spanner.CommitTimestamp})
	if _, err := client.Apply(ctx, []*spanner.Mutation{m}); err != nil {
		return fmt.Errorf("failed to write the heartbeat of replica %s: %w", r.ID, err)
	}
	return nil
}

// ReadReplicas returns the rows of GcReplicas.
func ReadReplicas(ctx context.Context, client *spanner.Client) ([]Replica, error) {
	var replicas []Replica
	err := client.Single().Read(ctx, replicasTable, spanner.AllKeys(), replicaColumns).Do(func(row *spanner.Row) error {
		var r Replica
		var touch, ttl int64
		if err := row.Columns(&r.ID, &r.Version, &r.GC, &touch, &ttl, &r.LastSeen); err != nil {
			return err
		}
		r.TouchInterval = time.Duration(touch) * time.Second
		r.CacheTTL = time.Duration(ttl) * time.Second
		replicas = append(replicas, r)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to read the GC replicas: %w", err)
	}
	return replicas, nil
}
