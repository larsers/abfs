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
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
)

const (
	serversTable = "GcServers"
	// MaxHeartbeatInterval is the longest heartbeat interval of a server with
	// GC.
	MaxHeartbeatInterval = 10 * time.Minute
	// ValidationWriteBound bounds the time from the start of a server's link
	// validation to the end of the last write that records the reference it
	// validated: a new object, ref or map result. It also bounds the time
	// from the start of a call to an answer that objects are present.
	ValidationWriteBound = 2 * time.Minute
	// HeartbeatTimeout is the deadline of the write of a heartbeat. A server
	// stops serving two heartbeat intervals and this long after its last
	// successful heartbeat.
	HeartbeatTimeout = 30 * time.Second
)

// Server is the heartbeat row of a server process in GcServers. It stays
// when the process stops, so that the collector can tell for any time window
// which processes ran, with which version of the GC rules and settings.
type Server struct {
	// ID is made by NewReplicaID.
	ID string
	// Version, GC, TouchInterval and CacheTTL are as in Replica.
	Version       int64
	GC            bool
	TouchInterval time.Duration
	CacheTTL      time.Duration
	// HeartbeatInterval is the time between two heartbeats.
	HeartbeatInterval time.Duration
	// LinkValidation reports whether the server validates the links of new
	// objects, refs and map results, which rescues the objects they link.
	LinkValidation bool
	// The fields below are set by ReadServers. LastSeen is the commit time
	// of the last heartbeat. TouchLossTime is the commit time of the first heartbeat or stop that
	// recorded an object touch the server lost, the newest one when there
	// were several, and StoppedAt the commit time of a clean stop.
	// MapRowLossTime is as TouchLossTime for the map row touches. The losses
	// are reported, not checked. All three are zero when unset.
	LastSeen       time.Time
	TouchLossTime  time.Time
	StoppedAt      time.Time
	MapRowLossTime time.Time
}

var serverColumns = []string{"ServerId", "Version", "GcEnabled", "TouchIntervalSeconds", "CacheTTLSeconds", "HeartbeatIntervalSeconds", "LinkValidation", "LastSeen", "TouchLossTime", "StoppedAt", "MapRowLossTime"}

// WriteServer writes the heartbeat of the server process s. lost records that
// the process lost object touches since its last heartbeat was written, and
// mapRowsLost that it lost map row touches.
func WriteServer(ctx context.Context, client *spanner.Client, s *Server, lost, mapRowsLost bool) error {
	cols := serverColumns[:8:8]
	vals := []any{s.ID, s.Version, s.GC, int64(s.TouchInterval / time.Second), int64(s.CacheTTL / time.Second),
		int64(s.HeartbeatInterval / time.Second), s.LinkValidation, spanner.CommitTimestamp}
	cols, vals = appendLosses(cols, vals, lost, mapRowsLost)
	if _, err := client.Apply(ctx, []*spanner.Mutation{spanner.InsertOrUpdate(serversTable, cols, vals)}); err != nil {
		return fmt.Errorf("failed to write the heartbeat of server %s: %w", s.ID, err)
	}
	return nil
}

// StopServer records that the server process id stopped after writing its
// buffered touches, and that it lost object or map row touches since its last
// heartbeat when lost or mapRowsLost is set. The row stays.
func StopServer(ctx context.Context, client *spanner.Client, id string, lost, mapRowsLost bool) error {
	cols, vals := appendLosses([]string{"ServerId", "StoppedAt"}, []any{id, spanner.CommitTimestamp}, lost, mapRowsLost)
	if _, err := client.Apply(ctx, []*spanner.Mutation{spanner.Update(serversTable, cols, vals)}); err != nil {
		return fmt.Errorf("failed to record the stop of server %s: %w", id, err)
	}
	return nil
}

// appendLosses adds the loss times that lost and mapRowsLost record to the
// columns and values of a write.
func appendLosses(cols []string, vals []any, lost, mapRowsLost bool) ([]string, []any) {
	if lost {
		cols = append(cols, "TouchLossTime")
		vals = append(vals, spanner.CommitTimestamp)
	}
	if mapRowsLost {
		cols = append(cols, "MapRowLossTime")
		vals = append(vals, spanner.CommitTimestamp)
	}
	return cols, vals
}

// ReadServers returns the rows of GcServers.
func ReadServers(ctx context.Context, client *spanner.Client) ([]Server, error) {
	servers, _, err := readServers(ctx, client)
	return servers, err
}

// readServers returns the rows of GcServers and the time of the read, a
// Spanner time like the times in the rows.
func readServers(ctx context.Context, client *spanner.Client) ([]Server, time.Time, error) {
	var servers []Server
	ro := client.Single()
	defer ro.Close()
	err := ro.Read(ctx, serversTable, spanner.AllKeys(), serverColumns).Do(func(row *spanner.Row) error {
		var s Server
		var touch, ttl, heartbeat int64
		var loss, stopped, mapRowLoss spanner.NullTime
		if err := row.Columns(&s.ID, &s.Version, &s.GC, &touch, &ttl, &heartbeat, &s.LinkValidation, &s.LastSeen, &loss, &stopped, &mapRowLoss); err != nil {
			return err
		}
		s.TouchInterval = time.Duration(touch) * time.Second
		s.CacheTTL = time.Duration(ttl) * time.Second
		s.HeartbeatInterval = time.Duration(heartbeat) * time.Second
		s.TouchLossTime, s.StoppedAt, s.MapRowLossTime = loss.Time, stopped.Time, mapRowLoss.Time
		servers = append(servers, s)
		return nil
	})
	var read time.Time
	if err == nil {
		read, err = ro.Timestamp()
	}
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("failed to read the GC servers: %w", err)
	}
	return servers, read, nil
}

// PruneServers deletes the rows of GcServers whose process neither wrote a
// heartbeat nor stopped since before, crashed ones included, and returns how
// many it deleted.
func PruneServers(ctx context.Context, client *spanner.Client, before time.Time) (int64, error) {
	var n int64
	_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		var err error
		n, err = txn.Update(ctx, spanner.Statement{
			SQL:    "DELETE FROM " + serversTable + " WHERE LastSeen < @before AND (StoppedAt IS NULL OR StoppedAt < @before)",
			Params: map[string]any{"before": before},
		})
		return err
	})
	if err != nil {
		return 0, fmt.Errorf("failed to delete the old GC servers: %w", err)
	}
	return n, nil
}
