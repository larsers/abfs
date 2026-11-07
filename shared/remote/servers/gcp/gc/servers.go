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
	// GC. The collector waits up to this long for a heartbeat that read the
	// touch stats after its snapshot.
	MaxHeartbeatInterval = 10 * time.Minute
)

// Server is the heartbeat row of a server process in GcServers. Unlike a
// row of GcReplicas it stays when the process stops, so that the collector
// can tell for any time window which processes ran and whether one of them
// lost touches, crashed or ran with settings that break the timing rules.
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
	// of the last heartbeat. StatsTime is a Spanner read time before the
	// server read the touch stats that its last heartbeat recorded.
	// TouchLossTime is the commit time of the first heartbeat or stop that
	// recorded a touch the server lost, the newest one when there were
	// several, and StoppedAt the commit time of a clean stop. Both are zero
	// when unset.
	LastSeen      time.Time
	StatsTime     time.Time
	TouchLossTime time.Time
	StoppedAt     time.Time
}

var serverColumns = []string{"ServerId", "Version", "GcEnabled", "TouchIntervalSeconds", "CacheTTLSeconds", "HeartbeatIntervalSeconds", "LinkValidation", "LastSeen", "StatsTime", "TouchLossTime", "StoppedAt"}

// ReadServerTime returns the read time of a strong read of the row of the
// server id in GcServers. Spanner chose it, so a read of the touch stats
// that starts after the call returns comes after it in real time, whatever
// the local clock says.
func ReadServerTime(ctx context.Context, client *spanner.Client, id string) (time.Time, error) {
	ro := client.Single()
	defer ro.Close()
	err := ro.Read(ctx, serversTable, spanner.Key{id}, []string{"ServerId"}).Do(func(*spanner.Row) error { return nil })
	if err == nil {
		var ts time.Time
		if ts, err = ro.Timestamp(); err == nil {
			return ts, nil
		}
	}
	return time.Time{}, fmt.Errorf("failed to read the Spanner time for the heartbeat of server %s: %w", id, err)
}

// WriteServer writes the heartbeat of the server process s, with statsTime
// from ReadServerTime. lost records that the process lost touches since its
// last heartbeat was written.
func WriteServer(ctx context.Context, client *spanner.Client, s *Server, statsTime time.Time, lost bool) error {
	cols := serverColumns[:9]
	vals := []any{s.ID, s.Version, s.GC, int64(s.TouchInterval / time.Second), int64(s.CacheTTL / time.Second),
		int64(s.HeartbeatInterval / time.Second), s.LinkValidation, spanner.CommitTimestamp, statsTime}
	if lost {
		cols = append(cols[:9:9], "TouchLossTime")
		vals = append(vals, spanner.CommitTimestamp)
	}
	if _, err := client.Apply(ctx, []*spanner.Mutation{spanner.InsertOrUpdate(serversTable, cols, vals)}); err != nil {
		return fmt.Errorf("failed to write the heartbeat of server %s: %w", s.ID, err)
	}
	return nil
}

// StopServer records that the server process id stopped after writing its
// buffered touches, and that it lost touches since its last heartbeat when
// lost is set. The row stays.
func StopServer(ctx context.Context, client *spanner.Client, id string, lost bool) error {
	cols := []string{"ServerId", "StoppedAt"}
	vals := []any{id, spanner.CommitTimestamp}
	if lost {
		cols = append(cols, "TouchLossTime")
		vals = append(vals, spanner.CommitTimestamp)
	}
	if _, err := client.Apply(ctx, []*spanner.Mutation{spanner.Update(serversTable, cols, vals)}); err != nil {
		return fmt.Errorf("failed to record the stop of server %s: %w", id, err)
	}
	return nil
}

// ReadServers returns the rows of GcServers.
func ReadServers(ctx context.Context, client *spanner.Client) ([]Server, error) {
	var servers []Server
	err := client.Single().Read(ctx, serversTable, spanner.AllKeys(), serverColumns).Do(func(row *spanner.Row) error {
		var s Server
		var touch, ttl, heartbeat int64
		var loss, stopped spanner.NullTime
		if err := row.Columns(&s.ID, &s.Version, &s.GC, &touch, &ttl, &heartbeat, &s.LinkValidation, &s.LastSeen, &s.StatsTime, &loss, &stopped); err != nil {
			return err
		}
		s.TouchInterval = time.Duration(touch) * time.Second
		s.CacheTTL = time.Duration(ttl) * time.Second
		s.HeartbeatInterval = time.Duration(heartbeat) * time.Second
		s.TouchLossTime, s.StoppedAt = loss.Time, stopped.Time
		servers = append(servers, s)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to read the GC servers: %w", err)
	}
	return servers, nil
}
