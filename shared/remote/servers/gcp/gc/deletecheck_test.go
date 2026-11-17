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
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// deleteCase is a cycle whose every delete precondition holds, at the
// production timings, unless a test changes it.
type deleteCase struct {
	r  *Report
	ev *deleteEvidence
	o  *DeleteOpts
}

func newDeleteCase() *deleteCase {
	snapshot := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	listSnapshot := snapshot.Add(-96 * time.Hour)
	complete := listSnapshot.Add(time.Hour)
	o := &DeleteOpts{CondemnOpts: DefaultCondemnOpts()}
	o.MinReplicas = 2
	var servers []Server
	for _, id := range []string{"srv-0-aa", "srv-1-bb", "srv-2-cc"} {
		servers = append(servers, liveServer(id, snapshot))
	}
	var backups []Backup
	for _, table := range backupTables {
		backups = append(backups, Backup{Name: table + "-1", Table: table, Start: snapshot.Add(-time.Minute), Ready: true})
	}
	return &deleteCase{
		r: &Report{SnapshotTime: snapshot, Consistency: ConsistencyReport{Ran: true}},
		ev: &deleteEvidence{
			snapshot: snapshot,
			holds:    &listHolds{records: map[int64]*ListRecord{4: {Cycle: 4, SnapshotTime: listSnapshot, CompleteTime: complete, CacheTTL: 6 * time.Hour}}},
			servers:  &serverEvidence{servers: servers, readTime: snapshot},
			instance: instanceEvidence{clusters: []string{"c1"}, backups: backups, backupsChecked: true},
		},
		o: o,
	}
}

func liveServer(id string, now time.Time) Server {
	return Server{
		ID: id, Version: ExtractorVersion, GC: true, TouchInterval: 36 * time.Hour, CacheTTL: 6 * time.Hour,
		HeartbeatInterval: 5 * time.Minute, LinkValidation: true, LastSeen: now.Add(-time.Minute),
	}
}

// stoppedServer is a server that stopped cleanly at stopped.
func stoppedServer(id string, stopped time.Time) Server {
	s := liveServer(id, stopped)
	s.LastSeen, s.StoppedAt = stopped, stopped
	return s
}

func (c *deleteCase) addServer(s Server) {
	c.ev.servers.servers = append(c.ev.servers.servers, s)
}

func (c *deleteCase) setBackup(table string, change func(b *Backup)) {
	for i := range c.ev.instance.backups {
		if c.ev.instance.backups[i].Table == table {
			change(&c.ev.instance.backups[i])
		}
	}
}

func liveMounts(count string, n int64) func(c *deleteCase) {
	return func(c *deleteCase) {
		m := &c.r.LiveMounts
		m.Live = n
		switch count {
		case "without_keepalive":
			m.WithoutKeepAlive = n
		case "without_local_write_check":
			m.WithoutLocalWriteCheck = n
		case "touch_interval_too_long":
			m.TouchIntervalTooLong = n
		}
		m.Samples = map[string][]MountSample{count: {{Details: "d1", Host: "host-a", User: "u"}}}
	}
}

// Each delete precondition stops the delete on its own, and the conditions
// that keep the evidence intact do not.
func TestDeleteBlockers(t *testing.T) {
	tests := []struct {
		name         string
		change       func(c *deleteCase)
		want         []string
		wantWarnings []string
	}{
		{"all hold", func(c *deleteCase) {}, nil, nil},

		// The list records; which lists are eligible is the plan's.
		{"list records not read", func(c *deleteCase) { c.ev.holds = &listHolds{err: errors.New("table not found")} }, []string{"the GC list records could not be read"}, nil},
		{"no complete list", func(c *deleteCase) { c.ev.holds = &listHolds{records: map[int64]*ListRecord{}} }, nil, nil},

		// The processes of the window. A crash or a lost touch refuses
		// nothing: the delete rule rests on no buffered touch.
		{"crashed predecessor within the grace", func(c *deleteCase) {
			s := liveServer("srv-0-old", c.ev.snapshot)
			s.LastSeen = c.ev.snapshot.Add(-10 * time.Hour)
			c.addServer(s)
		}, nil, nil},
		{"clean stop with losses 2 h ago", func(c *deleteCase) {
			s := stoppedServer("srv-0-old", c.ev.snapshot.Add(-2*time.Hour))
			s.TouchLossTime = s.StoppedAt
			c.addServer(s)
		}, nil, nil},
		{"live server that lost touches and map row touches", func(c *deleteCase) {
			c.ev.servers.servers[1].TouchLossTime = c.ev.snapshot.Add(-time.Hour)
			c.ev.servers.servers[1].MapRowLossTime = c.ev.snapshot.Add(-time.Hour)
		}, nil, nil},
		{"server of an older version within the grace", func(c *deleteCase) {
			s := stoppedServer("srv-0-old", c.ev.snapshot.Add(-2*time.Hour))
			s.Version = ExtractorVersion - 1
			c.addServer(s)
		}, []string{"server srv-0-old: follows the GC rules of version 2, older than version 3 of this collector"}, nil},
		{"server of an older version before the window", func(c *deleteCase) {
			s := stoppedServer("srv-0-old", c.ev.snapshot.Add(-73*time.Hour))
			s.Version = ExtractorVersion - 1
			c.addServer(s)
		}, nil, nil},
		{"stopped server of the window with touch interval 0", func(c *deleteCase) {
			s := stoppedServer("srv-0-old", c.ev.snapshot.Add(-2*time.Hour))
			s.TouchInterval = 0
			c.addServer(s)
		}, []string{"server srv-0-old: writes no touches"}, nil},
		{"stopped server of the window with TTL 0", func(c *deleteCase) {
			s := stoppedServer("srv-0-old", c.ev.snapshot.Add(-2*time.Hour))
			s.CacheTTL = 0
			c.addServer(s)
		}, []string{"server srv-0-old: keeps cache entries without a TTL"}, nil},
		{"stopped server of the window with a TTL the grace does not cover", func(c *deleteCase) {
			s := stoppedServer("srv-0-old", c.ev.snapshot.Add(-2*time.Hour))
			s.CacheTTL = 70 * time.Hour
			c.addServer(s)
		}, []string{"grace 72h0m0s is shorter than the cache TTL 70h0m0s of server srv-0-old plus the longest push 3h0m0s"}, nil},
		{"stopped server before the window with bad settings", func(c *deleteCase) {
			s := stoppedServer("srv-0-old", c.ev.snapshot.Add(-73*time.Hour))
			s.TouchInterval, s.CacheTTL, s.LinkValidation = 0, 0, false
			c.addServer(s)
		}, nil, nil},
		{"server without link validation", func(c *deleteCase) {
			c.ev.servers.servers[1].LinkValidation = false
		}, []string{"server srv-1-bb runs without link validation"}, nil},

		{"no server evidence", func(c *deleteCase) { c.ev.servers = nil }, []string{"the GC servers were not read"}, nil},

		// The Bigtable instance.
		{"two clusters", func(c *deleteCase) { c.ev.instance.clusters = []string{"c1", "c2"} }, []string{"has 2 clusters"}, nil},
		{"cluster listing denied", func(c *deleteCase) {
			c.ev.instance.clusters, c.ev.instance.clustersErr = nil, status.Error(codes.PermissionDenied, "denied")
		}, []string{"cannot list the clusters of the Bigtable instance (the collector needs bigtable.clusters.list)"}, nil},
		{"cluster listing failed", func(c *deleteCase) {
			c.ev.instance.clusters, c.ev.instance.clustersErr = nil, status.Error(codes.Unavailable, "try later")
		}, []string{"cannot list the clusters of the Bigtable instance: rpc error: code = Unavailable desc = try later"}, nil},
		{"profile without transactions", func(c *deleteCase) {
			c.ev.instance.probeErr = status.Error(codes.FailedPrecondition, "Single-row transactions are not allowed by this app profile")
		}, []string{"refused a conditional write"}, nil},
		{"profile refusal as invalid argument", func(c *deleteCase) {
			c.ev.instance.probeErr = status.Error(codes.InvalidArgument, "bad profile")
		}, []string{"refused a conditional write"}, nil},
		{"probe failed otherwise", func(c *deleteCase) {
			c.ev.instance.probeErr = status.Error(codes.Unavailable, "try later")
		}, []string{"the conditional write probe failed: rpc error: code = Unavailable desc = try later"}, nil},
		{"probe denied", func(c *deleteCase) {
			c.ev.instance.probeErr = status.Error(codes.PermissionDenied, "denied")
		}, []string{"the conditional write probe failed (the collector needs bigtable.tables.readRows and bigtable.tables.mutateRows)"}, nil},

		// Backups.
		{"no backup of chunks", func(c *deleteCase) { c.ev.instance.backups = c.ev.instance.backups[:3] }, []string{"no ready backup of Bigtable table chunks"}, nil},
		{"backup of maps not ready", func(c *deleteCase) { c.setBackup("maps", func(b *Backup) { b.Ready = false }) }, []string{"no ready backup of Bigtable table maps"}, nil},
		{"backup of objects older than the list", func(c *deleteCase) {
			c.setBackup("objects", func(b *Backup) { b.Start = c.ev.holds.records[4].CompleteTime.Add(-time.Minute) })
		}, []string{"no ready backup of Bigtable table objects"}, nil},
		{"backup of objects older than the newest complete list", func(c *deleteCase) {
			c.ev.holds.records[5] = &ListRecord{Cycle: 5, CompleteTime: c.ev.snapshot.Add(-time.Hour)}
			c.ev.holds.records[6] = &ListRecord{Cycle: 6}
			c.setBackup("objects", func(b *Backup) { b.Start = c.ev.snapshot.Add(-2 * time.Hour) })
		}, []string{"no ready backup of Bigtable table objects started after the list was completed at 2026-11-08T11:00:00Z"}, nil},
		{"backup listing denied", func(c *deleteCase) {
			c.ev.instance.backups, c.ev.instance.backupsErr = nil, status.Error(codes.PermissionDenied, "denied")
		}, []string{"cannot list the backups of the Bigtable instance (the collector needs bigtable.backups.list)"}, nil},
		{"backup check off", func(c *deleteCase) {
			c.ev.instance.backups, c.ev.instance.backupsChecked = nil, false
		}, nil, nil},

		{"ref removal failed", func(c *deleteCase) { c.ev.refRemovalErr = errors.New("aborted") }, []string{"the removal of expired refs failed: aborted"}, nil},

		// Client caches.
		{"live mount without keep-alive, blobs allowed", liveMounts("without_keepalive", 1), []string{"1 live mounts run without the GC keep-alive, so their caches may hold the only copy of a blob, tree or eval object they wrote, for example details d1 of u@host-a; set gc-keepalive"}, nil},
		{"live mount without keep-alive, accepted", func(c *deleteCase) {
			liveMounts("without_keepalive", 1)(c)
			c.o.AcceptMountsWithoutKeepAlive = true
		}, nil, []string{"1 live mounts run without the GC keep-alive, so their caches may hold the only copy of a blob, tree or eval object they wrote, for example details d1 of u@host-a"}},
		{"live mount without keep-alive, accepted, after a map row loss", func(c *deleteCase) {
			liveMounts("without_keepalive", 1)(c)
			c.o.AcceptMountsWithoutKeepAlive = true
			c.ev.servers.servers[2].MapRowLossTime = c.ev.snapshot.Add(-time.Hour)
		}, nil, []string{"1 live mounts run without the GC keep-alive, so their caches may hold the only copy of a blob, tree or eval object they wrote, for example details d1 of u@host-a"}},
		{"live mount without keep-alive, commits only", func(c *deleteCase) {
			liveMounts("without_keepalive", 1)(c)
			c.o.Types = []string{"commit"}
		}, nil, nil},
		{"live mount without keep-alive, eval responses allowed", func(c *deleteCase) {
			liveMounts("without_keepalive", 1)(c)
			c.o.Types = []string{"commit", "eval-response"}
		}, []string{"1 live mounts run without the GC keep-alive"}, nil},
		{"keep-alive mount without the local write check, blobs allowed", liveMounts("without_local_write_check", 1), []string{"1 live mounts with the GC keep-alive run with a cacheman that does not keep locally written content"}, nil},
		{"keep-alive mount without the local write check, commits only", func(c *deleteCase) {
			liveMounts("without_local_write_check", 1)(c)
			c.o.Types = []string{"commit"}
		}, nil, nil},
		{"keep-alive mount with a 40 h touch interval", liveMounts("touch_interval_too_long", 1), []string{"1 live mounts touch their objects less often than the grace allows, for example details d1 of u@host-a"}, nil},
		{"server lost map row touches", func(c *deleteCase) { c.ev.servers.servers[2].MapRowLossTime = c.ev.snapshot.Add(-time.Hour) }, nil, nil},

		// Writers without --gc.
		{"too_new key without a gc row", func(c *deleteCase) {
			c.r.Consistency.TooNew = 2
			c.ev.tooNew = &TooNewReport{Keys: 2, WithoutGCRow: 1, Samples: []string{"ab12"}}
		}, []string{"1 objects without a gc row were written during the consistency scan, for example ab12: a server without --gc is running"}, nil},
		{"too_new keys with gc rows", func(c *deleteCase) {
			c.r.Consistency.TooNew = 2
			c.ev.tooNew = &TooNewReport{Keys: 2, Gone: 1}
		}, nil, nil},
		{"too_new keys not checked", func(c *deleteCase) { c.r.Consistency.TooNew = 2 }, []string{"were not checked"}, nil},
		{"too_new check failed", func(c *deleteCase) {
			c.r.Consistency.TooNew = 2
			c.ev.tooNewErr = errors.New("unavailable")
		}, []string{"cannot read the gc rows of the objects written during the consistency scan: unavailable"}, nil},
		{"many too_new keys with gc rows", func(c *deleteCase) {
			c.r.Consistency.TooNew = 2_000_000
			c.ev.tooNew = &TooNewReport{Keys: 2_000_000, NotHashes: 1, Gone: 300}
		}, nil, nil},
		{"too_new keys not all checked", func(c *deleteCase) {
			c.r.Consistency.TooNew = 2
			c.ev.tooNew = &TooNewReport{Keys: 1}
		}, []string{"only 1 of the 2 objects written during the consistency scan were checked for a gc row"}, nil},
		{"too_new key without a gc row before the check stopped", func(c *deleteCase) {
			c.r.Consistency.TooNew = 3
			c.ev.tooNew = &TooNewReport{Keys: 2, WithoutGCRow: 1, Samples: []string{"ab12"}}
		}, []string{"1 objects without a gc row were written during the consistency scan, for example ab12"}, nil},
		{"too_new key without a gc row before a read failed", func(c *deleteCase) {
			c.r.Consistency.TooNew = 3
			c.ev.tooNew = &TooNewReport{Keys: 2, WithoutGCRow: 1, Samples: []string{"ab12"}}
			c.ev.tooNewErr = errors.New("unavailable")
		}, []string{"1 objects without a gc row were written during the consistency scan, for example ab12"}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newDeleteCase()
			tt.change(c)
			got, warnings := deleteBlockers(c.r, c.ev, c.o)
			require.Len(t, got, len(tt.want), "%q", got)
			for i, w := range tt.want {
				require.Contains(t, got[i], w)
			}
			require.Equal(t, tt.wantWarnings, warnings)
		})
	}
}

// The replica range of an autoscaled fleet counts the hosts of both tables,
// and its maximum is optional.
func TestReplicaCounts(t *testing.T) {
	now := time.Now()
	live := func(n int, prefix string) []Replica {
		var rows []Replica
		for i := range n {
			rows = append(rows, Replica{ID: prefix + string(rune('a'+i)) + "-x", GC: true, Version: ExtractorVersion, TouchInterval: 36 * time.Hour, CacheTTL: 6 * time.Hour, LastSeen: now})
		}
		return rows
	}
	tests := []struct {
		name               string
		hosts              int
		expected, min, max int
		want               string
	}{
		{"exact", 3, 3, 0, 0, ""},
		{"exact, one more", 4, 3, 0, 0, "4 server hosts write a GC heartbeat, but 3 are expected"},
		{"below the minimum", 1, 0, 2, 0, "1 server hosts write a GC heartbeat, but at least 2 are expected"},
		{"at the minimum", 2, 0, 2, 0, ""},
		{"13 hosts and no maximum", 13, 0, 2, 0, ""},
		{"above the maximum", 12, 0, 2, 11, "12 server hosts write a GC heartbeat, but at most 11 are expected"},
		{"at the maximum", 11, 0, 2, 11, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := DefaultCondemnOpts()
			o.ExpectedReplicas, o.MinReplicas, o.MaxReplicas = tt.expected, tt.min, tt.max
			rep := checkReplicas(live(tt.hosts, "srv-"), now, &o.DryRunOpts)
			got := replicaCountProblems(&rep, &o)
			if tt.want == "" {
				require.Empty(t, got)
				return
			}
			require.Equal(t, []string{tt.want}, got)
		})
	}
}

// The live hosts are those whose rows may still serve: a row that did not
// record a stop counts within two heartbeat intervals, the write timeout and
// the clock skew margin of its last heartbeat, so a crashed pod replaced
// under a new name stops counting after about 11 minutes. Its settings are
// then no longer checked here; the window checks of a delete still read it.
func TestLiveBlockers_ServeLimit(t *testing.T) {
	require := require.New(t)
	now := time.Now()
	o := DefaultCondemnOpts()
	o.ExpectedReplicas = 3
	e := &serverEvidence{readTime: now}
	for _, id := range []string{"srv-0-aa", "srv-1-bb", "srv-2-cc"} {
		e.servers = append(e.servers, liveServer(id, now))
	}
	require.Empty(liveBlockers(e, &o))

	crashed := liveServer("srv-3-old", now)
	crashed.CacheTTL = 0
	limit := 2*crashed.HeartbeatInterval + HeartbeatTimeout + clockSkewMargin
	require.Equal(11*time.Minute+30*time.Second, limit)
	crashed.LastSeen = now.Add(-limit)
	e.servers = append(e.servers, crashed)
	got := liveBlockers(e, &o)
	require.Len(got, 2)
	require.Contains(got[0], "replica srv-3-old: keeps cache entries without a TTL")
	require.Equal("4 server hosts write a GC heartbeat, but 3 are expected", got[1])

	e.servers[3].LastSeen = now.Add(-limit - time.Second)
	require.Empty(liveBlockers(e, &o))

	// Two live rows of one host count once, and refuse nothing.
	e.servers[3] = liveServer("srv-1-cafe", now)
	rep := checkReplicas(func() []Replica { r, _ := serverReplicas(e.servers); return r }(), now, &o.DryRunOpts)
	require.Equal(int64(3), rep.Hosts)
	require.Equal(int64(2), rep.RowsPerHost["srv-1"])
	require.Empty(liveBlockers(e, &o))
}

// The types to delete must be known, there is at least one delete in
// flight per key range, so that the calls in flight do not exceed the
// workers, and the chunks of a deleted object outlive the signed URLs of its
// chunks that a server handed out before the delete.
func TestDeleteOpts_Validate(t *testing.T) {
	tests := []struct {
		change  func(o *DeleteOpts)
		wantErr string
	}{
		{func(o *DeleteOpts) {}, ""},
		{func(o *DeleteOpts) { o.Types = []string{"commit", "blob"} }, ""},
		{func(o *DeleteOpts) { o.Types = []string{"commits"} }, `invalid object type "commits"`},
		{func(o *DeleteOpts) { o.DeleteWorkers = 0 }, "invalid number of delete workers 0"},
		{func(o *DeleteOpts) { o.DeleteWorkers = DefaultScanShards - 1 }, "fewer than the 16 key ranges"},
		{func(o *DeleteOpts) { o.DeleteWorkers = DefaultScanShards }, ""},
		{func(o *DeleteOpts) { o.DeleteRate = -1 }, "invalid delete rate"},
		{func(o *DeleteOpts) { o.ChunkDelay = 10 * time.Minute }, "not longer than the 10m0s lifetime"},
		{func(o *DeleteOpts) { o.ChunkDelay = 12 * time.Minute }, ""},
		{func(o *DeleteOpts) { o.StartDelay = -time.Second }, "invalid delete start delay"},
		{func(o *DeleteOpts) { o.MinReplicas = 0 }, "invalid expected replicas 0"},
	}
	for _, tt := range tests {
		o := DefaultDeleteOpts()
		o.MinReplicas = 2
		tt.change(&o)
		err := o.Validate()
		if tt.wantErr == "" {
			require.NoError(t, err)
			continue
		}
		require.ErrorContains(t, err, tt.wantErr)
	}
}

// The second server read refuses when the evidence of the first read broke
// since, and when the live mounts touch too seldom for the servers that run
// now. It does not wait for heartbeats, and losses refuse nothing.
func TestCheckServersAgain(t *testing.T) {
	ctx := context.Background()
	c := newDeleteCase()
	snapshot := c.r.SnapshotTime
	evidence := func(change func(e *serverEvidence)) *serverEvidence {
		e := &serverEvidence{readTime: snapshot.Add(10 * time.Minute)}
		for _, id := range []string{"srv-0-aa", "srv-1-bb", "srv-2-cc"} {
			e.servers = append(e.servers, liveServer(id, snapshot.Add(10*time.Minute)))
		}
		if change != nil {
			change(e)
		}
		return e
	}
	check := func(e *serverEvidence) []string {
		t.Helper()
		reasons, err := checkServersAgain(ctx, func(context.Context) (*serverEvidence, error) { return e, nil }, c.r, c.o)
		require.NoError(t, err)
		return reasons
	}

	require.Empty(t, check(evidence(nil)))
	// Losses refuse nothing.
	require.Empty(t, check(evidence(func(e *serverEvidence) {
		e.servers[2].TouchLossTime = snapshot.Add(5 * time.Minute)
		e.servers[2].MapRowLossTime = snapshot.Add(5 * time.Minute)
	})))
	reasons, err := checkServersAgain(ctx, func(context.Context) (*serverEvidence, error) { return nil, errors.New("unavailable") }, c.r, c.o)
	require.NoError(t, err)
	require.Equal(t, []string{"the second read of the GC servers failed: unavailable"}, reasons)

	// A server of an older version seen only by the second read refuses.
	require.Equal(t, []string{"server srv-2-cc: follows the GC rules of version 2, older than version 3 of this collector", "replica srv-2-cc: follows the GC rules of version 2, older than version 3 of this collector"},
		check(evidence(func(e *serverEvidence) { e.servers[2].Version = ExtractorVersion - 1 })))

	// The live hosts are counted again.
	require.Equal(t, []string{"1 server hosts write a GC heartbeat, but at least 2 are expected"}, check(evidence(func(e *serverEvidence) { e.servers = e.servers[:1] })))

	// The touch interval of the live mounts is checked against the servers
	// of this read: a server with a longer touch interval leaves a mount less
	// time between its touches, and so does one with a longer cache TTL,
	// whose answer may rest on a touch that old.
	c.r.LiveMounts.longestTouch = 35 * time.Hour
	settings := func(touch, ttl time.Duration) func(e *serverEvidence) {
		return func(e *serverEvidence) {
			for i := range e.servers {
				e.servers[i].TouchInterval, e.servers[i].CacheTTL = 30*time.Hour, time.Hour
			}
			e.servers[2].TouchInterval, e.servers[2].CacheTTL = touch, ttl
		}
	}
	require.Empty(t, check(evidence(settings(30*time.Hour, time.Hour))))
	tooSeldom := []string{"live mounts touch their objects every 35h0m0s, too seldom for the grace 72h0m0s less the longest touch interval and cache TTL of the servers that run now"}
	require.Equal(t, tooSeldom, check(evidence(settings(36*time.Hour, time.Hour))))
	require.Equal(t, tooSeldom, check(evidence(settings(30*time.Hour, 6*time.Hour))))

	// A map row loss no longer shortens the limit: a mount at 1.1 x 37 h
	// fits 72 h less 30 h, 1 h and a minute.
	c.r.LiveMounts.longestTouch = 37 * time.Hour
	require.Empty(t, check(evidence(func(e *serverEvidence) {
		settings(30*time.Hour, time.Hour)(e)
		e.servers[2].MapRowLossTime = snapshot.Add(-time.Hour)
	})))
	c.o.Types = []string{"commit"}
	require.Empty(t, check(evidence(settings(36*time.Hour, time.Hour))))
}

// A map row loss since the window start, less the time clients reuse map
// results and the time a keep-alive pass may take, is reported and changes
// no limit and no check.
func TestServerEvidence_MapRowLoss(t *testing.T) {
	require := require.New(t)
	c := newDeleteCase()
	e, snapshot, grace := c.ev.servers, c.ev.snapshot, c.o.Grace
	limit := grace - 36*time.Hour - 6*time.Hour - clockSkewMargin
	require.Equal(limit, e.keepAliveTouchLimit(snapshot, grace))
	_, lost := e.mapRowLoss(snapshot, grace)
	require.False(lost)

	e.servers[1].MapRowLossTime = snapshot.Add(-time.Hour)
	e.servers[2].MapRowLossTime = snapshot.Add(-2 * time.Hour)
	require.Equal(limit, e.keepAliveTouchLimit(snapshot, grace))
	at, lost := e.mapRowLoss(snapshot, grace)
	require.True(lost)
	require.Equal(snapshot.Add(-time.Hour), at)
	require.Empty(windowBlockers(e, snapshot, &c.o.DryRunOpts))

	// A loss of a process that stopped before map row losses are reported is
	// not reported, and a crashed row is not a loss.
	before := windowStart(snapshot, grace).Add(-clientMapReuse - keepAlivePassAllowance - time.Minute)
	e.servers[1] = stoppedServer("srv-1-bb", before)
	e.servers[1].MapRowLossTime = before
	e.servers[2] = liveServer("srv-2-cc", snapshot)
	e.servers[2].LastSeen = windowStart(snapshot, grace).Add(-time.Hour)
	_, lost = e.mapRowLoss(snapshot, grace)
	require.False(lost)
}

// The cache TTL a list records is the largest of every row whose process may
// have run in the window: rows that stopped in it and crashed rows whose last
// heartbeat is within the age of a live row before it count, older ones do
// not. A window without a row, or with a row that does not bound its
// answers, completes no list.
func TestServerEvidence_ListCacheTTL(t *testing.T) {
	require := require.New(t)
	snapshot := time.Now()
	grace := DefaultGrace
	start := windowStart(snapshot, grace)
	server := func(id string, ttl time.Duration, lastSeen, stopped time.Time) Server {
		s := liveServer(id, snapshot)
		s.CacheTTL, s.LastSeen, s.StoppedAt = ttl, lastSeen, stopped
		return s
	}
	e := &serverEvidence{servers: []Server{
		server("live-0-aa", 2*time.Hour, snapshot.Add(-time.Minute), time.Time{}),
		server("stopped-0-aa", 7*time.Hour, start.Add(-time.Hour), start.Add(time.Minute)),
		server("crashed-0-aa", 9*time.Hour, start.Add(-liveReplicaAge+time.Minute), time.Time{}),
		server("stopped-1-bb", 20*time.Hour, start.Add(-2*time.Hour), start.Add(-time.Minute)),
		server("crashed-1-bb", 30*time.Hour, start.Add(-liveReplicaAge-time.Minute), time.Time{}),
	}}
	ttl, reasons := e.listCacheTTL(snapshot, grace)
	require.Empty(reasons)
	require.Equal(9*time.Hour, ttl)

	e.servers = e.servers[3:]
	_, reasons = e.listCacheTTL(snapshot, grace)
	require.Equal([]string{"no server that may have run within the grace wrote a GC heartbeat, so the largest cache TTL is not known"}, reasons)

	noTTL := server("nottl-0-aa", 0, snapshot.Add(-time.Minute), time.Time{})
	noHooks := server("nohooks-0-aa", time.Hour, snapshot.Add(-time.Minute), time.Time{})
	noHooks.GC = false
	older := server("older-0-aa", time.Hour, snapshot.Add(-time.Minute), time.Time{})
	older.Version = ExtractorVersion - 1
	e.servers = []Server{older, noTTL, noHooks}
	_, reasons = e.listCacheTTL(snapshot, grace)
	require.Equal([]string{
		"server nohooks-0-aa runs without the garbage collector hooks",
		"server nottl-0-aa keeps cache entries without a TTL",
		fmt.Sprintf("server older-0-aa follows the GC rules of version %d, older than version %d of this collector", ExtractorVersion-1, ExtractorVersion),
	}, reasons)
}

// The objects written during the consistency scan are read again: one that
// still has no gc row was written by a server without --gc, and one without
// a gc row or an objects row is gone. A read that stalls ends at the
// deadline of its attempt and is tried again.
func TestTooNewCheck(t *testing.T) {
	require := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var reads atomic.Int32
	stall := grpc.WithStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		if strings.HasSuffix(method, "/ReadRows") {
			// Two of the reads, which the batches in flight share.
			if n := reads.Add(1); n == 1 || n == 3 {
				<-ctx.Done()
				return nil, ctx.Err()
			}
		}
		return streamer(ctx, desc, cc, method, opts...)
	})
	client := newHooksTestBigtable(t, stall)
	put := func(table, key, family, column string) {
		mut := bigtable.NewMutation()
		mut.Set(family, column, bigtable.Now(), []byte("x"))
		require.NoError(client.Open(table).Apply(ctx, key, mut))
	}
	keys := testHashes(250)
	var all []string
	for i, k := range keys {
		all = append(all, k.String())
		if i%5 == 4 {
			continue // gone again
		}
		put(objectsTable, k.String(), objectsMetaFamily, "type")
		if i != 7 && i != 201 {
			put(TableName, k.String(), EdgesFamily, LinksColumn)
		}
	}
	// A touch alone is not a gc row.
	put(TableName, keys[7].String(), TouchFamily, TouchColumn)
	all = append(all, "abfs-setup-check")

	reads.Store(0)
	check := newTooNewCheck(client, writeBudget{deadline: 200 * time.Millisecond})
	check.start(ctx)
	for lo := 0; lo < len(all); lo += tooNewBatch {
		require.True(check.send(ctx, all[lo:min(lo+tooNewBatch, len(all))]))
	}
	rep, err := check.wait()
	require.NoError(err)
	require.Equal(&TooNewReport{Keys: 251, NotHashes: 1, Gone: 50, WithoutGCRow: 2, Samples: slices.Sorted(slices.Values([]string{keys[7].String(), keys[201].String()}))}, rep)
}

// A read that fails after the check found an object without a gc row does
// not hide that object: the report keeps it with the error, and the delete
// is refused naming it.
func TestTooNewCheck_ReadFailsAfterWriterFound(t *testing.T) {
	require := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	keys := testHashes(2 * tooNewBatch)
	without := keys[0].String()
	// Every read of gc rows fails, except the one of the batch with the
	// object without a gc row.
	client := newHooksTestBigtable(t, readHook(func(ctx context.Context, req *bigtablepb.ReadRowsRequest) error {
		if strings.HasSuffix(req.GetTableName(), "/"+TableName) && !slices.ContainsFunc(req.GetRows().GetRowKeys(), func(k []byte) bool { return string(k) == without }) {
			return status.Error(codes.PermissionDenied, "read refused by the test")
		}
		return nil
	}))
	put := func(table, key, family, column string) {
		mut := bigtable.NewMutation()
		mut.Set(family, column, bigtable.Now(), []byte("x"))
		require.NoError(client.Open(table).Apply(ctx, key, mut))
	}
	var all []string
	for _, k := range keys {
		all = append(all, k.String())
		put(objectsTable, k.String(), objectsMetaFamily, "type")
		if k.String() != without {
			put(TableName, k.String(), EdgesFamily, LinksColumn)
		}
	}

	check := newTooNewCheck(client, writeBudget{deadline: time.Second})
	check.start(ctx)
	require.True(check.send(ctx, all[:tooNewBatch]))
	require.Eventually(func() bool {
		check.mu.Lock()
		defer check.mu.Unlock()
		return check.rep.Keys == tooNewBatch
	}, 5*time.Second, 5*time.Millisecond)
	require.True(check.send(ctx, all[tooNewBatch:]))
	rep, err := check.wait()
	require.ErrorContains(err, "failed to read the gc rows of the objects written during the scan")
	require.Equal(&TooNewReport{Keys: tooNewBatch, WithoutGCRow: 1, Samples: []string{without}}, rep)

	c := newDeleteCase()
	c.r.Consistency.TooNew = int64(len(all))
	c.ev.tooNew, c.ev.tooNewErr = rep, err
	reasons, _ := deleteBlockers(c.r, c.ev, c.o)
	require.Equal([]string{"1 objects without a gc row were written during the consistency scan, for example " + without + ": a server without --gc is running"}, reasons)
}

type fakeInstance struct {
	clusters      []string
	probeErr      error
	probes        int
	backups       []Backup
	backupsCalled bool
}

func (f *fakeInstance) Clusters(context.Context) ([]string, error) { return f.clusters, nil }

func (f *fakeInstance) Backups(context.Context) ([]Backup, error) {
	f.backupsCalled = true
	return f.backups, nil
}

func (f *fakeInstance) ProbeConditionalWrite(context.Context) error {
	f.probes++
	return f.probeErr
}

// The checks of the instance are read for every delete, and the backups only
// unless the operator skips them.
func TestReadInstanceEvidence(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := &fakeInstance{clusters: []string{"c1"}, probeErr: status.Error(codes.FailedPrecondition, "no transactions")}
	o := &DeleteOpts{}
	e := readInstanceEvidence(ctx, f, writeBudget{}, o)
	require.True(f.backupsCalled)
	require.True(e.backupsChecked)
	require.Equal([]string{"c1"}, e.clusters)
	require.Equal(codes.FailedPrecondition, status.Code(e.probeErr))
	reasons := instanceBlockers(&e, time.Now())
	require.Len(reasons, 1+len(backupTables))

	f.backupsCalled = false
	o.NoBackupCheck = true
	e = readInstanceEvidence(ctx, f, writeBudget{}, o)
	require.False(f.backupsCalled)
	require.Len(instanceBlockers(&e, time.Now()), 1)

	// The probe is a write of the collector: it is retried on transient
	// errors and not sent once the lease could expire.
	f.probes, f.probeErr = 0, status.Error(codes.Unavailable, "busy")
	e = readInstanceEvidence(ctx, f, writeBudget{deadline: time.Second}, o)
	require.Equal(writeAttempts, f.probes)
	f.probes, f.probeErr = 0, nil
	expiring := &Lease{}
	expiring.expiry.Store(time.Now().Add(time.Minute).UnixNano())
	e = readInstanceEvidence(ctx, f, writeBudget{deadline: DefaultWriteDeadline, lease: expiring}, o)
	require.Zero(f.probes)
	require.ErrorIs(e.probeErr, errLeaseExpiring)
}

// The probe of the collector's own app profile is a conditional write that
// changes nothing.
func TestInstanceChecks_ProbeConditionalWrite(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	client := newHooksTestBigtable(t)
	require.NoError(NewInstanceChecks("p", "i", client).ProbeConditionalWrite(ctx))
	row, err := client.Open(objectsTable).ReadRow(ctx, "abfs-setup-check")
	require.NoError(err)
	require.Empty(row)
}

// The consistency scan of a delete cycle checks every object written during
// it, however many, and only those: with a gc row written after the scan of
// the table gc, the delete goes ahead; one object without a gc row refuses
// it. A check that fails does not fail the scan, and refuses the delete.
func TestCheckConsistency_ChecksTooNewKeys(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	client := newHooksTestBigtable(t)
	old := bigtable.Time(time.Now().Add(-time.Hour))
	const n = 12_000
	keys := testHashes(n + 2)
	meta := func(ts bigtable.Timestamp) *bigtable.Mutation {
		mut := bigtable.NewMutation()
		mut.Set(objectsMetaFamily, "type", ts, []byte("blob"))
		return mut
	}
	edges := func(ts bigtable.Timestamp) *bigtable.Mutation {
		mut := bigtable.NewMutation()
		mut.Set(EdgesFamily, LinksColumn, ts, PackEdges(nil))
		mut.Set(EdgesFamily, TypeColumn, ts, []byte("blob"))
		mut.Set(EdgesFamily, VersionColumn, ts, versionBytes(ExtractorVersion))
		return mut
	}
	bulk := func(table string, keys []Hash, mut func(bigtable.Timestamp) *bigtable.Mutation) {
		var rows []string
		var muts []*bigtable.Mutation
		for _, k := range keys {
			rows, muts = append(rows, k.String()), append(muts, mut(bigtable.Now()))
		}
		errs, err := client.Open(table).ApplyBulk(ctx, rows, muts)
		require.NoError(err)
		require.Nil(errs)
	}
	// One object with a gc row, and one without written before the scan.
	require.NoError(client.Open(TableName).Apply(ctx, keys[0].String(), edges(old)))
	require.NoError(client.Open(objectsTable).Apply(ctx, keys[0].String(), meta(old)))
	require.NoError(client.Open(objectsTable).Apply(ctx, keys[1].String(), meta(old)))
	bulk(objectsTable, keys[2:], meta)

	bounds, err := splitRange("", "", 4)
	require.NoError(err)
	bounds = prefixBounds(bounds)
	g, _, err := scanNodes(ctx, client.Open(TableName), bounds, time.Now().Add(-72*time.Hour))
	require.NoError(err)
	// The gc rows of the new objects land after the scan of the table gc, as
	// those of uploads during a cycle do; one object has none.
	without := keys[n/2].String()
	bulk(TableName, slices.DeleteFunc(slices.Clone(keys[2:]), func(k Hash) bool { return k.String() == without }), edges)

	scanWith := func(b writeBudget) *consistencyScan {
		scan, err := checkConsistency(ctx, client.Open(objectsTable), bounds, g, newBitmap(g.size()), time.Now().Add(-time.Minute), true, newTooNewCheck(client, b))
		require.NoError(err)
		require.Equal(int64(1), scan.report.MissingGCRows)
		require.Equal(int64(n), scan.report.TooNew)
		return scan
	}
	blockers := func(scan *consistencyScan) []string {
		c := newDeleteCase()
		c.r.Consistency = *scan.report
		c.ev.tooNew, c.ev.tooNewErr = scan.tooNew, scan.tooNewErr
		reasons, _ := deleteBlockers(c.r, c.ev, c.o)
		return reasons
	}

	scan := scanWith(writeBudget{deadline: time.Second})
	require.NoError(scan.tooNewErr)
	require.Equal(&TooNewReport{Keys: n, WithoutGCRow: 1, Samples: []string{without}}, scan.tooNew)
	reasons := blockers(scan)
	require.Len(reasons, 1)
	require.Contains(reasons[0], "1 objects without a gc row were written during the consistency scan, for example "+without)

	require.NoError(client.Open(TableName).Apply(ctx, without, edges(bigtable.Now())))
	scan = scanWith(writeBudget{deadline: time.Second})
	require.Equal(&TooNewReport{Keys: n}, scan.tooNew)
	require.Empty(blockers(scan))

	expiring := &Lease{}
	expiring.expiry.Store(time.Now().Add(time.Minute).UnixNano())
	scan = scanWith(writeBudget{deadline: DefaultWriteDeadline, lease: expiring})
	require.Equal(&TooNewReport{}, scan.tooNew)
	require.ErrorIs(scan.tooNewErr, errLeaseExpiring)
	reasons = blockers(scan)
	require.Len(reasons, 1)
	require.Contains(reasons[0], "cannot read the gc rows of the objects written during the consistency scan")

	// A scan without a check, as in a dry run or a condemn cycle, reads
	// nothing more.
	scan, err = checkConsistency(ctx, client.Open(objectsTable), bounds, g, newBitmap(g.size()), time.Now().Add(-time.Minute), false, nil)
	require.NoError(err)
	require.Nil(scan.tooNew)
	require.NoError(scan.tooNewErr)
}

// The evidence of a delete carries the snapshot, the list records and the
// servers the cycle read.
func TestNewDeleteEvidence(t *testing.T) {
	start := time.Now()
	lease := &Lease{Cycle: 7, Start: start}
	servers := &serverEvidence{readTime: start}
	holds := &listHolds{records: map[int64]*ListRecord{}}
	ev := newDeleteEvidence(lease, &Result{servers: servers, holds: holds})
	require.Equal(t, &deleteEvidence{snapshot: start, holds: holds, servers: servers}, ev)
}

// readHook returns a dial option that runs hook before each ReadRows request
// of the client; an error it returns fails the read.
func readHook(hook func(ctx context.Context, req *bigtablepb.ReadRowsRequest) error) grpc.DialOption {
	return grpc.WithStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		s, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil || !strings.HasSuffix(method, "/ReadRows") {
			return s, err
		}
		return &hookedRead{ClientStream: s, hook: hook}, nil
	})
}

type hookedRead struct {
	grpc.ClientStream
	hook func(ctx context.Context, req *bigtablepb.ReadRowsRequest) error
	err  error
}

func (r *hookedRead) SendMsg(m any) error {
	if req, ok := m.(*bigtablepb.ReadRowsRequest); ok {
		r.err = r.hook(r.Context(), req)
	}
	return r.ClientStream.SendMsg(m)
}

func (r *hookedRead) RecvMsg(m any) error {
	if r.err != nil {
		return r.err
	}
	return r.ClientStream.RecvMsg(m)
}

// tooNewScan is a store of objects written just now without a gc row in the
// mark, split into four key ranges. Their gc rows are written after the mark,
// as those of uploads during a cycle are. Once on is set, gcRead runs before
// each read of the table gc and objectsRange before each scan of a range of
// the table objects.
type tooNewScan struct {
	t            *testing.T
	client       *bigtable.Client
	bounds       []string
	g            *graph
	on           atomic.Bool
	gcRead       func(ctx context.Context) error
	objectsRange func(ctx context.Context, start string) error
}

func newTooNewScan(t *testing.T, n int) *tooNewScan {
	t.Helper()
	ctx := context.Background()
	s := &tooNewScan{t: t}
	s.client = newHooksTestBigtable(t, readHook(func(ctx context.Context, req *bigtablepb.ReadRowsRequest) error {
		if !s.on.Load() {
			return nil
		}
		switch {
		case strings.HasSuffix(req.GetTableName(), "/"+TableName) && s.gcRead != nil:
			return s.gcRead(ctx)
		case strings.HasSuffix(req.GetTableName(), "/"+objectsTable) && len(req.GetRows().GetRowRanges()) > 0 && s.objectsRange != nil:
			return s.objectsRange(ctx, string(req.GetRows().GetRowRanges()[0].GetStartKeyClosed()))
		}
		return nil
	}))
	bulk := func(table string, set func(mut *bigtable.Mutation)) {
		var rows []string
		var muts []*bigtable.Mutation
		for _, k := range testHashes(n) {
			mut := bigtable.NewMutation()
			set(mut)
			rows, muts = append(rows, k.String()), append(muts, mut)
		}
		errs, err := s.client.Open(table).ApplyBulk(ctx, rows, muts)
		require.NoError(t, err)
		require.Nil(t, errs)
	}
	bulk(objectsTable, func(mut *bigtable.Mutation) { mut.Set(objectsMetaFamily, "type", bigtable.Now(), []byte("blob")) })
	bounds, err := splitRange("", "", 4)
	require.NoError(t, err)
	s.bounds = prefixBounds(bounds)
	s.g, _, err = scanNodes(ctx, s.client.Open(TableName), s.bounds, time.Now().Add(-72*time.Hour))
	require.NoError(t, err)
	bulk(TableName, func(mut *bigtable.Mutation) {
		mut.Set(EdgesFamily, LinksColumn, bigtable.Now(), PackEdges(nil))
		mut.Set(EdgesFamily, TypeColumn, bigtable.Now(), []byte("blob"))
		mut.Set(EdgesFamily, VersionColumn, bigtable.Now(), versionBytes(ExtractorVersion))
	})
	return s
}

// check returns a check whose reads attempt for at most deadline.
func (s *tooNewScan) check(deadline time.Duration) *tooNewCheck {
	return newTooNewCheck(s.client, writeBudget{deadline: deadline})
}

func (s *tooNewScan) run(ctx context.Context, check *tooNewCheck) (*consistencyScan, error) {
	s.on.Store(true)
	defer s.on.Store(false)
	return checkConsistency(ctx, s.client.Open(objectsTable), s.bounds, s.g, newBitmap(s.g.size()), time.Now().Add(-time.Minute), false, check)
}

// blockers returns the reasons why a delete cycle with scan refuses.
func (s *tooNewScan) blockers(scan *consistencyScan) []string {
	c := newDeleteCase()
	c.r.Consistency = *scan.report
	c.ev.tooNew, c.ev.tooNewErr = scan.tooNew, scan.tooNewErr
	reasons, _ := deleteBlockers(c.r, c.ev, c.o)
	return reasons
}

// stall makes reads wait until their context ends and counts the reads that
// wait and those that ended.
type stall struct {
	waiting, ended atomic.Int32
}

func (s *stall) read(ctx context.Context) error {
	s.waiting.Add(1)
	defer s.ended.Add(1)
	<-ctx.Done()
	return ctx.Err()
}

// When the checks fall behind the scan, the scan waits for them, and every
// object is still checked.
func TestCheckConsistency_TooNewBackpressure(t *testing.T) {
	require := require.New(t)
	const n = 4000
	s := newTooNewScan(t, n)
	s.gcRead = func(ctx context.Context) error {
		time.Sleep(20 * time.Millisecond)
		return nil
	}
	check := s.check(time.Second)
	scan, err := s.run(context.Background(), check)
	require.NoError(err)
	require.Equal(int64(n), scan.report.TooNew)
	require.NoError(scan.tooNewErr)
	require.Equal(&TooNewReport{Keys: n}, scan.tooNew)
	require.Positive(check.waited)
	require.Empty(s.blockers(scan))
}

// When the scan has waited for the checks as long as the check allows, it
// stops checking and finishes, and the delete is refused because not every
// object was checked.
func TestCheckConsistency_TooNewWaitUsedUp(t *testing.T) {
	require := require.New(t)
	const n = 4000
	s := newTooNewScan(t, n)
	s.gcRead = func(ctx context.Context) error {
		time.Sleep(300 * time.Millisecond)
		return nil
	}
	check := s.check(time.Second)
	check.maxWait = 50 * time.Millisecond
	start := time.Now()
	scan, err := s.run(context.Background(), check)
	require.NoError(err)
	require.Less(time.Since(start), 3*time.Second)
	require.Equal(int64(n), scan.report.TooNew)
	require.NoError(scan.tooNewErr)
	require.Less(scan.tooNew.Keys, int64(n))
	require.GreaterOrEqual(check.waited, check.maxWait)
	reasons := s.blockers(scan)
	require.Len(reasons, 1)
	require.Contains(reasons[0], fmt.Sprintf("only %d of the %d objects written during the consistency scan were checked for a gc row", scan.tooNew.Keys, n))
}

// When the scan uses up its wait while the reads of the check hang, the check
// stops at once: the reads in flight are cancelled and their errors ignored,
// and the batches still queued are not read. The delete is refused because
// not every object was checked.
func TestCheckConsistency_TooNewWaitUsedUpStopsReads(t *testing.T) {
	require := require.New(t)
	const n = 4000
	s := newTooNewScan(t, n)
	var reads stall
	s.gcRead = reads.read
	check := s.check(time.Minute)
	type result struct {
		scan *consistencyScan
		err  error
	}
	done := make(chan result, 1)
	go func() {
		scan, err := s.run(context.Background(), check)
		done <- result{scan, err}
	}()
	require.Eventually(func() bool {
		return reads.waiting.Load() == tooNewReadsInFlight && len(check.batches) == cap(check.batches)
	}, 5*time.Second, 5*time.Millisecond)
	check.mu.Lock()
	check.maxWait = 0
	check.mu.Unlock()
	var res result
	select {
	case res = <-done:
	case <-time.After(5 * time.Second):
		require.FailNow("the scan did not return after its wait was used up")
	}
	require.NoError(res.err)
	scan := res.scan
	require.Equal(int64(n), scan.report.TooNew)
	require.NoError(scan.tooNewErr)
	require.Equal(&TooNewReport{}, scan.tooNew)
	require.Equal(int32(tooNewReadsInFlight), reads.waiting.Load())
	require.Equal(reads.waiting.Load(), reads.ended.Load())
	reasons := s.blockers(scan)
	require.Len(reasons, 1)
	require.Contains(reasons[0], fmt.Sprintf("only 0 of the %d objects written during the consistency scan were checked for a gc row", n))
}

// A check whose every read fails does not stop the scan, reads none of the
// batches still queued, and refuses the delete.
func TestCheckConsistency_TooNewReadsFail(t *testing.T) {
	require := require.New(t)
	const n = 4000
	s := newTooNewScan(t, n)
	var reads atomic.Int32
	release := make(chan struct{})
	s.gcRead = func(ctx context.Context) error {
		reads.Add(1)
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		return status.Error(codes.PermissionDenied, "read refused by the test")
	}
	check := s.check(time.Minute)
	type result struct {
		scan *consistencyScan
		err  error
	}
	done := make(chan result, 1)
	go func() {
		scan, err := s.run(context.Background(), check)
		done <- result{scan, err}
	}()
	require.Eventually(func() bool {
		return reads.Load() == tooNewReadsInFlight && len(check.batches) == cap(check.batches)
	}, 5*time.Second, 5*time.Millisecond)
	close(release)
	var res result
	select {
	case res = <-done:
	case <-time.After(5 * time.Second):
		require.FailNow("the scan did not return after the reads failed")
	}
	require.NoError(res.err)
	scan := res.scan
	require.Equal(int64(n), scan.report.TooNew)
	require.Equal(int32(tooNewReadsInFlight), reads.Load())
	require.Equal(&TooNewReport{}, scan.tooNew)
	require.ErrorContains(scan.tooNewErr, "failed to read the gc rows of the objects written during the scan")
	reasons := s.blockers(scan)
	require.Len(reasons, 1)
	require.Contains(reasons[0], "cannot read the gc rows of the objects written during the consistency scan")
}

// A scan that is cancelled while the check's reads hang and the scan waits
// for room returns at once, and every read of the check has ended.
func TestCheckConsistency_TooNewCancelled(t *testing.T) {
	require := require.New(t)
	s := newTooNewScan(t, 4000)
	var reads stall
	s.gcRead = reads.read
	check := s.check(time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := s.run(ctx, check)
		done <- err
	}()
	require.Eventually(func() bool {
		return reads.waiting.Load() == tooNewReadsInFlight && len(check.batches) == cap(check.batches)
	}, 5*time.Second, 5*time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.True(errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled, "%v", err)
	case <-time.After(2 * time.Second):
		require.Fail("the scan did not return after it was cancelled")
	}
	require.Equal(reads.waiting.Load(), reads.ended.Load())
}

// A key range whose scan fails while the other ranges wait for room ends the
// scan with its error, and every read of the check has ended.
func TestCheckConsistency_TooNewRangeFails(t *testing.T) {
	require := require.New(t)
	s := newTooNewScan(t, 4000)
	var reads stall
	s.gcRead = reads.read
	release := make(chan struct{})
	failing := s.bounds[1]
	s.objectsRange = func(ctx context.Context, start string) error {
		if start != failing {
			return nil
		}
		<-release
		return status.Error(codes.PermissionDenied, "scan refused by the test")
	}
	check := s.check(time.Minute)
	done := make(chan error, 1)
	go func() {
		_, err := s.run(context.Background(), check)
		done <- err
	}()
	require.Eventually(func() bool {
		return reads.waiting.Load() == tooNewReadsInFlight && len(check.batches) == cap(check.batches)
	}, 5*time.Second, 5*time.Millisecond)
	close(release)
	select {
	case err := <-done:
		require.ErrorContains(err, fmt.Sprintf("failed to scan the table objects from %q", failing))
		require.Equal(codes.PermissionDenied, status.Code(err))
	case <-time.After(2 * time.Second):
		require.Fail("the scan did not return after a range failed")
	}
	require.Equal(reads.waiting.Load(), reads.ended.Load())
}
