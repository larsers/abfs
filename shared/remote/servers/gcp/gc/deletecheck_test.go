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
	listSnapshot := snapshot.Add(-24 * time.Hour)
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
			snapshot:             snapshot,
			previousCondemnCycle: 4,
			list:                 ListState{Location: "gc/default/list-4", SnapshotTime: listSnapshot, Complete: true, CompleteTime: complete},
			listHeader:           &listHeader{cycle: 4, snapshot: listSnapshot},
			servers:              &serverEvidence{servers: servers, readTime: snapshot, legacyGone: snapshot.Add(-100 * time.Hour)},
			instance:             instanceEvidence{clusters: []string{"c1"}, backups: backups, backupsChecked: true},
		},
		o: o,
	}
}

func liveServer(id string, now time.Time) Server {
	return Server{
		ID: id, Version: ExtractorVersion, GC: true, TouchInterval: 36 * time.Hour, CacheTTL: 6 * time.Hour,
		HeartbeatInterval: 5 * time.Minute, LinkValidation: true, LastSeen: now.Add(-time.Minute), StatsTime: now.Add(-time.Minute),
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

		// The list.
		{"no list", func(c *deleteCase) { c.ev.list = ListState{}; c.ev.listHeader = nil }, []string{"no condemn cycle published a list"}, nil},
		{"list gone", func(c *deleteCase) { c.ev.listHeader, c.ev.listErr = nil, "not found" }, []string{"could not be read: not found"}, nil},
		{"incomplete list", func(c *deleteCase) { c.ev.list.Complete = false }, []string{"is not complete"}, nil},
		{"list of an older collector", func(c *deleteCase) { c.ev.list.CompleteTime = time.Time{} }, []string{"has no completion time after its snapshot"}, nil},
		{"stale completion time of an older list", func(c *deleteCase) { c.ev.list.CompleteTime = c.ev.list.SnapshotTime.Add(-time.Hour) }, []string{"has no completion time after its snapshot"}, nil},
		{"condemn lease between the list and this cycle", func(c *deleteCase) { c.ev.previousCondemnCycle = 5 }, []string{"is of cycle 4, but cycle 5 was the last condemn or delete cycle"}, nil},

		// The lower bound counts from the completion of the list.
		{"snapshot within the cache TTL of the completion", func(c *deleteCase) {
			c.ev.list.CompleteTime = c.ev.snapshot.Add(-6 * time.Hour)
		}, []string{"is earlier than the completion of the list"}, nil},
		{"snapshot past the cache TTL and the write bounds", func(c *deleteCase) {
			c.ev.list.CompleteTime = c.ev.snapshot.Add(-6*time.Hour - TouchDrainBound - clockSkewMargin - time.Second)
		}, nil, nil},
		// T is the largest TTL of every server of the window, stopped ones too.
		{"TTL of a stopped server of the window", func(c *deleteCase) {
			for i := range c.ev.servers.servers {
				c.ev.servers.servers[i].CacheTTL = time.Hour
			}
			c.ev.list.CompleteTime = c.ev.snapshot.Add(-2 * time.Hour)
			c.addServer(stoppedServer("srv-9-zz", c.ev.snapshot.Add(-3*time.Hour)))
		}, []string{"plus the largest server cache TTL 6h0m0s"}, nil},

		// The upper bound counts from the earlier snapshot of the list.
		{"snapshot past the grace of the list", func(c *deleteCase) {
			c.ev.list.SnapshotTime = c.ev.snapshot.Add(-72*time.Hour + 2*time.Minute)
			c.ev.listHeader.snapshot = c.ev.list.SnapshotTime
		}, []string{"is not earlier than the snapshot of the list"}, nil},
		{"header snapshot earlier than the state", func(c *deleteCase) {
			c.ev.listHeader.snapshot = c.ev.snapshot.Add(-72*time.Hour + 2*time.Minute)
		}, []string{"is not earlier than the snapshot of the list"}, nil},
		{"snapshot just inside the grace of the list", func(c *deleteCase) {
			c.ev.list.SnapshotTime = c.ev.snapshot.Add(-72*time.Hour + 3*time.Minute + time.Second)
			c.ev.listHeader.snapshot = c.ev.list.SnapshotTime
		}, nil, nil},

		// The processes of the window.
		{"crashed predecessor within the grace", func(c *deleteCase) {
			s := liveServer("srv-0-old", c.ev.snapshot)
			s.LastSeen = c.ev.snapshot.Add(-10 * time.Hour)
			c.addServer(s)
		}, []string{"server srv-0-old last wrote its heartbeat at 2026-11-08T02:00:00Z and did not record a stop"}, nil},
		{"crash before the window", func(c *deleteCase) {
			s := liveServer("srv-0-old", c.ev.snapshot)
			s.LastSeen = c.ev.snapshot.Add(-73 * time.Hour)
			c.addServer(s)
		}, nil, nil},
		{"clean stop with losses 2 h ago", func(c *deleteCase) {
			s := stoppedServer("srv-0-old", c.ev.snapshot.Add(-2*time.Hour))
			s.TouchLossTime = s.StoppedAt
			c.addServer(s)
		}, []string{"server srv-0-old lost touches"}, nil},
		{"losses before the window of a server that ran in it", func(c *deleteCase) {
			s := stoppedServer("srv-0-old", c.ev.snapshot.Add(-2*time.Hour))
			s.TouchLossTime = c.ev.snapshot.Add(-80 * time.Hour)
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
		}, []string{"plus the largest server cache TTL 70h0m0s", "grace 72h0m0s is shorter than the cache TTL 70h0m0s of server srv-0-old plus the longest push 3h0m0s"}, nil},
		{"stopped server before the window with bad settings", func(c *deleteCase) {
			s := stoppedServer("srv-0-old", c.ev.snapshot.Add(-73*time.Hour))
			s.TouchInterval, s.CacheTTL, s.LinkValidation = 0, 0, false
			c.addServer(s)
		}, nil, nil},
		{"server without link validation", func(c *deleteCase) {
			c.ev.servers.servers[1].LinkValidation = false
		}, []string{"server srv-1-bb runs without link validation"}, nil},

		// Servers of an older version.
		{"row in GcReplicas", func(c *deleteCase) {
			c.ev.servers.replicas = []Replica{{ID: "old-0-aa", GC: true, LastSeen: c.ev.snapshot}}
			c.ev.servers.legacyGone = time.Time{}
		}, []string{"GcReplicas holds 1 rows"}, nil},
		{"GcReplicas found empty inside the window", func(c *deleteCase) {
			c.ev.servers.legacyGone = c.ev.snapshot.Add(-10 * time.Hour)
		}, []string{"GcReplicas was first found empty at"}, nil},
		{"GcReplicas never found empty", func(c *deleteCase) {
			c.ev.servers.legacyGone = time.Time{}
		}, []string{"no cycle has recorded that GcReplicas is empty"}, nil},
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
			c.setBackup("objects", func(b *Backup) { b.Start = c.ev.list.CompleteTime.Add(-time.Minute) })
		}, []string{"no ready backup of Bigtable table objects"}, nil},
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
		}, []string{"1 live mounts run without the GC keep-alive, for example details d1 of u@host-a, and server srv-2-cc lost map row touches at 2026-11-08T11:00:00Z: such a mount may apply a map result whose outputs no map row keeps any more, which --accept-mounts-without-keepalive does not accept; set gc-keepalive in their config"}, nil},
		{"live mount without keep-alive, accepted, after a crash before the window", func(c *deleteCase) {
			liveMounts("without_keepalive", 1)(c)
			c.o.AcceptMountsWithoutKeepAlive = true
			c.ev.servers.servers[2].LastSeen = windowStart(c.ev.snapshot, c.o.Grace).Add(-time.Hour)
		}, []string{"1 live mounts run without the GC keep-alive, for example details d1 of u@host-a, and server srv-2-cc last wrote its heartbeat at 2026-11-05T10:59:00Z and did not record a stop: it crashed and may have lost map row touches"}, nil},
		{"live mount without keep-alive, accepted, commits only after a map row loss", func(c *deleteCase) {
			liveMounts("without_keepalive", 1)(c)
			c.o.AcceptMountsWithoutKeepAlive = true
			c.o.Types = []string{"commit"}
			c.ev.servers.servers[2].MapRowLossTime = c.ev.snapshot.Add(-time.Hour)
		}, nil, nil},
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
		{"keep-alive mount too slow after a map row loss", func(c *deleteCase) {
			liveMounts("touch_interval_too_long", 1)(c)
			c.ev.servers.servers[2].MapRowLossTime = c.ev.snapshot.Add(-time.Hour)
		}, []string{"1 live mounts touch their objects less often than the grace allows, and less the 8h0m0s for which clients reuse map results and the 1h0m0s a keep-alive pass may take, since server srv-2-cc lost map row touches at 2026-11-08T11:00:00Z, for example details d1 of u@host-a"}, nil},

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

// In a rolling upgrade both heartbeat tables hold live rows: their hosts
// count together, and the settings of the rows of the older servers are
// checked too.
func TestLiveBlockers_MixedFleet(t *testing.T) {
	require := require.New(t)
	now := time.Now()
	o := DefaultCondemnOpts()
	o.MinReplicas = 5
	e := &serverEvidence{readTime: now}
	for _, id := range []string{"new-0-aa", "new-1-bb", "new-2-cc"} {
		e.servers = append(e.servers, liveServer(id, now))
	}
	for _, id := range []string{"old-0-aa", "old-1-bb"} {
		e.replicas = append(e.replicas, Replica{ID: id, Version: ExtractorVersion, GC: true, TouchInterval: 36 * time.Hour, CacheTTL: 6 * time.Hour, LastSeen: now})
	}
	require.Empty(liveBlockers(e, &o))

	o.MinReplicas = 6
	require.Equal([]string{"5 server hosts write a GC heartbeat, but at least 6 are expected"}, liveBlockers(e, &o))

	o.MinReplicas = 5
	e.replicas[1].CacheTTL = 0
	got := liveBlockers(e, &o)
	require.Len(got, 1)
	require.Contains(got[0], "replica old-1-bb: keeps cache entries without a TTL")
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

// fakeServers serves the reads of pollServerStats from a list, one per read,
// repeating the last.
type fakeServers struct {
	reads []*serverEvidence
	n     int
}

func (f *fakeServers) read(context.Context) (*serverEvidence, error) {
	e := f.reads[min(f.n, len(f.reads)-1)]
	f.n++
	return e, nil
}

// The second server read waits for every live server to read its touch
// stats after the snapshot, and refuses when one does not within its
// heartbeat interval and the margin, or when the evidence broke since the
// first read.
func TestPollServerStats(t *testing.T) {
	oldMargin, oldPoll := statsWaitMargin, statsPollInterval
	statsWaitMargin, statsPollInterval = 0, 10*time.Millisecond
	t.Cleanup(func() { statsWaitMargin, statsPollInterval = oldMargin, oldPoll })
	ctx := context.Background()

	c := newDeleteCase()
	snapshot := c.r.SnapshotTime
	evidence := func(change func(e *serverEvidence)) *serverEvidence {
		e := &serverEvidence{readTime: snapshot.Add(10 * time.Minute), legacyGone: snapshot.Add(-100 * time.Hour)}
		for _, id := range []string{"srv-0-aa", "srv-1-bb", "srv-2-cc"} {
			s := liveServer(id, snapshot.Add(10*time.Minute))
			s.HeartbeatInterval = time.Second
			e.servers = append(e.servers, s)
		}
		if change != nil {
			change(e)
		}
		return e
	}
	poll := func(reads ...*serverEvidence) ([]string, int) {
		t.Helper()
		f := &fakeServers{reads: reads}
		reasons, err := pollServerStats(ctx, f.read, c.r, c.o)
		require.NoError(t, err)
		return reasons, f.n
	}

	reasons, n := poll(evidence(nil))
	require.Empty(t, reasons)
	require.Equal(t, 1, n)

	// A stats read just after the snapshot is within the clock skew margin.
	late := func(e *serverEvidence) { e.servers[1].StatsTime = snapshot.Add(clockSkewMargin / 2) }
	reasons, n = poll(evidence(late), evidence(late), evidence(nil))
	require.Empty(t, reasons)
	require.Equal(t, 3, n)

	start := time.Now()
	reasons, _ = poll(evidence(late))
	require.Equal(t, []string{"server srv-1-bb wrote no heartbeat with touch stats read after the snapshot within 1s"}, reasons)
	require.GreaterOrEqual(t, time.Since(start), time.Second)
	require.Less(t, time.Since(start), 1800*time.Millisecond)

	// A stopped server records its losses at the stop and is not waited for.
	reasons, _ = poll(evidence(func(e *serverEvidence) {
		late(e)
		e.servers[1].StoppedAt = snapshot.Add(5 * time.Minute)
	}))
	require.Empty(t, reasons)

	// Losses and older servers seen only by the second read refuse at once.
	reasons, n = poll(evidence(func(e *serverEvidence) { e.servers[2].TouchLossTime = snapshot.Add(5 * time.Minute) }))
	require.Len(t, reasons, 1)
	require.Contains(t, reasons[0], "server srv-2-cc lost touches")
	require.Equal(t, 1, n)
	reasons, _ = poll(evidence(func(e *serverEvidence) {
		e.replicas = []Replica{{ID: "old-0-aa", Version: ExtractorVersion, GC: true, TouchInterval: 36 * time.Hour, CacheTTL: 6 * time.Hour, LastSeen: snapshot}}
		e.legacyGone = time.Time{}
	}))
	require.Equal(t, []string{"GcReplicas holds 1 rows of servers of an older version, which keep no evidence of the touches they lose"}, reasons)

	// The live hosts are counted again.
	reasons, _ = poll(evidence(func(e *serverEvidence) { e.servers = e.servers[:1] }))
	require.Equal(t, []string{"1 server hosts write a GC heartbeat, but at least 2 are expected"}, reasons)

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
	reasons, _ = poll(evidence(settings(30*time.Hour, time.Hour)))
	require.Empty(t, reasons)
	tooSeldom := []string{"live mounts touch their objects every 35h0m0s, too seldom for the grace 72h0m0s less the longest touch interval and cache TTL of the servers that run now"}
	reasons, _ = poll(evidence(settings(36*time.Hour, time.Hour)))
	require.Equal(t, tooSeldom, reasons)
	reasons, _ = poll(evidence(settings(30*time.Hour, 6*time.Hour)))
	require.Equal(t, tooSeldom, reasons)
	c.o.Types = []string{"commit"}
	reasons, _ = poll(evidence(settings(36*time.Hour, time.Hour)))
	require.Empty(t, reasons)
	c.o.Types = nil

	// A map row touch lost since the window start, less the 8 h clients
	// reuse map results and the hour a keep-alive pass may take, leaves out
	// those 9 h too: 1.1 x 29.5 h fits 72 h less 30 h, 1 h, a minute and
	// 8 h, but not less the hour as well.
	c.r.LiveMounts.longestTouch = 29*time.Hour + 30*time.Minute
	mapRowLoss := func(at time.Time) func(e *serverEvidence) {
		return func(e *serverEvidence) {
			settings(30*time.Hour, time.Hour)(e)
			e.servers[2].MapRowLossTime = at
		}
	}
	windowStart := snapshot.Add(-c.o.Grace - clockSkewMargin)
	reasons, _ = poll(evidence(mapRowLoss(snapshot.Add(-time.Hour))))
	require.Equal(t, []string{"live mounts touch their objects every 29h30m0s, too seldom for the grace 72h0m0s less the longest touch interval and cache TTL of the servers that run now, and less the 8h0m0s for which clients reuse map results and the 1h0m0s a keep-alive pass may take, since server srv-2-cc lost map row touches at 2026-11-08T11:00:00Z"}, reasons)
	reasons, _ = poll(evidence(mapRowLoss(windowStart.Add(-9 * time.Hour))))
	require.Len(t, reasons, 1)
	reasons, _ = poll(evidence(mapRowLoss(windowStart.Add(-9*time.Hour - time.Second))))
	require.Empty(t, reasons)
	reasons, _ = poll(evidence(settings(30*time.Hour, time.Hour)))
	require.Empty(t, reasons)

	// Accepted mounts without the keep-alive refuse after a map row loss
	// that only the second read sees.
	c.r.LiveMounts = MountCounts{WithoutKeepAlive: 2}
	c.o.AcceptMountsWithoutKeepAlive = true
	reasons, _ = poll(evidence(nil))
	require.Empty(t, reasons)
	reasons, _ = poll(evidence(mapRowLoss(snapshot.Add(5 * time.Minute))))
	require.Equal(t, []string{"2 live mounts run without the GC keep-alive, and server srv-2-cc lost map row touches at 2026-11-08T12:05:00Z: such a mount may apply a map result whose outputs no map row keeps any more, which --accept-mounts-without-keepalive does not accept; set gc-keepalive in their config"}, reasons)
	c.o.Types = []string{"commit"}
	reasons, _ = poll(evidence(mapRowLoss(snapshot.Add(5 * time.Minute))))
	require.Empty(t, reasons)
}

// A map row loss since the window start, less the time clients reuse map
// results and the time a keep-alive pass may take, shortens the keep-alive
// limit by both and refuses nothing else. A row of a server of an older
// version has no map row loss time and records lost map row touches in its
// touch loss time, which keeps the limit and refuses as any lost touch.
func TestServerEvidence_MapRowLoss(t *testing.T) {
	require := require.New(t)
	c := newDeleteCase()
	e, snapshot, grace := c.ev.servers, c.ev.snapshot, c.o.Grace
	limit := grace - 36*time.Hour - 6*time.Hour - clockSkewMargin
	require.Equal(limit, e.keepAliveTouchLimit(snapshot, grace))

	e.servers[1].MapRowLossTime = snapshot.Add(-time.Hour)
	e.servers[2].MapRowLossTime = snapshot.Add(-2 * time.Hour)
	require.Equal(limit-9*time.Hour, e.keepAliveTouchLimit(snapshot, grace))
	s, lost := e.mapRowLoss(snapshot, grace)
	require.True(lost)
	require.False(s.crashed)
	require.Equal("srv-1-bb", s.server)
	require.Empty(windowBlockers(e, snapshot, &c.o.DryRunOpts))

	// A loss of a process that stopped before map row losses count is not
	// counted.
	before := mapRowLossStart(snapshot, grace).Add(-time.Minute)
	e.servers[1] = stoppedServer("srv-1-bb", before)
	e.servers[1].MapRowLossTime = before
	e.servers[2].MapRowLossTime = time.Time{}
	require.Equal(limit, e.keepAliveTouchLimit(snapshot, grace))

	e.servers[1] = liveServer("srv-1-bb", snapshot)
	e.servers[1].TouchLossTime = snapshot.Add(-time.Hour)
	require.Equal(limit, e.keepAliveTouchLimit(snapshot, grace))
	require.Equal([]string{"server srv-1-bb lost touches at 2026-11-08T11:00:00Z, after the window start 2026-11-05T11:59:00Z"}, windowBlockers(e, snapshot, &c.o.DryRunOpts))
}

// A server that did not record a stop may have lost map row touches after its
// last heartbeat that it never recorded, so its row counts as a map row loss
// at the last time it may have run, its last heartbeat plus the age of a live
// row, also when the crash is before the window start. A clean stop without a
// map row loss and a live row do not count.
func TestServerEvidence_CrashCountsAsMapRowLoss(t *testing.T) {
	require := require.New(t)
	c := newDeleteCase()
	e, snapshot, grace := c.ev.servers, c.ev.snapshot, c.o.Grace
	limit := grace - 36*time.Hour - 6*time.Hour - clockSkewMargin
	start, lossStart := windowStart(snapshot, grace), mapRowLossStart(snapshot, grace)

	crash := func(lastSeen time.Time) {
		e.servers[1] = liveServer("srv-1-bb", lastSeen)
		e.servers[1].LastSeen = lastSeen
	}
	crash(start.Add(-liveReplicaAge - time.Hour))
	require.Empty(windowBlockers(e, snapshot, &c.o.DryRunOpts), "the crash is before the window")
	require.Equal(limit-9*time.Hour, e.keepAliveTouchLimit(snapshot, grace))
	loss, lost := e.mapRowLoss(snapshot, grace)
	require.True(lost)
	require.Equal(mapRowLossEvent{server: "srv-1-bb", at: start.Add(-time.Hour), crashed: true}, loss)
	require.Equal(", and less the 8h0m0s for which clients reuse map results and the 1h0m0s a keep-alive pass may take, since server srv-1-bb last wrote its heartbeat at 2026-11-05T10:29:00Z and did not record a stop: it crashed and may have lost map row touches", e.mapRowLossNote(snapshot, grace))

	crash(lossStart.Add(-liveReplicaAge))
	require.Equal(limit-9*time.Hour, e.keepAliveTouchLimit(snapshot, grace))
	crash(lossStart.Add(-liveReplicaAge - time.Second))
	require.Equal(limit, e.keepAliveTouchLimit(snapshot, grace))

	// A failed record of the stop leaves a crashed row; a map row loss the
	// last heartbeat recorded is earlier than the crash.
	crash(start.Add(-liveReplicaAge - time.Hour))
	e.servers[1].MapRowLossTime = e.servers[1].LastSeen
	loss, _ = e.mapRowLoss(snapshot, grace)
	require.Equal(start.Add(-time.Hour), loss.at)

	e.servers[1] = stoppedServer("srv-1-bb", start.Add(-time.Hour))
	require.Equal(limit, e.keepAliveTouchLimit(snapshot, grace), "a clean stop")
	require.Empty(e.mapRowLossNote(snapshot, grace))

	// A live row is not a crash, even when its last heartbeat is long before
	// the snapshot of the cycle.
	e.servers[1] = liveServer("srv-1-bb", snapshot)
	e.readTime = e.servers[1].LastSeen.Add(liveReplicaAge)
	require.Equal(limit, e.keepAliveTouchLimit(snapshot, grace))
	e.readTime = e.servers[1].LastSeen.Add(liveReplicaAge + time.Second)
	require.Equal(limit-9*time.Hour, e.keepAliveTouchLimit(snapshot, grace))
}

// The collector waits for a server's heartbeat at most the largest heartbeat
// interval a server may run with, whatever interval its row holds.
func TestStatsWait(t *testing.T) {
	for _, tt := range []struct {
		interval, want time.Duration
	}{
		{5 * time.Minute, 5*time.Minute + statsWaitMargin},
		{MaxHeartbeatInterval, MaxHeartbeatInterval + statsWaitMargin},
		{time.Hour, MaxHeartbeatInterval + statsWaitMargin},
		{0, MaxHeartbeatInterval + statsWaitMargin},
	} {
		require.Equal(t, tt.want, statsWait(&Server{HeartbeatInterval: tt.interval}), tt.interval)
	}
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

// The evidence of a delete names the list the cycle read and the condemn
// cycle the lease replaced, and carries the servers the dry run read.
func TestNewDeleteEvidence(t *testing.T) {
	require := require.New(t)
	start := time.Now()
	lease := &Lease{Cycle: 7, Start: start, PreviousCondemnCycle: 5}
	state := ListState{Location: "gc/default/list-5", SnapshotTime: start.Add(-time.Hour), Complete: true, CompleteTime: start.Add(-50 * time.Minute)}
	header := &listHeader{cycle: 5, snapshot: state.SnapshotTime}
	servers := &serverEvidence{readTime: start}
	ev := newDeleteEvidence(lease, &condemner{prev: previousList{state: state, list: &condemnedList{header: header}}}, &Result{servers: servers})
	require.Equal(&deleteEvidence{snapshot: start, previousCondemnCycle: 5, list: state, listHeader: header, servers: servers}, ev)

	ev = newDeleteEvidence(lease, &condemner{prev: previousList{state: state, err: "not found"}}, &Result{servers: servers})
	require.Nil(ev.listHeader)
	require.Equal("not found", ev.listErr)
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
