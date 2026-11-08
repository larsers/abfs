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
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	"github.com/stretchr/testify/require"
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
		{"stopped server of the window with a TTL above the cycle", func(c *deleteCase) {
			s := stoppedServer("srv-0-old", c.ev.snapshot.Add(-2*time.Hour))
			s.CacheTTL = 30 * time.Hour
			c.addServer(s)
		}, []string{"plus the largest server cache TTL 30h0m0s", "server srv-0-old: cache TTL 30h0m0s is not shorter than the cycle"}, nil},
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
		{"keep-alive mount with a 40 h touch interval", liveMounts("touch_interval_too_long", 1), []string{"1 live mounts touch their objects less often than the grace allows"}, nil},

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
		{"too many too_new keys", func(c *deleteCase) {
			c.r.Consistency.TooNew = maxTooNew + 1
			c.ev.tooNew = &TooNewReport{Keys: maxTooNew + 1}
		}, []string{"more than 10000 to check again"}, nil},
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
	e.replicas[1].CacheTTL = 30 * time.Hour
	got := liveBlockers(e, &o)
	require.Len(got, 1)
	require.Contains(got[0], "replica old-1-bb: cache TTL 30h0m0s is not shorter than the cycle")
}

func TestDeleteOpts_ValidateTypes(t *testing.T) {
	o := DeleteOpts{CondemnOpts: DefaultCondemnOpts(), Types: []string{"commit", "blob"}}
	o.MinReplicas = 2
	require.NoError(t, o.Validate())
	o.Types = []string{"commits"}
	require.ErrorContains(t, o.Validate(), `invalid object type "commits"`)
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
	// time between its touches.
	c.r.LiveMounts.longestTouch = 35 * time.Hour
	touch := func(d time.Duration) func(e *serverEvidence) {
		return func(e *serverEvidence) {
			for i := range e.servers {
				e.servers[i].TouchInterval = 30 * time.Hour
			}
			e.servers[2].TouchInterval = d
		}
	}
	reasons, _ = poll(evidence(touch(30 * time.Hour)))
	require.Empty(t, reasons)
	reasons, _ = poll(evidence(touch(36 * time.Hour)))
	require.Equal(t, []string{"live mounts touch their objects every 35h0m0s, too seldom for the grace 72h0m0s less the longest touch interval of the servers that run now"}, reasons)
	c.o.Types = []string{"commit"}
	reasons, _ = poll(evidence(touch(36 * time.Hour)))
	require.Empty(t, reasons)
}

// The objects written during the consistency scan are read again: one that
// still has no gc row was written by a server without --gc.
func TestCheckTooNew(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	client := newHooksTestBigtable(t)
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

	rep, err := checkTooNew(ctx, client, all)
	require.NoError(err)
	require.Equal(&TooNewReport{Keys: 251, NotHashes: 1, Gone: 50, WithoutGCRow: 2, Samples: []string{keys[7].String(), keys[201].String()}}, rep)
}

type fakeInstance struct {
	clusters      []string
	probeErr      error
	backups       []Backup
	backupsCalled bool
}

func (f *fakeInstance) Clusters(context.Context) ([]string, error) { return f.clusters, nil }

func (f *fakeInstance) Backups(context.Context) ([]Backup, error) {
	f.backupsCalled = true
	return f.backups, nil
}

func (f *fakeInstance) ProbeConditionalWrite(context.Context) error { return f.probeErr }

// The checks of the instance are read for every delete, and the backups only
// unless the operator skips them.
func TestReadInstanceEvidence(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := &fakeInstance{clusters: []string{"c1"}, probeErr: status.Error(codes.FailedPrecondition, "no transactions")}
	o := &DeleteOpts{}
	e := readInstanceEvidence(ctx, f, o)
	require.True(f.backupsCalled)
	require.True(e.backupsChecked)
	require.Equal([]string{"c1"}, e.clusters)
	require.Equal(codes.FailedPrecondition, status.Code(e.probeErr))
	reasons := instanceBlockers(&e, time.Now())
	require.Len(reasons, 1+len(backupTables))

	f.backupsCalled = false
	o.NoBackupCheck = true
	e = readInstanceEvidence(ctx, f, o)
	require.False(f.backupsCalled)
	require.Len(instanceBlockers(&e, time.Now()), 1)
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

// The consistency scan of a condemn keeps the keys of the objects written
// during it, one more than a delete checks again, and only those.
func TestCheckConsistency_KeepsTooNewKeys(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	client := newHooksTestBigtable(t)
	old := bigtable.Time(time.Now().Add(-time.Hour))
	keys := testHashes(maxTooNew + 5)
	var rows []string
	var muts []*bigtable.Mutation
	meta := func(ts bigtable.Timestamp) *bigtable.Mutation {
		mut := bigtable.NewMutation()
		mut.Set(objectsMetaFamily, "type", ts, []byte("blob"))
		return mut
	}
	for _, k := range keys[2:] {
		rows, muts = append(rows, k.String()), append(muts, meta(bigtable.Now()))
	}
	errs, err := client.Open(objectsTable).ApplyBulk(ctx, rows, muts)
	require.NoError(err)
	require.Nil(errs)
	// One object with a gc row, and one without written before the scan.
	edges := bigtable.NewMutation()
	edges.Set(EdgesFamily, LinksColumn, old, PackEdges(nil))
	edges.Set(EdgesFamily, TypeColumn, old, []byte("blob"))
	edges.Set(EdgesFamily, VersionColumn, old, versionBytes(ExtractorVersion))
	require.NoError(client.Open(TableName).Apply(ctx, keys[0].String(), edges))
	require.NoError(client.Open(objectsTable).Apply(ctx, keys[0].String(), meta(old)))
	require.NoError(client.Open(objectsTable).Apply(ctx, keys[1].String(), meta(old)))

	bounds, err := splitRange("", "", 4)
	require.NoError(err)
	bounds = prefixBounds(bounds)
	g, _, err := scanNodes(ctx, client.Open(TableName), bounds, time.Now().Add(-72*time.Hour))
	require.NoError(err)
	scan, err := checkConsistency(ctx, client.Open(objectsTable), bounds, g, newBitmap(g.size()), time.Now().Add(-time.Minute), true)
	require.NoError(err)
	require.Equal(int64(1), scan.report.MissingGCRows)
	require.Equal(int64(maxTooNew+3), scan.report.TooNew)
	require.Len(scan.tooNew, maxTooNew+1)
	require.NotContains(scan.tooNew, keys[0].String())
	require.NotContains(scan.tooNew, keys[1].String())

	scan, err = checkConsistency(ctx, client.Open(objectsTable), bounds, g, newBitmap(g.size()), time.Now().Add(-time.Minute), false)
	require.NoError(err)
	require.Empty(scan.tooNew)
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
	ev := newDeleteEvidence(lease, &condemner{prevState: state, prev: &condemnedList{header: header}}, &Result{servers: servers})
	require.Equal(&deleteEvidence{snapshot: start, previousCondemnCycle: 5, list: state, listHeader: header, servers: servers}, ev)

	ev = newDeleteEvidence(lease, &condemner{prevState: state, prevErr: "not found"}, &Result{servers: servers})
	require.Nil(ev.listHeader)
	require.Equal("not found", ev.listErr)
}
