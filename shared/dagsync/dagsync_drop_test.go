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

package dagsync_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/google/abfs/shared/dagsync"
	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/objects"
	"github.com/google/abfs/shared/objects/gitobjects"
	"github.com/google/abfs/shared/remote"
	"github.com/google/abfs/shared/remote/servers/inmemory"
)

var (
	lostContent = []byte("in no store and not on the server")
	lostHash    = objects.HashBlobBytes(lostContent)
)

func newDropSync(t *testing.T, updater dagsync.ABFSUpdater, accessor *TestAccessor, cooldown time.Duration) *dagsync.DAGSync {
	t.Helper()
	d := dagsync.New(&dagsync.DagSyncOpts{Updater: updater, Accessor: accessor, WorkersCount: 2})
	d.FailCooldown = cooldown
	d.MaintenanceRunInterval = 10 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		<-stopped
	})
	go func() {
		d.Start(ctx)
		close(stopped)
	}()
	return d
}

// slowFailingUpdater fails every push of one object after a delay until it is turned off.
type slowFailingUpdater struct {
	*inmemory.InMemoryServer
	fail  entities.Hash
	delay time.Duration
	off   atomic.Bool
}

func (u *slowFailingUpdater) PutObject(ctx context.Context, key entities.Hash, r remote.ContentReadCloser) error {
	if key == u.fail && !u.off.Load() {
		time.Sleep(u.delay)
		return errors.New("unavailable")
	}
	return u.InMemoryServer.PutObject(ctx, key, r)
}

// dropRecorder is the callback of a request that may be dropped.
type dropRecorder struct {
	mu    sync.Mutex
	drops int
	at    time.Time
}

func (r *dropRecorder) callback(err error) {
	if !errors.Is(err, dagsync.ErrDropped) {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.drops++
	r.at = time.Now()
}

func (r *dropRecorder) dropped() (int, time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.drops, r.at
}

// busyNodes counts the nodes in a state other than placeholder.
func busyNodes(d *dagsync.DAGSync) int64 {
	var n int64
	for state, counts := range d.Stats().States {
		if state == dagsync.HolderState.String() {
			continue
		}
		for _, c := range counts {
			n += c
		}
	}
	return n
}

func requireState(t *testing.T, d *dagsync.DAGSync, h entities.Hash, want dagsync.NodeState) {
	t.Helper()
	got, ok := d.State(h.String())
	require.True(t, ok, "no node for %s", h)
	require.Equal(t, want, got, "state of %s", h)
}

func onServer(t *testing.T, updater *inmemory.InMemoryServer, h entities.Hash) bool {
	t.Helper()
	hdrs, err := updater.Headers(context.Background(), []entities.Hash{h})
	require.NoError(t, err)
	return !hdrs[0].IsEmpty()
}

func waitStuck(t *testing.T, d *dagsync.DAGSync, tree entities.Hash) {
	t.Helper()
	require.Eventually(t, func() bool {
		treeState, _ := d.State(tree.String())
		childState, _ := d.State(lostHash.String())
		return treeState == dagsync.BlockedState && childState == dagsync.FailedLinkBuildState
	}, 5*time.Second, time.Millisecond)
}

// A push that may be dropped, of a tree whose child is in no store and missing on the
// server, is dropped with its children after its wait. A later push of the tree succeeds once
// the child is there.
func TestDropStuckRequest(t *testing.T) {
	updater := inmemory.New()
	accessor := newTestAccessor()
	d := newDropSync(t, updater, accessor, 10*time.Millisecond)
	accessor.blobs[hash1] = content1
	d.PutBlob(hash1)
	waitForObjects(t, updater, 1, 5*time.Second)
	require.NoError(t, d.Wait(5*time.Second))
	tree := newObject(lostHash, hash1)
	accessor.Lock()
	accessor.objects[tree.Hash] = tree.Encoded
	accessor.Unlock()

	const dropAfter = 300 * time.Millisecond
	rec := &dropRecorder{}
	start := time.Now()
	d.PutObjectOrDrop(tree.Hash, gitobjects.ObjectTypeDir, dropAfter, rec.callback)
	waitStuck(t, d, tree.Hash)

	require.Eventually(t, func() bool { drops, _ := rec.dropped(); return drops > 0 }, 5*time.Second, time.Millisecond)
	_, at := rec.dropped()
	require.GreaterOrEqual(t, at.Sub(start), dropAfter)
	_, ok := d.State(tree.Hash.String())
	require.False(t, ok)
	_, ok = d.State(lostHash.String())
	require.False(t, ok)
	_, ok = d.State(hash1.String())
	require.False(t, ok)
	require.Zero(t, busyNodes(d))
	require.NoError(t, d.Wait(5*time.Second))
	// Retries that were scheduled before the drop change nothing.
	time.Sleep(100 * time.Millisecond)
	drops, _ := rec.dropped()
	require.Equal(t, 1, drops)
	require.Zero(t, busyNodes(d))
	assertSyncIsGood(t, d)

	accessor.Lock()
	accessor.blobs[lostHash] = lostContent
	accessor.Unlock()
	d.PutObjectOrDrop(tree.Hash, gitobjects.ObjectTypeDir, dropAfter, rec.callback)
	require.Eventually(t, func() bool { return onServer(t, updater, tree.Hash) }, 5*time.Second, time.Millisecond)
	require.True(t, onServer(t, updater, lostHash))
	require.NoError(t, d.Wait(5*time.Second))
	drops, _ = rec.dropped()
	require.Equal(t, 1, drops)
	assertSyncIsGood(t, d)
}

// A second missing child, whose retry may be running when the first one drops the tree, is
// dropped at its own next failure.
func TestDropStuckRequestTwoChildren(t *testing.T) {
	updater := inmemory.New()
	accessor := newTestAccessor()
	d := newDropSync(t, updater, accessor, 10*time.Millisecond)
	tree := newObject(lostHash, hash1stFail)
	accessor.objects[tree.Hash] = tree.Encoded

	rec := &dropRecorder{}
	d.PutObjectOrDrop(tree.Hash, gitobjects.ObjectTypeDir, 100*time.Millisecond, rec.callback)
	require.Eventually(t, func() bool { drops, _ := rec.dropped(); return drops > 0 }, 5*time.Second, time.Millisecond)
	require.Eventually(t, func() bool {
		_, lost := d.State(lostHash.String())
		_, other := d.State(hash1stFail.String())
		return !lost && !other && busyNodes(d) == 0
	}, 5*time.Second, time.Millisecond)
	require.NoError(t, d.Wait(5*time.Second))
	drops, _ := rec.dropped()
	require.Equal(t, 1, drops)
	assertSyncIsGood(t, d)
}

// A child that a drop removes while its failed push waits for a retry stays removed when the
// retry is due.
func TestDropFailedChildSkipsScheduledRetry(t *testing.T) {
	// The missing child fails at once and is dropped at its retry 300 ms later. The other
	// child fails after 100 ms, so it waits for its retry when the drop removes it.
	updater := &slowFailingUpdater{InMemoryServer: inmemory.New(), fail: hash1, delay: 100 * time.Millisecond}
	accessor := newTestAccessor()
	accessor.blobs[hash1] = content1
	d := newDropSync(t, updater, accessor, 300*time.Millisecond)
	tree := newObject(lostHash, hash1)
	accessor.objects[tree.Hash] = tree.Encoded

	rec := &dropRecorder{}
	d.PutObjectOrDrop(tree.Hash, gitobjects.ObjectTypeDir, 100*time.Millisecond, rec.callback)
	require.Eventually(t, func() bool {
		s, _ := d.State(hash1.String())
		return s == dagsync.FailedState
	}, 5*time.Second, time.Millisecond)
	require.Eventually(t, func() bool { drops, _ := rec.dropped(); return drops > 0 }, 5*time.Second, time.Millisecond)
	_, ok := d.State(hash1.String())
	require.False(t, ok, "the drop did not remove the failed child")
	updater.off.Store(true)
	require.Eventually(t, func() bool { return busyNodes(d) == 0 }, 5*time.Second, time.Millisecond)
	// Let the scheduled retry come due.
	time.Sleep(600 * time.Millisecond)
	for state, counts := range d.Stats().States {
		for typ, c := range counts {
			require.GreaterOrEqual(t, c, int64(0), "%s %s", state, typ)
		}
	}
	require.Zero(t, busyNodes(d))
	drops, _ := rec.dropped()
	require.Equal(t, 1, drops)
	assertSyncIsGood(t, d)
}

// A child that reaches the store within the wait is pushed as before.
func TestDropWaitsForArrivingChild(t *testing.T) {
	updater := inmemory.New()
	accessor := newTestAccessor()
	d := newDropSync(t, updater, accessor, 10*time.Millisecond)
	tree := newObject(lostHash)
	accessor.objects[tree.Hash] = tree.Encoded

	rec := &dropRecorder{}
	d.PutObjectOrDrop(tree.Hash, gitobjects.ObjectTypeDir, 10*time.Second, rec.callback)
	waitStuck(t, d, tree.Hash)
	time.Sleep(100 * time.Millisecond)
	accessor.Lock()
	accessor.blobs[lostHash] = lostContent
	accessor.Unlock()

	require.Eventually(t, func() bool { return onServer(t, updater, tree.Hash) }, 5*time.Second, time.Millisecond)
	require.NoError(t, d.Wait(5*time.Second))
	drops, _ := rec.dropped()
	require.Zero(t, drops)
	assertSyncIsGood(t, d)
}

// Requests that may not be dropped wait for the missing child as before, and so does a push
// that may be dropped when such a request needs the same child.
func TestDropKeepsOtherRequests(t *testing.T) {
	const dropAfter = 100 * time.Millisecond
	other := newObject(lostHash, hash1)
	tests := []struct {
		name string
		send func(d *dagsync.DAGSync, tree entities.Hash, rec *dropRecorder)
		// stuck lists the nodes that must still wait, besides the child.
		stuck []entities.Hash
	}{
		{
			name: "put tree, then a push that may be dropped",
			send: func(d *dagsync.DAGSync, tree entities.Hash, rec *dropRecorder) {
				d.PutTree(tree)
				d.PutObjectOrDrop(tree, gitobjects.ObjectTypeDir, dropAfter, rec.callback)
			},
			stuck: []entities.Hash{newObject(lostHash).Hash},
		},
		{
			name: "a push that may be dropped, then put tree",
			send: func(d *dagsync.DAGSync, tree entities.Hash, rec *dropRecorder) {
				d.PutObjectOrDrop(tree, gitobjects.ObjectTypeDir, dropAfter, rec.callback)
				d.PutTree(tree)
			},
			stuck: []entities.Hash{newObject(lostHash).Hash},
		},
		{
			name: "other tree with the same child",
			send: func(d *dagsync.DAGSync, tree entities.Hash, rec *dropRecorder) {
				d.PutObjectOrDrop(tree, gitobjects.ObjectTypeDir, dropAfter, rec.callback)
				d.PutTree(other.Hash)
			},
			stuck: []entities.Hash{newObject(lostHash).Hash, other.Hash},
		},
		{
			name: "phony",
			send: func(d *dagsync.DAGSync, tree entities.Hash, rec *dropRecorder) {
				d.Phony(0, []entities.Hash{lostHash}, func(error) {})
				d.PutObjectOrDrop(tree, gitobjects.ObjectTypeDir, dropAfter, rec.callback)
			},
			stuck: []entities.Hash{newObject(lostHash).Hash},
		},
		{
			name: "ref",
			send: func(d *dagsync.DAGSync, tree entities.Hash, rec *dropRecorder) {
				d.PutRefs("server", "project", []entities.RefHash{{Ref: "refs/heads/main", Hash: tree}})
				d.PutObjectOrDrop(tree, gitobjects.ObjectTypeDir, dropAfter, rec.callback)
			},
			stuck: []entities.Hash{newObject(lostHash).Hash},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			updater := inmemory.New()
			accessor := newTestAccessor()
			d := newDropSync(t, updater, accessor, 10*time.Millisecond)
			tree := newObject(lostHash)
			accessor.objects[tree.Hash] = tree.Encoded
			accessor.objects[other.Hash] = other.Encoded
			accessor.blobs[hash1] = content1

			rec := &dropRecorder{}
			tt.send(d, tree.Hash, rec)
			require.Eventually(t, func() bool {
				s, _ := d.State(lostHash.String())
				return s == dagsync.FailedLinkBuildState
			}, 5*time.Second, time.Millisecond)

			time.Sleep(5 * dropAfter)
			requireState(t, d, lostHash, dagsync.FailedLinkBuildState)
			for _, h := range tt.stuck {
				requireState(t, d, h, dagsync.BlockedState)
			}
			drops, _ := rec.dropped()
			require.Zero(t, drops)
			require.NotZero(t, busyNodes(d))
		})
	}
}
