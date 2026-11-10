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

package dagsync

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/remote"
)

type dummyRequest struct {
	tag string
}

func (r *dummyRequest) Links(accessor ObjectAccessor) ([]entities.Hash, error) {
	return nil, nil
}
func (r *dummyRequest) Execute(ctx context.Context, updater ABFSUpdater, accessor ObjectAccessor) error {
	return nil
}
func (r *dummyRequest) Type() string {
	return "object"
}
func (r *dummyRequest) String() string {
	return r.tag
}
func (r *dummyRequest) Key() entities.Hash {
	return entities.Hash{}
}

// TestPreventStall_HandleRequestLinked is a regression test to catch a previous stall bug where dagSync
// would spin at the head of the queue
func TestPreventStall_HandleRequestLinked(t *testing.T) {
	d := New(&DagSyncOpts{
		WorkersCount: 0, // prevent any background execution
	})

	reqN := &dummyRequest{tag: "n"}
	reqM := &dummyRequest{tag: "m"}

	d.lock.Lock()
	n := d.node(reqN)
	m := d.node(reqM)

	// Set up n depending on m. Since m is in HolderState (unlinked), n.degree remains 0.
	d.addDependency(n, m)

	// Activate n, sending it to QueuedState since its degree is 0.
	d.activate(n)

	if n.state != QueuedState {
		t.Fatalf("expected n to be in QueuedState, got %s", n.state)
	}
	d.lock.Unlock()

	// Simulate m finishing link building by directly passing a requestLinked message to handle().
	// This executes synchronously under lock inside handle().
	d.handle(&requestLinked{node: m, links: nil, err: nil})

	d.lock.Lock()
	defer d.lock.Unlock()

	// With the prevention fix, n should have automatically transitioned to BlockedState
	// when its degree became > 0. Without the fix, it would remain in QueuedState with degree > 0.
	if n.degree == 0 {
		t.Fatalf("expected n.degree > 0, got %d", n.degree)
	}
	if n.state == QueuedState {
		t.Fatalf("prevention failed: n remained in QueuedState with degree %d", n.degree)
	}
	if n.state != BlockedState {
		t.Fatalf("expected n to transition to BlockedState, got %s", n.state)
	}
}

func TestPreventStall_AddDependency(t *testing.T) {
	d := New(&DagSyncOpts{
		WorkersCount: 0,
	})

	reqN := &dummyRequest{tag: "n"}
	reqM := &dummyRequest{tag: "m"}

	d.lock.Lock()
	defer d.lock.Unlock()

	n := d.node(reqN)
	m := d.node(reqM)

	// Send n to QueuedState
	d.activate(n)
	if n.state != QueuedState {
		t.Fatalf("expected n to be in QueuedState, got %s", n.state)
	}

	// Manually set m to a linked state, e.g. BlockedState
	d.transit(m, BlockedState)

	// Now call addDependency(n, m). Since m is linked, n.degree increments.
	// With the fix, n should immediately transition to BlockedState.
	d.addDependency(n, m)

	if n.degree == 0 {
		t.Fatalf("expected n.degree > 0, got %d", n.degree)
	}
	if n.state == QueuedState {
		t.Fatalf("prevention failed: n remained in QueuedState with degree %d", n.degree)
	}
	if n.state != BlockedState {
		t.Fatalf("expected n to transition to BlockedState, got %s", n.state)
	}
}

// linksRequest is a request with fixed links.
type linksRequest struct {
	dummyRequest
	links []entities.Hash
}

func (r *linksRequest) Links(accessor ObjectAccessor) ([]entities.Hash, error) {
	return r.links, nil
}

// emptyAccessor holds no object.
type emptyAccessor struct{}

func (emptyAccessor) GetObject(ctx context.Context, key entities.Hash) ([]byte, error) {
	return nil, entities.ErrNotExist
}

func (emptyAccessor) OpenBlob(ctx context.Context, key entities.Hash) (ReadSizeCloser, error) {
	return nil, entities.ErrNotExist
}

func requireNoNodeCounted(t *testing.T, d *DAGSync) {
	t.Helper()
	d.lock.Lock()
	defer d.lock.Unlock()
	for state, counts := range d.stateCounts {
		for typ, c := range counts {
			require.Zero(t, c, "%s %s", NodeState(state), typ)
		}
	}
	require.Zero(t, d.droppable)
}

// A link request that the server's report sent for a node that a drop removed before the
// request was handled makes no node.
func TestDropIgnoresLinkRequestOfDroppedNode(t *testing.T) {
	d := New(&DagSyncOpts{Accessor: emptyAccessor{}, WorkersCount: 0})
	stuck := entities.Hash{1}
	missing := entities.Hash{2}
	tree := &linksRequest{dummyRequest: dummyRequest{tag: "tree"}, links: []entities.Hash{stuck}}

	d.handle(&newRequest{request: tree, dropAfter: time.Minute})
	d.lock.Lock()
	n := d.nodes["tree"]
	c := d.nodes[stuck.String()]
	// The tree runs while its child fails to link, and the server reports a second child
	// missing.
	d.transit(c, InitialLinkBuildingState)
	d.transit(n, RunningState)
	d.lock.Unlock()
	d.handle(&executed{node: n, err: &remote.ErrObjectLinkValidation{MissingLinks: []entities.Hash{missing}}})

	// The child's next failure, after it has been missing for longer than the wait, drops
	// the tree with both children before the link request is handled.
	d.lock.Lock()
	c.missingSince = time.Now().Add(-time.Hour)
	d.lock.Unlock()
	d.handle(&requestLinked{node: c, err: entities.ErrNotExist})
	d.lock.Lock()
	require.Empty(t, d.nodes)
	d.lock.Unlock()

	m := <-d.mailbox.Receive()
	r, ok := m.(*newRequest)
	require.True(t, ok)
	require.True(t, r.link)
	require.Equal(t, missing.String(), r.request.String())
	d.handle(m)
	d.lock.Lock()
	require.Empty(t, d.nodes)
	d.lock.Unlock()
	requireNoNodeCounted(t, d)
}

// The count of nodes with a wait follows new requests, waits that a drop passes on and
// deletes.
func TestDropCountsDroppableNodes(t *testing.T) {
	d := New(&DagSyncOpts{Accessor: emptyAccessor{}, WorkersCount: 0})
	stuck := entities.Hash{1}
	busy := entities.Hash{2}
	tree := &linksRequest{dummyRequest: dummyRequest{tag: "tree"}, links: []entities.Hash{stuck, busy}}
	droppable := func() int64 {
		d.lock.Lock()
		defer d.lock.Unlock()
		return d.droppable
	}

	d.handle(&newRequest{request: tree, dropAfter: time.Minute})
	d.handle(&newRequest{request: tree, dropAfter: time.Hour})
	d.handle(&newRequest{request: &dummyRequest{tag: "kept"}})
	require.Equal(t, int64(1), droppable())

	// A push that may be dropped and succeeds is no longer counted.
	pushed := &dummyRequest{tag: "pushed"}
	d.handle(&newRequest{request: pushed, dropAfter: time.Minute})
	require.Equal(t, int64(2), droppable())
	d.lock.Lock()
	p := d.nodes["pushed"]
	d.transit(p, RunningState)
	d.lock.Unlock()
	d.handle(&executed{node: p})
	require.Equal(t, int64(1), droppable())

	// The tree, which waits for its children, is dropped at the failure of one child, and the
	// other child, whose link building is in flight, takes over the wait.
	d.lock.Lock()
	d.transit(d.nodes["tree"], BlockedState)
	s := d.nodes[stuck.String()]
	b := d.nodes[busy.String()]
	d.transit(s, InitialLinkBuildingState)
	d.transit(b, InitialLinkBuildingState)
	s.missingSince = time.Now().Add(-2 * time.Hour)
	d.lock.Unlock()
	d.handle(&requestLinked{node: s, err: entities.ErrNotExist})
	d.lock.Lock()
	require.Nil(t, d.nodes["tree"])
	require.Nil(t, d.nodes[stuck.String()])
	require.Same(t, b, d.nodes[busy.String()])
	require.Equal(t, time.Hour, b.dropAfter)
	d.lock.Unlock()
	require.Equal(t, int64(1), droppable())

	// The other child is dropped at its own failure.
	d.lock.Lock()
	b.missingSince = time.Now().Add(-2 * time.Hour)
	d.lock.Unlock()
	d.handle(&requestLinked{node: b, err: entities.ErrNotExist})
	d.lock.Lock()
	require.Nil(t, d.nodes[busy.String()])
	require.NotNil(t, d.nodes["kept"])
	d.lock.Unlock()
	require.Zero(t, droppable())
}

// A push that is sent again while it waits adds no callback, and a drop at a failed
// execution calls the callback once, with ErrDropped.
func TestDropCallsCallbackOnce(t *testing.T) {
	d := New(&DagSyncOpts{Accessor: emptyAccessor{}, WorkersCount: 0})
	errs := make(chan error, 4)
	callback := func(err error) { errs <- err }
	blob := &dummyRequest{tag: "blob"}

	d.handle(&newRequest{request: blob, dropAfter: time.Minute, callbacks: []Callback{callback}})
	d.handle(&newRequest{request: blob, dropAfter: time.Minute, callbacks: []Callback{callback}})
	d.lock.Lock()
	n := d.nodes["blob"]
	d.transit(n, RunningState)
	n.missingSince = time.Now().Add(-time.Hour)
	d.lock.Unlock()
	d.handle(&executed{node: n, err: entities.ErrNotExist})

	require.ErrorIs(t, <-errs, ErrDropped)
	select {
	case err := <-errs:
		t.Fatalf("callback called again with %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	requireNoNodeCounted(t, d)
}
