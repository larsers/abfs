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

package ws

import (
	"bytes"
	"context"
	"errors"
	"log"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/google/abfs/binding"
	"github.com/google/abfs/shared/cmd"
	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/instance"
	"github.com/google/abfs/shared/objects"
	"github.com/google/abfs/workspaces"
)

type putRefsCall struct {
	server, project string
	refs            []entities.RefHash
	blocking        bool
}

type fakeRefPutter struct {
	calls []putRefsCall
	err   error
	// before runs at each call, before it is recorded.
	before func()
}

func (f *fakeRefPutter) PutRefs(server string, project string, refs []entities.RefHash, blocking bool) error {
	if f.before != nil {
		f.before()
	}
	f.calls = append(f.calls, putRefsCall{server, project, refs, blocking})
	return f.err
}

// With gc-keepalive, a snapshot gets a ref of its user in the ABFS meta
// project, under the user part that the ACL server lets each user write, and
// the write waits for the server so that a failure is seen. A failed write
// warns and does not fail the command. Without gc-keepalive, and in local-only
// mode, which has no server, nothing is written.
func TestSnapshotCmd_WritesRefWithKeepAlive(t *testing.T) {
	h := objects.HashBlobBytes([]byte("snapshot"))
	want := putRefsCall{
		server:   entities.ABFSMetaServer,
		project:  entities.ABFSMetaProject,
		refs:     []entities.RefHash{{Ref: "refs/abfs/snapshot/user/u/" + h.String(), Hash: h}},
		blocking: true,
	}
	tests := []struct {
		name      string
		keepAlive bool
		localOnly bool
		err       error
		want      []putRefsCall
		warning   string
	}{
		{name: "keep-alive on", keepAlive: true, want: []putRefsCall{want}},
		{name: "write fails", keepAlive: true, err: errors.New("injected"), want: []putRefsCall{want}, warning: "Warning: ref refs/abfs/snapshot/user/u/" + h.String() + " was not confirmed"},
		{name: "keep-alive off"},
		{name: "local only", keepAlive: true, localOnly: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			prev := log.Writer()
			log.SetOutput(&out)
			t.Cleanup(func() { log.SetOutput(prev) })
			config := instance.DefaultConfig(t.TempDir())
			config.GCKeepAlive = tt.keepAlive
			config.LocalOnly = tt.localOnly
			refs := &fakeRefPutter{err: tt.err}

			putSnapshotRef(config, refs, "u", h)
			require.Equal(t, tt.want, refs.calls)
			if tt.warning == "" {
				require.Empty(t, out.String())
			} else {
				require.Contains(t, out.String(), tt.warning)
				require.Contains(t, out.String(), "injected")
			}
		})
	}
}

type fakeObjectStore struct {
	entities.CASStore
	objects map[entities.Hash][]byte
}

func (s *fakeObjectStore) GetObject(_ context.Context, key entities.Hash) ([]byte, error) {
	content, ok := s.objects[key]
	if !ok {
		return nil, entities.ErrNotExist
	}
	return content, nil
}

func (s *fakeObjectStore) PutObject(_ context.Context, key entities.Hash, content []byte) error {
	s.objects[key] = content
	return nil
}

type fakeSnapshotClient struct {
	store      *fakeObjectStore
	restored   []binding.Snapshot
	restoreErr error
}

func (f *fakeSnapshotClient) Snapshot(context.Context) (binding.Snapshot, error) {
	return binding.Snapshot{Root: objects.HashBlobBytes([]byte("root"))}, nil
}

func (f *fakeSnapshotClient) RestoreSnapshot(_ context.Context, snap binding.Snapshot) error {
	if f.restoreErr != nil {
		return f.restoreErr
	}
	f.restored = append(f.restored, snap)
	return nil
}

func (f *fakeSnapshotClient) MountDetails() (*binding.MountDetails, error) {
	md := &binding.MountDetails{Version: binding.CurrentMountDetailsVersion}
	return md, md.Encode()
}

func (f *fakeSnapshotClient) Store() entities.CASStore {
	return f.store
}

// ws snapshot writes the ref of the snapshot it stores, and ws snapshot
// --restore writes the ref of the snapshot it restores, after the restore
// succeeded: a restore that fails writes none, and one that succeeds moves
// the update time of the ref, which keeps the snapshot for another retention
// period.
func TestSnapshotCmd_ExecWritesRef(t *testing.T) {
	newCmd := func() (*snapshotCmd, *fakeSnapshotClient, *fakeRefPutter) {
		config := instance.DefaultConfig(t.TempDir())
		config.GCKeepAlive = true
		abfs := &fakeSnapshotClient{store: &fakeObjectStore{objects: make(map[entities.Hash][]byte)}}
		refs := &fakeRefPutter{}
		c := &snapshotCmd{
			wsCmd: &wsCmd{
				ConfigProvider: &cmd.ConfigProvider{Config: config},
				workspaces:     &workspaces.Workspaces{User: "u"},
			},
			abfs: abfs,
			refs: refs,
		}
		return c, abfs, refs
	}
	refOf := func(h entities.Hash) []entities.RefHash {
		return []entities.RefHash{{Ref: "refs/abfs/snapshot/user/u/" + h.String(), Hash: h}}
	}

	t.Run("snapshot", func(t *testing.T) {
		c, abfs, refs := newCmd()
		require.NoError(t, c.exec(context.Background()))
		require.Len(t, abfs.store.objects, 1)
		var snap entities.Hash
		for h := range abfs.store.objects {
			snap = h
		}
		require.Len(t, refs.calls, 1)
		require.Equal(t, refOf(snap), refs.calls[0].refs)
	})

	stored := func(abfs *fakeSnapshotClient) entities.Hash {
		ws := &binding.WSSnapshot{Version: binding.CurrentWSSnapshotVersion, Snapshot: binding.Snapshot{Root: objects.HashBlobBytes([]byte("old"))}}
		require.NoError(t, ws.Encode())
		abfs.store.objects[ws.Hash] = ws.Encoded
		return ws.Hash
	}

	t.Run("restore", func(t *testing.T) {
		c, abfs, refs := newCmd()
		h := stored(abfs)
		c.restoreHex = h.String()
		refs.before = func() {
			require.Len(t, abfs.restored, 1, "the ref is written after the restore")
		}
		require.NoError(t, c.exec(context.Background()))
		require.Len(t, refs.calls, 1)
		require.Equal(t, refOf(h), refs.calls[0].refs)
	})

	t.Run("restore fails", func(t *testing.T) {
		c, abfs, refs := newCmd()
		c.restoreHex = stored(abfs).String()
		abfs.restoreErr = errors.New("injected")
		require.ErrorContains(t, c.exec(context.Background()), "injected")
		require.Empty(t, refs.calls)
	})
}
