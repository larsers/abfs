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
	"crypto/sha1"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/google/abfs/binding"
	"github.com/google/abfs/manifest"
	"github.com/google/abfs/shared/cmd"
	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/instance"
	"github.com/google/abfs/shared/objects"
	"github.com/google/abfs/shared/remote"
	"github.com/google/abfs/shared/remote/grpc/client"
	"github.com/google/abfs/shared/remote/grpc/server"
	"github.com/google/abfs/shared/remote/servers/filebased"
	"github.com/google/abfs/shared/remote/servers/inmemory"
	"github.com/google/abfs/store"
	"github.com/google/abfs/workspaces"
)

type fakeManifestHashes struct {
	called           bool
	manifest, pinned entities.Hash
}

func (f *fakeManifestHashes) SetManifestHashes(manifest, pinned entities.Hash) {
	f.called = true
	f.manifest, f.pinned = manifest, pinned
}

func TestSetPinnedManifest(t *testing.T) {
	content := []byte(`<manifest><project name="p" revision="0123456789abcdef0123456789abcdef01234567"/></manifest>`)
	manFile := filepath.Join(t.TempDir(), "pinned.xml")
	require.NoError(t, os.WriteFile(manFile, content, 0644))
	fileHash := objects.HashBlobBytes(content)
	givenHash := objects.HashBlobBytes([]byte("given"))

	tests := []struct {
		name      string
		opts      MountRepoOpts
		keepAlive bool
		want      entities.Hash
	}{
		{name: "manifest hash", opts: MountRepoOpts{ManifestHash: givenHash}, keepAlive: true, want: givenHash},
		{name: "manifest file", opts: MountRepoOpts{ManifestFile: manFile}, keepAlive: true, want: fileHash},
		{name: "unreadable snapshot", opts: MountRepoOpts{ManifestHash: givenHash, IsSnapshot: true}, keepAlive: true},
		{name: "keep-alive off", opts: MountRepoOpts{ManifestFile: manFile}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			ctx := context.Background()
			srv := inmemory.New()
			casStore := remote.NewRemoteStore(srv)
			config := instance.DefaultConfig(t.TempDir())
			config.GCKeepAlive = tt.keepAlive
			c := &mountRepoCmd{
				wsCmd: &wsCmd{
					StoreProvider:  &cmd.StoreProvider{Store: casStore},
					ConfigProvider: &cmd.ConfigProvider{Config: config},
				},
				MountRepoOpts: tt.opts,
			}
			var got fakeManifestHashes
			require.NoError(c.setPinnedManifest(ctx, &got))
			if tt.want.IsZero() {
				require.False(got.called)
				// Nothing is stored either.
				hdrs, err := srv.Headers(ctx, []entities.Hash{fileHash})
				require.NoError(err)
				require.True(hdrs[0].IsEmpty())
				return
			}
			require.Equal(tt.want, got.manifest)
			require.Equal(tt.want, got.pinned)
			if tt.opts.ManifestFile != "" {
				stored, err := store.ReadAllBlob(context.Background(), casStore, fileHash)
				require.NoError(err)
				require.Equal(content, stored)
			}
		})
	}
}

const (
	testManifestServer  = "gerrit.example.com"
	testManifestProject = "platform/manifest"
	testBranch          = "main"
)

// newTestPinServer starts a file-based server over gRPC, so refs are looked up
// by host as a client does, and stores a manifest branch whose default.xml
// lists platform/a and, in group "extra", platform/b. Only the projects in
// pushed get a branch ref. It returns the client and the commit of each
// project.
func newTestPinServer(t *testing.T, pushed ...string) (*client.ABFSClient, map[string]entities.Hash) {
	require := require.New(t)
	fserver, err := filebased.New(t.TempDir())
	require.NoError(err)
	s, err := server.NewGRPCServer(&server.GRPCServerOpts{Server: fserver})
	require.NoError(err)
	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(err)
	go func() { _ = s.Serve(lis) }()
	c, err := client.NewABFSClient(&client.ABFSClientOpts{Servers: []string{lis.Addr().String()}, AuthInfo: instance.CreateAuthInfo(instance.AuthTypeNone, "", "", true)})
	require.NoError(err)
	t.Cleanup(func() {
		_ = c.Close()
		s.Stop()
		_ = fserver.Close()
	})

	ctx := context.Background()
	xml := `<manifest>
  <remote name="origin" fetch=".." />
  <default revision="main" remote="origin" />
  <project name="platform/a" path="a" />
  <project name="platform/b" path="b" groups="extra" />
</manifest>`
	manifestCommit := putTestCommit(t, c, "default.xml", []byte(xml))
	require.NoError(c.PutRefs(ctx, testManifestServer, testManifestProject, []entities.RefHash{{Ref: "refs/heads/" + testBranch, Hash: manifestCommit}}))
	commits := make(map[string]entities.Hash)
	for _, p := range pushed {
		commits[p] = putTestCommit(t, c, "README", []byte(p))
		require.NoError(c.PutRefs(ctx, testManifestServer, p, []entities.RefHash{{Ref: "refs/heads/" + testBranch, Hash: commits[p]}}))
	}
	return c, commits
}

func putTestObject(t *testing.T, s remote.RemoteProvider, objType string, content []byte) entities.Hash {
	var buf bytes.Buffer
	require.NoError(t, entities.WriteObject(objType, content, &buf))
	key := entities.Hash(sha1.Sum(buf.Bytes()))
	require.NoError(t, s.PutObject(context.Background(), key, remote.NewReaderFromBytes(buf.Bytes())))
	return key
}

// putTestCommit stores a commit whose tree holds one file.
func putTestCommit(t *testing.T, s remote.RemoteProvider, name string, content []byte) entities.Hash {
	blob := putTestObject(t, s, "blob", content)
	var tree bytes.Buffer
	tree.WriteString("100644 " + name + "\x00")
	tree.Write(blob[:])
	treeHash := putTestObject(t, s, "tree", tree.Bytes())
	return putTestObject(t, s, "commit", fmt.Appendf(nil, "tree %s\nauthor A <a@example.com> 1 +0000\ncommitter A <a@example.com> 1 +0000\n\n%s\n", treeHash, name))
}

func newTestBranchCmd(t *testing.T, s entities.CASStore, opts MountRepoOpts, keepAlive bool) *mountRepoCmd {
	config := instance.DefaultConfig(t.TempDir())
	config.ManifestServer = testManifestServer
	config.ManifestProjectName = testManifestProject
	config.ManifestScheme = "https"
	config.GCKeepAlive = keepAlive
	return &mountRepoCmd{
		wsCmd: &wsCmd{
			StoreProvider:  &cmd.StoreProvider{Store: s},
			ConfigProvider: &cmd.ConfigProvider{Config: config},
		},
		MountRepoOpts: opts,
	}
}

func TestSetPinnedManifest_Branch(t *testing.T) {
	c, commits := newTestPinServer(t, "platform/a", "platform/b")
	s := remote.NewRemoteStore(c)

	tests := []struct {
		name      string
		opts      MountRepoOpts
		keepAlive bool
		wantPin   bool
	}{
		{name: "branch", opts: MountRepoOpts{Branch: testBranch}, keepAlive: true, wantPin: true},
		// Smart sync and smart tag check out other revisions than the branch tips.
		{name: "smart sync", opts: MountRepoOpts{Branch: testBranch, SmartSync: true}, keepAlive: true},
		{name: "smart tag", opts: MountRepoOpts{Branch: testBranch, SmartTag: "green"}, keepAlive: true},
		// Without gc-keepalive a client sends nothing extra.
		{name: "keep-alive off", opts: MountRepoOpts{Branch: testBranch}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			var got fakeManifestHashes
			require.NoError(newTestBranchCmd(t, s, tt.opts, tt.keepAlive).setPinnedManifest(context.Background(), &got))
			if !tt.wantPin {
				require.False(got.called)
				return
			}
			require.Equal(got.pinned, got.manifest)
			content, err := store.ReadAllBlob(context.Background(), s, got.pinned)
			require.NoError(err)
			pinned, err := manifest.Parse(content)
			require.NoError(err)
			revisions := make(map[string]string)
			for _, p := range pinned.Project {
				revisions[p.Name] = p.Revision
			}
			require.Equal(map[string]string{
				"platform/a": commits["platform/a"].String(),
				"platform/b": commits["platform/b"].String(),
			}, revisions)
		})
	}
}

// A branch whose manifest names a project that is not on the server still
// mounts, and pins every project that resolves. A project left out by the
// groups is dropped from the pin; one in the groups keeps the branch name,
// which the collector counts in ignored_revisions.
func TestSetPinnedManifest_BranchWithUnresolvedProject(t *testing.T) {
	c, commits := newTestPinServer(t, "platform/a")
	s := remote.NewRemoteStore(c)
	other := putTestCommit(t, c, "other.xml", []byte(`<manifest><remote name="origin" fetch=".." /><default revision="main" remote="origin" /><project name="platform/a" /></manifest>`))
	require.NoError(t, c.PutRefs(context.Background(), testManifestServer, testManifestProject, []entities.RefHash{{Ref: "refs/heads/other", Hash: other}}))

	tests := []struct {
		name       string
		branch     string
		groups     []string
		branchFile string
		want       map[string]string
	}{
		{name: "all groups", want: map[string]string{"platform/a": commits["platform/a"].String(), "platform/b": "main"}},
		{name: "groups without the project", groups: []string{"default", "-extra"}, want: map[string]string{"platform/a": commits["platform/a"].String()}},
		{name: "branch file", branch: "other", branchFile: "other.xml", want: map[string]string{"platform/a": commits["platform/a"].String()}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			var got fakeManifestHashes
			branch := testBranch
			if tt.branch != "" {
				branch = tt.branch
			}
			cmd := newTestBranchCmd(t, s, MountRepoOpts{Branch: branch, Groups: tt.groups, BranchFile: tt.branchFile}, true)
			require.NoError(cmd.setPinnedManifest(context.Background(), &got))
			require.True(got.called)
			content, err := store.ReadAllBlob(context.Background(), s, got.pinned)
			require.NoError(err)
			pinned, err := manifest.Parse(content)
			require.NoError(err)
			revisions := make(map[string]string)
			for _, p := range pinned.Project {
				revisions[p.Name] = p.Revision
			}
			require.Equal(tt.want, revisions)
		})
	}
}

func TestSnapshotPin(t *testing.T) {
	ctx := context.Background()
	c, _ := newTestPinServer(t)
	s := remote.NewRemoteStore(c)

	pinnedHash, _, err := store.StoreBlob(ctx, s, []byte("<manifest/>"))
	require.NoError(t, err)
	details := &binding.MountDetails{Version: binding.CurrentMountDetailsVersion, ManifestHash: pinnedHash, PinnedManifestHash: pinnedHash}
	require.NoError(t, details.Encode())
	require.NoError(t, s.PutObject(ctx, details.Hash, details.Encoded))
	root := putTestObject(t, c, "tree", nil)
	snap := &binding.WSSnapshot{Version: binding.CurrentWSSnapshotVersion, MountDetails: details.Hash, Snapshot: binding.Snapshot{Root: root}}
	ws := &workspaces.Workspaces{User: "u", Store: s}
	require.NoError(t, snap.Encode())
	require.NoError(t, s.PutObject(ctx, snap.Hash, snap.Encoded))
	require.NoError(t, s.PutRefs(entities.ABFSMetaServer, entities.ABFSMetaProject, []entities.RefHash{{Ref: "refs/abfs/ws/user/u/w", Hash: snap.Hash}}))

	t.Run("mount-repo of a snapshot", func(t *testing.T) {
		require := require.New(t)
		var got fakeManifestHashes
		cmd := newTestBranchCmd(t, s, MountRepoOpts{ManifestHash: snap.Hash, IsSnapshot: true}, true)
		require.NoError(cmd.setPinnedManifest(ctx, &got))
		require.Equal(pinnedHash, got.pinned)
		require.Equal(pinnedHash, got.manifest)
	})
	t.Run("mount of a workspace", func(t *testing.T) {
		require := require.New(t)
		var got fakeManifestHashes
		cmd := &mountCmd{wsCmd: newTestBranchCmd(t, s, MountRepoOpts{}, true).wsCmd, MountOpts: MountOpts{WSName: "w"}}
		cmd.workspaces = ws
		cmd.setSnapshotPin(ctx, &got)
		require.Equal(pinnedHash, got.pinned)
	})
}
