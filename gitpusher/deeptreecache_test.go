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

package gitpusher

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/google/abfs/binding"
	shellgit "github.com/google/abfs/git"
	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/objects"
	"github.com/google/abfs/shared/objects/gitobjects"
	"github.com/google/abfs/shared/remote"
	abfsgrpc "github.com/google/abfs/shared/remote/grpc"
	"github.com/google/abfs/shared/remote/servers/inmemory"
)

// linkCheckingStore rejects a deep tree that links a deep tree it does not
// hold, the way the server validates the direct links of a new object, and
// returns the error as the gRPC client does.
type linkCheckingStore struct {
	entities.CASStore
	mu      sync.Mutex
	objects map[entities.Hash][]byte
}

func (f *linkCheckingStore) PutObject(ctx context.Context, key entities.Hash, content []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if objType, _, err := entities.DecodeObject(content); err == nil && objType == string(gitobjects.ObjectTypeDeepTree) {
		dt := &gitobjects.DeepTreeObject{}
		if err := dt.Decode(key, content); err != nil {
			return err
		}
		var missing []entities.Hash
		for _, e := range dt.Entries {
			if _, ok := f.objects[e.Hash]; e.Mode == entities.FileModeDeepTree && !ok {
				missing = append(missing, e.Hash)
			}
		}
		if len(missing) > 0 {
			st, err := abfsgrpc.LinkValidationToStatus(&remote.ErrObjectLinkValidation{MissingLinks: missing})
			if err != nil {
				return err
			}
			return abfsgrpc.UnwrapMissingLinkError(st.Err())
		}
	}
	f.objects[key] = content
	return nil
}

func (f *linkCheckingStore) MapResults(ctx context.Context, id entities.Hash) ([]entities.Hash, error) {
	return nil, nil
}

func (f *linkCheckingStore) remove(keys ...entities.Hash) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, k := range keys {
		delete(f.objects, k)
	}
}

type mapResultRemote struct {
	remote.RemoteCloser
}

func (mapResultRemote) AddMapResult(ctx context.Context, from, to entities.Hash) error {
	return nil
}

func TestPusher_ForgetsMissingDeepTree(t *testing.T) {
	require := require.New(t)
	tempDir := t.TempDir()

	// foo/bar and foo each hold more than DeepTreeThreshold entries, so bar
	// gets a deep tree and foo gets one that links it.
	fakeGitPath := filepath.Join(tempDir, "fake_git")
	fakeGitScript := `#!/bin/bash
if [[ "$1" == "ls-tree" ]]; then
	printf "040000 tree 1111111111111111111111111111111111111111 -\tfoo\0"
	printf "040000 tree 2222222222222222222222222222222222222222 -\tfoo/bar\0"
	for i in $(seq 1 1005); do
		printf "100644 blob e69de29bb2d1d6434b8b29ae775ad8c2e48c5391 1\tfoo/bar/file_$i.txt\0"
		printf "100644 blob e69de29bb2d1d6434b8b29ae775ad8c2e48c5391 1\tfoo/file_$i.txt\0"
	done
	exit 0
fi
exit 0
`
	require.NoError(os.WriteFile(fakeGitPath, []byte(fakeGitScript), 0755))
	repoPath := filepath.Join(tempDir, "repo.git")
	require.NoError(os.MkdirAll(repoPath, 0755))

	store := &linkCheckingStore{objects: make(map[entities.Hash][]byte)}
	p := &pusher{
		config:      &ProjectConfig{},
		dirPath:     repoPath,
		shgit:       &shellgit.ShellGit{BinPath: fakeGitPath},
		store:       store,
		abfsClient:  mapResultRemote{},
		deepTreeMap: make(map[entities.Hash]entities.Hash),
	}
	ctx := context.Background()
	fooTree := entities.NewHash("1111111111111111111111111111111111111111")
	barTree := entities.NewHash("2222222222222222222222222222222222222222")
	refs := []string{"refs/heads/main"}

	require.NoError(p.saveDeepTree(ctx, refs, []entities.Hash{entities.NewHash("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")}))
	fooDT, ok := p.deepTreeMap[fooTree]
	require.True(ok)
	barDT, ok := p.deepTreeMap[barTree]
	require.True(ok)

	// The server removed both deep trees. The next root links the cached foo
	// deep tree and is rejected.
	store.remove(fooDT, barDT)
	nextRoot := entities.NewHash("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	err := p.saveDeepTree(ctx, refs, []entities.Hash{nextRoot})
	var linkErr *remote.ErrObjectLinkValidation
	require.ErrorAs(err, &linkErr)
	require.Equal([]entities.Hash{fooDT}, linkErr.MissingLinks)
	require.Empty(p.deepTreeMap)

	// The next push stores foo and the bar deep tree nested in it again.
	require.NoError(p.saveDeepTree(ctx, refs, []entities.Hash{nextRoot}))
	require.Contains(store.objects, fooDT)
	require.Contains(store.objects, barDT)
	require.Equal(fooDT, p.deepTreeMap[fooTree])
	require.Equal(barDT, p.deepTreeMap[barTree])
}

// failingStore fails every PutObject with err.
type failingStore struct {
	entities.CASStore
	err error
}

func (f failingStore) PutObject(ctx context.Context, key entities.Hash, content []byte) error {
	return f.err
}

func TestPusher_KeepsDeepTreesOnOtherErrors(t *testing.T) {
	tree := entities.NewHash("1111111111111111111111111111111111111111")
	cached := entities.NewHash("2222222222222222222222222222222222222222")
	p := &pusher{
		store:       failingStore{err: abfsgrpc.UnwrapMissingLinkError(status.Error(codes.Unavailable, "unavailable"))},
		deepTreeMap: map[entities.Hash]entities.Hash{tree: cached},
	}
	_, err := p.putDeepTree(context.Background(), []*entities.DeepEntry{{Mode: entities.FileModeDir, Hash: entities.NewHash("3333333333333333333333333333333333333333")}})
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Equal(t, map[entities.Hash]entities.Hash{tree: cached}, p.deepTreeMap)
}

// hidingStore stores in a remote, except that it rejects an object that links
// a hidden object, the way a server rejects a link to an object it deleted. A
// put of a hidden object stores it again.
type hidingStore struct {
	entities.CASStore
	mu     sync.Mutex
	hidden map[entities.Hash]bool
}

func (f *hidingStore) hide(keys ...entities.Hash) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, k := range keys {
		f.hidden[k] = true
	}
}

func (f *hidingStore) PutObject(ctx context.Context, key entities.Hash, content []byte) error {
	f.mu.Lock()
	delete(f.hidden, key)
	var missing []entities.Hash
	if links, err := objects.Factory.Links(key, content); err == nil {
		for _, l := range links {
			if f.hidden[l] {
				missing = append(missing, l)
			}
		}
	}
	f.mu.Unlock()
	if len(missing) > 0 {
		return &remote.ErrObjectLinkValidation{MissingLinks: missing}
	}
	return f.CASStore.PutObject(ctx, key, content)
}

func runGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.com", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.com")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "git %v: %s", args, out)
}

func gitOutput(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.Output()
	require.NoError(t, err, "git %v", args)
	return string(out[:len(out)-1])
}

// A push whose deep tree the server rejects has already recorded the ref as
// pushed, so the next push must save the commit's helper objects again.
func TestPusher_RetriesHelperObjectsOfAFailedPush(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found")
	}
	require := require.New(t)
	ctx := context.Background()
	tempDir := t.TempDir()

	// foo/bar and foo each hold more than DeepTreeThreshold entries, so bar
	// gets a deep tree and foo gets one that links it.
	src := filepath.Join(tempDir, "src")
	require.NoError(os.MkdirAll(filepath.Join(src, "foo", "bar"), 0755))
	for i := range binding.DeepTreeThreshold + 5 {
		for _, dir := range []string{"foo", "foo/bar"} {
			require.NoError(os.WriteFile(filepath.Join(src, dir, fmt.Sprintf("file_%d.txt", i)), nil, 0644))
		}
	}
	runGit(t, src, "init", "-q", "-b", "main")
	runGit(t, src, "add", ".")
	runGit(t, src, "commit", "-q", "-m", "first")

	server := inmemory.New()
	store := &hidingStore{CASStore: remote.NewRemoteStore(server), hidden: make(map[entities.Hash]bool)}
	dirPath := filepath.Join(tempDir, "pusher", "project.git")
	p := &pusher{
		config: &ProjectConfig{Server: "server", Project: "project", URL: src, FetchConfig: FetchConfig{
			FetchRefSpecs:    []string{"+refs/heads/*:refs/heads/*"},
			Workers:          4,
			PackPushInterval: time.Hour,
		}},
		remoteName:     defaultRemoteName,
		abfsClient:     server,
		store:          store,
		dirPath:        dirPath,
		pushedRefsFile: path.Join(dirPath, pushedRefsFile),
		state:          PushStateCreated,
		shgit:          &shellgit.ShellGit{BinPath: "git"},
		deepTreeMap:    make(map[entities.Hash]entities.Hash),
		lastPackPushed: time.Now(),
	}
	require.NoError(p.Push(ctx))
	fooTree := entities.NewHash(gitOutput(t, src, "rev-parse", "HEAD:foo"))
	barTree := entities.NewHash(gitOutput(t, src, "rev-parse", "HEAD:foo/bar"))
	fooDT, ok := p.deepTreeMap[fooTree]
	require.True(ok)
	barDT, ok := p.deepTreeMap[barTree]
	require.True(ok)

	// The server deleted both deep trees. The next commit keeps foo, so its
	// root deep tree links the cached foo deep tree and is rejected.
	store.hide(fooDT, barDT)
	require.NoError(os.WriteFile(filepath.Join(src, "top.txt"), []byte("top"), 0644))
	runGit(t, src, "add", ".")
	runGit(t, src, "commit", "-q", "-m", "second")
	var linkErr *remote.ErrObjectLinkValidation
	require.ErrorAs(p.Push(ctx), &linkErr)
	require.Equal([]entities.Hash{fooDT}, linkErr.MissingLinks)

	root := entities.NewHash(gitOutput(t, src, "rev-parse", "HEAD^{tree}"))
	deepTreeRequest := &binding.DeepTreeRequest{Version: binding.CurrentDeepTreeRequestVersion, Tree: root}
	require.NoError(deepTreeRequest.Encode())
	digestMapRequest := &binding.DigestMapRequest{Version: binding.CurrentDigestMapRequestVersion, Tree: root}
	require.NoError(digestMapRequest.Encode())
	results, err := server.MapResults(ctx, deepTreeRequest.Hash)
	require.NoError(err)
	require.Empty(results)

	// The ref did not move, and the next push stores the deep tree of the
	// commit, with foo and bar again, and its digest map.
	require.NoError(p.Push(ctx))
	results, err = server.MapResults(ctx, deepTreeRequest.Hash)
	require.NoError(err)
	require.Len(results, 1)
	require.Empty(store.hidden)
	require.Equal(fooDT, p.deepTreeMap[fooTree])
	results, err = server.MapResults(ctx, digestMapRequest.Hash)
	require.NoError(err)
	require.Len(results, 1)
	require.Empty(p.helperRetry)
}
