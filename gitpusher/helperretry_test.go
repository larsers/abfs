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
	"github.com/google/abfs/shared/remote"
	"github.com/google/abfs/shared/remote/servers/inmemory"
)

// hidingStore stores in a remote, except that it rejects an object that links
// a hidden object, the way a server rejects a link to an object it deleted. A
// put of a hidden object stores it again. While err is set, every put fails
// with it.
type hidingStore struct {
	entities.CASStore
	mu     sync.Mutex
	hidden map[entities.Hash]bool
	err    error
	puts   int
}

func (f *hidingStore) hide(keys ...entities.Hash) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, k := range keys {
		f.hidden[k] = true
	}
}

func (f *hidingStore) failWith(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

// takePuts returns the number of puts since the last call.
func (f *hidingStore) takePuts() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := f.puts
	f.puts = 0
	return n
}

func (f *hidingStore) PutObject(ctx context.Context, key entities.Hash, content []byte) error {
	f.mu.Lock()
	f.puts++
	if f.err != nil {
		f.mu.Unlock()
		return f.err
	}
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

// helperFixture is a pusher of a git repository whose tree has deep trees
// for foo and foo/bar, after a first push.
type helperFixture struct {
	t       *testing.T
	src     string
	server  remote.RemoteCloser
	store   *hidingStore
	p       *pusher
	commits int
	fooTree entities.Hash
	// root is the root tree of the newest commit.
	root entities.Hash
}

func newHelperFixture(t *testing.T) *helperFixture {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found")
	}
	require := require.New(t)
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

	server := inmemory.New()
	store := &hidingStore{CASStore: remote.NewRemoteStore(server), hidden: make(map[entities.Hash]bool)}
	dirPath := filepath.Join(tempDir, "pusher", "project.git")
	f := &helperFixture{t: t, src: src, server: server, store: store}
	f.p = &pusher{
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
	f.commit()
	require.NoError(f.p.Push(context.Background()))
	f.fooTree = entities.NewHash(gitOutput(t, src, "rev-parse", "HEAD:foo"))
	require.True(f.hasDeepTree(f.root))
	return f
}

// commit commits a change of a file at the top of the tree, which keeps foo.
func (f *helperFixture) commit() {
	f.t.Helper()
	f.commits++
	require.NoError(f.t, os.WriteFile(filepath.Join(f.src, "top.txt"), fmt.Appendf(nil, "%d", f.commits), 0644))
	runGit(f.t, f.src, "add", ".")
	runGit(f.t, f.src, "commit", "-q", "-m", fmt.Sprint(f.commits))
	f.root = entities.NewHash(gitOutput(f.t, f.src, "rev-parse", "HEAD^{tree}"))
}

// rejectNextCommit pushes a new commit whose root deep tree the server
// rejects, because it deleted the deep trees of foo and foo/bar that the
// pusher cached, and returns the foo deep tree.
func (f *helperFixture) rejectNextCommit() entities.Hash {
	f.t.Helper()
	require := require.New(f.t)
	fooDT, ok := f.p.deepTreeMap[f.fooTree]
	require.True(ok)
	barDT, ok := f.p.deepTreeMap[entities.NewHash(gitOutput(f.t, f.src, "rev-parse", "HEAD:foo/bar"))]
	require.True(ok)
	f.store.hide(fooDT, barDT)
	f.commit()
	var linkErr *remote.ErrObjectLinkValidation
	require.ErrorAs(f.p.Push(context.Background()), &linkErr)
	require.Equal([]entities.Hash{fooDT}, linkErr.MissingLinks)
	require.False(f.hasDeepTree(f.root))
	return fooDT
}

func (f *helperFixture) hasDeepTree(tree entities.Hash) bool {
	f.t.Helper()
	request := &binding.DeepTreeRequest{Version: binding.CurrentDeepTreeRequestVersion, Tree: tree}
	require.NoError(f.t, request.Encode())
	return f.hasMapResult(request.Hash)
}

func (f *helperFixture) hasDigestMap(tree entities.Hash) bool {
	f.t.Helper()
	request := &binding.DigestMapRequest{Version: binding.CurrentDigestMapRequestVersion, Tree: tree}
	require.NoError(f.t, request.Encode())
	return f.hasMapResult(request.Hash)
}

func (f *helperFixture) hasMapResult(request entities.Hash) bool {
	f.t.Helper()
	results, err := f.server.MapResults(context.Background(), request)
	require.NoError(f.t, err)
	return len(results) > 0
}

// A push whose deep tree the server rejects has already recorded the ref as
// pushed, so the next push must save the commit's helper objects again.
func TestPusher_RetriesHelperObjectsOfAFailedPush(t *testing.T) {
	require := require.New(t)
	f := newHelperFixture(t)
	fooDT := f.rejectNextCommit()

	// The ref did not move, and the next push stores the deep tree of the
	// commit, with foo and bar again, and its digest map.
	require.NoError(f.p.Push(context.Background()))
	require.True(f.hasDeepTree(f.root))
	require.Empty(f.store.hidden)
	require.Equal(fooDT, f.p.deepTreeMap[f.fooTree])
	require.True(f.hasDigestMap(f.root))
	require.Empty(f.p.helperRetry)
}

// A helper save that fails for another reason than a missing link fails its
// push only: the next pushes of the unchanged ref succeed without the helper
// objects and do not build them again.
func TestPusher_OtherHelperFailuresFailOnlyTheirPush(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newHelperFixture(t)
	f.store.failWith(&remote.ErrMessageTooLarge{Size: 5 << 20, MaxSize: 4 << 20})
	f.commit()
	var tooLarge *remote.ErrMessageTooLarge
	require.ErrorAs(f.p.Push(ctx), &tooLarge)
	require.Empty(f.p.helperRetry)
	f.store.takePuts()
	for range 2 {
		require.NoError(f.p.Push(ctx))
		require.Zero(f.store.takePuts())
	}
	require.False(f.hasDeepTree(f.root))
}

// A retry that fails never fails a push. The commit stays for later pushes
// until it reached maxHelperRetries, and then costs nothing more.
func TestPusher_RetryFailuresDoNotFailAPush(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	f := newHelperFixture(t)
	f.rejectNextCommit()
	head := entities.NewHash(gitOutput(t, f.src, "rev-parse", "HEAD"))
	f.store.failWith(status.Error(codes.Unavailable, "unavailable"))
	for attempt := 1; attempt < maxHelperRetries; attempt++ {
		require.NoError(f.p.Push(ctx))
		require.Positive(f.store.takePuts())
		require.Equal(helperRetry{ref: "refs/heads/main", attempts: attempt}, f.p.helperRetry[head])
	}
	require.NoError(f.p.Push(ctx))
	require.Empty(f.p.helperRetry)
	f.store.takePuts()
	require.NoError(f.p.Push(ctx))
	require.Zero(f.store.takePuts())
	require.False(f.hasDeepTree(f.root))
}

// A ref that moved after a rejected save drops the retry of its old commit,
// which no ref points at any more; the new commit gets the helper objects.
func TestPusher_DropsTheRetryOfAMovedRef(t *testing.T) {
	require := require.New(t)
	f := newHelperFixture(t)
	f.rejectNextCommit()
	old := f.root
	f.commit()
	require.NoError(f.p.Push(context.Background()))
	require.True(f.hasDeepTree(f.root))
	require.False(f.hasDeepTree(old))
	require.Empty(f.p.helperRetry)
}
