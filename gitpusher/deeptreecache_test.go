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
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	shellgit "github.com/google/abfs/git"
	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/objects/gitobjects"
	"github.com/google/abfs/shared/remote"
	abfsgrpc "github.com/google/abfs/shared/remote/grpc"
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
