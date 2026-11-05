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
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/google/abfs/shared/cmd"
	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/objects"
	"github.com/google/abfs/shared/remote"
	"github.com/google/abfs/shared/remote/servers/inmemory"
	"github.com/google/abfs/store"
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
		name string
		opts MountRepoOpts
		want entities.Hash
	}{
		{name: "manifest hash", opts: MountRepoOpts{ManifestHash: givenHash}, want: givenHash},
		{name: "manifest file", opts: MountRepoOpts{ManifestFile: manFile}, want: fileHash},
		{name: "branch", opts: MountRepoOpts{Branch: "main"}},
		{name: "snapshot", opts: MountRepoOpts{ManifestHash: givenHash, IsSnapshot: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			casStore := remote.NewRemoteStore(inmemory.New())
			c := &mountRepoCmd{
				wsCmd:         &wsCmd{StoreProvider: &cmd.StoreProvider{Store: casStore}},
				MountRepoOpts: tt.opts,
			}
			var got fakeManifestHashes
			require.NoError(c.setPinnedManifest(context.Background(), &got))
			if tt.want.IsZero() {
				require.False(got.called)
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
