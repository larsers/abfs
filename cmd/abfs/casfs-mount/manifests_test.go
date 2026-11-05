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

package mount

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/google/abfs/shared/cmd"
	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/instance"
	"github.com/google/abfs/shared/objects"
)

func TestNewMountDetails_ManifestHashes(t *testing.T) {
	require := require.New(t)

	c := &casfsMountCmd{ConfigProvider: &cmd.ConfigProvider{Config: &instance.Config{}}}
	details, err := c.newMountDetails()
	require.NoError(err)
	require.Empty(details.Links())

	manifest := objects.HashBlobBytes([]byte("manifest"))
	pinned := objects.HashBlobBytes([]byte("pinned"))
	c.SetManifestHashes(manifest, pinned)
	details, err = c.newMountDetails()
	require.NoError(err)
	require.Equal(manifest, details.ManifestHash)
	require.Equal(pinned, details.PinnedManifestHash)
	require.Equal([]entities.Hash{manifest, pinned}, details.Links())
}
