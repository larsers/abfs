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

package abfsrepo

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/google/abfs/manifest"
	"github.com/google/abfs/shared/entities"
)

// The paths of one name resolve to their own revisions.
func TestResolveRevisions_ByPath(t *testing.T) {
	m, err := manifest.Parse([]byte(`<manifest>
  <remote name="origin" fetch="https://gerrit.example.com" />
  <default revision="main" remote="origin" />
  <project name="platform/shared" path="a/shared" />
  <project name="platform/shared" path="b/shared" revision="release" />
</manifest>`))
	require.NoError(t, err)
	mainHash := entities.NewHash("1111111111111111111111111111111111111111")
	releaseHash := entities.NewHash("2222222222222222222222222222222222222222")
	r := &Repo{
		RepoOpts: RepoOpts{Store: &revisionStore{refs: map[[2]string]map[string]entities.Hash{
			{"gerrit.example.com", "platform/shared"}: {"refs/heads/main": mainHash, "refs/heads/release": releaseHash},
		}}},
		flatMan: m,
	}
	r.groupsInclude, r.groupsExclude = FilterGroups(nil)
	r.filterProjectsByGroups()

	r.resolveRevisions(map[string]struct{}{"a/shared": {}, "b/shared": {}})
	require.Equal(t, mainHash, r.resolved["a/shared"].Hash)
	require.Equal(t, releaseHash, r.resolved["b/shared"].Hash)
}
