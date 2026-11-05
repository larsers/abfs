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

package ready

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/google/abfs/shared/remote/servers/inmemory"
)

// abfs ready checks the projects a mount creates: removed projects are not asked for and
// extended revisions are.
func TestCheck_ProjectedManifest(t *testing.T) {
	require := require.New(t)
	s := inmemory.New()
	t.Cleanup(func() {
		_ = s.Close()
	})
	commit := putManifestCommit(t, s, map[string]string{"default.xml": `<manifest>
  <remote name="aosp" fetch=".." />
  <default revision="main" remote="aosp" />
  <project name="platform/a" path="a" />
  <project name="platform/gone" path="gone" />
  <remove-project name="platform/gone" />
  <extend-project name="platform/a" revision="release" />
</manifest>`})
	putRef(t, s, manifestServer, manifestProject, "refs/heads/main", commit)
	a := putObject(t, s, "commit", commitContent("a"))
	putRef(t, s, manifestServer, "platform/a", "refs/heads/release", a)

	r, err := check(context.Background(), s, testOpts("main"))
	require.NoError(err)
	require.True(r.Ready)
	require.Equal(1, r.Total)
	require.Len(r.pinned.Project, 1)
	require.Equal(a.String(), r.pinned.Project[0].Revision)
}
