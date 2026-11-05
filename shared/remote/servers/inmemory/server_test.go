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

package inmemory

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/google/abfs/shared/objects"
	"github.com/google/abfs/shared/remote"
)

func TestPutObject_HashMismatch(t *testing.T) {
	ctx := context.Background()
	server := New()

	content := []byte("blob 4\x00data")
	other := objects.HashBytes([]byte("blob 5\x00other"))

	err := server.PutObject(ctx, other, remote.NewZStdContentReader(content))
	require.Error(t, err, "PutObject accepted content under another object's key")

	headers, err := server.Headers(ctx, []remote.Hash{other})
	require.NoError(t, err)
	require.Len(t, headers, 1)
	require.True(t, headers[0].IsEmpty(), "the rejected object is stored")

	key := objects.HashBytes(content)
	require.NoError(t, server.PutObject(ctx, key, remote.NewZStdContentReader(content)))
}
