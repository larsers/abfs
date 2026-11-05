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

package ninjago

import (
	"errors"
	"syscall"
	"testing"

	ninjaruntime "github.com/google/ninja-go/runtime"
	"github.com/stretchr/testify/require"

	"github.com/google/abfs/binding"
	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/objects"
)

type discardLogger struct {
	ninjaruntime.Logger
}

func (discardLogger) Debugf(format string, a ...any) {}

func TestSkipMissingResp(t *testing.T) {
	hash := objects.HashBlobBytes([]byte("resp"))
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "not exist", err: entities.NotExistErrf("object %s not found", hash), want: true},
		{name: "cacheman not found", err: syscall.ENOENT, want: true},
		{name: "other", err: errors.New("unavailable")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			c := &ABFSBackend{log: discardLogger{}}
			eval := &Eval{req: &binding.EvalRequest{}, triedResps: make(map[entities.Hash]struct{})}
			require.Equal(tt.want, c.skipMissingResp(eval, hash, tt.err))
			_, tried := eval.triedResps[hash]
			require.Equal(tt.want, tried)
		})
	}
}
