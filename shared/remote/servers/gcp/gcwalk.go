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

package gcp

import (
	"context"
	"errors"

	"github.com/google/abfs/shared/remote/servers/gcp/gc"
)

// WalkGC walks the pinned revisions of mount details by content; see gc.Walk.
// It fails on a server that rescues or touches what it reads, so that the
// walk does not keep the objects it is asked about.
func (s *GCPServer) WalkGC(ctx context.Context, opts *gc.WalkOpts) (*gc.WalkResult, error) {
	if s.gcHooks.rescue != nil || s.gcHooks.toucher != nil {
		return nil, errors.New("the GC walk needs a server that neither rescues nor touches the objects it reads")
	}
	return gc.Walk(ctx, s, opts)
}
