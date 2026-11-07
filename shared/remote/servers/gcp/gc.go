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

// BackfillGC writes the missing and outdated rows of the table gc; see
// gc.Backfill.
func (s *GCPServer) BackfillGC(ctx context.Context, opts *gc.BackfillOpts) (*gc.BackfillStats, error) {
	return gc.Backfill(ctx, s.casdb.BTClient(), s, opts)
}

// DryRunGC runs a garbage collection cycle that only reports; see gc.DryRun.
func (s *GCPServer) DryRunGC(ctx context.Context, opts *gc.DryRunOpts) (*gc.Result, error) {
	return gc.DryRun(ctx, s.casdb.SpannerClient(), s.casdb.BTClient(), s, opts)
}

// CondemnGC runs a garbage collection cycle that condemns, with its lists in
// bucket; see gc.Condemn.
func (s *GCPServer) CondemnGC(ctx context.Context, bucket string, opts *gc.CondemnOpts) (*gc.Result, error) {
	if s.storage == nil {
		return nil, errors.New("the server has no Cloud Storage client for the condemned lists")
	}
	return gc.Condemn(ctx, s.casdb.SpannerClient(), s.casdb.BTClient(), s.storage.Bucket(bucket), s, opts)
}
