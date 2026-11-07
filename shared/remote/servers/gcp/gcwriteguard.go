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
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/google/abfs/shared/remote/servers/gcp/gc"
)

// writeGuard bounds the writes that record a reference after its links were
// validated by gc.ValidationWriteBound from the start of the validation. A
// condemn marker that lands after a validation read the object can be
// followed by a delete only once the collector can count on every such write
// having landed. Without GC it does nothing.
type writeGuard struct {
	on    bool
	start uint32
}

// validationGuard returns the guard of a validation that starts now.
func (s *GCPServer) validationGuard() writeGuard {
	if !s.gcHooks.on {
		return writeGuard{}
	}
	return writeGuard{on: true, start: cacheNow()}
}

// do runs the write f under a context that ends with the bound. It fails
// with Unavailable, without calling f, once the bound has passed: the client
// sends the call again, which validates again.
func (w writeGuard) do(ctx context.Context, f func(ctx context.Context) error) error {
	if !w.on {
		return f(ctx)
	}
	// The cacheNow clock counts whole seconds, so one second is kept back.
	var elapsed time.Duration
	if now := cacheNow(); now > w.start {
		elapsed = time.Duration(now-w.start) * time.Second
	}
	left := gc.ValidationWriteBound - elapsed - time.Second
	if left <= 0 {
		return status.Errorf(codes.Unavailable, "the link validation of this call started %s ago, more than the GC allows before its writes; send it again", elapsed)
	}
	ctx, cancel := context.WithTimeout(ctx, left)
	defer cancel()
	return f(ctx)
}
