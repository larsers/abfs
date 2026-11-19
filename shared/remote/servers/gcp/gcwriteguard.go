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
	"fmt"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/google/abfs/shared/remote"
	"github.com/google/abfs/shared/remote/servers/gcp/gc"
)

// writeGuard bounds the writes that record a reference to
// gc.ValidationWriteBound from the start of its link validation, so the
// collector can count on them having landed before it deletes.
type writeGuard struct {
	on    bool
	start uint32
}

func (s *GCPServer) validationGuard() writeGuard {
	if !s.gcHooks.on {
		return writeGuard{}
	}
	return writeGuard{on: true, start: cacheNow()}
}

// The cacheNow clock counts whole seconds, so one second is kept back.
func boundPassed(start, now uint32) bool {
	return now >= start && now-start >= uint32(gc.ValidationWriteBound/time.Second)-1
}

// writeBound is checked by the client interceptors before each attempt,
// retries included: a suspend stops the context deadline, not cacheNow.
type writeBound struct {
	start  uint32
	passed atomic.Bool
}

type writeBoundKey struct{}

// do fails with Unavailable once the bound has passed, so that the client
// sends the call again, which validates again.
func (w writeGuard) do(ctx context.Context, f func(ctx context.Context) error) error {
	if !w.on {
		return f(ctx)
	}
	var elapsed time.Duration
	now := cacheNow()
	if now > w.start {
		elapsed = time.Duration(now-w.start) * time.Second
	}
	if boundPassed(w.start, now) {
		return &remote.ErrUnavailable{Reason: fmt.Sprintf("the link validation of this call started %s ago, more than the GC allows before its writes; send it again", elapsed)}
	}
	ctx, cancel := context.WithTimeout(ctx, gc.ValidationWriteBound-elapsed-time.Second)
	defer cancel()
	b := &writeBound{start: w.start}
	err := f(context.WithValue(ctx, writeBoundKey{}, b))
	if err != nil && b.passed.Load() {
		return &remote.ErrUnavailable{Reason: fmt.Sprintf("the link validation of this call started %s or more ago before a write attempt, more than the GC allows; send it again", gc.ValidationWriteBound)}
	}
	return err
}

// checkWriteBound returns a code the client libraries do not retry, so the
// write ends at once.
func checkWriteBound(ctx context.Context) error {
	b, ok := ctx.Value(writeBoundKey{}).(*writeBound)
	if !ok || !boundPassed(b.start, cacheNow()) {
		return nil
	}
	b.passed.Store(true)
	return status.Error(codes.FailedPrecondition, "the GC write bound of the call has passed")
}

func writeBoundUnary(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	if err := checkWriteBound(ctx); err != nil {
		return err
	}
	return invoker(ctx, method, req, reply, cc, opts...)
}

func writeBoundStream(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	if err := checkWriteBound(ctx); err != nil {
		return nil, err
	}
	return streamer(ctx, desc, cc, method, opts...)
}

type answerTimer struct {
	start uint32
	wall  time.Time
}

func (g *gcHooks) startAnswer() answerTimer {
	if !g.answerBound {
		return answerTimer{}
	}
	return answerTimer{start: cacheNow(), wall: time.Now()}
}

// checkAnswer fails once gc.ValidationWriteBound has passed since the start:
// the collector bounds the use of a present answer from the start of the call.
func (g *gcHooks) checkAnswer(ctx context.Context, path rescuePath, a answerTimer) error {
	if a.start == 0 {
		return nil
	}
	if g.answerSeconds != nil {
		g.answerSeconds.Record(ctx, time.Since(a.wall).Seconds(), metric.WithAttributes(attribute.String("call", rescuePathNames[path])))
	}
	if now := cacheNow(); boundPassed(a.start, now) {
		return &remote.ErrUnavailable{Reason: fmt.Sprintf("the call took %d s, more than the GC allows before a present answer; send it again", now-a.start)}
	}
	return nil
}
