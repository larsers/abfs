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
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
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

// boundPassed reports whether gc.ValidationWriteBound has passed since start
// at now, both on the cacheNow clock. That clock counts whole seconds, so one
// second is kept back.
func boundPassed(start, now uint32) bool {
	return now >= start && now-start >= uint32(gc.ValidationWriteBound/time.Second)-1
}

// writeBound is the bound of a guarded write that the interceptors of the
// server's Bigtable and Spanner connections check before each attempt, the
// retries of the client libraries included. A suspend of the machine stops the
// context deadline of the write, not the cacheNow clock. passed records that
// an interceptor refused an attempt.
type writeBound struct {
	start  uint32
	passed atomic.Bool
}

type writeBoundKey struct{}

// do runs the write f under a context that ends with the bound and carries it
// for the interceptors. It fails with Unavailable, without calling f, once the
// bound has passed, and when an interceptor refused an attempt of f: the
// client sends the call again, which validates again.
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
		return status.Errorf(codes.Unavailable, "the link validation of this call started %s ago, more than the GC allows before its writes; send it again", elapsed)
	}
	ctx, cancel := context.WithTimeout(ctx, gc.ValidationWriteBound-elapsed-time.Second)
	defer cancel()
	b := &writeBound{start: w.start}
	err := f(context.WithValue(ctx, writeBoundKey{}, b))
	if err != nil && b.passed.Load() {
		return status.Errorf(codes.Unavailable, "the link validation of this call started %s or more ago before a write attempt, more than the GC allows; send it again", gc.ValidationWriteBound)
	}
	return err
}

// checkWriteBound fails when ctx carries a write bound that has passed. The
// code is one the client libraries do not retry, so the write ends at once.
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

// writeBoundOptions install the interceptors of the write bound on the
// connections of a client. casdb.New turns the Bigtable session transport off
// when it gets options, since the interceptors do not see the calls it sends.
// A client of the Bigtable emulator dials its own connection and ignores them.
func writeBoundOptions() []option.ClientOption {
	return []option.ClientOption{
		option.WithGRPCDialOption(grpc.WithChainUnaryInterceptor(writeBoundUnary)),
		option.WithGRPCDialOption(grpc.WithChainStreamInterceptor(writeBoundStream)),
	}
}

// answerTimer is the start of a call that may answer that objects are
// present, on the cacheNow clock and on the wall clock.
type answerTimer struct {
	start uint32
	wall  time.Time
}

// startAnswer returns the start of a call that may answer present; zero
// unless the server bounds its answers.
func (g *gcHooks) startAnswer() answerTimer {
	if !g.answerBound {
		return answerTimer{}
	}
	return answerTimer{start: cacheNow(), wall: time.Now()}
}

// checkAnswer records the time the call of path took and fails with
// Unavailable when gc.ValidationWriteBound or more has passed since its start:
// a present answer rests on information whose use the collector bounds from
// the start of the call, and a suspend of the machine can stall a call for
// longer.
func (g *gcHooks) checkAnswer(ctx context.Context, path rescuePath, a answerTimer) error {
	if a.start == 0 {
		return nil
	}
	if g.answerSeconds != nil {
		g.answerSeconds.Record(ctx, time.Since(a.wall).Seconds(), metric.WithAttributes(attribute.String("call", rescuePathNames[path])))
	}
	if now := cacheNow(); boundPassed(a.start, now) {
		return status.Errorf(codes.Unavailable, "the call took %d s, more than the GC allows before a present answer; send it again", now-a.start)
	}
	return nil
}
