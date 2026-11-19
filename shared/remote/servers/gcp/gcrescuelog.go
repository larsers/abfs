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
	"encoding/binary"
	"fmt"
	"log"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"

	"github.com/google/abfs/shared/remote/servers/gcp/gc"
)

const (
	rescueKeyLines  = 100
	rescueCallLines = 100
	rescueLogWindow = time.Minute
	otherType       = "other"
)

var rescueTypes, rescueTypeIndex = func() ([]string, map[string]int) {
	types := append(gc.TypeNames(), otherType)
	index := make(map[string]int, len(types))
	for i, t := range types {
		index[t] = i
	}
	return types, index
}()

func rescueType(typ string) int {
	if i, ok := rescueTypeIndex[typ]; ok {
		return i
	}
	return rescueTypeIndex[otherType]
}

type rescueLog struct {
	now          func() time.Time
	mu           sync.Mutex
	start        time.Time
	lines        int
	dropped      int
	calls        int
	droppedCalls int
}

func newRescueLog() *rescueLog {
	return &rescueLog{now: time.Now}
}

func (l *rescueLog) call(ctx context.Context, path rescuePath, req string, read, rescued, failed int) {
	if rescued == 0 && failed == 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.windowLocked()
	if l.calls == rescueCallLines {
		l.droppedCalls++
		return
	}
	l.calls++
	addr, forwarded, version := rescueClient(ctx)
	log.Printf("Rescued %d condemned objects of %d read in %s, %d failed: request %s, peer %s, x-forwarded-for %q, client-version %q",
		rescued, read, rescuePathNames[path], failed, req, addr, forwarded, version)
}

// lost is not rate limited: each line is an object a client may have needed.
func (l *rescueLog) lost(ctx context.Context, path rescuePath, req string, hdr *ObjectHeader) {
	addr, forwarded, version := rescueClient(ctx)
	log.Printf("Condemned object %s was deleted before it could be rescued in %s: request %s, peer %s, x-forwarded-for %q, client-version %q",
		hdr.Id, rescuePathNames[path], req, addr, forwarded, version)
}

func (l *rescueLog) object(path rescuePath, req string, hdr *ObjectHeader, condemned []byte) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.windowLocked()
	if l.lines == rescueKeyLines {
		l.dropped++
		return
	}
	l.lines++
	log.Printf("Rescued %s %s in %s, condemned by GC cycle %s: request %s", hdr.Type, hdr.Id, rescuePathNames[path], markerCycle(condemned), req)
}

func (l *rescueLog) windowLocked() {
	if now := l.now(); now.Sub(l.start) >= rescueLogWindow {
		l.flushLocked()
		l.start = now
		l.lines, l.calls = 0, 0
	}
}

func (l *rescueLog) flush() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.flushLocked()
}

func (l *rescueLog) flushLocked() {
	if l.droppedCalls > 0 {
		log.Printf("Left out the lines of %d calls with rescues in the minute from %s", l.droppedCalls, l.start.UTC().Format(time.RFC3339))
		l.droppedCalls = 0
	}
	if l.dropped > 0 {
		log.Printf("Left out the lines of %d rescued objects in the minute from %s", l.dropped, l.start.UTC().Format(time.RFC3339))
		l.dropped = 0
	}
}

func markerCycle(condemned []byte) string {
	if len(condemned) == 8 {
		return fmt.Sprint(binary.BigEndian.Uint64(condemned))
	}
	return fmt.Sprintf("%x", condemned)
}

// The calls of one client operation can share a trace, so the trace ID alone
// does not name the call.
func newRequestID(ctx context.Context) string {
	id := fmt.Sprintf("%016x", rand.Uint64())
	if sc := trace.SpanContextFromContext(ctx); sc.HasTraceID() {
		return id + ", trace " + sc.TraceID().String()
	}
	return id
}

func rescueClient(ctx context.Context) (addr, forwarded, version string) {
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		addr = p.Addr.String()
	}
	md, _ := metadata.FromIncomingContext(ctx)
	return addr, strings.Join(md.Get("x-forwarded-for"), ", "), strings.Join(md.Get("client-version"), ", ")
}
