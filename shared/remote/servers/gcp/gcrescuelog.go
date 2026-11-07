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
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"

	"github.com/google/abfs/shared/remote/servers/gcp/gc"
)

const (
	// rescueKeyLines is the number of rescued objects logged per
	// rescueLogWindow; a call that rescues a whole mount logs only its call
	// line once the limit is reached. rescueCallLines limits the call lines
	// the same way, as GetObject logs one call per object.
	rescueKeyLines  = 100
	rescueCallLines = 100
	rescueLogWindow = time.Minute
	otherType       = "other"
)

// rescueTypes are the object types of the rescue metric: the types the
// collector knows and otherType for any other.
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

// rescueLog logs the rescues of a server: one line per call that rescued or
// failed to rescue an object, with the client, at most rescueCallLines per
// rescueLogWindow, and one line per rescued object, at most rescueKeyLines.
// A line then counts the lines the limits left out.
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

// call logs a call that read the rows of read objects and rescued or failed
// to rescue some of them.
func (l *rescueLog) call(ctx context.Context, path rescuePath, read, rescued, failed int) {
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
	log.Printf("Rescued %d condemned objects of %d read in %s, %d failed: peer %s, x-forwarded-for %q, client-version %q",
		rescued, read, rescuePathNames[path], failed, addr, forwarded, version)
}

// object logs the rescue of hdr, whose marker was condemned.
func (l *rescueLog) object(path rescuePath, hdr *ObjectHeader, condemned []byte) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.windowLocked()
	if l.lines == rescueKeyLines {
		l.dropped++
		return
	}
	l.lines++
	log.Printf("Rescued %s %s in %s, condemned by GC cycle %s", hdr.Type, hdr.Id, rescuePathNames[path], markerCycle(condemned))
}

// windowLocked starts a new window once rescueLogWindow has passed.
func (l *rescueLog) windowLocked() {
	if now := l.now(); now.Sub(l.start) >= rescueLogWindow {
		l.flushLocked()
		l.start = now
		l.lines, l.calls = 0, 0
	}
}

// flush logs the count of lines left out so far.
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

// markerCycle returns the cycle number of a condemn marker, 8 bytes big-endian
// as gc run --condemn writes it, or the marker in hexadecimal.
func markerCycle(condemned []byte) string {
	if len(condemned) == 8 {
		return fmt.Sprint(binary.BigEndian.Uint64(condemned))
	}
	return fmt.Sprintf("%x", condemned)
}

// rescueClient returns what identifies the client of ctx: the peer address,
// which behind a load balancer is the proxy, and the x-forwarded-for and
// client-version metadata.
func rescueClient(ctx context.Context) (addr, forwarded, version string) {
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		addr = p.Addr.String()
	}
	md, _ := metadata.FromIncomingContext(ctx)
	return addr, strings.Join(md.Get("x-forwarded-for"), ", "), strings.Join(md.Get("client-version"), ", ")
}
