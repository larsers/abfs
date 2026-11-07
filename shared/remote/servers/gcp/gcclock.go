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
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// maxPause is the largest gap between the growth of the wall clock and of the
// monotonic clock between two heartbeats that is not taken for a pause of the
// process. Shorter pauses are within the clock skew margin of the collector.
const maxPause = 10 * time.Second

// stopFreeClock counts the seconds since its epoch on a clock that does not
// stop while the process is suspended: the larger of the monotonic and the
// wall-clock time since the epoch. The monotonic clock stops in a suspend on
// some systems, the wall clock does not; a backward step of the wall clock
// does not make the count go back.
type stopFreeClock struct {
	epoch time.Time
	last  atomic.Uint32
}

// elapsed returns the monotonic and the wall-clock time since the epoch.
func (c *stopFreeClock) elapsed() (mono, wall time.Duration) {
	now := time.Now()
	return now.Sub(c.epoch), now.Round(0).Sub(c.epoch.Round(0))
}

// seconds returns the count for the given times since the epoch, plus one so
// that zero means unset, and never less than a count it returned before.
func (c *stopFreeClock) seconds(mono, wall time.Duration) uint32 {
	v := uint32(max(mono, wall, 0)/time.Second) + 1
	for {
		last := c.last.Load()
		if v <= last {
			return last
		}
		if c.last.CompareAndSwap(last, v) {
			return v
		}
	}
}

// cacheClock gives cache entries their load and touch times, and the
// heartbeat its pause check. Tests replace cacheNow and clockElapsed.
var (
	cacheClock   = &stopFreeClock{epoch: time.Now()}
	clockElapsed = cacheClock.elapsed
	cacheNow     = func() uint32 { return cacheClock.seconds(clockElapsed()) }
)

// pauseDetector tells from the growth of the wall clock and of the monotonic
// clock between two calls of check whether the process was paused in between,
// for example while its VM was suspended.
type pauseDetector struct {
	started    bool
	mono, wall time.Duration
}

// check records the times since the clock epoch and reports a pause since the
// last call.
func (p *pauseDetector) check(mono, wall time.Duration) bool {
	paused := p.started && (wall-p.wall)-(mono-p.mono) > maxPause
	p.started, p.mono, p.wall = true, mono, wall
	return paused
}

// checkReadAge fails with Unavailable when a read that started at start on
// the cacheNow clock took the cache TTL or longer, because what it read may
// be older than the collector allows a server to use.
func (g *gcHooks) checkReadAge(start uint32) error {
	if g.on && g.cacheTTL > 0 {
		if now := cacheNow(); now >= start && now-start >= g.cacheTTL {
			return status.Errorf(codes.Unavailable, "a Bigtable read took %d s, not shorter than the cache TTL", now-start)
		}
	}
	return nil
}
