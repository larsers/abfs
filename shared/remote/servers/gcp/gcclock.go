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

	"github.com/google/abfs/shared/remote/servers/gcp/gc"
)

// maxPause is the largest gap between the growth of the wall clock and of the
// monotonic clock between two heartbeats that is not taken for a pause of the
// process. Shorter pauses are within the clock skew margin of the collector.
const maxPause = 10 * time.Second

// stopFreeClock counts the seconds since its epoch on a clock that does not
// stop while the process is suspended: the monotonic time since the epoch plus
// the largest lead of the wall clock over it seen so far. The monotonic clock
// stops in a suspend on some systems, the wall clock does not. A backward step
// of the wall clock neither moves the count back nor stops it, and a forward
// step that is undone later leaves the count ahead, which only makes cache
// entries expire early.
type stopFreeClock struct {
	epoch time.Time
	// lead is the largest lead of the wall clock over the monotonic clock,
	// in nanoseconds.
	lead atomic.Int64
	last atomic.Uint32
}

// elapsed returns the monotonic and the wall-clock time since the epoch.
func (c *stopFreeClock) elapsed() (mono, wall time.Duration) {
	now := time.Now()
	return now.Sub(c.epoch), now.Round(0).Sub(c.epoch.Round(0))
}

// maxLead records the lead of wall over mono and returns the largest lead
// seen so far.
func (c *stopFreeClock) maxLead(mono, wall time.Duration) time.Duration {
	v := int64(wall - mono)
	for {
		lead := c.lead.Load()
		if v <= lead || c.lead.CompareAndSwap(lead, v) {
			return time.Duration(max(lead, v))
		}
	}
}

// seconds returns the count for the given times since the epoch, plus one so
// that zero means unset, and never less than a count it returned before.
func (c *stopFreeClock) seconds(mono, wall time.Duration) uint32 {
	v := uint32(max(mono+c.maxLead(mono, wall), 0)/time.Second) + 1
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
// heartbeat its pause check. Tests replace cacheClock, cacheNow and
// clockElapsed.
var (
	cacheClock   = &stopFreeClock{epoch: time.Now()}
	clockElapsed = cacheClock.elapsed
	cacheNow     = func() uint32 { return cacheClock.seconds(clockElapsed()) }
)

// pausedSince reports whether the process was paused since the last call of
// pausedSince.
func (g *gcHooks) pausedSince() bool {
	mono, wall := clockElapsed()
	return g.pause.check(cacheClock.maxLead(mono, wall))
}

// pauseDetector tells from the growth of the largest lead of the wall clock
// over the monotonic clock between two calls of check whether the process was
// paused in between, for example while its VM was suspended. The largest lead
// keeps a pause that a backward step of the wall clock undid before the check.
type pauseDetector struct {
	started bool
	lead    time.Duration
}

// check records the largest lead and reports a pause since the last call.
func (p *pauseDetector) check(lead time.Duration) bool {
	paused := p.started && lead-p.lead > maxPause
	p.started, p.lead = true, lead
	return paused
}

// heartbeatLate records start, the start of a heartbeat attempt, and reports
// whether the last attempt started more than the heartbeat interval plus the
// touch drain bound before it. The process was then frozen or starved for
// longer than the drain bound, with the monotonic clock running, and touches
// may land later than the collector allows.
func (g *gcHooks) heartbeatLate(start uint32) bool {
	last := g.lastAttempt
	g.lastAttempt = start
	return last != 0 && start > last && time.Duration(start-last)*time.Second > g.server.HeartbeatInterval+gc.TouchDrainBound
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
