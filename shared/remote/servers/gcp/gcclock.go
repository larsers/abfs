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
	"fmt"
	"sync/atomic"
	"time"

	"github.com/google/abfs/shared/remote"
	"github.com/google/abfs/shared/remote/servers/gcp/gc"
)

// maxPause is within the clock skew margin of the collector.
const maxPause = 10 * time.Second

// stopFreeClock counts seconds on a clock that keeps running while the process
// is suspended: monotonic time plus the largest lead of the wall (or boot)
// clock seen. Wall clock steps only move it ahead, expiring cache entries early.
type stopFreeClock struct {
	epoch time.Time
	// boot is the boot clock at the epoch, when hasBoot.
	boot    time.Duration
	hasBoot bool
	// lead is the largest lead of wall over monotonic time, in nanoseconds.
	lead atomic.Int64
	last atomic.Uint32
}

func newStopFreeClock() *stopFreeClock {
	c := &stopFreeClock{epoch: time.Now()}
	c.boot, c.hasBoot = bootClock()
	return c
}

func (c *stopFreeClock) elapsed() (mono, wall time.Duration) {
	now := time.Now()
	mono, wall = now.Sub(c.epoch), now.Round(0).Sub(c.epoch.Round(0))
	if c.hasBoot {
		if boot, ok := bootClock(); ok {
			wall = max(wall, boot-c.boot)
		}
	}
	return mono, wall
}

func (c *stopFreeClock) maxLead(mono, wall time.Duration) time.Duration {
	v := int64(wall - mono)
	for {
		lead := c.lead.Load()
		if v <= lead || c.lead.CompareAndSwap(lead, v) {
			return time.Duration(max(lead, v))
		}
	}
}

// seconds is one-based, so that zero means unset, and never goes back.
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

// Tests replace these.
var (
	cacheClock   = newStopFreeClock()
	clockElapsed = cacheClock.elapsed
	cacheNow     = func() uint32 { return cacheClock.seconds(clockElapsed()) }
)

func (g *gcHooks) pausedSince() bool {
	mono, wall := clockElapsed()
	return g.pause.check(cacheClock.maxLead(mono, wall))
}

// pauseDetector detects a pause, such as a VM suspend, from the growth of the
// largest wall-clock lead between calls of check.
type pauseDetector struct {
	started bool
	lead    time.Duration
}

func (p *pauseDetector) check(lead time.Duration) bool {
	paused := p.started && lead-p.lead > maxPause
	p.started, p.lead = true, lead
	return paused
}

// heartbeatLate reports a gap since the last attempt longer than the interval
// plus the drain bound: touches may then land later than the collector allows.
func (g *gcHooks) heartbeatLate(start uint32) bool {
	last := g.lastAttempt
	g.lastAttempt = start
	return last != 0 && start > last && time.Duration(start-last)*time.Second > g.server.HeartbeatInterval+gc.TouchDrainBound
}

// checkReadAge fails a read that took the cache TTL or longer: what it read
// may be older than the collector allows a server to use.
func (g *gcHooks) checkReadAge(start uint32) error {
	if g.on && g.cacheTTL > 0 {
		if now := cacheNow(); now >= start && now-start >= g.cacheTTL {
			return &remote.ErrUnavailable{Reason: fmt.Sprintf("a Bigtable read took %d s, not shorter than the cache TTL", now-start)}
		}
	}
	return nil
}
