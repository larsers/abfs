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

package daemon

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"maps"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/iafan/cwalk"

	"github.com/google/abfs/binding"
	"github.com/google/abfs/filesystem"
	"github.com/google/abfs/shared/bindcas"
	"github.com/google/abfs/shared/entities"
)

const (
	gcKeepAliveMaxTick = 15 * time.Minute
	// Fraction of the interval by which each later wait varies.
	gcKeepAliveSpread = 0.1
	// The per-call key limit of the gRPC client.
	gcTouchBatchSize = 50000
	// Keys per second, so that the server's touch writers keep up.
	gcTouchRate = 10000
	// Doubles with each failure in a row, up to half the touch interval.
	gcTouchRetryWait = 5 * time.Minute
)

// GCKeepAliveRemote is the part of the cacheman client that GCKeepAlive uses.
type GCKeepAliveRemote interface {
	PutRefs(server string, project string, refs []entities.RefHash, blocking bool) error
	RemoteHeaders(keys []entities.Hash) ([]bool, error)
}

type MountScan struct {
	// Hashes are those of loaded files and of unloaded entries.
	Hashes []entities.Hash
	// Dirs maps the hash of each clean loaded directory to its paths.
	Dirs map[entities.Hash][]string
}

// GCKeepAlive keeps a live mount visible to the server GC: it re-writes the
// mount ref and sends the hashes the mount holds to the server.
type GCKeepAlive struct {
	Remote        GCKeepAliveRemote
	Server        string
	Ref           string
	Target        entities.Hash
	RefInterval   time.Duration
	TouchInterval time.Duration
	// TouchRate is the most keys a touch sends per second; zero means no limit.
	TouchRate float64
	// Scan scans the mount below each of roots, or all of it when roots is empty.
	Scan func(ctx context.Context, roots []string) (MountScan, error)
	// Rand returns a number in [0, 1) that spreads the waits; nil means none.
	Rand func() float64

	lastRef   time.Time
	lastTouch time.Time
	// Zero means the interval.
	refWait       time.Duration
	touchWait     time.Duration
	touchFailures int

	localWrites *localWriteWatch
}

// MountRef returns the ref in the ABFS meta project that points to a mount's details.
func MountRef(d *binding.MountDetails) string {
	return fmt.Sprintf("refs/abfs/mount/%s/%d", d.MetaRefPrefix, d.StartTimestamp)
}

// NewGCKeepAlive returns the GC keep-alive of the mount at mountPoint, or nil
// if it needs none.
func NewGCKeepAlive(info filesystem.MountInfo, details *binding.MountDetails, server string, remote GCKeepAliveRemote, mountPoint string) *GCKeepAlive {
	if info.LocalOnly || details == nil || details.Config == nil || !details.Config.GCKeepAlive {
		return nil
	}
	k := &GCKeepAlive{
		Remote:        remote,
		Server:        server,
		RefInterval:   details.Config.GCKeepAliveRefInterval,
		TouchInterval: details.Config.GCKeepAliveTouchInterval,
		TouchRate:     gcTouchRate,
		Rand:          rand.Float64,
		Scan: func(ctx context.Context, roots []string) (MountScan, error) {
			if len(roots) == 0 {
				roots = []string{mountPoint}
			}
			return scanMountHashes(ctx, roots, bindcasAttrs{})
		},
	}
	// Cacheman does not write refs in remote read-only mode, so such a mount has no ref to refresh.
	if !info.RemoteReadOnly {
		k.Ref = MountRef(details)
		k.Target = details.Hash
	}
	if k.tick() == 0 {
		return nil
	}
	return k
}

func (k *GCKeepAlive) tick() time.Duration {
	var tick time.Duration
	for _, d := range []time.Duration{k.refInterval(), k.TouchInterval} {
		if d > 0 && (tick == 0 || d < tick) {
			tick = d
		}
	}
	return min(tick, gcKeepAliveMaxTick)
}

func (k *GCKeepAlive) refInterval() time.Duration {
	if k.Ref == "" || k.Target.IsZero() {
		return 0
	}
	return k.RefInterval
}

// Run refreshes the ref and touches the objects until ctx is done. The refresh
// runs on its own, so that a long touch does not delay it.
func (k *GCKeepAlive) Run(ctx context.Context) {
	if k.tick() <= 0 {
		return
	}
	k.start(time.Now())
	var wg sync.WaitGroup
	if k.refInterval() > 0 || k.localWrites != nil {
		wg.Go(func() { runSteps(ctx, k.untilRef, k.refStep) })
	}
	if k.TouchInterval > 0 {
		wg.Go(func() { runSteps(ctx, k.untilTouch, k.touchStep) })
	}
	wg.Wait()
}

func runSteps(ctx context.Context, until func(time.Time) time.Duration, step func(context.Context, time.Time)) {
	timer := time.NewTimer(until(time.Now()))
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-timer.C:
			step(ctx, now)
			timer.Reset(until(time.Now()))
		}
	}
}

// start spreads the first waits over a whole interval, so that mounts started
// together do not refresh and touch together.
func (k *GCKeepAlive) start(now time.Time) {
	k.lastRef, k.lastTouch = now, now
	k.refWait = k.firstWait(k.refInterval())
	k.touchWait = k.firstWait(k.TouchInterval)
}

// firstWait returns a wait in (0, d].
func (k *GCKeepAlive) firstWait(d time.Duration) time.Duration {
	if k.Rand == nil || d <= 0 {
		return d
	}
	return max(time.Duration((1-k.Rand())*float64(d)), 1)
}

func (k *GCKeepAlive) nextWait(d time.Duration) time.Duration {
	if k.Rand == nil || d <= 0 {
		return d
	}
	return d + time.Duration((2*k.Rand()-1)*gcKeepAliveSpread*float64(d))
}

func waitOr(wait, d time.Duration) time.Duration {
	if wait > 0 {
		return wait
	}
	return d
}

func (k *GCKeepAlive) untilRef(now time.Time) time.Duration {
	wait := gcKeepAliveMaxTick
	if d := k.refInterval(); d > 0 {
		wait = min(wait, k.lastRef.Add(waitOr(k.refWait, d)).Sub(now))
	}
	return max(wait, 0)
}

func (k *GCKeepAlive) untilTouch(now time.Time) time.Duration {
	wait := gcKeepAliveMaxTick
	if k.TouchInterval > 0 {
		wait = min(wait, k.lastTouch.Add(waitOr(k.touchWait, k.TouchInterval)).Sub(now))
	}
	return max(wait, 0)
}

func (k *GCKeepAlive) refStep(ctx context.Context, now time.Time) {
	k.checkLocalWrites(ctx)
	if d := k.refInterval(); d > 0 && now.Sub(k.lastRef) >= waitOr(k.refWait, d) {
		k.lastRef = now
		k.refWait = k.nextWait(d)
		if err := k.refreshRef(); err != nil {
			log.Printf("Failed to refresh mount ref %s: %v", k.Ref, err)
		}
	}
}

func (k *GCKeepAlive) touchStep(ctx context.Context, now time.Time) {
	if k.TouchInterval > 0 && now.Sub(k.lastTouch) >= waitOr(k.touchWait, k.TouchInterval) {
		k.lastTouch = now
		found, total, err := k.touch(ctx)
		if err != nil {
			k.touchFailures++
			k.touchWait = k.retryWait()
			log.Printf("GC keep-alive touch failed after %d of %d objects, retrying in %s: %v", found, total, k.touchWait.Round(time.Second), err)
		} else {
			k.touchFailures = 0
			k.touchWait = k.nextWait(k.TouchInterval)
			log.Printf("GC keep-alive touched %d of %d objects on the server", found, total)
		}
	}
}

// retryWait returns the wait after k.touchFailures failed touches in a row. It
// is short, since the objects a failed touch missed may be condemned, and it
// grows, so that a server that cannot answer does not cause a scan per tick.
func (k *GCKeepAlive) retryWait() time.Duration {
	limit := k.TouchInterval / 2
	wait := gcTouchRetryWait
	for i := 1; i < k.touchFailures && wait < limit; i++ {
		wait *= 2
	}
	return k.nextWait(min(wait, limit))
}

func (k *GCKeepAlive) refreshRef() error {
	refs := []entities.RefHash{{Ref: k.Ref, Hash: k.Target}}
	return k.Remote.PutRefs(k.Server, entities.ABFSMetaProject, refs, true)
}

// touch sends the hashes of the mount to the server and returns how many the
// server has and how many were sent. It scans a clean loaded directory only
// when the server lacks its hash. A scan error does not stop the hashes found.
func (k *GCKeepAlive) touch(ctx context.Context) (int, int, error) {
	p := &touchPacer{rate: k.TouchRate}
	sent := make(map[entities.Hash]struct{})
	dirPresent := make(map[entities.Hash]bool)
	found := 0
	var roots []string
	var scanErr error
	for {
		scan, err := k.Scan(ctx, roots)
		if err != nil && scanErr == nil {
			scanErr = err
		}
		hashes := newHashes(sent, scan.Hashes)
		present, err := k.remoteHeaders(ctx, p, hashes)
		found += count(present)
		if err != nil {
			return found, len(sent), err
		}

		roots = nil
		var dirs []entities.Hash
		for h, paths := range scan.Dirs {
			ok, asked := dirPresent[h]
			switch {
			case !asked:
				dirs = append(dirs, h)
			case !ok:
				roots = append(roots, paths...)
			}
		}
		dirs = newHashes(sent, dirs)
		present, err = k.remoteHeaders(ctx, p, dirs)
		found += count(present)
		if err != nil {
			return found, len(sent), err
		}
		for i, ok := range present {
			dirPresent[dirs[i]] = ok
			if !ok {
				roots = append(roots, scan.Dirs[dirs[i]]...)
			}
		}
		if len(roots) == 0 {
			break
		}
	}
	if scanErr != nil {
		return found, len(sent), fmt.Errorf("failed to scan the mount: %w", scanErr)
	}
	return found, len(sent), nil
}

func newHashes(sent map[entities.Hash]struct{}, hashes []entities.Hash) []entities.Hash {
	var out []entities.Hash
	for _, h := range hashes {
		if _, ok := sent[h]; !ok {
			sent[h] = struct{}{}
			out = append(out, h)
		}
	}
	return out
}

func count(present []bool) int {
	n := 0
	for _, ok := range present {
		if ok {
			n++
		}
	}
	return n
}

func (k *GCKeepAlive) remoteHeaders(ctx context.Context, p *touchPacer, keys []entities.Hash) ([]bool, error) {
	present := make([]bool, 0, len(keys))
	for batch := range slices.Chunk(keys, gcTouchBatchSize) {
		if err := p.wait(ctx, len(batch)); err != nil {
			return present, err
		}
		found, err := k.Remote.RemoteHeaders(batch)
		if err != nil {
			return present, fmt.Errorf("failed to read remote headers: %w", err)
		}
		present = append(present, found...)
	}
	return present, nil
}

// touchPacer spaces batches so that the batch after one of n keys starts at
// least n/rate seconds later, with no credit for time spent elsewhere.
type touchPacer struct {
	rate float64
	next time.Time
}

func (p *touchPacer) wait(ctx context.Context, n int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.rate <= 0 || n == 0 {
		return nil
	}
	if d := time.Until(p.next); d > 0 {
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	p.next = time.Now().Add(time.Duration(float64(n) / p.rate * float64(time.Second)))
	return nil
}

type mountAttrs interface {
	Loaded(path string) (bool, error)
	Dirty(path string) (bool, error)
	Excluded(path string) (bool, error)
	Hash(path string) (entities.Hash, error)
}

type bindcasAttrs struct{}

func (bindcasAttrs) Loaded(path string) (bool, error)        { return bindcas.Loaded(path) }
func (bindcasAttrs) Dirty(path string) (bool, error)         { return bindcas.Dirty(path) }
func (bindcasAttrs) Excluded(path string) (bool, error)      { return bindcas.Excluded(path) }
func (bindcasAttrs) Hash(path string) (entities.Hash, error) { return bindcas.Hash(path) }

// scanMountHashes scans the mount below each of roots for the hashes of loaded
// files, of unloaded entries and of clean loaded directories, whose hash casfs
// keeps. That hash covers the entries below except excluded ones, such as
// projects and linkfile files, which the scan walks below it to find.
func scanMountHashes(ctx context.Context, roots []string, attrs mountAttrs) (MountScan, error) {
	var mu sync.Mutex
	seen := make(map[entities.Hash]struct{})
	dirs := make(map[entities.Hash][]string)
	var errs []error
	for _, root := range roots {
		// Directories whose entries a clean directory's hash covers.
		var covered sync.Map
		err := cwalk.Walk(root, func(rel string, info fs.FileInfo, err error) error {
			if err != nil {
				if entities.IsNotExist(err) {
					return filepath.SkipDir
				}
				return err
			}
			if ctx.Err() != nil {
				return filepath.SkipDir
			}
			if base := filepath.Base(rel); base == bindcas.GitDirName || base == bindcas.ControlDirName {
				return filepath.SkipDir
			}
			p := filepath.Join(root, rel)
			skip := func(err error) error {
				if entities.IsNotExist(err) {
					return filepath.SkipDir
				}
				return err
			}

			if _, ok := covered.Load(filepath.Dir(rel)); ok && rel != "." {
				excluded, err := attrs.Excluded(p)
				if err != nil {
					return skip(err)
				}
				if !excluded {
					if !info.IsDir() {
						return nil
					}
					// An unloaded directory holds no excluded entries.
					loaded, err := attrs.Loaded(p)
					if err != nil {
						return skip(err)
					}
					if !loaded {
						return filepath.SkipDir
					}
					covered.Store(rel, struct{}{})
					return nil
				}
			}

			loaded, err := attrs.Loaded(p)
			if err != nil {
				return skip(err)
			}
			if loaded && info.IsDir() {
				if rel == "." {
					return nil
				}
				dirty, err := attrs.Dirty(p)
				if err != nil {
					return skip(err)
				}
				if dirty {
					return nil
				}
				h, err := attrs.Hash(p)
				if err != nil {
					return skip(err)
				}
				if !h.IsZero() {
					mu.Lock()
					dirs[h] = append(dirs[h], p)
					mu.Unlock()
				}
				covered.Store(rel, struct{}{})
				return nil
			}

			h, err := attrs.Hash(p)
			if err != nil {
				return skip(err)
			}
			if !h.IsZero() {
				mu.Lock()
				seen[h] = struct{}{}
				mu.Unlock()
			}
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		})
		if err != nil {
			errs = append(errs, err)
		}
	}
	err := errors.Join(errs...)
	if err == nil {
		err = ctx.Err()
	}
	return MountScan{Hashes: slices.Collect(maps.Keys(seen)), Dirs: dirs}, err
}

func (c *casfs) startGCKeepAlive(ctx context.Context) {
	if k := NewGCKeepAlive(c.MountInfo, c.mountDetails, c.manifestServer, c.cacheman, c.mountPoint); k != nil {
		k.WatchLocalWriteCheck(c.mountDetails, c.cacheman.Features, c.store)
		go k.Run(ctx)
	}
}
