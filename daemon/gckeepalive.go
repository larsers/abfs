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
	// Same as the per-call key limit of the gRPC client, so each request is one server call.
	gcTouchBatchSize = 50000
	// Keys a touch sends per second at most, so that the server's touch
	// writers keep up with the touch of a large mount.
	gcTouchRate = 10000
	// The wait before the first retry of a failed touch. It doubles with each
	// failure in a row, up to half the touch interval.
	gcTouchRetryWait = 5 * time.Minute
)

// GCKeepAliveRemote is the part of the cacheman client that GCKeepAlive uses.
type GCKeepAliveRemote interface {
	PutRefs(server string, project string, refs []entities.RefHash, blocking bool) error
	// RemoteHeaders returns for each key whether the server has it.
	RemoteHeaders(keys []entities.Hash) ([]bool, error)
}

// MountScan is what a scan of a mount found.
type MountScan struct {
	// Hashes are those of loaded files and of unloaded entries.
	Hashes []entities.Hash
	// Dirs maps the hash of each clean loaded directory to its paths. The
	// hash covers what the directory holds, except excluded entries, which
	// the scan lists on their own.
	Dirs map[entities.Hash][]string
}

// GCKeepAlive keeps a live mount visible to the server GC. It re-writes the
// mount ref with the same target, which moves the ref's update time, and sends
// the hashes the mount holds to the server in batched Headers calls, which the
// server counts as a use of each object it has.
type GCKeepAlive struct {
	Remote        GCKeepAliveRemote
	Server        string
	Ref           string
	Target        entities.Hash
	RefInterval   time.Duration
	TouchInterval time.Duration
	// TouchRate is the most keys a touch sends per second; zero means no limit.
	TouchRate float64
	// Scan scans the mount below each of roots, or the whole mount when roots
	// is empty. The roots themselves are not reported as clean directories.
	Scan func(ctx context.Context, roots []string) (MountScan, error)
	// Rand returns a number in [0, 1) that spreads the waits, so that mounts
	// started at the same time do not refresh and touch at the same time. Nil
	// means every wait is exactly the interval.
	Rand func() float64

	lastRef   time.Time
	lastTouch time.Time
	// Zero means the interval.
	refWait   time.Duration
	touchWait time.Duration
	// The touches that failed in a row.
	touchFailures int
}

// MountRef returns the ref in the ABFS meta project that points to a mount's details.
func MountRef(d *binding.MountDetails) string {
	return fmt.Sprintf("refs/abfs/mount/%s/%d", d.MetaRefPrefix, d.StartTimestamp)
}

// NewGCKeepAlive returns the GC keep-alive of the mount at mountPoint, or nil
// if gc-keepalive is off, the mount does not use a server or both intervals
// are zero. Call it after the mount details and the mount ref are written.
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

// Run refreshes the ref and touches the objects until ctx is done. Errors are
// logged and never stop it.
func (k *GCKeepAlive) Run(ctx context.Context) {
	if k.tick() <= 0 {
		return
	}
	k.start(time.Now())
	timer := time.NewTimer(k.untilNext(time.Now()))
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-timer.C:
			k.step(ctx, now)
			timer.Reset(k.untilNext(time.Now()))
		}
	}
}

// start sets the first waits to a random part of each interval, so that the
// first refreshes and touches of mounts started together are spread over a
// whole interval.
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

// nextWait returns d plus or minus gcKeepAliveSpread of d.
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

// untilNext returns the time from now to the next refresh or touch, at most
// gcKeepAliveMaxTick.
func (k *GCKeepAlive) untilNext(now time.Time) time.Duration {
	wait := gcKeepAliveMaxTick
	if d := k.refInterval(); d > 0 {
		wait = min(wait, k.lastRef.Add(waitOr(k.refWait, d)).Sub(now))
	}
	if k.TouchInterval > 0 {
		wait = min(wait, k.lastTouch.Add(waitOr(k.touchWait, k.TouchInterval)).Sub(now))
	}
	return max(wait, 0)
}

func (k *GCKeepAlive) step(ctx context.Context, now time.Time) {
	if d := k.refInterval(); d > 0 && now.Sub(k.lastRef) >= waitOr(k.refWait, d) {
		k.lastRef = now
		k.refWait = k.nextWait(d)
		if err := k.refreshRef(); err != nil {
			log.Printf("Failed to refresh mount ref %s: %v", k.Ref, err)
		}
	}
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

// retryWait returns the wait before the next attempt after k.touchFailures
// failed touches in a row. A failed touch is retried long before the next
// interval, since the objects it did not reach may be condemned, and the wait
// grows, so that a server or cacheman that cannot answer does not cause a
// mount scan on every tick.
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
// server has and how many were sent. For a clean loaded directory it sends the
// directory's hash, and scans the directory only when the server lacks it,
// which happens when the directory was changed and hashed locally. A scan
// error does not stop the hashes found so far.
func (k *GCKeepAlive) touch(ctx context.Context) (int, int, error) {
	p := &touchPacer{rate: k.TouchRate, start: time.Now()}
	sent := make(map[entities.Hash]struct{})
	// The directory hashes asked so far, and whether the server has them.
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

// newHashes returns the hashes not in sent and adds them to it.
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

// remoteHeaders asks the server for keys in batches, paced by p, and returns
// for each key whether the server has it.
func (k *GCKeepAlive) remoteHeaders(ctx context.Context, p *touchPacer, keys []entities.Hash) ([]bool, error) {
	present := make([]bool, 0, len(keys))
	for batch := range slices.Chunk(keys, gcTouchBatchSize) {
		if err := p.wait(ctx); err != nil {
			return present, err
		}
		found, err := k.Remote.RemoteHeaders(batch)
		if err != nil {
			return present, fmt.Errorf("failed to read remote headers: %w", err)
		}
		present = append(present, found...)
		p.sent += len(batch)
	}
	return present, nil
}

// touchPacer spaces the batches of a touch so that the keys sent since start
// stay below rate per second.
type touchPacer struct {
	rate  float64
	start time.Time
	sent  int
}

func (p *touchPacer) wait(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.rate <= 0 || p.sent == 0 {
		return nil
	}
	d := time.Until(p.start.Add(time.Duration(float64(p.sent) / p.rate * float64(time.Second))))
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
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

// scanMountHashes scans the mount below each of roots for what the server
// must keep: the hashes of loaded files and of unloaded files and
// directories, which the mount fetches from the server when they are read,
// and the hashes of clean loaded directories. casfs keeps the hash of a clean
// directory, so reading it costs nothing, while a dirty directory would be
// hashed; dirty directories are scanned entry by entry. The entries below a
// clean directory are covered by its hash, except excluded ones, which casfs
// leaves out of the hash of their parent, such as projects and the files of
// linkfile; the scan still walks loaded directories below a clean one to find
// them, but reads no hash there.
func scanMountHashes(ctx context.Context, roots []string, attrs mountAttrs) (MountScan, error) {
	var mu sync.Mutex
	seen := make(map[entities.Hash]struct{})
	dirs := make(map[entities.Hash][]string)
	var errs []error
	for _, root := range roots {
		// The relative paths of the directories whose entries a clean
		// directory's hash covers.
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
		go k.Run(ctx)
	}
}
