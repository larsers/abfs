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
	"fmt"
	"io/fs"
	"log"
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
)

// GCKeepAliveRemote is the part of the cacheman client that GCKeepAlive uses.
type GCKeepAliveRemote interface {
	PutRefs(server string, project string, refs []entities.RefHash, blocking bool) error
	RemoteHeaders(keys []entities.Hash) (int, error)
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
	Scan          func(ctx context.Context) ([]entities.Hash, error)
	// Rand returns a number in [0, 1) that spreads the waits, so that mounts
	// started at the same time do not refresh and touch at the same time. Nil
	// means every wait is exactly the interval.
	Rand func() float64

	lastRef   time.Time
	lastTouch time.Time
	// Zero means the interval.
	refWait   time.Duration
	touchWait time.Duration
}

// MountRef returns the ref in the ABFS meta project that points to a mount's details.
func MountRef(d *binding.MountDetails) string {
	return fmt.Sprintf("refs/abfs/mount/%s/%d", d.MetaRefPrefix, d.StartTimestamp)
}

// NewGCKeepAlive returns the GC keep-alive of the mount at mountPoint, or nil
// if the mount does not use a server or both intervals are zero. Call it after
// the mount details and the mount ref are written.
func NewGCKeepAlive(info filesystem.MountInfo, details *binding.MountDetails, server string, remote GCKeepAliveRemote, mountPoint string) *GCKeepAlive {
	if info.LocalOnly || details == nil || details.Config == nil {
		return nil
	}
	k := &GCKeepAlive{
		Remote:        remote,
		Server:        server,
		RefInterval:   details.Config.GCKeepAliveRefInterval,
		TouchInterval: details.Config.GCKeepAliveTouchInterval,
		Rand:          rand.Float64,
		Scan: func(ctx context.Context) ([]entities.Hash, error) {
			return scanMountHashes(ctx, mountPoint, bindcasAttrs{})
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
		// A failed touch waits for the next interval too, so a server or cacheman
		// that cannot answer does not cause a mount scan on every tick.
		k.lastTouch = now
		k.touchWait = k.nextWait(k.TouchInterval)
		found, total, err := k.touch(ctx)
		if err != nil {
			log.Printf("GC keep-alive touch failed after %d of %d objects: %v", found, total, err)
		} else {
			log.Printf("GC keep-alive touched %d of %d objects on the server", found, total)
		}
	}
}

func (k *GCKeepAlive) refreshRef() error {
	refs := []entities.RefHash{{Ref: k.Ref, Hash: k.Target}}
	return k.Remote.PutRefs(k.Server, entities.ABFSMetaProject, refs, true)
}

// touch sends the scanned hashes to the server and returns how many the server
// has and how many were sent. A scan error does not stop the hashes found so far.
func (k *GCKeepAlive) touch(ctx context.Context) (int, int, error) {
	hashes, scanErr := k.Scan(ctx)
	found := 0
	for batch := range slices.Chunk(hashes, gcTouchBatchSize) {
		if err := ctx.Err(); err != nil {
			return found, len(hashes), err
		}
		n, err := k.Remote.RemoteHeaders(batch)
		if err != nil {
			return found, len(hashes), fmt.Errorf("failed to read remote headers: %w", err)
		}
		found += n
	}
	if scanErr != nil {
		return found, len(hashes), fmt.Errorf("failed to scan the mount: %w", scanErr)
	}
	return found, len(hashes), nil
}

type mountAttrs interface {
	Loaded(path string) (bool, error)
	Hash(path string) (entities.Hash, error)
}

type bindcasAttrs struct{}

func (bindcasAttrs) Loaded(path string) (bool, error)        { return bindcas.Loaded(path) }
func (bindcasAttrs) Hash(path string) (entities.Hash, error) { return bindcas.Hash(path) }

// scanMountHashes returns the hashes the server must keep for the mount at
// root: those of loaded files, and those of unloaded files and directories,
// which the mount fetches from the server when they are read. It does not read
// the hash of a loaded directory, which may be dirty and costly to hash; its
// entries are scanned instead.
func scanMountHashes(ctx context.Context, root string, attrs mountAttrs) ([]entities.Hash, error) {
	var mu sync.Mutex
	seen := make(map[entities.Hash]struct{})
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
		loaded, err := attrs.Loaded(p)
		if err != nil {
			if entities.IsNotExist(err) {
				return filepath.SkipDir
			}
			return err
		}
		if loaded && info.IsDir() {
			return nil
		}

		h, err := attrs.Hash(p)
		if err != nil {
			if entities.IsNotExist(err) {
				return filepath.SkipDir
			}
			return err
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
	if err == nil {
		err = ctx.Err()
	}
	hashes := make([]entities.Hash, 0, len(seen))
	for h := range seen {
		hashes = append(hashes, h)
	}
	return hashes, err
}

func (c *casfs) startGCKeepAlive(ctx context.Context) {
	if k := NewGCKeepAlive(c.MountInfo, c.mountDetails, c.manifestServer, c.cacheman, c.mountPoint); k != nil {
		go k.Run(ctx)
	}
}
