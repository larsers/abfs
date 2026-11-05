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

package gc

import (
	"context"
	"fmt"
	"log"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigtable"
	"golang.org/x/sync/errgroup"

	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/remote"
)

const (
	DefaultBackfillShards    = 16
	DefaultBackfillBatchSize = 500
	objectsTable             = "objects"
	backfillProgressInterval = time.Minute
)

// BackfillOpts sets the scan of Backfill.
type BackfillOpts struct {
	// Start and End limit the scan to the object keys in [Start, End), given as
	// lowercase hexadecimal prefixes. Empty means the start or the end of the
	// table.
	Start, End string
	// Shards is the number of key ranges scanned in parallel.
	Shards int
	// BatchSize is the number of object rows read per request.
	BatchSize int
	// Rate limits the object rows scanned per second; 0 means no limit.
	Rate float64
	// DryRun counts the gc rows to write and writes none.
	DryRun bool
}

// TypeCounts counts the objects of one type that a backfill handled.
type TypeCounts struct {
	// Written counts the gc rows written, or to write in a dry run.
	Written int64
	// Edges counts the edges of those rows.
	Edges int64
	// Failed counts the objects that could not be read or decoded.
	Failed int64
}

// BackfillStats reports a backfill.
type BackfillStats struct {
	Scanned int64
	// Current counts the objects whose gc row has the current extractor
	// version or a newer one.
	Current   int64
	Malformed int64
	Types     map[string]*TypeCounts
	// Done reports that the scan reached End. Otherwise ResumeKey is the Start
	// of a run that continues this one: every key before it is done.
	Done      bool
	ResumeKey string
}

// Failed returns the number of objects that could not be read or decoded.
func (s *BackfillStats) Failed() int64 {
	var n int64
	for _, c := range s.Types {
		n += c.Failed
	}
	return n
}

// Backfill writes the gc row of every object that has none or one of an older
// extractor version, for objects added before the server wrote gc rows or
// while --gc was off. Each row gets the time of the object row, so the grace
// counts from the upload of the object, not from the backfill. Blobs are not
// read; other objects are read through objects and decoded. An object that cannot be read or decoded is logged and
// counted, the scan goes on, and the error says how many failed; the object
// keeps no gc row, so the next run tries it again.
func Backfill(ctx context.Context, client *bigtable.Client, objects remote.RemoteProvider, opts *BackfillOpts) (*BackfillStats, error) {
	o := *opts
	if o.Shards <= 0 {
		o.Shards = DefaultBackfillShards
	}
	if o.BatchSize <= 0 {
		o.BatchSize = DefaultBackfillBatchSize
	}
	bounds, err := splitRange(o.Start, o.End, o.Shards)
	if err != nil {
		return nil, err
	}
	b := &backfill{
		objectsTable: client.Open(objectsTable),
		gcTable:      client.Open(TableName),
		objects:      objects,
		writer:       NewEdgeWriter(client),
		opts:         o,
		limiter:      newLimiter(o.Rate),
		stats:        BackfillStats{Types: make(map[string]*TypeCounts)},
		resume:       slices.Clone(bounds[:len(bounds)-1]),
		done:         make([]bool, len(bounds)-1),
	}

	stopProgress := make(chan struct{})
	progressDone := make(chan struct{})
	go func() {
		defer close(progressDone)
		ticker := time.NewTicker(backfillProgressInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s := b.snapshot()
				log.Printf("GC backfill: scanned %d objects, %d current, %d failed, resume key %q", s.Scanned, s.Current, s.Failed(), s.ResumeKey)
			case <-stopProgress:
				return
			}
		}
	}()

	g, gctx := errgroup.WithContext(ctx)
	for i := range b.done {
		g.Go(func() error { return b.scanShard(gctx, i, bounds[i], bounds[i+1]) })
	}
	err = g.Wait()
	close(stopProgress)
	<-progressDone

	stats := b.snapshot()
	if err == nil && stats.Failed() > 0 {
		err = fmt.Errorf("failed to backfill the gc rows of %d objects", stats.Failed())
	}
	return &stats, err
}

type backfill struct {
	objectsTable *bigtable.Table
	gcTable      *bigtable.Table
	objects      remote.RemoteProvider
	writer       *EdgeWriter
	opts         BackfillOpts
	limiter      *limiter

	mu     sync.Mutex
	stats  BackfillStats
	resume []string
	done   []bool
}

// scanShard handles the keys in [start, end) in batches.
func (b *backfill) scanShard(ctx context.Context, shard int, start, end string) error {
	filter := bigtable.RowFilter(bigtable.ChainFilters(bigtable.FamilyFilter("meta"), bigtable.ColumnFilter("type"), bigtable.LatestNFilter(1)))
	from := start
	for {
		if err := b.limiter.wait(ctx, b.opts.BatchSize); err != nil {
			return err
		}
		var keys, types []string
		var times []bigtable.Timestamp
		err := b.objectsTable.ReadRows(ctx, bigtable.NewRange(from, end), func(row bigtable.Row) bool {
			for _, item := range row["meta"] {
				keys = append(keys, row.Key())
				types = append(types, string(item.Value))
				times = append(times, item.Timestamp)
			}
			return true
		}, filter, bigtable.LimitRows(int64(b.opts.BatchSize)))
		if err != nil {
			return fmt.Errorf("failed to scan objects from %q: %w", from, err)
		}
		if len(keys) > 0 {
			if err := b.handleBatch(ctx, keys, types, times); err != nil {
				return err
			}
		}
		last := len(keys) < b.opts.BatchSize
		b.mu.Lock()
		if len(keys) > 0 {
			// Resuming at the last key handles it again, which costs one read.
			b.resume[shard] = keys[len(keys)-1]
		}
		b.done[shard] = last
		b.mu.Unlock()
		if last {
			return nil
		}
		from = keys[len(keys)-1] + "\x00"
	}
}

// handleBatch writes the gc rows of the objects keys, of the given types and
// object row times.
func (b *backfill) handleBatch(ctx context.Context, keys, types []string, times []bigtable.Timestamp) error {
	versions := make(map[string]int64, len(keys))
	err := b.gcTable.ReadRows(ctx, bigtable.RowList(keys), func(row bigtable.Row) bool {
		// A row that does not parse is written again.
		if r, err := ParseRow(row); err == nil {
			versions[row.Key()] = r.Version
		}
		return true
	}, bigtable.RowFilter(bigtable.ChainFilters(bigtable.FamilyFilter(EdgesFamily), bigtable.ColumnFilter(VersionColumn), bigtable.LatestNFilter(1))))
	if err != nil {
		return fmt.Errorf("failed to read the gc rows from %s: %w", keys[0], err)
	}

	stats := BackfillStats{Scanned: int64(len(keys)), Types: make(map[string]*TypeCounts)}
	for i, key := range keys {
		if v, ok := versions[key]; ok && v >= ExtractorVersion {
			stats.Current++
			continue
		}
		counts := stats.Types[types[i]]
		if counts == nil {
			counts = &TypeCounts{}
			stats.Types[types[i]] = counts
		}
		hash, edges, malformed, err := b.edges(ctx, key, types[i])
		if err != nil {
			log.Printf("Failed to backfill the gc row of %s (%s): %v", key, types[i], err)
			counts.Failed++
			continue
		}
		if !b.opts.DryRun {
			if err := b.writer.WriteAt(ctx, hash, types[i], edges, times[i]); err != nil {
				return err
			}
		}
		counts.Written++
		counts.Edges += int64(len(edges))
		stats.Malformed += int64(malformed)
	}
	b.mu.Lock()
	b.stats.add(&stats)
	b.mu.Unlock()
	return nil
}

func (b *backfill) edges(ctx context.Context, key, typ string) (Hash, []Hash, int, error) {
	if !entities.IsHash(key) {
		return Hash{}, nil, 0, fmt.Errorf("invalid object key")
	}
	hash := entities.NewHash(key)
	if typ == string(entities.ObjectTypeBlob) {
		return hash, nil, 0, nil
	}
	decodedType, obj, err := remote.DecodeObject(ctx, b.objects, hash)
	if err != nil {
		return hash, nil, 0, err
	}
	if string(decodedType) != typ {
		return hash, nil, 0, fmt.Errorf("content has type %q", decodedType)
	}
	edges, malformed, err := Edges(typ, obj)
	return hash, edges, malformed, err
}

// snapshot returns a copy of the stats so far, with the resume key of the
// first shard that is not done.
func (b *backfill) snapshot() BackfillStats {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := BackfillStats{Types: make(map[string]*TypeCounts)}
	s.add(&b.stats)
	s.Done = true
	for i, done := range b.done {
		if !done {
			s.Done = false
			s.ResumeKey = b.resume[i]
			break
		}
	}
	return s
}

func (s *BackfillStats) add(o *BackfillStats) {
	s.Scanned += o.Scanned
	s.Current += o.Current
	s.Malformed += o.Malformed
	for typ, c := range o.Types {
		t := s.Types[typ]
		if t == nil {
			t = &TypeCounts{}
			s.Types[typ] = t
		}
		t.Written += c.Written
		t.Edges += c.Edges
		t.Failed += c.Failed
	}
}

// splitRange returns n+1 boundaries that split [start, end) into n key ranges
// of about the same number of keys, for keys that are hexadecimal hashes. It
// returns fewer when the range is too small to split.
func splitRange(start, end string, n int) ([]string, error) {
	lo, err := keyPrefix(start, 0)
	if err != nil {
		return nil, err
	}
	hi, err := keyPrefix(end, 1<<32)
	if err != nil {
		return nil, err
	}
	if end != "" && start >= end {
		return nil, fmt.Errorf("empty key range [%q, %q)", start, end)
	}
	bounds := []string{start}
	for i := 1; i < n && hi > lo; i++ {
		b := fmt.Sprintf("%08x", lo+(hi-lo)*uint64(i)/uint64(n))
		if b > bounds[len(bounds)-1] && (end == "" || b < end) {
			bounds = append(bounds, b)
		}
	}
	return append(bounds, end), nil
}

// keyPrefix returns the first 32 bits of a hexadecimal key, or empty for an
// empty key.
func keyPrefix(key string, empty uint64) (uint64, error) {
	if key == "" {
		return empty, nil
	}
	if len(key) > entities.HashHexSize || strings.Trim(key, "0123456789abcdef") != "" {
		return 0, fmt.Errorf("invalid key %q: expected a lowercase hexadecimal hash or prefix", key)
	}
	p := key[:min(len(key), 8)]
	v, err := strconv.ParseUint(p+strings.Repeat("0", 8-len(p)), 16, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid key %q: %w", key, err)
	}
	return v, nil
}

// limiter spaces out work to a rate of units per second.
type limiter struct {
	mu       sync.Mutex
	interval time.Duration
	next     time.Time
}

// newLimiter returns a limiter for rate units per second, or nil for no limit.
func newLimiter(rate float64) *limiter {
	if rate <= 0 {
		return nil
	}
	return &limiter{interval: time.Duration(float64(time.Second) / rate)}
}

// wait reserves n units and waits until their turn.
func (l *limiter) wait(ctx context.Context, n int) error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	now := time.Now()
	if l.next.Before(now) {
		l.next = now
	}
	at := l.next
	l.next = l.next.Add(time.Duration(n) * l.interval)
	l.mu.Unlock()

	timer := time.NewTimer(time.Until(at))
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
