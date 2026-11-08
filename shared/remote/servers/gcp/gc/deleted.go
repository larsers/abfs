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
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"cloud.google.com/go/storage"

	"github.com/google/abfs/shared/entities"
)

// A delete record is the object gc/deleted/<state>/<cycle>/<level>-<range>-<batch>
// in the server bucket, written by the delete step before it deletes the rows
// of one batch, so that the chunks of every deleted object can be found after
// its row is gone. It holds a header, then one entry per key of the batch:
// for an object, the 20-byte key and the length and bytes of its
// data:content descriptor, empty when every chunk is inline; for a map row,
// the keys of its two ends. Integers are big-endian.
const (
	recordPrefix        = "gc/deleted/"
	recordMagic         = "ABFSGCD\n"
	recordFormatVersion = 1
	recordObject        = 1
	recordMapRow        = 2
	// inlineDepot is the depot of the server that keeps content in the
	// descriptor itself, which needs no delete.
	inlineDepot = "inline"
	// prunedMarker is the object <cycle>/pruned among the delete records,
	// written before the records of the cycle are pruned, so that an undo of
	// the cycle fails instead of finding nothing to restore. It carries the
	// flag of a done record, so that the chunk step skips it.
	prunedMarker = "pruned"
	// undoWindow is the shortest time delete and ref records are kept. It is
	// the shortest soft delete retention under which the chunk step deletes
	// bucket chunks, the time a deleted chunk can come back, and longer than
	// the retention of the backups an undo restores from.
	undoWindow = 7 * 24 * time.Hour
)

// recordPrefixOf returns the prefix of the delete records of the garbage
// collector state.
func recordPrefixOf(state string) string {
	return recordPrefix + state + "/"
}

// recordObjectName returns the name of the delete record of one batch of a
// range and level of the delete step of cycle.
func recordObjectName(state string, cycle int64, level, rng, batch int) string {
	return recordPrefixOf(state) + strconv.FormatInt(cycle, 10) + "/" + strconv.Itoa(level) + "-" + strconv.Itoa(rng) + "-" + strconv.Itoa(batch)
}

// prunedMarkerName returns the name of the marker of the pruned records of
// cycle.
func prunedMarkerName(state string, cycle int64) string {
	return recordPrefixOf(state) + strconv.FormatInt(cycle, 10) + "/" + prunedMarker
}

// recordRetention returns how long ref records are kept after they were
// written, and delete records after they were flagged: as long as the bucket
// keeps a deleted chunk in soft delete,
// and at least undoWindow, so that an undo finds them while it can restore
// what they name. The read runs within the deadline of b.
func recordRetention(ctx context.Context, b writeBudget, bucket *storage.BucketHandle) (time.Duration, error) {
	var attrs *storage.BucketAttrs
	err := retryWith(ctx, firstRetryWait, writeBudget{deadline: b.deadline}, func(actx context.Context) error {
		var err error
		attrs, err = bucket.Attrs(actx)
		return err
	})
	if err != nil {
		return 0, fmt.Errorf("failed to read the soft delete retention of the bucket: %w", err)
	}
	keep := undoWindow
	if p := attrs.SoftDeletePolicy; p != nil {
		keep = max(keep, p.RetentionDuration)
	}
	return keep, nil
}

// recordHeader is the header of a delete record.
type recordHeader struct {
	cycle int64
	runID string
	// gcRowsAfter is the grace start of the cycle: a gc row whose newest
	// e:links cell is newer belongs to an object written again after the
	// mark, and stays.
	gcRowsAfter time.Time
	// listComplete is when the list the cycle deleted from was complete.
	listComplete time.Time
	count        int64
}

// recordEntry is an entry of a delete record: an object and its content
// descriptor, or a map row from key to mapTo.
type recordEntry struct {
	kind    uint8
	key     Hash
	mapTo   Hash
	content []byte
}

func (h *recordHeader) write(w io.Writer) error {
	b := []byte(recordMagic)
	b = binary.BigEndian.AppendUint32(b, recordFormatVersion)
	b = binary.BigEndian.AppendUint64(b, uint64(h.cycle))
	b = appendString(b, h.runID)
	b = binary.BigEndian.AppendUint64(b, uint64(h.gcRowsAfter.UnixMicro()))
	b = binary.BigEndian.AppendUint64(b, uint64(h.listComplete.UnixMicro()))
	b = binary.BigEndian.AppendUint64(b, uint64(h.count))
	_, err := w.Write(b)
	return err
}

// writeRecord writes the delete record name with the header h and entries,
// only if no object of that name exists or this run wrote it.
func writeRecord(ctx context.Context, b writeBudget, bucket *storage.BucketHandle, name string, h *recordHeader, entries []recordEntry) error {
	h.count = int64(len(entries))
	return createObject(ctx, b, bucket, name, h.runID, "application/octet-stream", func(w io.Writer) error {
		if err := h.write(w); err != nil {
			return err
		}
		var buf []byte
		for i := range entries {
			e := &entries[i]
			buf = append(buf[:0], e.kind)
			buf = append(buf, e.key[:]...)
			switch e.kind {
			case recordObject:
				buf = binary.BigEndian.AppendUint32(buf, uint32(len(e.content)))
				buf = append(buf, e.content...)
			case recordMapRow:
				buf = append(buf, e.mapTo[:]...)
			default:
				return fmt.Errorf("invalid delete record entry kind %d", e.kind)
			}
			if _, err := w.Write(buf); err != nil {
				return err
			}
		}
		return nil
	})
}

// readRecord reads the delete record name.
func readRecord(ctx context.Context, bucket *storage.BucketHandle, name string) (_ *recordHeader, _ []recordEntry, retErr error) {
	r, err := bucket.Object(name).NewReader(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read the delete record %s: %w", name, err)
	}
	defer entities.CloseErrCheck(r, &retErr)
	br := bufio.NewReader(r)
	h, err := readRecordHeader(br)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read the header of the delete record %s: %w", name, err)
	}
	entries := make([]recordEntry, 0, h.count)
	for range h.count {
		e, err := readRecordEntry(br)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read the delete record %s after %d entries: %w", name, len(entries), err)
		}
		entries = append(entries, e)
	}
	if _, err := br.ReadByte(); err != io.EOF {
		return nil, nil, fmt.Errorf("the delete record %s has more than the %d entries its header counts", name, h.count)
	}
	return h, entries, nil
}

func readRecordHeader(r io.Reader) (*recordHeader, error) {
	magic := make([]byte, len(recordMagic))
	if _, err := io.ReadFull(r, magic); err != nil {
		return nil, err
	}
	if string(magic) != recordMagic {
		return nil, errors.New("not a delete record")
	}
	var fixed struct {
		Version uint32
		Cycle   int64
	}
	if err := binary.Read(r, binary.BigEndian, &fixed); err != nil {
		return nil, err
	}
	if fixed.Version != recordFormatVersion {
		return nil, fmt.Errorf("unsupported delete record format version %d", fixed.Version)
	}
	h := &recordHeader{cycle: fixed.Cycle}
	var err error
	if h.runID, err = readString(r); err != nil {
		return nil, err
	}
	var times struct {
		GCRowsAfter  int64
		ListComplete int64
		Count        int64
	}
	if err := binary.Read(r, binary.BigEndian, &times); err != nil {
		return nil, err
	}
	h.gcRowsAfter, h.listComplete, h.count = time.UnixMicro(times.GCRowsAfter), time.UnixMicro(times.ListComplete), times.Count
	return h, nil
}

func readRecordEntry(r io.Reader) (recordEntry, error) {
	var e recordEntry
	kind := make([]byte, 1+entities.HashSize)
	if _, err := io.ReadFull(r, kind); err != nil {
		return e, err
	}
	e.kind, e.key = kind[0], Hash(kind[1:])
	switch e.kind {
	case recordObject:
		var n uint32
		if err := binary.Read(r, binary.BigEndian, &n); err != nil {
			return e, err
		}
		if n > 0 {
			e.content = make([]byte, n)
			if _, err := io.ReadFull(r, e.content); err != nil {
				return e, err
			}
		}
	case recordMapRow:
		to := make([]byte, entities.HashSize)
		if _, err := io.ReadFull(r, to); err != nil {
			return e, err
		}
		e.mapTo = Hash(to)
	default:
		return e, fmt.Errorf("invalid entry kind %d", e.kind)
	}
	return e, nil
}

// recordedContent returns the descriptor of data:content that a delete
// record keeps for an object: the descriptor when a chunk of it is outside
// the inline depot, else nil. A descriptor that does not parse is kept as it
// is.
func recordedContent(content []byte) []byte {
	var chunks []struct {
		Depot string `json:"depot"`
	}
	if err := json.Unmarshal(content, &chunks); err != nil {
		return content
	}
	for _, c := range chunks {
		if c.Depot != inlineDepot {
			return content
		}
	}
	return nil
}
