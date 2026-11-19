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
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/storage"

	"github.com/google/abfs/shared/entities"
)

// A delete record is written before the deletes of its batch, so the chunks can
// be found and an undo can write the rows back. Integers are big-endian.
const (
	recordPrefix        = "abfsgc/deleted/"
	recordMagic         = "ABFSGCD\n"
	recordFormatVersion = 2
	recordObject        = 1
	recordMapRow        = 2
	// inlineDepot keeps content in the descriptor, which needs no delete.
	inlineDepot = "inline"
	// prunedMarker carries the flag of a done record, so the chunk step skips it.
	prunedMarker = "pruned"
	// undoWindow is how long after its record the chunk step deletes chunks.
	undoWindow = 7 * 24 * time.Hour
	// recordRetention keeps the records an undo needs.
	recordRetention = undoWindow
)

// maxRecordBytes is the most bytes of entries in one delete record. Tests change it.
var maxRecordBytes = 16 << 20

func recordPrefixOf(state string) string {
	return recordPrefix + state + "/"
}

func recordObjectName(state string, cycle int64, level, rng, batch int) string {
	return recordPrefixOf(state) + strconv.FormatInt(cycle, 10) + "/" + strconv.Itoa(level) + "-" + strconv.Itoa(rng) + "-" + strconv.Itoa(batch)
}

func prunedMarkerName(state string, cycle int64) string {
	return recordPrefixOf(state) + strconv.FormatInt(cycle, 10) + "/" + prunedMarker
}

type recordHeader struct {
	cycle int64
	runID string
	// gcRowsAfter is the grace start; a gc row with a newer e:links cell stays.
	gcRowsAfter time.Time
	count       int64
}

type recordCell struct {
	family, column string
	ts             bigtable.Timestamp
	value          []byte
}

// An object entry without edges had no gc row.
type recordEntry struct {
	kind  uint8
	key   Hash
	mapTo Hash
	cells []recordCell
	edges []recordCell
}

func objectEntry(key Hash, object, gc bigtable.Row) recordEntry {
	e := recordEntry{kind: recordObject, key: key}
	for _, family := range []string{objectsMetaFamily, objectsDataFamily} {
		for _, item := range object[family] {
			e.cells = append(e.cells, recordCell{family: family, column: strings.TrimPrefix(item.Column, family+":"), ts: item.Timestamp, value: item.Value})
		}
	}
	for _, item := range gc[EdgesFamily] {
		e.edges = append(e.edges, recordCell{family: EdgesFamily, column: strings.TrimPrefix(item.Column, EdgesFamily+":"), value: item.Value})
	}
	return e
}

func (e *recordEntry) content() []byte {
	for _, c := range e.cells {
		if c.family == objectsDataFamily && c.column == objectsContentColumn {
			return c.value
		}
	}
	return nil
}

func (e *recordEntry) edge(column string) ([]byte, bool) {
	for _, c := range e.edges {
		if c.column == column {
			return c.value, true
		}
	}
	return nil, false
}

func (e *recordEntry) size() int {
	n := 1 + entities.HashSize
	if e.kind == recordMapRow {
		return n + entities.HashSize
	}
	n += 2 + 1
	for _, c := range e.cells {
		n += 2 + len(c.family) + 2 + len(c.column) + 8 + 4 + len(c.value)
	}
	for _, c := range e.edges {
		n += 2 + len(c.column) + 4 + len(c.value)
	}
	return n
}

func (h *recordHeader) write(w io.Writer) (int, error) {
	b := []byte(recordMagic)
	b = binary.BigEndian.AppendUint32(b, recordFormatVersion)
	b = binary.BigEndian.AppendUint64(b, uint64(h.cycle))
	b = appendString(b, h.runID)
	b = binary.BigEndian.AppendUint64(b, uint64(h.gcRowsAfter.UnixMicro()))
	b = binary.BigEndian.AppendUint64(b, uint64(h.count))
	return w.Write(b)
}

// writeRecord writes only if no object of that name exists or this run wrote it.
func writeRecord(ctx context.Context, b writeBudget, bucket *storage.BucketHandle, name string, h *recordHeader, entries []recordEntry) (int64, error) {
	var size int64
	err := createObject(ctx, b, bucket, name, h.runID, "application/octet-stream", func(w io.Writer) error {
		var err error
		size, err = encodeRecord(w, h, entries)
		return err
	})
	return size, err
}

func encodeRecord(w io.Writer, h *recordHeader, entries []recordEntry) (int64, error) {
	h.count = int64(len(entries))
	n, err := h.write(w)
	size := int64(n)
	if err != nil {
		return size, err
	}
	var buf []byte
	for i := range entries {
		e := &entries[i]
		buf = append(buf[:0], e.kind)
		buf = append(buf, e.key[:]...)
		switch e.kind {
		case recordObject:
		case recordMapRow:
			buf = append(buf, e.mapTo[:]...)
		default:
			return size, fmt.Errorf("invalid delete record entry kind %d", e.kind)
		}
		n, err := w.Write(buf)
		size += int64(n)
		if err != nil {
			return size, err
		}
	}
	for i := range entries {
		e := &entries[i]
		if e.kind != recordObject {
			continue
		}
		if len(e.cells) > 1<<16-1 || len(e.edges) > 1<<8-1 {
			return size, fmt.Errorf("the object %s has too many cells for a delete record", e.key)
		}
		buf = binary.BigEndian.AppendUint16(buf[:0], uint16(len(e.cells)))
		for _, c := range e.cells {
			buf = appendString(appendString(buf, c.family), c.column)
			buf = binary.BigEndian.AppendUint64(buf, uint64(c.ts))
			buf = appendBytes(buf, c.value)
		}
		buf = append(buf, uint8(len(e.edges)))
		for _, c := range e.edges {
			buf = appendBytes(appendString(buf, c.column), c.value)
		}
		n, err := w.Write(buf)
		size += int64(n)
		if err != nil {
			return size, err
		}
	}
	return size, nil
}

func appendBytes(b, v []byte) []byte {
	return append(binary.BigEndian.AppendUint32(b, uint32(len(v))), v...)
}

func readRecord(ctx context.Context, bucket *storage.BucketHandle, name string) (*recordHeader, []recordEntry, error) {
	return readRecordSections(ctx, bucket, name, true)
}

func readRecordKeys(ctx context.Context, bucket *storage.BucketHandle, name string) (*recordHeader, []recordEntry, error) {
	return readRecordSections(ctx, bucket, name, false)
}

func readRecordSections(ctx context.Context, bucket *storage.BucketHandle, name string, rows bool) (_ *recordHeader, _ []recordEntry, retErr error) {
	r, err := bucket.Object(name).NewReader(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read the delete record %s: %w", name, err)
	}
	if rows {
		defer entities.CloseErrCheck(r, &retErr)
	} else {
		// Closed before the end, the reader stops the download.
		defer func() { _ = r.Close() }()
	}
	h, entries, err := decodeRecord(r, r.Attrs.Size, rows)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read the delete record %s: %w", name, err)
	}
	return h, entries, nil
}

func decodeRecord(r io.Reader, size int64, rows bool) (*recordHeader, []recordEntry, error) {
	br := bufio.NewReader(r)
	sr := &sizedReader{r: br, left: size}
	h, err := readRecordHeader(sr)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read the header: %w", err)
	}
	if err := sr.checkCount(h.count, 1+entities.HashSize); err != nil {
		return nil, nil, fmt.Errorf("invalid header: %w", err)
	}
	entries := make([]recordEntry, 0, h.count)
	for range h.count {
		e, err := readRecordKey(sr)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read the key after %d keys: %w", len(entries), err)
		}
		entries = append(entries, e)
	}
	if !rows {
		return h, entries, nil
	}
	for i := range entries {
		e := &entries[i]
		if e.kind != recordObject {
			continue
		}
		if err := readRecordRow(sr, e); err != nil {
			return nil, nil, fmt.Errorf("failed to read the row of %s: %w", e.key, err)
		}
	}
	if err := checkEnd(br, h.count); err != nil {
		return nil, nil, err
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
		GCRowsAfter int64
		Count       int64
	}
	if err := binary.Read(r, binary.BigEndian, &times); err != nil {
		return nil, err
	}
	h.gcRowsAfter, h.count = time.UnixMicro(times.GCRowsAfter).UTC(), times.Count
	return h, nil
}

func readRecordKey(r io.Reader) (recordEntry, error) {
	var e recordEntry
	kind := make([]byte, 1+entities.HashSize)
	if _, err := io.ReadFull(r, kind); err != nil {
		return e, err
	}
	e.kind, e.key = kind[0], Hash(kind[1:])
	switch e.kind {
	case recordObject:
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

func readRecordRow(r *sizedReader, e *recordEntry) error {
	var cells uint16
	if err := binary.Read(r, binary.BigEndian, &cells); err != nil {
		return err
	}
	for range cells {
		var c recordCell
		var err error
		if c.family, err = readString(r); err != nil {
			return err
		}
		if c.column, err = readString(r); err != nil {
			return err
		}
		var ts int64
		if err := binary.Read(r, binary.BigEndian, &ts); err != nil {
			return err
		}
		c.ts = bigtable.Timestamp(ts)
		if c.value, err = readBytes(r); err != nil {
			return err
		}
		e.cells = append(e.cells, c)
	}
	var edges uint8
	if err := binary.Read(r, binary.BigEndian, &edges); err != nil {
		return err
	}
	for range edges {
		c := recordCell{family: EdgesFamily}
		var err error
		if c.column, err = readString(r); err != nil {
			return err
		}
		if c.value, err = readBytes(r); err != nil {
			return err
		}
		e.edges = append(e.edges, c)
	}
	return nil
}

func readBytes(r *sizedReader) ([]byte, error) {
	var n uint32
	if err := binary.Read(r, binary.BigEndian, &n); err != nil {
		return nil, err
	}
	if int64(n) > r.left {
		return nil, fmt.Errorf("a value of %d bytes is longer than the %d bytes left", n, r.left)
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, err
	}
	return b, nil
}
