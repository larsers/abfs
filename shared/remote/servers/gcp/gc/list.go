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
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/google/abfs/shared/entities"
)

// A condemned list is the object gc/condemned/<state>/<cycle> in the server
// bucket: a header, then one record per condemned object in key order, of
// the 20-byte key, the type as an index into the header's type names, and
// the size, big-endian. Format version 1 has no chunk descriptors.
const (
	listPrefix        = "gc/condemned/"
	liveSuffix        = ".live"
	listMagic         = "ABFSGCL\n"
	listFormatVersion = 1
	listRecordSize    = entities.HashSize + 1 + 8
	// runIDMetadata holds the run that wrote a list object, so that a retried
	// create can tell its own earlier attempt from another run.
	runIDMetadata = "abfs-gc-run"
)

// listObjectName returns the name of the condemned list of a cycle of the
// garbage collector state.
func listObjectName(state string, cycle int64) string {
	return listPrefix + state + "/" + strconv.FormatInt(cycle, 10)
}

// listHeader is the header of a condemned list.
type listHeader struct {
	cycle    int64
	snapshot time.Time
	// holder and epoch name the run that wrote it.
	holder string
	epoch  int64
	count  int64
	types  []string
}

func (h *listHeader) runID() string {
	return runID(h.holder, h.epoch)
}

// runID names the run of a lease holder and epoch.
func runID(holder string, epoch int64) string {
	return holder + "#" + strconv.FormatInt(epoch, 10)
}

func (h *listHeader) write(w io.Writer) error {
	b := []byte(listMagic)
	b = binary.BigEndian.AppendUint32(b, listFormatVersion)
	b = binary.BigEndian.AppendUint64(b, uint64(h.cycle))
	b = binary.BigEndian.AppendUint64(b, uint64(h.snapshot.UnixMicro()))
	b = appendString(b, h.holder)
	b = binary.BigEndian.AppendUint64(b, uint64(h.epoch))
	b = binary.BigEndian.AppendUint64(b, uint64(h.count))
	b = binary.BigEndian.AppendUint16(b, uint16(len(h.types)))
	for _, t := range h.types {
		b = appendString(b, t)
	}
	_, err := w.Write(b)
	return err
}

func appendString(b []byte, s string) []byte {
	return append(binary.BigEndian.AppendUint16(b, uint16(len(s))), s...)
}

func readListHeader(r io.Reader) (*listHeader, error) {
	magic := make([]byte, len(listMagic))
	if _, err := io.ReadFull(r, magic); err != nil {
		return nil, err
	}
	if string(magic) != listMagic {
		return nil, errors.New("not a condemned list")
	}
	var fixed struct {
		Version  uint32
		Cycle    int64
		Snapshot int64
	}
	if err := binary.Read(r, binary.BigEndian, &fixed); err != nil {
		return nil, err
	}
	if fixed.Version != listFormatVersion {
		return nil, fmt.Errorf("unsupported condemned list format version %d", fixed.Version)
	}
	h := &listHeader{cycle: fixed.Cycle, snapshot: time.UnixMicro(fixed.Snapshot)}
	var err error
	if h.holder, err = readString(r); err != nil {
		return nil, err
	}
	var counts struct {
		Epoch int64
		Count int64
		Types uint16
	}
	if err := binary.Read(r, binary.BigEndian, &counts); err != nil {
		return nil, err
	}
	h.epoch, h.count = counts.Epoch, counts.Count
	for range counts.Types {
		t, err := readString(r)
		if err != nil {
			return nil, err
		}
		h.types = append(h.types, t)
	}
	return h, nil
}

func readString(r io.Reader) (string, error) {
	var n uint16
	if err := binary.Read(r, binary.BigEndian, &n); err != nil {
		return "", err
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return "", err
	}
	return string(b), nil
}

// listRecord is one record of a condemned list.
type listRecord struct {
	key  Hash
	typ  uint8
	size int64
}

// writeList writes the condemned list name with the header h and the
// records that next returns, until it returns false. The object is created
// only if it does not exist.
func writeList(ctx context.Context, bucket *storage.BucketHandle, name string, h *listHeader, next func() (listRecord, bool)) error {
	return createObject(ctx, bucket, name, h.runID(), "application/octet-stream", func(w io.Writer) error {
		if err := h.write(w); err != nil {
			return err
		}
		var n int64
		b := make([]byte, 0, listRecordSize)
		for rec, ok := next(); ok; rec, ok = next() {
			b = append(b[:0], rec.key[:]...)
			b = append(b, rec.typ)
			b = binary.BigEndian.AppendUint64(b, uint64(rec.size))
			if _, err := w.Write(b); err != nil {
				return err
			}
			n++
		}
		if n != h.count {
			return fmt.Errorf("wrote %d records, the header counts %d", n, h.count)
		}
		return nil
	})
}

// createObject writes the object name with write, only if no object of that
// name exists. If one exists, written by the same run, an earlier attempt of
// this call wrote it and the call succeeds.
func createObject(ctx context.Context, bucket *storage.BucketHandle, name, runID, contentType string, write func(io.Writer) error) error {
	obj := bucket.Object(name)
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	w := obj.If(storage.Conditions{DoesNotExist: true}).NewWriter(wctx)
	w.ContentType = contentType
	w.Metadata = map[string]string{runIDMetadata: runID}
	bw := bufio.NewWriterSize(w, 1<<20)
	err := write(bw)
	if err == nil {
		err = bw.Flush()
	}
	if err != nil {
		// Canceling the context before Close discards what was written.
		cancel()
		_ = w.Close()
		return fmt.Errorf("failed to write the Cloud Storage object %s: %w", name, err)
	}
	err = w.Close()
	if err == nil {
		return nil
	}
	if !preconditionFailed(err) {
		return fmt.Errorf("failed to write the Cloud Storage object %s: %w", name, err)
	}
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return fmt.Errorf("the Cloud Storage object %s exists and cannot be read: %w", name, err)
	}
	if got := attrs.Metadata[runIDMetadata]; got != runID {
		return fmt.Errorf("the Cloud Storage object %s exists, written by the GC run %q, not by this run %q", name, got, runID)
	}
	return nil
}

func preconditionFailed(err error) bool {
	var gerr *googleapi.Error
	if errors.As(err, &gerr) && gerr.Code == http.StatusPreconditionFailed {
		return true
	}
	return status.Code(err) == codes.FailedPrecondition
}

// condemnedList holds the keys of a condemned list and their types.
type condemnedList struct {
	header *listHeader
	keys   []Hash
	// types holds the index of each key's type in header.types.
	types []uint8
}

// typeName returns the type name of the key i.
func (l *condemnedList) typeName(i int) string {
	if t := int(l.types[i]); t < len(l.header.types) {
		return l.header.types[t]
	}
	return "unknown"
}

// readList reads the condemned list name.
func readList(ctx context.Context, bucket *storage.BucketHandle, name string) (_ *condemnedList, retErr error) {
	r, err := bucket.Object(name).NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read the condemned list %s: %w", name, err)
	}
	defer entities.CloseErrCheck(r, &retErr)
	br := bufio.NewReaderSize(r, 1<<20)
	h, err := readListHeader(br)
	if err != nil {
		return nil, fmt.Errorf("failed to read the header of the condemned list %s: %w", name, err)
	}
	l := &condemnedList{header: h, keys: make([]Hash, 0, h.count), types: make([]uint8, 0, h.count)}
	b := make([]byte, listRecordSize)
	for range h.count {
		if _, err := io.ReadFull(br, b); err != nil {
			return nil, fmt.Errorf("failed to read the condemned list %s after %d records: %w", name, len(l.keys), err)
		}
		l.keys = append(l.keys, Hash(b[:entities.HashSize]))
		l.types = append(l.types, b[entities.HashSize])
	}
	if _, err := br.ReadByte(); err != io.EOF {
		return nil, fmt.Errorf("the condemned list %s has more than the %d records its header counts", name, h.count)
	}
	return l, nil
}

// pruneLists deletes the list objects of the state beyond the newest keep
// cycles, with their companions, and returns how many it deleted. It leaves
// other names under the prefix alone.
func pruneLists(ctx context.Context, bucket *storage.BucketHandle, state string, keep int) (int64, error) {
	prefix := listPrefix + state + "/"
	byCycle := make(map[int64][]string)
	it := bucket.Objects(ctx, &storage.Query{Prefix: prefix})
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return 0, fmt.Errorf("failed to list the condemned lists under %s: %w", prefix, err)
		}
		cycle, err := strconv.ParseInt(strings.TrimSuffix(strings.TrimPrefix(attrs.Name, prefix), liveSuffix), 10, 64)
		if err != nil {
			continue
		}
		byCycle[cycle] = append(byCycle[cycle], attrs.Name)
	}
	cycles := slices.Sorted(maps.Keys(byCycle))
	var n int64
	for _, c := range cycles[:max(0, len(cycles)-keep)] {
		for _, name := range byCycle[c] {
			if err := bucket.Object(name).Delete(ctx); err != nil && !errors.Is(err, storage.ErrObjectNotExist) {
				return n, fmt.Errorf("failed to delete the old condemned list %s: %w", name, err)
			}
			n++
		}
	}
	return n, nil
}
