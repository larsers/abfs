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
	"io"
	"time"
)

const (
	// DefaultWriteDeadline is the deadline of one attempt of a collector
	// write call, the library's own retries included.
	DefaultWriteDeadline = 30 * time.Second
	// streamRate is the throughput in bytes per second that a streamed
	// object write gets on top of the write deadline.
	streamRate = 8 << 20
)

// writeBudget bounds the writes of the collector. Without a deadline, a call
// that the library retries runs until the run's context ends, which only a
// lost lease does, so a stalled write would have no bound. Each attempt of a
// write call runs under the deadline, and no attempt starts when the lease
// could expire before two attempts and the clock skew margin have passed, so
// a holder that lost its renewals stops writing before another can take the
// lease over. The zero value adds no deadline and no lease check.
type writeBudget struct {
	deadline time.Duration
	lease    *Lease
}

// attempt returns the context of one attempt of a write call.
func (b writeBudget) attempt(ctx context.Context) (context.Context, context.CancelFunc, error) {
	if b.lease != nil {
		if err := b.lease.checkWrite(b.deadline); err != nil {
			return nil, nil, err
		}
	}
	if b.deadline <= 0 {
		return ctx, func() {}, nil
	}
	actx, cancel := context.WithTimeout(ctx, b.deadline)
	return actx, cancel, nil
}

// stream returns the context of a streamed object write, and wrap, which
// returns the writer through which the write sends its data to w. The context
// ends when the write runs longer than the deadline plus the time to send
// what it wrote so far at streamRate. stop releases the timer.
func (b writeBudget) stream(ctx context.Context) (_ context.Context, wrap func(w io.Writer) io.Writer, stop func(), _ error) {
	if b.lease != nil {
		if err := b.lease.checkWrite(b.deadline); err != nil {
			return nil, nil, nil, err
		}
	}
	if b.deadline <= 0 {
		return ctx, func(w io.Writer) io.Writer { return w }, func() {}, nil
	}
	sctx, cancel := context.WithCancel(ctx)
	dw := &deadlineWriter{start: time.Now(), deadline: b.deadline}
	dw.timer = time.AfterFunc(b.deadline, cancel)
	wrap = func(w io.Writer) io.Writer {
		dw.w = w
		return dw
	}
	return sctx, wrap, func() { dw.timer.Stop(); cancel() }, nil
}

// deadlineWriter moves the end of a streamed write later by the time to send
// each byte written at streamRate.
type deadlineWriter struct {
	w        io.Writer
	start    time.Time
	deadline time.Duration
	n        int64
	timer    *time.Timer
}

// Write implements io.Writer.
func (w *deadlineWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.n += int64(n)
	w.timer.Reset(time.Until(w.start.Add(w.deadline + time.Duration(float64(w.n)/streamRate*float64(time.Second)))))
	return n, err
}
