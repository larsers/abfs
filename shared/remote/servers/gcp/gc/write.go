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
	// DefaultWriteDeadline bounds one attempt of a collector write, library retries included.
	DefaultWriteDeadline = 30 * time.Second
	// streamRate is the bytes per second a streamed write gets on top of the deadline.
	streamRate = 8 << 20
)

// writeBudget bounds the collector writes: each attempt runs under the deadline,
// and none starts unless the lease outlives two attempts and the clock skew
// margin, so a holder that lost its renewals stops writing before a takeover.
type writeBudget struct {
	deadline time.Duration
	lease    *Lease
}

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

// stream returns the context of a streamed write, which ends after the deadline
// plus the time to send the bytes written so far at streamRate.
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

type deadlineWriter struct {
	w        io.Writer
	start    time.Time
	deadline time.Duration
	n        int64
	timer    *time.Timer
}

func (w *deadlineWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.n += int64(n)
	w.timer.Reset(time.Until(w.start.Add(w.deadline + time.Duration(float64(w.n)/streamRate*float64(time.Second)))))
	return n, err
}
