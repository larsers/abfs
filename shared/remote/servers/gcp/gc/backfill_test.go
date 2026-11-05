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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSplitRange(t *testing.T) {
	tests := []struct {
		name       string
		start, end string
		n          int
		want       []string
	}{
		{"whole table", "", "", 4, []string{"", "40000000", "80000000", "c0000000", ""}},
		{"one shard", "", "", 1, []string{"", ""}},
		{"from a key", "8", "", 2, []string{"8", "c0000000", ""}},
		{"bounded", "10", "20", 2, []string{"10", "18000000", "20"}},
		{"long start key", "abcd1234ff", "abcd1236", 2, []string{"abcd1234ff", "abcd1235", "abcd1236"}},
		{"too small to split", "abcd1234ff", "abcd1234ff01", 4, []string{"abcd1234ff", "abcd1234ff01"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := splitRange(tt.start, tt.end, tt.n)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestSplitRange_RejectsBadKeys(t *testing.T) {
	tests := []struct {
		name       string
		start, end string
		want       string
	}{
		{"upper case", "ABC", "", "lowercase hexadecimal"},
		{"not hex", "xyz", "", "lowercase hexadecimal"},
		{"too long", "00000000000000000000000000000000000000000", "", "lowercase hexadecimal"},
		{"empty range", "20", "10", "empty key range"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := splitRange(tt.start, tt.end, 2)
			require.ErrorContains(t, err, tt.want)
		})
	}
}

func TestLimiter_SpacesOutWork(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	require.NoError(newLimiter(0).wait(ctx, 1000))

	l := newLimiter(100)
	start := time.Now()
	for range 3 {
		require.NoError(l.wait(ctx, 10))
	}
	// The first 10 units go at once, the next two batches 100 ms apart.
	require.GreaterOrEqual(time.Since(start), 190*time.Millisecond)

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	require.NoError(l.wait(ctx, 1000))
	require.ErrorIs(l.wait(cancelled, 1), context.Canceled)
}
