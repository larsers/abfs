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

package protocol

// RemoteCheckStats counts the checks of cached content with the server that keep locally
// written content in the cache until the server confirms it.
type RemoteCheckStats struct {
	// Queued is the number of hashes waiting for a check.
	Queued int64 `json:"queued"`
	// Sent, Present and Missing count the hashes sent in checks and the answers.
	Sent    int64 `json:"sent"`
	Present int64 `json:"present"`
	Missing int64 `json:"missing"`
	// Pushed counts missing hashes pushed from the cache; Unpushable those that cannot be
	// pushed and stay in the cache.
	Pushed     int64 `json:"pushed"`
	Unpushable int64 `json:"unpushable"`
	// ClearRetries counts the retries of index transactions that took a local write out of
	// the LRU after a conflict.
	ClearRetries int64 `json:"clear_retries"`
	// ClearFailures counts local writes whose index transaction failed with an error other
	// than a conflict; their check takes the entry out of the LRU when the server lacks it.
	ClearFailures int64 `json:"clear_failures"`
	// EvictSkippedUnconfirmed counts the entries eviction kept because no check confirmed them.
	EvictSkippedUnconfirmed int64 `json:"evict_skipped_unconfirmed"`
	// StartPass counts the hashes the check after a start sent.
	StartPass int64 `json:"start_pass"`
	// EvictedUnchecked counts the entries evicted before the check after a start reached
	// them, because the cache had reached its eviction threshold.
	EvictedUnchecked int64 `json:"evicted_unchecked"`
	// LostBeforeIndex counts local writes that were gone from the cache and missing on the
	// server when the check answered.
	LostBeforeIndex int64 `json:"lost_before_index"`
	// PhonyConfirmed counts entries that a successful phony request confirmed.
	PhonyConfirmed int64 `json:"phony_confirmed"`
}
