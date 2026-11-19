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
	// Dropped counts the drops of entry pushes by dagsync, because an object they needed was
	// in no local store and missing on the server. A drop counts once for each entry, also
	// when later checks sent its push again while it waited. The entries are parked.
	Dropped int64 `json:"dropped"`
	// ClearRetries counts the retries of index transactions that took a local write out of
	// the LRU, or parked an entry, after a conflict.
	ClearRetries int64 `json:"clear_retries"`
	// ClearFailures counts local writes whose index transaction failed with an error other
	// than a conflict; their check takes the entry out of the LRU when the server lacks it.
	// It also counts failed parks of the check after a start, which then stops and leaves
	// eviction held.
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
	// Skipped counts cached entries the checks left in the LRU without asking the server or
	// after a missing answer, because the cache holds no blob, tree or object of the ABFS
	// server under their key: ARI entries, RBE CAS blobs and entries whose content is gone.
	Skipped int64 `json:"skipped"`
	// Restored counts such entries that held a check marker and went back to the LRU at a
	// start.
	Restored int64 `json:"restored"`
	// Parked counts the entries parked: cached entries the check after a start found missing
	// on the server, and entries whose push was dropped. A parked entry stays out of eviction
	// without a push until a mount names it or its release. ParkedSize is the size of the
	// entries parked now, which counts toward the eviction threshold.
	Parked     int64 `json:"parked"`
	ParkedSize int64 `json:"parked_size"`
	// Released counts the parked entries that went back to the LRU after their park time.
	Released int64 `json:"released"`
	// EvictedParked counts the parked entries evicted because the cache was above its
	// eviction target with no other entry left to evict.
	EvictedParked int64 `json:"evicted_parked"`
}
