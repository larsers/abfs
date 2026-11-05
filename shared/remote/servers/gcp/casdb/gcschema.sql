-- Copyright 2026 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- The state of the garbage collector, created by init --gc. Every statement
-- can be applied again to an existing database.

-- One row per garbage collector instance. The process that holds the lease
-- (LeaseHolder until LeaseExpiry, fenced by LeaseEpoch) runs the cycles.
CREATE TABLE IF NOT EXISTS GcState (
    Name STRING(64) NOT NULL,
    Cycle INT64 NOT NULL,
    Phase STRING(64) NOT NULL,
    LeaseHolder STRING(MAX),
    LeaseEpoch INT64 NOT NULL,
    LeaseExpiry TIMESTAMP,
    CycleStartTime TIMESTAMP,
    PreviousCycleStartTime TIMESTAMP,
    SnapshotTime TIMESTAMP,
    GraceSeconds INT64,
    ListLocation STRING(MAX),
    UpdateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (Name);

-- One heartbeat row per server replica. Version is the GC edge extractor
-- version the replica writes into the Bigtable table gc.
CREATE TABLE IF NOT EXISTS GcReplicas (
    ReplicaId STRING(MAX) NOT NULL,
    Version INT64 NOT NULL,
    LastSeen TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (ReplicaId);
