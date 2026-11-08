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
-- (LeaseHolder until LeaseExpiry, fenced by LeaseEpoch) runs the cycles, and
-- Phase is the mode of its cycle. PreviousLeaseExpiry is the expiry of the
-- lease that the current holder took over, NULL when it had been released.
CREATE TABLE IF NOT EXISTS GcState (
    Name STRING(64) NOT NULL,
    Cycle INT64 NOT NULL,
    Phase STRING(64),
    LeaseHolder STRING(MAX),
    LeaseEpoch INT64 NOT NULL,
    LeaseExpiry TIMESTAMP,
    PreviousLeaseExpiry TIMESTAMP,
    CycleStartTime TIMESTAMP,
    PreviousCycleStartTime TIMESTAMP,
    SnapshotTime TIMESTAMP,
    GraceSeconds INT64,
    ListLocation STRING(MAX),
    UpdateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (Name);

-- The condemned list of the last condemn cycle: ListLocation is its object in
-- the server bucket, ListSnapshotTime the snapshot time of that cycle, and
-- ListComplete tells that every marker of the list was written. Added after
-- the table, so that init --gc adds them to a GcState created without them.
ALTER TABLE GcState ADD COLUMN IF NOT EXISTS ListSnapshotTime TIMESTAMP;
ALTER TABLE GcState ADD COLUMN IF NOT EXISTS ListComplete BOOL;

-- What a delete cycle needs to trust the list: ListCompleteTime is the commit
-- time of the write that completed the list, CondemnCycle the cycle of the
-- last condemn or delete lease, MaxGraceSeconds the largest grace of such a
-- lease, and LegacyServersGoneTime the time a cycle first found GcReplicas
-- empty, NULL while it holds a row.
ALTER TABLE GcState ADD COLUMN IF NOT EXISTS ListCompleteTime TIMESTAMP OPTIONS (allow_commit_timestamp=true);
ALTER TABLE GcState ADD COLUMN IF NOT EXISTS CondemnCycle INT64;
ALTER TABLE GcState ADD COLUMN IF NOT EXISTS LegacyServersGoneTime TIMESTAMP OPTIONS (allow_commit_timestamp=true);
ALTER TABLE GcState ADD COLUMN IF NOT EXISTS MaxGraceSeconds INT64;

-- One heartbeat row per server replica. Version is the GC edge extractor
-- version the replica writes into the Bigtable table gc; GcEnabled, the touch
-- period and the cache TTL are its settings, zero meaning off.
CREATE TABLE IF NOT EXISTS GcReplicas (
    ReplicaId STRING(MAX) NOT NULL,
    Version INT64 NOT NULL,
    GcEnabled BOOL NOT NULL,
    TouchIntervalSeconds INT64 NOT NULL,
    CacheTTLSeconds INT64 NOT NULL,
    LastSeen TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (ReplicaId);

-- One heartbeat row per server process with GC, kept when it stops, so that
-- the collector can check every process that ran in a time window. StatsTime
-- is a Spanner read time taken before the process read the touch stats its
-- last heartbeat recorded; TouchLossTime is set when a heartbeat or the stop
-- records lost touches, StoppedAt by a clean stop. A process that has neither
-- a recent heartbeat nor StoppedAt crashed.
CREATE TABLE IF NOT EXISTS GcServers (
    ServerId STRING(MAX) NOT NULL,
    Version INT64 NOT NULL,
    GcEnabled BOOL NOT NULL,
    TouchIntervalSeconds INT64 NOT NULL,
    CacheTTLSeconds INT64 NOT NULL,
    HeartbeatIntervalSeconds INT64 NOT NULL,
    LinkValidation BOOL NOT NULL,
    LastSeen TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
    StatsTime TIMESTAMP NOT NULL,
    TouchLossTime TIMESTAMP OPTIONS (allow_commit_timestamp=true),
    StoppedAt TIMESTAMP OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (ServerId);
