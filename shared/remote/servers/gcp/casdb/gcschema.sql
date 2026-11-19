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
-- MaxGraceSeconds is the largest grace of a condemn or delete lease. The
-- condemned list of the last condemn cycle: ListLocation is its object in the
-- server bucket, ListSnapshotTime the snapshot time of that cycle,
-- ListComplete tells that every marker of the list was written, and
-- ListCompleteTime is the commit time of the write that completed it.
CREATE TABLE IF NOT EXISTS AbfsGcState (
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
    MaxGraceSeconds INT64,
    ListLocation STRING(MAX),
    ListSnapshotTime TIMESTAMP,
    ListComplete BOOL,
    ListCompleteTime TIMESTAMP OPTIONS (allow_commit_timestamp=true),
    UpdateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (Name);

-- One row per condemned list of a garbage collector. A condemn cycle inserts
-- it before its first marker and sets CompleteTime, the largest cache TTL of
-- the servers and the number of markers after the last marker was written. A
-- marker of a cycle without a complete row is written again by a later
-- condemn; one with a complete row is deleted a hold after CompleteTime.
CREATE TABLE IF NOT EXISTS AbfsGcLists (
    Name STRING(64) NOT NULL,
    Cycle INT64 NOT NULL,
    SnapshotTime TIMESTAMP NOT NULL,
    Location STRING(MAX),
    CompleteTime TIMESTAMP OPTIONS (allow_commit_timestamp=true),
    CacheTTLSeconds INT64,
    Markers INT64,
) PRIMARY KEY (Name, Cycle);

-- One heartbeat row per server process with GC, kept when it stops, so that
-- the collector can check every process that ran in a time window. Version is
-- the version of the GC rules the process follows; GcEnabled, the touch
-- period and the cache TTL are its settings, zero meaning off. TouchLossTime
-- is set when a heartbeat or the stop records lost object touches, and
-- MapRowLossTime when they record lost map row touches; both are reported,
-- not checked. StoppedAt is set by a clean stop. A process that has neither a
-- recent heartbeat nor StoppedAt crashed.
CREATE TABLE IF NOT EXISTS AbfsGcServers (
    ServerId STRING(MAX) NOT NULL,
    Version INT64 NOT NULL,
    GcEnabled BOOL NOT NULL,
    TouchIntervalSeconds INT64 NOT NULL,
    CacheTTLSeconds INT64 NOT NULL,
    HeartbeatIntervalSeconds INT64 NOT NULL,
    LinkValidation BOOL NOT NULL,
    LastSeen TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
    TouchLossTime TIMESTAMP OPTIONS (allow_commit_timestamp=true),
    MapRowLossTime TIMESTAMP OPTIONS (allow_commit_timestamp=true),
    StoppedAt TIMESTAMP OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (ServerId);
