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

-- Spanner holds projects, refs and the state of the garbage collector; objects,
-- maps and chunks live in Bigtable and Cloud Storage. Ref targets therefore
-- have no foreign key.
-- Every statement can be applied again to an existing database.

CREATE SEQUENCE IF NOT EXISTS ProjectIdSequence OPTIONS (sequence_kind="bit_reversed_positive");
CREATE TABLE IF NOT EXISTS Projects (
    ProjectId INT64 DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE ProjectIdSequence)),
    Host STRING(MAX) NOT NULL,
    Project STRING(MAX) NOT NULL,
) PRIMARY KEY (ProjectId);
CREATE UNIQUE INDEX IF NOT EXISTS ProjectsIndex ON Projects(Host, Project);

CREATE TABLE IF NOT EXISTS Refs (
    ProjectId INT64 NOT NULL,
    Ref STRING(MAX) NOT NULL,
    UpdateTime TIMESTAMP NOT NULL,
    Target STRING(128),
    CONSTRAINT FK_Refs_ProjectId FOREIGN KEY (ProjectId) REFERENCES Projects (ProjectId),
) PRIMARY KEY (ProjectId, Ref);
CREATE INDEX IF NOT EXISTS RefsRefIndex ON Refs(Ref);

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
-- version the replica writes into the Bigtable table gc, 0 when it writes none.
CREATE TABLE IF NOT EXISTS GcReplicas (
    ReplicaId STRING(MAX) NOT NULL,
    Version INT64 NOT NULL,
    LastSeen TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (ReplicaId);
