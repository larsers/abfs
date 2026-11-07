# syntax=docker/dockerfile:1.27@sha256:4edf897a3ffa55b89f906fc8cc78afdb3f1834cc9c7083565e611a8a7d5fe99e

# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# The GCP server, abfs-server with flat flags. It reads no instance config and
# writes no files (logs go to stderr), so it runs as 65532 on a read-only root
# file system. The image also carries the full abfs binary of the same commit
# at /usr/local/bin/abfs for admin use, for `abfs server gcp init` (the
# backend check of docs/runbook.md); the client and pusher binaries have no
# server command.
#
# Build context: scripts/build-images.sh stages the ABFS tree in abfs/.

ARG GO_IMAGE=golang:1.27.3-trixie@sha256:af1bfc2614ebb1dbd31af514b78740fd678850e76498762b425ecee4c5c6ea80
ARG BASE_IMAGE=debian:13.7-slim@sha256:a72b4433de1cf7d6bae0c4cee74d0547f58601a643b3375f070c51600d63b6b2

FROM ${GO_IMAGE} AS build
ARG VERSION
RUN test -n "${VERSION}" || { echo "VERSION build argument is required" >&2; exit 1; }
WORKDIR /src
COPY abfs/ ./
RUN --mount=type=cache,target=/root/go/pkg/mod go mod download
RUN --mount=type=cache,target=/root/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    VERSION="${VERSION}" make VERSION="${VERSION}" DESTDIR=/out abfs-server abfs

FROM ${BASE_IMAGE}
ARG VERSION
# The Debian packages are not pinned: the archive keeps only the current
# versions, so a rebuild takes those. The image digest in the chart values pins
# what runs.
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*
COPY --from=build /out/abfs-server /usr/local/bin/abfs-server
COPY --from=build /out/abfs /usr/local/bin/abfs
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/abfs-server"]
LABEL org.opencontainers.image.version="${VERSION}"
