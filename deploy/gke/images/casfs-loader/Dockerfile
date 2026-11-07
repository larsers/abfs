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

# The casfs module source for the loader DaemonSet, which builds it with the
# node's own headers and compiler. The loader script comes from the chart.
# The tree has no git metadata, so the build passes CASFS_VERSION to make.
#
# Build context: scripts/build-images.sh stages the abfs-casfs tree in casfs/.

ARG BASE_IMAGE=debian:13.7-slim@sha256:a72b4433de1cf7d6bae0c4cee74d0547f58601a643b3375f070c51600d63b6b2

FROM ${BASE_IMAGE}
ARG CASFS_VERSION
ARG CASFS_COMMIT
RUN test -n "${CASFS_VERSION}" || { echo "CASFS_VERSION build argument is required" >&2; exit 1; }
COPY casfs/ /usr/src/casfs/
# bash, mawk and coreutils (sort -V) come with the base image; check them.
RUN bash --version >/dev/null && awk 'BEGIN { exit 0 }' && \
    printf '1.10\n1.9\n' | sort -V >/dev/null && \
    test -f /usr/src/casfs/Makefile
ENV CASFS_VERSION=${CASFS_VERSION}
LABEL org.opencontainers.image.version="${CASFS_VERSION}" \
      org.opencontainers.image.revision="${CASFS_COMMIT}"
