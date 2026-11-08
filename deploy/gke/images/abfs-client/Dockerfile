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

# The client tool set for CI nodes, all static, so that it runs in any build
# image whatever its libc: abfs (musl, the one tool with a cgo dependency), the
# mounters and helpers, the seven -no-cgo build hooks that `abfs setup` copies
# into each Soong top, and procps-ng ps and psmisc fuser for the mount
# preflight and the unmount. The cacheman DaemonSet copies /opt/abfs/bin with
# `cp -a`, which keeps the file capabilities set here. The image also runs
# cacheman and the bootstrap Job.
#
# Build context: scripts/build-images.sh stages the ABFS tree in abfs/.

ARG GO_IMAGE=golang:1.27.3-trixie@sha256:af1bfc2614ebb1dbd31af514b78740fd678850e76498762b425ecee4c5c6ea80
ARG BASE_IMAGE=debian:13.7-slim@sha256:a72b4433de1cf7d6bae0c4cee74d0547f58601a643b3375f070c51600d63b6b2

FROM ${GO_IMAGE} AS procs
# ps must accept -C and -o uname:32, which BusyBox ps does not. Debian's
# patches change Makefile.am, hence autoreconf; libtool links a program fully
# static only with -all-static.
RUN sed -i 's/^Types: deb$/Types: deb deb-src/' /etc/apt/sources.list.d/debian.sources && \
    apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      musl-tools dpkg-dev autoconf automake libtool autopoint pkg-config gettext && \
    rm -rf /var/lib/apt/lists/*
WORKDIR /build
# The current trixie sources of procps and psmisc, not pinned, like the packages
# of the final stage. The source files go into the final image with the two
# GPL binaries built from them.
RUN apt-get update && apt-get source procps psmisc && rm -rf /var/lib/apt/lists/* && \
    mkdir -p /out-src && cp -a ./*.dsc ./*.tar.* /out-src/ && ls -l /out-src
RUN cd procps-* && autoreconf -fi && \
    ./configure CC=musl-gcc --disable-shared --enable-static --disable-nls \
      --without-ncurses --without-systemd --without-elogind \
      --disable-pidwait --disable-kill --disable-w --disable-modern-top && \
    make -j"$(nproc)" src/ps/pscommand LDFLAGS=-all-static && \
    install -D -m 0755 src/ps/pscommand /out/ps
# fuser links no terminal library; only pstree needs the one configure asks for.
RUN cd psmisc-* && autoreconf -fi && \
    ./configure CC=musl-gcc --disable-nls --disable-harden-flags ac_cv_lib_tinfo_tgetent=yes && \
    make -j"$(nproc)" src/fuser LDFLAGS=-static && \
    install -D -m 0755 src/fuser /out/fuser

FROM ${GO_IMAGE} AS build
ARG VERSION
RUN test -n "${VERSION}" || { echo "VERSION build argument is required" >&2; exit 1; }
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends musl-tools && \
    rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY abfs/ ./
RUN --mount=type=cache,target=/root/go/pkg/mod go mod download
# The maybe-copy-license step of the Makefile.
RUN cp LICENSE cmd/abfs/license/LICENSE
# abfs: the ldflags of the abfs-client target, linked static with musl as the
# cacheman-static target does.
RUN --mount=type=cache,target=/root/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CC=musl-gcc CGO_ENABLED=1 go build -tags netgo,osusergo \
      -ldflags="-X github.com/google/abfs/shared/version.version=${VERSION}-client \
        -X github.com/google/abfs/shared/version.expiration= \
        -X github.com/google/abfs/shared/instance.forceFlavor=abfs.client \
        -linkmode external -extldflags '-static' -s -w" \
      -o /out/abfs cmd/abfs/abfs.go
# Their Makefile targets use plain go install, which links net through cgo
# when gcc is present.
RUN --mount=type=cache,target=/root/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    vl="-X github.com/google/abfs/shared/version.version=${VERSION} \
      -X github.com/google/abfs/shared/remote/servers/gcp.version=${VERSION}" && \
    for tool in casfs-mounter casfuse-mounter abfs-resources; do \
      CGO_ENABLED=0 go build -ldflags="${vl}" -o "/out/${tool}" "cmd/${tool}/${tool}.go" || exit 1; \
    done
# The seven build hooks, by their Makefile targets.
RUN --mount=type=cache,target=/root/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    VERSION="${VERSION}" make VERSION="${VERSION}" DESTDIR=/out \
      abfs-wrap-no-cgo abfsbox-no-cgo ninjago-no-cgo abfs-cmd-map-no-cgo \
      abfs-bazel-daemon-no-cgo rbetap-no-cgo tap-no-cgo
COPY --from=procs /out/ps /out/fuser /out/
# Fail unless every file is static: no INTERP program header.
RUN for f in /out/*; do \
      readelf -l "$f" >/dev/null || { echo "$f: not an ELF file" >&2; exit 1; }; \
      if readelf -l "$f" | grep -q INTERP; then echo "$f: dynamically linked" >&2; exit 1; fi; \
    done && ls -l /out

FROM ${BASE_IMAGE}
ARG VERSION
# The Debian packages are not pinned: the archive keeps only the current
# versions, so a rebuild takes those. The image digest in the chart values pins
# what runs.
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      bash ca-certificates git libcap2-bin && \
    rm -rf /var/lib/apt/lists/*
COPY --from=build /out/ /opt/abfs/bin/
COPY --from=procs /out-src/ /usr/share/abfs/source/
# The capabilities of the Makefile targets.
RUN setcap CAP_SYS_ADMIN,CAP_SYS_MODULE+ep /opt/abfs/bin/casfs-mounter && \
    setcap CAP_SYS_RESOURCE+ep /opt/abfs/bin/casfuse-mounter && \
    setcap cap_dac_override,cap_syslog+ep /opt/abfs/bin/abfs-resources && \
    getcap /opt/abfs/bin/casfs-mounter /opt/abfs/bin/casfuse-mounter /opt/abfs/bin/abfs-resources
ENV PATH=/opt/abfs/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
LABEL org.opencontainers.image.version="${VERSION}"
