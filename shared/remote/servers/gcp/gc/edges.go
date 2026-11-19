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

// Package gc holds the garbage collector of the GCP server: the GC edges of
// each object and the Bigtable table abfsgc that stores them.
package gc

import (
	"bytes"
	"errors"
	"fmt"
	"slices"

	"github.com/google/abfs/binding"
	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/objects"
	"github.com/google/abfs/shared/objects/gitobjects"
)

type Hash = entities.Hash

// ExtractorVersion is the version of the GC rules a server follows; the collector
// refuses to delete while an older server may have run. Bump it when Edges or a
// server rule the delete depends on changes, and raise requiredVersion for the
// types whose edges changed.
const ExtractorVersion = 3

func requiredVersion(typ string) int64 {
	if typ == string(gitobjects.ObjectTypeDeepTree) {
		return 2
	}
	return 1
}

var errUnsupportedType = errors.New("unsupported object type")

// Edges returns the sorted, unique GC edges of an object of type typ decoded as
// obj (nil for a blob), and the count of hash strings that do not parse. An
// unknown type is an error, so that a new type cannot lose its edges unnoticed.
func Edges(typ string, obj objects.Object) (edges []Hash, malformed int, err error) {
	if typ == string(entities.ObjectTypeBlob) {
		return nil, 0, nil
	}
	if obj == nil {
		return nil, 0, fmt.Errorf("object of type %q is not decoded", typ)
	}
	switch o := obj.(type) {
	case *binding.BuildInfo:
		edges, malformed = buildInfoEdges(o)
	case *binding.BuildStart:
		// PreviousBuild is left out: clients read it locally, and it chains all builds.
		edges, malformed = stringHashes(nil, o.MountDetails, o.RootBegin)
	case *binding.EvalResponse:
		edges = evalResponseEdges(o)
	case *gitobjects.DeepTreeObject:
		// A nested deep tree may be named by no map row, so only this edge keeps it.
		for _, ent := range o.Entries {
			if ent.Mode == entities.FileModeDeepTree {
				edges = append(edges, ent.Hash)
			}
		}
	case *binding.DigestMap:
		// The sub-maps are reachable only from here.
		edges = slices.Clone(o.DigestMaps)
	case *gitobjects.Tree, *gitobjects.Commit, *gitobjects.Tag, *gitobjects.SymRefObject,
		*objects.DeepTreeRequest, *binding.DigestMapRequest,
		*binding.EnvVars, *binding.EvalRequest, *binding.PatchObject,
		*binding.BuildRequest, *binding.BuildResult,
		*binding.MountDetails, *binding.UnmountDetails,
		*binding.LFSRequest, *binding.LFSResult,
		*binding.PackFiles, *binding.ProjectStatus,
		*binding.WSSnapshot, *binding.IndexSnapObject:
		edges = slices.Clone(obj.Links())
	default:
		return nil, 0, fmt.Errorf("%w %q (%T)", errUnsupportedType, typ, obj)
	}
	return normalize(edges), malformed, nil
}

func buildInfoEdges(o *binding.BuildInfo) ([]Hash, int) {
	edges, malformed := stringHashes(nil, o.MountDetails, o.RootBegin, o.RootEnd, o.NinjaLog, o.WrapLog, o.CmdMapLog, o.MismatchesLog)
	for _, run := range o.NinjaRunInfo {
		var n int
		for _, h := range run.BuildsFiles {
			edges, n = stringHashes(edges, h)
			malformed += n
		}
		if run.NinjaStats == nil {
			continue
		}
		for _, f := range run.Failures {
			edges, n = stringHashes(edges, f.Req, f.Resp)
			malformed += n
		}
	}
	return edges, malformed
}

// evalResponseEdges leaves out the ReadPatch inputs, which ninjago never fetches.
func evalResponseEdges(o *binding.EvalResponse) []Hash {
	edges := make([]Hash, 0, 1+len(o.WritePatch))
	edges = append(edges, o.EvalRequest)
	for _, ent := range o.WritePatch {
		edges = append(edges, ent.Hash)
	}
	return edges
}

func stringHashes(edges []Hash, hashes ...string) (_ []Hash, malformed int) {
	for _, s := range hashes {
		if s == "" {
			continue
		}
		if !entities.IsHash(s) {
			malformed++
			continue
		}
		edges = append(edges, entities.NewHash(s))
	}
	return edges, malformed
}

func normalize(edges []Hash) []Hash {
	edges = slices.DeleteFunc(edges, func(h Hash) bool { return h.IsZero() })
	slices.SortFunc(edges, func(a, b Hash) int { return bytes.Compare(a[:], b[:]) })
	return slices.Compact(edges)
}

// PackEdges returns the edges as consecutive 20-byte hashes.
func PackEdges(edges []Hash) []byte {
	b := make([]byte, 0, len(edges)*entities.HashSize)
	for _, h := range edges {
		b = append(b, h[:]...)
	}
	return b
}

func UnpackEdges(b []byte) ([]Hash, error) {
	if len(b)%entities.HashSize != 0 {
		return nil, fmt.Errorf("invalid packed edges of %d bytes", len(b))
	}
	edges := make([]Hash, len(b)/entities.HashSize)
	for i := range edges {
		copy(edges[i][:], b[i*entities.HashSize:])
	}
	return edges, nil
}
