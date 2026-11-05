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

package gc

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/google/abfs/binding"
	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/objects"
	"github.com/google/abfs/shared/objects/gitobjects"
)

func testHash(s string) Hash {
	return objects.HashBlobBytes([]byte(s))
}

func sorted(hashes ...Hash) []Hash {
	hashes = slices.Clone(hashes)
	slices.SortFunc(hashes, func(a, b Hash) int { return bytes.Compare(a[:], b[:]) })
	return hashes
}

// decoded returns the type and the object objects.Factory decodes from the
// encoding of obj.
func decoded(t *testing.T, obj objects.Object) (string, objects.Object) {
	t.Helper()
	require.NoError(t, obj.Encode())
	typ, out, err := objects.Factory.Decode(obj.GetHash(), obj.GetEncoded())
	require.NoError(t, err)
	return string(typ), out
}

// decodedRaw decodes a git object of type typ with the given body.
func decodedRaw(t *testing.T, typ string, body []byte) (string, objects.Object) {
	t.Helper()
	content := append(fmt.Appendf(nil, "%s %d\x00", typ, len(body)), body...)
	gotType, obj, err := objects.Factory.Decode(entities.Hash(sha1.Sum(content)), content)
	require.NoError(t, err)
	return string(gotType), obj
}

func requireEdges(t *testing.T, typ string, obj objects.Object, want []Hash, wantMalformed int) {
	t.Helper()
	edges, malformed, err := Edges(typ, obj)
	require.NoError(t, err)
	require.Equal(t, sorted(want...), edges)
	require.Equal(t, wantMalformed, malformed)
}

func TestEdges_Blob(t *testing.T) {
	requireEdges(t, string(entities.ObjectTypeBlob), nil, nil, 0)
}

func TestEdges_ObjectNotDecoded(t *testing.T) {
	_, _, err := Edges(string(gitobjects.ObjectTypeDir), nil)
	require.ErrorContains(t, err, "not decoded")
}

type unknownObject struct {
	objects.JSONBaseObject
}

func (o *unknownObject) Encode() error             { return nil }
func (o *unknownObject) Decode(Hash, []byte) error { return nil }
func (o *unknownObject) Links() []Hash             { return []Hash{testHash("a")} }

func TestEdges_UnknownTypeFails(t *testing.T) {
	_, _, err := Edges("unknown", &unknownObject{})
	require.ErrorIs(t, err, errUnsupportedType)
}

func TestEdges_EveryRegisteredTypeIsKnown(t *testing.T) {
	for typ, newObject := range objects.Factory {
		t.Run(string(typ), func(t *testing.T) {
			err := func() (err error) {
				// The Links method of some empty objects panics; it is called
				// only for types Edges knows.
				defer func() { _ = recover() }()
				_, _, err = Edges(string(typ), newObject())
				return err
			}()
			require.NotErrorIs(t, err, errUnsupportedType)
		})
	}
}

func TestEdges_TreeSkipsSubmodulesAndDuplicates(t *testing.T) {
	blob, dir, sub := testHash("blob"), testHash("dir"), testHash("submodule")
	content, err := gitobjects.EncodeTree([]entities.FileEntry{
		{Name: "a", Mode: entities.FileModeRegular, Hash: blob},
		{Name: "b", Mode: entities.FileModeRegular, Hash: blob},
		{Name: "d", Mode: entities.FileModeDir, Hash: dir},
		{Name: "s", Mode: entities.FileModeSubmodule, Hash: sub},
	})
	require.NoError(t, err)
	typ, obj, err := objects.Factory.Decode(entities.Hash(sha1.Sum(content)), content)
	require.NoError(t, err)
	requireEdges(t, string(typ), obj, []Hash{blob, dir}, 0)
}

func TestEdges_Commit(t *testing.T) {
	tree, parent := testHash("tree"), testHash("parent")
	body := fmt.Appendf(nil, "tree %s\nparent %s\nauthor A <a@example.com> 0 +0000\ncommitter A <a@example.com> 0 +0000\n\nmessage\n", tree, parent)
	typ, obj := decodedRaw(t, "commit", body)
	requireEdges(t, typ, obj, []Hash{tree, parent}, 0)
}

func TestEdges_BuildInfo(t *testing.T) {
	h := func(s string) string { return testHash(s).String() }
	info := &binding.BuildInfo{
		Version: binding.CurrentBuildInfoVersion,
		BuildStartData: binding.BuildStartData{
			MountDetails:  h("mount"),
			RootBegin:     h("begin"),
			PreviousBuild: h("previous"),
		},
		BuildStart:    h("start"),
		RootEnd:       h("end"),
		NinjaLog:      h("ninja"),
		WrapLog:       entities.ZeroHash.String(), // a log that did not exist
		CmdMapLog:     "",
		MismatchesLog: "not a hash",
		NinjaRunInfo: []binding.NinjaRunInfo{
			{
				NinjaStats: &binding.NinjaStats{Failures: []binding.NinjaFailure{
					{Req: h("req"), Resp: h("resp")},
					{Req: h("req only")},
				}},
				BuildsFiles: map[string]string{"out/build.ninja": h("build.ninja"), "out/bad": "1234"},
			},
			{BuildsFiles: map[string]string{"out/other.ninja": h("other.ninja")}},
		},
	}
	typ, obj := decoded(t, info)
	require.Nil(t, obj.(*binding.BuildInfo).NinjaRunInfo[1].NinjaStats)
	// Neither the BuildStart nor PreviousBuild are edges.
	requireEdges(t, typ, obj, []Hash{
		testHash("mount"), testHash("begin"), testHash("end"), testHash("ninja"),
		testHash("req"), testHash("resp"), testHash("req only"),
		testHash("build.ninja"), testHash("other.ninja"),
	}, 2)
}

func TestEdges_BuildStartWithoutPreviousBuild(t *testing.T) {
	start := &binding.BuildStart{
		Version: binding.CurrentBuildInfoVersion,
		BuildStartData: binding.BuildStartData{
			MountDetails:  testHash("mount").String(),
			RootBegin:     testHash("begin").String(),
			PreviousBuild: testHash("previous").String(),
		},
	}
	typ, obj := decoded(t, start)
	requireEdges(t, typ, obj, []Hash{testHash("mount"), testHash("begin")}, 0)
}

func TestEdges_EvalResponseWithoutReadPatch(t *testing.T) {
	resp := &binding.EvalResponse{
		Version:     binding.CurrentEvalResponseVersion,
		EvalRequest: testHash("request"),
		ReadPatch: entities.Patch{
			{Path: "in/file", Mode: entities.FileModeRegular, Hash: testHash("input")},
			{Path: "in/dir", Mode: entities.FileModeDir, Hash: testHash("dir list")},
		},
		WritePatch: entities.Patch{
			{Path: "out/file", Mode: entities.FileModeRegular, Hash: testHash("output")},
			{Path: "out/gone", Mode: entities.FileModeRegular},
		},
	}
	typ, obj := decoded(t, resp)
	requireEdges(t, typ, obj, []Hash{testHash("request"), testHash("output")}, 0)
}

func TestEdges_EvalRequestKeepsInputPatch(t *testing.T) {
	req := &binding.EvalRequest{
		Version:    binding.CurrentEvalRequestVersion,
		InputPatch: entities.Patch{{Path: "in/file", Mode: entities.FileModeRegular, Hash: testHash("input")}},
		EnvVars:    testHash("env"),
	}
	typ, obj := decoded(t, req)
	requireEdges(t, typ, obj, []Hash{testHash("input"), testHash("env")}, 0)
}

func TestEdges_DeepTreeHasNone(t *testing.T) {
	tree := &gitobjects.DeepTreeObject{DeepTree: entities.DeepTree{Entries: []*entities.DeepEntry{
		{Name: "a/b", Mode: entities.FileModeRegular, Hash: testHash("b"), Size: 1},
		{Name: "a", Mode: entities.FileModeDir, Hash: testHash("a")},
	}}}
	typ, obj := decoded(t, tree)
	require.NotEmpty(t, obj.Links())
	requireEdges(t, typ, obj, nil, 0)
}

func TestEdges_DigestMapKeepsOnlySubMaps(t *testing.T) {
	dm := &binding.DigestMap{
		Version:    binding.CurrentDigestMapVersion,
		Digests:    []binding.DigestEntry{{Hash: testHash("file")}},
		DigestMaps: []Hash{testHash("sub map 1"), testHash("sub map 2")},
	}
	typ, obj := decoded(t, dm)
	requireEdges(t, typ, obj, []Hash{testHash("sub map 1"), testHash("sub map 2")}, 0)
}

func TestEdges_MountDetailsUsesLinks(t *testing.T) {
	md := &binding.MountDetails{
		Version:            binding.CurrentMountDetailsVersion,
		ManifestHash:       testHash("manifest"),
		PinnedManifestHash: testHash("pinned"),
	}
	typ, obj := decoded(t, md)
	requireEdges(t, typ, obj, []Hash{testHash("manifest"), testHash("pinned")}, 0)
}

func TestPackEdges_RoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		edges []Hash
	}{
		{"none", nil},
		{"one", []Hash{testHash("a")}},
		{"several", []Hash{testHash("a"), testHash("b"), testHash("c")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			packed := PackEdges(tt.edges)
			require.Len(t, packed, 20*len(tt.edges))
			got, err := UnpackEdges(packed)
			require.NoError(t, err)
			require.Len(t, got, len(tt.edges))
			for i := range got {
				require.Equal(t, tt.edges[i], got[i])
			}
		})
	}
}

func TestUnpackEdges_RejectsPartialHash(t *testing.T) {
	_, err := UnpackEdges(make([]byte, 21))
	require.ErrorContains(t, err, "21 bytes")
}
