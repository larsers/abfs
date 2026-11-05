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

package repo

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/google/abfs/manifest"
	"github.com/google/abfs/manifest/resolve"

	"github.com/google/abfs/shared/bindcas"
	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/instance"
	"github.com/google/abfs/store"
)

const ABFSDirHashTag = "abfs.dir.hash.tag"

type projectRefURLMap map[string]entities.Hash

type remoteData struct {
	fetchSchemeURL string
	revision       string
}

type CASGit interface {
	GetCommit(ctx context.Context, key entities.Hash) (*object.Commit, error)
	ResolveCommit(ctx context.Context, key entities.Hash) (*object.Commit, error)
	GetSymRef(ctx context.Context, server, project, ref string) (string, error) //nolint:abfsstyle // matches casgit.CASGit.GetSymRef
}

type Repo struct {
	store               entities.CASStore
	git                 CASGit
	overrides           map[string]entities.Hash
	manifest            *manifest.Manifest
	pinned              *manifest.Manifest
	manifestProjectName string
	manifestServer      string
	manifestScheme      string
	localManifestDir    string

	remoteCache           map[string]remoteData
	defaultSchemeURL      string
	ignoreMissingProjects bool
	groups                []string

	manifestSet bool
	rootHash    entities.Hash

	// The manifest project commit that .repo/manifests is built from, resolved before the mount.
	manifestRef    string
	manifestCommit entities.Hash
}

func New(store entities.CASStore, git CASGit, manifestServer, manifestProjectName, manifestScheme, localManifestDir string, ignoreMissingProjects bool) *Repo {
	return &Repo{
		store:                 store,
		git:                   git,
		overrides:             make(projectRefURLMap),
		manifestProjectName:   manifestProjectName,
		manifestServer:        instance.MaybeRemoveScheme(manifestServer),
		manifestScheme:        manifestScheme,
		localManifestDir:      localManifestDir,
		remoteCache:           make(map[string]remoteData),
		ignoreMissingProjects: ignoreMissingProjects,
	}
}

// SchemeURLForRemote returns the SchemeURL for a named remote. Should only be called
// after manifest is set.
func (r *Repo) SchemeURLForRemote(remote string) (string, error) {
	if !r.manifestSet {
		return "", errors.New("the manifest has not been set")
	}

	data := r.remoteCache[remote]
	if data.fetchSchemeURL != "" {
		return data.fetchSchemeURL, nil
	}

	return r.defaultSchemeURL, nil
}

// AddURLRemoteCache is not concurrency safe.
func (r *Repo) AddURLRemoteCache(remote, schemeURL string) error {
	maybeExisting, found := r.remoteCache[remote]
	if found {
		if maybeExisting.fetchSchemeURL == schemeURL {
			// NOP.
			return nil
		}
		if maybeExisting.fetchSchemeURL != "" {
			return fmt.Errorf("remote %q already found. existing: %v attempted: %v", remote, maybeExisting.fetchSchemeURL, schemeURL)
		}
	}
	maybeExisting.fetchSchemeURL = schemeURL
	r.remoteCache[remote] = maybeExisting
	return nil
}

// SetGroups sets the mount groups; a project outside them does not have to resolve.
func (r *Repo) SetGroups(groups []string) {
	r.groups = groups
}

func (r *Repo) Manifest() *manifest.Manifest {
	return r.manifest
}

func (r *Repo) PinnedManifest() *manifest.Manifest {
	return r.pinned
}

func (r *Repo) WorkspaceManifest(root string) (*manifest.Manifest, error) {
	startTime := time.Now()
	pinned, err := manifest.CloneManifest(r.PinnedManifest())
	if err != nil {
		return nil, err
	}

	for _, p := range pinned.Project {
		wsPath := filepath.Join(root, p.GetPath())
		path := filepath.Join(wsPath, bindcas.GitDirName, "HEAD")
		b, err := os.ReadFile(path)
		if err != nil {
			if r.ignoreMissingProjects && entities.IsNotExist(err) {
				continue
			}
			return nil, fmt.Errorf("failed to read workspace HEAD %s: %w", path, err)
		}
		headVal := string(b)

		// repo start is using symbolic refs
		if strings.HasPrefix(headVal, "ref: ") {
			refsPath := strings.TrimPrefix(headVal, "ref: ")
			refsPath = strings.TrimSuffix(refsPath, "\n")
			b, err = os.ReadFile(filepath.Join(wsPath, bindcas.GitDirName, refsPath))
			if err != nil {
				return nil, fmt.Errorf("failed to read symbolic ref %s: %w", filepath.Join(wsPath, bindcas.GitDirName, refsPath), err)
			}
		}
		if len(b) < entities.HashHexSize {
			return nil, fmt.Errorf("invalid hash at path %s", path)
		}
		p.Revision = string(b[0:entities.HashHexSize])
		// Add annoation for current directory hash
		dirHash, err := bindcas.Hash(wsPath)
		if err != nil {
			return nil, fmt.Errorf("cannot get workspace hash for %s: %w", wsPath, err)
		}
		anno := &manifest.Annotation{
			Name:  ABFSDirHashTag,
			Value: dirHash.String(),
		}
		p.Annotation = append(p.Annotation, anno)
	}
	log.Printf("Workspace manifest setup in %f seconds\n", time.Since(startTime).Seconds())
	return pinned, nil
}

func (r *Repo) ManifestFromFile(fileName string) error {
	manifest, err := manifest.ParseFile(fileName)
	if err != nil {
		return err
	}
	return r.setManifest(context.TODO(), manifest)
}

func (r *Repo) ManifestFromBytes(content []byte) error {
	manifest, err := manifest.Parse(content)
	if err != nil {
		return err
	}
	return r.setManifest(context.TODO(), manifest)
}

func (r *Repo) ManifestFromBranch(ctx context.Context, branch, manifestProjectName, manifestBranchFile string) (returnErr error) {
	if err := r.ResolveManifestBranch(manifestProjectName, branch); err != nil {
		return err
	}

	h := r.manifestCommit
	log.Printf("Using the manifest from commit %s for %s", h, branch)

	manifestHash, err := r.manifestFileAt(ctx, h, manifestBranchFile)
	if err != nil {
		return err
	}
	return r.manifestFromBlobHash(ctx, manifestHash)
}

// PinManifestAt pins a manifest file of a manifest project commit as a mount does,
// returning the selected projects that do not resolve instead of failing.
func (r *Repo) PinManifestAt(ctx context.Context, commit entities.Hash, manifestFile string) (*manifest.Manifest, []resolve.Request, error) {
	manifestHash, err := r.manifestFileAt(ctx, commit, manifestFile)
	if err != nil {
		return nil, nil, err
	}
	man, err := r.readManifest(ctx, manifestHash)
	if err != nil {
		return nil, nil, err
	}
	return r.pin(ctx, man)
}

func (r *Repo) manifestFileAt(ctx context.Context, commitHash entities.Hash, manifestFile string) (entities.Hash, error) {
	commit, err := r.git.GetCommit(ctx, commitHash)
	if err != nil {
		return entities.Hash{}, fmt.Errorf("cannot load manifest commit %s: %w", commitHash, err)
	}
	r.rootHash = entities.Hash(commit.TreeHash)
	manifestHash, err := bindcas.ResolveToHash(r.store, r.rootHash, manifestFile)
	if err != nil {
		return entities.Hash{}, fmt.Errorf("cannot get file %q from %s: %s", manifestFile, commitHash, err)
	}
	return manifestHash, nil
}

// ResolveManifestBranch looks up refs/heads/<branch> of the manifest project and nothing else.
// A tag, a full ref, a commit or a ref below the name is not taken for the branch.
func (r *Repo) ResolveManifestBranch(manifestProjectName, branch string) error {
	ref := "refs/heads/" + branch
	// Refs are stored as paths, so a name like "rel/" or "../tags/v1" would reach another ref.
	if branch == "" || path.Clean(ref) != ref {
		return fmt.Errorf("invalid manifest branch %q: --branch takes a branch name", branch)
	}
	hashes, err := r.store.GetRefs(r.manifestServer, manifestProjectName, []string{ref, "refs/tags/" + branch})
	if entities.IsNotExist(err) {
		return r.noManifestRefsError(err)
	}
	if err != nil {
		return fmt.Errorf("failed to resolve manifest branch %q: %w", branch, err)
	}
	if len(hashes) == 2 && !hashes[0].IsZero() {
		r.manifestRef = ref
		r.manifestCommit = hashes[0]
		return nil
	}

	where := r.manifestServer + "/" + manifestProjectName
	switch {
	case len(hashes) == 2 && !hashes[1].IsZero():
		return fmt.Errorf("manifest branch %q not found in %s, only a tag of that name: --branch takes a branch name", branch, where)
	case strings.HasPrefix(branch, "refs/"):
		return fmt.Errorf("manifest branch %q not found in %s: --branch takes a branch name, not a full ref", branch, where)
	case plumbing.IsHash(branch):
		return fmt.Errorf("manifest branch %q not found in %s: --branch takes a branch name, not a commit", branch, where)
	default:
		return fmt.Errorf("manifest branch %q not found in %s: --branch takes a branch name", branch, where)
	}
}

// ResolveManifestHead takes the branch that HEAD of the manifest project points to, as git clone
// does. Without a usable HEAD it takes the last branch in ref order.
func (r *Repo) ResolveManifestHead(ctx context.Context, manifestProjectName string) error {
	// GetSymRef reads a server without refs for the host as "no HEAD", so check the host first.
	if _, err := r.store.GetRefs(r.manifestServer, manifestProjectName, []string{"HEAD"}); entities.IsNotExist(err) {
		return r.noManifestRefsError(err)
	}
	head, err := r.git.GetSymRef(ctx, r.manifestServer, manifestProjectName, "HEAD")
	if err != nil {
		return fmt.Errorf("failed to read HEAD of manifest project %s: %w", manifestProjectName, err)
	}
	if strings.HasPrefix(head, "refs/heads/") {
		hashes, err := r.store.GetRefs(r.manifestServer, manifestProjectName, []string{head})
		if err != nil && !entities.IsNotExist(err) {
			return fmt.Errorf("failed to resolve manifest %s: %w", head, err)
		}
		if len(hashes) == 1 && !hashes[0].IsZero() {
			r.manifestRef = head
			r.manifestCommit = hashes[0]
			return nil
		}
	}

	heads, err := r.store.SearchRefs(r.manifestServer, manifestProjectName, "refs/heads/", 0)
	if err != nil && !entities.IsNotExist(err) {
		return fmt.Errorf("failed to list branches of manifest project %s: %w", manifestProjectName, err)
	}
	if len(heads) == 0 {
		return fmt.Errorf("manifest project %s/%s has no branches", r.manifestServer, manifestProjectName)
	}
	log.Printf("Warning: manifest project %s has no usable HEAD, using %s", manifestProjectName, heads[0].Ref)
	r.manifestRef = heads[0].Ref
	r.manifestCommit = heads[0].Hash
	return nil
}

// noManifestRefsError reports a server that holds no refs for the manifest host, as before the
// first push or with a wrong manifest server, so it is not mistaken for a wrong branch name.
func (r *Repo) noManifestRefsError(err error) error {
	return fmt.Errorf("the ABFS server holds no refs for manifest server %s: %w", r.manifestServer, err)
}

// ManifestCommit returns the manifest project ref and commit resolved by ResolveManifestBranch,
// ResolveManifestHead or ManifestFromBranch, or a zero hash if none ran.
func (r *Repo) ManifestCommit() (string, entities.Hash) {
	return r.manifestRef, r.manifestCommit
}

func (r *Repo) manifestProjectFileBytes(ctx context.Context, name string) ([]byte, error) {
	manifestHash, err := bindcas.ResolveToHash(r.store, r.rootHash, name)
	if err != nil {
		return nil, fmt.Errorf("cannot get file %q from %s: %s", name, r.rootHash, err)
	}

	return store.ReadAllBlob(ctx, r.store, manifestHash)
}

func localManifestFileBytes(path string) ([]byte, error) {
	return os.ReadFile(path)
}

func (r *Repo) manifestFromBlobHash(ctx context.Context, manifestHash entities.Hash) error {
	manifest, err := r.readManifest(ctx, manifestHash)
	if err != nil {
		return err
	}

	return r.setManifest(ctx, manifest)
}

func (r *Repo) readManifest(ctx context.Context, manifestHash entities.Hash) (*manifest.Manifest, error) {
	manBytes, err := store.ReadAllBlob(ctx, r.store, manifestHash)
	if err != nil {
		return nil, fmt.Errorf("cannot read default.xml (blob=%s): %s", manifestHash, err)
	}

	return manifest.Parse(manBytes)
}

func (r *Repo) ManifestFromBlobHash(ctx context.Context, manifestHash string) error {
	if !plumbing.IsHash(manifestHash) {
		return fmt.Errorf("%q is not a valid hash", manifestHash)
	}
	return r.manifestFromBlobHash(ctx, entities.NewHash(manifestHash))
}

// updateRemoteCache updates remoteCache. It contains a mapping of remoteNames
// to manifest values, which may be derived or extended.
func (r *Repo) updateRemoteCache(man *manifest.Manifest) error {
	manServerSchemeURL := instance.MaybeAddScheme(r.manifestServer, r.manifestScheme)
	manProjectSchemeURL, err := url.JoinPath(manServerSchemeURL, r.manifestProjectName)
	if err != nil {
		return err
	}
	baseSchemeURL, err := url.Parse(manProjectSchemeURL)
	if err != nil {
		return err
	}

	remoteCache := make(map[string]remoteData)
	for _, remote := range man.Remote {
		resolvedSchemeURL, err := resolve.FetchURL(remote.Fetch, baseSchemeURL.String())
		if err != nil {
			return err
		}
		if curr, found := remoteCache[remote.Name]; found {
			if curr.fetchSchemeURL != resolvedSchemeURL {
				return fmt.Errorf("duplicate remote found with different fetch url. Original: %q, Duplicate: %q", curr.fetchSchemeURL, resolvedSchemeURL)
			}
		} else {
			remoteCache[remote.Name] = remoteData{fetchSchemeURL: resolvedSchemeURL, revision: remote.Revision}
		}
	}

	r.remoteCache = remoteCache
	return nil
}

func (r *Repo) setManifest(ctx context.Context, man *manifest.Manifest) error {
	pinned, missing, err := r.pin(ctx, man)
	if err != nil {
		return err
	}
	if len(missing) > 0 {
		return &resolve.UnresolvedError{Requests: missing}
	}

	r.manifest = man
	r.pinned = pinned
	r.manifestSet = true
	return nil
}

// pin returns a copy of the manifest with every resolved project pinned to its commit,
// and the selected projects that do not resolve.
func (r *Repo) pin(ctx context.Context, man *manifest.Manifest) (*manifest.Manifest, []resolve.Request, error) {
	pinned, err := manifest.CloneManifest(man)
	if err != nil {
		return nil, nil, err
	}

	manifestBytes := func(ctx context.Context, name string) ([]byte, error) {
		return r.manifestProjectFileBytes(ctx, name)
	}
	localManifestBytes := func(ctx context.Context, path string) ([]byte, error) {
		return localManifestFileBytes(path)
	}

	if len(pinned.Include) > 0 {
		if r.rootHash == entities.ZeroHash {
			return nil, nil, errors.New("cannot include a file from an unknown manifest project revision. Please mount with `-b` or provide a flat manifest file")
		}
		if err := pinned.FlattenIncludes(ctx, manifestBytes); err != nil {
			return nil, nil, err
		}
	}

	if r.localManifestDir != "" {
		entries, err := os.ReadDir(r.localManifestDir)
		if err != nil {
			return nil, nil, err
		}

		for _, ent := range entries {
			if strings.HasSuffix(ent.Name(), ".xml") {
				path := filepath.Join(r.localManifestDir, ent.Name())
				if err := pinned.IncludeManifestFile(ctx, path, localManifestBytes); err != nil {
					return nil, nil, err
				}
			}
		}
	}

	// Create fast lookup of remote properties.
	err = r.updateRemoteCache(pinned)
	if err != nil {
		return nil, nil, err
	}

	// Resolve the project values that can be inherited.
	for _, p := range pinned.Project {
		if h, ok := r.overrides[p.Name]; ok {
			p.Revision = h.String()
		}
		r.applyInheritedProjectValues(pinned.Default, p)
	}

	if pinned.Default != nil {
		r.defaultSchemeURL = r.remoteCache[pinned.Default.Remote].fetchSchemeURL
	}

	reqs := make([]resolve.Request, len(pinned.Project))
	for i, p := range pinned.Project {
		if p.Revision == "" {
			return nil, nil, fmt.Errorf("revision for %s is not specified", p.Name)
		}
		remSchemeURL := r.remoteCache[p.Remote].fetchSchemeURL
		if remSchemeURL == "" {
			remSchemeURL = r.defaultSchemeURL
		}
		reqs[i] = resolve.Request{Project: p.Name, FetchURL: remSchemeURL, Revision: p.Revision}
	}

	results, err := resolve.Revisions(r.store, reqs)
	var unresolved *resolve.UnresolvedError
	if err != nil && !errors.As(err, &unresolved) {
		return nil, nil, err
	}

	// A project outside the mount groups is pinned when it resolves and never fails the mount.
	var missing []resolve.Request
	for i, p := range pinned.Project {
		if !results[i].Hash.IsZero() {
			p.Revision = results[i].Hash.String()
		} else if resolve.MatchGroups(p.Groups, r.groups) {
			missing = append(missing, reqs[i])
		}
	}
	return pinned, missing, nil
}

func (r *Repo) applyInheritedProjectValues(def *manifest.Default, project *manifest.Project) {
	if project.Remote == "" {
		if def != nil {
			project.Remote = def.Remote
		}
	}

	if project.Revision == "" {
		// Revision can come from the remote with higher priority than default.
		if rev, ok := r.remoteCache[project.Remote]; ok {
			project.Revision = rev.revision
		}
		if project.Revision == "" {
			if def != nil {
				project.Revision = def.Revision
			}
		}
	}

	if def == nil {
		return
	}

	if project.DestBranch == "" {
		project.DestBranch = def.DestBranch
	}
	if project.Upstream == "" {
		project.Upstream = def.Upstream
	}
	if project.SyncC == "" {
		project.SyncC = def.SyncC
	}
	if project.SyncS == "" {
		project.SyncS = def.SyncS
	}
}

func (r *Repo) AddChanges(changeIDs []string) error {
	for _, id := range changeIDs {
		ref := fmt.Sprintf("refs/changes/%s/%s", id[len(id)-2:], id)
		refHash, err := r.store.SearchRefs(r.manifestServer, "", ref, 0)
		if err != nil {
			return fmt.Errorf("cannot find ref %s: %w", ref, err)
		}

		if len(refHash) == 0 {
			return fmt.Errorf("cannot find ref %s", ref)
		}

		slices.SortFunc(refHash, func(a, b entities.ProjectRefHash) int {
			ai, _ := strconv.Atoi(a.Ref)
			bi, _ := strconv.Atoi(b.Ref)
			return ai - bi
		})

		hash := refHash[0].Hash
		project := refHash[0].Project
		r.overrides[project] = hash
	}
	return nil
}
