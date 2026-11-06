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

package abfsrepo

import (
	"context"
	"errors"
	"fmt"
	"log"
	"maps"
	"os"
	"path"
	"runtime"
	"slices"
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/google/abfs/manifest"
	"github.com/google/abfs/manifest/resolve"

	"github.com/google/abfs/shared/cmd/clierr"
	"github.com/google/abfs/shared/entities"
)

type SyncOpts struct {
	Jobs                   int
	Rebase                 bool
	SmartSync              bool
	SmartTag               string
	ManifestServerUsername string
	ManifestServerPassword string
	FallbackToGit          bool
}

func (r *Repo) Sync(ctx context.Context, projectsToSync []string, opts SyncOpts) error {
	defer func() {
		if err := r.writeRepoFiles(); err != nil {
			fmt.Printf("Error writing repo files: %v\n", err)
		}
	}()

	// Always delete the override manifest at the start of sync
	// to ensure we load the original manifest first.
	overridePath := path.Join(r.manifestsDir, "smart_sync_override.xml")
	if err := os.Remove(overridePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove smart sync override: %w", err)
	}

	if err := r.fetchManifest(ctx, opts.FallbackToGit); err != nil {
		return err
	}

	if err := r.Load(ctx); err != nil {
		return err
	}

	// Apply smart manifest if requested
	if opts.SmartSync || opts.SmartTag != "" {
		if err := r.applySmartManifest(ctx, opts); err != nil {
			return fmt.Errorf("failed to apply smart manifest: %w", err)
		}
		// Re-load the manifest after applying the smart one (which will now load the override)
		if err := r.Load(ctx); err != nil {
			return fmt.Errorf("failed to load smart manifest: %w", err)
		}
	}

	return r.sync(ctx, projectsToSync, opts)
}

func (r *Repo) sync(ctx context.Context, projectsToSync []string, opts SyncOpts) error {
	if len(projectsToSync) == 0 {
		projectsToSync = slices.Collect(maps.Keys(r.projectsByPath))
	}

	// Projects are synced by path: a name listed at several paths is checked out at each of them.
	toSync := make(map[string]struct{}, len(projectsToSync))
	for _, t := range projectsToSync {
		if _, ok := r.projectsByPath[t]; ok {
			toSync[t] = struct{}{}
			continue
		}
		if projects, ok := r.projectsByName[t]; ok {
			for _, p := range projects {
				toSync[p.GetPath()] = struct{}{}
			}
			continue
		}

		if relPath, err := r.RelativeToRoot(t); err == nil {
			if _, ok := r.projectsByPath[relPath]; ok {
				toSync[relPath] = struct{}{}
				continue
			}
		}

		return fmt.Errorf("project %s not found", t)
	}

	if err := r.syncRemoves(); err != nil {
		return err
	}

	r.resolveRevisions(toSync)

	progress := NewProgress("Syncing:", len(toSync))
	defer progress.Stop()

	var lock sync.Mutex
	var failedErrors []error
	var failedProjects []string
	var synced []*manifest.Project
	limit := opts.Jobs
	if limit <= 0 {
		limit = runtime.NumCPU()
	}
	// Projects are checked out level by level, as repo does: a project whose path lies inside
	// another project's path is checked out after that project, into its work tree.
	for _, level := range PathLevels(slices.Collect(maps.Keys(toSync))) {
		// The paths of one name share the objects directory .repo/project-objects/<name>.git, so they
		// are synced one after another, as repo does.
		byName := make(map[string][]*manifest.Project)
		for _, p := range level {
			project := r.projectsByPath[p]
			byName[project.Name] = append(byName[project.Name], project)
		}

		grp, gctx := errgroup.WithContext(ctx)
		grp.SetLimit(limit)
		for _, projects := range byName {
			grp.Go(func() error {
				for _, project := range projects {
					progress.StartTask(project.GetPath())
					err := r.syncProject(gctx, project, opts)
					progress.EndTask(project.GetPath(), err == nil)
					lock.Lock()
					if err != nil {
						failedErrors = append(failedErrors, fmt.Errorf("project %s: %w", project.GetPath(), err))
						failedProjects = append(failedProjects, project.GetPath())
					} else {
						r.curProjects[project.GetPath()] = struct{}{}
						synced = append(synced, project)
					}
					lock.Unlock()
				}
				return nil
			})
		}

		if err := grp.Wait(); err != nil {
			return err
		}
	}

	errs, failed := r.finishCheckouts(synced)
	failedErrors = append(failedErrors, errs...)
	failedProjects = append(failedProjects, failed...)

	if len(failedProjects) > 0 {
		for _, e := range failedErrors {
			fmt.Fprintf(os.Stderr, "%s\n", e)
		}
		fmt.Fprintf(os.Stderr, "error: Exited sync due to syncing errors\n")
		fmt.Fprintf(os.Stderr, "Failing projects:\n")
		for _, p := range failedProjects {
			fmt.Fprintf(os.Stderr, "%s\n", p)
		}
		return clierr.CLIErr("failed to sync projects")
	}

	return nil
}

// finishCheckouts makes the link and copy files of the checked-out projects after every checkout,
// as a destination may lie inside the path of a project checked out later. Then it checks that
// every work tree is still there. It returns the errors and the failing projects.
func (r *Repo) finishCheckouts(synced []*manifest.Project) ([]error, []string) {
	var errs []error
	var failed []string
	slices.SortFunc(synced, func(a, b *manifest.Project) int { return strings.Compare(a.GetPath(), b.GetPath()) })
	for _, project := range synced {
		if err := r.syncProjectFiles(project); err != nil {
			errs = append(errs, fmt.Errorf("project %s: %w", project.GetPath(), err))
			failed = append(failed, project.GetPath())
		}
	}
	for _, project := range synced {
		if err := r.checkWorkTree(project); err != nil {
			delete(r.curProjects, project.GetPath())
			if !slices.Contains(failed, project.GetPath()) {
				failed = append(failed, project.GetPath())
			}
			errs = append(errs, err)
		}
	}
	return errs, failed
}

// resolveRevisions leaves out projects that do not resolve, so their clones can still fall back to git.
// Results are keyed by path: the paths of one name may sit at different revisions.
func (r *Repo) resolveRevisions(projectPaths map[string]struct{}) {
	r.resolved = nil
	var projects []*manifest.Project
	var reqs []resolve.Request
	for projectPath := range projectPaths {
		p := r.projectsByPath[projectPath]
		if p == nil {
			continue
		}
		remote, err := r.projectRemote(p)
		if err != nil {
			continue
		}
		revision, err := r.projectRevision(p)
		if err != nil || revision == "" {
			continue
		}
		fetchURL, err := resolve.FetchURL(remote.Fetch, r.manifestURL)
		if err != nil {
			continue
		}
		projects = append(projects, p)
		reqs = append(reqs, resolve.Request{Project: p.Name, FetchURL: fetchURL, Revision: revision})
	}

	results, err := resolve.Revisions(r.Store, reqs)
	var unresolved *resolve.UnresolvedError
	if err != nil && !errors.As(err, &unresolved) {
		log.Printf("Warning: cannot resolve project revisions: %v", err)
		return
	}
	r.resolved = make(map[string]resolve.Result, len(results))
	for i, res := range results {
		if !res.Hash.IsZero() {
			r.resolved[projects[i].GetPath()] = res
		}
	}
}

func (r *Repo) syncRemoves() error {
	if err := r.syncProjectRemoves(); err != nil {
		return err
	}
	if err := r.syncCopyFileRemoves(); err != nil {
		return err
	}
	if err := r.syncLinkFileRemoves(); err != nil {
		return err
	}
	return nil
}

func (r *Repo) syncProjectRemoves() error {
	//TODO remove .git and objects
	toRemove := make([]string, 0)
	for p := range r.curProjects {
		if _, ok := r.projectsByPath[p]; !ok {
			toRemove = append(toRemove, p)
		}
	}
	// Deepest first, as repo removes obsolete projects, so an inner project goes before the
	// project around it.
	slices.Sort(toRemove)
	slices.Reverse(toRemove)
	for _, p := range toRemove {
		if err := removeWorkTree(path.Join(r.repoRoot, p)); err != nil {
			return err
		}
		delete(r.curProjects, p)
	}
	return nil
}

// removeWorkTree removes a work tree except the directories that hold another project's .git,
// as repo deletes an obsolete checkout, so projects inside it and their changes stay.
func removeWorkTree(dir string) error {
	entries, err := os.ReadDir(dir)
	if entities.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	kept := false
	for _, e := range entries {
		p := path.Join(dir, e.Name())
		if !e.IsDir() {
			if err := os.Remove(p); err != nil && !entities.IsNotExist(err) {
				return err
			}
			continue
		}
		if _, err := os.Lstat(path.Join(p, ".git")); err == nil {
			kept = true
			continue
		}
		if err := removeWorkTree(p); err != nil {
			return err
		}
		if _, err := os.Lstat(p); err == nil {
			kept = true
		}
	}
	if kept {
		return nil
	}
	return os.Remove(dir)
}

// checkWorkTree fails when the project's work tree has no .git.
func (r *Repo) checkWorkTree(project *manifest.Project) error {
	if _, err := os.Lstat(path.Join(r.repoRoot, project.GetPath(), ".git")); err != nil {
		return fmt.Errorf("project %s: work tree %s is missing after sync: %w", project.Name, project.GetPath(), err)
	}
	return nil
}

func (r *Repo) syncCopyFileRemoves() error {
	oldCopyFiles := maps.Clone(r.curCopyFiles)

	for _, p := range r.projectsByPath {
		for _, c := range p.Copyfile {
			delete(oldCopyFiles, c.Dest)
		}
	}

	for c := range oldCopyFiles {
		if err := os.Remove(path.Join(r.repoRoot, c)); err != nil {
			if !entities.IsNotExist(err) {
				return err
			}
		}
		delete(r.curCopyFiles, c)
	}

	return nil
}

func (r *Repo) syncLinkFileRemoves() error {
	oldLinkFiles := maps.Clone(r.curLinkFiles)

	for _, p := range r.projectsByPath {
		for _, c := range p.Linkfile {
			delete(oldLinkFiles, c.Dest)
		}
	}

	for c := range oldLinkFiles {
		if err := os.Remove(path.Join(r.repoRoot, c)); err != nil {
			if !entities.IsNotExist(err) {
				return err
			}
		}
		delete(r.curLinkFiles, c)
	}

	return nil
}

func (r *Repo) syncProject(ctx context.Context, project *manifest.Project, opts SyncOpts) error {
	if err := r.cloneProject(ctx, project, opts); err != nil {
		return err
	}

	if err := r.createCommitHook(project); err != nil {
		return fmt.Errorf("failed to create symlink for commit-msg hook: %w", err)
	}

	if err := r.linkShareableDir(project); err != nil {
		return fmt.Errorf("error while link repo shareable dir: %w", err)
	}

	return nil
}

func (r *Repo) syncProjectFiles(project *manifest.Project) error {
	if err := r.createSymlinks(project); err != nil {
		return fmt.Errorf("error while creating the manifest symlinks: %w", err)
	}

	if err := r.copyFiles(project); err != nil {
		return fmt.Errorf("error while creating the manifest copyfiles: %w", err)
	}

	return nil
}
