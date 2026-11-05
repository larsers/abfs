# Manifests and tree layouts

This page is for the people who set up ABFS for CI/CD and for developers whose source tree
does not look like AOSP: a supplier delivery layered over AOSP and SoC vendor lists, with
`remove-project`, `extend-project` and include groups, an Android tree in a subdirectory next to
other parts of the product, or both. It says how ABFS reads such a manifest, what it needs from
such a layout, and how to check that a mount is ready to build.

ABFS applies a manifest by the rules of the
[`repo` tool](https://gerrit.googlesource.com/git-repo/+/HEAD/docs/manifest-format.md), on
both mount paths: the classic mount (`abfs mount -b`) and workspace mounts (`--ws`,
`--tmp-ws`, or any mount when the client config sets `force-workspaces: true`). A standard
AOSP manifest and layout give the same tree as before.

## Manifests

### How a mount reads the manifest

1. It reads the entry file of the manifest branch that `--branch-file` names (default
   `default.xml`), on both mount paths.
2. It expands every `<include>`, nested includes too.
3. It adds local manifests: `--local-manifest-dir` on the classic mount,
   `.repo/local_manifests/*.xml` in name order on workspace mounts.
4. It applies `project`, `remove-project` and `extend-project` in document order and checks
   out the projects that remain.

| Element | What ABFS does |
|---|---|
| `<include name="f.xml" groups="g">` | Adds `g` to every `project` and `extend-project` in `f.xml` and in the files it includes. |
| `<include name="f.xml" revision="r">` | Sets `r` on every `project` and nested `include` in `f.xml` that has no `revision`. It takes precedence over the remote's `revision`. |
| `<remove-project name="n">` | Removes every project named `n`. |
| `<remove-project path="p">` | Removes the project at `p`, whatever its name. |
| `<remove-project name="n" path="p">` | Removes only the project named `n` at `p`. Other paths of `n` stay. |
| `remove-project optional="true"` | A remove that matches nothing is skipped. Without it, the mount fails. |
| `remove-project base-rev="r"` | The mount fails unless the removed project's revision is `r`. |
| `<extend-project name="n" path="p">` | Changes every project named `n`, or only the one at `p`. `revision`, `remote`, `dest-branch` and `upstream` replace the project's values, `groups` are added, `dest-path` moves the project, and `copyfile`, `linkfile` and `annotation` children are added. |
| `extend-project revision="r" base-rev="b"` | The mount fails unless the project's revision before the change is `b`. `base-rev` without `revision` checks nothing. |
| `extend-project remote="x"` | Changes the remote. The project keeps the revision it had, even when remote `x` has a `revision`. |

A project's revision is its own `revision`, else its remote's `revision`, else the
`<default>` revision.

### Rules a manifest must follow

The mount fails, as `repo init` and `repo sync` do, when:

- two projects sit at one path (`duplicate path`);
- an `extend-project` names a project that was never listed, or that a `remove-project`
  with only `name` removed (`extend-project element specifies non-existent project`);
- a `remove-project` without `optional="true"` matches nothing, or has neither `name` nor
  `path`;
- a `base-rev` does not match; the error lists every mismatch;
- an `extend-project` names a remote that the manifest does not define;
- an `extend-project` has `dest-path` but no `path` and the name sits at several paths.

ABFS also fails a `dest-path` that moves a project onto a path another project uses. The
`repo` tool accepts that manifest and then stops syncing the project that was at that path,
without a message.

### What is not applied

The mount prints a line starting with `Warning: manifest` for each of these and goes on
without them:

- a `<project>` nested in another `<project>`;
- a `<submanifest>`;
- an attribute ABFS does not read on `remote`, `default`, `project`, `extend-project`,
  `remove-project` or `include` (for example `rebase` or `sync-strategy`).

A group filter such as `-g local:my-file` prints a warning too: projects from local manifests
do not get the `local:` groups that `repo` gives them.

`<repo-hooks>` is kept in the manifest but no hook runs, so a `post-sync` hook that the
build needs has to run by hand after each sync.

### Seed the projects with git-pusher

List every entry file that your builds mount in the pusher pool config, each with its branch
(see [`gitpusher/README.md`](../../gitpusher/README.md)). The pusher follows includes:

```yaml
repos:
  - manifest:
      url: https://gerrit.example.com/platform/manifest
      files:
        - branch: release-1
          file: default.xml
        - branch: release-1
          file: supplier-only.xml
```

The pusher mirrors every `<project>` element of every file, including projects that a
`remove-project` drops later; such a project costs push time, and a push error if the
project does not exist on your Gerrit. It does not follow an `extend-project` that changes a
project's `remote`: add that project under its new host to the `projects:` list of the pool
config.

### Mount

Classic mount of a named entry file, with a group filter:

```sh
abfs mount -b release-1 --branch-file supplier-only.xml -g shared $MOUNT
```

`-g` selects projects whose groups come from the project, from an `<include groups=>` or
from an `extend-project`, as `repo init -g` does.

A workspace mount takes the same options; `abfs mount` passes `--branch-file` to `repo init`
as its `-m`:

```sh
abfs mount --ws ci -b release-1 --branch-file supplier-only.xml -g shared $MOUNT
```

#### Local manifests

- Classic mount: `abfs mount -b release-1 --local-manifest-dir $HOME/local-manifests $MOUNT`
  merges every `*.xml` file of that directory after the includes.
- Workspace mounts: put the files in `$MOUNT/.repo/local_manifests/`. Every `repo sync`
  (`abfs repo sync`) applies them in name order after the includes. An `<include>` inside a
  local manifest is relative to `.repo/`.

#### Existing workspaces

The first `repo sync` (`abfs repo sync`) of a workspace made by an earlier ABFS version deletes
the checkouts that the manifest removes, or moves with `dest-path`; earlier versions checked
them out anyway. It deletes them without checking for local changes, while the `repo` tool
refuses to delete a checkout with uncommitted work. Commit and push, or copy, local changes in
those projects before that sync.

### Check a branch before CI mounts it

Compare the projects a workspace mount checked out with the list of the `repo` tool, run
with the same entry file and groups:

```sh
mkdir /tmp/repo-check && cd /tmp/repo-check
repo init -u https://gerrit.example.com/platform/manifest -b release-1 -g shared
repo list -a -p | sort > /tmp/repo-paths.txt
sort $MOUNT/.repo/projects.list | diff /tmp/repo-paths.txt -
```

No output means the same project list. On a classic mount, check that every path in
`/tmp/repo-paths.txt` exists in the mount:

```sh
cd $MOUNT && while read -r p; do test -d "$p" || echo "missing: $p"; done < /tmp/repo-paths.txt
```

## Tree layouts

In AOSP the Android tree is the whole repo workspace: `build/soong`, `build/make` and
`prebuilts/build-tools` sit at the workspace root. Many product trees put Android in a
subdirectory instead, next to other parts of the product, for example:

```
.repo/
android/            # the Android tree: build/soong, build/make, frameworks/, prebuilts/, ...
other/              # another part of the product, built with its own tools
tools/              # scripts and host tools shared by the product
```

ABFS mounts, syncs and caches any such layout. The Android build cache needs ABFS's build
hooks in the Android tree itself, which `abfs setup` installs.

### Soong tops

A Soong top is a directory that Soong accepts as the root of an Android tree: it holds
`build/soong/root.bp` and `prebuilts/build-tools/linux-x86/bin/`. `build/envsetup.sh` and
`lunch` must run there, and that is where Soong looks for the ABFS hooks.

`abfs setup` finds the tops from the projects of the mount. It tests the mount root and every
directory that holds a `build/soong`, `build/make` or `build` project, and keeps those that pass
the test above.
A tree can have any number of tops at any depth (`android/`, `src/aosp/`, two Android
releases side by side, an SDK tree inside another tree). When no top is found, setup sets up
the mount root as before; if some project suggested a top, setup warns and names what each
candidate lacks (`build/soong/root.bp` or the prebuilt tools directory).

To name the tops instead, set the client config key `setup.tops` to a list of paths relative to
the mount root, for example in the central client configuration (see the
[sysadmin guide](README.md#default-client-configuration)):

```yaml
    setup.tops:
      - android
```

An entry that is not a directory on a mount is skipped with a warning, so one client config can
serve trees of different layouts; when no entry exists on a mount, setup finds the tops itself.

Or name them for one run:

```sh
abfs setup --top android $MOUNT
```

`--chroot` keeps its older meaning: a scratch checkout for `abfs nsjail`, left out of the
workspace snapshot. Use `--top` for an Android tree in a workspace.

The positional argument of `abfs setup` is the mount; any directory inside the mount means the
mount, so `abfs setup android` sets up the tops of the whole mount, not `android/`.

To see the tops and what is in place in each:

```sh
abfs setup --list $MOUNT
```

It prints each top with its `.ninjago.conf` (its `Chroot` is the top's path in the mount), the
generated files and binaries, the nsjail wrapper and, below the root, whether the top's
daemon socket answers. `--list` changes nothing.

### What setup installs in each top

- the ABFS binaries in `prebuilts/build-tools/linux-x86/bin/`: `abfsbox`, `ninjago`,
  `abfs-wrap`, `ckati-wrap`, `tap`, `rbetap`, `abfs-cmd-map`, `abfs-bazel-daemon`;
- `.ninjago.conf` with this client's cache directory and settings, and `Chroot` set to the
  top's path in the mount (empty at the root);
- `vendor/abfs/vendorsetup.sh`, which `source build/envsetup.sh` reads to select ninjago and
  to pin the build metadata (`BUILD_DATETIME`, `BUILD_HOSTNAME`, `BUILD_USERNAME`,
  `BUILD_NUMBER`) that would otherwise make some steps miss the cache on every build;
- below the root, `.repo/mount-details`, a relative link to the mount's `.abfs.mount-details`,
  and a daemon socket `.abfs.sock` for the build tools that run in the top;
- the nsjail wrapper where the top's Soong passes `{ABFS_DIR}` to nsjail (see the
  [Android developer guide](../android-dev/README.md#build)).

Setup copies the binaries from the `-no-cgo` files that `make install-client` installs (for
example `ninjago-no-cgo`). They must be on the `PATH` of the process that runs `abfs mount` or
`abfs remount`, which also runs setup; a CI runner or a systemd unit often has a shorter `PATH`
than a login shell. If one is missing, setup fails and names the file.

### When setup runs

With `sync.auto-setup: true` in the client config, as the sysadmin guide sets it, setup runs:

- after a classic mount (`abfs mount -b <branch>` without a workspace) has created the projects;
- after every `abfs repo sync`, including the one a workspace mount runs;
- after `abfs remount` restores a workspace.

`sync.auto-setup` controls all of them; the separate `auto-setup` key has no effect. If setup
fails, the command fails with setup's error: a classic mount and `abfs repo sync` report it, and
`abfs remount` unmounts the workspace again. Fix the cause, often the `-no-cgo` files missing on
`PATH`, and run the command again.

With `sync.auto-setup` off, run `abfs setup $MOUNT` yourself after every mount and remount. A
workspace snapshot does not keep `.ninjago.conf`, so after a remount ninjago has no
configuration until setup runs.

Setup rewrites its files on every run. If a build is running in a top (`out/.build_start`
without `out/.build_info`), setup leaves that top's binaries alone and says so; run it again
after the build.

### What a workspace keeps

A workspace snapshot keeps every file under a top, including files you create outside any
project, except:

- `out/` and `dist/`, as in every workspace;
- `.ninjago.conf` and `vendor/abfs/vendorsetup.sh`, which hold this client's settings and are
  written again by setup on the next mount or remount. If the tree has no `vendor/` project,
  setup creates `vendor/` itself and the snapshot leaves out all of it, so keep your own files
  under `vendor/` inside a project.

## Building

Run the Android build from the top:

```sh
cd $MOUNT/android
source build/envsetup.sh
lunch <target>
m
```

`build/envsetup.sh` finds the top from the current directory and reads `vendorsetup.sh` there.
That file selects ninjago, which matters on releases where Siso is the default.

### Parts of the product outside the top

- A build step that reads a sibling of the top (`../tools/` or the absolute path of
  `$MOUNT/tools/`) works on a local build: Soong's sandbox shows the rest of the file system
  read-only. Its cache entry depends on how the step names the file, so give every machine the
  same mount path (see below).
- Remote compile runs only steps whose inputs are all inside the top; other steps build locally.
- `OUT_DIR` must be relative and inside the top, or unset. On Android 15 and 16, soong_ui
  rewrites an absolute `OUT_DIR` to a sandbox path that the ABFS hooks cannot use outside the
  sandbox; an `OUT_DIR` outside the top does not work with Soong's sandbox on any release.
- Builds of the other parts (`other/` in the example) read their sources from the mount lazily,
  like any program, but they do not use the ABFS build cache.

### Cache hits across machines

- Mount at the same path on every machine and for every user (for example `/abfs/ws` on every
  CI runner). Soong on Android 15 and 16 writes absolute links to some host tools into its
  outputs, so a different mount path changes those outputs and the steps that read them.
- Keep the build metadata pins of `vendorsetup.sh`: build with the environment that
  `source build/envsetup.sh` sets in the top, or set the same four `BUILD_*` values yourself.

## CI recipe

1. Push the product's manifest project and its projects with git-pusher, as in the
   [sysadmin guide](README.md#create-required-hashpaths), with every entry file your builds
   use under `files:` (see [Seed the projects with git-pusher](#seed-the-projects-with-git-pusher)).
   A layout needs no pusher setting of its own.
2. Set the client config centrally, with `sync.auto-setup: true`, the manifest server and
   project of the product, and `setup.tops` only if detection does not find your tops:

    ```sh
    abfs edit --add --stdin config:clients/configs << 'EOF'
    - select:
      config:
        manifest-scheme: https
        manifest-server: gerrit.example.com
        manifest-project-name: product/manifest
        sync.auto-setup: true
        force-workspaces: true
    EOF
    ```

3. Mount at a fixed path, check the tops, and build:

    ```sh
    MOUNT=/abfs/ci
    mkdir -p $MOUNT
    abfs mount --tmp-ws --branch main $MOUNT
    abfs setup --list $MOUNT
    cd $MOUNT/android
    source build/envsetup.sh
    lunch <target>
    m
    ```

   `--list` must show each top with a `.ninjago.conf` and no missing binaries.

## Known limits

- `repo-hooks` do not run (see above).
- `repo sync` inside a workspace mount with an option that `abfs repo sync` does not accept
  (for example `-q`, `-v` or `--verify`) falls back to the `repo` tool's own network sync.
  Run `abfs repo sync`, or `repo sync` without those options.
- A `<project revision="">` in a file included with `revision="r"` gets `r`. The `repo` tool
  treats the empty attribute as set and uses the remote's or the default revision. Leave the
  attribute out, or give the revision.
