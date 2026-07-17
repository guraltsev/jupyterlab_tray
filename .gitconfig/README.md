# Repository-local Git remote bootstrapper

This directory is meant to live inside a Git repository as `.gitconfig/`.

## Files

- `.gitconfig` — a git-config formatted snapshot that stores the remote definitions you care about.
- `gitconfig.py` — synchronizes those remotes into `.git/config` and can align a local branch to a remote branch.

## Expected layout

```text
repo/
  .git/
  .gitconfig/
    .gitconfig
    gitconfig.py
```

## Basic usage

From inside `.gitconfig/`:

```bash
./gitconfig.py
```

That will:

1. find the surrounding repository root,
2. initialize `.git` if needed,
3. create `.gitconfig/.gitconfig` if missing,
4. import any missing `remote.*` entries from `.git/config`,
5. ensure remotes from the snapshot exist in Git,
6. fetch the selected remote,
7. point the local branch at the remote branch,
8. run `git reset --mixed <remote>/<branch>`.

## Output behavior

The script now prints progress messages by default so the user can see what it is doing.

- Use `--quiet` to suppress normal progress output and print only errors.
- Use `--verbose` to also show the underlying `git` commands and captured Git output.

Example:

```text
[gitconfig] Repository root: /work/repo
[gitconfig] Tool directory: /work/repo/.gitconfig
[gitconfig] Snapshot file: /work/repo/.gitconfig/.gitconfig
[gitconfig] Using existing Git directory: /work/repo/.git
[gitconfig] Using snapshot file: /work/repo/.gitconfig/.gitconfig
[gitconfig] Remote 'github' already matches the snapshot.
[gitconfig] Fetching remote 'github'
[gitconfig] Fetched remote 'github'
[gitconfig] Updating local branch 'main' to track 'github/main'
[gitconfig] Branch 'main' now tracks 'github/main'
[gitconfig] Running mixed reset against 'github/main'
[gitconfig] Mixed reset complete for 'github/main'
[gitconfig] Finished.
```

## Common commands

```bash
./gitconfig.py --sync-snapshot
./gitconfig.py --ensure-remotes
./gitconfig.py --fetch --remote github
./gitconfig.py --track-branch --remote github --branch main
./gitconfig.py --mixed-reset --remote github --branch main
./gitconfig.py --verbose
./gitconfig.py --quiet
```

## Notes

- If several remotes are defined, the script prefers `github` unless you pass `--remote`.
- If a remote already exists with a different URL, pass `--set-remote-url` to overwrite it.
- The snapshot is additive: the script imports missing remote settings from `.git/config`, but does not remove entries automatically.
- Remote URLs shown in normal messages have embedded credentials removed before printing.
