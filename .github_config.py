#!/usr/bin/env python3
"""
Smart git bootstrapper for the current directory.

Source of truth for remotes: .git_config (a git-config formatted file).

Auto behavior:
- If neither .git nor .git_config exists: initialize an empty repo and exit.
- Ensure .git exists.
- Ensure .git_config exists:
  - If missing and .git/config exists, create it and copy remote.* keys.
- Ensure configured remotes from .git_config exist in git (register if missing).
- If a remote exists in .git/config but not in .git_config, append it to .git_config.
- If a chosen remote exists and has remote.<name>.url in .git_config:
  - fetch --prune <remote>
  - create/point local 'main' to <remote>/main without checkout
  - set upstream tracking
  - mixed reset to <remote>/main (does not modify working tree)

Each behavior is available as a single-action switch.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

GIT_DIR = Path(".git")
GITCONFIG_SNAPSHOT = Path(".git_config")

DEFAULT_BRANCH = "main"
DEFAULT_GIT_REMOTE = "github"


class GitError(RuntimeError):
    pass


def run(
    cmd: List[str],
    *,
    check: bool = True,
    capture: bool = False,
) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            cmd,
            check=check,
            text=True,
            stdout=subprocess.PIPE if capture else subprocess.DEVNULL,
            stderr=subprocess.STDOUT if capture else subprocess.STDOUT,
        )
    except subprocess.CalledProcessError as e:
        out = e.stdout if isinstance(e.stdout, str) else ""
        raise GitError(f"Command failed: {' '.join(cmd)}\n{out}".rstrip()) from e


def require_git() -> None:
    cp = run(["git", "--version"], check=False, capture=True)
    if cp.returncode != 0:
        raise SystemExit("git is not available on PATH.")


def ensure_git_dir() -> bool:
    """
    Ensure .git exists. Returns True if it already existed, False if created.
    """
    if GIT_DIR.exists():
        return True
    run(["git", "init"])
    return False


def ensure_snapshot_exists() -> None:
    if not GITCONFIG_SNAPSHOT.exists():
        GITCONFIG_SNAPSHOT.write_text("", encoding="utf-8")


def config_kv_from_file(config_path: Path, key_regex: str) -> List[Tuple[str, str]]:
    """
    Return [(key, value), ...] for keys matching key_regex from a git-config file.
    """
    if not config_path.exists():
        return []

    cp = run(
        ["git", "config", "--file", str(config_path), "--null", "--get-regexp", key_regex],
        check=False,
        capture=True,
    )
    if cp.returncode != 0:
        return []

    parts = [p for p in cp.stdout.split("\x00") if p]
    out: List[Tuple[str, str]] = []
    for chunk in parts:
        if "\n" not in chunk:
            continue
        k, v = chunk.split("\n", 1)
        out.append((k.strip(), v.rstrip("\n")))
    return out


def config_has_key(config_path: Path, key: str) -> bool:
    if not config_path.exists():
        return False
    cp = run(["git", "config", "--file", str(config_path), "--get", key], check=False)
    return cp.returncode == 0


def config_set_if_missing(config_path: Path, key: str, value: str) -> bool:
    """
    Set key=value if key does not exist. Returns True if written.
    """
    if config_has_key(config_path, key):
        return False
    run(["git", "config", "--file", str(config_path), key, value])
    return True


def create_or_update_snapshot_from_gitconfig() -> int:
    """
    Ensure .git_config exists and contains any remote.* keys from .git/config that it lacks.
    Returns number of keys added.
    """
    ensure_snapshot_exists()
    kvs = config_kv_from_file(Path(".git/config"), r"^remote\.")
    added = 0
    for k, v in kvs:
        if config_set_if_missing(GITCONFIG_SNAPSHOT, k, v):
            added += 1
    return added


def snapshot_remote_urls() -> Dict[str, str]:
    """
    Read remote.<name>.url entries from .git_config.
    """
    if not GITCONFIG_SNAPSHOT.exists():
        return {}
    kvs = config_kv_from_file(GITCONFIG_SNAPSHOT, r"^remote\..*\.url$")
    remotes: Dict[str, str] = {}
    for k, v in kvs:
        # key format: remote.<name>.url
        parts = k.split(".")
        if len(parts) >= 3 and parts[0] == "remote" and parts[-1] == "url":
            name = ".".join(parts[1:-1])
            remotes[name] = v
    return remotes


def list_git_remotes() -> List[str]:
    cp = run(["git", "remote"], check=False, capture=True)
    if cp.returncode != 0:
        return []
    return [line.strip() for line in cp.stdout.splitlines() if line.strip()]


def git_remote_url(remote: str) -> str | None:
    cp = run(["git", "remote", "get-url", remote], check=False, capture=True)
    if cp.returncode != 0:
        return None
    val = cp.stdout.strip()
    return val or None


def ensure_remote_registered(remote: str, url: str, *, set_url_if_mismatch: bool) -> None:
    remotes = set(list_git_remotes())
    if remote not in remotes:
        run(["git", "remote", "add", remote, url])
        return

    existing = git_remote_url(remote)
    if existing is None:
        run(["git", "remote", "set-url", remote, url])
        return

    if existing != url:
        if set_url_if_mismatch:
            run(["git", "remote", "set-url", remote, url])
            return
        raise GitError(
            f"Remote '{remote}' exists with a different URL:\n"
            f"  existing: {existing}\n"
            f"  desired:  {url}\n"
            f"Use --set-remote-url to overwrite."
        )


def fetch(remote: str) -> None:
    run(["git", "fetch", "--prune", remote])


def ensure_branch_tracks_remote(remote: str, branch: str) -> None:
    remote_ref = f"refs/remotes/{remote}/{branch}"
    local_ref = f"refs/heads/{branch}"

    cp = run(["git", "show-ref", "--verify", "--quiet", remote_ref], check=False)
    if cp.returncode != 0:
        raise GitError(f"Missing remote ref {remote_ref}. Did you fetch?")

    run(["git", "update-ref", local_ref, remote_ref])
    run(["git", "symbolic-ref", "HEAD", local_ref])
    run(["git", "config", f"branch.{branch}.remote", remote])
    run(["git", "config", f"branch.{branch}.merge", f"refs/heads/{branch}"])


def mixed_reset_to(remote: str, branch: str) -> None:
    run(["git", "reset", "--mixed", f"{remote}/{branch}"])


@dataclass(frozen=True)
class Args:
    remote: str | None
    branch: str
    set_remote_url: bool
    auto: bool
    init: bool
    sync_git_config: bool
    ensure_remote: bool
    fetch: bool
    track_main: bool
    mixed_reset: bool


def parse_args(argv: List[str]) -> Args:
    p = argparse.ArgumentParser(prog="git_bootstrap.py")

    p.add_argument("--remote", default=None, help="Remote name to use.")
    p.add_argument("--branch", default=DEFAULT_BRANCH, help="Branch name (default: main).")
    p.add_argument("--set-remote-url", action="store_true", help="Overwrite remote URL if mismatched.")

    g = p.add_mutually_exclusive_group()
    g.add_argument("--auto", action="store_true", help="Run full automatic behavior (default).")
    g.add_argument("--init", action="store_true", help="Only ensure .git exists (git init if missing).")
    g.add_argument("--sync-git-config", action="store_true", help="Only sync remote.* into .git_config.")
    g.add_argument("--ensure-remote", action="store_true", help="Only register remote(s) from .git_config.")
    g.add_argument("--fetch", action="store_true", help="Only fetch from selected remote.")
    g.add_argument("--track-main", action="store_true", help="Only set HEAD/main tracking (no checkout).")
    g.add_argument("--mixed-reset", action="store_true", help="Only mixed-reset index to <remote>/<branch>.")

    ns = p.parse_args(argv)

    single_action = any([ns.init, ns.sync_git_config, ns.ensure_remote, ns.fetch, ns.track_main, ns.mixed_reset])
    run_auto = ns.auto or not single_action

    return Args(
        remote=ns.remote,
        branch=ns.branch,
        set_remote_url=ns.set_remote_url,
        auto=run_auto,
        init=ns.init,
        sync_git_config=ns.sync_git_config,
        ensure_remote=ns.ensure_remote,
        fetch=ns.fetch,
        track_main=ns.track_main,
        mixed_reset=ns.mixed_reset,
    )


def choose_remote(remotes: Dict[str, str], requested: str | None) -> str | None:
    if requested:
        return requested
    if len(remotes) == 1:
        return next(iter(remotes.keys()))
    if len(remotes) == 0:
        return None

    if DEFAULT_GIT_REMOTE in remotes:
        print(
            "Multiple remotes found in .git_config; assuming default remote "
            f"'{DEFAULT_GIT_REMOTE}'. To override, pass --remote <name>.\n"
            f"Available: {', '.join(sorted(remotes.keys()))}",
            file=sys.stderr,
        )
        return DEFAULT_GIT_REMOTE

    raise GitError(
        "Multiple remotes found in .git_config; specify one with --remote.\n"
        f"Available: {', '.join(sorted(remotes.keys()))}"
    )


def ensure_remotes_from_snapshot(remotes: Dict[str, str], *, set_url_if_mismatch: bool) -> None:
    for name, url in remotes.items():
        ensure_remote_registered(name, url, set_url_if_mismatch=set_url_if_mismatch)


def auto_flow(args: Args) -> int:
    # If neither .git nor .git_config exists: init empty repo and exit.
    if not GIT_DIR.exists() and not GITCONFIG_SNAPSHOT.exists():
        run(["git", "init"])
        return 0

    # Ensure .git exists.
    ensure_git_dir()

    # Ensure .git_config exists, and sync any missing remote.* from .git/config into it.
    create_or_update_snapshot_from_gitconfig()

    remotes = snapshot_remote_urls()
    ensure_remotes_from_snapshot(remotes, set_url_if_mismatch=args.set_remote_url)

    # If no remotes defined, stop here (can't fetch/track/reset).
    if not remotes:
        return 0

    remote = choose_remote(remotes, args.remote)
    if remote is None:
        return 0
    if remote not in remotes:
        raise GitError(f"Remote '{remote}' not found in .git_config.")

    fetch(remote)
    ensure_branch_tracks_remote(remote, args.branch)
    mixed_reset_to(remote, args.branch)
    return 0


def main(argv: List[str]) -> int:
    require_git()
    args = parse_args(argv)

    try:
        if args.auto:
            return auto_flow(args)

        # Single-action modes:
        if args.init:
            ensure_git_dir()
            return 0

        if args.sync_git_config:
            if not GIT_DIR.exists() and not Path(".git/config").exists():
                ensure_snapshot_exists()
                return 0
            ensure_git_dir()
            create_or_update_snapshot_from_gitconfig()
            return 0

        # Remaining actions require .git.
        if not GIT_DIR.exists():
            raise GitError("No .git directory; run --init or --auto first.")

        create_or_update_snapshot_from_gitconfig()
        remotes = snapshot_remote_urls()

        if args.ensure_remote:
            ensure_remotes_from_snapshot(remotes, set_url_if_mismatch=args.set_remote_url)
            return 0

        remote = choose_remote(remotes, args.remote)
        if remote is None:
            raise GitError("No remote.<name>.url entries found in .git_config.")
        if remote not in remotes:
            raise GitError(f"Remote '{remote}' not found in .git_config.")

        if args.fetch:
            fetch(remote)
            return 0

        if args.track_main:
            ensure_branch_tracks_remote(remote, args.branch)
            return 0

        if args.mixed_reset:
            mixed_reset_to(remote, args.branch)
            return 0

        return 2

    except GitError as e:
        print(str(e), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))