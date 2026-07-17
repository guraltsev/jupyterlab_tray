#!/usr/bin/env python3
"""Synchronize repository remotes from ``.gitconfig/.gitconfig``.

This script is intended to live inside a repository-local ``.gitconfig``
directory, for example::

    my-repo/
      .git/
      .gitconfig/
        .gitconfig
        gitconfig.py

Typical usage from inside ``.gitconfig/``::

    ./gitconfig.py

The script prints progress messages by default so users can see which step is
running. Use ``--quiet`` to suppress normal output, or ``--verbose`` to also
show the underlying ``git`` commands and captured Git output.

What automatic mode does:
1. Find the surrounding Git repository.
2. Create ``.git`` if it does not exist yet.
3. Create ``.gitconfig/.gitconfig`` if it is missing.
4. Copy any existing ``remote.*`` keys from ``.git/config`` into the snapshot.
5. Ensure remotes listed in the snapshot exist in Git.
6. Fetch the selected remote.
7. Point the local branch at ``<remote>/<branch>`` and set upstream tracking.
8. Run ``git reset --mixed`` to align the index with the remote branch.

The snapshot file is the source of truth for remote definitions. Existing Git
config values are imported into it when missing, but never silently removed.
"""

from __future__ import annotations

import argparse
import shlex
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

DEFAULT_BRANCH = "main"
DEFAULT_REMOTE = "github"
TOOL_DIRNAME = ".gitconfig"
SNAPSHOT_FILENAME = ".gitconfig"


class GitConfigError(RuntimeError):
    """Raised when the repository or Git operations are invalid."""


@dataclass(frozen=True)
class Paths:
    """Resolved filesystem locations used by the tool."""

    repo_root: Path
    git_dir: Path
    tool_dir: Path
    snapshot: Path
    git_config: Path

    @classmethod
    def discover(cls, start: Path) -> "Paths":
        """Find the repository root by walking upward from ``start``.

        Preferred detection order:
        1. the first directory containing ``.git``;
        2. if running from ``.gitconfig/`` before ``git init``, treat its parent
           as the repository root;
        3. if an ancestor contains ``.gitconfig/``, treat that ancestor as the
           repository root.
        """
        for candidate in (start, *start.parents):
            git_dir = candidate / ".git"
            if git_dir.exists():
                return cls(
                    repo_root=candidate,
                    git_dir=git_dir,
                    tool_dir=candidate / TOOL_DIRNAME,
                    snapshot=candidate / TOOL_DIRNAME / SNAPSHOT_FILENAME,
                    git_config=git_dir / "config",
                )

            if candidate.name == TOOL_DIRNAME:
                repo_root = candidate.parent
                return cls(
                    repo_root=repo_root,
                    git_dir=repo_root / ".git",
                    tool_dir=repo_root / TOOL_DIRNAME,
                    snapshot=repo_root / TOOL_DIRNAME / SNAPSHOT_FILENAME,
                    git_config=repo_root / ".git" / "config",
                )

            tool_dir = candidate / TOOL_DIRNAME
            if tool_dir.exists():
                return cls(
                    repo_root=candidate,
                    git_dir=candidate / ".git",
                    tool_dir=tool_dir,
                    snapshot=tool_dir / SNAPSHOT_FILENAME,
                    git_config=candidate / ".git" / "config",
                )

        raise GitConfigError(
            f"Could not infer the repository root from {start}. "
            "Run this from inside a repository, or pass --repo-root explicitly."
        )


@dataclass(frozen=True)
class RemoteSpec:
    """A remote entry declared in the snapshot file."""

    name: str
    url: str


@dataclass(frozen=True)
class SnapshotSyncResult:
    """Summary of a snapshot import from ``.git/config``."""

    created_snapshot: bool
    source_remote_entry_count: int
    imported_entry_count: int


@dataclass(frozen=True)
class RemoteRegistrationResult:
    """Summary of one remote registration attempt."""

    name: str
    url: str
    action: str


@dataclass(frozen=True)
class CliArgs:
    """Validated command-line options."""

    remote: str | None
    branch: str
    set_remote_url: bool
    repo_root: Path | None
    auto: bool
    init: bool
    sync_snapshot: bool
    ensure_remotes: bool
    fetch: bool
    track_branch: bool
    mixed_reset: bool
    verbose: bool
    quiet: bool


class Reporter:
    """Simple user-facing logger for normal, verbose, and error output."""

    PREFIX = "[gitconfig]"

    def __init__(self, *, verbose: bool = False, quiet: bool = False):
        self.verbose = verbose
        self.quiet = quiet

    def _write(self, message: str, *, stream: object) -> None:
        print(f"{self.PREFIX} {message}", file=stream)

    def info(self, message: str) -> None:
        if not self.quiet:
            self._write(message, stream=sys.stdout)

    def detail(self, message: str) -> None:
        if self.verbose and not self.quiet:
            self._write(message, stream=sys.stdout)

    def warn(self, message: str) -> None:
        if not self.quiet:
            self._write(f"WARNING: {message}", stream=sys.stderr)

    def error(self, message: str) -> None:
        self._write(f"ERROR: {message}", stream=sys.stderr)

    def success(self, message: str) -> None:
        if not self.quiet:
            self._write(message, stream=sys.stdout)


@dataclass
class GitRunner:
    """Small wrapper around ``git`` commands for one repository."""

    repo_root: Path
    reporter: Reporter

    def run(
        self,
        *args: str,
        check: bool = True,
        announce: str | None = None,
        show_output: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        cmd = ["git", *args]
        if announce:
            self.reporter.info(announce)
        self.reporter.detail(f"Running: {self._format_command(cmd)}")

        cp = subprocess.run(
            cmd,
            cwd=self.repo_root,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        output = (cp.stdout or "").strip()

        if cp.returncode != 0 and check:
            message = f"Command failed in {self.repo_root}: {self._format_command(cmd)}"
            if output:
                message = f"{message}\n{output}"
            raise GitConfigError(message)

        if show_output and output:
            for line in output.splitlines():
                self.reporter.detail(f"git: {line}")
        return cp

    @staticmethod
    def _format_command(cmd: list[str]) -> str:
        return " ".join(shlex.quote(part) for part in cmd)

    def must_have_git(self) -> None:
        if shutil.which("git") is None:
            raise GitConfigError("git is not available on PATH.")

    def init(self) -> None:
        self.run(
            "init",
            announce=f"Initializing Git repository in {self.repo_root}",
            show_output=True,
        )
        self.reporter.success(f"Initialized Git directory: {self.repo_root / '.git'}")

    def list_remotes(self) -> list[str]:
        cp = self.run("remote", check=False)
        if cp.returncode != 0:
            return []
        return [line.strip() for line in cp.stdout.splitlines() if line.strip()]

    def remote_url(self, remote: str) -> str | None:
        cp = self.run("remote", "get-url", remote, check=False)
        if cp.returncode != 0:
            return None
        value = cp.stdout.strip()
        return value or None

    def has_ref(self, refname: str) -> bool:
        cp = self.run("show-ref", "--verify", "--quiet", refname, check=False)
        return cp.returncode == 0


class SnapshotConfig:
    """Read and write Git config data stored in files."""

    def __init__(self, path: Path):
        self.path = path

    def ensure_exists(self) -> bool:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.exists():
            return False
        self.path.write_text("", encoding="utf-8")
        return True

    def entries(self, key_pattern: str) -> list[tuple[str, str]]:
        if not self.path.exists():
            return []

        cp = subprocess.run(
            ["git", "config", "--file", str(self.path), "--null", "--get-regexp", key_pattern],
            text=True,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
        if cp.returncode != 0:
            return []

        pairs: list[tuple[str, str]] = []
        for chunk in (part for part in cp.stdout.split("\x00") if part):
            if "\n" not in chunk:
                continue
            key, value = chunk.split("\n", 1)
            pairs.append((key.strip(), value.rstrip("\n")))
        return pairs

    def has_key(self, key: str) -> bool:
        if not self.path.exists():
            return False
        cp = subprocess.run(
            ["git", "config", "--file", str(self.path), "--get", key],
            text=True,
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return cp.returncode == 0

    def set_if_missing(self, key: str, value: str) -> bool:
        self.ensure_exists()
        if self.has_key(key):
            return False
        subprocess.run(
            ["git", "config", "--file", str(self.path), key, value],
            text=True,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return True

    def remote_specs(self) -> dict[str, RemoteSpec]:
        remotes: dict[str, RemoteSpec] = {}
        for key, value in self.entries(r"^remote\..*\.url$"):
            parts = key.split(".")
            if len(parts) < 3 or parts[0] != "remote" or parts[-1] != "url":
                continue
            name = ".".join(parts[1:-1])
            remotes[name] = RemoteSpec(name=name, url=value)
        return remotes


def display_url(url: str) -> str:
    """Remove embedded credentials from URLs before showing them to users."""
    parsed = urlsplit(url)
    if not parsed.scheme or not parsed.netloc:
        return url
    host = parsed.netloc.rsplit("@", 1)[-1]
    return urlunsplit((parsed.scheme, host, parsed.path, parsed.query, parsed.fragment))


def parse_args(argv: list[str]) -> CliArgs:
    parser = argparse.ArgumentParser(
        prog="gitconfig.py",
        description=(
            "Synchronize Git remotes from .gitconfig/.gitconfig. "
            "Prints progress messages by default."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  ./gitconfig.py\n"
            "  ./gitconfig.py --verbose\n"
            "  ./gitconfig.py --sync-snapshot\n"
            "  ./gitconfig.py --fetch --remote github\n"
            "  ./gitconfig.py --track-branch --remote github --branch main\n"
            "  ./gitconfig.py --quiet\n"
        ),
    )
    parser.add_argument("--remote", help="Remote name to use.")
    parser.add_argument("--branch", default=DEFAULT_BRANCH, help=f"Branch name. Default: {DEFAULT_BRANCH}.")
    parser.add_argument(
        "--set-remote-url",
        action="store_true",
        help="Overwrite an existing remote URL when it differs from the snapshot.",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        help="Explicit repository root. By default the tool searches upward from the current directory.",
    )

    action_group = parser.add_mutually_exclusive_group()
    action_group.add_argument("--auto", action="store_true", help="Run the full workflow. This is the default.")
    action_group.add_argument("--init", action="store_true", help="Only create .git when missing.")
    action_group.add_argument(
        "--sync-snapshot",
        action="store_true",
        help="Only import missing remote.* keys into .gitconfig/.gitconfig.",
    )
    action_group.add_argument(
        "--ensure-remotes",
        action="store_true",
        help="Only ensure remotes from the snapshot exist in Git.",
    )
    action_group.add_argument("--fetch", action="store_true", help="Only fetch the selected remote.")
    action_group.add_argument(
        "--track-branch",
        action="store_true",
        help="Only update branch tracking and HEAD for the selected branch.",
    )
    action_group.add_argument(
        "--mixed-reset",
        action="store_true",
        help="Only run git reset --mixed <remote>/<branch>.",
    )

    verbosity_group = parser.add_mutually_exclusive_group()
    verbosity_group.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress normal progress output; only errors are printed.",
    )
    verbosity_group.add_argument(
        "--verbose",
        action="store_true",
        help="Print extra detail, including git commands and Git output.",
    )

    ns = parser.parse_args(argv)
    single_action = any(
        [
            ns.init,
            ns.sync_snapshot,
            ns.ensure_remotes,
            ns.fetch,
            ns.track_branch,
            ns.mixed_reset,
        ]
    )
    return CliArgs(
        remote=ns.remote,
        branch=ns.branch,
        set_remote_url=ns.set_remote_url,
        repo_root=ns.repo_root,
        auto=ns.auto or not single_action,
        init=ns.init,
        sync_snapshot=ns.sync_snapshot,
        ensure_remotes=ns.ensure_remotes,
        fetch=ns.fetch,
        track_branch=ns.track_branch,
        mixed_reset=ns.mixed_reset,
        verbose=ns.verbose,
        quiet=ns.quiet,
    )


def choose_remote(
    remotes: dict[str, RemoteSpec],
    requested: str | None,
    reporter: Reporter,
) -> str | None:
    if requested:
        return requested
    if not remotes:
        return None
    if len(remotes) == 1:
        return next(iter(remotes))
    if DEFAULT_REMOTE in remotes:
        available = ", ".join(sorted(remotes))
        reporter.info(
            f"Multiple remotes found; using default '{DEFAULT_REMOTE}'. Available: {available}"
        )
        return DEFAULT_REMOTE
    available = ", ".join(sorted(remotes))
    raise GitConfigError(f"Multiple remotes found. Choose one with --remote. Available: {available}")


def ensure_git_dir(paths: Paths, git: GitRunner) -> bool:
    if paths.git_dir.exists():
        git.reporter.info(f"Using existing Git directory: {paths.git_dir}")
        return False
    git.init()
    return True


def sync_snapshot_from_git_config(paths: Paths) -> SnapshotSyncResult:
    snapshot = SnapshotConfig(paths.snapshot)
    created_snapshot = snapshot.ensure_exists()

    existing = SnapshotConfig(paths.git_config)
    source_entries = existing.entries(r"^remote\.")
    imported = 0
    for key, value in source_entries:
        if snapshot.set_if_missing(key, value):
            imported += 1

    return SnapshotSyncResult(
        created_snapshot=created_snapshot,
        source_remote_entry_count=len(source_entries),
        imported_entry_count=imported,
    )


def report_snapshot_sync(result: SnapshotSyncResult, paths: Paths, reporter: Reporter) -> None:
    if result.created_snapshot:
        reporter.info(f"Created snapshot file: {paths.snapshot}")
    else:
        reporter.info(f"Using snapshot file: {paths.snapshot}")

    if result.source_remote_entry_count == 0:
        reporter.info(f"No remote.* entries found in {paths.git_config} to import.")
        return

    if result.imported_entry_count == 0:
        reporter.info("Snapshot already contains all remote settings from .git/config.")
        return

    label = "entry" if result.imported_entry_count == 1 else "entries"
    reporter.info(f"Imported {result.imported_entry_count} remote config {label} into the snapshot.")


def ensure_remote_registered(
    git: GitRunner,
    spec: RemoteSpec,
    *,
    overwrite_url: bool,
) -> RemoteRegistrationResult:
    remotes = set(git.list_remotes())
    if spec.name not in remotes:
        git.run("remote", "add", spec.name, spec.url)
        return RemoteRegistrationResult(name=spec.name, url=spec.url, action="added")

    current_url = git.remote_url(spec.name)
    if current_url is None:
        git.run("remote", "set-url", spec.name, spec.url)
        return RemoteRegistrationResult(name=spec.name, url=spec.url, action="updated")

    if current_url == spec.url:
        return RemoteRegistrationResult(name=spec.name, url=spec.url, action="unchanged")

    if overwrite_url:
        git.run("remote", "set-url", spec.name, spec.url)
        return RemoteRegistrationResult(name=spec.name, url=spec.url, action="updated")

    raise GitConfigError(
        f"Remote '{spec.name}' already exists with a different URL.\n"
        f"  existing: {display_url(current_url)}\n"
        f"  desired:  {display_url(spec.url)}\n"
        "Use --set-remote-url to overwrite it."
    )


def ensure_remotes(
    git: GitRunner,
    remotes: list[RemoteSpec],
    *,
    overwrite_url: bool,
) -> list[RemoteRegistrationResult]:
    return [
        ensure_remote_registered(git, spec, overwrite_url=overwrite_url)
        for spec in remotes
    ]


def report_remote_updates(results: list[RemoteRegistrationResult], reporter: Reporter) -> None:
    if not results:
        reporter.info("No remotes are defined in the snapshot.")
        return

    for result in results:
        safe_url = display_url(result.url)
        if result.action == "added":
            reporter.info(f"Added remote '{result.name}' -> {safe_url}")
        elif result.action == "updated":
            reporter.info(f"Updated remote '{result.name}' -> {safe_url}")
        else:
            reporter.info(f"Remote '{result.name}' already matches the snapshot.")
            reporter.detail(f"Remote '{result.name}' URL: {safe_url}")


def fetch_remote(git: GitRunner, remote: str) -> None:
    git.run(
        "fetch",
        "--prune",
        remote,
        announce=f"Fetching remote '{remote}'",
        show_output=True,
    )
    git.reporter.success(f"Fetched remote '{remote}'")


def track_branch(git: GitRunner, remote: str, branch: str) -> None:
    remote_ref = f"refs/remotes/{remote}/{branch}"
    local_ref = f"refs/heads/{branch}"
    if not git.has_ref(remote_ref):
        raise GitConfigError(f"Missing remote ref {remote_ref}. Fetch the remote first.")

    git.reporter.info(f"Updating local branch '{branch}' to track '{remote}/{branch}'")
    git.run("update-ref", local_ref, remote_ref)
    git.run("symbolic-ref", "HEAD", local_ref)
    git.run("config", f"branch.{branch}.remote", remote)
    git.run("config", f"branch.{branch}.merge", f"refs/heads/{branch}")
    git.reporter.success(f"Branch '{branch}' now tracks '{remote}/{branch}'")


def mixed_reset(git: GitRunner, remote: str, branch: str) -> None:
    git.run(
        "reset",
        "--mixed",
        f"{remote}/{branch}",
        announce=f"Running mixed reset against '{remote}/{branch}'",
        show_output=True,
    )
    git.reporter.success(f"Mixed reset complete for '{remote}/{branch}'")


def load_snapshot_remotes(paths: Paths) -> dict[str, RemoteSpec]:
    return SnapshotConfig(paths.snapshot).remote_specs()


def report_no_snapshot_remotes(paths: Paths, reporter: Reporter) -> None:
    reporter.info(f"No remote.<name>.url entries found in {paths.snapshot}.")
    reporter.info(f"Add remotes to {paths.snapshot}, then rerun {Path(__file__).name}.")


def auto_flow(paths: Paths, git: GitRunner, args: CliArgs) -> int:
    ensure_git_dir(paths, git)
    sync_result = sync_snapshot_from_git_config(paths)
    report_snapshot_sync(sync_result, paths, git.reporter)

    remotes = load_snapshot_remotes(paths)
    if not remotes:
        if args.remote:
            raise GitConfigError(
                f"Remote '{args.remote}' was requested, but no remotes are defined in {paths.snapshot}."
            )
        report_no_snapshot_remotes(paths, git.reporter)
        return 0

    remote_results = ensure_remotes(
        git,
        list(remotes.values()),
        overwrite_url=args.set_remote_url,
    )
    report_remote_updates(remote_results, git.reporter)

    selected = choose_remote(remotes, args.remote, git.reporter)
    if selected is None:
        return 0
    if selected not in remotes:
        raise GitConfigError(f"Remote '{selected}' is not defined in {paths.snapshot}.")

    fetch_remote(git, selected)
    track_branch(git, selected, args.branch)
    mixed_reset(git, selected, args.branch)
    return 0


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    reporter = Reporter(verbose=args.verbose, quiet=args.quiet)

    try:
        start = (args.repo_root or Path.cwd()).resolve()
        paths = Paths.discover(start)
        reporter.info(f"Repository root: {paths.repo_root}")
        reporter.info(f"Tool directory: {paths.tool_dir}")
        reporter.info(f"Snapshot file: {paths.snapshot}")

        git = GitRunner(paths.repo_root, reporter)
        git.must_have_git()

        if args.auto:
            result = auto_flow(paths, git, args)
            reporter.success("Finished.")
            return result

        if args.init:
            ensure_git_dir(paths, git)
            reporter.success("Finished.")
            return 0

        if args.sync_snapshot:
            ensure_git_dir(paths, git)
            sync_result = sync_snapshot_from_git_config(paths)
            report_snapshot_sync(sync_result, paths, reporter)
            reporter.success("Finished.")
            return 0

        if not paths.git_dir.exists():
            raise GitConfigError("No .git directory exists yet. Run --init or --auto first.")

        sync_result = sync_snapshot_from_git_config(paths)
        report_snapshot_sync(sync_result, paths, reporter)
        remotes = load_snapshot_remotes(paths)

        if args.ensure_remotes:
            remote_results = ensure_remotes(
                git,
                list(remotes.values()),
                overwrite_url=args.set_remote_url,
            )
            report_remote_updates(remote_results, reporter)
            reporter.success("Finished.")
            return 0

        selected = choose_remote(remotes, args.remote, reporter)
        if selected is None:
            raise GitConfigError(f"No remote.<name>.url entries found in {paths.snapshot}.")
        if selected not in remotes:
            raise GitConfigError(f"Remote '{selected}' is not defined in {paths.snapshot}.")

        if args.fetch:
            fetch_remote(git, selected)
            reporter.success("Finished.")
            return 0
        if args.track_branch:
            track_branch(git, selected, args.branch)
            reporter.success("Finished.")
            return 0
        if args.mixed_reset:
            mixed_reset(git, selected, args.branch)
            reporter.success("Finished.")
            return 0

        raise GitConfigError("No action selected.")
    except GitConfigError as exc:
        reporter.error(str(exc))
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
