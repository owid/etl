"""Assemble / update the archive DAG from git history of the active DAG.

The archive DAG (``dag/archive/*.yml``) is treated as a *derived* record rather
than something maintained by hand: every step that was once an **active** step
(a key under ``steps:`` in ``dag/main.yml`` or one of its includes) but no longer
is, gets recorded here together with the last commit at which it was active.

That commit hash is the recovery point: ``git checkout <sha>`` brings back the
step's code, its dependencies and the exact library versions it ran with, so
there is no need to keep the (non-functional) archived step code around in the
working tree.

This command replaces the imperative ``etl archive`` workflow: instead of moving
a step from the active to the archive DAG by hand, you simply delete it from the
active DAG (and delete its files), and this command reconstructs the archive
record from git.

Algorithm
---------
1. Walk every commit that touched an active DAG file (in chronological order)
   and, for each, reconstruct the merged active DAG. This yields, for every step
   that was ever active, the *last* commit at which it appeared — its recovery
   point — along with its dependencies and the DAG file it lived in.
2. Compare against the current state:
   * Steps still active  → ignored.
   * Steps already in the archive DAG → a ``# archived; ...`` marker comment with
     the recovery hash is back-filled above them if missing.
   * Steps active in history but neither active nor archived now → appended to the
     mirror archive file (``dag/<x>.yml`` → ``dag/archive/<x>.yml``).

The command only ever edits ``dag/archive/*.yml``. It never deletes step code or
snapshots — that is done separately (see ``ai/archive-removal-plan.md``).
"""

from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import click
import yaml
from structlog import get_logger

from etl import paths
from etl.dag_helpers import (
    create_dag_archive_file,
    load_dag,
    load_single_dag_file,
    write_to_dag_file,
)

log = get_logger()

# DAG files that are not part of the "active" DAG and must be skipped when
# reconstructing the active DAG at a historical commit.
SKIP_INCLUDES = ("dag/archive/", "dag/temp.yml")

# Marker comment written above each archived step. ``<sha>`` is the last commit at
# which the step was active (the recovery point); ``<date>`` is that commit's date.
MARKER_PREFIX = "# archived; last active in"
MARKER_RE = re.compile(r"#\s*archived;\s*last active in\s+([0-9a-f]{7,40})\b")


@dataclass
class LastSeen:
    """Where and when a step was last seen as an active step."""

    sha: str
    timestamp: int
    deps: set[str]
    dag_file: str  # active DAG file (relative to repo root) that declared the step

    @property
    def date(self) -> str:
        return datetime.fromtimestamp(self.timestamp, tz=timezone.utc).strftime("%Y-%m-%d")

    @property
    def short_sha(self) -> str:
        return self.sha[:12]

    @property
    def marker(self) -> str:
        return f"{MARKER_PREFIX} {self.short_sha} on {self.date}"


# --------------------------------------------------------------------------- #
# Git plumbing
# --------------------------------------------------------------------------- #
def _run_git(*args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=paths.BASE_DIR,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout


def _active_dag_commits(since: str | None) -> list[tuple[str, int]]:
    """Return ``(sha, unix_timestamp)`` for every commit that touched an active DAG file.

    Ordered oldest → newest so that the last write of ``last_seen`` wins.
    """
    rev_range = [f"{since}..HEAD"] if since else []
    pathspec = ["dag", ":(exclude)dag/archive/*", ":(exclude)dag/temp.yml"]
    out = _run_git("log", "--reverse", "--format=%H %ct", *rev_range, "--", *pathspec)
    commits = []
    for line in out.splitlines():
        sha, ct = line.split()
        commits.append((sha, int(ct)))
    return commits


class _BlobReader:
    """Read git blobs by content hash via a persistent ``git cat-file --batch`` process.

    Caching by blob hash means each unique file version is decoded once, even
    though the same file appears unchanged across thousands of commits.
    """

    def __init__(self) -> None:
        self._proc = subprocess.Popen(
            ["git", "cat-file", "--batch"],
            cwd=paths.BASE_DIR,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
        self._cache: dict[str, str | None] = {}

    def read(self, blob_sha: str) -> str | None:
        if blob_sha in self._cache:
            return self._cache[blob_sha]
        assert self._proc.stdin is not None and self._proc.stdout is not None
        self._proc.stdin.write((blob_sha + "\n").encode())
        self._proc.stdin.flush()
        header = self._proc.stdout.readline().decode()
        parts = header.split()
        if len(parts) < 3 or parts[1] != "blob":
            self._cache[blob_sha] = None
            return None
        size = int(parts[2])
        # A pipe read may return fewer bytes than requested; loop until we have all.
        buf = bytearray()
        while len(buf) < size:
            chunk = self._proc.stdout.read(size - len(buf))
            if not chunk:
                break
            buf.extend(chunk)
        self._proc.stdout.read(1)  # trailing newline
        text = buf.decode("utf-8", "replace")
        self._cache[blob_sha] = text
        return text

    def close(self) -> None:
        if self._proc.stdin is not None:
            self._proc.stdin.close()
        self._proc.wait()


def _tree_blob_map(sha: str) -> dict[str, str]:
    """Map ``relpath -> blob_sha`` for every file under ``dag/`` at ``sha`` (one git call)."""
    out = _run_git("ls-tree", "-r", sha, "--", "dag")
    mapping = {}
    for line in out.splitlines():
        meta, _, path = line.partition("\t")
        fields = meta.split()
        if len(fields) >= 2 and fields[1] == "blob":
            mapping[path] = fields[2]
    return mapping


# --------------------------------------------------------------------------- #
# DAG parsing (lenient — historical states must not raise)
# --------------------------------------------------------------------------- #
def _parse_file_steps(text: str) -> dict[str, set[str]]:
    """Parse the ``steps:`` block of a single DAG file into ``{step: {deps}}``.

    Lenient on purpose: historical DAG states may contain duplicates, ``!!set``
    forms, or nested chains that the strict loader rejects. We only need step
    membership and direct dependencies, so we tolerate anything parseable.
    """
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError:
        return {}
    if not isinstance(data, dict):
        return {}
    steps = data.get("steps") or {}
    if not isinstance(steps, dict):
        return {}

    result: dict[str, set[str]] = {}

    def insert(node: str, deps: Any) -> None:
        names: set[str] = set()
        if isinstance(deps, list):
            for item in deps:
                if isinstance(item, str):
                    names.add(item)
                elif isinstance(item, dict):
                    for sub_node, sub_deps in item.items():
                        names.add(sub_node)
                        insert(sub_node, sub_deps)
        elif isinstance(deps, (set, tuple)):
            names |= {str(d) for d in deps}
        result.setdefault(node, set()).update(names)

    for node, deps in steps.items():
        insert(node, deps)
    return result


def _active_dag_at(sha: str, blobs: dict[str, str], reader: _BlobReader) -> dict[str, tuple[set[str], str]]:
    """Reconstruct the merged active DAG at ``sha`` as ``{step: (deps, declaring_file)}``."""
    main_blob = blobs.get("dag/main.yml")
    if not main_blob:
        return {}
    main_text = reader.read(main_blob)
    if not main_text:
        return {}

    files = ["dag/main.yml"]
    try:
        includes = (yaml.safe_load(main_text) or {}).get("include") or []
    except yaml.YAMLError:
        includes = []
    for f in includes:
        if isinstance(f, str) and not any(f.startswith(skip) or f == skip for skip in SKIP_INCLUDES):
            files.append(f)

    merged: dict[str, tuple[set[str], str]] = {}
    for f in files:
        blob = blobs.get(f)
        if not blob:
            continue
        text = main_text if f == "dag/main.yml" else reader.read(blob)
        if not text:
            continue
        for step, deps in _parse_file_steps(text).items():
            if step in merged:
                merged[step][0].update(deps)
            else:
                merged[step] = (set(deps), f)
    return merged


def _build_last_seen(since: str | None, limit: int | None) -> dict[str, LastSeen]:
    """Walk active-DAG history and return the last active appearance of every step."""
    commits = _active_dag_commits(since)
    if limit:
        commits = commits[-limit:]
    log.info("archive_dag.history", n_commits=len(commits))

    reader = _BlobReader()
    last_seen: dict[str, LastSeen] = {}
    try:
        for i, (sha, ts) in enumerate(commits):
            blobs = _tree_blob_map(sha)
            dag = _active_dag_at(sha, blobs, reader)
            for step, (deps, dag_file) in dag.items():
                last_seen[step] = LastSeen(sha=sha, timestamp=ts, deps=deps, dag_file=dag_file)
            if (i + 1) % 250 == 0:
                log.info("archive_dag.progress", processed=i + 1, total=len(commits), steps_seen=len(last_seen))
    finally:
        reader.close()
    return last_seen


# --------------------------------------------------------------------------- #
# Current state
# --------------------------------------------------------------------------- #
def _current_active_keys() -> set[str]:
    return set(load_dag(paths.DAG_FILE))


def _archive_files() -> list[Path]:
    return sorted(paths.DAG_DIR.glob("archive/*.yml"))


def _current_archive_keys_by_file() -> dict[Path, set[str]]:
    """Map each archive DAG file to the set of step keys it declares (no include-following)."""
    result = {}
    for f in _archive_files():
        if f == paths.DAG_ARCHIVE_FILE:
            # main.yml is just an aggregator of includes; it declares no steps of its own.
            continue
        result[f] = set(load_single_dag_file(f))
    return result


# --------------------------------------------------------------------------- #
# Writing
# --------------------------------------------------------------------------- #
def _backfill_markers(dag_file: Path, hashes: dict[str, LastSeen], dry_run: bool) -> int:
    """Insert a ``# archived; ...`` marker above each step in ``dag_file`` that lacks one.

    Line-based and surgical: existing steps, dependencies, comments and ordering
    are preserved; only marker comment lines are added. Returns the number of
    markers inserted.
    """
    lines = dag_file.read_text().splitlines(keepends=True)
    out: list[str] = []
    inserted = 0
    # Track the comment lines immediately preceding the current line.
    preceding_markers: list[int] = []  # indices into `out` that are archived-markers
    for line in lines:
        stripped = line.strip()
        # Top-level step declaration: ``  data://...:`` (indent 2, ends with ':', not a dep).
        is_step = (
            line.startswith("  ")
            and not line.startswith("   ")
            and stripped.endswith(":")
            and not stripped.startswith("-")
            and not stripped.startswith("#")
            and stripped not in ("steps:", "include:")
        )
        if is_step:
            step = stripped[:-1]
            already_marked = bool(preceding_markers)
            if step in hashes and not already_marked:
                out.append(f"  {hashes[step].marker}\n")
                inserted += 1
        out.append(line)
        # Maintain preceding-marker state for the *next* line.
        if MARKER_RE.search(line):
            preceding_markers.append(len(out) - 1)
        elif stripped == "" or not stripped.startswith("#"):
            # Reset once we leave the contiguous comment block above a step.
            if not (stripped.startswith("#")):
                preceding_markers = []
    if inserted and not dry_run:
        dag_file.write_text("".join(out))
    return inserted


def _append_new_steps(new_steps: dict[str, LastSeen], dry_run: bool) -> dict[str, int]:
    """Append steps to their mirror archive file with deps + marker comment.

    Returns ``{archive_file_name: count}``.
    """
    # Group by mirror archive file: dag/<x>.yml -> dag/archive/<x>.yml
    by_file: dict[Path, dict[str, LastSeen]] = {}
    for step, info in new_steps.items():
        mirror = paths.DAG_DIR / "archive" / Path(info.dag_file).name
        by_file.setdefault(mirror, {})[step] = info

    counts: dict[str, int] = {}
    for mirror, steps in sorted(by_file.items()):
        counts[mirror.name] = len(steps)
        if dry_run:
            continue
        if not mirror.exists():
            create_dag_archive_file(dag_file_archive=mirror)
        dag_part = {step: info.deps for step, info in steps.items()}
        comments = {step: info.marker for step, info in steps.items()}
        write_to_dag_file(dag_file=mirror, dag_part=dag_part, comments=comments)
    return counts


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
@click.command(name="archive-dag", help=__doc__, cls=click.Command)
@click.option(
    "--since",
    default=None,
    help="Only walk commits after this ref (incremental update). Default: full history.",
)
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Only consider the most recent N active-DAG commits (for quick testing).",
)
@click.option(
    "--backfill-hashes/--no-backfill-hashes",
    default=True,
    help="Insert recovery-hash marker comments above existing archive steps that lack one.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Report what would change without editing any files.",
)
def cli(since: str | None, limit: int | None, backfill_hashes: bool, dry_run: bool) -> None:
    last_seen = _build_last_seen(since=since, limit=limit)

    current_active = _current_active_keys()
    archive_by_file = _current_archive_keys_by_file()
    current_archive = set().union(*archive_by_file.values()) if archive_by_file else set()

    # Steps active in history but neither active nor already archived now.
    new_steps = {
        step: info for step, info in last_seen.items() if step not in current_active and step not in current_archive
    }
    # Existing archive steps for which we found a recovery hash in history.
    backfillable = {
        step: last_seen[step] for file_keys in archive_by_file.values() for step in file_keys if step in last_seen
    }
    # Existing archive steps with no recovery hash found (active before recorded history, etc.).
    no_hash = current_archive - set(last_seen)

    log.info(
        "archive_dag.summary",
        steps_ever_active=len(last_seen),
        current_active=len(current_active),
        current_archive=len(current_archive),
        new_to_archive=len(new_steps),
        existing_with_recoverable_hash=len(backfillable),
        existing_without_hash=len(no_hash),
        dry_run=dry_run,
    )

    if new_steps:
        appended = _append_new_steps(new_steps, dry_run=dry_run)
        for fname, count in sorted(appended.items()):
            log.info("archive_dag.append", file=fname, n=count)

    if backfill_hashes:
        total_markers = 0
        for dag_file, keys in archive_by_file.items():
            hashes = {step: last_seen[step] for step in keys if step in last_seen}
            if not hashes:
                continue
            total_markers += _backfill_markers(dag_file, hashes, dry_run=dry_run)
        log.info("archive_dag.markers", inserted=total_markers)

    if dry_run:
        log.info("archive_dag.dry_run_done", note="No files were modified.")
    else:
        log.info("archive_dag.done")


if __name__ == "__main__":
    cli()
