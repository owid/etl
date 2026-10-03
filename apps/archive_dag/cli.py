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
from etl.steps import extract_step_attributes

log = get_logger()


def _is_parseable_step(step: str) -> bool:
    """True if ``step`` is a well-formed step URI that downstream tooling can parse.

    History contains a few malformed legacy URIs (e.g. a fasttrack step with a
    slash-dated version ``.../22/01/11/...``) that ``extract_step_attributes`` —
    and therefore ``VersionTracker`` — chokes on. We must not record those in the
    archive DAG, so we validate with the same parser the rest of the codebase uses.
    """
    try:
        extract_step_attributes(step)
        return True
    except (ValueError, IndexError):
        return False


# DAG files that are not part of the "active" DAG and must be skipped when
# reconstructing the active DAG at a historical commit.
SKIP_INCLUDES = ("dag/archive/", "dag/temp.yml")

# Marker comment written above each archived step. ``<sha>`` is the last commit at
# which the step was active (the recovery point); ``<date>`` is that commit's date.
MARKER_PREFIX = "# archived; last active in"
MARKER_RE = re.compile(r"#\s*archived;\s*last active in\s+([0-9a-f]{7,40})\b")


@dataclass
class LastSeen:
    """The recovery point for an archived step: the last commit at which it was active.

    ``sha`` is the parent of the commit that removed the step from the active DAG —
    i.e. the last commit where the step was still active, with all its code, metadata
    and snapshot edits in place (including those made in commits that didn't touch the
    DAG). ``git checkout <sha>`` recovers the final working version of the step.
    """

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

    Ordered oldest → newest so removals are detected in the order they happened.
    """
    rev_range = [f"{since}..HEAD"] if since else []
    pathspec = ["dag", ":(exclude)dag/archive/*", ":(exclude)dag/temp.yml"]
    out = _run_git("log", "--reverse", "--format=%H %ct", *rev_range, "--", *pathspec)
    commits = []
    for line in out.splitlines():
        sha, ct = line.split()
        commits.append((sha, int(ct)))
    return commits


def _commit_parent(sha: str, cache: dict[str, tuple[str, int]]) -> tuple[str, int]:
    """Return ``(first_parent_sha, parent_unix_timestamp)`` for ``sha`` (cached).

    The first parent of the removal commit is the last commit where the step was
    still active — the recovery point. Falls back to the commit itself for a root
    commit with no parent.
    """
    if sha not in cache:
        parents = _run_git("rev-list", "-1", "--parents", sha).split()
        parent = parents[1] if len(parents) > 1 else parents[0]
        ts = int(_run_git("show", "-s", "--format=%ct", parent).strip())
        cache[sha] = (parent, ts)
    return cache[sha]


def _default_branch_sha() -> str | None:
    """SHA of ``origin/master``, or None when the remote-tracking ref is unavailable."""
    try:
        return _run_git("rev-parse", "--verify", "origin/master").strip()
    except subprocess.CalledProcessError:
        return None


def _is_ancestor(sha: str, of: str) -> bool:
    result = subprocess.run(
        ["git", "merge-base", "--is-ancestor", sha, of],
        cwd=paths.BASE_DIR,
        capture_output=True,
    )
    return result.returncode == 0


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
        # Parsed results keyed by blob sha. A DAG file changes rarely, so the same
        # blob recurs across thousands of commits — parsing it once collapses ~80k
        # yaml.safe_load calls to a few thousand.
        self._parsed: dict[str, dict[str, set[str]]] = {}
        self._includes: dict[str, list[str]] = {}

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

    def parse_steps(self, blob_sha: str) -> dict[str, set[str]]:
        """Return the parsed ``{step: {deps}}`` for a DAG-file blob (cached by sha)."""
        if blob_sha not in self._parsed:
            text = self.read(blob_sha)
            self._parsed[blob_sha] = _parse_file_steps(text) if text else {}
        return self._parsed[blob_sha]

    def includes(self, blob_sha: str) -> list[str]:
        """Return the ``include:`` list of a DAG-file blob (cached by sha)."""
        if blob_sha not in self._includes:
            text = self.read(blob_sha)
            try:
                inc = (yaml.safe_load(text) or {}).get("include") or [] if text else []
            except yaml.YAMLError:
                inc = []
            self._includes[blob_sha] = inc
        return self._includes[blob_sha]

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

    files = ["dag/main.yml"]
    for f in reader.includes(main_blob):
        if isinstance(f, str) and not any(f.startswith(skip) or f == skip for skip in SKIP_INCLUDES):
            files.append(f)

    merged: dict[str, tuple[set[str], str]] = {}
    for f in files:
        blob = blobs.get(f)
        if not blob:
            continue
        for step, deps in reader.parse_steps(blob).items():
            if step in merged:
                merged[step][0].update(deps)
            else:
                merged[step] = (set(deps), f)
    return merged


def _build_last_seen(since: str | None, limit: int | None) -> dict[str, LastSeen]:
    """Walk active-DAG history and return the recovery point of every removed step.

    Detects *removals*: a step that was an active key at commit N-1 but not at commit
    N was removed at N, so its recovery point is N's parent (the last commit where it
    was still active, with all non-DAG code/metadata edits in place). Steps re-added
    later are dropped from the result. The returned dict therefore contains exactly the
    steps that are removed-and-not-restored over the walked range, each mapped to its
    recovery commit, final dependencies, and the DAG file it last lived in.
    """
    commits = _active_dag_commits(since)
    if limit:
        commits = commits[-limit:]
    log.info("archive_dag.history", n_commits=len(commits))

    reader = _BlobReader()
    parent_cache: dict[str, tuple[str, int]] = {}
    removed: dict[str, LastSeen] = {}
    # Recovery points must survive a squash-merge: branch-local commits disappear from
    # history once the PR is squashed, leaving `git checkout <sha>` with nothing to check
    # out. When a removal's parent is not reachable from origin/master, record the tip of
    # origin/master instead (the newest shipped state of the step — it may have received
    # updates on master after this branch was cut). If master no longer declares the step,
    # fall back to the merge-base — the last shared commit that still contains it. Steps
    # that only ever existed on the branch have no recoverable state on the default branch
    # and are skipped entirely.
    master_sha = _default_branch_sha()
    reachable: dict[str, bool] = {}
    master_state: tuple[int, dict[str, tuple[set[str], str]]] | None = None
    merge_base: tuple[str, int, dict[str, tuple[set[str], str]]] | None = None
    try:
        # Seed the previous state from the baseline (the ``since`` commit) so a removal
        # in the very first walked commit is detected. Without this, ``since..HEAD``
        # omits the baseline and a step deleted just after ``since`` would be missed.
        prev: dict[str, tuple[set[str], str]] = {}
        if since:
            prev = _active_dag_at(since, _tree_blob_map(since), reader)

        for i, (sha, ts) in enumerate(commits):
            dag = _active_dag_at(sha, _tree_blob_map(sha), reader)
            cur_keys = set(dag)
            for step in set(prev) - cur_keys:
                if not _is_parseable_step(step):
                    # Skip malformed legacy URIs that VersionTracker can't parse.
                    continue
                deps, dag_file = prev[step]
                # Drop dependencies VersionTracker would choke on. It tolerates
                # ``walden://`` (filtered by prefix) and any parseable URI; anything
                # else (e.g. the legacy ``data://garden/reference`` or 3-segment
                # ``grapher://`` steps) must not enter the archive graph.
                deps = {d for d in deps if d.startswith("walden") or _is_parseable_step(d)}
                rec_sha, rec_ts = _commit_parent(sha, parent_cache)
                if master_sha is not None:
                    if rec_sha not in reachable:
                        reachable[rec_sha] = _is_ancestor(rec_sha, master_sha)
                    if not reachable[rec_sha]:
                        if master_state is None:
                            master_ts = int(_run_git("show", "-s", "--format=%ct", master_sha).strip())
                            master_state = (master_ts, _active_dag_at(master_sha, _tree_blob_map(master_sha), reader))
                        master_ts, master_dag = master_state
                        if step in master_dag:
                            # The step is still active on master's tip — record that: it
                            # carries any updates the step received on master after this
                            # branch was cut, and it survives the squash-merge.
                            deps, dag_file = master_dag[step]
                            rec_sha, rec_ts = master_sha, master_ts
                        else:
                            if merge_base is None:
                                mb_sha = _run_git("merge-base", "HEAD", master_sha).strip()
                                mb_ts = int(_run_git("show", "-s", "--format=%ct", mb_sha).strip())
                                mb_dag = _active_dag_at(mb_sha, _tree_blob_map(mb_sha), reader)
                                merge_base = (mb_sha, mb_ts, mb_dag)
                            mb_sha, mb_ts, mb_dag = merge_base
                            if step not in mb_dag:
                                # Branch-only transient step (created and removed within this
                                # branch): it never shipped, so there is nothing to archive.
                                log.info("archive_dag.skip_branch_only_step", step=step)
                                continue
                            # Master already dropped the step independently; the merge-base
                            # is the last shared commit that still contains it.
                            deps, dag_file = mb_dag[step]
                            rec_sha, rec_ts = mb_sha, mb_ts
                        deps = {d for d in deps if d.startswith("walden") or _is_parseable_step(d)}
                removed[step] = LastSeen(sha=rec_sha, timestamp=rec_ts, deps=deps, dag_file=dag_file)
            for step in cur_keys:
                removed.pop(step, None)  # re-added → no longer archived
            prev = dag
            if (i + 1) % 250 == 0:
                log.info("archive_dag.progress", processed=i + 1, total=len(commits), removed=len(removed))
    finally:
        reader.close()
    return removed


# --------------------------------------------------------------------------- #
# Current state
# --------------------------------------------------------------------------- #
def _current_active_keys() -> set[str]:
    return set(load_dag(paths.DAG_FILE))


def _archive_files() -> list[Path]:
    return sorted(paths.DAG_DIR.glob("archive/*.yml"))


def _current_archive_keys_by_file() -> dict[Path, set[str]]:
    """Map each archive DAG file to the set of step keys it declares (no include-following).

    Note ``dag/archive/main.yml`` both aggregates the other files via ``include:``
    *and* declares its own steps, so it must be read like any other archive file —
    otherwise its steps look unarchived and get re-appended as cross-file duplicates.
    """
    result = {}
    for f in _archive_files():
        keys = set(load_single_dag_file(f))
        if keys:
            result[f] = keys
    return result


# --------------------------------------------------------------------------- #
# Writing
# --------------------------------------------------------------------------- #
def _backfill_markers(dag_file: Path, hashes: dict[str, LastSeen], dry_run: bool) -> int:
    """Insert or refresh a ``# archived; ...`` marker above each step in ``dag_file``.

    Line-based and surgical: existing steps, dependencies, comments and ordering are
    preserved. If a step already has an archived-marker in the contiguous comment block
    directly above it, the marker is replaced only when its recovery hash is broken
    (not reachable from ``origin/master`` — e.g. a branch commit dropped by a
    squash-merge); a differing-but-reachable hash is left alone, since both are valid
    recovery points and replacing would churn every previously archived entry. Steps
    with no marker get one inserted. This makes the command idempotent and
    self-correcting. Returns the number of markers inserted or updated.
    """
    master_sha = _default_branch_sha()
    reachable: dict[str, bool] = {}

    def _marker_is_valid(marker_line: str) -> bool:
        m = MARKER_RE.search(marker_line)
        if not m:
            return False
        if master_sha is None:
            return True  # no origin/master to validate against — keep what's there
        sha = m.group(1)
        if sha not in reachable:
            reachable[sha] = _is_ancestor(sha, master_sha)
        return reachable[sha]

    lines = dag_file.read_text().splitlines(keepends=True)
    out: list[str] = []
    changed = 0
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
        if is_step and stripped[:-1] in hashes:
            marker_line = f"  {hashes[stripped[:-1]].marker}\n"
            # Look back over the contiguous comment block directly above for an
            # existing marker (stop at the first non-comment line).
            k = len(out) - 1
            existing_idx = None
            while k >= 0 and out[k].lstrip().startswith("#"):
                if MARKER_RE.search(out[k]):
                    existing_idx = k
                    break
                k -= 1
            if existing_idx is not None:
                if out[existing_idx] != marker_line and not _marker_is_valid(out[existing_idx]):
                    out[existing_idx] = marker_line
                    changed += 1
            else:
                out.append(marker_line)
                changed += 1
        out.append(line)
    if changed and not dry_run:
        dag_file.write_text("".join(out))
    return changed


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
    removed = _build_last_seen(since=since, limit=limit)

    current_active = _current_active_keys()
    archive_by_file = _current_archive_keys_by_file()
    current_archive = set().union(*archive_by_file.values()) if archive_by_file else set()

    # Steps removed from the active DAG but not yet recorded in the archive DAG.
    new_steps = {
        step: info for step, info in removed.items() if step not in current_active and step not in current_archive
    }
    # Existing archive steps for which we found a recovery hash in history.
    backfillable = {
        step: removed[step] for file_keys in archive_by_file.values() for step in file_keys if step in removed
    }
    # Existing archive steps with no recovery hash found (removed before recorded history, etc.).
    no_hash = current_archive - set(removed)

    log.info(
        "archive_dag.summary",
        steps_removed_in_history=len(removed),
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
            hashes = {step: removed[step] for step in keys if step in removed}
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
