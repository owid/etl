"""Propose `owners:` assignments for active garden datasets from git history.

Writes a Markdown report to ``ai/owners-proposal.md`` that lists the top
candidate owners for each active garden dataset, ranked by how often each
OWID team member has touched the dataset's `.py` / `.meta.yml` files in
non-sweep, non-formatting commits.

The script is **read-only** — it does not edit any YAML. Use the output to
drive per-author/per-namespace follow-up PRs that actually write `owners:`
into each dataset's `.meta.yml`.

Run::

    .venv/bin/python apps/owners/propose_owners.py
"""

from __future__ import annotations

import subprocess
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path

from etl.dag_helpers import load_dag
from etl.owners import OWID_DATA_TEAM, resolve_owner
from etl.paths import BASE_DIR

# A commit that touches more datasets than this is treated as a sweep
# (formatting pass, repo-wide refactor, bulk script). Sweep commits are
# excluded from per-dataset attribution — they shouldn't make Mojmír the
# owner of every cherry_blossom in the repo.
SWEEP_DATASET_CUTOFF = 50

# How many top candidates to surface per dataset in the proposal.
TOP_N = 3

OUTPUT_PATH = BASE_DIR / "ai" / "owners-proposal.md"
GARDEN_STEP_DIR = BASE_DIR / "etl" / "steps" / "data" / "garden"


@dataclass
class _Commit:
    sha: str
    email: str
    name: str
    files: list[str]


def _active_garden_datasets() -> dict[str, list[Path]]:
    """Map each active garden step URI to its source files on disk (relative to repo)."""
    dag = load_dag()
    out: dict[str, list[Path]] = {}
    for step in dag:
        if not (step.startswith("data://garden/") or step.startswith("data-private://garden/")):
            continue
        # e.g. data://garden/biodiversity/2024-01-25/cherry_blossom
        suffix = step.split("//", 1)[1]  # garden/biodiversity/.../cherry_blossom
        rel_base = Path("etl/steps/data") / suffix
        files = [rel_base.with_suffix(ext) for ext in (".py", ".meta.yml")]
        existing = [p for p in files if (BASE_DIR / p).exists()]
        if existing:
            out[step] = existing
    return out


def _git_log_for_files(files: list[Path]) -> list[_Commit]:
    """Run a single `git log` over all the given files and return parsed commits."""
    if not files:
        return []
    # %H, %ae, %an separated by \t; --no-merges to skip merge commits;
    # --name-only to emit changed file paths.
    pretty = "--pretty=format:--COMMIT--%n%H%x09%ae%x09%an"
    cmd = ["git", "log", "--no-merges", "--name-only", pretty, "--"] + [str(p) for p in files]
    result = subprocess.run(cmd, cwd=BASE_DIR, capture_output=True, text=True, check=True)
    commits: list[_Commit] = []
    for block in result.stdout.split("--COMMIT--\n"):
        block = block.strip()
        if not block:
            continue
        lines = block.splitlines()
        header = lines[0]
        try:
            sha, email, name = header.split("\t")
        except ValueError:
            continue
        commits.append(_Commit(sha=sha, email=email, name=name, files=[line for line in lines[1:] if line]))
    return commits


def _drop_sweeps(commits: list[_Commit], file_to_step: dict[str, str]) -> list[_Commit]:
    """Remove commits that touch more than SWEEP_DATASET_CUTOFF distinct datasets."""
    kept: list[_Commit] = []
    for c in commits:
        touched_steps = {file_to_step[f] for f in c.files if f in file_to_step}
        if len(touched_steps) > SWEEP_DATASET_CUTOFF:
            continue
        kept.append(c)
    return kept


def _aggregate(commits: list[_Commit], file_to_step: dict[str, str]) -> dict[str, Counter[str]]:
    """For each step, count non-sweep commits per resolved OWID owner."""
    per_step: dict[str, Counter[str]] = defaultdict(Counter)
    for c in commits:
        owner = resolve_owner(c.name) or resolve_owner(c.email)
        if owner is None:
            continue  # bot, ex-employee not in mapping, or unknown contributor
        for f in c.files:
            step = file_to_step.get(f)
            if step is not None:
                per_step[step][owner] += 1
    return per_step


def _write_markdown(per_step: dict[str, Counter[str]], all_steps: dict[str, list[Path]]) -> None:
    by_namespace: dict[str, list[str]] = defaultdict(list)
    for step in all_steps:
        # data://garden/<namespace>/<version>/<short_name>
        ns = step.split("/")[3]
        by_namespace[ns].append(step)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        f.write("# Proposed dataset owners\n\n")
        f.write(
            f"Top candidates per active garden dataset, ranked by non-sweep "
            f"commits to the dataset's `.py` / `.meta.yml` files. "
            f"Sweep commits (touching >{SWEEP_DATASET_CUTOFF} datasets) and commits by "
            f"non-team contributors are excluded.\n\n"
        )
        for ns in sorted(by_namespace):
            f.write(f"## {ns}\n\n")
            f.write("| dataset | top candidates (commits) |\n")
            f.write("|---|---|\n")
            for step in sorted(by_namespace[ns]):
                counts = per_step.get(step, Counter())
                if not counts:
                    candidates = "_no team contributions found_"
                else:
                    top = counts.most_common(TOP_N)
                    candidates = ", ".join(f"{name} ({n})" for name, n in top)
                short = step.rsplit("/", 1)[-1]
                version = step.split("/")[-2]
                f.write(f"| `{version}/{short}` | {candidates} |\n")
            f.write("\n")

        f.write("---\n\n")
        f.write("Reference: canonical names = " + ", ".join(OWID_DATA_TEAM) + "\n")


def main() -> None:
    steps = _active_garden_datasets()
    file_to_step: dict[str, str] = {str(p): step for step, files in steps.items() for p in files}
    all_files = sorted({p for files in steps.values() for p in files})
    commits = _git_log_for_files(all_files)
    commits = _drop_sweeps(commits, file_to_step)
    per_step = _aggregate(commits, file_to_step)
    _write_markdown(per_step, steps)
    print(f"Wrote proposal for {len(steps)} active garden datasets to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
