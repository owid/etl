"""Print a Markdown overview of a dataset's ETL pipeline, for the refresher at the start of /update-dataset.

Lists, from the active DAG and the files on disk:
  1. the chain: every step of `<namespace>/<version>/<short_name>` (all channels) and the same-short-name steps
     upstream of it at older versions, plus same-version sibling steps the chain depends on (extra snapshots,
     helper gardens like `income_groups_aggregations`);
  2. each step's files and its inputs from outside the chain (population, regions, other datasets);
  3. the direct consumers outside the chain (other data steps, viz://, export://);
  4. history: garden `dataset.owners`, the last commits touching the chain's files, the previous version
     (archived, or still active during an update), and any newer active version.

It reports structure only — Claude still reads the step code to explain what each step does.

    .venv/bin/python .claude/skills/update-dataset/scripts/pipeline_overview.py <namespace>/<version>/<short_name>
"""

import argparse
import subprocess
import sys
from pathlib import Path

import yaml

from etl import paths
from etl.dag_helpers import graph_nodes, load_dag, load_single_dag_file
from etl.steps import reverse_graph

# Companion files that sit next to a data step's script.
COMPANION_SUFFIXES = [".meta.yml", ".countries.json", ".excluded_countries.json", ".corrections.yml"]
# Snapshot schemes (private snapshots live in the same `snapshots/` tree).
SNAPSHOT_SCHEMES = ("snapshot", "snapshot-private")
# Pipeline order used to sort the chain.
STAGES = ["snapshot", "meadow", "garden", "grapher"]
# Consumers listed by name before the rest are only counted.
MAX_CONSUMERS = 25
# Commits shown under "History".
N_COMMITS = 3


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("dataset", help="<namespace>/<version>/<short_name>")
    args = parser.parse_args()

    namespace, version, short_name = args.dataset.strip("/").split("/")
    dag = load_dag()

    seeds = [step for step in graph_nodes(dag) if step_key(step) == (namespace, version, short_name)]
    if not seeds:
        sys.exit(f"No active DAG step matches {args.dataset}.")
    members = find_upstream_members(dag, seeds, namespace, version, short_name)
    chain = sorted((s for s in members if (key := step_key(s)) and key[2] == short_name), key=stage_order)
    siblings = sorted(members - set(chain), key=stage_order)

    print(f"# Pipeline overview: `{args.dataset}`\n")
    print(f"DAG file(s): {', '.join(f'`{f}`' for f in dag_files_declaring(members))}\n")

    print("## Chain\n")
    for step in chain:
        print_step(step, dag, members)
    if siblings:
        print("\n## Siblings the chain depends on (same namespace and version)\n")
        print("Part of this chain (extra snapshot, helper step) or a companion dataset that bumps together.\n")
        for step in siblings:
            print_step(step, dag, members)

    print("\n## Direct consumers outside the chain\n")
    print_consumers(dag, members)

    print("\n## History\n")
    print_history(dag, chain, namespace, version, short_name)


def stage_order(step: str) -> tuple[int, str]:
    """Sort key: snapshot → meadow → garden → grapher → everything else."""
    stage = channel(step)
    return (STAGES.index(stage) if stage in STAGES else len(STAGES)), step


def channel(step: str) -> str:
    """`snapshot`, `meadow`, `garden`, `grapher`, … — the scheme for snapshots, else the first path segment."""
    scheme, _, path = step.partition("://")
    return "snapshot" if scheme in SNAPSHOT_SCHEMES else path.split("/")[0]


def step_key(step: str) -> tuple[str, str, str] | None:
    """Return (namespace, version, short_name) of a step URI, or None if it doesn't have that shape."""
    scheme, _, path = step.partition("://")
    parts = path.split("/")
    if scheme in SNAPSHOT_SCHEMES and len(parts) == 3:
        return parts[0], parts[1], parts[2].split(".")[0]
    if len(parts) == 4:
        return parts[1], parts[2], parts[3]
    return None


def find_upstream_members(
    dag: dict[str, set[str]], seeds: list[str], namespace: str, version: str, short_name: str
) -> set[str]:
    """Steps reachable upstream from `seeds` through deps of the same namespace and either the same short name
    (any version: older chains mix versions, e.g. a 2023-01-18 garden on a 2023-01-10 meadow) or the same version
    (extra snapshots, helper steps)."""
    found = set(seeds)
    to_visit = list(seeds)
    while to_visit:
        for dep in dag.get(to_visit.pop(), set()):
            key = step_key(dep)
            if dep in found or key is None or key[0] != namespace:
                continue
            if key[2] == short_name or key[1] == version:
                found.add(dep)
                to_visit.append(dep)
    return found


def step_files(step: str) -> list[Path]:
    """Files on disk that define a step (script or package, plus companion files)."""
    scheme, _, path = step.partition("://")
    if scheme in SNAPSHOT_SCHEMES:
        base = paths.SNAPSHOTS_DIR / path
        candidates = [base.with_name(base.name + ".dvc"), base.with_name(base.name.split(".")[0] + ".py")]
    elif scheme in ("data", "data-private"):
        base = paths.STEPS_DATA_DIR / path
        candidates = [base.with_suffix(".py"), base] + [base.with_name(base.name + s) for s in COMPANION_SUFFIXES]
    else:
        base = paths.STEP_DIR / scheme / path
        candidates = [base.with_suffix(".py"), base, base.with_suffix(".yml")]
    return [p for p in candidates if p.exists()]


def describe_file(file: Path) -> str:
    rel = file.relative_to(paths.BASE_DIR)
    if file.is_dir():
        n_lines = sum(len(f.read_text().splitlines()) for f in file.rglob("*.py"))
        return f"`{rel}/` (package, {n_lines} lines of Python)"
    if file.suffix == ".py":
        return f"`{rel}` ({len(file.read_text().splitlines())} lines)"
    if file.suffix == ".dvc":
        origin = ((yaml.safe_load(file.read_text()) or {}).get("meta") or {}).get("origin") or {}
        details = [f"{k}: {origin[k]}" for k in ("producer", "title", "url_main") if origin.get(k)]
        return f"`{rel}`" + (f" — {'; '.join(details)}" if details else "")
    return f"`{rel}`"


def print_step(step: str, dag: dict[str, set[str]], members: set[str]) -> None:
    print(f"- **`{step}`**")
    files = step_files(step)
    for file in files:
        print(f"  - {describe_file(file)}")
    if not files:
        print("  - (no files found on disk)")
    external = sorted(dag.get(step, set()) - members)
    if external:
        print(f"  - inputs from outside the chain: {', '.join(f'`{d}`' for d in external)}")


def print_consumers(dag: dict[str, set[str]], members: set[str]) -> None:
    reverse = reverse_graph(dag)
    consumers = sorted({c for m in members for c in reverse.get(m, set())} - members)
    note = "Charts built in the admin read the grapher dataset directly and are not counted here."
    if not consumers:
        print(f"No steps in the active DAG read this dataset. {note}")
        return
    by_kind: dict[str, int] = {}
    for consumer in consumers:
        kind = f"{consumer.partition('://')[0]}://{channel(consumer)}"
        by_kind[kind] = by_kind.get(kind, 0) + 1
    print(f"{len(consumers)} step(s): " + ", ".join(f"{n} {kind}" for kind, n in sorted(by_kind.items())) + "\n")
    for consumer in consumers[:MAX_CONSUMERS]:
        print(f"- `{consumer}`")
    if len(consumers) > MAX_CONSUMERS:
        print(f"- … and {len(consumers) - MAX_CONSUMERS} more")
    print(f"\n{note}")


def print_history(dag: dict[str, set[str]], chain: list[str], namespace: str, version: str, short_name: str) -> None:
    garden_base = paths.STEPS_GARDEN_DIR / namespace / version
    # Module-style steps keep the .meta.yml next to the script, package-style ones inside the folder.
    for garden_meta in [garden_base / f"{short_name}.meta.yml", garden_base / short_name / f"{short_name}.meta.yml"]:
        if not garden_meta.exists():
            continue
        dataset_block = (yaml.safe_load(garden_meta.read_text()) or {}).get("dataset") or {}
        owners = dataset_block.get("owners") or []
        print(f"- Owners (garden `.meta.yml`): {', '.join(map(str, owners)) or '(none set)'}")
        if dataset_block.get("update_period_days"):
            print(f"- `update_period_days`: {dataset_block['update_period_days']}")

    files = [str(f.relative_to(paths.BASE_DIR)) for step in chain for f in step_files(step)]
    garden_dir = str(paths.STEPS_GARDEN_DIR.relative_to(paths.BASE_DIR))
    garden_script = next((f for f in files if f.startswith(garden_dir)), files[0] if files else None)
    if garden_script and (paths.BASE_DIR / garden_script).is_dir():
        garden_script = f"{garden_script}/__init__.py"
    archived = {v for v in versions_of(archived_steps(), namespace, short_name) if v < version}
    # The old version stays active until the update archives it, so active older gardens count as predecessors too.
    active_gardens = {s for s in graph_nodes(dag) if channel(s) == "garden" and s not in chain}
    active = {v for v in versions_of(active_gardens, namespace, short_name) if v < version}
    older = sorted(archived | active)
    created = git_log(["-n1", "--diff-filter=A"], [garden_script] if garden_script else [])
    # Without a predecessor, the commit that created the garden step built the dataset, not updated it.
    label = "the last update" if older else "first built — no earlier version, so this is its first update"
    print(f"- Garden step created ({label}): {created or '(not found)'}")
    print("- Last commits touching the chain's files (often repo-wide sweeps, not updates):")
    for line in git_log([f"-n{N_COMMITS}"], files).splitlines() or ["(none found)"]:
        print(f"  - {line}")

    if older:
        state = "still active" if older[-1] in active else "archived"
        print(f"- Previous version: {older[-1]} ({state})")
    else:
        print("- Previous version: (none, active or in dag/archive)")

    newer = sorted(v for v in versions_of(set(dag), namespace, short_name) if v > version)
    if newer:
        print(f"- ⚠️ Newer active version(s) already in the DAG: {', '.join(newer)}")


def git_log(options: list[str], files: list[str]) -> str:
    if not files:
        return ""
    result = subprocess.run(
        ["git", "log", *options, "--date=short", "--format=%h %ad %an — %s", "--", *files],
        capture_output=True,
        text=True,
        cwd=paths.BASE_DIR,
    )
    # An empty log must mean "no history", never "git could not read the history".
    if result.returncode != 0:
        raise RuntimeError(f"git log failed (exit {result.returncode}): {result.stderr.strip()}")
    return result.stdout.strip()


def versions_of(steps: set[str], namespace: str, short_name: str) -> set[str]:
    keys = [step_key(s) for s in steps]
    return {k[1] for k in keys if k is not None and (k[0], k[2]) == (namespace, short_name)}


def archived_steps() -> set[str]:
    steps: set[str] = set()
    for file in sorted((paths.DAG_DIR / "archive").glob("*.yml")):
        graph = load_single_dag_file(file)
        steps |= set(graph) | {d for deps in graph.values() for d in deps}
    return steps


def dag_files_declaring(steps: set[str]) -> list[str]:
    files = []
    for file in sorted(paths.DAG_DIR.glob("*.yml")):
        if set(load_single_dag_file(file)) & steps:
            files.append(str(file.relative_to(paths.BASE_DIR)))
    return files


if __name__ == "__main__":
    main()
