"""Exclude archived steps from files tab and search in VSCode."""

import json
from typing import Set, Tuple

from etl.paths import BASE_DIR, SNAPSHOTS_DIR, STEPS_DATA_DIR
from etl.steps import load_dag


def active_steps_and_snapshots() -> Tuple[Set[str], Set[str]]:
    DAG = load_dag()

    active_snapshots = set()
    active_steps = set()

    for s in set(DAG.keys()) | {x for v in DAG.values() for x in v}:
        if s.startswith("snapshot"):
            active_snapshots.add(s.split("://")[1])
        else:
            active_steps.add(s.split("://")[1])

    # Strip dataset name after version
    active_steps = {s.rsplit("/", 1)[0] for s in active_steps}
    active_snapshots = {s.rsplit("/", 1)[0] for s in active_snapshots}

    return active_steps, active_snapshots


def snapshots_to_exclude(active_snapshots: Set[str]) -> Set[str]:
    to_exclude = set()

    for d in SNAPSHOTS_DIR.rglob("*"):
        d = d.relative_to(SNAPSHOTS_DIR)
        if len(d.parts) == 2:
            if str(d) not in active_snapshots:
                to_exclude.add(f"snapshots/{d}")

    return to_exclude


def steps_to_exclude(active_steps: Set[str]) -> Set[str]:
    to_exclude = set()

    for d in STEPS_DATA_DIR.rglob("*"):
        d = d.relative_to(STEPS_DATA_DIR)
        if len(d.parts) == 3 and d.parts[0] in ("meadow", "garden", "grapher"):
            if str(d) not in active_steps:
                to_exclude.add(f"etl/steps/data/{d}")

    return to_exclude


def main():
    active_steps, active_snapshots = active_steps_and_snapshots()

    to_exclude = {s: True for s in sorted(snapshots_to_exclude(active_snapshots) | steps_to_exclude(active_steps))}

    # Update .vscode/settings file
    # update .vscode/settings.json
    settings = json.loads((BASE_DIR / ".vscode/settings.json").read_text())
    settings["files.exclude"].update(to_exclude)
    settings["search.exclude"].update(to_exclude)
    (BASE_DIR / ".vscode/settings.json").write_text(json.dumps(settings, indent=2))

    print(f"Excluded {len(to_exclude)} steps and snapshots")


if __name__ == "__main__":
    main()
