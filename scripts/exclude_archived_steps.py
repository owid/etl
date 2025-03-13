"""Exclude archived steps from files tab and search in VSCode."""

from collections import OrderedDict
from pathlib import Path
from typing import Set, Tuple

import click
import commentjson

from etl.dag_helpers import load_dag
from etl.paths import BASE_DIR, SNAPSHOTS_DIR, STEPS_DATA_DIR


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


@click.command(help="Exclude archived steps with a chosen settings scope.")
@click.option(
    "--settings-scope",
    type=click.Choice(["project", "user"], case_sensitive=False),
    default="project",
    help="Select whether to update project or user settings.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Perform a dry run without making any changes.",
)
def main(settings_scope, dry_run):
    active_steps, active_snapshots = active_steps_and_snapshots()

    to_exclude = {s: True for s in sorted(snapshots_to_exclude(active_snapshots) | steps_to_exclude(active_steps))}

    if dry_run:
        print(f"[Dry Run] Would exclude {len(to_exclude)} steps and snapshots")
    else:
        if settings_scope == "project":
            settings_path = BASE_DIR / ".vscode/settings.json"
        else:
            settings_path = Path.home() / "Library/Application Support/Code/User/settings.json"

        # Update settings file
        settings_text = settings_path.read_text()
        settings = commentjson.loads(settings_text)

        # Update exclusions
        for col in ("files.exclude", "search.exclude"):
            if col not in settings:
                settings[col] = {}
            settings[col].update(to_exclude)

        # Reorder settings to move 'files.exclude' and 'search.exclude' to the end
        reordered_settings = OrderedDict()
        for key, value in settings.items():
            if key not in ["files.exclude", "search.exclude"]:
                reordered_settings[key] = value
        reordered_settings["files.exclude"] = settings["files.exclude"]
        reordered_settings["search.exclude"] = settings["search.exclude"]

        settings_path.write_text(commentjson.dumps(reordered_settings, indent=2))

        print(f"Excluded {len(to_exclude)} steps and snapshots")


if __name__ == "__main__":
    main()
