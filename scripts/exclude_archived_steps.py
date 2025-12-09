"""Exclude archived steps from files tab and search in VSCode."""

import shutil
from collections import OrderedDict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Set

import click
import commentjson

from etl.dag_helpers import get_active_snapshots, get_active_steps
from etl.paths import BASE_DIR, SNAPSHOTS_DIR, STEPS_DATA_DIR

# Time cutoff for recent modifications (1 month)
RECENT_MODIFICATION_CUTOFF = datetime.now() - timedelta(days=30)


def should_exclude(path: Path) -> bool:
    """Determine if a path should be excluded based on shared content and modification time."""
    # Skip if path contains 'shared'
    if "shared" in str(path):
        return False

    # Skip if modified is recent
    if path.is_dir() and path.exists() and datetime.fromtimestamp(path.stat().st_mtime) > RECENT_MODIFICATION_CUTOFF:
        return False

    return True


def snapshots_to_exclude(active_snapshots: Set[str]) -> Set[str]:
    to_exclude = set()

    active_snapshots_folders = {s.rsplit("/", 1)[0] for s in active_snapshots}

    for d in SNAPSHOTS_DIR.rglob("*"):
        # Use folder
        d_rel = d.relative_to(SNAPSHOTS_DIR)
        if len(d_rel.parts) == 2:
            if not should_exclude(d):
                continue

            if str(d_rel) not in active_snapshots_folders:
                to_exclude.add(f"snapshots/{d_rel}")

    return to_exclude


def steps_to_exclude(active_steps: Set[str]) -> Set[str]:
    to_exclude = set()

    for d in STEPS_DATA_DIR.rglob("*"):
        d_rel = d.relative_to(STEPS_DATA_DIR)
        if len(d_rel.parts) == 3 and d_rel.parts[0] in ("meadow", "garden", "grapher"):
            if not should_exclude(d):
                continue

            if str(d_rel) not in active_steps:
                to_exclude.add(f"etl/steps/data/{d_rel}")

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
    active_snapshots = get_active_snapshots()
    active_steps = get_active_steps()

    to_exclude = {s: True for s in sorted(snapshots_to_exclude(active_snapshots) | steps_to_exclude(active_steps))}

    if dry_run:
        print(f"[Dry Run] Would exclude {len(to_exclude)} steps and snapshots")
    else:
        if settings_scope == "project":
            settings_path = BASE_DIR / ".vscode/settings.json"
        else:
            settings_path = Path.home() / "Library/Application Support/Code/User/settings.json"

        # Create a backup of the settings file
        backup_path = settings_path.with_suffix(".json.bak")
        shutil.copy2(settings_path, backup_path)
        print(f"Created backup at {backup_path}")

        # Update settings file
        settings_text = settings_path.read_text()
        settings = commentjson.loads(settings_text)

        # Update exclusions
        for col in ("files.exclude", "search.exclude"):
            if col not in settings:
                settings[col] = {}

            # Remove all existing exclusions
            for k in list(settings[col].keys()):
                if k.startswith("etl/steps/data/") or k.startswith("snapshots/"):
                    del settings[col][k]

            # Add new exclusions
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
