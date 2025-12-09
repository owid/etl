"""Automatically update data snapshots and optionally create a pull request with the changes.

**Main use case**: Run all autoupdate-enabled snapshots, update their data if needed, and create a PR if there are changes.

Examples:

```
# Run all autoupdate snapshots, update data, and create PRs if needed
etl autoupdate --create-pr

# Run in dry-run mode (no changes will be made)
etl autoupdate --dry-run

# Only process snapshots matching a filter
etl autoupdate --filter "population"
```
"""

import datetime as dt
import subprocess
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

import click
import yaml
from owid.catalog.utils import underscore
from rich_click import RichCommand
from structlog import get_logger
from tqdm.auto import tqdm

from etl.dag_helpers import get_active_snapshots
from etl.git_api_helpers import GithubApiRepo
from etl.paths import BASE_DIR, SNAPSHOTS_DIR
from etl.snapshot import Snapshot

log = get_logger()

# Create a GitHub API repo instance for all GitHub operations
github_repo = GithubApiRepo()


@dataclass
class SnapshotUpdate:
    """Class to store the snapshot update information."""

    name: str
    snapshot_script: Path
    dvc_files: list[Path]


def create_autoupdate_pr(update_name: str, files: list[Path]):
    """Create a pull request with given files. It creates it via API without modifying the local repo."""
    # name = Path(snapshot).stem.replace("_", "-")
    # version = snapshot.split("/")[1]
    # snapshot_hash = hashlib.md5(snapshot.encode()).hexdigest()[:3]
    branch_name = f"auto-{underscore(update_name).replace('_', '-')}"
    title = f"🤖 Autoupdate: {update_name}"

    # Get the latest commit SHA on master
    master_sha = github_repo.get_master_commit_sha()

    # Check if the branch already exists
    branch_exists, branch_info = github_repo.check_branch_exists(branch_name)

    # If the branch exists, check if there's a PR already. If not, it means that it got either merged or deleted
    #   in which case we delete the branch to start fresh.
    if branch_exists:
        open_prs = github_repo.get_open_prs(branch_name)
        if not open_prs:
            # No open PRs for this branch, delete it
            github_repo.delete_branch(branch_name)
            branch_exists = False

    # Create a new branch if it doesn't exist
    if not branch_exists:
        github_repo.create_branch(branch_name, master_sha)
    else:
        # Merge with master, resolving any conflicts in favor of our branch
        github_repo.merge_with_master_resolve_conflicts(branch_name)

    # Prepare parent and base tree SHA
    if branch_exists:
        # Get the current branch HEAD (after merge)
        _, current_branch_info = github_repo.check_branch_exists(branch_name)
        parent_sha = current_branch_info["object"]["sha"]
        # Get the tree SHA from the parent commit, not the commit SHA itself
        parent_commit = github_repo.repo.get_git_commit(parent_sha)
        base_tree_sha = parent_commit.tree.sha
    else:
        parent_sha = master_sha
        # Get the tree SHA from master commit
        master_commit = github_repo.repo.get_git_commit(master_sha)
        base_tree_sha = master_commit.tree.sha

    # Create commit with files (this will now be on top of the merged state)
    has_changes, _ = github_repo.create_commit_with_files(
        files=files, branch_name=branch_name, commit_message=title, parent_sha=parent_sha, base_tree_sha=base_tree_sha
    )

    # Don't create PR if there are no changes
    if not has_changes:
        log.info(f"No changes in {update_name}")
        return

    existing_prs = github_repo.get_open_prs(branch_name)

    if existing_prs:
        log.info(f"Pull request already exists: {existing_prs[0].html_url}")
    else:
        # Create a pull request
        body = "" if has_changes else "This PR was created without file changes but includes a merge with master."
        pr_url = github_repo.create_pull_request(title, branch_name, body)
        log.info(f"Pull request created: {pr_url}")


def run_updates(
    group_updates: list[SnapshotUpdate],
    create_pr: bool,
    dry_run: bool = False,
) -> None:
    """Update all snapshots in a group."""

    files_to_update = []
    identicals = []

    for update in group_updates:
        # Extract attributes from the update.
        snapshot = update.name
        snapshot_script = update.snapshot_script
        dvc_files = update.dvc_files
        dvc_file = dvc_files[0]

        files_to_update += update.dvc_files

        if dry_run:
            log.info(f"[DRY-RUN] Would execute {snapshot}.")
            continue

        log.info(f"Executing {snapshot} / {snapshot_script.relative_to(SNAPSHOTS_DIR)}")

        # Get the original .dvc file content from origin/master branch
        dvc_path_relative = str(dvc_file.relative_to(BASE_DIR))
        result = subprocess.run(
            ["git", "show", f"origin/master:{dvc_path_relative}"], capture_output=True, text=True, check=True
        )
        original_dvc_content = result.stdout

        # Parse the original YAML content
        original_outs = yaml.safe_load(original_dvc_content)["outs"][0]

        # Try to execute snapshot and print error output if it fails.
        try:
            subprocess.run(["python", snapshot_script, "--upload"], check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            log.error(f"Snapshot script failed: {snapshot_script}\nstdout:\n{e.stdout}\nstderr:\n{e.stderr}")
            raise

        # Load md5 and size from the (possibly) updated file
        with open(dvc_file, "r") as f:
            new_outs = yaml.safe_load(f)["outs"][0]

        # Data is not new, MD5 or size is identical.
        # NOTE: Some snapshots may have the same data but different md5s (e.g. scraped htmls).
        if original_outs["md5"] == new_outs["md5"]:
            identical = True

        elif original_outs["size"] == new_outs["size"]:
            identical = True
            subprocess.run(["git", "restore", "--"] + [str(f) for f in dvc_files])

        else:
            identical = False

        # If the snapshot is different, update origin.date_accessed
        if not identical:
            for f in dvc_files:
                snap = Snapshot(str(dvc_files[0].relative_to(SNAPSHOTS_DIR)).replace(".dvc", ""))
                if snap.m.origin:
                    snap.m._update_metadata_file({"meta": {"origin": {"date_accessed": str(dt.date.today())}}})

        identicals.append(identical)

    # If MD5 has changed, create a PR.
    if create_pr and not any(identical for identical in identicals):
        create_autoupdate_pr(update_name=update.name, files=files_to_update)  # type: ignore

    # Restore the DVC files to their original state after processing
    if not dry_run:
        # Restore all DVC files that were processed
        all_dvc_files = []
        for update in group_updates:
            all_dvc_files.extend([str(f) for f in update.dvc_files])

        if all_dvc_files:
            subprocess.run(["git", "restore", "--"] + all_dvc_files, check=True, capture_output=True)


@click.command(
    name="autoupdate",
    cls=RichCommand,
    help=__doc__,
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Run in dry-run mode. No snapshot scripts will be executed and no files will be changed.",
)
@click.option(
    "--create-pr",
    is_flag=True,
    help="If there is an update, create a pull request with the changes.",
)
@click.option(
    "--filter",
    type=str,
    help="Process only snapshots whose name includes the given substring.",
)
def cli(
    dry_run: bool,
    create_pr: bool,
    filter: str,
):
    active_snapshots = get_active_snapshots()
    if filter:
        active_snapshots = {s for s in active_snapshots if filter in s}

    # Create a group of updates by name.
    updates: dict[str, list[SnapshotUpdate]] = defaultdict(list)

    # Loop over all snapshot scripts.
    for snapshot in active_snapshots:
        snapshot_script = SNAPSHOTS_DIR / snapshot

        # Find .dvc file belonging to the snapshot script
        dvc_files = list(snapshot_script.parent.glob(f"{snapshot_script.stem}.*.dvc"))
        assert len(dvc_files) >= 1, f"Expected to find at least one .dvc file for {snapshot}"

        # Quite rare but possible to have multiple .dvc files for a single snapshot.
        dvc_file = dvc_files[0]

        with open(dvc_file, "r") as f:
            meta = yaml.safe_load(f)
            if not meta.get("autoupdate"):
                # log.info("run_all_snapshots.skip", snapshot=snapshot, reason="autoupdate not enabled")
                continue

        # Raise warning when using multiple .dvc files.
        if len(dvc_files) > 1:
            log.warning(f"Multiple .dvc files found for {snapshot}. Using the first one.")

        updates[meta["autoupdate"]["name"]].append(
            SnapshotUpdate(
                name=meta["autoupdate"]["name"],
                snapshot_script=snapshot_script,
                dvc_files=dvc_files,
            )
        )

    for name, group_updates in tqdm(updates.items()):
        run_updates(
            group_updates,
            create_pr=create_pr,
            dry_run=dry_run,
        )
