"""Run many snapshots, time how long they took, and which ones failed.

RESULTS:
This script attempted to execute 440 snapshots, of which:
* 20% failed.
* 4% took longer than 100s (and where therefore interrupted).
* 86% were executed successfully.

All results are stored in the accompanying file: snapshot_execution_times.json

"""

import datetime as dt
import json
import random
import subprocess
import time
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional, Set

import click
import yaml
from owid.catalog.utils import underscore
from owid.datautils.io import save_json
from structlog import get_logger
from tqdm.auto import tqdm

from etl.dag_helpers import load_dag
from etl.git_api_helpers import GithubApiRepo
from etl.paths import BASE_DIR, SNAPSHOTS_DIR
from etl.snapshot import Snapshot

log = get_logger()

SNAPSHOT_SCRIPTS = sorted(list((BASE_DIR / "snapshots").glob("*/*/*.py")))
random.shuffle(SNAPSHOT_SCRIPTS)

# Kill a subprocess if it takes longer than this many seconds.
OUTPUT_FILE = BASE_DIR / "etl" / "scripts" / "archive" / "run_all_snapshots" / "snapshot_execution_times.json"

# Create a GitHub API repo instance for all GitHub operations
github_repo = GithubApiRepo()


def get_active_snapshots() -> Set[str]:
    DAG = load_dag()

    active_snapshots = set()

    for s in set(DAG.keys()) | {x for v in DAG.values() for x in v}:
        if s.startswith("snapshot"):
            active_snapshots.add(s.split("://")[1])

    # Strip extension
    return {s.split(".")[0] + ".py" for s in active_snapshots}


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
        # Always merge with master, regardless of whether there are changes
        merge_successful = github_repo.merge_with_master(branch_name)

        # If merge unsuccessful, log warning but continue to create PR
        if not merge_successful:
            log.warning(f"Failed to merge master into {branch_name}, but will still create PR")

    # Prepare parent and base tree SHA
    parent_sha = master_sha
    base_tree_sha = master_sha

    # If branch exists, use its latest commit as parent instead of master
    if branch_exists:
        parent_sha = branch_info["object"]["sha"]
        base_tree_sha = parent_sha

    # Create commit with files
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


@dataclass
class SnapshotUpdate:
    """Class to store the snapshot update information."""

    name: str
    snapshot_script: Path
    dvc_files: list[Path]


@dataclass
class ExecutionResult:
    """Class to store the execution result of a snapshot."""

    name: str
    status: str = ""
    identical: bool = False
    duration: float = 0.0


def run_updates(
    group_updates: list[SnapshotUpdate],
    create_pr: bool,
    timeout: Optional[int] = None,
    continue_on_failure: bool = False,
    dry_run: bool = False,
) -> list[ExecutionResult]:
    """Update all snapshots in a group."""

    exec_results = []
    files_to_update = []

    for update in group_updates:
        # Extract attributes from the update.
        snapshot = update.name
        snapshot_script = update.snapshot_script
        dvc_files = update.dvc_files
        dvc_file = dvc_files[0]

        files_to_update += update.dvc_files

        # Start timer.
        start_time = time.time()

        exec_result = ExecutionResult(name=snapshot)

        if dry_run:
            log.info(f"[DRY-RUN] Would execute {snapshot}.")
            exec_result.status = "DRY-RUN"
            exec_results.append(exec_result)
            continue

        try:
            log.info(f"Executing {snapshot} / {snapshot_script.relative_to(SNAPSHOTS_DIR)}")

            # Get the original .dvc file content from origin/master branch
            dvc_path_relative = str(dvc_file.relative_to(BASE_DIR))
            result = subprocess.run(
                ["git", "show", f"origin/master:{dvc_path_relative}"], capture_output=True, text=True, check=True
            )
            original_dvc_content = result.stdout

            # Parse the original YAML content
            original_outs = yaml.safe_load(original_dvc_content)["outs"][0]

            # Try to execute snapshot.
            kwargs = {}
            if timeout:
                kwargs["timeout"] = timeout
            subprocess.run(
                ["python", snapshot_script, "--upload"], check=True, capture_output=True, text=True, **kwargs
            )

            # Load md5 and size from the (possibly) updated file
            with open(dvc_file, "r") as f:
                new_outs = yaml.safe_load(f)["outs"][0]

            exec_result.status = "SUCCESS"

            # Data is not new, MD5 or size is identical.
            # NOTE: Some snapshots may have the same data but different md5s (e.g. scraped htmls).
            if original_outs["md5"] == new_outs["md5"]:
                exec_result.identical = True

            elif original_outs["size"] == new_outs["size"]:
                exec_result.identical = True
                subprocess.run(["git", "restore", "--"] + [str(f) for f in dvc_files])

            else:
                exec_result.identical = False

            # If the snapshot is different, update origin.date_accessed
            if not exec_result.identical:
                for f in dvc_files:
                    snap = Snapshot(str(dvc_files[0].relative_to(SNAPSHOTS_DIR)).replace(".dvc", ""))
                    if snap.m.origin:
                        snap.m._update_metadata_file({"meta": {"origin": {"date_accessed": str(dt.date.today())}}})

            # Add duration time for successfully executed snapshot.
            exec_result.duration = round(time.time() - start_time, 3)

        except subprocess.TimeoutExpired:
            # Stop snapshot that is taking too long and mark it as timeout.
            exec_result.status = "TIMEOUT"
            log.error(f"Timeout expired for {snapshot}. Marking as 'TIMEOUT'.")
        except subprocess.CalledProcessError as e:
            # Mark snapshot as failed.
            exec_result.status = "FAILED"
            if continue_on_failure:
                log.warning(f"Continuing despite failure in {snapshot}.")
            else:
                log.error(f"Failed to execute {snapshot}.")
                raise e

        exec_results.append(exec_result)

    # If MD5 has changed, create a PR.
    if create_pr and not any(exec_result.identical for exec_result in exec_results):
        create_autoupdate_pr(update_name=update.name, files=files_to_update)  # type: ignore

    return exec_results


@click.command()
@click.option("--dry-run", is_flag=True, help="Run the script in dry-run mode.")
@click.option("--create-pr", is_flag=True, help="If there's an update, create a PR.")
@click.option("--filter", type=str, help="Process only snapshots that include the given substring.")
@click.option(
    "--timeout",
    type=int,
    default=None,
    help="Timeout in seconds for each snapshot.",
)
@click.option("--skip", is_flag=True, help="Skip snapshots that have already been processed.")
@click.option("--all", is_flag=True, help="Run snapshots even if they don't have `autoupdate` set.")
@click.option("--continue-on-failure", is_flag=True, help="Continue running snapshots even if some fail.")
def main(
    dry_run: bool,
    create_pr: bool,
    filter: str,
    timeout: Optional[int],
    skip: bool,
    all: bool,
    continue_on_failure: bool,
):
    # Create a dictionary to store the execution results: duration (in seconds) or "FAILED"
    if OUTPUT_FILE.exists():
        # Load existing results from the JSON file to allow resuming.
        with open(OUTPUT_FILE, "r") as f:
            execution_results = json.load(f)
    else:
        execution_results = {}

    active_snapshots = get_active_snapshots()
    if filter:
        active_snapshots = {s for s in active_snapshots if filter in s}

    # Filter fasttrack snapshots
    active_snapshots = {s for s in active_snapshots if not s.startswith("fasttrack/")}

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

        # If all is set, run all snapshots, even if they don't have autoupdate set.
        if all:
            # Create artificial autoupdate.
            meta = {
                "autoupdate": {
                    "name": snapshot,
                }
            }
        else:
            with open(dvc_file, "r") as f:
                meta = yaml.safe_load(f)
                if not meta.get("autoupdate"):
                    # log.info("run_all_snapshots.skip", snapshot=snapshot, reason="autoupdate not enabled")
                    continue

        # Raise warning when using multiple .dvc files.
        if len(dvc_files) > 1:
            log.warning(f"Multiple .dvc files found for {snapshot}. Using the first one.")

        # Skip snapshots that have already been processed.
        if skip and snapshot in execution_results:
            log.info("run_all_snapshots.skip", snapshot=snapshot, reason="already processed")
            continue

        # Snapshot script does not exist.
        if not snapshot_script.exists():
            log.info("run_all_snapshots.skip", snapshot=snapshot, reason="script not found")
            continue

        # Read the script file.
        with open(snapshot_script, "r") as f:
            snapshot_text = f.read()

        # Skip script files that are not snapshots (or at least do not have an "--upload" flag explicitly defined).
        if "--upload" not in snapshot_text:
            log.info("run_all_snapshots.skip", snapshot=snapshot, reason="no --upload flag")
            continue
        # Skip scripts that require the use of a local file.
        if ("--path-to-file" in snapshot_text) or ("-f " in snapshot_text):
            log.info("run_all_snapshots.skip", snapshot=snapshot, reason="requires local file")
            continue

        updates[meta["autoupdate"]["name"]].append(
            SnapshotUpdate(
                name=meta["autoupdate"]["name"],
                snapshot_script=snapshot_script,
                dvc_files=dvc_files,
            )
        )

    for name, group_updates in tqdm(updates.items()):
        execution_results[name] = run_updates(
            group_updates,
            create_pr=create_pr,
            timeout=timeout,
            continue_on_failure=continue_on_failure,
            dry_run=dry_run,
        )

        # Convert results to a list of dictionaries.
        execution_results[name] = [asdict(result) for result in execution_results[name]]

        # Save intermediate results to a JSON file.
        save_json(data=execution_results, json_file=OUTPUT_FILE, indent=4, sort_keys=False)

    return execution_results


if __name__ == "__main__":
    main()
