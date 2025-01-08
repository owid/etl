"""Run many snapshots, time how long they took, and which ones failed.

RESULTS:
This script attempted to execute 440 snapshots, of which:
* 20% failed.
* 4% took longer than 100s (and where therefore interrupted).
* 86% were executed successfully.

All results are stored in the accompanying file: snapshot_execution_times.json

"""

import hashlib
import json
import subprocess
import time
from typing import Set

import click
import requests
from git import GitCommandError, Repo
from owid.datautils.io import save_json
from structlog import get_logger
from tqdm.auto import tqdm

from etl.config import GITHUB_API_URL, GITHUB_TOKEN
from etl.paths import BASE_DIR, SNAPSHOTS_DIR
from etl.steps import load_dag

log = get_logger()

# Kill a subprocess if it takes longer than this many seconds.
TIMEOUT = 100
OUTPUT_FILE = BASE_DIR / "etl" / "scripts" / "archive" / "run_all_snapshots" / "snapshot_execution_times.json"


def get_active_snapshots() -> Set[str]:
    DAG = load_dag()

    active_snapshots = set()

    for s in set(DAG.keys()) | {x for v in DAG.values() for x in v}:
        if s.startswith("snapshot"):
            active_snapshots.add(s.split("://")[1])

    # Strip extension
    return {s.split(".")[0] + ".py" for s in active_snapshots}


def create_autoupdate_pr(snapshot: str) -> None:
    # Create branch name
    name = snapshot.split("/")[-1].replace(".py", "").replace("_", "-")
    snapshot_hash = hashlib.md5(snapshot.encode()).hexdigest()[:3]
    branch_name = f"auto-{name}-{snapshot_hash}"

    # Create PR title
    title = f"🤖: Auto-update {snapshot.replace('.py', '')}"

    # Init repo
    repo = Repo(BASE_DIR)

    # Add all files from snapshot_dir to git
    snapshot_dir = (SNAPSHOTS_DIR / snapshot).parent

    # Stash changes
    try:
        repo.git.stash("save", "--include-untracked")
        stashed = True
    except GitCommandError:
        stashed = False

    # Check-out to a new branch
    repo.git.checkout("master")
    try:
        repo.git.checkout("-b", branch_name)
    except GitCommandError:
        repo.git.checkout(branch_name)

    if stashed:
        repo.git.stash("pop")

    repo.git.add(snapshot_dir)
    commit_msg = f"Update snapshot {snapshot}"
    try:
        repo.git.commit("-m", commit_msg)
        repo.git.push("--set-upstream", "origin", branch_name)
    except GitCommandError as e:
        raise e

    # Create a PR
    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
    data = {
        "title": title,
        "head": branch_name,
        "base": "master",
        "body": "",
    }

    response = requests.post(GITHUB_API_URL, json=data, headers=headers)
    if response.status_code == 201:
        js = response.json()
        log.info(f"Pull request created: {js['html_url']}")
    else:
        raise click.ClickException(f"Failed to create pull request:\n{response.json()}")


@click.command()
@click.option("--dry-run", is_flag=True, help="Run the script in dry-run mode.")
@click.option("--filter", type=str, help="Process only snapshots that include the given substring.")
def main(dry_run, filter):
    # Create a dictionary to store the execution results: duration (in seconds) or "FAILED"
    if OUTPUT_FILE.exists():
        # Load existing results from the JSON file to allow resuming.
        with open(OUTPUT_FILE, "r") as f:
            execution_results = json.load(f)
    else:
        # Initialize dictionary of results.
        execution_results = {}

    active_snapshots = get_active_snapshots()
    if filter:
        active_snapshots = {s for s in active_snapshots if filter in s}

    # Loop over all snapshot scripts.
    for snapshot in tqdm(active_snapshots):
        snapshot_script = SNAPSHOTS_DIR / snapshot

        if str(snapshot_script) in execution_results:
            log.info(f"Skipping {snapshot} because it has already been processed.")
            continue

        # Start timer.
        start_time = time.time()

        # Read the script file.
        with open(snapshot_script, "r") as f:
            snapshot_text = f.read()

        # Skip script files that are not snapshots (or at least do not have an "--upload" flag explicitly defined).
        if "--upload" not in snapshot_text:
            log.info(f"Skipping {snapshot} because it does not have --upload flag.")
            continue
        # Skip scripts that require the use of a local file.
        if ("--path-to-file" in snapshot_text) or ("-f " in snapshot_text):
            log.info(f"Skipping {snapshot} because it requires a local file.")
            continue

        execution_results[snapshot] = {}

        if dry_run:
            log.info(f"[DRY-RUN] Would execute {snapshot}.")
            execution_results[snapshot] = {"status": "DRY-RUN"}
            save_json(data=execution_results, json_file=OUTPUT_FILE, indent=4, sort_keys=False)
            continue

        try:
            log.info(f"Executing {snapshot}.")

            # Try to execute snapshot.
            result = subprocess.run(
                ["python", snapshot_script, "--upload"], check=True, capture_output=True, timeout=TIMEOUT, text=True
            )

            execution_results[snapshot]["status"] = "SUCCESS"

            # Data is not new, MD5 is identical.
            execution_results[snapshot]["identical"] = "File already exists with the same md5" in result.stdout

            # Add duration time for successfully executed snapshot.
            duration = time.time() - start_time
            execution_results[snapshot]["duration"] = duration

            # If MD5 has changed, create a PR.
            if not execution_results[snapshot]["identical"]:
                create_autoupdate_pr(snapshot, dry_run=dry_run)

        except subprocess.TimeoutExpired:
            # Stop snapshot that is taking too long and mark it as timeout.
            execution_results[snapshot]["status"] = "TIMEOUT"
            log.error(f"Timeout expired for {snapshot}. Marking as 'TIMEOUT'.")
        except subprocess.CalledProcessError:
            # Mark snapshot as failed.
            execution_results[snapshot]["status"] = "FAILED"
            log.error(f"Error executing {snapshot}. Marking as 'FAILED'.")

        # Save the current results to a JSON file.
        save_json(data=execution_results, json_file=OUTPUT_FILE, indent=4, sort_keys=False)

    return execution_results


if __name__ == "__main__":
    main()
