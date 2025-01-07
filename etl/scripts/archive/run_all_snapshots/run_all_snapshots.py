"""Run many snapshots, time how long they took, and which ones failed.

RESULTS:
This script attempted to execute 440 snapshots, of which:
* 20% failed.
* 4% took longer than 100s (and where therefore interrupted).
* 86% were executed successfully.

All results are stored in the accompanying file: snapshot_execution_times.json

"""

import json
import subprocess
import time
from typing import Set

import click
from owid.datautils.io import save_json
from structlog import get_logger
from tqdm.auto import tqdm

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
    for snapshot_script_relative in tqdm(active_snapshots):
        snapshot_script = SNAPSHOTS_DIR / snapshot_script_relative

        if str(snapshot_script) in execution_results:
            log.info(f"Skipping {snapshot_script} because it has already been processed.")
            continue

        # Start timer.
        start_time = time.time()

        # Read the script file.
        with open(snapshot_script, "r") as f:
            snapshot_text = f.read()

        # Skip script files that are not snapshots (or at least do not have an "--upload" flag explicitly defined).
        if "--upload" not in snapshot_text:
            log.info(f"Skipping {snapshot_script} because it does not have --upload flag.")
            continue
        # Skip scripts that require the use of a local file.
        if ("--path-to-file" in snapshot_text) or ("-f " in snapshot_text):
            log.info(f"Skipping {snapshot_script} because it requires a local file.")
            continue

        execution_results[snapshot_script_relative] = {}

        if dry_run:
            log.info(f"[DRY-RUN] Would execute {snapshot_script_relative}.")
            execution_results[snapshot_script_relative] = {"status": "DRY-RUN"}
            save_json(data=execution_results, json_file=OUTPUT_FILE, indent=4, sort_keys=False)
            continue

        try:
            # Try to execute snapshot.
            result = subprocess.run(
                ["python", snapshot_script, "--upload"], check=True, capture_output=True, timeout=TIMEOUT, text=True
            )

            execution_results[snapshot_script_relative]["status"] = "SUCCESS"

            # Data is not new, MD5 is identical.
            execution_results[snapshot_script_relative]["identical"] = (
                "File already exists with the same md5" in result.stdout
            )

            # Add duration time for successfully executed snapshot.
            duration = time.time() - start_time
            execution_results[snapshot_script_relative]["duration"] = duration
        except subprocess.TimeoutExpired:
            # Stop snapshot that is taking too long and mark it as timeout.
            execution_results[snapshot_script_relative]["status"] = "TIMEOUT"
            log.error(f"Timeout expired for {snapshot_script_relative}. Marking as 'TIMEOUT'.")
        except subprocess.CalledProcessError:
            # Mark snapshot as failed.
            execution_results[snapshot_script_relative]["status"] = "FAILED"
            log.error(f"Error executing {snapshot_script_relative}. Marking as 'FAILED'.")

        # Save the current results to a JSON file.
        save_json(data=execution_results, json_file=OUTPUT_FILE, indent=4, sort_keys=False)

    return execution_results


if __name__ == "__main__":
    main()
