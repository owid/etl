"""Run many snapshots, time how long they took, and which ones failed.

RESULTS:
This script attempted to execute 440 snapshots, of which:
* 20% failed.
* 4% took longer than 100s (and where therefore interrupted).
* 86% were executed successfully.

All results are stored in the accompanying file: snapshot_execution_times.json

"""

import json
import random
import subprocess
import time

from owid.datautils.io import save_json
from structlog import get_logger
from tqdm.auto import tqdm

from etl.paths import BASE_DIR, SNAPSHOTS_DIR

log = get_logger()

SNAPSHOT_SCRIPTS = sorted(list((BASE_DIR / "snapshots").glob("*/*/*.py")))
random.shuffle(SNAPSHOT_SCRIPTS)
# Kill a subprocess if it takes longer than this many seconds.
TIMEOUT = 100
OUTPUT_FILE = BASE_DIR / "etl" / "scripts" / "archive" / "run_all_snapshots" / "snapshot_execution_times.json"


def main():
    # Create a dictionary to store the execution results: duration (in seconds) or "FAILED"
    if OUTPUT_FILE.exists():
        # Load existing results from the JSON file to allow resuming.
        with open(OUTPUT_FILE, "r") as f:
            execution_results = json.load(f)
    else:
        # Initialize dictionary of results.
        execution_results = {}

    # Loop over all snapshot scripts.
    for snapshot_script in tqdm(SNAPSHOT_SCRIPTS):
        snapshot_script_relative = str(snapshot_script.relative_to(SNAPSHOTS_DIR))

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
