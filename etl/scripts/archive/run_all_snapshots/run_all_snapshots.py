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
import subprocess
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional

import click
import yaml
from owid.datautils.io import save_json
from structlog import get_logger
from tqdm.auto import tqdm

from etl.dag_helpers import get_active_snapshots
from etl.paths import BASE_DIR, SNAPSHOTS_DIR
from etl.snapshot import Snapshot

log = get_logger()

# Kill a subprocess if it takes longer than this many seconds.
OUTPUT_FILE = BASE_DIR / "etl" / "scripts" / "archive" / "run_all_snapshots" / "snapshot_execution_times.json"


@dataclass
class ExecutionResult:
    """Class to store the execution result of a snapshot."""

    name: str
    status: str = ""
    identical: bool = False
    duration: float = 0.0


def run_snapshot(
    snapshot: str,
    snapshot_script: Path,
    dvc_files: list[Path],
    timeout: Optional[int] = None,
    continue_on_failure: bool = False,
    dry_run: bool = False,
) -> ExecutionResult:
    """Update all snapshots in a group."""

    # Start timer.
    start_time = time.time()

    dvc_file = dvc_files[0]
    exec_result = ExecutionResult(name=snapshot)

    if dry_run:
        log.info(f"[DRY-RUN] Would execute {snapshot}.")
        exec_result.status = "DRY-RUN"
        return exec_result

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
        subprocess.run(["python", snapshot_script, "--upload"], check=True, capture_output=True, text=True, **kwargs)

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

    return exec_result


@click.command()
@click.option("--dry-run", is_flag=True, help="Run the script in dry-run mode.")
@click.option("--filter", type=str, help="Process only snapshots that include the given substring.")
@click.option(
    "--timeout",
    type=int,
    default=None,
    help="Timeout in seconds for each snapshot.",
)
@click.option("--skip", is_flag=True, help="Skip snapshots that have already been processed.")
@click.option("--continue-on-failure", is_flag=True, help="Continue running snapshots even if some fail.")
def main(
    dry_run: bool,
    filter: str,
    timeout: Optional[int],
    skip: bool,
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

    # Loop over all snapshot scripts.
    for snapshot in tqdm(active_snapshots):
        snapshot_script = SNAPSHOTS_DIR / snapshot

        # Find .dvc file belonging to the snapshot script
        dvc_files = list(snapshot_script.parent.glob(f"{snapshot_script.stem}.*.dvc"))
        assert len(dvc_files) >= 1, f"Expected to find at least one .dvc file for {snapshot}"

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

        execution_result = run_snapshot(
            snapshot=snapshot,
            snapshot_script=snapshot_script,
            dvc_files=dvc_files,
            timeout=timeout,
            continue_on_failure=continue_on_failure,
            dry_run=dry_run,
        )

        # Convert results to a list of dictionaries.
        execution_results[snapshot] = asdict(execution_result)

        # Save intermediate results to a JSON file.
        save_json(data=execution_results, json_file=OUTPUT_FILE, indent=4, sort_keys=False)

    return execution_results


if __name__ == "__main__":
    main()
