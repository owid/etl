"""Script to create a snapshot of dataset.

Confirmed measles cases among US residents by state, behind the map on CDC's Measles Cases and Outbreaks page
(https://www.cdc.gov/measles/data-research/index.html).

NOTE: www.cdc.gov returns 403 to browser-like user agents (which the generic .dvc download uses), but a plain request works.
"""

from pathlib import Path

import requests

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


def run(upload: bool = True) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"cdc/{SNAPSHOT_VERSION}/measles_cases_by_state.json")

    # Download data, add file to DVC and upload to S3.
    response = requests.get(snap.metadata.origin.url_download)  # type: ignore[union-attr]
    response.raise_for_status()
    snap.path.parent.mkdir(parents=True, exist_ok=True)
    snap.path.write_bytes(response.content)
    snap.create_snapshot(filename=snap.path, upload=upload)
