"""Script to create a snapshot of dataset.

Data must be downloaded manually from here: https://www.ethnologue.com/codes/Language_Code_Data_20260217.zip

Available at: https://www.ethnologue.com/codes/

The site blocks automated requests (HTTP 403 for both browser-like and plain user agents), so download the file in a browser and run:

    etls language/2026-09-24/ethnologue --path-to-file ~/Downloads/Language_Code_Data_20260217.zip
"""

from pathlib import Path

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


def run(upload: bool = True, path_to_file: str | None = None) -> None:
    assert path_to_file, "Download the zip file manually (see module docstring) and pass it via --path-to-file."

    # Create a new snapshot.
    snap = Snapshot(f"language/{SNAPSHOT_VERSION}/ethnologue.zip")

    # Add the local file to DVC and upload to S3.
    snap.create_snapshot(upload=upload, filename=path_to_file)
