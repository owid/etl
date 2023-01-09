"""Get our manually curated dataset.

This dataset was obtained by examining the data provided by the source, and transcribing it into a Google Spreadsheet. It can be found
at https://docs.google.com/spreadsheets/d/1Xo5e8aiFJnmv_aasFXU8MtzizSU0LxiysoUuErEJx3Q/edit?usp=sharing."""

from pathlib import Path

import click

from etl.snapshot import Snapshot

SNAPSHOT_VERSION = "2023-01-09"


@click.command()
@click.option("--path-to-file", prompt=True, type=str, help="Path to local data file.")
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(path_to_file: str, upload: bool) -> None:
    # Create new snapshot.
    snap = Snapshot(f"conflicts/{SNAPSHOT_VERSION}/sorokin_1937.csv")
    # Ensure destination folder exists.
    snap.path.parent.mkdir(exist_ok=True, parents=True)
    # Copy local data file to snapshots data folder.
    snap.path.write_bytes(Path(path_to_file).read_bytes())
    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
