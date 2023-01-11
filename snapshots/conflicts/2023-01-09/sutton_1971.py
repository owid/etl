"""Get our manually curated dataset into Snapshot.

We maintain a manually curated transcription of the data from Sutton (1971) in a Google Spreadsheet.
Then, port this file to Snapshot. To obtain this file, download it as a CSV from
https://docs.google.com/spreadsheets/d/1Xo5e8aiFJnmv_aasFXU8MtzizSU0LxiysoUuErEJx3Q/edit?usp=sharing and use
it with the `--path-to-file` argument."""

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
    snap = Snapshot(f"conflicts/{SNAPSHOT_VERSION}/sutton_1971.csv")
    # Ensure destination folder exists.
    snap.path.parent.mkdir(exist_ok=True, parents=True)
    # Copy local data file to snapshots data folder.
    snap.path.write_bytes(Path(path_to_file).read_bytes())
    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
