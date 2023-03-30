"""Get our manually curated dataset into Snapshot.

We maintain a manually curated transcription of the data from Bouthoul and Carrere (1978) in a Google Spreadsheet.
Then, port this file to Snapshot. To obtain this file, download it as a CSV from
https://docs.google.com/spreadsheets/d/1JXkz05kb_gkGXuvdnI21B6lhHIVZJD5BaiEOZi8mqRk/edit?usp=sharing and use
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
    snap = Snapshot(f"war/{SNAPSHOT_VERSION}/bouthoul_carrere_1978.csv")
    # Ensure destination folder exists.
    snap.path.parent.mkdir(exist_ok=True, parents=True)
    # Copy local data file to snapshots data folder.
    snap.path.write_bytes(Path(path_to_file).read_bytes())
    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
