"""Script to create a snapshot of dataset."""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--icl", default=None, type=str, help="Path to ICL local data file.")
@click.option("--ihme", default=None, type=str, help="Path to IHME local data file.")
@click.option("--lshtm", default=None, type=str, help="Path to LSHTM local data file.")
@click.option("--youyang", default=None, type=str, help="Path to Youyang Gu local data file.")
def main(icl: str, ihme: str, lshtm: str, youyang: str, upload: bool) -> None:
    estimates = [
        ("icl", icl),
        ("ihme", ihme),
        ("lshtm", lshtm),
        ("youyang", youyang),
    ]
    # Create a new snapshots.
    for estimate in estimates:
        name = estimate[0]
        filename = estimate[1]

        if filename is not None:
            snap = Snapshot(f"covid/{SNAPSHOT_VERSION}/infections_model_{name}.csv")
            # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
            snap.create_snapshot(filename=filename, upload=upload)


if __name__ == "__main__":
    main()
