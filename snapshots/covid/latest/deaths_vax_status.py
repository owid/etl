"""Script to create a snapshot of dataset.

This data was downloaded from Grapher. It had been imported to Grapher before covid-19-data repository was created.
"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--england", default=None, type=str, help="Path to ICL local data file.")
@click.option("--us", default=None, type=str, help="Path to IHME local data file.")
@click.option("--switzerland", default=None, type=str, help="Path to LSHTM local data file.")
@click.option("--chile", default=None, type=str, help="Path to Youyang Gu local data file.")
def main(england: str, us: str, switzerland: str, chile: str, upload: bool) -> None:
    estimates = [
        ("england", england),
        ("us", us),
        ("switzerland", switzerland),
        ("chile", chile),
    ]
    # Create a new snapshots.
    for estimate in estimates:
        name = estimate[0]
        filename = estimate[1]

        if filename is not None:
            snap = Snapshot(f"covid/{SNAPSHOT_VERSION}/deaths_vax_status_{name}.csv")
            # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
            snap.create_snapshot(filename=filename, upload=upload)


if __name__ == "__main__":
    main()
