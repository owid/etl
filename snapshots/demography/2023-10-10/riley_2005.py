"""The data for this snapshot was manually extracted from a PDF and transcribed into a CSV.

Steps to reproduce:

    1. Visit the website hosting the paper: https://u.demog.berkeley.edu/~jrw/Biblio/Eprints/%20P-S/riley.2005_estimates.global.e0.pdf
    2. Check the table 1, in page 3.
    3. Transcribe it into a CSV file.
    4. To transcribe it, basically copy-paste values and years.
        - When the years are ranges, use the middle year.
        - Output generated file is attached in this folder (`riley_2005.csv`)
"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
SNAPSHOT_FILE = Path(__file__).parent / "riley_2005.csv"


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"demography/{SNAPSHOT_VERSION}/riley_2005.pdf")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=SNAPSHOT_FILE, upload=upload)


if __name__ == "__main__":
    main()
