"""Script to create a snapshot of dataset 'UCDP Candidate Events Dataset'.

The UCDP Candidate Events Dataset (UCDP Candidate) is based on UCDP Georeferenced Event Dataset (UCDP GED), but published at a monthly release cycle. It makes available monthly releases of candidate events data with not more than a month’s lag globally. See codebook for similarieties and differences between the two products.

Go to https://ucdp.uu.se/downloads/index.html#candidate to find latest available versions.
"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

VERSIONS = [
    "v24_01_24_12",
]


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    for version in VERSIONS:
        snapshot_path = f"war/{SNAPSHOT_VERSION}/ucdp_ced_{version}.csv"
        snap = Snapshot(snapshot_path)
        snap.download_from_source()
        snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
