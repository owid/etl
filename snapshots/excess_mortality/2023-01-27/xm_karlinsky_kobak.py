"""Script to create a snapshot of dataset 'Excess mortality during the COVID-19 pandemic'."""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    # Data aggregated on all ages
    snap = Snapshot(f"excess_mortality/{SNAPSHOT_VERSION}/xm_karlinsky_kobak.csv")
    snap.download_from_source()
    snap.dvc_add(upload=upload)

    # Data broken down by ages
    snap = Snapshot(f"excess_mortality/{SNAPSHOT_VERSION}/xm_karlinsky_kobak.ages.csv")
    snap.download_from_source()
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
