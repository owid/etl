"""Snapshot of Clark et al. (2022) "Environmental impacts of food" — Our World in Data's
curated 211-product roll-up, mirrored from the legacy `owid-datasets` GitHub repo.

This is the data that has historically backed the "Specific food products" view of the
food-footprints explorer. We bring it under ETL so the explorer can be authored as a
single ETL-managed `export://explorers/food/latest/food_footprints` step.
"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    snap = Snapshot(f"food/{SNAPSHOT_VERSION}/environmental_impacts_of_food_clark_et_al_2022.csv")
    snap.create_snapshot(upload=upload)
