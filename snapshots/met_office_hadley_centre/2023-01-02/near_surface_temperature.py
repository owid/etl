"""Ingest snapshot of the HadCRUT5 near surface temperature dataset (temperature anomaly) by Met Office Hadley Centre.

The HadCRUT5 near surface temperature data set is produced by blending data from the CRUTEM5 surface air temperature
dataset and the HadSST4 sea-surface temperature dataset.
"""

import pathlib

import click

from etl.snapshot import Snapshot

CURRENT_DIR = pathlib.Path(__file__).parent
SNAPSHOT_VERSION = "2023-01-02"
FILE_NAMES = [
    "near_surface_temperature_global.csv",
    "near_surface_temperature_northern_hemisphere.csv",
    "near_surface_temperature_southern_hemisphere.csv",
]


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    for file_name in FILE_NAMES:
        snap = Snapshot(f"met_office_hadley_centre/{SNAPSHOT_VERSION}/{file_name}")
        snap.download_from_source()
        snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
