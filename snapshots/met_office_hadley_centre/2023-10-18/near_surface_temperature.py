"""Ingest snapshot of the HadCRUT5 near surface temperature dataset (temperature anomaly) by Met Office Hadley Centre.

The HadCRUT5 near surface temperature data set is produced by blending data from the CRUTEM5 surface air temperature
dataset and the HadSST4 sea-surface temperature dataset.
"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Names of input data files to create snapshots for.
DATA_FILES = [
    "near_surface_temperature_global.csv",
    "near_surface_temperature_northern_hemisphere.csv",
    "near_surface_temperature_southern_hemisphere.csv",
]

# Define common metadata fields (to be written to dvc files).
CITATION_FULL = """Morice, C.P., J.J. Kennedy, N.A. Rayner, J.P. Winn, E. Hogan, R.E. Killick, R.J.H. Dunn, T.J. Osborn, P.D. Jones and I.R. Simpson (in press) An updated assessment of near-surface temperature change from 1850: the HadCRUT5 dataset. Journal of Geophysical Research (Atmospheres) [doi:10.1029/2019JD032361](https://www.metoffice.gov.uk/hadobs/hadcrut5/HadCRUT5_accepted.pdf) ([supporting information](https://www.metoffice.gov.uk/hadobs/hadcrut5/HadCRUT5_supporting_information_accepted.pdf))."""
DESCRIPTION = """The HadCRUT5 near surface temperature data set is produced by blending data from the CRUTEM5 surface air temperature dataset and the HadSST4 sea-surface temperature dataset.\n\nTemperature anomalies are based on the HadCRUT5 near-surface temperature dataset as published by the Met Office Hadley Centre."""


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    # Create a new snapshot for each dataset.
    for data_file in DATA_FILES:
        snap = Snapshot(f"met_office_hadley_centre/{SNAPSHOT_VERSION}/{data_file}")

        # Replace the full citation and description in the metadata.
        snap.metadata.origin.citation_full = CITATION_FULL  # type: ignore
        snap.metadata.origin.description = DESCRIPTION  # type: ignore

        # Rewrite metadata to dvc file.
        snap.metadata_path.write_text(snap.metadata.to_yaml())

        # Download data from source, add file to DVC and upload to S3.
        snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    main()
