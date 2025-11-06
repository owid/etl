"""Script to create a snapshot of dataset 'UCDP Candidate Events Dataset'.

The UCDP Candidate Events Dataset (UCDP Candidate) is based on UCDP Georeferenced Event Dataset (UCDP GED), but published at a monthly release cycle with preliminary data. See codebook for similarities and differences between the two products. We import this data every quarter (see notes and instructions below for more details).

Go to https://ucdp.uu.se/downloads/index.html#candidate to find latest available versions.

NOTE:
- Currently we aim at combining the CED dataset with latest UCDP stable dataset (yearly release) so that we can have recent data.
- The scheduled updates of CED are defined in https://github.com/owid/owid-issues/issues/2033:
    - February: Add preliminary data of previous year (full year)
    - May: Add Q1 (Jan-Mar) data of current year
    - August: Add Q2 (Apr-Jun) data of current year
    - November: Add Q3 (Jul-Sep) data of current year


INSTRUCTIONS TO UPDATE SNAPSHOT:
    1. Go to the DVC file that is currently in use in the pipeline, copy it and create a new one.

    2. Modify its name and edit the fields to reflect the new version (dates, producer's version, links, etc.).


    3. Update the value of VERSIONS in this script.
        - Variable VERSIONS is a list in case we need to add multiple versions in the future. However, it generally should only contain one version (corresponding to the DVC's that you created in 2), which is the latest cumulated preview data released in the year (e.g. Q1, or Q1+Q2, etc.)

    4. Run the script to download the latest version of the dataset and upload it to Snapshot. python snapshots/war/latest/ucdp_ced.py


    5. Update the war.yml DAG dependency of `meadow/war/latest/ucdp_ced` to reflect the new version(s) of the DVC file.

    6. Go to the Meadow step (`meadow/war/latest/ucdp_ced`) , update the snap.read(...), run it

    7. Go to the Garden step (`garden/war/latest/ucdp_preview`), update `LAST_YEAR`, `LAST_YEAR_PREVIEW` variables if applicable, and run it. Probably you will need to also update `NUM_MISSING_LOCATIONS`.

    8. Go back to the GitHub issue to address further steps that concern chart and Grapher updates (e.g. subtitle edits).
"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

VERSIONS = [
    "v25_01_25_09",
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
