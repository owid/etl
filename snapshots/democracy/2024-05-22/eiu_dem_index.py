"""The data from the EIU is not shared cleanly. Instead, the EIU shares a single-year PDF report every year.

To overcome this we have:

- 2021-2023: Transcribed each PDF report into a CSV file.
- 2006-2020: Sourced the data from Gapminder, who have themselves compiled the data from the EIU reports.

All these trancriptions and imports are saved in a Google sheet: https://docs.google.com/spreadsheets/d/1902iwPdR-PKjmpONceb1u9h2GzR-9Kzac4C9cnNDcHo/edit?usp=sharing.

To run this step, download each of the sheets in there (except README) and pass their local paths to this script.

NOTES: To get the data from EIU, you need to submit a form with few details. They'll send the data via e-mail.
"""

from pathlib import Path
from typing import Optional

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file-2006-2020", type=str, help="Path to local data file.")
@click.option("--path-to-file-2021", type=str, help="Path to local data file.")
@click.option("--path-to-file-2022", type=str, help="Path to local data file.")
@click.option("--path-to-file-2023", type=str, help="Path to local data file.")
def main(
    path_to_file_2006_2020: Optional[str],
    path_to_file_2021: Optional[str],
    path_to_file_2022: Optional[str],
    path_to_file_2023: Optional[str],
    upload: bool,
) -> None:
    # Assign short names to each file path
    snaps = [
        (path_to_file_2006_2020, "eiu_gapminder"),
        (path_to_file_2021, "eiu_2021"),
        (path_to_file_2022, "eiu_2022"),
        (path_to_file_2023, "eiu_2023"),
    ]
    # Create Snapshot for each file path and push to catalog
    for path, short_name in snaps:
        if path is not None:
            # Create a new snapshot.
            snap = Snapshot(f"democracy/{SNAPSHOT_VERSION}/{short_name}.csv")
            # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
            snap.create_snapshot(filename=path, upload=upload)


if __name__ == "__main__":
    main()
