"""
Script to create a snapshot of the decile ratios from the OECD Data Explorer.

The download link should be working automatically, but if not, you can follow these steps

STEPS TO OBTAIN THE DATA:
    1. Go to the OECD Data Explorer: https://data-explorer.oecd.org/
    2. In the section "Employment", select "Benefits, earnings and wages".
    3. Select "Decile ratios of gross earnings".
    4. Select these filters:
        1. Time period: all years available. Select "----" as the start year and "----" as the end year.
        2. Reference area: all countries available. By default, all countries are selected (check if it is not).
        3. Unit of measure: all the options available, which is the same as leaving them unselected.
        4. Aggregation operation: all the options available, which is the same as leaving them unselected.
        5. Sex: select "Total".
    5. Click the Download button.
    6. Right click on "Filtered data in tabular text (CSV)".
    7. Select "Copy link address".
    8. Paste the link in the `url_download` field in the income_distribution_database.csv.dvc file.
    9. Run
        python snapshots/oecd/{version}/decile_ratios.py

"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"oecd/{SNAPSHOT_VERSION}/decile_ratios.csv")

    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    main()
