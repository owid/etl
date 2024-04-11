"""
Script to create a snapshot of the OECD Income Distribution Database.

The download link should be working automatically, but if not, you can follow these steps

STEPS TO OBTAIN THE DATA:
    1. Go to the OECD Data Explorer: https://data-explorer.oecd.org/
    2. In the section "Society", select "Inequality".
    3. Select "Income distribution database".
    4. Select these filters:
        1. Time period: all years available. Select "----" as the start year and "----" as the end year.
        2. Reference area: all countries available. By default, all countries are selected (check if it is not).
        3. Measure: select
            - Poverty rate based on disposable income
            - Gini (disposable income)
            - Quintile share ratio (disposable income)
            - Palma ratio (disposable income)
            - P90/P10 disposable income decile ratio
            - P90/P50 disposable income decile ratio
            - P50/P10 disposable income decile ratio
            - Poverty rate based on market income
            - Gini (market income)
            - Gini (gross income)
        4. Statistical operation: all the options available, which is the same as leaving them unselected.
        5. Unit of measure: all the options available, which is the same as leaving them unselected.
        6. Age: select "Total", "From 18 to 65 years" and "Over 65 years".
        7. Methodology: select "Income definition since 2012".
        8. Definition: select "Current definition"
        9. Poverty line: all the options available, which is the same as leaving them unselected.
    5. Click the Download button.
    6. Right click on "Filtered data in tabular text (CSV)".
    7. Select "Copy link address".
    8. Paste the link in the `url_download` field in the income_distribution_database.csv.dvc file.
    9. Run
        python snapshots/oecd/{version}/income_distribution_database.py

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
    snap = Snapshot(f"oecd/{SNAPSHOT_VERSION}/income_distribution_database.csv")

    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    main()
