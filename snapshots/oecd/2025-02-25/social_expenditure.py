"""
Script to create a snapshot of the OECD Social Expenditure Database (SOCX).

STEPS TO OBTAIN THE DATA:
    1. Go to the OECD Data Explorer: https://data-explorer.oecd.org/
    2. In the section "Society", select "Social policy".
    3. Select "Social expenditure aggregates".
    4. Select these filters:
        1. Time period: all years available. Select "----" as the start year and "----" as the end year.
        2. Reference area: all countries available. By default, all countries are selected (check if it is not).
        3. Unit of measure: select
            - US dollars per person, PPP converted
            - Percentage of GDP
            - Percentage of general government expenditure
        4. Expenditure source: select all the options available. Pick the options one by one.
        5. Spending type: select all the options available. Pick the options one by one.
        6. Programme type: select all the options available. Pick the options one by one.
        7. Price base: select "Constant prices" and "Not applicable".
    5. Click the Download button.
    6. Select "Filtered data in tabular text (CSV)".
    7. Just for convenience, copy the file to this directory and rename it social_expenditure.csv.
    8. Run the script with the `--path-to-file` option:
        ```
        python snapshots/oecd/{version}/social_expenditure.py --path-to-file <path_to_file>
        ```

"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file", "-f", prompt=True, type=str, help="Path to local data file.")
def main(path_to_file: str, upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"oecd/{SNAPSHOT_VERSION}/social_expenditure.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
