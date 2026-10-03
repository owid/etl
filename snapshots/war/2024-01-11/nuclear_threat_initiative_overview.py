"""Script to create a snapshot of dataset.

We do not extract any data from NTI directly. Instead, we visually inspect
https://www.nti.org/countries/
And manually create a table of countries' status on nuclear weapons from 2017 onwards, to complement the data provided
by Philipp C. Bleek, "When Did (and Didn't) States Proliferate? Chronicling the Spread of Nuclear Weapons".

"""

from pathlib import Path

import click
import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Latest year for which NTI has data.
LATEST_YEAR = 2023


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"war/{SNAPSHOT_VERSION}/nuclear_threat_initiative_overview.csv")

    # Manually create a table of countries' status on nuclear weapons from 2017 onwards, which should be considered as
    # a continuation of Bleek (2017).
    # NOTE: Currently, no country has changed its status since 2016.
    data = [
        ("Algeria", 2017, 0),
        ("Argentina", 2017, 0),
        ("Australia", 2017, 0),
        ("Brazil", 2017, 0),
        ("China", 2017, 3),
        ("Egypt", 2017, 0),
        ("France", 2017, 3),
        ("Germany", 2017, 0),
        ("India", 2017, 3),
        ("Indonesia", 2017, 0),
        ("Iran", 2017, 2),
        ("Iraq", 2017, 0),
        ("Israel", 2017, 3),
        ("Italy", 2017, 0),
        ("Japan", 2017, 0),
        ("North Korea", 2017, 3),
        ("Libya", 2017, 0),
        ("Norway", 2017, 0),
        ("Pakistan", 2017, 3),
        ("Romania", 2017, 0),
        ("Russia", 2017, 3),
        ("South Africa", 2017, 0),
        ("South Korea", 2017, 0),
        ("Sweden", 2017, 0),
        ("Switzerland", 2017, 0),
        ("Syria", 2017, 1),
        ("Taiwan", 2017, 0),
        ("United Kingdom", 2017, 3),
        ("United States", 2017, 3),
        ("Serbia", 2017, 0),
    ]
    df_latest = pd.DataFrame.from_records((data), columns=["country", "year", "status"])
    # Since nothing has changed, repeat data for every year.
    df = df_latest.copy()
    for year in range(2018, LATEST_YEAR + 1):
        df = pd.concat([df, df_latest.assign(**{"year": year})], ignore_index=True)

    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    main()
