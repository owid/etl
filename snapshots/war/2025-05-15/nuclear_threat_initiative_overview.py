"""Script to create a snapshot of dataset.

The data is manually added in the table below, to complement the data provided by Philipp C. Bleek, "When Did (and Didn't) States Proliferate? Chronicling the Spread of Nuclear Weapons" (whose last update was 2017) from 2017 onwards.

1. Go to the NTI website https://www.nti.org/countries/
2. Open a tab for each country, inspect the "nuclear" section, and update the table below using one of the following statuses:
    0: Not considering nuclear weapons
    1: Considering
    2: Pursuing
    3: Possessing

########################################################################################################################
# TODO: Complete the inspection. For now, keep the same data as in the last update.
Alternatively, consider checking out https://banmonitor.org/profiles/
########################################################################################################################

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

    # Given that the page is regularly updated, assume publication date is the same as the date accessed.
    snap.metadata.origin.date_published = snap.metadata.origin.date_accessed
    # Update snapshot metadata.
    snap.metadata.save()

    # Create snapshot.
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    main()
