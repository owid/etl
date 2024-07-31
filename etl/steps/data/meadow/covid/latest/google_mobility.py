"""Load a snapshot and create a meadow dataset."""

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

cols = [
    "retail_and_recreation_percent_change_from_baseline",
    "grocery_and_pharmacy_percent_change_from_baseline",
    "parks_percent_change_from_baseline",
    "transit_stations_percent_change_from_baseline",
    "workplaces_percent_change_from_baseline",
    "residential_percent_change_from_baseline",
    "census_fips_code",
    "country_region",
    "sub_region_1",
    "sub_region_2",
    "metro_area",
    "iso_3166_2_code",
    "date",
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("google_mobility.csv")

    # Load data from snapshot.
    tb = snap.read(usecols=cols)

    #
    # Process data.
    #
    tb = tb.rename(columns={"country_region": "country"})
    tb = set_dtypes(tb)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "date", "sub_region_1", "sub_region_2", "metro_area"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def set_dtypes(tb: Table) -> Table:
    tb["date"] = pd.to_datetime(tb["date"])
    col_str = [
        "country",
        "sub_region_1",
        "sub_region_2",
        "metro_area",
        "iso_3166_2_code",
    ]
    tb[col_str] = tb[col_str].astype("string")
    return tb
