"""Load a snapshot and create a meadow dataset."""

from typing import cast

import numpy as np
from owid.catalog import Table
from owid.catalog.processing import concat

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Columns rename
COLUMNS_RENAME = {"location": "country"}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("vaccinations_global.csv")
    snap_who = paths.load_snapshot("vaccinations_global_who.csv")

    # Load data from snapshot.
    tb = snap.read()
    tb_who = snap_who.read()

    #
    # Process data.
    #
    # Process main data
    tb = tb[
        [
            "date",
            "location",
            "total_vaccinations",
            "people_vaccinated",
            "people_fully_vaccinated",
            "total_boosters",
        ]
    ]
    tb = cast(Table, tb.rename(columns=COLUMNS_RENAME))
    tb = set_dtypes(tb)

    # Process WHO data
    tb_who = process_who_data(tb_who)

    # Combine
    tb = add_who_data(tb, tb_who)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "date"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def process_who_data(tb: Table) -> Table:
    """Process data from WHO."""
    # Keep rows that 'make sense'
    tb = filter_data_who(tb)
    # Calculate metrics
    tb = calculate_metrics_who(tb)
    # Rename columns
    tb = tb.rename(
        columns={
            "DATE_UPDATED": "date",
            "COUNTRY": "country",
        }
    )
    # Keel relevant columns
    tb = tb.loc[
        :,
        [
            "date",
            "country",
            "total_vaccinations",
            "people_vaccinated",
            "people_fully_vaccinated",
            "total_boosters",
        ],
    ]
    # Dtypes
    tb = set_dtypes(tb)

    return tb


def filter_data_who(tb: Table) -> Table:
    """Get valid entries.

    - Countries not coming from OWID (avoid loop)
    - All-indicator-NaN rows
    - Rows with total_vaccinations >= people_vaccinated >= people_fully_vaccinated
    - Only preserve countries which are in the WHO_COUNTRIES dict (those set in the config file)
    """
    # Not from OWID
    tb = tb.loc[tb["DATA_SOURCE"] == "REPORTING"].copy()
    # Ensure numbers make sense
    mask_1 = (tb["TOTAL_VACCINATIONS"] >= tb["PERSONS_VACCINATED_1PLUS_DOSE"]) | tb[
        "PERSONS_VACCINATED_1PLUS_DOSE"
    ].isnull()
    mask_2 = (tb["TOTAL_VACCINATIONS"] >= tb["PERSONS_LAST_DOSE"]) | tb["PERSONS_LAST_DOSE"].isnull()
    tb = tb.loc[(mask_1 & mask_2)]
    # Remove all-nan rows
    columns_idx = [
        "COUNTRY",
        "ISO3",
        "WHO_REGION",
        "DATA_SOURCE",
    ]
    tb = tb.dropna(subset=[col for col in tb.columns if col not in columns_idx], how="all")

    return tb


def calculate_metrics_who(tb: Table) -> Table:
    """Calculate metrics for WHO data."""
    tb[["people_vaccinated", "people_fully_vaccinated"]] = (
        tb[["PERSONS_VACCINATED_1PLUS_DOSE", "PERSONS_LAST_DOSE"]].astype("Int64").fillna(np.nan)
    )
    tb["total_vaccinations"] = tb["TOTAL_VACCINATIONS"].astype("Int64").fillna(np.nan)
    tb["total_boosters"] = tb["PERSONS_BOOSTER_ADD_DOSE"].astype("Int64").fillna(np.nan)
    return tb


def set_dtypes(tb: Table) -> Table:
    """Set dtypes for indicators."""
    tb = tb.astype(
        {
            "date": "datetime64[ns]",
            "country": "string",
        }
    )
    return tb


def add_who_data(tb: Table, tb_who: Table) -> Table:
    """Incorporate WHO's data into main data."""
    ## Build `tb_last`, which contains the latest non-NaN values in the 'main' data
    tb_last = tb.sort_values("date")
    cols = ["total_vaccinations", "people_vaccinated", "people_fully_vaccinated", "total_boosters"]
    tb_last[cols] = tb_last.groupby("country")[cols].ffill()  # type: ignore
    tb_last = tb_last.drop_duplicates(subset=["country"], keep="last")

    ## Merge with WHO, to compare WHO's values with latest in 'main'
    tb_who = tb_who.merge(tb_last, on="country", suffixes=("", "_last"))

    ## Filter out those with least up-to-date data
    tb_who = tb_who.loc[tb_who["date"] > tb_who["date_last"]]
    for col in cols:
        mask = tb_who[col] <= tb_who[f"{col}_last"]
        tb_who.loc[mask, col] = np.nan

    # Keep relevant columns
    tb_who = tb_who.loc[:, ["date", "country"] + cols]

    # Merge with main table
    tb = concat([tb, tb_who], short_name="vaccinations_global")

    return tb
