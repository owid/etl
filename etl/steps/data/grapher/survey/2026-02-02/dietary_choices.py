"""Load a garden dataset and create a grapher dataset."""

import owid.catalog.processing as pr
import pandas as pd

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Groups to select from the survey data, per country.
SELECTED_GROUPS = {
    "United Kingdom": ["All adults", "18-24", "25-49", "50-64", "65+"],
    "United States": ["All adults", "18-29", "30-44", "45-64", "65+"],
}


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its main table.
    ds_garden = paths.load_dataset("dietary_choices")
    tb = ds_garden.read("dietary_choices", safe_types=False)

    #
    # Process data.
    #
    # Adapt table format to grapher requirements.
    tb = tb.drop(columns=["base", "base_unweighted"], errors="raise")

    # Filter each country to its selected age groups.
    pct_cols = [c for c in tb.columns if c not in ["country", "group", "date"]]
    tables = []
    for country, groups in SELECTED_GROUPS.items():
        tb_country = tb[tb["country"] == country].copy()
        tb_country = tb_country[tb_country["group"].isin(groups)].reset_index(drop=True)

        # Sanity check.
        error = f"A survey group may have been renamed for {country}."
        assert set(tb_country["group"]) == set(groups), error

        tables.append(tb_country)

    tb = pr.concat(tables)

    # Sanity check: percentages should be close to 100% (within 2.5pp due to rounding in the source).
    error = "Percentages deviate from 100% by more than 2.5 percentage points for some rows."
    assert (abs(tb[pct_cols].sum(axis=1) - 100) <= 2.5).all(), error

    # Adjust "None of these" so that percentages add up to exactly 100%.
    # (The original data has small rounding gaps, typically less than 2.5 percentage points.)
    tb["none"] += 100 - tb[pct_cols].sum(axis=1)

    error = "Percentages do not add up to exactly 100% after adjustment."
    assert (tb[pct_cols].sum(axis=1) == 100).all(), error

    tb = tb.rename(columns={"date": "year"}, errors="raise")

    # Prepare display metadata.
    date_earliest = tb["year"].astype(str).min()
    for column in tb.drop(columns=["country", "year", "group"]).columns:
        tb[column].metadata.display["yearIsDay"] = True
        tb[column].metadata.display["zeroDay"] = date_earliest

    # Convert year column into a number of days since the earliest date in the table.
    tb["year"] = tb["year"].astype("datetime64")
    tb["year"] = (tb["year"] - pd.to_datetime(date_earliest)).dt.days

    # Improve table format.
    tb = tb.format(keys=["country", "year", "group"])

    #
    # Save outputs.
    #
    # Initialize a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)

    # Save grapher dataset.
    ds_grapher.save()
