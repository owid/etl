"""Load a meadow dataset and create a garden dataset."""

import pandas as pd

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

COL_TO_KEEP = [
    "country",
    "year",
    "low_est_all_irregular",
    "centr_est_all_irregular",
    "high_est_all_irregular",
    "low_est_all_irregular_incl_asylum",
    "centr_est_all_irregular_incl_asylum",
    "high_est_all_irregular_incl_asylum",
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("mirrem_irregular_migration")

    # Read table from meadow dataset.
    tb = ds_meadow["mirrem_irregular_migration"].reset_index()

    # some estimates are given as <100,000,
    # which we intepret as high estimate 100,000 and low estimate 0
    tb["high_est_<100k"] = tb["highestimate"] == "<100,000"
    tb["low_est_<100k"] = tb["lowestimate"] == "-"

    tb = tb.replace("nan", pd.NA).replace("NaN", pd.NA)

    tb["low_est"] = tb["lowestimate"].replace("-", 0).replace("<100,000", 0).astype("Float64")
    tb["high_est"] = tb["highestimate"].replace("<100,000", 100000).astype("Float64")
    tb["centr_est"] = tb["centralestimate"].astype("Float64")

    # fill in missing central estimates with mean of low and high estimates
    tb["centr_est"] = tb["centr_est"].fillna((tb["high_est"] + tb["low_est"]) / 2)

    tb_meta = tb["centr_est"].m

    # pivot table to have one row per country and year
    tb = tb.pivot_table(
        index=["country", "year"],
        columns="population",
        values=["low_est", "centr_est", "high_est"],
    )

    # flatten multiindex columns and keep only relevant columns
    tb.columns = ["_".join(col).strip() for col in tb.columns.values]
    tb = tb.reset_index()
    tb = tb[COL_TO_KEEP]

    for col in COL_TO_KEEP:
        tb[col].metadata = tb_meta

    # harmonize and format table
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
