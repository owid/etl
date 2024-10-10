"""Load a meadow dataset and create a garden dataset."""

import pandas as pd

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("ons_migration_estimates")

    # Read table from meadow dataset.
    tb = ds_meadow["ons_migration_estimates"].reset_index()

    #
    # Process data.
    #

    # pivot table to get separate columns for each flow and estimate
    tb = tb.pivot(
        index=["country", "year", "period"], columns="flow", values=["estimate", "lower_bound", "upper_bound"]
    ).reset_index()

    tb.columns = ["_".join(col[::-1]).strip() if col[1] != "" else col[0] for col in tb.columns.values]

    for col in [col for col in tb.columns if col not in ["country", "year", "period"]]:
        tb[col] = tb[col].replace("Not available", pd.NA)
        tb[col] = tb[col].astype("Int64")

    # remove rows with years ending in june:
    tb = tb[~tb.period.str.contains("June")]
    tb = tb.drop(columns=["period"], errors="raise")

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
