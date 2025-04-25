"""Load a meadow dataset and create a garden dataset."""

import pandas as pd

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("world_economic_outlook")

    # Read table from meadow dataset.
    tb = ds_meadow.read("world_economic_outlook")

    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    tb = tb.sort_values(["country", "year"]).reset_index(drop=True)
    for col in tb.columns:
        if col in [
            "gross_domestic_product__constant_prices__percent_change_observation",
            "unemployment_rate__percent_of_total_labor_force_observation",
        ]:
            # 1. Identify the last observed (non-null) year per country for this column
            last_obs = tb.loc[tb[col].notnull()].groupby("country")["year"].max()

            # 2. Add helper column to mark if the row is beyond last observation
            tb["last_obs_year"] = tb["country"].map(last_obs)
            mask = tb["year"] <= tb["last_obs_year"]  # only keep rows up to last non-null year

            # 3. Apply rolling mean **only on rows up to last observation**
            tb["rolling_" + col] = (
                tb[mask]  # restrict to valid rows
                .groupby("country")[col]
                .transform(lambda x: x.rolling(10, min_periods=5).mean())
            )

            # 4. Clean up: make sure other rows (after last obs) are NaN in the rolling column
            tb["rolling_" + col] = tb["rolling_" + col].where(mask)

    # Drop helper column if not needed
    tb = tb.drop(columns="last_obs_year")
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
