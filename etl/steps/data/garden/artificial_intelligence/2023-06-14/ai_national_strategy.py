"""Load a meadow dataset and create a garden dataset."""

from typing import cast

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("ai_national_strategy"))
    # Load region dataset to find all possible countries and later fill the ones that don't exist in the spreadsheet as not released (according to source that's the implication)
    ds_regions = cast(Dataset, paths.load_dependency("regions"))
    countries_national_ai = pd.DataFrame(ds_regions["regions"]["name"])
    countries_national_ai.reset_index(drop=True, inplace=True)
    countries_national_ai["released"] = np.NaN
    # Generate the column names from "2017" to "2022"
    column_names = [str(year) for year in range(2017, 2023)]

    # Fill the columns with the corresponding years
    for column in column_names:
        countries_national_ai[column] = column

    # Melt the columns into a single "year" column
    countries_national_ai = countries_national_ai.melt(
        id_vars=["name", "released"], var_name="year", value_name="value"
    )
    countries_national_ai.drop("value", axis=1, inplace=True)
    countries_national_ai["year"] = countries_national_ai["year"].astype(int)

    countries_national_ai.rename(columns={"name": "country"}, inplace=True)
    # Read table from meadow dataset.
    tb = ds_meadow["ai_national_strategy"]
    tb.reset_index(inplace=True)

    #
    # Process data.
    #
    tb: Table = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb["released_national_strategy_on_ai"] = tb["released_national_strategy_on_ai"].replace(
        {0: "In development", 1: "Released"}
    )
    df_merged = pd.merge(countries_national_ai, tb, on=["country", "year"], how="outer")
    df_merged.sort_values(by=["year"], inplace=True)

    # Fill with the most recent value (e.g., if a strategy was released in 2015, then it's still released in 2016 onwards; for countries that have NaNs everywhere fill with Not Release)
    for country, group in df_merged.groupby("country"):
        # Check if any year for the current country is not NaN
        if not group["released_national_strategy_on_ai"].isna().all():
            # Forward fill NaN values after "Released"
            group["released_national_strategy_on_ai"].fillna(method="ffill", inplace=True)

        # Fill remaining NaN values with "Not Released"
        group["released_national_strategy_on_ai"].fillna("Not released", inplace=True)
        df_merged.loc[group.index] = group
    df_merged.drop("released", axis=1, inplace=True)
    tb = Table(df_merged, short_name=paths.short_name, underscore=True)

    tb.set_index(["country", "year"], inplace=True)
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
