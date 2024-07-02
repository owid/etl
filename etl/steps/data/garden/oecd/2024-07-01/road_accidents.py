"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Origin, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


data_cols = [
    "Fatalities",
    "Injured",
    "Injury crashes",
    "deaths",
    "injuries",
    "accidents_involving_casualties",
    "deaths_per_million_inhabitants",
    "deaths__per_million_vehicles",
]


def add_origins(tb: Table, cols: list, origins: Origin) -> Table:
    for col in cols:
        tb[col].origins = origins
    return tb


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("road_accidents", channel="meadow", version="2024-07-01")
    ds_old = paths.load_dataset("road_accidents", version="2023-08-11")
    ds_population = paths.load_dataset("population")

    # Read table from meadow dataset.
    tb = ds_meadow["road_accidents"].reset_index()
    tb_old = ds_old["road_accidents"].reset_index()

    col_origins = tb["obs_value"].origins.copy()

    tb = tb.pivot_table(index=["country", "year"], columns=["measure"], values="obs_value").reset_index()

    # harmonize country names
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path, warn_on_unused_countries=False)
    tb_old = geo.harmonize_countries(
        df=tb_old, countries_file=paths.country_mapping_path, warn_on_unused_countries=False
    )

    # Combine new and old data
    tb = tb.merge(tb_old, how="outer", on=["country", "year"], suffixes=("", "_old")).copy_metadata(tb)

    #
    # process data - combine indicators and add death per million inhabitants
    #

    # change NA to -1 for easier handling
    tb[data_cols] = tb[data_cols].fillna(-1)
    # if one column is -1 use other column, otherwise use new data (in columns Fatalities, Injured, Injury crashes)
    tb["accident_deaths"] = tb.apply(lambda x: x["Fatalities"] if x["Fatalities"] != -1 else x["deaths"], axis=1)
    tb["accident_injuries"] = tb.apply(lambda x: x["Injured"] if x["Injured"] != -1 else x["injuries"], axis=1)
    tb["accidents_with_injuries"] = tb.apply(
        lambda x: x["Injury crashes"] if x["Injury crashes"] != -1 else x["accidents_involving_casualties"], axis=1
    )
    # change -1 back to NA
    tb = tb.replace(-1, pd.NA)

    # drop old columns
    tb = tb.drop(
        columns=["Fatalities", "Injured", "Injury crashes", "deaths", "injuries", "accidents_involving_casualties"]
    )

    # add death per million inhabitants to compare
    tb = geo.add_population_to_table(tb, ds_population)
    tb["deaths_per_million_population"] = (tb["accident_deaths"] / tb["population"]) * 1_000_000

    # drop population as well as old death per million and per vehicle (these numbers are most likely wrong)
    tb = tb.drop(columns=["population", "deaths_per_million_inhabitants", "deaths__per_million_vehicles"])

    # change dtypes:
    for col in ["accident_deaths", "accident_injuries", "accidents_with_injuries"]:
        tb[col] = tb[col].astype("Int64")
    tb["deaths_per_million_population"] = tb["deaths_per_million_population"].astype("Float64")

    # add back origins
    tb = add_origins(
        tb,
        ["accident_deaths", "accident_injuries", "accidents_with_injuries", "deaths_per_million_population"],
        col_origins,
    )

    # format table
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
