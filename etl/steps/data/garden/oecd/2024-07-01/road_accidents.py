"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Origin, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


old_data_cols = [
    "Fatalities",
    "Injured",
    "Injury crashes",
    "deaths",
    "injuries",
    "accidents_involving_casualties",
    "deaths_per_million_inhabitants",
    "deaths__per_million_vehicles",
]

new_data_cols = [
    "accident_deaths",
    "accident_injuries",
    "accidents_with_injuries",
    "deaths_per_million_population",
    "passenger_kms_rail",
    "passenger_kms_road",
    "passenger_kms_car",
    "passenger_kms_bus",
    "deaths_per_billion_kms",
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
    ds_passenger = paths.load_dataset("passenger_travel", channel="meadow", version="2024-07-01")
    ds_old = paths.load_dataset("road_accidents", version="2023-08-11")
    ds_population = paths.load_dataset("population")

    # Read table from meadow dataset.
    tb = ds_meadow["road_accidents"].reset_index()
    tb_old = ds_old["road_accidents"].reset_index()
    tb_passenger = ds_passenger["passenger_travel"].reset_index()

    # standardize both tables
    tb_passenger = tb_passenger.rename(columns={"vehicle_type": "measure"})

    # concatenate the two tables
    tb = pr.concat([tb, tb_passenger], ignore_index=True)

    col_origins = tb["obs_value"].origins.copy()

    # pivot table
    tb = tb.pivot_table(index=["country", "year"], columns=["measure"], values="obs_value").reset_index()

    # harmonize country names
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path, warn_on_unused_countries=False)
    tb_old = geo.harmonize_countries(
        df=tb_old, countries_file=paths.country_mapping_path, warn_on_unused_countries=False
    )

    # Combine new and old data
    tb = tb.merge(tb_old, how="outer", on=["country", "year"], suffixes=("", "_old")).copy_metadata(tb)

    # make all column names lowercase
    tb.columns = tb.columns.str.lower()

    #
    # process data - combine indicators and add death per million inhabitants/ per thousand passenger kilometers
    #
    # if one column is nan use other column, otherwise use new data (in columns Fatalities, Injured, Injury crashes)
    tb["accident_deaths"] = tb.apply(lambda x: x["deaths"] if pd.isna(x["fatalities"]) else x["fatalities"], axis=1)
    tb["accident_injuries"] = tb.apply(lambda x: x["injuries"] if pd.isna(x["injured"]) else x["injured"], axis=1)
    tb["accidents_with_injuries"] = tb.apply(
        lambda x: x["accidents_involving_casualties"] if pd.isna(x["crashes"]) else x["crashes"],
        axis=1,
    )
    # drop old columns
    tb = tb.drop(columns=["fatalities", "injured", "crashes", "deaths", "injuries", "accidents_involving_casualties"])

    # rename passenger travel columns
    tb = tb.rename(
        columns={
            "rail": "passenger_kms_rail",
            "road": "passenger_kms_road",
            "passenger cars": "passenger_kms_car",
            "buses": "passenger_kms_bus",
        }
    )

    # add death per million inhabitants
    tb = geo.add_population_to_table(tb, ds_population)
    tb["deaths_per_million_population"] = (tb["accident_deaths"] / tb["population"]) * 1_000_000
    # drop population as well as old death per million and per vehicle (these numbers are most likely wrong)
    tb = tb.drop(columns=["population", "deaths_per_million_inhabitants", "deaths__per_million_vehicles"])

    # add death per billion passenger kilometers:
    tb["deaths_per_billion_kms"] = tb["accident_deaths"] / tb["passenger_kms_road"] * 1_000_000_000

    # change dtypes:
    for col in [col for col in new_data_cols if col not in ["deaths_per_million_population", "deaths_per_billion_kms"]]:
        tb[col] = tb[col].astype("Int64")

    tb["deaths_per_million_population"] = tb["deaths_per_million_population"].astype("Float64")
    tb["deaths_per_billion_kms"] = tb["deaths_per_billion_kms"].astype("Float64")

    # add back origins
    tb = add_origins(tb, new_data_cols, col_origins)

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
