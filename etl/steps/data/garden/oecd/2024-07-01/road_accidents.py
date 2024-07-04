"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Origin, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

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


def check_road_passenger_travel(tb_row: pd.Series):
    """Check if road passenger travel is identical to bus or car passenger travel. If so, remove road passenger travel."""
    road_passengers = tb_row["passenger_kms_road"]
    # if road passenger travel is NA or both bus and car passenger travel are NA, there is nothing to compare
    if pd.isna(road_passengers) or (pd.isna(tb_row["passenger_kms_bus"]) & pd.isna(tb_row["passenger_kms_car"])):
        return road_passengers
    # check whether road passenger travel is identical to bus travel
    elif not pd.isna(tb_row["passenger_kms_bus"]) and tb_row["passenger_kms_bus"] == road_passengers:
        return None
    # check whether road passenger travel is identical to car travel
    elif not pd.isna(tb_row["passenger_kms_car"]) and tb_row["passenger_kms_car"] == road_passengers:
        tb_row["passenger_kms_road"] = None
        return None


def choose_latest_data(tb_row: pd.Series, old_col: str, new_col: str):
    if pd.isna(tb_row[new_col]):
        return tb_row[old_col]
    else:
        return tb_row[new_col]


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

    # rename passenger travel columns
    tb = tb.rename(
        columns={
            "Rail": "passenger_kms_rail",
            "Road": "passenger_kms_road",
            "Passenger cars": "passenger_kms_car",
            "Buses": "passenger_kms_bus",
        }
    )

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
    # if one column is nan use other column, otherwise use new data
    tb["accident_deaths"] = tb.apply(lambda x: choose_latest_data(x, "deaths", "fatalities"), axis=1)
    tb["accident_injuries"] = tb.apply(lambda x: choose_latest_data(x, "injuries", "injured"), axis=1)
    tb["accidents_with_injuries"] = tb.apply(
        lambda x: choose_latest_data(x, "accidents_involving_casualties", "crashes"), axis=1
    )

    # drop old columns
    tb = tb.drop(columns=["fatalities", "injured", "crashes", "deaths", "injuries", "accidents_involving_casualties"])

    # remove passenger travel by road if it is identical to bus or car column
    tb["passenger_kms_road"] = tb.apply(lambda x: check_road_passenger_travel(x), axis=1)

    # add death per million inhabitants
    tb = geo.add_population_to_table(tb, ds_population)
    tb["deaths_per_million_population"] = (tb["accident_deaths"] / tb["population"]) * 1_000_000
    # drop population as well as old death per million and per vehicle (these numbers are wrong)
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
