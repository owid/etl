"""Load a meadow dataset and create a garden dataset."""


import numpy as np
import owid.catalog.processing as pr
from owid.catalog import Table
from owid.catalog.utils import underscore_table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("anthromes")
    ds_meadow_input = paths.load_dataset("anthromes_input")
    # Read table from meadow dataset.
    tb = ds_meadow["anthromes"].reset_index()
    tb_input = ds_meadow_input["anthromes_input"].reset_index().drop(columns=["pot_veg", "pot_vll"])
    # Bit of a hack as the metadata for tb_input is not passed though when opening the shapefile
    tb_input.metadata = tb.metadata

    land_areas = calculate_regional_land_areas(tb_input)

    # Combine the land type of each cell with the area of each cell
    tb_long = pr.melt(tb, id_vars="id", var_name="name", value_name="value")
    tb_merge = pr.merge(tb_long, tb_input, on="id", how="left")

    tb_merge = convert_years_to_number(tb_merge)
    # Calculated global and regional total of each land type, each year
    tb_combined = calculate_area_of_each_land_type(tb_merge)

    # Assign land use categories more meaningful names
    tb_combined = assign_land_use_types(tb_combined)

    # Calculate share of each land type
    tb_combined = calculate_share_of_land_type(tb_combined, land_areas)
    tb_combined = underscore_table(tb_combined)
    tb_combined = add_aggregate_land_types(tb_combined)
    # Save outputs.

    tb_combined = tb_combined.set_index(["country", "year"], verify_integrity=True).sort_index()
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb_combined], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_aggregate_land_types(tb: Table) -> Table:
    """
    Aggregating the groups to broader levels, shown in Ellis et al. (2020)
    """

    aggregate_dict = {
        "land_ar_dense_settlements": ["land_ar_mixed_settlements", "land_ar_urban"],
        "land_ar_villages": [
            "land_ar_rice_villages",
            "land_ar_irrigated_villages",
            "land_ar_rainfed_villages",
            "land_ar_pastoral_villages",
        ],
        "land_ar_croplands": [
            "land_ar_residential_irrigated_croplands",
            "land_ar_residential_rainfed_croplands",
            "land_ar_populated_croplands",
            "land_ar_remote_croplands",
        ],
        "land_ar_rangelands": [
            "land_ar_residential_rangelands",
            "land_ar_populated_rangelands",
            "land_ar_remote_rangelands",
        ],
        "land_ar_seminatural_lands": [
            "land_ar_residential_woodlands",
            "land_ar_populated_woodlands",
            "land_ar_remote_woodlands",
            "land_ar_inhabited_treeless_and_barren_lands",
        ],
        "land_ar_wild_lands": ["land_ar_wild_woodlands", "land_ar_wild_treeless_and_barren_lands"],
        "land_ar_used_lands": [
            "land_ar_mixed_settlements",
            "land_ar_urban",
            "land_ar_rice_villages",
            "land_ar_irrigated_villages",
            "land_ar_rainfed_villages",
            "land_ar_pastoral_villages",
            "land_ar_residential_irrigated_croplands",
            "land_ar_residential_rainfed_croplands",
            "land_ar_populated_croplands",
            "land_ar_remote_croplands",
            "land_ar_residential_rangelands",
            "land_ar_populated_rangelands",
            "land_ar_remote_rangelands",
        ],
        "share_of_land_type_dense_settlements": ["share_of_land_type_mixed_settlements", "share_of_land_type_urban"],
        "share_of_land_type_villages": [
            "share_of_land_type_rice_villages",
            "share_of_land_type_irrigated_villages",
            "share_of_land_type_rainfed_villages",
            "share_of_land_type_pastoral_villages",
        ],
        "share_of_land_type_croplands": [
            "share_of_land_type_residential_irrigated_croplands",
            "share_of_land_type_residential_rainfed_croplands",
            "share_of_land_type_populated_croplands",
            "share_of_land_type_remote_croplands",
        ],
        "share_of_land_type_rangelands": [
            "share_of_land_type_residential_rangelands",
            "share_of_land_type_populated_rangelands",
            "share_of_land_type_remote_rangelands",
        ],
        "share_of_land_type_seminatural_lands": [
            "share_of_land_type_residential_woodlands",
            "share_of_land_type_populated_woodlands",
            "share_of_land_type_remote_woodlands",
            "share_of_land_type_inhabited_treeless_and_barren_lands",
        ],
        "share_of_land_type_wild_lands": [
            "share_of_land_type_wild_woodlands",
            "share_of_land_type_wild_treeless_and_barren_lands",
        ],
        "share_of_land_type_used_lands": [
            "share_of_land_type_mixed_settlements",
            "share_of_land_type_urban",
            "share_of_land_type_rice_villages",
            "share_of_land_type_irrigated_villages",
            "share_of_land_type_rainfed_villages",
            "share_of_land_type_pastoral_villages",
            "share_of_land_type_residential_irrigated_croplands",
            "share_of_land_type_residential_rainfed_croplands",
            "share_of_land_type_populated_croplands",
            "share_of_land_type_remote_croplands",
            "share_of_land_type_residential_rangelands",
            "share_of_land_type_populated_rangelands",
            "share_of_land_type_remote_rangelands",
        ],
    }

    for new_col, cols_to_sum in aggregate_dict.items():
        assert all(
            col in tb.columns for col in cols_to_sum
        ), f"One or more columns from {cols_to_sum} are not in the table"
        tb[new_col] = tb[cols_to_sum].sum(axis=1)

    return tb


def calculate_share_of_land_type(tb: Table, land_areas: Table) -> Table:
    tb = tb.merge(land_areas, on="regn_nm")
    tb["share_of_land_type"] = (tb["land_ar"] / tb["total_region_area"]) * 100
    tb = tb.drop(columns=["total_region_area"])
    tb = tb.rename(columns={"regn_nm": "country"})
    tb = tb.pivot(index=["country", "year"], columns="value", join_column_levels_with="_")
    tb = tb.reset_index(drop=True)

    return tb


def calculate_area_of_each_land_type(tb: Table) -> Table:
    tb_global = tb.groupby(["year", "value"])["land_ar"].sum()
    tb_global = tb_global.reset_index()
    tb_global["regn_nm"] = "World"
    tb_global["land_ar"] = tb_global["land_ar"].replace(np.nan, 0)

    tb_regional = tb.groupby(["year", "value", "regn_nm"])["land_ar"].sum()
    tb_regional = tb_regional.reset_index()
    tb_regional["land_ar"] = tb_regional["land_ar"].replace(np.nan, 0)

    tb_combined = pr.concat([tb_global, tb_regional], ignore_index=True)

    return tb_combined


def calculate_regional_land_areas(tb: Table) -> Table:
    """
    Calculating the total land area for each region

    """
    # Calculate land area for each region
    land_areas = tb.groupby("regn_nm").sum().drop(columns="id").reset_index()
    # Add a row for 'world' with the sum of 'land_ar'
    world_row = Table({"regn_nm": ["World"], "land_ar": [land_areas["land_ar"].sum()]})
    # Concatenate the 'world_row' DataFrame with the original DataFrame
    land_areas = pr.concat([land_areas, world_row], ignore_index=True)
    land_areas = land_areas.rename(columns={"land_ar": "total_region_area"})

    return land_areas


def convert_years_to_number(tb: Table) -> Table:
    """
    Converting the given year string to a number

    """
    unique_years = tb["name"].unique()
    year_mapping = {year: convert_year(year) for year in unique_years}
    tb["year"] = tb["name"].map(year_mapping)
    tb = tb.drop(columns=["name"])
    return tb


def convert_year(year_str: str) -> int:
    is_bc = "bc" in year_str.lower()
    cleaned_year = "".join(filter(str.isdigit, year_str))
    year_int = int(cleaned_year)
    return -year_int if is_bc else year_int


def assign_land_use_types(tb: Table) -> Table:
    """
    Assigning the land use types to the numbers given in the table, based on
    """
    land_use_dict = {
        11: "Urban",
        12: "Mixed settlements",
        21: "Rice villages",
        22: "Irrigated villages",
        23: "Rainfed villages",
        24: "Pastoral villages",
        31: "Residential irrigated croplands",
        32: "Residential rainfed croplands",
        33: "Populated croplands",
        34: "Remote croplands",
        41: "Residential rangelands",
        42: "Populated rangelands",
        43: "Remote rangelands",
        51: "Residential woodlands",
        52: "Populated woodlands",
        53: "Remote woodlands",
        54: "Inhabited treeless and barren lands",
        61: "Wild woodlands",
        62: "Wild treeless and barren lands",
        63: "Ice, uninhabited",
        70: "No land",
    }

    tb["value"] = tb["value"].replace(land_use_dict)

    return tb
