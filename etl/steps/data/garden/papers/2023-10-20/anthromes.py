"""Load a meadow dataset and create a garden dataset."""

import re

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table

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

    # Calculate total land areas
    land_areas = tb_input.groupby("regn_nm").sum().drop(columns="id").reset_index()
    # Add a row for 'world' with the sum of 'land_ar'
    world_row = pd.DataFrame({"regn_nm": ["World"], "land_ar": [land_areas["land_ar"].sum()]})
    # Concatenate the 'world_row' DataFrame with the original DataFrame
    land_areas = pd.concat([land_areas, world_row], ignore_index=True)
    land_areas = land_areas.rename(columns={"land_ar": "total_region_area"})
    land_areas = Table(land_areas, metadata=tb_input.metadata)
    # Make the tb long
    tb_long = pr.melt(tb, id_vars="id", var_name="name", value_name="value")
    tb_merge = tb_long.merge(tb_input, on="id", how="left")
    # Add categories to the value column and figure out how to sum the area values
    tb_global = tb_merge.groupby(["name", "value"])["land_ar"].sum()
    tb_global = tb_global.reset_index()
    tb_global["regn_nm"] = "World"

    tb_regional = tb_merge.groupby(["name", "value", "regn_nm"])["land_ar"].sum()
    tb_regional = tb_regional.reset_index()

    tb_combined = pr.concat([tb_global, tb_regional], ignore_index=True)
    # Convert year string

    tb_combined["year"] = tb_combined["name"].apply(convert_years_to_number)
    tb_combined = tb_combined.drop(columns=["name"])

    # Assign land use categories more meaningful names
    tb_combined = assign_land_use_types(tb_combined)
    # Calculate share of each  land type

    tb_combined = tb_combined.merge(land_areas, on="regn_nm")
    tb_combined["share_of_land_type"] = (tb_combined["land_ar"] / tb_combined["total_region_area"]) * 100
    tb_combined = tb_combined.drop(columns=["total_region_area"])
    tb_combined = tb_combined.pivot(index=["regn_nm", "year"], columns="value")
    tb_combined.columns = [" ".join(col).strip() for col in tb_combined.columns.values]
    tb_combined.metadata = tb.metadata
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def convert_years_to_number(year: str) -> str:
    """
    Converting the given year string to a number

    """
    match = re.match(r"^_(\d+)(bc|ad)$", year)

    if match:
        year, era = match.groups()
        year = int(year)

        if era == "bc":
            year = -year

        return year
    else:
        return None


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
