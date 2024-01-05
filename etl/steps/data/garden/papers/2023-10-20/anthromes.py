"""Load a meadow dataset and create a garden dataset."""


import owid.catalog.processing as pr
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

    tb_combined.metadata = tb.metadata

    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb_combined], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def calculate_share_of_land_type(tb: Table, land_areas: Table) -> Table:
    tb = tb.merge(land_areas, on="regn_nm")
    tb["share_of_land_type"] = (tb["land_ar"] / tb["total_region_area"]) * 100
    tb = tb.drop(columns=["total_region_area"])
    tb = tb.rename(columns={"regn_nm": "country"})
    tb = tb.pivot(index=["country", "year"], columns="value")
    tb.columns = [" ".join(col).strip() for col in tb.columns.values]

    return tb


def calculate_area_of_each_land_type(tb: Table) -> Table:
    tb_global = tb.groupby(["name", "value"])["land_ar"].sum()
    tb_global = tb_global.reset_index()
    tb_global["regn_nm"] = "World"

    tb_regional = tb.groupby(["name", "value", "regn_nm"])["land_ar"].sum()
    tb_regional = tb_regional.reset_index()

    tb_combined = pr.concat([tb_global, tb_regional], ignore_index=True)
    # Convert year string

    tb_combined = tb_combined.drop(columns=["name"])

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
