from owid.catalog import Table

from etl.grapher.helpers import _metadata_for_dimensions
from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Map of variable titles to their corresponding public titles.
TITLE_PUBLIC_START = {
    "Disasters": "Annual number of ",
    "Disasters per 100,000 people": "Annual rate of ",
    "Requiring assistance": "Annual number of people requiring immediate assistance due to ",
    "Requiring assistance per 100,000 people": "Annual rate of people requiring immediate assistance due to ",
    "Deaths": "Annual number of deaths from ",
    "Deaths per 100,000 people": "Annual rate of deaths from ",
    "Homeless": "Annual number of people left homeless by ",
    "Homeless per 100,000 people": "Annual rate of people left homeless by ",
    "Injured": "Annual number of people injured by ",
    "Injured per 100,000 people": "Annual rate of people injured by ",
    "Total affected": "Annual number of people affected by ",
    "Total affected per 100,000 people": "Annual rate of people affected by ",
    "Insured damages": "Annual insured damages from ",
    "Insured damages as a share of GDP": "Annual insured damages as a share of GDP from ",
    "Insured damages adjusted for inflation": "Annual insured damages, adjusted for inflation, from ",
    "Insured damages adjusted for inflation using the US CPI": "Annual insured damages, adjusted for inflation using the US CPI, from ",
    "Reconstruction costs": "Annual reconstruction costs from ",
    "Reconstruction costs as a share of GDP": "Annual reconstruction costs as a share of GDP from ",
    "Reconstruction costs adjusted for inflation": "Annual reconstruction costs, adjusted for inflation, from ",
    "Reconstruction costs adjusted for inflation using the US CPI": "Annual reconstruction costs, adjusted for inflation using the US CPI, from ",
    "Total economic damages": "Annual economic damages from ",
    "Total economic damages as a share of GDP": "Annual economic damages as a share of GDP from ",
    "Total economic damages adjusted for inflation": "Annual economic damages, adjusted for inflation, from ",
    "Total economic damages adjusted for inflation using the US CPI": "Annual economic damages, adjusted for inflation using the US CPI, from ",
}
# Map disaster type to their name at the end of the public title (plural).
TITLE_PUBLIC_END = {
    "earthquake": "earthquakes",
    "extreme_weather": "storms",
    "volcanic_activity": "volcanoes",
    "flood": "floods",
    "all_disasters": "all disasters",
    # "glacial_lake_outburst_flood": ...,  # Folded into "Flood" — see EXPECTED_DISASTER_TYPES.
    "all_disasters_excluding_earthquakes": "all disasters excluding earthquakes",
    "wildfire": "wildfires",
    "landslide": "landslides",
    # Wet/dry mass movements are folded into "landslide" — see EXPECTED_DISASTER_TYPES in the garden step.
    # "wet_mass_movement": "wet mass movements",
    # "dry_mass_movement": "dry mass movements",
    # "fog": "fogs",  # Excluded — see EXPECTED_DISASTER_TYPES in the garden step.
    "drought": "droughts",
    "extreme_temperature": "extreme temperatures",
    "all_disasters_excluding_extreme_temperature": "all disasters excluding extreme temperatures",
}
# Map disaster type to their name in the title (singular).
TITLE_DISASTER = {
    "earthquake": "Earthquake",
    "extreme_weather": "Storms",
    "volcanic_activity": "Volcanoes",
    "flood": "Flood",
    "all_disasters": "All disasters",
    # "glacial_lake_outburst_flood": ...,  # Folded into "Flood" — see EXPECTED_DISASTER_TYPES.
    "all_disasters_excluding_earthquakes": "All disasters excluding earthquakes",
    "wildfire": "Wildfire",
    "landslide": "Landslide",
    # Wet/dry mass movements are folded into "Landslide" — see EXPECTED_DISASTER_TYPES in the garden step.
    # "wet_mass_movement": "Wet mass movement",
    # "dry_mass_movement": "Dry mass movement",
    # "fog": "Fog",  # Excluded — see EXPECTED_DISASTER_TYPES in the garden step.
    "drought": "Drought",
    "extreme_temperature": "Extreme temperature",
    "all_disasters_excluding_extreme_temperature": "All disasters excluding extreme temperature",
}


def create_wide_tables(table: Table, is_decade: bool) -> Table:
    # Create wide tables.
    table_wide = table.copy()

    table_wide = table_wide.pivot(index=["country", "year"], columns="type", join_column_levels_with="-")

    if is_decade:
        variable_title_public_prefix = "Decadal average: "
        variable_name_suffix = "_decadal"
        variable_title_suffix = " (decadal)"
    else:
        variable_title_public_prefix = ""
        variable_name_suffix = "_yearly"
        variable_title_suffix = ""

    # Improve variable names and titles.
    for column in table_wide.drop(columns=["country", "year"], errors="raise").columns:
        old_title = table_wide[column].metadata.title
        _, disaster = column.split("-")
        new_title = old_title + " - " + TITLE_DISASTER[disaster] + variable_title_suffix
        new_title_public = f"{variable_title_public_prefix}{TITLE_PUBLIC_START[old_title]}{TITLE_PUBLIC_END[disaster]}"
        table_wide[column].metadata.title = new_title
        display = {"name": TITLE_DISASTER[disaster]}
        # Keep decadal averages of people counts without decimals.
        if is_decade and (table_wide[column].metadata.unit or "").strip().lower() == "people":
            display["numDecimalPlaces"] = 0
        table_wide[column].metadata.display = display
        table_wide[column].metadata.presentation.title_public = new_title_public
        # Run the framework's per-dimension Jinja expansion so any `<% if type ==
        # ... %>` templates in description_key (and other fields inherited from
        # the garden long-form metadata) resolve against this column's disaster.
        # Manual `Table.pivot` skips the auto-widen path that normally calls this,
        # so we invoke it explicitly here.
        table_wide[column].metadata = _metadata_for_dimensions(table_wide[column].metadata, {"type": disaster}, column)
        table_wide = table_wide.rename(
            columns={column: column.replace("-", "_") + variable_name_suffix}, errors="raise"
        )

    # Set an appropriate index and sort conveniently.
    table_wide = table_wide.format()

    return table_wide


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden tables and remove unnecessary columns.
    ds_garden = paths.load_dataset("natural_disasters")
    tb_yearly = ds_garden.read("natural_disasters_yearly")
    tb_decadal = ds_garden.read("natural_disasters_decadal")

    #
    # Process data.
    #
    # Remove unnecessary columns.
    tb_yearly = tb_yearly.drop(columns=["population"], errors="raise")
    tb_decadal = tb_decadal.drop(columns=["population"], errors="raise")

    # Create wide tables.
    tb_yearly_wide = create_wide_tables(table=tb_yearly, is_decade=False)
    tb_decadal_wide = create_wide_tables(table=tb_decadal, is_decade=True)

    #
    # Save outputs.
    #
    # Create new grapher dataset, add tables, and save dataset.
    ds_grapher = paths.create_dataset(tables=[tb_yearly_wide, tb_decadal_wide], default_metadata=ds_garden.metadata)
    ds_grapher.save()
