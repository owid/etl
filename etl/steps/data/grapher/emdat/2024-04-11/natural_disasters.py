from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

paths = PathFinder(__file__)

# Map of variable titles to their corresponding public titles.
TITLE_MAPPING = {
    "Natural disasters": "Annual number of natural disasters",
    "Natural disasters per 100,000 people": "Annual rate of natural disasters",
    "Affected": "Annual number of people requiring immediate assistance",
    "Affected per 100,000 people": "Annual rate of people requiring immediate assistance",
    "Deaths": "Annual number of deaths",
    "Deaths per 100,000 people": "Annual rate of deaths",
    "Homeless": "Annual number of people left homeless",
    "Homeless per 100,000 people": "Annual rate of people left homeless",
    "Injured": "Annual number of people injured",
    "Injured per 100,000 people": "Annual rate of people injured",
    "Total affected": "Annual number of people affected",
    "Total affected per 100,000 people": "Annual rate of people affected",
    "Insured damages": "Annual insured damages",
    "Insured damages as a share of GDP": "Annual insured damages as a share of GDP",
    "Reconstruction costs": "Annual reconstruction costs",
    "Reconstruction costs as a share of GDP": "Annual reconstruction costs as a share of GDP",
    "Total economic damages": "Annual economic damages",
    "Total economic damages as a share of GDP": "Annual economic damages as a share of GDP",
}


def create_wide_tables(table: Table, is_decade: bool) -> Table:
    # Create wide tables.
    table_wide = table.reset_index()

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
        impact, disaster = column.split("-")
        disaster = disaster.replace("_", " ")
        new_title = old_title + " - " + disaster.capitalize() + variable_title_suffix
        new_title_public = f"{variable_title_public_prefix}{TITLE_MAPPING[old_title]} from {disaster + 's' if disaster[-1] != 's' else disaster}"
        table_wide[column].metadata.title = new_title
        table_wide[column].metadata.display = {"name": disaster.capitalize()}
        table_wide[column].metadata.presentation.title_public = new_title_public
        table_wide = table_wide.rename(
            columns={column: column.replace("-", "_") + variable_name_suffix}, errors="raise"
        )

    # Set an appropriate index and sort conveniently.
    table_wide = table_wide.format()

    return table_wide


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden tables and remove unnecessary columns.
    ds_garden = paths.load_dataset("natural_disasters")
    tb_yearly = ds_garden["natural_disasters_yearly"]
    tb_decadal = ds_garden["natural_disasters_decadal"]

    #
    # Process data.
    #
    # Remove unnecessary columns.
    tb_yearly = tb_yearly.drop(columns=["population", "gdp"], errors="raise")
    tb_decadal = tb_decadal.drop(columns=["population", "gdp"], errors="raise")

    # Create wide tables.
    tb_yearly_wide = create_wide_tables(table=tb_yearly, is_decade=False)
    tb_decadal_wide = create_wide_tables(table=tb_decadal, is_decade=True)

    #
    # Save outputs.
    #
    # Create new grapher dataset, add tables, and save dataset.
    ds_grapher = create_dataset(
        dest_dir,
        tables=[tb_yearly_wide, tb_decadal_wide],
        default_metadata=ds_garden.metadata,
        check_variables_metadata=True,
    )
    ds_grapher.save()
