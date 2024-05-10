"""Load a garden dataset and create a grapher dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# Imported and adapted from the grapher natural_disasters step.
def create_wide_tables(table: Table, is_decade: bool) -> Table:
    # Create wide tables.
    table_wide = table.reset_index()

    table_wide = table_wide.pivot(index=["country", "year"], columns="type", join_column_levels_with="-")

    if is_decade:
        variable_name_suffix = "_decadal"
        variable_title_suffix = " (decadal)"
    else:
        variable_name_suffix = "_yearly"
        variable_title_suffix = ""

    # Improve variable names and titles.
    for column in table_wide.drop(columns=["country", "year"], errors="raise").columns:
        table_wide[column].metadata.title += (
            " - " + column.split("-")[-1].capitalize().replace("_", " ") + variable_title_suffix
        )

        # Identify the disaster from the column name.
        disaster = [disaster for disaster in ["earthquake", "tsunami", "volcan"] if disaster in column][0]
        # Select the relevant origin for the current type of disaster.
        origin = [origin for origin in table_wide[column].metadata.origins if disaster in origin.title.lower()]
        assert len(origin) == 1, f"Expected one origin, found {len(origin)}."
        table_wide[column].metadata.origins = origin

        # Rename columns.
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
    # Load garden dataset and read its main table.
    ds_garden = paths.load_dataset("natural_hazards")
    tb_yearly = ds_garden["natural_hazards"]

    #
    # Process data.
    #
    # Create wide tables.
    tb_yearly_wide = create_wide_tables(table=tb_yearly, is_decade=False)

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb_yearly_wide], check_variables_metadata=True)
    ds_grapher.save()
