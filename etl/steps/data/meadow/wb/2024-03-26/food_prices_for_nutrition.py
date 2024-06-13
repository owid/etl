"""Load a snapshot and create a meadow dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def prepare_data(tb: Table) -> Table:
    # Years are given in columns, like "YR2017". Make them integers.
    tb = tb.rename(columns={column: int(column.replace("YR", "")) for column in tb.columns if column.startswith("YR")})

    # Create a column for years.
    tb = tb.melt(id_vars=["classification", "economy", "id", "variable_title"], var_name="year")

    # Gather a mapping of variable ids and titles.
    variable_id_to_title = tb[["id", "variable_title"]].drop_duplicates().set_index(["id"])["variable_title"].to_dict()

    # Transpose the table to have a column per variable.
    tb = tb.drop(columns=["variable_title"]).pivot(
        index=["classification", "economy", "year"], columns="id", join_column_levels_with="_"
    )

    # Columns now start with "value_", remove that prefix to recover the original names.
    tb = tb.rename(columns={column: column.replace("value_", "") for column in tb.columns})

    # Add titles to each variable metadata.
    for variable_id, variable_title in variable_id_to_title.items():
        tb[variable_id].metadata.title = variable_title
        tb = tb.rename(columns={variable_id: variable_title})

    # Set an appropriate index and sort conveniently.
    tb = tb.format(keys=["classification", "economy", "year"])

    return tb


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot and read its data.
    snap = paths.load_snapshot("food_prices_for_nutrition.csv")
    tb = snap.read_csv()

    #
    # Process data.
    #
    # Create a new table and ensure all columns are snake-case.
    tb = prepare_data(tb=tb)

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_meadow.save()
