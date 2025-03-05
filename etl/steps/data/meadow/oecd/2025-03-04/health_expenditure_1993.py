"""Load a snapshot and create a meadow dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("health_expenditure_1993.xlsx")

    # Load data from snapshot.
    tb_gdp = snap.read(sheet_name="GDP ", skiprows=1)
    tb_health_expenditure = snap.read(sheet_name="Pub Health exp", skiprows=1)

    #
    # Process data.
    #
    # Rename the first column as "country" and make the table long, adding year as a column.
    tb_gdp = rename_columns_and_make_long(tb=tb_gdp, short_name="gdp")
    tb_health_expenditure = rename_columns_and_make_long(tb=tb_health_expenditure, short_name="health_expenditure")

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tables = [tb_gdp, tb_health_expenditure]

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
        default_metadata=snap.metadata,
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def rename_columns_and_make_long(tb: Table, short_name: str) -> Table:
    """
    Rename the first column as "country" and make the table long, adding year as a column.
    """
    tb = tb.rename(columns={"Unnamed: 0": "country"})

    # Make the table long.
    tb = tb.melt(id_vars=["country"], var_name="year", value_name=short_name)

    # Format and add short_name
    tb = tb.format(["country", "year"], short_name=short_name)

    return tb
