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
    snap_income = paths.load_snapshot("household_income_australia_2017_2018.xlsx")
    snap_wealth = paths.load_snapshot("household_wealth_australia_2017_2018.xlsx")

    # Load data from snapshot.
    tb_income = snap_income.read(sheet_name="Table 1.1", skiprows=4, nrows=43)
    tb_wealth = snap_wealth.read(sheet_name="Table 2.2 ", skiprows=4, nrows=41)

    #
    # Process data.
    #
    tb_income = reformat_tables(tb=tb_income, short_name="income")
    tb_wealth = reformat_tables(tb=tb_wealth, short_name="wealth")

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir, tables=[tb_income, tb_wealth], check_variables_metadata=True, default_metadata=snap_income.metadata
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def reformat_tables(tb: Table, short_name: str) -> Table:
    """
    Rename columns, and make tables wide for further processing
    """
    # Rename unnamed columns.
    tb = tb.rename(columns={"Unnamed: 0": "indicator", "Unnamed: 1": "unit"})

    # remove rows with missing values in indicator
    tb = tb.dropna(subset=["indicator"])

    # Remove rows where data is missing for all columns except indicator and unit
    tb = tb.dropna(subset=[col for col in tb.columns if col not in ["indicator", "unit"]], how="all")

    # Transform indicator column to indicator + unit
    tb["indicator"] = tb["indicator"] + " - " + tb["unit"]

    # Drop unit column
    tb = tb.drop(columns=["unit"])

    # Make table long
    tb = tb.melt(id_vars=["indicator"], var_name="year", value_name="value")

    # Now make table wide with indicator as columns
    tb = tb.pivot(index="year", columns="indicator", values="value").reset_index()

    # Add country
    tb["country"] = "Australia"

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year"], short_name=short_name)

    return tb
