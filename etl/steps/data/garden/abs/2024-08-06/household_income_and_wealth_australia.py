"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define columns to keep and their new names.
COLUMNS_TO_KEEP = {"gini_coefficient__no": "gini"}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("household_income_and_wealth_australia")

    # Read table from meadow dataset.
    tb_income = ds_meadow["income"].reset_index()
    tb_wealth = ds_meadow["wealth"].reset_index()

    #
    # Process data.
    #
    tb_income = reformat_years_and_rename(tb_income)
    tb_wealth = reformat_years_and_rename(tb_wealth)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb_income, tb_wealth], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def reformat_years_and_rename(tb: Table) -> Table:
    """
    Reformat years as integers, and rename columns.
    """
    # Reformat year, to keep only the latest year (last two characters).
    tb["year"] = tb["year"].str[-2:]
    tb["year"] = tb["year"].astype(int)

    # Add 2000 to the year if it is less than 50. Add 1900 if it is greater or equal to 50.
    tb["year"] = tb["year"].apply(lambda x: x + 2000 if x < 50 else x + 1900)

    tb = tb.format(["country", "year"])

    # Keep the columns we want to keep.
    tb = tb.loc[:, COLUMNS_TO_KEEP.keys()].rename(columns=COLUMNS_TO_KEEP)

    # Multiply gini by 100 to get an index.
    tb["gini"] = tb["gini"] * 100

    return tb
