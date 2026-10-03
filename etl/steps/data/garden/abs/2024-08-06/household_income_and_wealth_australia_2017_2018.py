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
    ds_meadow = paths.load_dataset("household_income_and_wealth_australia_2017_2018")

    # Read table from meadow dataset.
    tb_income = ds_meadow["income"].reset_index()
    tb_wealth = ds_meadow["wealth"].reset_index()

    #
    # Process data.
    #
    tb_income = reformat_years_and_rename(tb=tb_income, short_name="income")
    tb_wealth = reformat_years_and_rename(tb=tb_wealth, short_name="wealth")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb_income, tb_wealth], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def reformat_years_and_rename(tb: Table, short_name: str) -> Table:
    """
    Reformat years as integers, and rename columns.
    Also, create spells in the case of income data.
    """
    # Reformat year, to keep only the latest year (last two characters).
    if short_name == "income":
        # Add an additional column identifying spells: split year into two columns, using the first parenthesis as a separator.
        # For example "2017(a)" will be split into "2017" and "a)".
        tb[["year", "spell"]] = tb["year"].str.split("(", expand=True)

    tb["year"] = tb["year"].str[-2:]
    tb["year"] = tb["year"].astype(int)

    # Add 2000 to the year if it is less than 50. Add 1900 if it is greater or equal to 50.
    tb["year"] = tb["year"].apply(lambda x: x + 2000 if x < 50 else x + 1900)

    if short_name == "income":
        # Additional formatting for the spell column.
        # Fill forward missing values.
        tb["spell"] = tb["spell"].ffill()

        # Factorize spell
        tb.loc[:, "spell_number"] = tb.loc[:, "spell"].factorize()[0] + 1

        # Make spell_number integer.
        tb["spell_number"] = tb["spell_number"].astype(int)

        tb = tb.format(["country", "year", "spell_number"])

    elif short_name == "wealth":
        tb = tb.format(["country", "year"])

    # Keep the columns we want to keep
    tb = tb.loc[:, COLUMNS_TO_KEEP.keys()].rename(columns=COLUMNS_TO_KEEP)

    # Multiply gini by 100 to get an index.
    tb["gini"] = tb["gini"] * 100

    return tb
