"""Load a snapshot and create a meadow dataset."""

from typing import Dict, List

from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

AGGREGATION_NAMES = {
    "All employeesa": "All employees",
    "Full-timea,b": "Full-time",
    "Part-timea,c": "Part-time",
}

GROSS_WEEKLY_EARNINGS_CATEGORY_NAME = "Gross weekly earnings (£)"

TEXTS_BELOW_TABLE = [
    "a Employees on adult rates, whose pay for the survey period was unaffected by absence. Estimates for 2020 and 2021 include employees who have been furloughed under the Coronavirus Job Retention Scheme (CJRS).",
    "b Full-time defined as employees working more than 30 paid hours per week (or 25 or more for the teaching professions).",
    "c Part-time defined as employees working 30 paid hours or less per week (or less than 25 for the teaching professions).",
    "Go to Contents",
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("hours_and_earnings_uk.xlsx")

    # Load data from snapshot.
    tb = snap.read(sheet_name="Table 5", header=2)

    #
    # Process data.

    tb = reformat_table(tb, AGGREGATION_NAMES, GROSS_WEEKLY_EARNINGS_CATEGORY_NAME, TEXTS_BELOW_TABLE)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year", "spell", "indicator", "aggregation"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def reformat_table(
    tb: Table, aggregation_names: Dict, gross_weekly_earnings_name: str, text_below_table: List[str]
) -> Table:
    """Format table to be able to process it."""

    # Rename first columms, to facilitate the next steps
    tb = tb.rename(columns={tb.columns[0]: "indicator", tb.columns[1]: "aggregation"})

    # Strip space from the first two columns
    tb["indicator"] = tb["indicator"].str.strip()
    tb["aggregation"] = tb["aggregation"].str.strip()

    # Delete all the rows in indicator containing gross_weekly_earnings_name
    tb = tb[~tb["indicator"].str.contains(gross_weekly_earnings_name, na=False, regex=False)].reset_index(drop=True)

    # Rename aggregation column with aggregation_names dict
    tb["aggregation"] = tb["aggregation"].replace(aggregation_names)

    # Fill null values in aggregation
    tb["aggregation"] = tb["aggregation"].fillna(method="ffill")

    # Delete rows with null values in indicator
    tb = tb.dropna(subset=["indicator"]).reset_index(drop=True)

    # Drop rows with indicator = ""
    tb = tb[tb["indicator"] != ""].reset_index(drop=True)

    # Remove indicator rows including text_below_table
    tb = tb[~tb["indicator"].isin(text_below_table)].reset_index(drop=True)

    # Add a new column with the name of the country
    tb["country"] = "United Kingdom"

    # Make all column names strings
    tb.columns = tb.columns.astype(str)

    # Make table long
    tb = tb.melt(id_vars=["indicator", "aggregation", "country"], var_name="year", value_name="value")

    # Create a new column, spell, with the value of year when it's not a number
    tb["spell"] = tb.loc[~tb.year.str[:4].str.isnumeric(), "year"]

    # Fill null values in spell
    tb["spell"] = tb["spell"].fillna(method="ffill")

    # Remove rows when year == spell
    tb = tb[tb["year"] != tb["spell"]]

    # Name the rest of null values in spell as "Spell 1"
    tb["spell"] = tb["spell"].fillna("Spell 1")

    # Factorize spell column
    tb["spell"] = tb["spell"].factorize()[0] + 1

    return tb
