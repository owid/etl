"""Load a meadow dataset and create a garden dataset."""

from typing import List

import owid.catalog.processing as pr
from owid.catalog import Table
from structlog import get_logger
from tabulate import tabulate

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Initialize logger.
log = get_logger()

# Define new names for categories
INDICATOR_NAMES = {
    "Gini (disposable income)": "gini_disposable",
    "Gini (gross income)": "gini_gross",
    "Gini (market income)": "gini_market",
    "P50/P10 disposable income decile ratio": "p50_p10_ratio_disposable",
    "P90/P10 disposable income decile ratio": "p90_p10_ratio_disposable",
    "P90/P50 disposable income decile ratio": "p90_p50_ratio_disposable",
    "Palma ratio (disposable income)": "palma_ratio_disposable",
    "Quintile share ratio (disposable income)": "s80_s20_ratio_disposable",
    "Poverty rate based on disposable income": "headcount_ratio_disposable",
    "Poverty rate based on market income": "headcount_ratio_market",
}

POVERTY_LINES = {
    "Not applicable": "not_applicable",
    "50% of the national\xa0median disposable income": "50_median",
    "60% of the national\xa0median disposable income": "60_median",
}

AGE_GROUPS = {"From 18 to 65 years": "Working population", "Over 65 years": "Over 65 years", "Total": "Total"}

# Set table format when printing
TABLEFMT = "pretty"


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("income_distribution_database")

    # Read table from meadow dataset.
    tb = ds_meadow["income_distribution_database"].reset_index()

    #
    # Process data.
    tb = rename_and_create_columns(tb)

    tb = create_relative_poverty_columns(tb)

    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    sanity_checks(tb)

    tb = tb.format(["country", "year", "age"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def rename_and_create_columns(tb: Table) -> Table:
    """
    Rename categories in measure, poverty_line and age columns and make the table wide.
    Also, add a gini_reduction column.
    """
    # Assert if all keys of dictionary are in the columns.
    assert set(INDICATOR_NAMES.keys()) == set(tb["measure"]), "Not all expected categories are in the measure column"
    assert set(POVERTY_LINES.keys()) == set(
        tb["poverty_line"]
    ), "Not all expected categories are in the poverty_line column"
    assert set(AGE_GROUPS.keys()) == set(tb["age"]), "Not all expected categories are in the age column"

    # Rename categories in measure, poverty_line and age columns.
    tb["measure"] = tb["measure"].replace(INDICATOR_NAMES)
    tb["poverty_line"] = tb["poverty_line"].replace(POVERTY_LINES)
    tb["age"] = tb["age"].replace(AGE_GROUPS)

    # Make the table wide, using measure as columns.
    tb = tb.pivot(index=["country", "year", "poverty_line", "age"], columns="measure", values="value").reset_index()

    # Create a variable that calculates the reduction from gini_market to gini_disposable
    tb["gini_reduction"] = (tb["gini_market"] - tb["gini_disposable"]) / tb["gini_market"] * 100

    return tb


def create_relative_poverty_columns(tb: Table) -> Table:
    """
    Pivot table for headcount ratios and create multiple relative poverty columns with the poverty lines.
    """

    tb_inequality = tb.copy()
    tb_poverty = tb.copy()

    # Filter poverty_line column
    tb_inequality = tb_inequality[tb_inequality["poverty_line"] == "not_applicable"].reset_index(drop=True)
    tb_poverty = tb_poverty[tb_poverty["poverty_line"] != "not_applicable"].reset_index(drop=True)

    # Define columns for both tables: tb_inequality has all the columns not containing headcount_ratio
    # tb_poverty has all the columns containing headcount_ratio
    inequality_columns = [c for c in tb_inequality.columns if "headcount_ratio" not in c]
    poverty_columns = [c for c in tb_poverty.columns if "headcount_ratio" in c]

    tb_inequality = tb_inequality[inequality_columns]
    tb_poverty = tb_poverty[["country", "year", "poverty_line", "age"] + poverty_columns]

    # Make tb_poverty wider
    tb_poverty = tb_poverty.pivot(
        index=["country", "year", "age"], columns="poverty_line", values=poverty_columns, join_column_levels_with="_"
    ).reset_index(drop=True)

    # Remove poverty_line column in tb_inequality
    tb_inequality = tb_inequality.drop(columns=["poverty_line"], errors="raise")

    # Merge both tables
    tb = pr.merge(tb_inequality, tb_poverty, on=["country", "year", "age"])

    return tb


def sanity_checks(tb: Table) -> None:
    """Run several sanity checks on the table."""

    # Define headcount ratio columns
    headcount_ratio_columns = [c for c in tb.columns if "headcount_ratio" in c]

    # Divide headcount_ratio columns by 100
    tb[headcount_ratio_columns] = tb[headcount_ratio_columns] / 100

    check_between_0_and_1(
        tb,
        [
            "gini_disposable",
            "gini_gross",
            "gini_market",
        ]
        + headcount_ratio_columns,
    )

    # Multiply headcount_ratio columns by 100
    tb[headcount_ratio_columns] = tb[headcount_ratio_columns] * 100

    check_negative_values(tb)

    return None


def check_between_0_and_1(tb: Table, variables: List[str]) -> None:
    """
    Check that indicators are between 0 and 1
    """

    tb = tb.copy()

    for v in variables:
        # Filter only values lower than 0 or higher than 1
        mask = (tb[v] > 1) | (tb[v] < 0)
        tb_error = tb[mask].copy().reset_index()

        if not tb_error.empty:
            log.fatal(
                f"""Values for {v} are not between 0 and 1:
                {tabulate(tb_error[['country', 'year', 'poverty_line', 'age', v]], headers = 'keys', tablefmt = TABLEFMT)}"""
            )

    return None


def check_negative_values(tb: Table) -> None:
    """
    Check if there are negative values in the variables
    """

    tb = tb.copy()

    # Define variables: all in the table, except for country, year and age
    variables = [c for c in tb.columns if c not in ["country", "year", "age"]]

    for v in variables:
        # Create a mask to check if any value is negative
        mask = tb[v] < 0
        tb_error = tb[mask].reset_index(drop=True).copy()

        if not tb_error.empty:
            log.fatal(
                f"""{len(tb_error)} observations for {v} are negative:
                {tabulate(tb_error[['country', 'year', 'poverty_line', 'age', v]], headers = 'keys', tablefmt = TABLEFMT)}"""
            )

    return None
