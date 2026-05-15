"""Load a meadow dataset and create a garden dataset."""

from datetime import date

import owid.catalog.processing as pr
from owid.catalog import Table
from owid.catalog.utils import underscore
from shared import harmonize_countries  # ty: ignore
from structlog import get_logger

from etl.data_helpers.misc import check_values_in_column
from etl.helpers import PathFinder

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# This year
THIS_YEAR = date.today().year
# Minimum and maximum years expected in data
YEAR_MIN_EXPECTED = 1990
YEAR_MAX_EXPECTED = 2026
# Year range to be used (rest is filtered out)
YEAR_MIN = 2010
YEAR_MAX = 3000  # (No actual limit)


def run() -> None:
    log.info("hmd_stmf: start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("hmd_stmf")

    # Read table from meadow dataset.
    tb = ds_meadow["hmd_stmf"].reset_index()

    #
    # Process data.
    #
    log.info("hmd_stmf: processing data")
    tb_garden = process(tb)

    # Set index
    tb_garden = tb_garden.set_index(["entity", "week", "age"], verify_integrity=True)
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(
        tables=[tb_garden],
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("hmd_stmf: end")


def process(tb: Table) -> Table:
    # Check dataframe fields and values
    log.info("\thmd_stmf: initial dataframe API check")
    tb_api_check(tb)
    # Harmonize country names
    log.info("\thmd_stmf: harmonizing country names")
    tb = harmonize_countries(tb, "countrycode", paths.country_mapping_path, paths.excluded_countries_path)
    # Filter some rows
    log.info("\thmd_stmf: filtering entries")
    tb = filter_entries(tb)
    # Reshape dataframe
    log.info("\thmd_stmf: reshaping dataframe")
    tb = reshape_tb(tb)
    # Add UK entries (sum nations)
    log.info("\thmd_stmf: adding UK entries")
    tb = add_uk(tb)
    # Clean age display names
    log.info("\thmd_stmf: clean age display names")
    tb = format_age(tb)
    # Final touches on dataframe format
    log.info("\thmd_stmf: format columns in dataframe")
    tb = format_columns(tb)
    return tb


def tb_api_check(tb: Table) -> None:
    check_values_in_column(tb, "year", list(range(YEAR_MIN_EXPECTED, YEAR_MAX_EXPECTED + 1)))
    check_values_in_column(tb, "week", list(range(1, 54)))
    check_values_in_column(tb, "sex", ["m", "f", "b"])


def filter_entries(tb: Table) -> Table:
    """Filter some rows."""
    # Select only years YEAR_MIN - YEAR_MAX (2010-2019 (for baseline) and 2020-now)
    tb = tb[(tb["year"] >= YEAR_MIN) & (tb["year"] <= YEAR_MAX)]
    # Keep only both sex data
    tb = tb[tb["sex"] == "b"].drop(columns=["sex"])
    return tb


def reshape_tb(tb: Table) -> Table:
    """Pivot/unpivot to get data in the right format."""
    # Unpivot [Entity, Year, Week, Sex, [D*]] -> [Entity, Week, Sex, Age, [Years]]
    tb = tb.melt(
        id_vars=["entity", "year", "week"],
        value_vars=["d0_14", "d15_64", "d65_74", "d75_84", "d85p", "dtotal"],
        var_name="age",
        value_name="deaths",
    )
    # Pivot wide
    tb = pr.pivot(
        tb,
        index=["entity", "week", "age"],
        columns="year",
        values="deaths",
    ).reset_index()

    # Rename columns
    return tb


def add_uk(tb: Table):
    """Add UK to main dataframe.

    By default, the dataset only contains data for England and Wales, Scotland and Northern Ireland.
    """
    # Get UK Nations
    tb_uk = tb[tb["entity"].isin(["England and Wales", "Scotland", "Northern Ireland"])].copy()
    # Years to consider (starting from 2015)
    column_years = list(filter(lambda x: x >= 2015, tb_uk.filter(regex=r"20\d\d").columns))
    # Sanity check
    # NOTE: this used to be
    #     tb_uk[[col for col in column_years if col != THIS_YEAR]].isna().sum() < 20
    # but it started failing in 2024
    assert (tb_uk[[col for col in column_years if col <= 2023]].isna().sum() < 20).all(), (
        "Too many missing values. Check values in year columns!"
    )
    # Group by and get sum
    tb_uk = tb_uk.groupby(["week", "age"], as_index=False)[column_years].sum(min_count=3)
    # Assign Entity name
    tb_uk["entity"] = "United Kingdom"
    # Add UK to main dataset
    tb = pr.concat([tb, tb_uk], ignore_index=True)
    return tb


def format_age(tb: Table) -> Table:
    """Remove 'd' from age strings."""
    tb["age"] = tb["age"].str.replace("d", "")
    tb.loc[tb["age"] == "total", "age"] = "all_ages"
    return tb


def format_columns(tb: Table) -> Table:
    """Adapt dataframe column names to WMD-like format."""
    # Sort columns
    cols_first = ["entity", "week", "age"]
    tb = tb[cols_first + sorted(col for col in tb.columns if col not in cols_first)]
    # String columns
    tb.columns = [underscore(str(col)) for col in tb.columns]
    return tb
