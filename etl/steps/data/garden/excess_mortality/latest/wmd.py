"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table
from owid.catalog.utils import underscore
from shared import harmonize_countries
from structlog import get_logger

from etl.data_helpers.misc import check_values_in_column
from etl.helpers import PathFinder

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Minimum and maximum years expected in data
YEAR_MIN_EXPECTED = 2015
YEAR_MAX_EXPECTED = 2024


def run() -> None:
    log.info("wmd: start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("wmd")

    # Read table from meadow dataset.
    tb = ds_meadow["wmd"].reset_index()

    #
    # Process data.
    #
    log.info("wmd: processing data")
    tb_garden = process(tb)

    # Set index
    tb_garden = tb_garden.set_index(["entity", "time", "time_unit", "age"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(
        [tb_garden],
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("wmd: end")


def process(tb: Table) -> Table:
    # Clean dataframe
    log.info("\thmd_stmf: cleaning bad values")
    tb = tb_clean(tb)
    # Check dataframe fields and values
    log.info("\thmd_stmf: initial dataframe API check")
    tb_api_check(tb)
    # Harmonize country names
    log.info("\twmd: harmonising country names")
    tb = harmonize_countries(tb, "country_name", paths.country_mapping_path)
    # Reshape dataframe
    log.info("\twmd: reshaping dataframe")
    tb = reshape_tb(tb)
    # Clean age display names
    log.info("\twmd: clean age display names")
    tb = format_age(tb)
    # Ensure columns match expected format
    log.info("\twmd: format columns in dataframe")
    tb = format_columns(tb)
    return tb


def tb_clean(tb: Table) -> Table:
    ix = tb.year == 0
    # NOTE: There are some values for FJI with year 0. Drop them as a hotfix.
    if ix.any():
        log.warning(f"\t\twmd: dropping {ix.sum()} rows with year==0")
    tb = tb[~ix]
    return tb


def tb_api_check(tb: Table) -> None:
    # Check years
    check_values_in_column(tb, "year", list(range(YEAR_MIN_EXPECTED, YEAR_MAX_EXPECTED + 1)))
    # Check time and time_unit
    check_values_in_column(tb, "time", list(range(1, 54)))
    check_values_in_column(tb, "time_unit", ["monthly", "weekly"])
    # If time_unit=="monthly", time should be in range(1, 13).
    check_values_in_column(tb[tb["time_unit"] == "monthly"], "time", list(range(1, 13)))
    # If time_unit=="weekly", time should be in range(1, 54).
    check_values_in_column(tb[tb["time_unit"] == "weekly"], "time", list(range(1, 54)))


def reshape_tb(tb: Table) -> Table:
    # Make wide [...l -> [[index], [years]]
    tb = (
        pr.pivot(
            tb,
            index=["entity", "time", "time_unit"],
            columns="year",
            values="deaths",
        )
        .sort_values(["entity", "time"])
        .reset_index()
    )
    return tb


def format_age(tb: Table) -> Table:
    """Create column `age` with value 'all_ages'."""
    return tb.assign(**{"age": "all_ages"})


def format_columns(tb: Table) -> Table:
    """Final touches."""
    # Sort columns
    cols_first = ["entity", "time", "time_unit", "age"]
    tb = tb[cols_first + sorted(col for col in tb.columns if col not in cols_first)]
    # String columns
    tb.columns = [underscore(str(col)) for col in tb.columns]
    return tb
