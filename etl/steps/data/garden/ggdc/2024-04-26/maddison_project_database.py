"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table
from structlog import get_logger
from tabulate import tabulate

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define indicator columns to be used in the output dataset, and their output names.
MPD_COLUMNS = {"gdppc": "gdp_per_capita", "pop": "population", "gdp": "gdp"}

# Define extreme poverty line for sanity checks.
EXTREME_POVERTY_LINE = 1.9 * 365

# Set table format when printing
TABLEFMT = "pretty"


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("maddison_project_database")

    # Read table from meadow dataset.
    tb = ds_meadow["maddison_project_database"].reset_index()

    #
    # Process data.
    tb = adjust_pop_units_and_add_gdp(tb)

    # Remove unnecessary columns.
    tb = tb.drop(columns=["countrycode", "region"])

    tb = remove_empty_rows_and_rename_columns(tb)

    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    sanity_checks(tb)

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def adjust_pop_units_and_add_gdp(tb: Table) -> Table:
    """
    Show population in people instead of thousands and add GDP column.
    """
    tb["pop"] *= 1000

    tb["gdp"] = tb["gdppc"] * tb["pop"]

    return tb


def remove_empty_rows_and_rename_columns(tb: Table) -> Table:
    """
    Remove rows with empty values for all the indicators.
    MPD keeps ~100,000 rows with empty values for all indicators (probably for aggregation purposes).
    """

    # Drop rows with empty values for all indicators.
    tb = tb.dropna(axis=0, subset=MPD_COLUMNS.keys(), how="all", ignore_index=True)

    # Rename columns
    tb = tb.rename(columns=MPD_COLUMNS, errors="raise")

    return tb


def sanity_checks(tb: Table) -> None:
    """
    Check if values are negative, zero or there are too many values under subsistence levels.
    """

    tb = tb.copy()

    # Negative values
    for col in MPD_COLUMNS.values():
        mask = tb[col] < 0

        tb_error = tb[mask].reset_index(drop=True).copy()

        if not tb_error.empty:
            log.fatal(
                f"""There are {len(tb_error)} observations with negative values in {col}! In
                {tabulate(tb_error[['country', 'year', col]], headers = 'keys', tablefmt = TABLEFMT)}"""
            )

    # Zero values
    for col in MPD_COLUMNS.values():
        mask = tb[col] == 0

        tb_error = tb[mask].reset_index(drop=True).copy()

        if not tb_error.empty:
            log.fatal(
                f"""There are {len(tb_error)} observations with zero values in {col}! In
                {tabulate(tb_error[['country', 'year', col]], headers = 'keys', tablefmt = TABLEFMT)}"""
            )

    # Subsistence levels
    mask = tb["gdp_per_capita"] < EXTREME_POVERTY_LINE

    tb_error = tb[mask].reset_index(drop=True).copy()

    list_of_countries = list(tb_error.country.unique())

    if not tb_error.empty:
        log.warning(
            f"""There are {len(tb_error)} observations with values under subsistence levels (${EXTREME_POVERTY_LINE}) for GDP per capita. For these {len(list_of_countries)} countries: {list_of_countries}
            {tabulate(tb_error[['country', 'year', 'gdp_per_capita']].sort_values('gdp_per_capita').reset_index(drop=True), headers = 'keys', tablefmt = TABLEFMT)}"""
        )

    return None
