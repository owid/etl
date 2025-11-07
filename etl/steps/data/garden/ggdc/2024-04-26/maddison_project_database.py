"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table
from owid.datautils.dataframes import map_series
from structlog import get_logger
from tabulate import tabulate

from etl.helpers import PathFinder

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


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("maddison_project_database")

    # Read table from meadow dataset.
    tb = ds_meadow.read("maddison_project_database")

    #
    # Process data.
    #
    # Convert units of population, from thousands to people.
    tb["pop"] *= 1000

    # Add GDP column.
    tb["gdp"] = tb["gdppc"] * tb["pop"]

    # Remove unnecessary columns.
    tb = tb.drop(columns=["countrycode"])

    # Drop rows with empty values for all indicators.
    # MPD keeps ~100,000 rows with empty values for all indicators (probably for aggregation purposes).
    tb = tb.dropna(axis=0, subset=MPD_COLUMNS.keys(), how="all", ignore_index=True)

    # Rename columns.
    tb = tb.rename(columns=MPD_COLUMNS, errors="raise")

    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    # Sanity checks.
    sanity_checks(tb)

    # Rename Western Offshoots -> Western offshoots in the region column.
    tb["region"] = map_series(
        series=tb["region"],
        mapping={"Western Offshoots": "Western offshoots"},
        warn_on_missing_mappings=False,
        warn_on_unused_mappings=True,
    )

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


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
