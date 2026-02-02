"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Dataset, Table
from owid.catalog import processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("prison_rates")
    ds_population = paths.load_dataset("population")
    # Read table from meadow dataset.
    tb = ds_meadow.read("prison_rates")
    # Don't keep this
    tb = tb.dropna(subset="year")

    #
    # Process data.
    #
    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)
    # Add UK aggregate
    tb = calculate_united_kingdom(tb, ds_population)

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def calculate_united_kingdom(tb: Table, ds_population: Dataset) -> Table:
    """
    Calculate data for the UK based on the component countries.

    Aggregates data from England & Wales, Northern Ireland, and Scotland.
    Requires at least 2 of 3 component countries for a given year.
    """
    uk_countries = ["England & Wales", "Northern Ireland", "Scotland"]
    pct_columns = ["pretrial_detainees_pct", "female_prisoners_pct", "juvenile_prisoners_pct", "foreign_prisoners_pct"]

    # Filter to UK component countries
    tb_uk = tb[tb["country"].isin(uk_countries)].copy()

    # Skip if no UK data available
    if tb_uk.empty:
        return tb

    # Ensure we have at least 2 of 3 UK components for meaningful aggregation
    countries_per_year = tb_uk.groupby("year")["country"].nunique()
    valid_years = countries_per_year[countries_per_year >= 2].index
    tb_uk = tb_uk[tb_uk["year"].isin(valid_years)]

    if tb_uk.empty:
        return tb

    # Convert percentages to absolute numbers
    for col in pct_columns:
        total_col = col.replace("_pct", "_total")
        tb_uk[total_col] = (tb_uk[col] / 100) * tb_uk["prison_population_total"]

    # Define columns to sum
    sum_columns = [
        "year",
        "prison_population_total",
        "number_of_institutions",
        "official_capacity",
    ] + [col.replace("_pct", "_total") for col in pct_columns]

    # Aggregate by year (min_count=1 ensures NaN if all inputs are NaN)
    tb_uk = tb_uk[sum_columns].groupby("year", as_index=False).sum(min_count=1)

    # Convert absolute numbers back to percentages
    for col in pct_columns:
        total_col = col.replace("_pct", "_total")
        tb_uk[col] = (tb_uk[total_col] / tb_uk["prison_population_total"]) * 100
        tb_uk = tb_uk.drop(columns=[total_col])

    # Add country label and calculate prison rate
    tb_uk["country"] = "United Kingdom"
    tb_uk = geo.add_population_to_table(tb_uk, ds_population=ds_population, warn_on_missing_countries=False)
    tb_uk["prison_population_rate"] = (tb_uk["prison_population_total"] / tb_uk["population"]) * 100_000
    tb_uk = tb_uk.drop(columns=["population"])

    # Combine with original data
    tb = pr.concat([tb, tb_uk], ignore_index=True)
    return tb
