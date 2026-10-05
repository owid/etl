"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

START_YEAR = 2000  # WHO started tracking MNT elimination efforts in 2000
CURR_YEAR = 2026  # Note: change this to current year when updating


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("maternal_tetanus")

    # Read table from meadow dataset.
    tb = ds_meadow.read("maternal_tetanus")

    #
    # Process data.
    #
    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    # Expand to one row per country-year, with elimination_status reflecting each year.
    meta = tb["elimination_status"].metadata.copy()
    origins = tb["elimination_status"].metadata.origins.copy()

    tb = expand_tb_to_yearly_data(tb)

    tb["elimination_status"].metadata = meta  # ty: ignore
    tb["elimination_status"].metadata.origins = origins  # ty: ignore

    tb = tb[["country", "year", "elimination_status", "elimination_year"]]

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def expand_tb_to_yearly_data(tb: Table) -> Table:
    """The table only includes each country's current elimination status and the year it was
    eliminated (if any).
    Expand it to one row per country-year from START_YEAR to CURR_YEAR,
    recomputing elimination_status for each year. All other columns (iso3,
    elimination_year, source) are repeated unchanged across a country's years."""
    tb = tb.copy()

    years = Table(pd.DataFrame({"year": range(START_YEAR, CURR_YEAR + 1)}))
    tb = pr.merge(tb, years, how="cross")

    # Recompute status per year, preserving the column's metadata.
    eliminated_by_year = tb["elimination_year"].notna() & (tb["year"] >= tb["elimination_year"])
    tb["elimination_status"] = "Not eliminated"
    tb.loc[eliminated_by_year, "elimination_status"] = "Eliminated"

    sanity_check_outputs(tb)

    return tb


def sanity_check_outputs(tb: Table) -> None:
    n_countries = tb["country"].nunique()
    n_years = CURR_YEAR - START_YEAR + 1
    assert len(tb) == n_countries * n_years, "Unexpected row count after expanding to country-year panel."
    assert set(tb["elimination_status"]) == {"Eliminated", "Not eliminated"}, "Unexpected elimination_status values."
    assert tb["year"].between(START_YEAR, CURR_YEAR).all(), "Year out of expected range."
