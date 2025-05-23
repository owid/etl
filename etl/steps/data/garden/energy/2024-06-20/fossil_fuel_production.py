"""Garden step for Fossil fuel production dataset (part of the OWID Energy dataset), based on a combination of the
Energy Institute Statistical Review dataset and Shift data on fossil fuel production.

"""

import numpy as np
from owid.catalog import Dataset, Table
from owid.datautils import dataframes

from etl.data_helpers.geo import add_population_to_table
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Conversion factors.
# Terawatt-hours to kilowatt-hours.
TWH_TO_KWH = 1e9


def prepare_statistical_review_data(tb_review: Table) -> Table:
    """Prepare Statistical Review data.

    Parameters
    ----------
    tb_review : Table
        Statistical Review data.

    Returns
    -------
    tb_review : Table
        Selected data from the Statistical Review.

    """
    tb_review = tb_review.reset_index()

    columns = {
        "country": "country",
        "year": "year",
        "coal_production_twh": "Coal production (TWh)",
        "gas_production_twh": "Gas production (TWh)",
        "oil_production_twh": "Oil production (TWh)",
    }
    tb_review = tb_review[list(columns)].rename(columns=columns, errors="raise")

    return tb_review


def prepare_shift_data(tb_shift: Table) -> Table:
    """Prepare Shift data.

    Parameters
    ----------
    tb_shift : Table
        Shift data.

    Returns
    -------
    shift_table : Table
        Selected data from Shift.

    """
    tb_shift = tb_shift.reset_index()

    columns = {
        "country": "country",
        "year": "year",
        "coal": "Coal production (TWh)",
        "gas": "Gas production (TWh)",
        "oil": "Oil production (TWh)",
    }
    tb_shift = tb_shift[list(columns)].rename(columns=columns, errors="raise")

    return tb_shift


def combine_statistical_review_and_shift_data(tb_review: Table, tb_shift: Table) -> Table:
    """Combine Statistical Review and Shift data.

    Parameters
    ----------
    tb_review : Table
        Processed Statistical Review table.
    tb_shift : Table
        Process Shift table.

    Returns
    -------
    combined : Table
        Combined data.

    """
    # Check that there are no duplicated rows in any of the two datasets.
    assert tb_review[tb_review.duplicated(subset=["country", "year"])].empty, "Duplicated rows in Statistical Review."
    assert tb_shift[tb_shift.duplicated(subset=["country", "year"])].empty, "Duplicated rows in Shift data."

    # Combine Shift data (which goes further back in the past) with Statistical Review data (which is more up-to-date).
    # On coincident rows, prioritize Statistical Review data.
    index_columns = ["country", "year"]
    combined = dataframes.combine_two_overlapping_dataframes(df1=tb_review, df2=tb_shift, index_columns=index_columns)

    # Update the name of the new combined table.
    combined.metadata.short_name = paths.short_name

    # Remove rows that only have nan.
    combined = combined.dropna(subset=combined.drop(columns=["country", "year"]).columns, how="all")

    # Sort data appropriately.
    combined = combined.sort_values(index_columns).reset_index(drop=True)

    return combined


def add_annual_change(tb: Table) -> Table:
    """Add annual change variables to combined Statistical Review and Shift data.

    Parameters
    ----------
    tb : Table
        Combined Statistical Review and Shift data.

    Returns
    -------
    combined : Table
        Combined data after adding annual change variables.

    """
    combined = tb.copy()

    # Calculate annual change.
    combined = combined.sort_values(["country", "year"]).reset_index(drop=True)
    for cat in ("Coal", "Oil", "Gas"):
        combined[f"Annual change in {cat.lower()} production (%)"] = (
            combined.groupby("country", observed=True)[f"{cat} production (TWh)"].pct_change(fill_method=None) * 100
        )
        combined[f"Annual change in {cat.lower()} production (TWh)"] = combined.groupby("country", observed=True)[
            f"{cat} production (TWh)"
        ].diff()

    return combined


def add_per_capita_variables(tb: Table, ds_population: Dataset) -> Table:
    """Add per-capita variables to combined Statistical Review and Shift data.

    Parameters
    ----------
    tb : Table
        Combined Statistical Review and Shift data.
    ds_population : Dataset
        Population dataset.

    Returns
    -------
    combined : Table
        Combined data after adding per-capita variables.

    """
    tb = tb.copy()

    # List countries for which we expect to have no population.
    # These are countries and regions defined by the Energy Institute and Shift.
    expected_countries_without_population = [
        country for country in tb["country"].unique() if (("(EI)" in country) or ("(Shift)" in country))
    ]
    # Add population to data.
    combined = add_population_to_table(
        tb=tb,
        ds_population=ds_population,
        warn_on_missing_countries=True,
        interpolate_missing_population=True,
        expected_countries_without_population=expected_countries_without_population,
    )

    # Calculate production per capita.
    for cat in ("Coal", "Oil", "Gas"):
        combined[f"{cat} production per capita (kWh)"] = (
            combined[f"{cat} production (TWh)"] / combined["population"] * TWH_TO_KWH
        )
    combined = combined.drop(errors="raise", columns=["population"])

    return combined


def remove_spurious_values(tb: Table) -> Table:
    """Remove spurious infinity values.

    These values are generated when calculating the annual change of a variable that is zero or nan the previous year.

    Parameters
    ----------
    tb : Table
        Data that may contain infinity values.

    Returns
    -------
    tb : Table
        Corrected data.

    """
    # Replace any infinity value by nan.
    tb = tb.replace([np.inf, -np.inf], np.nan)

    # Remove rows that only have nan.
    tb = tb.dropna(subset=tb.drop(columns=["country", "year"]).columns, how="all").reset_index(drop=True)

    return tb


def run() -> None:
    #
    # Load data.
    #
    # Load Statistical Review dataset and read its main table.
    ds_review = paths.load_dataset("statistical_review_of_world_energy")
    tb_review = ds_review.read("statistical_review_of_world_energy", reset_index=False)

    # Load Shift dataset and read its main table.
    ds_shift = paths.load_dataset("energy_production_from_fossil_fuels")
    tb_shift = ds_shift.read("energy_production_from_fossil_fuels", reset_index=False)

    # Load population dataset.
    ds_population = paths.load_dataset("population")

    #
    # Process data.
    #
    # Prepare Statistical Review data.
    tb_review = prepare_statistical_review_data(tb_review=tb_review)

    # Prepare Shift data on fossil fuel production.
    tb_shift = prepare_shift_data(tb_shift=tb_shift)

    # Combine Statistical Review and Shift data.
    tb = combine_statistical_review_and_shift_data(tb_review=tb_review, tb_shift=tb_shift)

    # Add annual change.
    tb = add_annual_change(tb=tb)

    # Add per-capita variables.
    tb = add_per_capita_variables(tb=tb, ds_population=ds_population)

    # Remove spurious values and rows that only have nans.
    tb = remove_spurious_values(tb=tb)

    # Create an appropriate index and sort conveniently.
    tb = tb.format()

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
