"""Garden step for Fossil fuel production dataset (part of the OWID Energy dataset), based on a combination of BP's
Statistical Review dataset and Shift data on fossil fuel production.

"""

import numpy as np
from owid.catalog import Dataset, Table
from owid.datautils import dataframes

from etl.data_helpers.geo import add_population_to_dataframe
from etl.helpers import PathFinder, create_dataset_with_combined_metadata

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Conversion factors.
# Terawatt-hours to kilowatt-hours.
TWH_TO_KWH = 1e9


def prepare_bp_data(tb_bp: Table) -> Table:
    """Prepare BP data.

    Parameters
    ----------
    tb_bp : Table
        BP data.

    Returns
    -------
    tb_bp : Table
        BP data as a table with metadata.

    """
    tb_bp = tb_bp.reset_index()

    bp_columns = {
        "country": "country",
        "year": "year",
        "coal_production__twh": "Coal production (TWh)",
        "gas_production__twh": "Gas production (TWh)",
        "oil_production__twh": "Oil production (TWh)",
    }
    tb_bp = tb_bp[list(bp_columns)].rename(columns=bp_columns)

    return tb_bp


def prepare_shift_data(tb_shift: Table) -> Table:
    """Prepare Shift data.

    Parameters
    ----------
    tb_shift : Table
        Shift data.

    Returns
    -------
    shift_table : Table
        Shift data as a table with metadata.

    """
    tb_shift = tb_shift.reset_index()

    shift_columns = {
        "country": "country",
        "year": "year",
        "coal": "Coal production (TWh)",
        "gas": "Gas production (TWh)",
        "oil": "Oil production (TWh)",
    }
    tb_shift = tb_shift[list(shift_columns)].rename(columns=shift_columns)

    return tb_shift


def combine_bp_and_shift_data(tb_bp: Table, tb_shift: Table) -> Table:
    """Combine BP and Shift data.

    Parameters
    ----------
    tb_bp : Table
        Processed BP table.
    tb_shift : Table
        Process Shift table.

    Returns
    -------
    combined : Table
        Combined data.

    """
    # Check that there are no duplicated rows in any of the two datasets.
    assert tb_bp[tb_bp.duplicated(subset=["country", "year"])].empty, "Duplicated rows in BP data."
    assert tb_shift[tb_shift.duplicated(subset=["country", "year"])].empty, "Duplicated rows in Shift data."

    # Combine Shift data (which goes further back in the past) with BP data (which is more up-to-date).
    # On coincident rows, prioritise BP data.
    index_columns = ["country", "year"]
    combined = dataframes.combine_two_overlapping_dataframes(df1=tb_bp, df2=tb_shift, index_columns=index_columns)

    # Remove rows that only have nan.
    combined = combined.dropna(subset=combined.drop(columns=["country", "year"]).columns, how="all")

    # Sort data appropriately.
    combined = combined.sort_values(index_columns).reset_index(drop=True)

    return combined


def add_annual_change(tb: Table) -> Table:
    """Add annual change variables to combined BP & Shift dataset.

    Parameters
    ----------
    tb : Table
        Combined BP & Shift dataset.

    Returns
    -------
    combined : Table
        Combined BP & Shift dataset after adding annual change variables.

    """
    combined = tb.copy()

    # Calculate annual change.
    combined = combined.sort_values(["country", "year"]).reset_index(drop=True)
    for cat in ("Coal", "Oil", "Gas"):
        combined[f"Annual change in {cat.lower()} production (%)"] = (
            combined.groupby("country")[f"{cat} production (TWh)"].pct_change() * 100
        )
        combined[f"Annual change in {cat.lower()} production (TWh)"] = combined.groupby("country")[
            f"{cat} production (TWh)"
        ].diff()

    return combined


def add_per_capita_variables(tb: Table, ds_population: Dataset) -> Table:
    """Add per-capita variables to combined BP & Shift dataset.

    Parameters
    ----------
    tb : Table
        Combined BP & Shift dataset.
    ds_population : Dataset
        Population dataset.

    Returns
    -------
    combined : Table
        Combined BP & Shift dataset after adding per-capita variables.

    """
    tb = tb.copy()

    # List countries for which we expect to have no population.
    # These are countries and regions defined by BP and Shift.
    expected_countries_without_population = [
        country for country in tb["country"].unique() if (("(BP)" in country) or ("(Shift)" in country))
    ]
    # Add population to data.
    combined = add_population_to_dataframe(
        df=tb,
        ds_population=ds_population,
        country_col="country",
        year_col="year",
        population_col="population",
        warn_on_missing_countries=False,
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


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load BP statistical review dataset and read its main table.
    ds_bp: Dataset = paths.load_dependency("statistical_review")
    tb_bp = ds_bp["statistical_review"]

    # Load Shift dataset and read its main table.
    ds_shift: Dataset = paths.load_dependency("fossil_fuel_production")
    tb_shift = ds_shift["fossil_fuel_production"]

    # Load population dataset.
    ds_population: Dataset = paths.load_dependency("population")

    #
    # Process data.
    #
    # Prepare BP data.
    tb_bp = prepare_bp_data(tb_bp=tb_bp)

    # Prepare Shift data on fossil fuel production.
    tb_shift = prepare_shift_data(tb_shift=tb_shift)

    # Combine BP and Shift data.
    tb = combine_bp_and_shift_data(tb_bp=tb_bp, tb_shift=tb_shift)

    # Add annual change.
    tb = add_annual_change(tb=tb)

    # Add per-capita variables.
    tb = add_per_capita_variables(tb=tb, ds_population=ds_population)

    # Remove spurious values and rows that only have nans.
    tb = remove_spurious_values(tb=tb)

    # Create an appropriate index and sort conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    # Update table name.
    tb.metadata.short_name = paths.short_name

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset_with_combined_metadata(dest_dir, datasets=[ds_bp, ds_shift], tables=[tb])
    ds_garden.save()
