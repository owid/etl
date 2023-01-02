"""Garden step for Fossil fuel production dataset (part of the OWID Energy dataset), based on a combination of BP's
Statistical Review dataset and Shift data on fossil fuel production.

"""

import numpy as np
import pandas as pd
from owid import catalog
from shared import CURRENT_DIR, HISTORIC_TO_CURRENT_REGION, add_population
from structlog import get_logger

from etl.paths import DATA_DIR

log = get_logger()

# Namespace and dataset short name for output dataset.
NAMESPACE = "energy"
DATASET_SHORT_NAME = "fossil_fuel_production"
# Metadata file.
METADATA_FILE_PATH = CURRENT_DIR / f"{DATASET_SHORT_NAME}.meta.yml"
# Namespace, dataset short name and version for required Shift dataset.
SHIFT_NAMESPACE = "shift"
SHIFT_DATASET_NAME = "fossil_fuel_production"
SHIFT_VERSION = "2022-07-18"
# Namespace, dataset short name and version for required BP dataset (processed Statistical Review from garden).
BP_NAMESPACE = "bp"
BP_DATASET_NAME = "statistical_review"
BP_VERSION = "2022-12-28"

# Conversion factors.
# Terawatt-hours to kilowatt-hours.
TWH_TO_KWH = 1e9


def load_bp_data() -> catalog.Table:
    """Load BP data from the local catalog, and rename columns conveniently.

    Returns
    -------
    bp_table : catalog.Table
        BP data as a table with metadata.

    """
    # Load BP Statistical Review dataset.
    bp_dataset = catalog.Dataset(DATA_DIR / "garden" / BP_NAMESPACE / BP_VERSION / BP_DATASET_NAME)

    # Get table.
    bp_table = bp_dataset[bp_dataset.table_names[0]].reset_index()
    bp_columns = {
        "country": "country",
        "year": "year",
        "coal_production__twh": "Coal production (TWh)",
        "gas_production__twh": "Gas production (TWh)",
        "oil_production__twh": "Oil production (TWh)",
    }
    bp_table = bp_table[list(bp_columns)].rename(columns=bp_columns)

    return bp_table


def load_shift_data() -> catalog.Table:
    """Load Shift data from the local catalog, and rename columns conveniently.

    Returns
    -------
    shift_table : catalog.Table
        Shift data as a table with metadata.

    """
    shift_columns = {
        "country": "country",
        "year": "year",
        "coal": "Coal production (TWh)",
        "gas": "Gas production (TWh)",
        "oil": "Oil production (TWh)",
    }
    shift_dataset = catalog.Dataset(DATA_DIR / "garden" / SHIFT_NAMESPACE / SHIFT_VERSION / SHIFT_DATASET_NAME)
    shift_table = shift_dataset[shift_dataset.table_names[0]].reset_index()
    shift_table = shift_table[list(shift_columns)].rename(columns=shift_columns)

    return shift_table


def combine_bp_and_shift_data(bp_table: catalog.Table, shift_table: catalog.Table) -> pd.DataFrame:
    """Combine BP and Shift data.

    Parameters
    ----------
    bp_table : catalog.Table
        Table from BP Statistical Review dataset.
    shift_table : catalog.Table
        Table from Shift fossil fuel production dataset.

    Returns
    -------
    combined : pd.DataFrame
        Combined data.

    """
    # Check that there are no duplicated rows in any of the two datasets.
    assert bp_table[bp_table.duplicated(subset=["country", "year"])].empty, "Duplicated rows in BP data."
    assert shift_table[shift_table.duplicated(subset=["country", "year"])].empty, "Duplicated rows in Shift data."

    # Combine Shift data (which goes further back in the past) with BP data (which is more up-to-date).
    # On coincident rows, prioritise BP data.
    index_columns = ["country", "year"]
    data_columns = [col for col in bp_table.columns if col not in index_columns]
    # We should not concatenate bp and shift data directly, since there are nans in different places.
    # Instead, go column by column, concatenate, remove nans, and then keep the BP version on duplicated rows.

    combined = pd.DataFrame({column: [] for column in index_columns})
    for variable in data_columns:
        _shift_data = shift_table[index_columns + [variable]].dropna(subset=variable)
        _bp_data = bp_table[index_columns + [variable]].dropna(subset=variable)
        _combined = pd.concat([_shift_data, _bp_data], ignore_index=True)  # type: ignore
        # On rows where both datasets overlap, give priority to BP data.
        _combined = _combined.drop_duplicates(subset=index_columns, keep="last")  # type: ignore
        # Combine data for different variables.
        combined = pd.merge(combined, _combined, on=index_columns, how="outer")

    # Sort data appropriately.
    combined = combined.sort_values(index_columns).reset_index(drop=True)

    return combined


def add_annual_change(df: pd.DataFrame) -> pd.DataFrame:
    """Add annual change variables to combined BP & Shift dataset.

    Parameters
    ----------
    df : pd.DataFrame
        Combined BP & Shift dataset.

    Returns
    -------
    combined : pd.DataFrame
        Combined BP & Shift dataset after adding annual change variables.

    """
    combined = df.copy()

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


def add_per_capita_variables(df: pd.DataFrame) -> pd.DataFrame:
    """Add per-capita variables to combined BP & Shift dataset.

    Parameters
    ----------
    df : pd.DataFrame
        Combined BP & Shift dataset.

    Returns
    -------
    combined : pd.DataFrame
        Combined BP & Shift dataset after adding per-capita variables.

    """
    df = df.copy()

    # Add population to data.
    combined = add_population(
        df=df,
        country_col="country",
        year_col="year",
        population_col="population",
        warn_on_missing_countries=False,
        regions=HISTORIC_TO_CURRENT_REGION,
    )

    # Calculate production per capita.
    for cat in ("Coal", "Oil", "Gas"):
        combined[f"{cat} production per capita (kWh)"] = (
            combined[f"{cat} production (TWh)"] / combined["population"] * TWH_TO_KWH
        )
    combined = combined.drop(errors="raise", columns=["population"])

    return combined


def remove_spurious_values(df: pd.DataFrame) -> pd.DataFrame:
    """Remove spurious infinity values.

    These values are generated when calculating the annual change of a variable that is zero or nan the previous year.

    Parameters
    ----------
    df : pd.DataFrame
        Data that may contain infinity values.

    Returns
    -------
    df : pd.DataFrame
        Corrected data.

    """
    for column in df.columns:
        issues_mask = df[column] == np.inf
        issues = df[issues_mask]
        if len(issues) > 0:
            df.loc[issues_mask, column] = np.nan

    return df


def run(dest_dir: str) -> None:
    log.info(f"{DATASET_SHORT_NAME}.start")

    #
    # Load data.
    #
    # Load BP statistical review dataset.
    bp_table = load_bp_data()

    # Load Shift data on fossil fuel production.
    shift_table = load_shift_data()

    #
    # Process data.
    #
    # Combine BP and Shift data.
    df = combine_bp_and_shift_data(bp_table=bp_table, shift_table=shift_table)

    # Add annual change.
    df = add_annual_change(df=df)

    # Add per-capita variables.
    df = add_per_capita_variables(df=df)

    # Remove spurious values.
    df = remove_spurious_values(df=df)

    # Create an appropriate index and sort conveniently.
    df = df.set_index(["country", "year"], verify_integrity=True).sort_index()

    # Create new table.
    table = catalog.Table(df, underscore=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = catalog.Dataset.create_empty(dest_dir)

    # Add table to dataset.
    table.metadata.short_name = "fossil_fuel_production"
    ds_garden.add(table)

    # Update dataset and table metadata using yaml file.
    ds_garden.update_metadata(METADATA_FILE_PATH)

    # Save dataset.
    ds_garden.save()

    log.info(f"{DATASET_SHORT_NAME}.end")
