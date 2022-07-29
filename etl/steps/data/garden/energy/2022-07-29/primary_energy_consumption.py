"""Garden step for Primary energy consumption dataset (part of the OWID Energy dataset), based on a combination of BP's
Statistical Review dataset and EIA data on energy consumption.

"""

import pandas as pd
from structlog import get_logger

from etl.paths import DATA_DIR
from owid import catalog
from owid.catalog.utils import underscore_table
from shared import CURRENT_DIR, add_population

log = get_logger()

# Namespace and dataset short name for output dataset.
NAMESPACE = "energy"
DATASET_SHORT_NAME = "primary_energy_consumption"
# Metadata file.
METADATA_PATH = CURRENT_DIR / f"{DATASET_SHORT_NAME}.meta.yml"
# Namespace, dataset short name and version for required EIA dataset (energy consumption).
EIA_NAMESPACE = "eia"
EIA_DATASET_NAME = "energy_consumption"
EIA_VERSION = "2022-07-27"
# Namespace, dataset short name and version for required BP dataset (processed Statistical Review from garden).
BP_NAMESPACE = "bp"
BP_DATASET_NAME = "statistical_review"
BP_VERSION = "2022-07-14"

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
    bp_dataset = catalog.Dataset(
        DATA_DIR / "garden" / BP_NAMESPACE / BP_VERSION / BP_DATASET_NAME
    )

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


def load_eia_data() -> catalog.Table:
    """Load EIA data from the local catalog, and rename columns conveniently.

    Returns
    -------
    eia_table : catalog.Table
        EIA data as a table with metadata.

    """
    # Load EIA energy consumption dataset.
    eia_dataset = catalog.Dataset(DATA_DIR / "garden" / EIA_NAMESPACE / EIA_VERSION / EIA_DATASET_NAME)

    # Get table.
    eia_table = eia_dataset[eia_dataset.table_names[0]].reset_index()
    eia_columns = {
        "country": "country",
        "year": "year",
        "energy_consumption": "energy_consumption",
    }
    eia_table = eia_table[list(eia_columns)].rename(columns=eia_columns)

    return eia_table


def combine_bp_and_eia_data(
    bp_table: catalog.Table, eia_table: catalog.Table
) -> pd.DataFrame:
    """Combine BP and EIA data.

    Parameters
    ----------
    bp_table : catalog.Table
        Table from BP Statistical Review dataset.
    eia_table : catalog.Table
        Table from EIA energy consumption dataset.

    Returns
    -------
    combined : pd.DataFrame
        Combined data.

    """
    # Check that there are no duplicated rows in any of the two datasets.
    assert bp_table[
        bp_table.duplicated(subset=["country", "year"])
    ].empty, "Duplicated rows in BP data."
    assert eia_table[
        eia_table.duplicated(subset=["country", "year"])
    ].empty, "Duplicated rows in EIA data."

    # Combine EIA data (which goes further back in the past) with BP data (which is more up-to-date).
    # On coincident rows, prioritise BP data.
    index_columns = ["country", "year"]
    data_columns = [col for col in bp_table.columns if col not in index_columns]
    # We should not concatenate bp and EIA data directly, since there are nans in different places.
    # Instead, go column by column, concatenate, remove nans, and then keep the BP version on duplicated rows.

    combined = pd.DataFrame({column: [] for column in index_columns})
    for variable in data_columns:
        _eia_data = eia_table[index_columns + [variable]].dropna(subset=variable)
        _bp_data = bp_table[index_columns + [variable]].dropna(subset=variable)
        _combined = pd.concat([_eia_data, _bp_data], ignore_index=True)
        # On rows where both datasets overlap, give priority to BP data.
        _combined = _combined.drop_duplicates(subset=index_columns, keep="last")
        # Combine data for different variables.
        combined = pd.merge(combined, _combined, on=index_columns, how="outer")

    # Sort data appropriately.
    combined = combined.sort_values(index_columns).reset_index(drop=True)

    return combined


def add_per_capita_variables(df: pd.DataFrame) -> pd.DataFrame:
    """Add per-capita variables to combined BP & EIA dataset.

    Parameters
    ----------
    df : pd.DataFrame
        Combined BP & EIA dataset.

    Returns
    -------
    combined : pd.DataFrame
        Combined BP & EIA dataset after adding per-capita variables.

    """
    df = df.copy()

    # Add population to data.
    combined = add_population(
        df=df,
        country_col="country",
        year_col="year",
        population_col="population",
        warn_on_missing_countries=False,
    )

    # Calculate production per capita.
    for cat in ("Coal", "Oil", "Gas"):
        combined[f"{cat} production per capita (kWh)"] = (
            combined[f"{cat} production (TWh)"] / combined["population"] * TWH_TO_KWH
        )
    combined = combined.drop(errors="raise", columns=["population"])

    return combined


def run(dest_dir: str) -> None:
    log.info(f"{DATASET_SHORT_NAME}.start")

    #
    # Load data.
    #
    # Load BP statistical review dataset.
    bp_table = load_bp_data()

    # Load EIA data on energy_consumption.
    eia_table = load_eia_data()

    #
    # Process data.
    #
    # Combine BP and EIA data.
    df = combine_bp_and_eia_data(bp_table=bp_table, eia_table=eia_table)

    # Add per-capita variables.
    df = add_per_capita_variables(df=df)

    #
    # Save outputs.
    #
    # Initialize new garden dataset.
    dataset = catalog.Dataset.create_empty(dest_dir)
    # Add metadata to dataset.
    dataset.metadata.update_from_yaml(METADATA_PATH)
    # Create new dataset in garden.
    dataset.save()

    # Create new table and add it to new dataset.
    tb_garden = underscore_table(catalog.Table(df))
    tb_garden = tb_garden.set_index(["country", "year"])
    tb_garden.update_metadata_from_yaml(METADATA_PATH, DATASET_SHORT_NAME)
    dataset.add(tb_garden)

    log.info(f"{DATASET_SHORT_NAME}.end")
