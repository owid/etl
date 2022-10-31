"""WB Gender Meadow step."""
import re
import tempfile
from pathlib import Path
from typing import Any, Tuple

import pandas as pd
import structlog
from owid import walden
from owid.catalog import Dataset, Table, utils
from owid.catalog.utils import underscore
from owid.datautils.io import decompress_file

from etl.steps.data.converters import convert_walden_metadata

log = structlog.get_logger()

# Dataset details
NAMESPACE = "wb"
VERSION = "2022-10-29"
SHORT_NAME = "wb_gender"

# Renaming of column fields in data
COLUMNS_DATA_RENAME = {
    "Country Name": "country",
    "Indicator Name": "variable",
    "Indicator Code": "variable_code",
}
# Get list of relevant columns from data file
COLUMNS_RELEVANT = list(COLUMNS_DATA_RENAME.keys()) + [str(i) for i in range(1960, 2022)]
# Get list of not relevant columns from data file
COLUMNS_NOT_RELEVANT = [
    "Country Code",
    "Unnamed: 66",
]
# Get list of known columns from data file (for sanity checks)
COLUMNS_KNOWN = COLUMNS_RELEVANT + COLUMNS_NOT_RELEVANT
# Get list of known columns from data file (for sanity checks)
COLUMNS_IDX = [c for c in COLUMNS_DATA_RENAME.values() if c not in "variable_code"] + ["year"]
# Name of the data file
DATA_FILENAME = "Gender_StatsData.csv"
# Name of the metadata (variables) file
METADATA_VARIABLES_FILENAME = "Gender_StatsSeries.csv"
# Name of the metadata (countries) file
METADATA_COUNTRIES_FILENAME = "Gender_StatsCountry.csv"


def load_data_from_walden(walden_ds: walden.Catalog) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load data and metadata."""
    # Get Walden dataset
    with tempfile.TemporaryDirectory() as f:
        output_folder = Path(f)
        # Uncompress
        log.info("Extracting data and metadata from walden zip file...")
        decompress_file(input_file=walden_ds.local_path, output_folder=output_folder, overwrite=True)
        # Read data and metadata
        table = _load_data(output_folder)
        metadata_variables = _load_metadata_variables(output_folder)
        metadata_countries = _load_metadata_countries(output_folder)
        return table, metadata_variables, metadata_countries


def _load_data(folder_path: Path) -> pd.DataFrame:
    """Load data from file, as DataFrame."""
    dtypes = {
        "Country Name": "category",
        "Country Code": "category",
        "Indicator Name": "category",
        "Indicator Code": "category",
    }
    df = pd.read_csv(folder_path / DATA_FILENAME, dtype=dtypes)
    # Sanity check
    _sanity_check_columns_data(df)
    return df


def _load_metadata_variables(folder_path: Path) -> pd.DataFrame:
    """Load metadata (variables) from file, as DataFrame."""
    dtypes = {
        "Indicator Name": "category",
    }
    df = pd.read_csv(folder_path / METADATA_VARIABLES_FILENAME, dtype=dtypes)
    return df


def _load_metadata_countries(folder_path: Path) -> pd.DataFrame:
    """Load metadata (countries) from file, as DataFrame."""
    dtypes = {
        "Indicator Name": "category",
    }
    df = pd.read_csv(folder_path / METADATA_COUNTRIES_FILENAME, dtype=dtypes)
    return df


def _sanity_check_columns_data(df: pd.DataFrame) -> None:
    """Run minimal sanity checks on data."""
    log.info("Checking columns in data are as expected...")
    assert not df.columns.difference(COLUMNS_KNOWN).any()
    assert not set(COLUMNS_KNOWN).difference(df.columns)


def format_data(df: pd.DataFrame, metadata_variables: pd.DataFrame) -> Any:
    """Format data.

    Output should be in long format, with four columns:
        - country: Country name.
        - variable: Variable name.
        - year: Year (starting from 1960 til 2021).
        - value: Value for the variable in the given country and year.

    Also runs checks on the data to see that it is consistent with the metadata.

    Parameters
    ----------
    df : pd.DataFrame
        Data dataframe
    metadata_variables : pd.DataFrame
        Metadata (countries) dataframe.
    """
    # Get only necessary columns
    log.info("Reshaping dataframe: Preserving only relevant columns in data...")
    df = df[COLUMNS_RELEVANT]
    # Reshape years
    log.info("Reshaping dataframe: Years in rows...")
    df = df.melt(["Country Name", "Indicator Name", "Indicator Code"], var_name="year").astype({"year": int})
    # Dropna
    log.info("Reshaping dataframe: Dropping NaNs...")
    df = df.dropna(subset="value").reset_index(drop=True)
    # Column rename
    log.info("Reshaping dataframe: Renaming columns...")
    df = df.rename(columns=COLUMNS_DATA_RENAME)
    # Clean variable name
    log.info("Reshaping dataframe: Cleaning variable names...")
    df = df.assign(variable=df["variable"].apply(_clean_variable_name))
    # Check consistency between data and metadata (primarily review variable names)
    check_consistency_data_and_metadata(df, metadata_variables)
    # Final formatting
    df = final_formatting(df, metadata_variables)
    return df


def format_metadata_variables(df: pd.DataFrame) -> pd.DataFrame:
    """Format metadata (variables)."""
    # Drop columns
    log.info("Reshaping dataframe: Preserving only relevant columns in metadata (variables)...")
    df = df.drop(columns=df.filter(regex="Unnamed").columns)
    # Rename columns
    log.info("Reshaping dataframe: Renaming columns in metadata (variables)...")
    df.columns = [underscore(m) for m in df.columns]
    # Clean variable name
    log.info("Reshaping metadata dataframe: Cleaning variable names in metadata (variables)...")
    df = df.assign(indicator_name=df["indicator_name"].apply(_clean_variable_name))
    return df


def format_metadata_countries(df: pd.DataFrame) -> pd.DataFrame:
    """Format metadata (countries)."""
    # Drop columns
    log.info("Reshaping dataframe: Preserving only relevant columns in metadata (countries)...")
    df = df.drop(columns=df.filter(regex="Unnamed").columns)
    # Rename columns
    log.info("Reshaping dataframe: Renaming columns in metadata (countries)...")
    df.columns = [underscore(m) for m in df.columns]
    return df


def check_consistency_data_and_metadata(df: pd.DataFrame, metadata_variables: pd.DataFrame) -> None:
    """Check that variable names in data and metadata (variables) are consistent.

    Some variable names may differ from the data to the metadata (variables). This function checks that those
    that that differ are the ones expected.

    Parameters
    ----------
    df : pd.DataFrame
        Data dataframe.
    metadata_variables : pd.DataFrame
        Metadata dataframe.
    """
    log.info("Sanity check: Check that variable names in data and metadata (variables) are consistent...")
    # Get table with variable names for data and metadata
    df_var = df[["variable", "variable_code"]].drop_duplicates().astype(str)
    metadata_var = metadata_variables[["indicator_name", "series_code"]].astype(str)
    # Combine
    merged = df_var.merge(metadata_var, left_on="variable_code", right_on="series_code", how="outer")
    # Check no code is left unmapped
    assert (
        merged.variable_code.isna().sum() == 0
    ), "There is some variable_code missing in the data that was present in the metadata (series_code)!"
    assert (
        merged.series_code.isna().sum() == 0
    ), "There is some series_code missing in the metadata that was present in the data (variable_code)!"
    # Final check: Some variable names are not expected to be equivalent. At the moment there are 45 discrepancies, which have been reviewed.
    # If this number was to change, we would need to review the discrepancies again.
    # pd.set_option("display.max_colwidth", 40000)
    msk = merged["variable"] != merged["indicator_name"]
    x = merged.loc[msk, ["variable", "indicator_name"]].sort_values("variable")
    assert x.shape == (
        45,
        2,
    ), (
        "There are 45 expected variables to miss-match namings between data and metadata file, but a different"
        f" amount was found {x.shape}!"
    )


def final_formatting(df: pd.DataFrame, metadata_variables: pd.DataFrame) -> pd.DataFrame:
    """Final formatting of the data.

    To finalise the formatting of the data, we replace the variable names with those from metadata.
    In addition, we sort the rows and columns.

    Parameters
    ----------
    df : pd.DataFrame
        Data table.
    metadata_variables : pd.DataFrame
        Metadata (variables) table.

    Returns
    -------
    pd.DataFrame
        Formatted data table.
    """
    # Use variable names from metadata
    log.info("Replacing variable names with those from metadata...")
    df = df.merge(
        metadata_variables[["series_code", "indicator_name"]], left_on="variable_code", right_on="series_code"
    ).reset_index(drop=True)
    df = df[["country", "indicator_name", "year", "value"]].rename(columns={"indicator_name": "variable"})
    # Sort dataframe and set index
    log.info("Sorting dataframe and setting index...")
    df = df.sort_values(COLUMNS_IDX).set_index(COLUMNS_IDX)
    return df


def _clean_variable_name(text: str) -> str:
    """Clean string."""
    return re.sub(r"\s+", " ", text).replace(" 00", " 0")


def init_dataset(dest_dir: str, walden_ds: walden.Dataset) -> Dataset:
    "Initialise Meadow dataset."
    log.info("Initialising Meadow dataset...")
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_walden_metadata(walden_ds)
    ds.metadata.short_name = "wb_gender"
    ds.save()
    return ds


def add_tables_to_ds(
    ds: Dataset, df: pd.DataFrame, metadata_variables: pd.DataFrame, metadata_countries: pd.DataFrame
) -> Dataset:
    """Add tables to Meadow dataset"""
    log.info("Adding tables to Meadow dataset...")
    tables = [
        (df, "wb_gender"),
        (metadata_variables, "metadata_variables"),
        (metadata_countries, "metadata_countries"),
    ]
    for t in tables:
        table = Table(t[0])
        table.metadata.short_name = t[1]
        ds.add(utils.underscore_table(table))
    return ds


def run(dest_dir: str) -> None:
    """Run pipeline."""
    # Load data and metadata
    ds_walden = walden.Catalog().find_one(NAMESPACE, VERSION, SHORT_NAME)
    table, metadata_variables, metadata_countries = load_data_from_walden(ds_walden)
    # Format metadata (variables)
    metadata_variables = metadata_variables.pipe(format_metadata_variables)
    # Format metadata (countries)
    metadata_countries = metadata_countries.pipe(format_metadata_countries)
    # Format data
    table = table.pipe(format_data, metadata_variables)
    # Initiate dataset
    ds = init_dataset(dest_dir, ds_walden)
    # Add tables to dataset
    ds = add_tables_to_ds(ds, table, metadata_variables, metadata_countries)
