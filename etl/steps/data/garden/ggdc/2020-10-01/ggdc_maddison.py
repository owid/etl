"""Load dataset for the Maddison Project Database from walden, process it, and transfer it to garden.

Current dataset assumes the following approximate mapping:
* "U.R. of Tanzania: Mainland" -> "Tanzania" (ignoring Zanzibar).

Definitions according to the Notes in the data file:
* "gdppc": Real GDP per capita in 2011$.
* "pop": Population, mid-year (thousands).

"""

import json
import yaml
from typing import cast, Dict

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from owid.walden import Catalog
from pathlib import Path

from etl.paths import STEP_DIR
from etl.steps.data.converters import convert_walden_metadata


# Institution name.
NAMESPACE = "ggdc"
# Dataset name.
DATASET_NAME = "ggdc_maddison"
# Original dataset publication date.
VERSION = "2020-10-01"
# Column name for GDP in output dataset.
GDP_COLUMN = "gdp"
# Column name for GDP per capita in output dataset.
GDP_PER_CAPITA_COLUMN = "gdp_per_capita"
# Additional description to be prepended to the description given in walden.
ADDITIONAL_DESCRIPTION = """Notes:
- Tanzania refers only to Mainland Tanzania.
- Time series for former countries and territories are calculated forward in time by estimating values based on their last official borders.

"""


def load_countries() -> Dict[str, str]:
    """Load country mappings file.

    Returns
    -------
    countries : dict
        Country mappings.

    """
    # Define path to countries file.
    garden_dir = STEP_DIR / "data" / "garden" / NAMESPACE / VERSION
    # Identify countries file.
    # If none, or more than one files are found, this step will raise an error.
    (countries_file,) = list(garden_dir.glob("*countries.json"))
    # Load countries from file.
    with open(countries_file, "r") as _file:
        countries = json.loads(_file.read())

    return cast(Dict[str, str], countries)


def load_main_data(data_file: str) -> pd.DataFrame:
    """Load data from the main sheet of the original dataset.

    Note: This function does not standardize countries (since this is done later).

    Parameters
    ----------
    data_file : str or Path
        Path to original data file.

    Returns
    -------
    data : pd.DataFrame
        Data from the main sheet of the original dataset.

    """
    # Load main sheet from original excel file.
    data = pd.read_excel(data_file, sheet_name="Full data").rename(
        columns={
            "country": "country",
            "year": "year",
            "pop": "population",
            "gdppc": GDP_PER_CAPITA_COLUMN,
        },
        errors="raise",
    )[["country", "year", "population", GDP_PER_CAPITA_COLUMN]]
    # Convert units.
    data["population"] = data["population"] * 1000
    # Create column for GDP.
    data[GDP_COLUMN] = data[GDP_PER_CAPITA_COLUMN] * data["population"]

    return cast(pd.DataFrame, data)


def load_additional_data(data_file: str) -> pd.DataFrame:
    """Load regional data from the original dataset.

    Note: This function does not standardize countries (since this is done later).

    Parameters
    ----------
    data_file : str or Path
        Path to original data file.

    Returns
    -------
    additional_combined_data : pd.DataFrame
        Regional data.

    """
    # Load regional data from original excel file.
    additional_data = pd.read_excel(data_file, sheet_name="Regional data", skiprows=1)[
        1:
    ]

    # Prepare additional population data.
    population_columns = [
        "Region",
        "Western Europe.1",
        "Western Offshoots.1",
        "Eastern Europe.1",
        "Latin America.1",
        "Asia (South and South-East).1",
        "Asia (East).1",
        "Middle East.1",
        "Sub-Sahara Africa.1",
        "World",
    ]
    additional_population_data = additional_data[population_columns]
    additional_population_data.columns = [
        region.replace(".1", "") for region in additional_population_data.columns
    ]
    additional_population_data = additional_population_data.melt(
        id_vars="Region", var_name="country", value_name="population"
    ).rename(columns={"Region": "year"})

    # Prepare additional GDP data.
    gdp_columns = [
        "Region",
        "Western Europe",
        "Eastern Europe",
        "Western Offshoots",
        "Latin America",
        "Asia (East)",
        "Asia (South and South-East)",
        "Middle East",
        "Sub-Sahara Africa",
        "World GDP pc",
    ]
    additional_gdp_data = additional_data[gdp_columns].rename(
        columns={"World GDP pc": "World"}
    )
    additional_gdp_data = additional_gdp_data.melt(
        id_vars="Region", var_name="country", value_name=GDP_PER_CAPITA_COLUMN
    ).rename(columns={"Region": "year"})

    # Merge additional population and GDP data.
    additional_combined_data = pd.merge(
        additional_population_data,
        additional_gdp_data,
        on=["year", "country"],
        how="inner",
    )
    # Convert units.
    additional_combined_data["population"] = (
        additional_combined_data["population"] * 1000
    )

    # Create column for GDP.
    additional_combined_data[GDP_COLUMN] = (
        additional_combined_data[GDP_PER_CAPITA_COLUMN]
        * additional_combined_data["population"]
    )

    assert len(additional_combined_data) == len(additional_population_data)
    assert len(additional_combined_data) == len(additional_gdp_data)

    return additional_combined_data


def generate_ggdc_data(data_file: str) -> pd.DataFrame:
    """Load and process GGDC data (including standardizing country names).

    Parameters
    ----------
    data_file : str or Path
        Path to original data file.

    Returns
    -------
    combined : pd.DataFrame
        Processed GGDC data.

    """
    # Load main and additional GDP data.
    gdp_data = load_main_data(data_file=data_file)
    additional_data = load_additional_data(data_file=data_file)

    # Combine both dataframes.
    combined = pd.concat([gdp_data, additional_data], ignore_index=True).dropna(
        how="all", subset=[GDP_PER_CAPITA_COLUMN, "population", GDP_COLUMN]
    )

    # Standardize country names.
    countries = load_countries()
    combined["country"] = combined["country"].replace(countries)

    # Sort rows and columns conveniently.
    combined = combined.sort_values(["country", "year"]).reset_index(drop=True)[
        ["country", "year", GDP_PER_CAPITA_COLUMN, "population", GDP_COLUMN]
    ]

    # Some rows have spurious zero GDP. Convert them into nan.
    zero_gdp_rows = combined[GDP_COLUMN] == 0
    if zero_gdp_rows.any():
        combined.loc[zero_gdp_rows, [GDP_COLUMN, GDP_PER_CAPITA_COLUMN]] = np.nan

    return combined


DATA_PATH = "snapshots/ggdc/2020-10-01/ggdc_maddison/ggdc_maddison.xlsx"
META_PATH = "snapshots/ggdc/2020-10-01/ggdc_maddison/ggdc_maddison.meta.yml"


# NOTE: dotdict is a hack to avoid rewriting `convert_walden_metadata`
# this would be obviously done differently in production (e.g. by loading
# metadata into dataclass / pydantic with validation)
class dotdict(dict):
    """dot.notation access to dictionary attributes"""

    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__


def run(dest_dir: str) -> None:
    # Initialise dataset.
    ds = Dataset.create_empty(dest_dir)

    with open(META_PATH, "r") as istream:
        meta = dotdict(yaml.safe_load(istream))

    # Assign the same metadata of the walden dataset to this dataset.
    ds.metadata = convert_walden_metadata(meta)

    # Load and process data.
    df = generate_ggdc_data(data_file=DATA_PATH)

    # Set meaningful indexes.
    df = df.set_index(["country", "year"])

    # Create a new table with the processed data.
    t = Table(df)

    # Update metadata
    meta_path = Path(__file__).parent / "ggdc_maddison.meta.yml"
    ds.metadata.update_from_yaml(meta_path)
    t.update_metadata_from_yaml(meta_path, "maddison_gdp")
    ds.metadata.description = ADDITIONAL_DESCRIPTION + ds.metadata.description

    assert len(ds.metadata.sources) == 1

    # Add table to current dataset.
    ds.add(t)

    # Save dataset to garden.
    ds.save()
