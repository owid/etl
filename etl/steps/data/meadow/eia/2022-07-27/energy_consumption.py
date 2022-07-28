"""Meadow step to generate a dataset on total energy consumption using EIA data.

"""

import pandas as pd
from structlog import get_logger

from etl.steps.data.converters import convert_walden_metadata
from owid.catalog import Dataset, Table, TableMeta
from owid.catalog.utils import underscore_table
from owid.walden import Catalog as WaldenCatalog
from shared import VERSION

log = get_logger()

# Namespace and short name of output dataset.
NAMESPACE = "eia"
DATASET_SHORT_NAME = "energy_consumption"
# Name of variable and unit as given in the raw data file.
VARIABLE_NAME = "Total energy consumption"
UNIT_NAME = "terajoules"


def extract_variable_from_raw_eia_data(raw_data: pd.DataFrame, variable_name: str, unit_name: str,
                                       data_time_interval: str = "Annual") -> pd.DataFrame:
    """Extract data for a certain variable and unit from the raw EIA data (the International Energy Data obtained via
    bulk download).

    The raw data is in a json format. After reading it with pandas (`pd.read_json(data_file, lines=True)`), the
    dataframe has one row per variable-country, e.g. `Total energy consumption, Germany, Annual`, and the data for this
    variable-country is given in the same row, but a different column. That cell with data is a list of lists, e.g.
    `[[2000, 0.5], [2001, 0.6], ...]`. This dataframe seems to have some duplicated rows (which we will simply drop).

    This function extracts will extract that data and create a more convenient, long-format dataframe indexed by
    country-year. It will also contain a column of 'members', which gives the country code of countries included in each
    row. This may be useful to know how aggregate regions are defined by EIA.

    Parameters
    ----------
    raw_data : pd.DataFrame
        Raw EIA data.
    variable_name : str
        Name of variable to extract, as given in the raw data file.
    unit_name : str
        Name of unit to extract, as given in the raw data file.
    data_time_interval : str
        Time interval (e.g. 'Annual'), as given in the raw data file.

    Returns
    -------
    data : pd.DataFrame
        Extracted data for given variable and unit, as a dataframe indexed by country-year.

    """
    columns = {
        "name": "country",
        "geography": "members",
        "data": "values",
    }
    # Keep only rows with data for the given variable and unit.
    data = raw_data[raw_data["name"].str.contains(variable_name, regex=False) &
                    (raw_data["units"] == unit_name)].reset_index(drop=True)
    # Select and rename columns.
    data = data[list(columns)].rename(columns=columns)

    # Extract the country name.
    data["country"] = data["country"].str.split(f"{variable_name}, ").str[1].str.split(f", {data_time_interval}").str[0]

    # For some reason some countries are duplicated; drop those duplicates.
    data = data.drop_duplicates(subset="country", keep="last")

    # Expand the list of lists (e.g. `[[2000, 0.5], [2001, 0.6], ...]`) as one year-value per row (e.g. `[2000, 0.5]`).
    data = data.explode("values").reset_index(drop=True)

    # Separate years from values in different columns.
    data["year"] = data["values"].str[0]
    data["values"] = data["values"].str[1]

    # Set index and sort appropriately.
    data = data.set_index(["country", "year"], verify_integrity=True).sort_index()

    return data


def run(dest_dir: str) -> None:
    log.info(f"{DATASET_SHORT_NAME}.start")

    #
    # Load data.
    #
    # Load ingested raw data from walden.
    walden_ds = WaldenCatalog().find_one(namespace=NAMESPACE, short_name=DATASET_SHORT_NAME, version=VERSION)
    local_file = walden_ds.ensure_downloaded()
    raw_data = pd.read_csv(local_file)

    #
    # Process data.
    #
    # Extract total energy consumption from the raw data.
    data = extract_variable_from_raw_eia_data(raw_data=raw_data, variable_name=VARIABLE_NAME, unit_name=UNIT_NAME)

    #
    # Save outputs.
    #
    # Create new dataset using metadata from walden.
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_walden_metadata(walden_ds)
    ds.save()

    # Create a table in the dataset with the same metadata as the dataset.
    table_metadata = TableMeta(short_name=walden_ds.short_name, title=walden_ds.name, description=walden_ds.description)
    tb = Table(data, metadata=table_metadata)

    # Ensure all columns are lower-case and snake-case.
    tb = underscore_table(tb)

    # Add table to a dataset.
    ds.add(tb)

    log.info(f"{DATASET_SHORT_NAME}.end")
