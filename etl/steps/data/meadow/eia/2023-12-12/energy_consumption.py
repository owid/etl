"""Load a snapshot and create a meadow dataset."""

import numpy as np
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Name of variable and unit as given in the raw data file.
VARIABLE_NAME = "Total energy consumption"
UNIT_NAME = "terajoules"
DATE_TIME_INTERVAL = "Annual"


def extract_variable_from_raw_eia_data(
    data_raw: Table,
    variable_name: str,
    unit_name: str,
    data_time_interval: str = "Annual",
) -> Table:
    """Extract data for a certain variable and unit from the raw EIA data (the International Energy Data obtained via
    bulk download).

    The raw data is in a json format. After reading it, the resulting table has one row per variable-country, e.g.
    `Total energy consumption, Germany, Annual`, and the data for this variable-country is given in the same row, but a
    different column. That cell with data is a list of lists, e.g.
    `[[2000, 0.5], [2001, 0.6], ...]`. This data seems to have some duplicated rows (which we will simply drop).

    This function extracts that data and creates a more convenient, long-format table indexed by country-year. It will
    also add a column of 'members', which gives the country code of countries included in each row. This may be useful
    to know how aggregate regions are defined by EIA.

    Parameters
    ----------
    data_raw : Table
        Raw EIA data.
    variable_name : str
        Name of variable to extract, as given in the raw data file.
    unit_name : str
        Name of unit to extract, as given in the raw data file.
    data_time_interval : str
        Time interval (e.g. 'Annual'), as given in the raw data file.

    Returns
    -------
    data : Table
        Extracted data for given variable and unit, as a table indexed by country-year.

    """

    columns = {
        "name": "country",
        "geography": "members",
        "data": "values",
    }
    # Keep only rows with data for the given variable and unit.
    data = data_raw[
        data_raw["name"].str.contains(variable_name, regex=False) & (data_raw["units"] == unit_name)
    ].reset_index(drop=True)

    # Select and rename columns.
    data = data.loc[:, list(columns)].rename(columns=columns)

    # Remove rows without data.
    data = data.dropna(subset=["values"])

    # Extract the country name.
    data["country"] = data["country"].str.split(f"{variable_name}, ").str[1].str.split(f", {data_time_interval}").str[0]

    # For some reason some countries are duplicated; drop those duplicates.
    data = data.drop_duplicates(subset="country", keep="last")

    # Expand the list of lists (e.g. `[[2000, 0.5], [2001, 0.6], ...]`) as one year-value per row (e.g. `[2000, 0.5]`).
    data = data.explode("values").reset_index(drop=True)

    # Separate years from values in different columns.
    data["year"] = data["values"].str[0]
    data["values"] = data["values"].str[1]

    # Missing values are given as '--' in the original data, replace them with nan.
    data["values"] = data["values"].replace("--", np.nan).astype(float)

    # Set index and sort appropriately.
    data = data.set_index(["country", "year"], verify_integrity=True).sort_index()

    return data


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("international_energy_data.zip")

    # Load raw data from snapshot.
    data_raw = snap.read_json(lines=True)

    #
    # Process data.
    #
    tb = extract_variable_from_raw_eia_data(
        data_raw=data_raw, variable_name=VARIABLE_NAME, unit_name=UNIT_NAME, data_time_interval=DATE_TIME_INTERVAL
    )

    # Change the name of the main table.
    tb.metadata.short_name = paths.short_name

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_meadow.save()
