"""Load dataset for the Maddison Project Database from walden, process it, and transfer it to garden.

Current dataset assumes the following approximate mapping:
* "U.R. of Tanzania: Mainland" -> "Tanzania" (ignoring Zanzibar).

Definitions according to the Notes in the data file:
* "gdppc": Real GDP per capita in 2011$.
* "pop": Population, mid-year (thousands).
"""

import numpy as np
import owid.catalog.processing as pr
from owid.catalog import Origin, Table, TableMeta

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

paths = PathFinder(__file__)

# Column name for GDP in output dataset.
GDP_COLUMN = "gdp"
# Column name for GDP per capita in output dataset.
GDP_PER_CAPITA_COLUMN = "gdp_per_capita"


def load_main_data(data: pr.ExcelFile, metadata: TableMeta, origin: Origin) -> Table:
    """Load data from the main sheet of the original dataset.

    Note: This function does not standardize countries (since this is done later).

    Parameters
    ----------
    data : ExcelFile
        Original data.

    Returns
    -------
    data : Table
        Data from the main sheet of the original dataset.

    """
    # Load main sheet from original excel file.
    data = data.parse(sheet_name="Full data", metadata=metadata, origin=origin).rename(
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

    return data


def load_additional_data(data: pr.ExcelFile, metadata: TableMeta, origin: Origin) -> Table:
    """Load regional data from the original dataset.

    Note: This function does not standardize countries (since this is done later).

    Parameters
    ----------
    data : pr.ExcelFile
        Additional data from original file.

    Returns
    -------
    additional_combined_data : Table
        Regional data.

    """
    # Load regional data from original excel file.
    additional_data = data.parse(sheet_name="Regional data", skiprows=1, metadata=metadata, origin=origin)[1:]

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
    additional_population_data = additional_population_data.rename(
        columns={region: region.replace(".1", "") for region in additional_population_data.columns}
    )
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
    additional_gdp_data = additional_data[gdp_columns].rename(columns={"World GDP pc": "World"})
    additional_gdp_data = additional_gdp_data.melt(
        id_vars="Region", var_name="country", value_name=GDP_PER_CAPITA_COLUMN
    ).rename(columns={"Region": "year"})

    # Merge additional population and GDP data.
    additional_combined_data = pr.merge(
        additional_population_data,
        additional_gdp_data,
        on=["year", "country"],
        how="inner",
    )
    # Convert units.
    additional_combined_data["population"] = additional_combined_data["population"] * 1000

    # Create column for GDP.
    additional_combined_data[GDP_COLUMN] = (
        additional_combined_data[GDP_PER_CAPITA_COLUMN] * additional_combined_data["population"]
    )

    assert len(additional_combined_data) == len(additional_population_data)
    assert len(additional_combined_data) == len(additional_gdp_data)

    return additional_combined_data


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load snapshot.
    snap = paths.load_snapshot("ggdc_maddison")

    # Read data from excel file.
    data = pr.ExcelFile(snap.path)

    #
    # Process data.
    #
    # Load main and additional GDP data.
    gdp_data = load_main_data(data=data, metadata=snap.to_table_metadata(), origin=snap.metadata.origin)
    additional_data = load_additional_data(data=data, metadata=snap.to_table_metadata(), origin=snap.metadata.origin)

    # Combine both dataframes.
    combined = pr.concat([gdp_data, additional_data], ignore_index=True, short_name="maddison_gdp").dropna(
        how="all", subset=[GDP_PER_CAPITA_COLUMN, "population", GDP_COLUMN]
    )

    # Harmonize country names.
    combined = geo.harmonize_countries(df=combined, countries_file=paths.country_mapping_path)

    # Sort rows and columns conveniently.
    combined = combined.sort_values(["country", "year"]).reset_index(drop=True)[
        ["country", "year", GDP_PER_CAPITA_COLUMN, "population", GDP_COLUMN]
    ]

    # Some rows have spurious zero GDP. Convert them into nan.
    zero_gdp_rows = combined[GDP_COLUMN] == 0
    if zero_gdp_rows.any():
        combined.loc[zero_gdp_rows, [GDP_COLUMN, GDP_PER_CAPITA_COLUMN]] = np.nan

    # Set an appropriate index.
    combined = combined.set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[combined], default_metadata=snap.metadata, check_variables_metadata=True
    )

    # Save dataset to garden.
    ds_garden.save()
