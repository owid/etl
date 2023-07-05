"""Garden step for European Electricity Review (Ember, 2022).

"""

from typing import cast

import pandas as pd
from owid import catalog
from owid.datautils import dataframes
from shared import CURRENT_DIR

from etl.data_helpers import geo
from etl.helpers import PathFinder
from etl.paths import DATA_DIR

paths = PathFinder(__file__)

# Details for dataset to export.
DATASET_SHORT_NAME = "european_electricity_review"
# Details for dataset to import.
MEADOW_DATASET_PATH = DATA_DIR / f"meadow/ember/2022-08-01/{DATASET_SHORT_NAME}"

COUNTRY_MAPPING_PATH = CURRENT_DIR / f"{DATASET_SHORT_NAME}.countries.json"
METADATA_PATH = CURRENT_DIR / f"{DATASET_SHORT_NAME}.meta.yml"

# Convert from megawatt-hours to kilowatt-hours.
MWH_TO_KWH = 1000


def process_net_flows_data(table: catalog.Table, countries_regions: catalog.Table) -> catalog.Table:
    """Process net flows data, including country harmonization.

    Parameters
    ----------
    table : catalog.Table
        Table from the meadow dataset on net flows.
    countries_regions : catalog.Table
        Table from the owid countries-regions dataset.

    Returns
    -------
    table: catalog.Table
        Processed table.

    """
    df = pd.DataFrame(table).reset_index()

    # Create dictionary mapping country codes to harmonized country names.
    country_code_to_name = countries_regions[["name"]].to_dict()["name"]
    # Add Kosovo, which is missing in countries-regions.
    if "XKX" not in country_code_to_name:
        country_code_to_name["XKX"] = "Kosovo"

    columns = {
        "source_country_code": "source_country",
        "target_country_code": "target_country",
        "year": "year",
        "net_flow_twh": "net_flow__twh",
    }
    df = df[list(columns)].rename(columns=columns)
    # Change country codes to harmonized country names in both columns.
    for column in ["source_country", "target_country"]:
        df[column] = dataframes.map_series(
            series=df[column],
            mapping=country_code_to_name,
            warn_on_missing_mappings=True,
            show_full_warning=True,
        )

    table = (
        catalog.Table(df).set_index(["source_country", "target_country", "year"], verify_integrity=True).sort_index()
    )

    return table


def process_generation_data(table: catalog.Table) -> catalog.Table:
    """Process electricity generation data, including country harmonization.

    Parameters
    ----------
    table : catalog.Table
        Table from the meadow dataset on electricity generation.

    Returns
    -------
    table: catalog.Table
        Processed table.

    """
    df = pd.DataFrame(table).reset_index()

    # Sanity checks.
    error = "Columns fuel_code and fuel_desc have inconsistencies."
    assert df.groupby("fuel_code").agg({"fuel_desc": "nunique"})["fuel_desc"].max() == 1, error
    assert df.groupby("fuel_desc").agg({"fuel_code": "nunique"})["fuel_code"].max() == 1, error

    # Select useful columns and rename them conveniently.
    columns = {
        "country_name": "country",
        "year": "year",
        "fuel_desc": "fuel_desc",
        "generation_twh": "TWh",
        "share_of_generation_pct": "%",
    }
    # Convert from long to wide format dataframe.
    df = df[list(columns)].rename(columns=columns).pivot(index=["country", "year"], columns=["fuel_desc"])

    # Collapse the two column levels into one, with the naming "variable (unit)" (except for country and year, that
    # have no units).
    df.columns = [f"{variable} ({unit})" for unit, variable in df.columns]

    # Harmonize country names.
    df = geo.harmonize_countries(
        df=df.reset_index(),
        countries_file=str(COUNTRY_MAPPING_PATH),
        warn_on_unused_countries=False,
        warn_on_missing_countries=True,
    )

    # Create a table with a well-constructed index.
    table = catalog.Table(df).set_index(["country", "year"], verify_integrity=True).sort_index()

    return table


def process_country_overview_data(table: catalog.Table) -> catalog.Table:
    """Process country overview data, including country harmonization.

    Parameters
    ----------
    table : catalog.Table
        Table from the meadow dataset.

    Returns
    -------
    table: catalog.Table
        Processed table.

    """
    # Rename columns for consistency with global electricity review.
    columns = {
        "country_name": "country",
        "year": "year",
        "generation_twh": "total_generation__twh",
        "net_import_twh": "net_imports__twh",
        "demand_twh": "demand__twh",
        "demand_mwh_per_capita": "demand_per_capita__kwh",
    }
    df = pd.DataFrame(table).reset_index()[list(columns)].rename(columns=columns)
    # Harmonize country names.
    df = geo.harmonize_countries(
        df=df,
        countries_file=str(COUNTRY_MAPPING_PATH),
        warn_on_unused_countries=False,
        warn_on_missing_countries=True,
    )

    # Convert units of demand per capita.
    df["demand_per_capita__kwh"] = df["demand_per_capita__kwh"] * MWH_TO_KWH

    # Create a table with a well-constructed index.
    table = catalog.Table(df).set_index(["country", "year"], verify_integrity=True).sort_index()

    return table


def process_emissions_data(table: catalog.Table) -> catalog.Table:
    """Process emissions data, including country harmonization.

    Parameters
    ----------
    table : catalog.Table
        Table from the meadow dataset on emissions.

    Returns
    -------
    table: catalog.Table
        Processed table.

    """
    # Rename columns for consistency with global electricity review.
    columns = {
        "country_name": "country",
        "year": "year",
        "emissions_intensity_gco2_kwh": "co2_intensity__gco2_kwh",
        "emissions_mtc02e": "total_emissions__mtco2",
    }
    df = pd.DataFrame(table).reset_index()[list(columns)].rename(columns=columns)
    # Harmonize country names.
    df = geo.harmonize_countries(
        df=df,
        countries_file=str(COUNTRY_MAPPING_PATH),
        warn_on_unused_countries=False,
        warn_on_missing_countries=True,
    )

    # Create a table with a well-constructed index.
    table = catalog.Table(df).set_index(["country", "year"], verify_integrity=True).sort_index()

    return table


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Read dataset from meadow.
    ds_meadow = catalog.Dataset(MEADOW_DATASET_PATH)

    # Load countries-regions table (required to convert country codes to country names in net flows table).
    countries_regions = cast(catalog.Dataset, paths.load_dependency("regions"))["regions"]

    #
    # Process data.
    #
    # Process each individual table.
    tables = {
        "Country overview": process_country_overview_data(table=ds_meadow["country_overview"]),
        "Emissions": process_emissions_data(table=ds_meadow["emissions"]),
        "Generation": process_generation_data(table=ds_meadow["generation"]),
        "Net flows": process_net_flows_data(table=ds_meadow["net_flows"], countries_regions=countries_regions),
    }

    #
    # Save outputs.
    #
    ds_garden = catalog.Dataset.create_empty(dest_dir)
    # Import metadata from meadow dataset and update attributes using the metadata yaml file.
    ds_garden.metadata = ds_meadow.metadata
    ds_garden.metadata.update_from_yaml(METADATA_PATH, if_source_exists="replace")
    # Create dataset.
    ds_garden.save()

    # Add all tables to dataset.
    for table_name in list(tables):
        table = tables[table_name]
        # Make column names snake lower case.
        table = catalog.utils.underscore_table(table)
        # Import metadata from meadow and update attributes that have changed.
        table.update_metadata_from_yaml(METADATA_PATH, catalog.utils.underscore(table_name))
        table.metadata.title = table_name
        table.metadata.short_name = catalog.utils.underscore(table_name)
        # Add table to dataset.
        ds_garden.add(table)
