"""Garden step that combines various datasets related to greenhouse emissions and produces the OWID CO2 dataset.

Datasets combined:
* Global Carbon Budget - Global Carbon Project.
* National contributions to climate change - Jones et al.
* Greenhouse gas emissions by sector - Climate Watch.
* Primary energy consumption - EI & EIA.

Additionally, OWID's regions dataset, population dataset and Maddison Project Database (Bolt and van Zanden, 2023) on
GDP are included.

"""

import re

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Origin, Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Conversion factor from tonnes to million tonnes.
TONNES_TO_MILLION_TONNES = 1e-6

# Select columns to use from each dataset, and how to rename them.
GCP_COLUMNS = {
    "country": "country",
    "year": "year",
    "emissions_total": "co2",
    "emissions_total_per_capita": "co2_per_capita",
    "traded_emissions": "trade_co2",
    "emissions_from_cement": "cement_co2",
    "emissions_from_cement_per_capita": "cement_co2_per_capita",
    "emissions_from_coal": "coal_co2",
    "emissions_from_coal_per_capita": "coal_co2_per_capita",
    "emissions_from_flaring": "flaring_co2",
    "emissions_from_flaring_per_capita": "flaring_co2_per_capita",
    "emissions_from_gas": "gas_co2",
    "emissions_from_gas_per_capita": "gas_co2_per_capita",
    "emissions_from_oil": "oil_co2",
    "emissions_from_oil_per_capita": "oil_co2_per_capita",
    "emissions_from_other_industry": "other_industry_co2",
    "emissions_from_other_industry_per_capita": "other_co2_per_capita",
    "pct_growth_emissions_total": "co2_growth_prct",
    "growth_emissions_total": "co2_growth_abs",
    "emissions_total_per_gdp": "co2_per_gdp",
    "emissions_total_per_unit_energy": "co2_per_unit_energy",
    "consumption_emissions": "consumption_co2",
    "consumption_emissions_per_capita": "consumption_co2_per_capita",
    "consumption_emissions_per_gdp": "consumption_co2_per_gdp",
    "cumulative_emissions_total": "cumulative_co2",
    "cumulative_emissions_from_cement": "cumulative_cement_co2",
    "cumulative_emissions_from_coal": "cumulative_coal_co2",
    "cumulative_emissions_from_flaring": "cumulative_flaring_co2",
    "cumulative_emissions_from_gas": "cumulative_gas_co2",
    "cumulative_emissions_from_oil": "cumulative_oil_co2",
    "cumulative_emissions_from_other_industry": "cumulative_other_co2",
    "pct_traded_emissions": "trade_co2_share",
    "emissions_total_as_share_of_global": "share_global_co2",
    "emissions_from_cement_as_share_of_global": "share_global_cement_co2",
    "emissions_from_coal_as_share_of_global": "share_global_coal_co2",
    "emissions_from_flaring_as_share_of_global": "share_global_flaring_co2",
    "emissions_from_gas_as_share_of_global": "share_global_gas_co2",
    "emissions_from_oil_as_share_of_global": "share_global_oil_co2",
    "emissions_from_other_industry_as_share_of_global": "share_global_other_co2",
    "cumulative_emissions_total_as_share_of_global": "share_global_cumulative_co2",
    "cumulative_emissions_from_cement_as_share_of_global": "share_global_cumulative_cement_co2",
    "cumulative_emissions_from_coal_as_share_of_global": "share_global_cumulative_coal_co2",
    "cumulative_emissions_from_flaring_as_share_of_global": "share_global_cumulative_flaring_co2",
    "cumulative_emissions_from_gas_as_share_of_global": "share_global_cumulative_gas_co2",
    "cumulative_emissions_from_oil_as_share_of_global": "share_global_cumulative_oil_co2",
    "cumulative_emissions_from_other_industry_as_share_of_global": "share_global_cumulative_other_co2",
    # New variables, related to land-use change emissions.
    "cumulative_emissions_from_land_use_change": "cumulative_luc_co2",
    "cumulative_emissions_from_land_use_change_as_share_of_global": "share_global_cumulative_luc_co2",
    "cumulative_emissions_total_including_land_use_change": "cumulative_co2_including_luc",
    "cumulative_emissions_total_including_land_use_change_as_share_of_global": "share_global_cumulative_co2_including_luc",
    "emissions_from_land_use_change": "land_use_change_co2",
    "emissions_from_land_use_change_as_share_of_global": "share_global_luc_co2",
    "emissions_from_land_use_change_per_capita": "land_use_change_co2_per_capita",
    "emissions_total_including_land_use_change": "co2_including_luc",
    "emissions_total_including_land_use_change_as_share_of_global": "share_global_co2_including_luc",
    "emissions_total_including_land_use_change_per_capita": "co2_including_luc_per_capita",
    "emissions_total_including_land_use_change_per_gdp": "co2_including_luc_per_gdp",
    "emissions_total_including_land_use_change_per_unit_energy": "co2_including_luc_per_unit_energy",
    "growth_emissions_total_including_land_use_change": "co2_including_luc_growth_abs",
    "pct_growth_emissions_total_including_land_use_change": "co2_including_luc_growth_prct",
}
JONES_COLUMNS = {
    "country": "country",
    "year": "year",
    "temperature_response_co2_total": "temperature_change_from_co2",
    "temperature_response_ghg_total": "temperature_change_from_ghg",
    "temperature_response_ch4_total": "temperature_change_from_ch4",
    "temperature_response_n2o_total": "temperature_change_from_n2o",
    "share_of_temperature_response_ghg_total": "share_of_temperature_change_from_ghg",
    # NOTE: The following columns used to come from climate watch. But Jones et al. provides a much wider coverage, and it's more up-to-date.
    "annual_emissions_ghg_fossil_co2eq": "total_ghg_excluding_lucf",
    "annual_emissions_ghg_fossil_co2eq_per_capita": "ghg_excluding_lucf_per_capita",
    "annual_emissions_ghg_total_co2eq": "total_ghg",
    "annual_emissions_ghg_total_co2eq_per_capita": "ghg_per_capita",
    "annual_emissions_ch4_total_co2eq": "methane",
    "annual_emissions_ch4_total_co2eq_per_capita": "methane_per_capita",
    "annual_emissions_n2o_total_co2eq": "nitrous_oxide",
    "annual_emissions_n2o_total_co2eq_per_capita": "nitrous_oxide_per_capita",
}
# NOTE: All climate watch indicators now come from Jones et al.
# CLIMATE_WATCH_GHG_COLUMNS = {
#     "country": "country",
#     "year": "year",
#     "total_ghg_emissions_excluding_lucf": "total_ghg_excluding_lucf",
#     "total_ghg_emissions_excluding_lucf_per_capita": "ghg_excluding_lucf_per_capita",
#     "total_ghg_emissions_including_lucf": "total_ghg",
#     "total_ghg_emissions_including_lucf_per_capita": "ghg_per_capita",
# }
# CLIMATE_WATCH_CH4_COLUMNS = {
#     "country": "country",
#     "year": "year",
#     "total_ch4_emissions_including_lucf": "methane",
#     "total_ch4_emissions_including_lucf_per_capita": "methane_per_capita",
# }
# CLIMATE_WATCH_N2O_COLUMNS = {
#     "country": "country",
#     "year": "year",
#     "total_n2o_emissions_including_lucf": "nitrous_oxide",
#     "total_n2o_emissions_including_lucf_per_capita": "nitrous_oxide_per_capita",
# }
PRIMARY_ENERGY_COLUMNS = {
    "country": "country",
    "year": "year",
    "primary_energy_consumption__twh": "primary_energy_consumption",
    "primary_energy_consumption_per_capita__kwh": "energy_per_capita",
    "primary_energy_consumption_per_gdp__kwh_per_dollar": "energy_per_gdp",
}
REGIONS_COLUMNS = {
    "name": "country",
    "iso_alpha3": "iso_code",
}
POPULATION_COLUMNS = {
    "country": "country",
    "year": "year",
    "population": "population",
}
GDP_COLUMNS = {
    "country": "country",
    "year": "year",
    "gdp": "gdp",
}

UNITS = {
    "tonnes": {"conversion": TONNES_TO_MILLION_TONNES, "new_unit": "million tonnes", "new_short_unit": "Mt"},
    "tonnes of CO₂ equivalents": {
        "conversion": TONNES_TO_MILLION_TONNES,
        "new_unit": "million tonnes",
        "new_short_unit": "Mt",
    },
}


def convert_units(table: Table) -> Table:
    """Convert units of table.

    Parameters
    ----------
    table : Table
        Data with its original units.

    Returns
    -------
    Table
        Data after converting units of specific columns.

    """
    table = table.copy()
    # Check units and convert to more convenient ones.
    for column in table.columns:
        unit = table[column].metadata.unit
        title = table[column].metadata.title
        description_short = table[column].metadata.description or table[column].metadata.description_short
        if unit in list(UNITS):
            table[column] *= UNITS[unit]["conversion"]
            table[column].metadata.unit = UNITS[unit]["new_unit"]
            table[column].metadata.short_unit = UNITS[unit]["new_short_unit"]
            table[column].metadata.title = title.replace(unit, UNITS[unit]["new_unit"])
            table[column].metadata.description_short = description_short.replace(unit, UNITS[unit]["new_unit"])

    return table


def combine_tables(
    tb_gcp: Table,
    tb_jones: Table,
    # tb_climate_watch_ghg: Table,
    # tb_climate_watch_ch4: Table,
    # tb_climate_watch_n2o: Table,
    tb_energy: Table,
    tb_gdp: Table,
    tb_population: Table,
    tb_regions: Table,
) -> Table:
    """Combine tables.

    Parameters
    ----------
    tb_gcp : Table
        Global Carbon Budget table (from Global Carbon Project).
    tb_jones : Table
        National contributions to climate change (from Jones et al. (2023)).
    # tb_climate_watch_ghg : Table
    #     Greenhouse gas emissions table (from Climate Watch).
    # tb_climate_watch_ch4 : Table
    #     CH4 emissions table (from Climate Watch).
    # tb_climate_watch_n2o : Table
    #     N2O emissions table (from Climate Watch).
    tb_energy : Table
        Primary energy consumption table (from BP & EIA).
    tb_gdp : Table
        Maddison GDP table (from GGDC).
    tb_population : Table
        OWID population table (from various sources).
    tb_regions : Table
        OWID regions table.

    Returns
    -------
    combined : Table
        Combined table with metadata and variables metadata.

    """
    # Combine main tables (with an outer join, to gather all entities from all tables).
    combined = tb_gcp.copy()
    # for table in [tb_jones, tb_climate_watch_ghg, tb_climate_watch_ch4, tb_climate_watch_n2o]:
    for table in [tb_jones]:
        combined = combined.merge(table, on=["country", "year"], how="outer", short_name=paths.short_name)

    # Add secondary tables (with a left join, to keep only entities for which we have emissions data).
    for table in [tb_energy, tb_gdp, tb_population]:
        combined = combined.merge(table, on=["country", "year"], how="left")

    # Countries-regions dataset does not have a year column, so it has to be merged on country.
    combined = combined.merge(tb_regions, on="country", how="left")

    # Check that there were no repetition in column names.
    error = "Repeated columns in combined data."
    assert len([column for column in set(combined.columns) if "_x" in column]) == 0, error

    # Adjust units.
    combined = convert_units(combined)

    return combined


def prepare_outputs(combined: Table, ds_regions: Dataset) -> Table:
    """Clean and prepare output table.

    Parameters
    ----------
    combined : Table
        Combined table.
    ds_regions : Dataset
        Regions dataset, only used to get its version.

    Returns
    -------
    combined: Table
        Cleaned combined table.

    """
    # Remove rows that only have nan (ignoring if country, year, iso_code, population and gdp do have data).
    columns_that_must_have_data = [
        column for column in combined.columns if column not in ["country", "year", "iso_code", "population", "gdp"]
    ]
    combined = combined.dropna(subset=columns_that_must_have_data, how="all").reset_index(drop=True)

    # Add metadata to the ISO column (loaded from the regions dataset).
    combined["iso_code"].m.origins = [
        Origin(
            producer="International Organization for Standardization",
            title="Regions",
            date_published=ds_regions.version,
        )
    ]
    combined["iso_code"].metadata.title = "ISO code"
    combined["iso_code"].metadata.description_short = "ISO 3166-1 alpha-3 three-letter country codes."
    combined["iso_code"].metadata.unit = ""

    # Sanity check.
    columns_with_inf = [column for column in combined.columns if len(combined[combined[column] == np.inf]) > 0]
    assert len(columns_with_inf) == 0, f"Infinity values detected in columns: {columns_with_inf}"

    # Sort rows and columns conveniently.
    first_columns = ["country", "year", "iso_code", "population", "gdp"]
    combined = combined[first_columns + [column for column in sorted(combined.columns) if column not in first_columns]]

    # Improve table format.
    combined = combined.format()

    return combined


def remove_details_on_demand(text: str) -> str:
    # Remove references to details on demand from a text.
    # Example: "This is a [description](#dod:something)." -> "This is a description."
    regex = r"\(\#dod\:.*\)"
    if "(#dod:" in text:
        text = re.sub(regex, "", text).replace("[", "").replace("]", "")

    return text


def prepare_codebook(tb: Table) -> pd.DataFrame:
    table = tb.reset_index()

    # Manually create an origin for the regions dataset.
    regions_origin = [Origin(producer="Our World in Data", title="Regions", date_published=str(table["year"].max()))]

    # Manually edit some of the metadata fields.
    table["country"].metadata.title = "Country"
    table["country"].metadata.description_short = "Geographic location."
    table["country"].metadata.description = None
    table["country"].metadata.unit = ""
    table["country"].metadata.origins = regions_origin
    table["year"].metadata.title = "Year"
    table["year"].metadata.description_short = "Year of observation."
    table["year"].metadata.description = None
    table["year"].metadata.unit = ""
    table["year"].metadata.origins = regions_origin

    ####################################################################################################################
    if table["population"].metadata.description is None:
        print("WARNING: Column population has no longer a description field. Remove this part of the code")
    else:
        table["population"].metadata.description = None

    ####################################################################################################################

    # Gather column names, titles, short descriptions, unit and origins from the indicators' metadata.
    metadata = {"column": [], "description": [], "unit": [], "source": []}
    for column in table.columns:
        metadata["column"].append(column)

        if hasattr(table[column].metadata, "description") and table[column].metadata.description is not None:
            print(f"WARNING: Column {column} still has a 'description' field.")
        # Prepare indicator's description.
        description = ""
        if (
            hasattr(table[column].metadata.presentation, "title_public")
            and table[column].metadata.presentation.title_public is not None
        ):
            description += table[column].metadata.presentation.title_public
        else:
            description += table[column].metadata.title
        if table[column].metadata.description_short:
            description += f" - {table[column].metadata.description_short}"
            description = remove_details_on_demand(description)
        metadata["description"].append(description)

        # Prepare indicator's unit.
        if table[column].metadata.unit is None:
            print(f"WARNING: Column {column} does not have a unit.")
            unit = ""
        else:
            unit = table[column].metadata.unit
        metadata["unit"].append(unit)

        # Gather unique origins of current variable.
        unique_sources = []
        for origin in table[column].metadata.origins:
            # Construct the source name from the origin's attribution.
            # If not defined, build it using the default format "Producer - Data product (year)".
            source_name = (
                origin.attribution
                or f"{origin.producer} - {origin.title or origin.title_snapshot} ({origin.date_published.split('-')[0]})"
            )

            # Add url at the end of the source.
            if origin.url_main:
                source_name += f" [{origin.url_main}]"

            # Add the source to the list of unique sources.
            if source_name not in unique_sources:
                unique_sources.append(source_name)

        # Concatenate all sources.
        sources_combined = "; ".join(unique_sources)
        metadata["source"].append(sources_combined)

    # Create a dataframe with the gathered metadata and sort conveniently by column name.
    codebook = pd.DataFrame(metadata).set_index("column").sort_index()
    # For clarity, ensure column descriptions are in the same order as the columns in the data.
    first_columns = ["country", "year", "iso_code", "population", "gdp"]
    codebook = pd.concat([codebook.loc[first_columns], codebook.drop(first_columns, errors="raise")]).reset_index()
    # Create a table with the appropriate metadata.
    codebook = Table(codebook).format(
        keys=["column"], sort_rows=False, sort_columns=False, short_name="owid_co2_codebook"
    )
    codebook_origin = [
        Origin(producer="Our World in Data", title="CO2-data codebook", date_published=str(table["year"].max()))
    ]
    for column in ["description", "unit", "source"]:
        codebook[column].metadata.origins = codebook_origin

    return codebook


def sanity_check_outputs(tb: Table, tb_codebook: Table) -> None:
    error = "Dataset columns should coincide with the codebook 'columns'."
    assert set(tb_codebook.reset_index()["column"]) == set(tb.reset_index().columns), error

    error = "All rows in dataset should contain at least one non-NaN value."
    assert not tb.isnull().all(axis=1).any(), error


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load the global carbon budget dataset from the Global Carbon Project (GCP).
    ds_gcp = paths.load_dataset("global_carbon_budget")

    # Load the Jones et al. dataset on national contributions to climate change.
    ds_jones = paths.load_dataset("national_contributions")

    # Load the greenhouse gas emissions by sector dataset by Climate Watch.
    # ds_climate_watch = paths.load_dataset("emissions_by_sector")

    # Load the GDP dataset by GGDC Maddison.
    ds_gdp = paths.load_dataset("maddison_project_database")

    # Load primary energy consumption dataset (by different sources in our 'energy' namespace).
    ds_energy = paths.load_dataset("primary_energy_consumption")

    # Load population dataset.
    ds_population = paths.load_dataset("population")

    # Load countries-regions dataset (required to get ISO codes).
    ds_regions = paths.load_dataset("regions")

    # Gather all required tables from all datasets.
    tb_gcp = ds_gcp["global_carbon_budget"]
    tb_jones = ds_jones["national_contributions"]
    # tb_climate_watch_ghg = ds_climate_watch["greenhouse_gas_emissions_by_sector"]
    # tb_climate_watch_ch4 = ds_climate_watch["methane_emissions_by_sector"]
    # tb_climate_watch_n2o = ds_climate_watch["nitrous_oxide_emissions_by_sector"]
    tb_energy = ds_energy["primary_energy_consumption"]
    tb_gdp = ds_gdp["maddison_project_database"]
    tb_population = ds_population["population"]
    tb_regions = ds_regions["regions"]

    #
    # Process data.
    #
    # Choose required columns and rename them.
    tb_gcp = tb_gcp.reset_index()[list(GCP_COLUMNS)].rename(columns=GCP_COLUMNS, errors="raise")
    tb_jones = tb_jones.reset_index()[list(JONES_COLUMNS)].rename(columns=JONES_COLUMNS, errors="raise")
    # tb_climate_watch_ghg = tb_climate_watch_ghg.reset_index()[list(CLIMATE_WATCH_GHG_COLUMNS)].rename(
    #     columns=CLIMATE_WATCH_GHG_COLUMNS, errors="raise"
    # )
    # tb_climate_watch_ch4 = tb_climate_watch_ch4.reset_index()[list(CLIMATE_WATCH_CH4_COLUMNS)].rename(
    #     columns=CLIMATE_WATCH_CH4_COLUMNS, errors="raise"
    # )
    # tb_climate_watch_n2o = tb_climate_watch_n2o.reset_index()[list(CLIMATE_WATCH_N2O_COLUMNS)].rename(
    #     columns=CLIMATE_WATCH_N2O_COLUMNS, errors="raise"
    # )
    tb_energy = tb_energy.reset_index()[list(PRIMARY_ENERGY_COLUMNS)].rename(
        columns=PRIMARY_ENERGY_COLUMNS, errors="raise"
    )
    tb_gdp = tb_gdp.reset_index()[list(GDP_COLUMNS)].rename(columns=GDP_COLUMNS, errors="raise")
    tb_population = tb_population.reset_index()[list(POPULATION_COLUMNS)].rename(
        columns=POPULATION_COLUMNS, errors="raise"
    )
    tb_regions = tb_regions.reset_index()[list(REGIONS_COLUMNS)].rename(columns=REGIONS_COLUMNS, errors="raise")

    # Combine tables.
    combined = combine_tables(
        tb_gcp=tb_gcp,
        tb_jones=tb_jones,
        # tb_climate_watch_ghg=tb_climate_watch_ghg,
        # tb_climate_watch_ch4=tb_climate_watch_ch4,
        # tb_climate_watch_n2o=tb_climate_watch_n2o,
        tb_energy=tb_energy,
        tb_gdp=tb_gdp,
        tb_population=tb_population,
        tb_regions=tb_regions,
    )

    # Prepare output data table.
    tb = prepare_outputs(combined=combined, ds_regions=ds_regions)

    # Prepare codebook.
    tb_codebook = prepare_codebook(tb=tb)

    # Sanity check.
    sanity_check_outputs(tb=tb, tb_codebook=tb_codebook)

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb, tb_codebook], check_variables_metadata=True)
    ds_grapher.save()
