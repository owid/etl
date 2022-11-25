import re
from functools import reduce

import pandas as pd
from owid import catalog
from owid.catalog import Dataset
from owid.catalog.utils import underscore
from owid.datautils import dataframes
from structlog import get_logger

from etl.paths import DATA_DIR, REFERENCE_DATASET

from .shared import CURRENT_DIR

log = get_logger()

METADATA_PATH = CURRENT_DIR / "overview.meta.yml"
# Details for datasets to import.
# Population
KI_DATASET_PATH = DATA_DIR / "garden/owid/latest/key_indicators"

# Per capita CO2 emissions
CO2_EMISSIONS_DATASET_PATH = DATA_DIR / "garden/gcp/2022-11-11/global_carbon_budget"

# Energy consumption by source
ENERGY_CON_DATASET_PATH = DATA_DIR / "garden/bp/2022-07-14/energy_mix"

# Share of population using the internet
# GDP per capita
# Electricity access
WDI_DATASET_PATH = DATA_DIR / "garden/worldbank_wdi/2022-05-26/wdi"

## Backports:
# Electoral democracy
# https://owid.cloud/admin/datasets/5600
# Homicide rate
# https://owid.cloud/admin/datasets/5599
# Average years of schooling
# https://owid.cloud/admin/datasets/4129
# Life expectancy
# https://owid.cloud/admin/datasets/1892
# Child mortality
# https://owid.cloud/admin/datasets/2710
# Daily supply of calories per person
# https://owid.cloud/admin/datasets/581

countries = Dataset(REFERENCE_DATASET)["countries_regions"]
# Get only countries which have an ISO2 code - we don't want regions just yet
countries_list = countries[["name", "iso_alpha2"]].dropna()["name"].to_list()


def run(dest_dir: str) -> None:
    """
    Combine each of the datasets listed above and then split them into a table per country, saved as a csv.
    """
    # Population
    pop = get_population()
    # emissions per capita
    emissions_pc = get_emissions()
    # electricity mix
    energy_mix = get_energy_mix()
    # WDI: share using internet; gdp per capita; share electricity access
    wdi = get_wdi_variables()
    # Backport: Electoral democracy; Homicide rate; Average years of schooling; Life expectancy; Child mortality; Daily calories
    backports = get_backports()

    data_frames = [pop, emissions_pc, energy_mix, wdi, backports]
    # df_merged = reduce(lambda left, right: pd.merge(left, right, on=["country", "year"], how="outer"), data_frames)
    df_merged = dataframes.multi_merge(data_frames, on=["country", "year"], how="outer")
    df_merged = df_merged.sort_values("year")

    ds_garden = catalog.Dataset.create_empty(dest_dir)
    # Get the rest of the metadata from the yaml file.
    ds_garden.metadata.update_from_yaml(METADATA_PATH, if_source_exists="replace")
    # Create dataset.
    ds_garden.save()

    for country in countries_list:
        log.info(f"Saving... {country}")
        # making snake case version of country name
        country_snake_case = underscore(country)
        df_country = df_merged[df_merged["country"] == country]

        if df_country.shape[1] > 2:
            # Drop columns where the country doesn't have data
            df_country = df_country.dropna(how="all")
            # Skip countries where we don't have any data
            if df_country.shape[1] > 2:
                # Create a new table with combined data (and no metadata).
                tb_combined = catalog.Table(df_country)

                # Details for dataset to export.
                table_short_name = f"overview_{country_snake_case}"
                table_title = f"Country Profile Overview - {country}"

                # Add other metadata fields to table.
                tb_combined.metadata.short_name = table_short_name
                tb_combined.metadata.title = table_title

                # Add combined tables to the new dataset.
                tb_combined = tb_combined.reset_index()
                ds_garden.add(tb_combined, formats=["csv"])


def get_population() -> pd.DataFrame:
    """
    Get the population variable from the key indicators dataset
    """
    ds_ki = catalog.Dataset(KI_DATASET_PATH)
    pop = ds_ki["population"].reset_index()
    pop = pop[["country", "year", "population"]]
    return pop


def get_emissions() -> pd.DataFrame:
    """
    Get the emissions per capita variable from the global carbon budget dataset
    """
    ds_emissions = catalog.Dataset(CO2_EMISSIONS_DATASET_PATH)
    df_emissions = ds_emissions["global_carbon_budget"].reset_index()
    df_emissions = df_emissions[["country", "year", "emissions_total_per_capita"]]
    df_emissions["year"] = df_emissions["year"].astype("int64")
    return df_emissions


def get_energy_mix() -> pd.DataFrame:
    """
    Get the energy consumption variables from the BP energy mix dataset
    """
    ds_energy_mix = catalog.Dataset(ENERGY_CON_DATASET_PATH)
    df_energy_mix = ds_energy_mix["energy_mix"].reset_index()
    cols = [
        "country",
        "year",
        "hydro__twh__direct",
        "nuclear__twh__direct",
        "solar__twh__direct",
        "wind__twh__direct",
        "other_renewables__twh__direct",
        "coal__twh",
        "oil__twh",
        "gas__twh",
        "biofuels__twh",
        "fossil_fuels__twh",
    ]
    df_energy_mix = df_energy_mix[cols]
    df_energy_mix["year"] = df_energy_mix["year"].astype("int64")
    return df_energy_mix


def get_wdi_variables() -> pd.DataFrame:
    """
    Get the i) Share population using internet, ii) GDP per capita and iii) Share electricity access
    from the WDI dataset.
    """
    ds_wdi = catalog.Dataset(WDI_DATASET_PATH)
    df_wdi = ds_wdi["wdi"].reset_index()
    cols = ["country", "year", "it_net_user_zs", "ny_gdp_pcap_kd", "eg_elc_accs_zs"]
    new_cols = ["country", "year", "share_pop_using_internet", "gdp_per_capita", "share_electricity_access"]
    df_wdi = df_wdi[cols]
    df_wdi.columns = new_cols
    df_wdi["year"] = df_wdi["year"].astype("int64")
    return df_wdi


def get_backports() -> pd.DataFrame:
    """
    Get the i) Child mortality rate, ii) Electoral democracy, iii) Homicide rate,
    iv) Average years of schooling, v) Life expectancy and vi) Daily supply of calories per person
    from the backported datasets.
    """
    base_path = DATA_DIR / "backport/owid/latest/"

    # list of all backports to include, map from dataset name to list of variables to include
    backports = {
        "dataset_2710_child_mortality_rates__selected_gapminder__v10__2017": [
            "child_mortality__select_gapminder__v10__2017"
        ],
        "dataset_5600_democracy_and_human_rights__owid_based_on_varieties_of_democracy__v12__and_regimes_of_the_world": [
            "electdem_vdem_high_owid",
            "electdem_vdem_low_owid",
            "electdem_vdem_owid",
        ],
        "dataset_5599_ihme__global_burden_of_disease__deaths_and_dalys__institute_for_health_metrics_and_evaluation__2022_04": [
            "deaths__interpersonal_violence__sex__both__age__all_ages__rate"
        ],
        "dataset_4129_years_of_schooling__based_on_lee_lee__2016__barro_lee__2018__and_undp__2018": [
            "average_total_years_of_schooling_for_adult_population__lee_lee__2016__barro_lee__2018__and_undp__2018"
        ],
        "dataset_1892_life_expectancy__riley__2005__clio_infra__2015__and_un__2019": ["life_expectancy"],
        "dataset_581_daily_supply_of_calories_per_person__owid_based_on_un_fao__and__historical_sources": [
            "daily_caloric_supply__owid_based_on_un_fao__and__historical_sources"
        ],
    }
    # make one mega table with all variables from all the backports
    t_all = pd.DataFrame()

    for dataset, variables in backports.items():
        log.info(f"Fetching the backport of... {dataset}")
        ds = catalog.Dataset(f"{base_path}/{dataset}")
        t = ds[dataset]

        # assert variables are in the table - if not throw an error
        # fix the index to be (year, entity_name)
        t = t.reset_index().drop(columns=["entity_id", "entity_code"]).set_index(["entity_name", "year"])[variables]

        if t_all.shape == tuple([0, 0]):
            # first time around
            t_all = t
        else:
            t_all = t_all.join(t, how="outer")  # omg hope
    t_all = t_all.reset_index()
    t_all = t_all.rename(columns={"entity_name": "country"})
    return t_all
