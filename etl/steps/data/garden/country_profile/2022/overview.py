# type: ignore
import re
from functools import reduce

import pandas as pd
from owid import catalog

from etl.paths import DATA_DIR

from .shared import CURRENT_DIR

ENTITY = "Italy"
ENTITY_SNAKE_CASE = re.sub(r"(?<!^)(?=[A-Z])", "_", ENTITY).lower()
# Details for dataset to export.
DATASET_SHORT_NAME = "overview"
DATASET_TITLE = f"Country Profile Overview"
METADATA_PATH = CURRENT_DIR / f"{DATASET_SHORT_NAME}.meta.yml"
# Details for datasets to import.
# Population
KI_DATASET_PATH = DATA_DIR / "garden/owid/latest/key_indicators"

# Per capita Co2 emissions
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


def run(dest_dir: str) -> None:
    # Load data.
    # Read all required datasets.
    # Population
    pop = get_population(entity=ENTITY)
    # emissions per capita
    em = get_emissions(entity=ENTITY)
    # electricity mix
    mix = get_energy_mix(entity=ENTITY)
    # share using internet
    # gdp per capita
    # share electricity access
    wdi = get_wdi_variables(entity=ENTITY)
    # Backport:
    # Electoral democracy
    # Homicide rate
    # Average years of schooling
    # Life expectancy
    # Child mortality
    # Daily calories
    bkp = get_backports(entity=ENTITY)

    data_frames = [pop, em, mix, wdi, bkp]
    df_merged = reduce(lambda left, right: pd.merge(left, right, on=["country", "year"], how="outer"), data_frames)
    df_merged = df_merged.sort_values("year").reset_index()
    # Create a new table with combined data (and no metadata).
    tb_combined = catalog.Table(df_merged)

    #
    # Save outputs.
    #
    ds_garden = catalog.Dataset.create_empty(dest_dir)
    # Get the rest of the metadata from the yaml file.
    ds_garden.metadata.update_from_yaml(METADATA_PATH, if_source_exists="replace")
    # Create dataset.
    ds_garden.save()

    # Add other metadata fields to table.
    tb_combined.metadata.short_name = DATASET_SHORT_NAME
    tb_combined.metadata.title = DATASET_TITLE
    tb_combined.update_metadata_from_yaml(METADATA_PATH, DATASET_SHORT_NAME)

    # Add combined tables to the new dataset.
    ds_garden.add(tb_combined)


def get_population(entity: str) -> pd.DataFrame:
    ds_ki = catalog.Dataset(KI_DATASET_PATH)
    pop = ds_ki["population"].reset_index()
    pop = pop[["country", "year", "population"]][pop["country"] == entity]
    return pop


def get_emissions(entity: str) -> pd.DataFrame:
    ds_em = catalog.Dataset(CO2_EMISSIONS_DATASET_PATH)
    df_em = ds_em["global_carbon_budget"].reset_index()
    df_em = df_em[["country", "year", "emissions_total_per_capita"]][df_em["country"] == ENTITY]
    df_em["emissions_total_per_capita"] = df_em["emissions_total_per_capita"].round(2)
    df_em["year"] = df_em["year"].astype("int64")
    return df_em


def get_energy_mix(entity: str) -> pd.DataFrame:
    ds_mix = catalog.Dataset(ENERGY_CON_DATASET_PATH)
    df_mix = ds_mix["energy_mix"].reset_index()
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
    df_mix = df_mix[df_mix["country"] == ENTITY][cols]
    return df_mix


def get_wdi_variables(entity: str) -> pd.DataFrame:
    ds_wdi = catalog.Dataset(WDI_DATASET_PATH)
    df_wdi = ds_wdi["wdi"].reset_index()
    cols = ["country", "year", "it_net_user_zs", "ny_gdp_pcap_kd", "eg_elc_accs_zs"]
    new_cols = ["country", "year", "share_pop_using_internet", "gdp_per_capita", "share_electricity_access"]
    df_wdi = df_wdi[cols][df_wdi["country"] == ENTITY]
    df_wdi.columns = new_cols
    return df_wdi


def get_backports(entity: str) -> pd.DataFrame:
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
        "dataset_2710_child_mortality_rates__selected_gapminder__v10__2017": [
            "child_mortality__select_gapminder__v10__2017"
        ],
        "dataset_581_daily_supply_of_calories_per_person__owid_based_on_un_fao__and__historical_sources": [
            "daily_caloric_supply__owid_based_on_un_fao__and__historical_sources"
        ],
    }
    # make one mega table with all variables from all the backports
    t_all = None

    for dataset, variables in backports.items():
        print(dataset)
        ds = catalog.Dataset(f"{base_path}/{dataset}")
        t = ds[dataset]

        # assert variables are in the table - if not throw an error
        # fix the index to be (year, entity_name)
        t = t.reset_index().drop(columns=["entity_id", "entity_code"]).set_index(["entity_name", "year"])[variables]

        if t_all is None:
            # first time around
            t_all = t

        else:
            t_all = t_all.join(t, how="outer")  # omg hope
    t_all = t_all.reset_index()
    t_all = t_all.rename(columns={"entity_name": "country"})
    t_all = t_all[t_all["country"] == ENTITY]
    return t_all
