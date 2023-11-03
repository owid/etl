"""Combine dataset on coverage of emissions with the average prices of emissions covered by an ETS or a carbon tax.

"""

import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore_table
from shared import LAST_INFORMED_YEAR, MEADOW_VERSION, VERSION

from etl.data_helpers import geo
from etl.paths import DATA_DIR, STEP_DIR

# Details on garden dataset to be exported.
DATASET_NAME = "emissions_weighted_carbon_price"
TABLE_NAME = DATASET_NAME
# Path to country names file.
# NOTE: This countries file contains as many countries as the file for world_cabon_pricing, plus "World".
#  Here we ignore all regions inside countries.
COUNTRIES_PATH = STEP_DIR / f"data/garden/rff/{VERSION}/{DATASET_NAME}.countries.json"
# Path to metadata file.
METADATA_PATH = STEP_DIR / f"data/garden/rff/{VERSION}/{DATASET_NAME}.meta.yml"
# Details on meadow datasets to be imported.
MEADOW_PATH_ECONOMY = DATA_DIR / f"meadow/rff/{MEADOW_VERSION}/emissions_weighted_carbon_price__economy"
MEADOW_PATH_COVERAGE = DATA_DIR / f"meadow/rff/{MEADOW_VERSION}/emissions_weighted_carbon_price__coverage"

# Columns to keep from raw dataset and how to rename them.
COLUMNS = {
    "jurisdiction": "country",
    "year": "year",
    # Emissions-weighted average price on emissions covered by either a carbon tax or an ETS.
    # Weights: share of jurisdiction total CO2 emissions.
    "ecp_all_jurco2_kusd": "price_with_tax_or_ets_weighted_by_share_of_co2",
    # Emissions-weighted average price on emissions covered by either a carbon tax or an ETS.
    # Weights: share of jurisdiction total GHG emissions.
    "ecp_all_jurghg_kusd": "price_with_tax_or_ets_weighted_by_share_of_ghg",
    # Emissions-weighted average price on emissions covered by an ETS.
    # Weights: share of jurisdiction total CO2 emissions.
    "ecp_ets_jurco2_kusd": "price_with_ets_weighted_by_share_of_co2",
    # Emissions-weighted average price on emissions covered by an ETS.
    # Weights: share of jurisdiction total GHG emissions.
    "ecp_ets_jurghg_kusd": "price_with_ets_weighted_by_share_of_ghg",
    # Emissions-weighted average price on emissions covered by a carbon tax.
    # Weights: share of jurisdiction total CO2 emissions.
    "ecp_tax_jurco2_kusd": "price_with_tax_weighted_by_share_of_co2",
    # Emissions-weighted average price on emissions covered by a carbon tax.
    # Weights: share of jurisdiction total GHG emissions.
    "ecp_tax_jurghg_kusd": "price_with_tax_weighted_by_share_of_ghg",
    # CO2 emissions covered by either a carbon tax or an ETS as a share of jurisdiction total CO2 emissions.
    "cov_all_co2_jurco2": "co2_with_tax_or_ets_as_share_of_co2",
    # CO2 emissions covered by either a carbon tax or an ETS as a share of jurisdiction total GHG emissions.
    "cov_all_co2_jurghg": "co2_with_tax_or_ets_as_share_of_ghg",
    # CO2 emissions covered by either carbon taxes or an ETS as a share of world total CO2 emissions.
    "cov_all_co2_wldco2": "co2_with_tax_or_ets_as_share_of_world_co2",
    # CO2 emissions covered by either carbon taxes or an ETS as a share of world total GHG emissions.
    "cov_all_co2_wldghg": "co2_with_tax_or_ets_as_share_of_world_ghg",
    # CO2 emissions covered by an ETS as a share of jurisdiction total CO2 emissions.
    "cov_ets_co2_jurco2": "co2_with_ets_as_share_of_co2",
    # CO2 emissions covered by an ETS as a share of jurisdiction total GHG emissions.
    "cov_ets_co2_jurghg": "co2_with_ets_as_share_of_ghg",
    # CO2 emissions covered by an ETS as a share of world total CO2 emissions.
    "cov_ets_co2_wldco2": "co2_with_ets_as_share_of_world_co2",
    # CO2 emissions covered by an ETS as a share of world total GHG emissions.
    "cov_ets_co2_wldghg": "co2_with_ets_as_share_of_world_ghg",
    # CO2 emissions covered by a carbon tax as a share of jurisdiction total CO2 emissions.
    "cov_tax_co2_jurco2": "co2_with_tax_as_share_of_co2",
    # CO2 emissions covered by a carbon tax as a share of jurisdiction total GHG emissions.
    "cov_tax_co2_jurghg": "co2_with_tax_as_share_of_ghg",
    # CO2 emissions covered by a carbon tax as a share of world total CO2 emissions.
    "cov_tax_co2_wldco2": "co2_with_tax_as_share_of_world_co2",
    # CO2 emissions covered by a carbon tax as a share of world total GHG emissions.
    "cov_tax_co2_wldghg": "co2_with_tax_as_share_of_world_ghg",
    # # Other variables that are only relevant when considering sub-country regions (that we ignore for now):
    # # Emissions-weighted average price on emissions covered by either a carbon tax or an ETS.
    # # Weights: share of national jurisdiction total CO2 emissions.
    # 'ecp_all_supraco2_kusd': 'price_with_tax_or_ets_weighted_by_share_of_country_co2',
    # # Emissions-weighted average price on emissions covered by either a carbon tax or an ETS.
    # # Weights: share of national jurisdiction total GHG emissions.
    # 'ecp_all_supraghg_kusd': 'price_with_tax_or_ets_weighted_by_share_of_country_ghg',
    # # Emissions-weighted average price on emissions covered by an ETS.
    # # Weights: share of national jurisdiction total CO2 emissions.
    # 'ecp_ets_supraco2_kusd': 'price_with_ets_weighted_by_share_of_country_co2',
    # # Emissions-weighted average price on emissions covered by an ETS.
    # # Weights: share of national jurisdiction total GHG emissions.
    # 'ecp_ets_supraghg_kusd': 'price_with_ets_weighted_by_share_of_country_ghg',
    # # Emissions-weighted average price on emissions covered by a carbon tax.
    # # Weights: share of national jurisdiction total CO2 emissions.
    # 'ecp_tax_supraco2_kusd': 'price_with_tax_weighted_by_share_of_country_co2',
    # # Emissions-weighted average price on emissions covered by a carbon tax.
    # # Weights: share of national jurisdiction total GHG emissions.
    # 'ecp_tax_supraghg_kusd': 'price_with_tax_weighted_by_share_of_country_ghg',
    # # CO2 emissions covered by either carbon taxes or an ETS as a share of national jurisdiction CO2 emissions.
    # 'cov_all_co2_supraco2': 'co2_with_tax_or_ets_as_share_of_country_co2',
    # # CO2 emissions covered by either carbon taxes or an ETS as a share of national jurisdiction GHG emissions.
    # 'cov_all_co2_supraghg': 'co2_with_tax_or_ets_as_share_of_country_ghg',
    # # CO2 emissions covered by an ETS as a share of national jurisdiction total CO2 emissions.
    # 'cov_ets_co2_supraco2': 'co2_with_ets_as_share_of_country_co2',
    # # CO2 emissions covered by an ETS as a share of national jurisdiction total GHG emissions.
    # 'cov_ets_co2_supraghg': 'co2_with_ets_as_share_of_country_ghg',
    # # CO2 emissions covered by a carbon tax as a share of national jurisdiction total CO2 emissions.
    # 'cov_tax_co2_supraco2': 'co2_with_tax_as_share_of_country_co2',
    # # CO2 emissions covered by a carbon tax as a share of national jurisdiction total GHG emissions.
    # 'cov_tax_co2_supraghg': 'co2_with_tax_as_share_of_country_ghg',
}


def sanity_checks(df_economy: pd.DataFrame, df_coverage: pd.DataFrame) -> None:
    """Sanity checks on the raw data from meadow.

    Parameters
    ----------
    df_economy : pd.DataFrame
        Raw data from meadow on prices.
    df_coverage : pd.DataFrame
        Raw data from meadow on coverage.

    """
    error = "Both dataframes were expected to have the same jurisdictions (although this may not be necessary)."
    assert set(df_economy["jurisdiction"]) == set(df_coverage["jurisdiction"]), error
    error = "Coverage should have the same (or less) years than economy (current year may be missing in coverage)."
    assert set(df_coverage["year"]) <= set(df_economy["year"]), error


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Read datasets from meadow.
    ds_economy = Dataset(MEADOW_PATH_ECONOMY)
    ds_coverage = Dataset(MEADOW_PATH_COVERAGE)
    # Get tables from datasets.
    tb_economy = ds_economy[ds_economy.table_names[0]]
    tb_coverage = ds_coverage[ds_coverage.table_names[0]]
    # Create dataframes from tables.
    df_economy = pd.DataFrame(tb_economy).reset_index()
    df_coverage = pd.DataFrame(tb_coverage).reset_index()

    #
    # Process data.
    #
    # Sanity checks on raw data.
    sanity_checks(df_economy=df_economy, df_coverage=df_coverage)

    # Convert all values in coverage to percentages (instead of fractions).
    df_coverage.loc[:, [column for column in df_coverage.columns if column not in ["jurisdiction", "year"]]] *= 100

    # Combine both dataframes.
    df_combined = pd.merge(df_economy, df_coverage, how="outer", on=["jurisdiction", "year"])

    # Select and rename columns.
    df_combined = df_combined[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Harmonize country names.
    # Notes:
    #  * Here it would be better to have a list of excluded countries, but this is not yet implemented
    #  in harmonize_countries. For example, if a new country is included, it will be ignored here
    #  (while instead it should raise a warning).
    df_combined = geo.harmonize_countries(
        df=df_combined,
        countries_file=str(COUNTRIES_PATH),
        warn_on_unused_countries=False,
        warn_on_missing_countries=False,
        make_missing_countries_nan=True,
    )

    # Remove sub-regions within a country.
    df_combined = df_combined.dropna(subset=["country"]).reset_index(drop=True)

    # Given that the most recent data is incomplete, keep only data points prior to (or at) a certain year
    # (given by global variables LAST_INFORMED_YEAR).
    df_combined = df_combined[df_combined["year"] <= LAST_INFORMED_YEAR].reset_index(drop=True)

    # Sanity checks.
    error = "There should be no columns with only nans."
    assert df_combined.columns[df_combined.isna().all()].empty, error
    error = f"Country named 'World' should be included in the countries file {COUNTRIES_PATH.name}."
    assert "World" in set(df_combined["country"]), error

    # Set an appropriate index and sort conveniently.
    df_combined = df_combined.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    # Create main table.
    tb_garden = underscore_table(Table(df_combined))

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = Dataset.create_empty(dest_dir)
    # Fetch metadata from any of the meadow steps (if any).
    ds_garden.metadata = ds_economy.metadata
    # Update dataset metadata using metadata yaml file.
    ds_garden.metadata.update_from_yaml(METADATA_PATH, if_source_exists="replace")
    # Update main table metadata using metadata yaml file.
    tb_garden.update_metadata_from_yaml(METADATA_PATH, TABLE_NAME)
    # Add tables to dataset.
    ds_garden.add(tb_garden)
    # Save dataset.
    ds_garden.save()
