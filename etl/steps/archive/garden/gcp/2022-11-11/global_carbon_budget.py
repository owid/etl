"""This step creates the Global Carbon Budget (GCB) dataset, by the Global Carbon Project (GCP).

It combines the following datasets:
- GCP's Fossil CO2 emissions (long-format csv).
- GCP's official GCB global emissions (excel file) containing global bunker fuel and land-use change emissions.
- GCP's official GCB national emissions (excel file) containing consumption-based emissions for each country.
  - Production-based emissions from this file are also used, but just to include total emissions of regions
    according to GCP (e.g. "Africa (GCP)") and for sanity checks.
- GCP's official GCB national land-use change emissions (excel file) with land-use change emissions for each country.
And additionally:
- GGDC's Maddison dataset on GDP, used to calculate emissions per GDP.
- Primary Energy Consumption (mix of sources from the 'energy' namespace) to calculate emissions per unit energy.
- Population (mix of sources from the 'owid' namespace), to calculate emissions per capita.
- Countries-regions (mix of sources from the 'reference' namespace), to generate aggregates for different continents.
- WorldBank's Income groups, to generate aggregates for different income groups.

"""

from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from owid import catalog
from owid.datautils import dataframes

from etl.data_helpers import geo
from etl.paths import DATA_DIR, STEP_DIR

# Define inputs.
MEADOW_VERSION = "2022-11-11"
# Country names harmonization file for fossil CO2 emissions data.
FOSSIL_CO2_EMISSIONS_COUNTRIES_FILE = (
    STEP_DIR / f"data/garden/gcp/{MEADOW_VERSION}/global_carbon_budget_fossil_co2_emissions.countries.json"
)
# Country names harmonization file for national emissions data.
NATIONAL_EMISSIONS_COUNTRIES_FILE = (
    STEP_DIR / f"data/garden/gcp/{MEADOW_VERSION}/global_carbon_budget_national_emissions.countries.json"
)
# Country names harmonization file for national land-use change emissions data.
LAND_USE_EMISSIONS_COUNTRIES_FILE = (
    STEP_DIR / f"data/garden/gcp/{MEADOW_VERSION}/global_carbon_budget_land_use_change_emissions.countries.json"
)
# Meadow dataset on GCB fossil CO2 emissions.
MEADOW_CO2_DATASET_PATH = DATA_DIR / f"meadow/gcp/{MEADOW_VERSION}/global_carbon_budget_fossil_co2_emissions"
# Meadow dataset on global emissions.
MEADOW_GLOBAL_EMISSIONS_DATASET_PATH = DATA_DIR / f"meadow/gcp/{MEADOW_VERSION}/global_carbon_budget_global_emissions"
# Meadow dataset on national emissions.
MEADOW_NATIONAL_EMISSIONS_DATASET_PATH = (
    DATA_DIR / f"meadow/gcp/{MEADOW_VERSION}/global_carbon_budget_national_emissions"
)
# Meadow dataset on GCB national land-use change emissions.
MEADOW_LAND_USE_EMISSIONS_DATASET_PATH = (
    DATA_DIR / f"meadow/gcp/{MEADOW_VERSION}/global_carbon_budget_land_use_change_emissions"
)
# Garden dataset on primary energy consumption.
GARDEN_PRIMARY_ENERGY_DATASET_PATH = DATA_DIR / "garden/energy/2022-07-29/primary_energy_consumption"
# Garden dataset on GDP.
GARDEN_GDP_DATASET_PATH = DATA_DIR / "garden/ggdc/2020-10-01/ggdc_maddison"
# Additionally, population dataset and income groups are also used (through datautils.geo functions).

# Define outputs.
# Name of output dataset.
VERSION = MEADOW_VERSION
DATASET_NAME = "global_carbon_budget"
# Path to metadata file.
METADATA_PATH = STEP_DIR / f"data/garden/gcp/{MEADOW_VERSION}/global_carbon_budget.meta.yml"

# Expected outliers in consumption-based emissions (with negative emissions in the original data, that will be removed).
OUTLIERS_IN_CONSUMPTION_DF = [
    ("Panama", 2003),
    ("Panama", 2004),
    ("Panama", 2005),
    ("Panama", 2006),
    ("Panama", 2011),
    ("Panama", 2012),
    ("Panama", 2013),
    ("Venezuela", 2018),
]

# Label used for international transport (emissions from oil in bunker fuels), included as a country in the
# fossil CO2 emissions dataset.
INTERNATIONAL_TRANSPORT_LABEL = "International Transport"

# Regions and income groups to create by aggregating contributions from member countries.
# In the following dictionary, if nothing is stated, the region is supposed to be a default continent/income group.
# Otherwise, the dictionary can have "regions_included", "regions_excluded", "countries_included", and
# "countries_excluded". The aggregates will be calculated on the resulting countries.
REGIONS = {
    # Default continents.
    "Africa": {},
    "Asia": {},
    "Europe": {},
    "European Union (27)": {},
    "North America": {},
    "Oceania": {},
    "South America": {},
    # Income groups.
    "Low-income countries": {},
    "Upper-middle-income countries": {},
    "Lower-middle-income countries": {},
    "High-income countries": {},
    # Additional composite regions.
    "Asia (excl. China and India)": {
        "regions_included": ["Asia"],
        "countries_excluded": ["China", "India"],
    },
    "Europe (excl. EU-27)": {"regions_included": ["Europe"], "regions_excluded": ["European Union (27)"]},
    "Europe (excl. EU-28)": {
        "regions_included": ["Europe"],
        "regions_excluded": ["European Union (27)"],
        "countries_excluded": ["United Kingdom"],
    },
    "European Union (28)": {
        "regions_included": ["European Union (27)"],
        "countries_included": ["United Kingdom"],
    },
    "North America (excl. USA)": {
        "regions_included": ["North America"],
        "countries_excluded": ["United States"],
    },
}

# Columns to use from GCB fossil CO2 emissions data and how to rename them.
CO2_COLUMNS = {
    "country": "country",
    "year": "year",
    "cement": "emissions_from_cement",
    "coal": "emissions_from_coal",
    "flaring": "emissions_from_flaring",
    "gas": "emissions_from_gas",
    "oil": "emissions_from_oil",
    "other": "emissions_from_other_industry",
    "total": "emissions_total",
}

# List all sources of emissions considered.
EMISSION_SOURCES = [column for column in CO2_COLUMNS.values() if column not in ["country", "year"]]

# Columns to use from primary energy consumption data and how to rename them.
PRIMARY_ENERGY_COLUMNS = {
    "country": "country",
    "year": "year",
    "primary_energy_consumption__twh": "primary_energy_consumption",
}

# Columns to use from GDP data and how to rename them.
GDP_COLUMNS = {
    "country": "country",
    "year": "year",
    "gdp": "gdp",
}

# Columns to use from primary energy consumption data and how to rename them.
HISTORICAL_EMISSIONS_COLUMNS = {
    "country": "country",
    "year": "year",
    # Global fossil emissions are used only for sanity checks.
    "global_fossil_emissions": "global_fossil_emissions",
    "global_land_use_change_emissions": "global_emissions_from_land_use_change",
}

# Columns to use from consumption-based emissions data and how to rename them.
CONSUMPTION_EMISSIONS_COLUMNS = {
    "country": "country",
    "year": "year",
    "consumption_emissions": "consumption_emissions",
}

# Conversion from terawatt-hours to kilowatt-hours.
TWH_TO_KWH = 1e9

# Conversion from million tonnes of CO2 to tonnes of CO2.
MILLION_TONNES_OF_CO2_TO_TONNES_OF_CO2 = 1e6

# Conversion from tonnes of CO2 to kg of CO2 (used for emissions per GDP and per unit energy).
TONNES_OF_CO2_TO_KG_OF_CO2 = 1000

# In order to remove uninformative columns, keep only rows where at least one of the following columns has data.
# All other columns are either derived variables, or global variables, or auxiliary variables from other datasets.
COLUMNS_THAT_MUST_HAVE_DATA = [
    "emissions_from_cement",
    "emissions_from_coal",
    "emissions_from_flaring",
    "emissions_from_gas",
    "emissions_from_oil",
    "emissions_from_other_industry",
    "emissions_total",
    "consumption_emissions",
    "emissions_from_land_use_change",
    # 'land_use_change_quality_flag',
]


def get_countries_in_region(
    region: str, region_modifications: Optional[Dict[str, Dict[str, List[str]]]] = None
) -> List[str]:
    """Get countries in a region, both for known regions (e.g. "Africa") and custom ones (e.g. "Europe (excl. EU-27)").

    Parameters
    ----------
    region : str
        Region name (e.g. "Africa", or "Europe (excl. EU-27)").
    region_modifications : dict or None
        If None (or an empty dictionary), the region should be in OWID's countries-regions dataset.
        If not None, it should be a dictionary with any (or all) of the following keys:
        - "regions_included": List of regions whose countries will be included.
        - "regions_excluded": List of regions whose countries will be excluded.
        - "countries_included": List of additional individual countries to be included.
        - "countries_excluded": List of additional individual countries to be excluded.
        NOTE: All regions and countries defined in this dictionary should be in OWID's countries-regions dataset.

    Returns
    -------
    countries : list
        List of countries in the specified region.

    """
    if region_modifications is None:
        region_modifications = {}

    # Check that the fields in the regions_modifications dictionary are well defined.
    expected_fields = ["regions_included", "regions_excluded", "countries_included", "countries_excluded"]
    assert all([field in expected_fields for field in region_modifications])

    # Get lists of regions whose countries will be included and excluded.
    regions_included = region_modifications.get("regions_included", [region])
    regions_excluded = region_modifications.get("regions_excluded", [])
    # Get lists of additional individual countries to include and exclude.
    countries_included = region_modifications.get("countries_included", [])
    countries_excluded = region_modifications.get("countries_excluded", [])

    # List countries from the list of regions included.
    countries_set = set(
        sum([geo.list_countries_in_region(region_included) for region_included in regions_included], [])
    )

    # Remove all countries from the list of regions excluded.
    countries_set -= set(
        sum([geo.list_countries_in_region(region_excluded) for region_excluded in regions_excluded], [])
    )

    # Add the list of individual countries to be included.
    countries_set |= set(countries_included)

    # Remove the list of individual countries to be excluded.
    countries_set -= set(countries_excluded)

    # Convert set of countries into a sorted list.
    countries = sorted(countries_set)

    return countries


def sanity_checks_on_input_data(
    production_df: pd.DataFrame, consumption_df: pd.DataFrame, historical_df: pd.DataFrame, co2_df: pd.DataFrame
) -> None:
    """Run sanity checks on input data files.

    These checks should be used prior to country harmonization, but after basic processing of the dataframes.

    Parameters
    ----------
    production_df : pd.DataFrame
        Production-based emissions from GCP's official national emissions dataset (excel file).
    consumption_df : pd.DataFrame
        Consumption-based emissions from GCP's official national emissions dataset (excel file).
    historical_df : pd.DataFrame
        Historical emissions from GCP's official global emissions dataset (excel file).
    co2_df : pd.DataFrame
        Production-based emissions from GCP's Fossil CO2 emissions dataset (csv file).

    """
    production_df = production_df.copy()
    consumption_df = consumption_df.copy()
    historical_df = historical_df.copy()
    co2_df = co2_df.copy()

    # In the original data, Bunkers was included in the national data file, as another country.
    # But I suppose it should be considered as another kind of global emission.
    # In fact, bunker emissions should coincide for production and consumption emissions.
    global_bunkers_emissions = (
        production_df[production_df["country"] == "Bunkers"][["year", "production_emissions"]]
        .reset_index(drop=True)
        .rename(columns={"production_emissions": "global_bunker_emissions"}, errors="raise")
    )

    # Check that we get exactly the same array of bunker emissions from the consumption emissions dataframe
    # (on years where there is data for bunker emissions in both datasets).
    comparison = pd.merge(
        global_bunkers_emissions,
        consumption_df[consumption_df["country"] == "Bunkers"][["year", "consumption_emissions"]]
        .reset_index(drop=True)
        .rename(columns={"consumption_emissions": "global_bunker_emissions"}, errors="raise"),
        how="inner",
        on="year",
        suffixes=("", "_check"),
    )

    error = "Bunker emissions were expected to coincide in production and consumption emissions dataframes."
    assert (comparison["global_bunker_emissions"] == comparison["global_bunker_emissions_check"]).all(), error

    # Check that all production-based emissions are positive.
    error = "There are negative emissions in production_df (from the additional variables dataset)."
    assert (production_df.drop(columns=["country", "year"]).fillna(0) >= 0).all().all(), error

    # Check that all production-based emissions from the fossil CO2 dataset are positive.
    error = "There are negative emissions in co2_df (from the fossil CO2 dataset)."
    assert (co2_df.drop(columns=["country", "year"]).fillna(0) >= 0).all().all(), error

    # Check that all consumption-based emissions are positive.
    error = "There are negative emissions in consumption_df (from the national emissions dataset)."
    assert (consumption_df.drop(columns=["country", "year"]).fillna(0) >= 0).all().all(), error

    # Check that, for the World, production emissions coincides with consumption emissions (on common years).
    error = "Production and consumption emissions for the world were expected to be identical."
    comparison = pd.merge(
        production_df[production_df["country"] == "World"].reset_index(drop=True),
        consumption_df[consumption_df["country"] == "World"].reset_index(drop=True),
        how="inner",
        on="year",
    )
    assert (comparison["production_emissions"] == comparison["consumption_emissions"]).all(), error

    # Check that production emissions for the World coincide with global (historical) emissions (on common years).
    comparison = pd.merge(
        production_df[production_df["country"] == "World"][["year", "production_emissions"]].reset_index(drop=True),
        historical_df[["year", "global_fossil_emissions"]],
        how="inner",
        on="year",
    )
    error = "Production emissions for the world were expected to coincide with global fossil emissions."
    assert (
        abs(comparison["production_emissions"] - comparison["global_fossil_emissions"])
        / (comparison["global_fossil_emissions"])
        < 0.001
    ).all(), error

    # Check that emissions in production_df (emissions from the national excel file) coincide with emissions in co2_df
    # (from the Fossil CO2 emissions csv file).
    # Given that country names have not yet been harmonized, rename the only countries that are present in both datasets.
    comparison = pd.merge(
        co2_df[["country", "year", "emissions_total"]],
        production_df.replace({"Bunkers": "International Transport", "World": "Global"}),
        on=["country", "year"],
        how="inner",
    ).dropna(subset=["emissions_total", "production_emissions"], how="any")
    # Since we included the emissions from the Kuwaiti oil fires in Kuwait (and they are not included in production_df),
    # omit that row in the comparison.
    comparison = comparison.drop(
        comparison[(comparison["country"] == "Kuwait") & (comparison["year"] == 1991)].index
    ).reset_index(drop=True)

    error = "Production emissions from national file were expected to coincide with the Fossil CO2 emissions dataset."
    assert (
        (
            100
            * abs(comparison["production_emissions"] - comparison["emissions_total"])
            / (comparison["emissions_total"])
        ).fillna(0)
        < 0.1
    ).all(), error


def sanity_checks_on_output_data(combined_df: pd.DataFrame) -> None:
    """Run sanity checks on output data.

    These checks should be run on the very final output dataframe (with an index) prior to storing it as a table.

    Parameters
    ----------
    combined_df : pd.DataFrame
        Combination of all input dataframes, after processing, harmonization, and addition of variables.

    """
    combined_df = combined_df.reset_index()
    error = "All variables (except traded emissions, growth, and land-use change) should be >= 0 or nan."
    positive_variables = [
        col
        for col in combined_df.columns
        if col != "country"
        if "traded" not in col
        if "growth" not in col
        if "land_use" not in col
    ]
    assert (combined_df[positive_variables].fillna(0) >= 0).all().all(), error

    error = "Production emissions as a share of global emissions should be 100% for 'World' (within 2% error)."
    assert combined_df[
        (combined_df["country"] == "World") & (abs(combined_df["emissions_total_as_share_of_global"] - 100) > 2)
    ].empty, error

    error = "Consumption emissions as a share of global emissions should be 100% for 'World' (within 2% error)."
    assert combined_df[
        (combined_df["country"] == "World") & (abs(combined_df["consumption_emissions_as_share_of_global"] - 100) > 2)
    ].empty, error

    error = "Population as a share of global population should be 100% for 'World'."
    assert combined_df[
        (combined_df["country"] == "World") & (combined_df["population_as_share_of_global"].fillna(100) != 100)
    ].empty, error

    error = "All share of global emissions should be smaller than 100% (within 2% error)."
    share_variables = [col for col in combined_df.columns if "share" in col]
    assert (combined_df[share_variables].fillna(0) <= 102).all().all(), error

    # Check that cumulative variables are monotonically increasing.
    # Firstly, list columns of cumulative variables, but ignoring cumulative columns as a share of global
    # (since they are not necessarily monotonic) and land-use change (which can be negative).
    cumulative_cols = [
        col for col in combined_df.columns if "cumulative" in col if "share" not in col if "land_use" not in col
    ]
    # Using ".is_monotonic_increasing" can fail when differences between consecutive numbers are very small.
    # Instead, sort data backwards in time, and check that consecutive values of cumulative variables always have
    # a percentage change that is smaller than, say, 0.1%.
    error = (
        "Cumulative variables (not given as a share of global) should be monotonically increasing (except when "
        "including land-use change emissions, which can be negative)."
    )
    assert (
        combined_df.sort_values("year", ascending=False)
        .groupby("country")
        .agg({col: lambda x: ((x.pct_change().dropna() * 100) <= 0.1).all() for col in cumulative_cols})
        .all()
        .all()
    ), error

    error = (
        "Production emissions as a share of global production emissions for the World should always be 100% "
        "(or larger than 98%, given small discrepancies)."
    )
    # Consumption emissions as a share of global production emissions is allowed to be smaller than 100%.
    share_variables = [col for col in combined_df.columns if "share" in col if "consumption" not in col]
    assert (combined_df[combined_df["country"] == "World"][share_variables].fillna(100) > 98).all().all(), error

    error = "Traded emissions for the World should be close to zero (within 2% error)."
    world_mask = combined_df["country"] == "World"
    assert (
        abs(
            100
            * combined_df[world_mask]["traded_emissions"].fillna(0)
            / combined_df[world_mask]["emissions_total"].fillna(1)
        )
        < 2
    ).all(), error


def prepare_fossil_co2_emissions(co2_df: pd.DataFrame) -> pd.DataFrame:
    """Prepare Fossil CO2 emissions data (basic processing).

    Select and rename columns to be used, adapt units, and fix known issues.

    Parameters
    ----------
    co2_df : pd.DataFrame
        Production-based emissions from GCP's Fossil CO2 emissions dataset (csv file).

    Returns
    -------
    co2_df : pd.DataFrame
        Fossil CO2 emissions data after basic processing.

    """
    # Select and rename columns from fossil CO2 data.
    co2_df = co2_df[list(CO2_COLUMNS)].rename(columns=CO2_COLUMNS, errors="raise")

    # Ensure all emissions are given in tonnes of CO2.
    co2_df[EMISSION_SOURCES] *= MILLION_TONNES_OF_CO2_TO_TONNES_OF_CO2

    ####################################################################################################################
    # NOTE: For certain years, column "emissions_from_other_industry" is not informed for "World" but it is informed
    # for some countries (namely China and US).
    # This causes the cumulative emissions from other industry as share of global for those countries to become larger
    # than 100%.
    # This temporary solution fixes the issue: We aggregate the data for China and US on those years when the world's
    # data is missing (without touching other years or other columns).
    # Firstly, list of years for which the world has no data for emissions_from_other_industry.
    world_missing_years = (
        co2_df[(co2_df["country"] == "Global") & (co2_df["emissions_from_other_industry"].isnull())]["year"]
        .unique()
        .tolist()  # type: ignore
    )
    # Data that needs to be aggregated.
    data_missing_in_world = co2_df[
        co2_df["year"].isin(world_missing_years) & (co2_df["emissions_from_other_industry"].notnull())
    ]
    # Check that there is indeed data to be aggregated (that is missing for the World).
    error = (
        "Expected emissions_from_other_industry to be null for the world but not null for certain countries "
        "(which was an issue in the original fossil CO2 data). The issue may be fixed and the code can be simplified."
    )
    assert len(data_missing_in_world) > 0, error
    # Create a dataframe of aggregate data for the World, on those years when it's missing.
    aggregated_missing_data = (
        data_missing_in_world.groupby("year")
        .agg({"emissions_from_other_industry": "sum"})
        .reset_index()
        .assign(**{"country": "Global"})
    )
    # Combine the new dataframe of aggregate data with the main dataframe.
    co2_df = dataframes.combine_two_overlapping_dataframes(
        df1=co2_df, df2=aggregated_missing_data, index_columns=["country", "year"], keep_column_order=True
    )
    ####################################################################################################################

    # We add the emissions from "Kuwaiti Oil Fires" (which is also included as a separate country) as part of the
    # emissions of Kuwait. This ensures that they will be included in region aggregates.
    error = "'Kuwaiti Oil Fires' was expected to only have not-null data for 1991."
    assert co2_df[
        (co2_df["country"] == "Kuwaiti Oil Fires")
        & (co2_df["emissions_total"].notnull())
        & (co2_df["emissions_total"] != 0)
    ]["year"].tolist() == [1991], error

    co2_df.loc[(co2_df["country"] == "Kuwait") & (co2_df["year"] == 1991), EMISSION_SOURCES] = (
        co2_df[(co2_df["country"] == "Kuwaiti Oil Fires") & (co2_df["year"] == 1991)][EMISSION_SOURCES].values
        + co2_df[(co2_df["country"] == "Kuwait") & (co2_df["year"] == 1991)][EMISSION_SOURCES].values
    )

    # Check that "emissions_total" agrees with the sum of emissions from individual sources.
    error = "The sum of all emissions should add up to total emissions (within 1%)."
    assert (
        abs(
            co2_df.drop(columns=["country", "year", "emissions_total"]).sum(axis=1)
            - co2_df["emissions_total"].fillna(0)
        )
        / (co2_df["emissions_total"].fillna(0) + 1e-7)
        < 1e-2
    ).all(), error

    # Many rows have zero total emissions, but actually the individual sources are nan.
    # Total emissions in those cases should be nan, instead of zero.
    no_individual_emissions = co2_df.drop(columns=["country", "year", "emissions_total"]).isnull().all(axis=1)
    co2_df.loc[no_individual_emissions, "emissions_total"] = np.nan

    return co2_df


def prepare_consumption_emissions(consumption_df: pd.DataFrame) -> pd.DataFrame:
    """Prepare consumption-based emissions data (basic processing).

    Select and rename columns to be used, adapt units, and fix known issues.

    Parameters
    ----------
    consumption_df : pd.DataFrame
        Consumption-based emissions from GCP's official national emissions dataset (excel file).

    Returns
    -------
    consumption_df : pd.DataFrame
        Consumption-based emissions after basic processing.

    """
    # Select and rename columns.
    consumption_df = consumption_df[list(CONSUMPTION_EMISSIONS_COLUMNS)].rename(
        columns=CONSUMPTION_EMISSIONS_COLUMNS, errors="raise"
    )

    # List indexes of rows in consumption_df corresponding to outliers (defined above in OUTLIERS_IN_CONSUMPTION_DF).
    outlier_indexes = [
        consumption_df[(consumption_df["country"] == outlier[0]) & (consumption_df["year"] == outlier[1])].index.item()
        for outlier in OUTLIERS_IN_CONSUMPTION_DF
    ]

    error = (
        "Outliers were expected to have negative consumption emissions. "
        "Maybe outliers have been fixed (and should be removed from the code)."
    )
    assert (consumption_df.loc[outlier_indexes]["consumption_emissions"] < 0).all(), error

    # Remove outliers.
    consumption_df = consumption_df.drop(outlier_indexes).reset_index(drop=True)

    return consumption_df


def extract_global_emissions(co2_df: pd.DataFrame, historical_df: pd.DataFrame) -> pd.DataFrame:
    """Extract World emissions by combining data from the Fossil CO2 emissions and the global emissions dataset.

    The resulting global emissions data includes bunker and land-use change emissions.

    NOTE: This function has to be used after selecting and renaming columns in co2_df, but before harmonizing country
    names in co2_df (so that "International Transport" is still listed as a country).

    Parameters
    ----------
    co2_df : pd.DataFrame
        Production-based emissions from GCP's Fossil CO2 emissions dataset (csv file).
    historical_df : pd.DataFrame
        Historical emissions from GCP's official global emissions dataset (excel file).

    Returns
    -------
    global_emissions : pd.DataFrame
        World emissions.

    """
    # For some reason, "International Transport" is included as another country, that only has emissions from oil.
    # We separate it as another variable (only given at the global level).
    global_transport = co2_df[co2_df["country"] == INTERNATIONAL_TRANSPORT_LABEL].reset_index(drop=True)

    # Check that total emissions for international transport coincide with oil emissions.
    error = "Total emissions from international transport do not coincide with oil emissions."
    assert all((global_transport["emissions_from_oil"] - global_transport["emissions_total"]).dropna() == 0), error

    # Therefore, we can keep only one column for international transport emissions.
    global_transport = (
        global_transport[["year", "emissions_from_oil"]]
        .dropna()
        .rename(columns={"emissions_from_oil": "global_emissions_from_international_transport"}, errors="raise")
    )

    # Create a new dataframe of global emissions.
    global_emissions = (
        co2_df[co2_df["country"].isin(["Global", "World"])][["year"] + EMISSION_SOURCES]
        .rename(columns={column: f"global_{column}" for column in EMISSION_SOURCES}, errors="raise")
        .sort_values("year")
        .reset_index(drop=True)
    )

    # Add bunker fuels to global emissions.
    global_emissions = pd.merge(global_emissions, global_transport, on=["year"], how="outer")

    # Add historical land-use change emissions to dataframe of global emissions.
    global_emissions = pd.merge(
        global_emissions, historical_df[["year", "global_emissions_from_land_use_change"]], how="left", on="year"
    )

    # Add variable of total emissions including fossil fuels and land use change.
    global_emissions["global_emissions_total_including_land_use_change"] = (
        global_emissions["global_emissions_total"] + global_emissions["global_emissions_from_land_use_change"]
    )

    # Calculate global cumulative emissions.
    for column in EMISSION_SOURCES + ["emissions_from_land_use_change", "emissions_total_including_land_use_change"]:
        global_emissions[f"global_cumulative_{column}"] = global_emissions[f"global_{column}"].cumsum()

    # Add a country column and add global population.
    global_emissions["country"] = "World"

    # Add global population.
    global_emissions = geo.add_population_to_dataframe(df=global_emissions, population_col="global_population")

    return global_emissions


def harmonize_co2_data(co2_df: pd.DataFrame) -> pd.DataFrame:
    """Harmonize country names in Fossil CO2 data, and fix known issues with certain regions.

    Parameters
    ----------
    co2_df : pd.DataFrame
        Production-based emissions from GCP's Fossil CO2 emissions dataset (csv file).

    Returns
    -------
    co2_df : pd.DataFrame
        Fossil CO2 emissions data after harmonizing country names.

    """
    # Harmonize country names in fossil CO2 data.
    co2_df = geo.harmonize_countries(
        df=co2_df,
        countries_file=FOSSIL_CO2_EMISSIONS_COUNTRIES_FILE,
        warn_on_missing_countries=True,
        warn_on_unused_countries=True,
    )

    # Check that there is only one data point for each country-year.
    # After harmonization, "Pacific Islands (Palau)" is mapped to "Palau", and therefore there are rows with different
    # data for the same country-year.
    # However, "Pacific Islands (Palau)" have data until 1991, and "Palau" has data from 1992 onwards.
    # After removing empty rows, there should be no overlap.
    columns_that_must_have_data = co2_df.drop(columns=["country", "year"]).columns
    check = co2_df.dropna(subset=columns_that_must_have_data, how="all").reset_index(drop=True)
    error = "After harmonizing country names, there is more than one data point for the same country-year."
    assert check[check.duplicated(subset=["country", "year"])].empty, error

    return co2_df


def combine_data_and_add_variables(
    co2_df: pd.DataFrame,
    production_df: pd.DataFrame,
    consumption_df: pd.DataFrame,
    global_emissions_df: pd.DataFrame,
    land_use_df: pd.DataFrame,
    gdp_df: pd.DataFrame,
    primary_energy_df: pd.DataFrame,
) -> pd.DataFrame:
    """Combine all relevant data into one dataframe, add region aggregates, and add custom variables (e.g. emissions per
    capita).

    Parameters
    ----------
    co2_df : pd.DataFrame
        Production-based emissions from GCP's Fossil CO2 emissions dataset (csv file), after harmonization.
    production_df : pd.DataFrame
        Production-based emissions from GCP's official national emissions dataset (excel file), after harmonization.
    consumption_df : pd.DataFrame
        Consumption-based emissions from GCP's official national emissions dataset (excel file), after harmonization.
    global_emissions_df : pd.DataFrame
        World emissions (including bunker and land-use change emissions).
    land_use_df : pd.DataFrame
        National land-use change emissions from GCP's official dataset (excel file), after harmonization.
    gdp_df : pd.DataFrame
        GDP data.
    primary_energy_df : pd.DataFrame
        Primary energy data.

    Returns
    -------
    combined_df : pd.DataFrame
        Combined data, with all additional variables and with region aggregates.

    """
    # Add region aggregates that were included in the national emissions file, but not in the Fossil CO2 emissions dataset.
    gcp_aggregates = sorted(set(production_df["country"]) - set(co2_df["country"]))
    co2_df = pd.concat(
        [
            co2_df,
            production_df[production_df["country"].isin(gcp_aggregates)]
            .rename(columns={"production_emissions": "emissions_total"})
            .astype({"year": int}),
        ],
        ignore_index=True,
    ).reset_index(drop=True)

    # Add consumption emissions to main dataframe (keep only the countries of the main dataframe).
    # Given that additional GCP regions (e.g. "Africa (GCP)") have already been added to co2_df
    # (when merging with production_df), all countries from consumption_df should be included in co2_df.
    error = "Some countries in consumption_df are not included in co2_df."
    assert set(consumption_df["country"]) < set(co2_df["country"]), error
    co2_df = pd.merge(co2_df, consumption_df, on=["country", "year"], how="outer")

    # Add population to dataframe.
    co2_df = geo.add_population_to_dataframe(df=co2_df, warn_on_missing_countries=False)

    # Add GDP to main dataframe.
    co2_df = pd.merge(co2_df, gdp_df, on=["country", "year"], how="left")

    # Add primary energy to main dataframe.
    co2_df = pd.merge(co2_df, primary_energy_df, on=["country", "year"], how="left")

    # For convenience, rename columns in land-use change emissions data.
    land_use_df = land_use_df.rename(
        columns={"emissions": "emissions_from_land_use_change", "quality_flag": "land_use_change_quality_flag"}
    )

    # Land-use change data does not include data for the World. Include it by merging with the global dataset.
    land_use_df = pd.concat(
        [
            land_use_df,
            global_emissions_df.rename(
                columns={"global_emissions_from_land_use_change": "emissions_from_land_use_change"}
            )[["year", "emissions_from_land_use_change"]]
            .dropna()
            .assign(**{"country": "World"}),
        ],
        ignore_index=True,
    ).astype({"year": int})

    # Add land-use change emissions to main dataframe.
    co2_df = pd.merge(co2_df, land_use_df, on=["country", "year"], how="outer")

    # Add total emissions (including land-use change) for each country.
    co2_df["emissions_total_including_land_use_change"] = (
        co2_df["emissions_total"] + co2_df["emissions_from_land_use_change"]
    )

    # Add region aggregates.
    # Aggregate not only emissions data, but also population, gdp and primary energy.
    # This way we ensure that custom regions (e.g. "North America (excl. USA)") will have all required data.
    aggregations = {
        column: "sum" for column in co2_df.columns if column not in ["country", "year", "land_use_change_quality_flag"]
    }
    for region in REGIONS:
        countries_in_region = get_countries_in_region(region=region, region_modifications=REGIONS[region])
        co2_df = geo.add_region_aggregates(
            df=co2_df,
            region=region,
            countries_in_region=countries_in_region,
            countries_that_must_have_data=[],
            frac_allowed_nans_per_year=0.999,
            aggregations=aggregations,
        )

    # Add global emissions and global cumulative emissions columns to main dataframe.
    co2_df = pd.merge(co2_df, global_emissions_df.drop(columns="country"), on=["year"], how="left")

    # Ensure main dataframe is sorted (so that cumulative emissions are properly calculated).
    co2_df = co2_df.sort_values(["country", "year"]).reset_index(drop=True)

    # Temporarily add certain global emissions variables.
    # This is done simply to be able to consider "consumption_emissions" as just another type of emission
    # when creating additional variables.
    co2_df["global_consumption_emissions"] = co2_df["global_emissions_total"]
    co2_df["global_cumulative_consumption_emissions"] = co2_df["global_cumulative_emissions_total"]

    # Add new variables for each source of emissions.
    for column in EMISSION_SOURCES + [
        "consumption_emissions",
        "emissions_from_land_use_change",
        "emissions_total_including_land_use_change",
    ]:
        # Add per-capita variables.
        co2_df[f"{column}_per_capita"] = co2_df[column] / co2_df["population"]

        # Add columns for cumulative emissions.
        # Rows that had nan emissions will have nan cumulative emissions.
        # But nans will not be propagated in the sum.
        # This means that countries with some (not all) nans will have the cumulative sum of the informed emissions
        # (treating nans as zeros), but will have nan on those rows that were not informed.
        co2_df[f"cumulative_{column}"] = co2_df.groupby(["country"])[column].cumsum()

        # Add share of global emissions.
        co2_df[f"{column}_as_share_of_global"] = 100 * co2_df[column] / co2_df[f"global_{column}"]

        # Add share of global cumulative emissions.
        co2_df[f"cumulative_{column}_as_share_of_global"] = (
            100 * co2_df[f"cumulative_{column}"] / co2_df[f"global_cumulative_{column}"]
        )

    # Add total emissions per unit energy (in kg of emissions per kWh).
    co2_df["emissions_total_per_unit_energy"] = (
        TONNES_OF_CO2_TO_KG_OF_CO2 * co2_df["emissions_total"] / (co2_df["primary_energy_consumption"] * TWH_TO_KWH)
    )

    # Add total emissions (including land-use change) per unit energy (in kg of emissions per kWh).
    co2_df["emissions_total_including_land_use_change_per_unit_energy"] = (
        TONNES_OF_CO2_TO_KG_OF_CO2
        * co2_df["emissions_total_including_land_use_change"]
        / (co2_df["primary_energy_consumption"] * TWH_TO_KWH)
    )

    # Add total emissions per unit GDP.
    co2_df["emissions_total_per_gdp"] = TONNES_OF_CO2_TO_KG_OF_CO2 * co2_df["emissions_total"] / co2_df["gdp"]

    # Add total emissions (including land-use change) per unit GDP.
    co2_df["emissions_total_including_land_use_change_per_gdp"] = (
        TONNES_OF_CO2_TO_KG_OF_CO2 * co2_df["emissions_total_including_land_use_change"] / co2_df["gdp"]
    )

    # Add total consumption emissions per unit GDP.
    co2_df["consumption_emissions_per_gdp"] = (
        TONNES_OF_CO2_TO_KG_OF_CO2 * co2_df["consumption_emissions"] / co2_df["gdp"]
    )

    # Add variable of emissions embedded in trade.
    co2_df["traded_emissions"] = co2_df["consumption_emissions"] - co2_df["emissions_total"]
    co2_df["pct_traded_emissions"] = 100 * co2_df["traded_emissions"] / co2_df["emissions_total"]
    co2_df["traded_emissions_per_capita"] = co2_df["traded_emissions"] / co2_df["population"]

    # Add variable of emissions embedded in trade, including land-use change emissions.
    co2_df["traded_emissions_including_land_use_change"] = (
        co2_df["consumption_emissions"] - co2_df["emissions_total_including_land_use_change"]
    )
    co2_df["pct_traded_emissions_including_land_use_change"] = (
        100 * co2_df["traded_emissions_including_land_use_change"] / co2_df["emissions_total_including_land_use_change"]
    )
    co2_df["traded_emissions_including_land_use_change_per_capita"] = (
        co2_df["traded_emissions_including_land_use_change"] / co2_df["population"]
    )

    # Remove temporary columns.
    co2_df = co2_df.drop(columns=["global_consumption_emissions", "global_cumulative_consumption_emissions"])

    # Add annual percentage growth of total emissions.
    co2_df["pct_growth_emissions_total"] = co2_df.groupby("country")["emissions_total"].pct_change() * 100

    # Add annual percentage growth of total emissions (including land-use change).
    co2_df["pct_growth_emissions_total_including_land_use_change"] = (
        co2_df.groupby("country")["emissions_total_including_land_use_change"].pct_change() * 100
    )

    # Add annual absolute growth of total emissions.
    co2_df["growth_emissions_total"] = co2_df.groupby("country")["emissions_total"].diff()

    # Add annual absolute growth of total emissions (including land-use change).
    co2_df["growth_emissions_total_including_land_use_change"] = co2_df.groupby("country")[
        "emissions_total_including_land_use_change"
    ].diff()

    # Create variable of population as a share of global population.
    co2_df["population_as_share_of_global"] = co2_df["population"] / co2_df["global_population"] * 100

    # Replace infinity values (for example when calculating growth from zero to non-zero) in the data by nan.
    for column in co2_df.drop(columns=["country", "year"]).columns:
        co2_df.loc[np.isinf(co2_df[column]), column] = np.nan

    # For special GCP countries/regions (e.g. "Africa (GCP)") we should keep only the original data.
    # Therefore, make nan all additional variables for those countries/regions, and keep only GCP's original data.
    added_variables = co2_df.drop(columns=["country", "year"] + COLUMNS_THAT_MUST_HAVE_DATA).columns.tolist()
    co2_df.loc[(co2_df["country"].str.contains(" (GCP)", regex=False)), added_variables] = np.nan

    # Remove uninformative rows (those that have only data for, say, gdp, but not for variables related to emissions).
    co2_df = co2_df.dropna(subset=COLUMNS_THAT_MUST_HAVE_DATA, how="all").reset_index(drop=True)

    return co2_df


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load fossil CO2 emissions data from Meadow.
    co2_ds = catalog.Dataset(MEADOW_CO2_DATASET_PATH)
    # Load main table from CO2 dataset.
    co2_tb = co2_ds[co2_ds.table_names[0]]
    # Create a dataframe out of the CO2 table.
    co2_df = pd.DataFrame(co2_tb).reset_index()

    # Load global (historical) emissions data from Meadow.
    historical_ds = catalog.Dataset(MEADOW_GLOBAL_EMISSIONS_DATASET_PATH)
    historical_tb = historical_ds[historical_ds.table_names[0]]
    historical_df = pd.DataFrame(historical_tb).reset_index()

    # Load national emissions data from Meadow.
    national_ds = catalog.Dataset(MEADOW_NATIONAL_EMISSIONS_DATASET_PATH)
    # Load tables for national production-based emissions and consumption-based emissions.
    production_tb = national_ds["production_emissions"]
    production_df = pd.DataFrame(production_tb).reset_index()
    consumption_tb = national_ds["consumption_emissions"]
    consumption_df = pd.DataFrame(consumption_tb).reset_index()

    # Load national land-use change emissions from Meadow.
    land_use_ds = catalog.Dataset(MEADOW_LAND_USE_EMISSIONS_DATASET_PATH)
    land_use_tb = land_use_ds[land_use_ds.table_names[0]]
    land_use_df = pd.DataFrame(land_use_tb).reset_index()

    # Load primary energy consumption from garden.
    primary_energy_ds = catalog.Dataset(GARDEN_PRIMARY_ENERGY_DATASET_PATH)
    # Create a  dataframe out of the main table of primary energy.
    primary_energy_df = pd.DataFrame(primary_energy_ds[primary_energy_ds.table_names[0]]).reset_index()

    # Load GDP dataset from garden.
    gdp_ds = catalog.Dataset(GARDEN_GDP_DATASET_PATH)
    # Create a dataframe out of the main table of GDP.
    gdp_df = pd.DataFrame(gdp_ds[gdp_ds.table_names[0]]).reset_index()

    #
    # Process data.
    #
    # Prepare fossil CO2 emissions data.
    co2_df = prepare_fossil_co2_emissions(co2_df=co2_df)

    # Prepare consumption-based emission data.
    consumption_df = prepare_consumption_emissions(consumption_df=consumption_df)

    # Select and rename columns from primary energy data.
    primary_energy_df = primary_energy_df[list(PRIMARY_ENERGY_COLUMNS)].rename(
        columns=PRIMARY_ENERGY_COLUMNS, errors="raise"
    )

    # Select and rename columns from primary energy data.
    gdp_df = gdp_df[list(GDP_COLUMNS)].rename(columns=GDP_COLUMNS, errors="raise")

    # Select and rename columns from historical emissions data.
    historical_df = historical_df[list(HISTORICAL_EMISSIONS_COLUMNS)].rename(
        columns=HISTORICAL_EMISSIONS_COLUMNS, errors="raise"
    )

    # Run sanity checks on input data.
    sanity_checks_on_input_data(
        production_df=production_df, consumption_df=consumption_df, historical_df=historical_df, co2_df=co2_df
    )

    # For some reason, "International Transport" is included as another country, that only has emissions from oil.
    # Extract that data and remove it from the rest of national emissions.
    global_emissions_df = extract_global_emissions(co2_df=co2_df, historical_df=historical_df)

    # Harmonize country names in consumption-based emissions data.
    consumption_df = (
        geo.harmonize_countries(
            df=consumption_df,
            countries_file=NATIONAL_EMISSIONS_COUNTRIES_FILE,
            warn_on_missing_countries=False,
            make_missing_countries_nan=True,
        )
        .dropna(subset="country")
        .reset_index(drop=True)
    )

    # Harmonize country names in production-based emissions data.
    production_df = (
        geo.harmonize_countries(
            df=production_df,
            countries_file=NATIONAL_EMISSIONS_COUNTRIES_FILE,
            warn_on_missing_countries=False,
            make_missing_countries_nan=True,
        )
        .dropna(subset="country")
        .reset_index(drop=True)
    )

    # Harmonize national land-use change emissions data.
    land_use_df = (
        geo.harmonize_countries(
            df=land_use_df,
            countries_file=LAND_USE_EMISSIONS_COUNTRIES_FILE,
            warn_on_missing_countries=True,
            make_missing_countries_nan=True,
        )
        .dropna(subset="country")
        .reset_index(drop=True)
    )

    # Harmonize fossil CO2 data.
    co2_df = harmonize_co2_data(co2_df=co2_df)

    # Add new variables to main dataframe (consumption-based emissions, emission intensity, per-capita emissions, etc.).
    combined_df = combine_data_and_add_variables(
        co2_df=co2_df,
        production_df=production_df,
        consumption_df=consumption_df,
        global_emissions_df=global_emissions_df,
        land_use_df=land_use_df,
        gdp_df=gdp_df,
        primary_energy_df=primary_energy_df,
    )

    # Set an appropriate index, ensure there are no rows that only have nan, and sort conveniently.
    combined_df = combined_df.set_index(["country", "year"], verify_integrity=True)
    combined_df = combined_df.dropna(subset=combined_df.columns, how="all").sort_index().sort_index(axis=1)

    # Run sanity checks on output data.
    sanity_checks_on_output_data(combined_df)

    #
    # Save outputs.
    #
    # Create a new garden dataset and use metadata from meadow dataset.
    ds_garden = catalog.Dataset.create_empty(dest_dir)
    ds_garden.metadata = co2_ds.metadata
    # Update metadata using the information in the yaml file.
    ds_garden.metadata.update_from_yaml(METADATA_PATH, if_source_exists="replace")

    # Create a table with the combined data.
    tb_garden = catalog.Table(combined_df)
    # Use metadata from yaml file.
    tb_garden.update_metadata_from_yaml(METADATA_PATH, DATASET_NAME)

    # Add combined table to garden dataset and save dataset.
    ds_garden.add(tb_garden)
    ds_garden.save()
