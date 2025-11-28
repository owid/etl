"""Generate historical poverty estimates by extrapolating income distributions backwards using GDP growth.

This step combines:
- thousand_bins_distribution: Modern income distribution data (1990+) with 1000 quantiles per country
- maddison_project_database: Historical GDP per capita data (back to 1820)
- population: Population data created by Our World in Data

The approach:
1. Start from the earliest available year in thousand_bins (typically 1990)
2. Use Maddison GDP growth rates to create a country growth series back to 1820, using a 3-tier fallback strategy:
    a. Country-level GDP growth
    b. Historical entity-level GDP growth (e.g., USSR, Yugoslavia)
    c. Regional-level GDP growth
3. Apply backward extrapolation to income distributions to estimate historical income distributions
4. Calculate the number and share of people living below different poverty lines, using OWID population data
"""

from typing import Set, Tuple

import numpy as np
import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table
from scipy import stats
from structlog import get_logger

from etl.data_helpers.misc import interpolate_table
from etl.helpers import PathFinder

# Initialize logger
log = get_logger()

# Get paths and naming conventions for current step
paths = PathFinder(__file__)

# Poverty lines (daily income in 2021 PPP$)
POVERTY_LINES = [3, 10, 30]

# Define if we want to interpolate the log of GDP per capita or the absolute values
INTERPOLATE_LOG_GDP = True

# Earliest year for extrapolation
EARLIEST_YEAR = 1820

# Latest year for extrapolation
LATEST_YEAR = 1990

# Latest year for extrapolation (filled PIP data)
LATEST_YEAR_PIP_FILLED = 1981

# Define current year as the year of the version of the step
CURRENT_YEAR = int(paths.version.split("-")[0])

# Define extreme growth factor thresholds
EXTREME_GROWTH_FACTOR_THRESHOLDS = [0.8, 1.20]

# Show warnings
SHOW_WARNINGS = True

# Keep original thousand bins series when calculating bins from mean and gini
KEEP_ORIGINAL_THOUSAND_BINS = True

# Countries that appear in the thousand bins dataset for which we don't have population data.
COUNTRIES_WITHOUT_POPULATION = ["Channel Islands"]

# Define indicator to extract from PIP
PIP_INDICATORS = ["gini", "mean"]

# Define categories to filter in PIP, for survey-based and filled data
PIP_CATEGORIES = {
    "survey": {
        "ppp_version": 2021,
        "poverty_line": "No poverty line",
        "welfare_type": "income or consumption",
        "table": "Income or consumption consolidated",
        "survey_comparability": "No spells",
    },
    "filled": {
        "ppp_version": 2021,
        "poverty_line": "No poverty line",
        "welfare_type": "income or consumption",
        "table": "Income or consumption intra/extrapolated",
        "survey_comparability": "No spells",
    },
}

# NOTE: See if we want to include these countries not available in Maddison Project Database
MISSING_COUNTRIES_AND_REGIONS = {
    "American Samoa": "East Asia",
    "Andorra": "Western Europe",
    "Antigua and Barbuda": "Latin America",
    "Aruba": "Latin America",
    "Bahamas": "Latin America",
    "Belize": "Latin America",
    "Bermuda": "Western Europe",
    "Bhutan": "South and South East Asia",
    "British Virgin Islands": "Latin America",
    "Brunei": "South and South East Asia",
    "Cayman Islands": "Latin America",
    "Channel Islands": "Western Europe",
    "Curacao": "Latin America",
    "East Timor": "South and South East Asia",
    "Eritrea": "Sub Saharan Africa",
    "Faroe Islands": "Western Europe",
    "Fiji": "East Asia",
    "French Polynesia": "East Asia",
    "Gibraltar": "Western Europe",
    "Greenland": "Western Europe",
    "Grenada": "Latin America",
    "Guam": "East Asia",
    "Guyana": "Latin America",
    "Isle of Man": "Western Europe",
    "Kiribati": "East Asia",
    "Liechtenstein": "Western Europe",
    "Macao": "East Asia",
    "Maldives": "South and South East Asia",
    "Marshall Islands": "East Asia",
    "Micronesia (country)": "East Asia",
    "Monaco": "Western Europe",
    "Nauru": "East Asia",
    "New Caledonia": "East Asia",
    "Northern Mariana Islands": "East Asia",
    "Palau": "East Asia",
    "Papua New Guinea": "East Asia",
    "Saint Kitts and Nevis": "Latin America",
    "Saint Martin (French part)": "Latin America",
    "Saint Vincent and the Grenadines": "Latin America",
    "Samoa": "East Asia",
    "San Marino": "Western Europe",
    "Sint Maarten (Dutch part)": "Latin America",
    "Solomon Islands": "East Asia",
    "Somalia": "Sub Saharan Africa",
    "Suriname": "Latin America",
    "Tonga": "East Asia",
    "Turks and Caicos Islands": "Latin America",
    "Tuvalu": "East Asia",
    "United States Virgin Islands": "Latin America",
    "Vanuatu": "East Asia",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Load thousand bins dataset, and read its main table.
    ds_thousand_bins = paths.load_dataset("thousand_bins_distribution")
    tb_thousand_bins = ds_thousand_bins.read("thousand_bins_distribution")

    # Load Maddison Project Database, and read its main table.
    ds_maddison = paths.load_dataset("maddison_project_database")
    tb_maddison = ds_maddison.read("maddison_project_database")

    # Load World Bank PIP dataset, and read its main table.
    ds_pip = paths.load_dataset("world_bank_pip")
    tb_pip = ds_pip.read("world_bank_pip")

    # Load historical inequality dataset, and read its main table.
    ds_van_zanden = paths.load_dataset("historical_inequality_van_zanden_et_al")
    tb_van_zanden = ds_van_zanden.read("historical_inequality_van_zanden_et_al")

    #
    # Prepare data.
    # #
    # Prepare GDP data
    tb_gdp = prepare_gdp_data(tb_maddison)

    ###############################################################################
    # 1. KEEPING INEQUALITY CONSTANT
    ###############################################################################

    # Perform backward extrapolation
    tb_extended = extrapolate_backwards(tb_thousand_bins=tb_thousand_bins, tb_gdp=tb_gdp)

    # Calculate poverty measures
    tb = calculate_poverty_measures(tb=tb_extended)

    # Create stacked variables for stacked area/bar charts
    tb = create_stacked_variables(tb=tb)

    # Calculate an alternative method with our population dataset
    tb, tb_population = calculate_alternative_method_with_population_dataset(tb_poverty=tb)

    ###############################################################################
    # 2. WITH INEQUALITY CHANGES
    ###############################################################################

    # Prepare World Bank PIP data
    tb_pip = prepare_pip_data(tb_pip=tb_pip, tb_thousand_bins=tb_thousand_bins)

    # Calculate Ginis from thousand bins distribution
    tb_pip = create_ginis_from_thousand_bins_distribution(tb_thousand_bins=tb_thousand_bins, tb_pip=tb_pip)

    # Add ginis from Van Zanden et al.
    tb_gini_mean = add_ginis_from_van_zanden(tb_pip=tb_pip, tb_van_zanden=tb_van_zanden)

    # Prepare mean and gini data for extrapolation
    tb_gini_mean = prepare_mean_gini_data(tb_gini_mean=tb_gini_mean, tb_gdp=tb_gdp)

    ###############################################################################
    # 2.1 USING EXTRAPOLATED MEANS AND GINIS
    ###############################################################################

    # Create 1000 bins from mean and gini data
    tb_thousand_bins_from_interpolated_mean_gini = expand_means_and_ginis_to_thousand_bins(
        tb_gini_mean=tb_gini_mean, tb_thousand_bins=tb_thousand_bins, mean_column="mean", gini_column="gini"
    )

    # Calculate poverty measures
    tb_from_interpolated_mean_gini = calculate_poverty_measures(tb=tb_thousand_bins_from_interpolated_mean_gini)

    # Create stacked variables for stacked area/bar charts
    tb_from_interpolated_mean_gini = create_stacked_variables(tb=tb_from_interpolated_mean_gini)

    # Calculate an alternative method with our population dataset
    tb_from_interpolated_mean_gini, tb_from_interpolated_mean_gini_population = (
        calculate_alternative_method_with_population_dataset(tb_poverty=tb_from_interpolated_mean_gini)
    )

    ###############################################################################
    # 2.2 USING EXTRAPOLATED MEANS (BUT NOT GINIS) AND INTERPOLATING THOUSAND BINS
    ###############################################################################

    # Create 1000 bins from inter/extrapolated means and original Ginis, except for years between the earliest year and first year with data
    tb_thousand_bins_from_interpolated_mean = expand_means_and_ginis_to_thousand_bins(
        tb_gini_mean=tb_gini_mean, tb_thousand_bins=tb_thousand_bins, mean_column="mean", gini_column="gini_original"
    )

    tb_thousand_bins_from_interpolated_mean = interpolate_quantiles_in_thousand_bins(
        tb_thousand_bins_from_interpolated_mean=tb_thousand_bins_from_interpolated_mean
    )

    # Calculate poverty measures
    tb_from_interpolated_mean = calculate_poverty_measures(tb=tb_thousand_bins_from_interpolated_mean)

    # Create stacked variables for stacked area/bar charts
    tb_from_interpolated_mean = create_stacked_variables(tb=tb_from_interpolated_mean)

    # Calculate an alternative method with our population dataset
    tb_from_interpolated_mean, tb_from_interpolated_mean_population = (
        calculate_alternative_method_with_population_dataset(tb_poverty=tb_from_interpolated_mean)
    )

    ###############################################################################

    tb = tb.format(["country", "year", "poverty_line"], short_name="historical_poverty")
    tb_population = tb_population.format(["country", "year"], short_name="population")
    # tb_extended = tb_extended.format(
    #     ["country", "year", "region", "region_old", "quantile"], short_name="historical_income_distribution"
    # )

    tb_from_interpolated_mean_gini = tb_from_interpolated_mean_gini.format(
        ["country", "year", "poverty_line"], short_name="historical_poverty_from_interpolated_mean_gini"
    )
    tb_from_interpolated_mean_gini_population = tb_from_interpolated_mean_gini_population.format(
        ["country", "year"], short_name="population_from_interpolated_mean_gini"
    )
    # tb_thousand_bins_from_interpolated_mean_gini = tb_thousand_bins_from_interpolated_mean_gini.format(
    #     ["country", "year", "region", "region_old", "quantile"], short_name="historical_income_distribution_from_interpolated_mean_gini"
    # )

    tb_from_interpolated_mean = tb_from_interpolated_mean.format(
        ["country", "year", "poverty_line"], short_name="historical_poverty_from_interpolated_mean"
    )
    tb_from_interpolated_mean_population = tb_from_interpolated_mean_population.format(
        ["country", "year"], short_name="population_from_interpolated_mean"
    )
    # tb_thousand_bins_from_interpolated_mean = tb_thousand_bins_from_interpolated_mean.format(
    #     ["country", "year", "region", "region_old", "quantile"], short_name="historical_income_distribution_from_interpolated_mean"
    # )

    #
    # Save outputs.
    #
    # Create dataset
    ds_garden = paths.create_dataset(
        tables=[
            tb,
            tb_population,
            tb_from_interpolated_mean_gini,
            tb_from_interpolated_mean_gini_population,
            tb_from_interpolated_mean,
            tb_from_interpolated_mean_population,
            # tb_extended,
            # tb_thousand_bins_from_interpolated_mean_gini,
            # tb_thousand_bins_from_interpolated_mean,
        ],
        default_metadata=ds_thousand_bins.metadata,
        repack=False,
    )

    # Save dataset
    ds_garden.save()


def prepare_gdp_data(tb_maddison: Table) -> Table:
    """
    Prepare GDP per capita data for extrapolation, creating growth factors from country, historical entity, and region levels.
    """
    # Prepare a dictionary of historical regions:
    historical_entities = {
        region: {
            field: value
            for field, value in paths.regions.get_region(region).items()
            if field in ["end_year", "successors"]
        }
        for region in ["USSR", "Yugoslavia", "Czechoslovakia", "Sudan (former)"]
    }

    # Select relevant columns
    tb_gdp = tb_maddison[["country", "year", "gdp_per_capita", "region"]].copy()

    # Restrict data to years EARLIEST_YEAR to LATEST_YEAR
    tb_gdp = tb_gdp[(tb_gdp["year"] >= EARLIEST_YEAR) & (tb_gdp["year"] <= LATEST_YEAR)].reset_index(drop=True)

    # Remove rows with missing GDP per capita
    tb_gdp = tb_gdp.dropna(subset=["gdp_per_capita"]).reset_index(drop=True)

    # Assert that historical_entities keys and successor states are in tb_gdp
    all_countries = set(tb_gdp["country"].unique())
    for entity_name, entity_data in historical_entities.items():
        if entity_name not in all_countries:
            log.error(f"prepare_gdp_data: Historical entity '{entity_name}' not found in GDP data")
        # Get region for this entity.
        region = tb_maddison[tb_maddison["country"] == entity_name]["region"].drop_duplicates().item()
        for successor in entity_data["successors"]:
            if successor not in all_countries:
                if SHOW_WARNINGS:
                    log.warning(
                        f"prepare_gdp_data: Successor state '{successor}' of '{entity_name}' not found in GDP data. Adding it to table."
                    )
                # Create a new row for the missing successor state
                tb_add_successor = Table(
                    pd.DataFrame(
                        data={
                            "country": [successor],
                            "year": [LATEST_YEAR],
                            "gdp_per_capita": [pd.NA],
                            "region": [region],
                        }
                    )
                )
                # Append to tb_gdp
                tb_gdp = pr.concat([tb_gdp, tb_add_successor], ignore_index=True)

    # Store region information separately (categorical column can't be interpolated)
    regions_map = tb_gdp.groupby("country")["region"].first().to_dict()

    ####################################
    # COUNTRIES INTERPOLATION
    ####################################

    # Interpolate missing years using geometric mean growth rate
    # This fills gaps between available years
    # Drop region before interpolation (categorical columns can't be interpolated)
    tb_gdp = tb_gdp.drop(columns=["region"])

    # Create log_gdp_per_capita column, as the logarithm of gdp_per_capita
    tb_gdp["log_gdp_per_capita"] = tb_gdp["gdp_per_capita"].apply(lambda x: np.log(x) if pd.notna(x) else x)

    tb_gdp = interpolate_table(
        tb_gdp,
        entity_col="country",
        time_col="year",
        time_mode="full_range",  # All the years between min and max year of the table for each country
        method="linear",
        limit_direction="both",
        limit_area="inside",
    )

    if INTERPOLATE_LOG_GDP:
        # Convert back from log to absolute values
        tb_gdp["gdp_per_capita"] = tb_gdp["log_gdp_per_capita"].apply(lambda x: np.exp(x) if pd.notna(x) else x)

    # Restore region information
    tb_gdp["region"] = tb_gdp["country"].map(regions_map)

    # Create growth_factor column, dividing GDP values by the value in the previous year.
    tb_gdp = tb_gdp.sort_values(["country", "year"])
    tb_gdp["growth_factor"] = tb_gdp.groupby("country")["gdp_per_capita"].transform(lambda x: x / x.shift(1))

    ###################################
    # REGIONS
    ###################################

    # Remove (Maddison) from country column if present
    tb_gdp["country"] = tb_gdp["country"].str.replace(" (Maddison)", "", regex=False)

    # Create tb_gdp_regions table, selecting only regions, which are the countries including (Maddison) and World
    region_entities = pd.Index(list(regions_map.values()) + ["World"]).dropna().unique().tolist()
    tb_gdp_regions = tb_gdp[tb_gdp["country"].isin(region_entities)].reset_index(drop=True)

    ###############################
    # HISTORICAL ENTITIES
    ###############################

    # Create a historical entities table
    tb_historical_entities = []
    for entity_name, entity_data in historical_entities.items():
        tb_entity = tb_gdp[(tb_gdp["country"] == entity_name)].reset_index(drop=True)
        tb_entity["successors"] = [list(entity_data["successors"]) for _ in range(len(tb_entity))]
        tb_historical_entities.append(tb_entity)

    # Concatenate all historical entities data
    tb_historical_entities = pr.concat(tb_historical_entities, ignore_index=True)

    # Expand successor_states into multiple rows and rename columns
    tb_historical_entities = tb_historical_entities.explode("successors").rename(
        columns={"country": "historical_entity", "successors": "country"}
    )

    # Restore region information
    tb_historical_entities["region"] = tb_historical_entities["country"].map(regions_map)

    # Merge tb, tb_gdp_regions, and tb_historical_entities
    tb_gdp = pr.merge(
        tb_gdp,
        tb_gdp_regions,
        left_on=["region", "year"],
        right_on=["country", "year"],
        how="outer",
        suffixes=("", "_region"),
    )

    # Drop extra country_region column
    tb_gdp = tb_gdp.drop(columns=["country_region", "region_region"], errors="raise")

    # Merge with historical entities
    tb_gdp = pr.merge(
        tb_gdp,
        tb_historical_entities,
        on=["country", "region", "year"],
        how="outer",
        suffixes=("", "_historical_entity"),
    )

    # Rename growth factor columns
    tb_gdp = tb_gdp.rename(columns={"growth_factor": "growth_factor_country"}, errors="raise")

    # Generate growth_factor column using priority: country > historical_entity > region
    tb_gdp[["growth_factor", "growth_factor_origin"]] = tb_gdp.apply(select_growth_factor, axis=1)

    # Copy metadata from growth_factor_country to growth_factor
    tb_gdp["growth_factor"] = tb_gdp["growth_factor"].copy_metadata(tb_gdp["growth_factor_country"])

    # Shift growth_factor down by one year to align with the starting year of extrapolation
    tb_gdp["growth_factor"] = tb_gdp.groupby("country")["growth_factor"].shift(-1)
    tb_gdp["growth_factor_origin"] = tb_gdp.groupby("country")["growth_factor_origin"].shift(-1)

    # Calculate the the cumulative growth factor product from LATEST_YEAR to each year
    # First, sort by country and year (descending)
    tb_gdp = tb_gdp.sort_values(["country", "year"], ascending=[True, False]).reset_index(drop=True)

    # Make growth_factor Float64 to avoid issues with cumprod
    tb_gdp["growth_factor"] = tb_gdp["growth_factor"].astype("Float64")
    tb_gdp["cumulative_growth_factor"] = tb_gdp.groupby("country")["growth_factor"].cumprod()

    # Restore original order
    tb_gdp = tb_gdp.sort_values(["country", "year"]).reset_index(drop=True)

    # Drop region_entities in country column
    tb_gdp = tb_gdp[~tb_gdp["country"].isin(region_entities)].reset_index(drop=True)

    # Drop empty values for country
    tb_gdp = tb_gdp.dropna(subset=["country"]).reset_index(drop=True)

    # Show country, year, region, and historical_entity columns first and keep relevant growth factor columns
    tb_gdp = tb_gdp[
        [
            "country",
            "year",
            "region",
            "historical_entity",
            "growth_factor",
            "growth_factor_origin",
            "cumulative_growth_factor",
        ]
    ]

    # Check for extreme growth rates
    extreme_growth = tb_gdp[
        (tb_gdp["growth_factor"] > EXTREME_GROWTH_FACTOR_THRESHOLDS[1])
        | (tb_gdp["growth_factor"] < EXTREME_GROWTH_FACTOR_THRESHOLDS[0])
    ].reset_index(drop=True)
    if SHOW_WARNINGS:
        if len(extreme_growth) > 0:
            log.warning(
                f"prepare_gdp_data: Found {len(extreme_growth)} instances of extreme growth "
                f"(<-{round((1- EXTREME_GROWTH_FACTOR_THRESHOLDS[0]) * 100, 1)}% or >+{round((EXTREME_GROWTH_FACTOR_THRESHOLDS[1]-1) * 100, 1)}% or  in a single year)"
            )
            # Filter extreme_growth to only include country-years where growth_factor_origin is historical_entity or region
            extreme_growth_introduced = extreme_growth[
                extreme_growth["growth_factor_origin"].isin(["historical_entity", "region"])
            ].reset_index(drop=True)
            if len(extreme_growth_introduced) > 0:
                log.warning(
                    f"prepare_gdp_data: Out of these, {len(extreme_growth_introduced)} instances ({round(len(extreme_growth_introduced) / len(extreme_growth) * 100, 1)}%) come from historical_entity or region-level growth rates."
                )
                # Show some examples
                log.warning(
                    f"{extreme_growth_introduced[['country', 'year', 'growth_factor', 'growth_factor_origin']]}"
                )

    return tb_gdp


def extrapolate_backwards(tb_thousand_bins: Table, tb_gdp: Table) -> Table:
    """
    Extrapolate income distributions backwards from 1990 to 1820, using the cumulative GDP growth factors in the 1000-binned income distribution data.
    """
    # Create tb_thousand_bins_to_extrapolate
    tb_thousand_bins_to_extrapolate = tb_thousand_bins[tb_thousand_bins["year"] == LATEST_YEAR].reset_index(drop=True)

    missing_in_thousand_bins_to_extrapolate, missing_in_gdp = compare_countries_available_in_two_tables(
        tb_1=tb_thousand_bins_to_extrapolate,
        tb_2=tb_gdp,
        name_tb_1="thousand_bins_to_extrapolate",
        name_tb_2="gdp",
    )

    # For tb_thousand_bins_to_extrapolate, column year, assign a list of years from EARLIEST_YEAR to LATEST_YEAR - 1 and then explode
    tb_thousand_bins_to_extrapolate = (
        tb_thousand_bins_to_extrapolate.assign(year=lambda df: [list(range(EARLIEST_YEAR, LATEST_YEAR))] * len(df))
        .explode("year")
        .reset_index(drop=True)
    )

    # Drop population column, we will add other data on population later
    tb_thousand_bins_to_extrapolate = tb_thousand_bins_to_extrapolate.drop(columns=["pop"])

    # Drop countries in missing_countries_bins from tb_thousand_bins_to_extrapolate
    tb_thousand_bins_to_extrapolate = tb_thousand_bins_to_extrapolate[
        ~tb_thousand_bins_to_extrapolate["country"].isin(missing_in_gdp)
    ].reset_index(drop=True)

    # Merge with tb_gdp to get growth factors
    tb_thousand_bins_to_extrapolate = pr.merge(
        tb_thousand_bins_to_extrapolate,
        tb_gdp[["country", "year", "cumulative_growth_factor"]],
        on=["country", "year"],
        how="left",
    )

    # Divide avg income columns by cumulative_growth_factor to extrapolate backwards
    # NOTE: The "avg" field refers to the values of 1990, which has been repeated for all previous years. And the cumulative growth factor contains, for each year, the ratio of per capita GDP, GDPpc(1990)/GDPpc(year). Therefore, to get the average income (or consumption) of a given year, we do:
    # Average(year) = Average(1990) / (GDPpc(1990)/GDPpc(year)) = Average(1990) / cumulative growth factor
    tb_thousand_bins_to_extrapolate["avg"] = (
        tb_thousand_bins_to_extrapolate["avg"] / tb_thousand_bins_to_extrapolate["cumulative_growth_factor"]
    )

    # Add a column with population data
    tb_thousand_bins_to_extrapolate = paths.regions.add_population(
        tb=tb_thousand_bins_to_extrapolate,
        population_col="pop",
        warn_on_missing_countries=True,
        interpolate_missing_population=True,
        expected_countries_without_population=COUNTRIES_WITHOUT_POPULATION,
    )

    # Divide pop into quantiles (1000 quantiles)
    tb_thousand_bins_to_extrapolate["pop"] /= 1000

    # Sanity check: Check for missing population data at country-year level.
    missing_pop_mask = tb_thousand_bins_to_extrapolate["pop"].isna()

    if missing_pop_mask.any():
        # Get all country-year combinations with missing population
        missing_pop_rows = tb_thousand_bins_to_extrapolate[missing_pop_mask][["country", "year"]].drop_duplicates()

        # Get unexpected missing data (countries not in the expected list)
        unexpected_missing = missing_pop_rows[~missing_pop_rows["country"].isin(COUNTRIES_WITHOUT_POPULATION)]

        if not unexpected_missing.empty:
            # Group by country to show detailed year information
            unexpected_summary = []
            for country in unexpected_missing["country"].unique():
                years = sorted(unexpected_missing[unexpected_missing["country"] == country]["year"].tolist())

                # Find consecutive year ranges to display more clearly
                ranges = []
                start = years[0]
                end = years[0]

                for i in range(1, len(years)):
                    if years[i] == end + 1:
                        end = years[i]
                    else:
                        ranges.append(f"{start}-{end}" if start != end else str(start))
                        start = years[i]
                        end = years[i]
                ranges.append(f"{start}-{end}" if start != end else str(start))

                # Format output - show all ranges
                years_display = ", ".join(ranges)
                unexpected_summary.append(f"{country}: {years_display} ({len(years)} years total)")

            # Log the error details
            log.error(
                f"Found {len(unexpected_missing)} unexpected country-year combinations with missing population data:\n"
                + "\n".join(unexpected_summary)
            )

            # Calculate and log population impact
            affected_countries = set(unexpected_missing["country"].unique())
            calculate_population_of_missing_countries(affected_countries)

            # Raise assertion error
            raise AssertionError(
                f"Found {len(unexpected_missing)} unexpected country-year combinations with missing population data. See log for details."
            )

        # Verify that all missing data is from expected countries
        set_countries_missing_pop = set(missing_pop_rows["country"])
        assert (
            set_countries_missing_pop == set(COUNTRIES_WITHOUT_POPULATION)
        ), f"Unexpected countries missing population data: {set_countries_missing_pop - set(COUNTRIES_WITHOUT_POPULATION)}"

    # Drop cumulative_growth_factor column, as it's no longer needed
    tb_thousand_bins_to_extrapolate = tb_thousand_bins_to_extrapolate.drop(columns=["cumulative_growth_factor"])

    # Concatenate with original tb_thousand_bins to get the 1000-binned distribution from EARLIEST_YEAR to present
    tb_thousand_bins_extended = pr.concat([tb_thousand_bins, tb_thousand_bins_to_extrapolate], ignore_index=True)

    # Sort values
    tb_thousand_bins_extended = tb_thousand_bins_extended.sort_values(["country", "year", "quantile"]).reset_index(
        drop=True
    )

    return tb_thousand_bins_extended


def calculate_poverty_measures(tb: Table) -> Table:
    """
    Calculate poverty headcount and headcount ratios and for all poverty lines.
    For each year, the data is sorted by income avg, and the cumulative population is calculated.
    The headcount is the cumulative population where avg < poverty line.
    The headcount ratio is the headcount as a percentage of the global population.

    This function returns two tables - one with poverty measures and another with population estimates (only to deal with duplicates in the dimension poverty_line).
    """
    # Sort table by year and avg
    tb = tb.sort_values(["year", "avg"]).reset_index(drop=True)

    # Calculate the cumulative sum of the population by year
    tb["cum_pop"] = tb.groupby("year")["pop"].cumsum()

    # Calculate the global population as the last value of cum_pop by year
    tb["global_population"] = tb.groupby("year")["cum_pop"].transform("max")

    # Calculate the cumulative sum of the population as a percentage of the global population by year
    tb["percentage_global_pop"] = tb["cum_pop"] / tb["global_population"] * 100

    # Define empty list to store poverty rows
    tb_poverty = []

    # Calculate results for each poverty line
    for poverty_line in POVERTY_LINES:
        # Filter rows where avg is less than poverty line, and keep only relevant columns
        tb_poverty_line = tb[tb["avg"] < poverty_line][
            ["year", "global_population", "cum_pop", "percentage_global_pop"]
        ].reset_index(drop=True)

        # Get the last row for each year (highest quantile below poverty line)
        tb_poverty_line = tb_poverty_line.groupby("year").tail(1).reset_index(drop=True)

        # Add poverty_line column
        tb_poverty_line["poverty_line"] = poverty_line

        # Append to tb_poverty
        tb_poverty.append(tb_poverty_line)

    # Concatenate all poverty lines
    tb_poverty = pr.concat(tb_poverty, ignore_index=True)

    # Rename columns
    tb_poverty = tb_poverty.rename(
        columns={"cum_pop": "headcount", "percentage_global_pop": "headcount_ratio", "global_population": "population"},
        errors="raise",
    )

    # Copy metadata from avg to headcount
    tb_poverty["headcount"] = tb_poverty["headcount"].copy_metadata(tb["avg"])
    tb_poverty["headcount_ratio"] = tb_poverty["headcount_ratio"].copy_metadata(tb["avg"])
    # Fix units
    tb_poverty["headcount"].metadata.unit = "people"
    tb_poverty["headcount"].metadata.short_unit = ""

    # Add country column
    tb_poverty["country"] = "World"

    return tb_poverty


def calculate_population_of_missing_countries(missing_countries: Set[str]) -> None:
    """
    Calculate population estimates for countries missing in the main population dataset, and what do they represent as a share of the world population.
    """
    # Create table with column country as missing_countries
    tb_population_missing = Table(pd.DataFrame(data={"country": list(missing_countries)}))

    # Assign column year as CURRENT_YEAR
    tb_population_missing["year"] = CURRENT_YEAR

    # Add population column using paths.regions.add_population
    tb_population_missing = paths.regions.add_population(
        tb=tb_population_missing,
        population_col="population",
        warn_on_missing_countries=False,
        interpolate_missing_population=True,
    )

    # Calculate total world population for CURRENT_YEAR
    tb_world_population = paths.regions.add_population(
        tb=Table(pd.DataFrame(data={"country": ["World"], "year": [CURRENT_YEAR]})),
        population_col="world_population",
        warn_on_missing_countries=False,
        interpolate_missing_population=True,
    )

    world_population = tb_world_population["world_population"].item()

    # Calculate population share of world population
    tb_population_missing["population_share_of_world"] = tb_population_missing["population"] / world_population * 100

    # Aggregate population and population_share_of_world
    tb_population_missing = tb_population_missing.groupby("year").sum().reset_index()

    # Define missing_population
    missing_population = tb_population_missing["population"].item()
    missing_population_share = tb_population_missing["population_share_of_world"].item()

    log.warning(
        f"This represents {int(missing_population):,} people in {CURRENT_YEAR} ({missing_population_share:.2f}% of the world population)."
    )

    return None


def select_growth_factor(row):
    """
    Select growth factor based on priority: country > historical_entity > region.
    This way, we have the longest country-specific growth series possible.
    """
    if not pd.isna(row["growth_factor_country"]):
        return pd.Series({"growth_factor": row["growth_factor_country"], "growth_factor_origin": "country"})
    elif not pd.isna(row["growth_factor_historical_entity"]):
        return pd.Series(
            {"growth_factor": row["growth_factor_historical_entity"], "growth_factor_origin": "historical_entity"}
        )
    else:
        return pd.Series({"growth_factor": row["growth_factor_region"], "growth_factor_origin": "region"})


def create_stacked_variables(tb: Table) -> Table:
    """
    Create stacked variables from the indicators to plot them as stacked area/bar charts
    """
    tb = tb.copy()

    # Define headcount_above and headcount_ratio_above variables
    tb["headcount_above"] = tb["population"] - tb["headcount"]
    tb["headcount_ratio_above"] = 100 * (tb["headcount_above"] / tb["population"])

    # Define stacked variables as headcount and headcount_ratio between poverty lines
    # Select only the necessary columns and pivot
    tb_pivot = pr.pivot(
        data=tb[["country", "year", "poverty_line", "headcount_ratio", "headcount", "population"]],
        index=["country", "year"],
        columns=["poverty_line"],
    )

    for i in range(len(POVERTY_LINES)):
        # if it's the first value only continue
        if i == 0:
            continue

        # If it's the last value calculate the people between this value and the previous
        # and also the people over this poverty line (and percentages)
        else:
            varname_n = ("headcount_between", f"{POVERTY_LINES[i-1]} and {POVERTY_LINES[i]}")
            varname_pct = ("headcount_ratio_between", f"{POVERTY_LINES[i-1]} and {POVERTY_LINES[i]}")
            tb_pivot[varname_n] = (
                tb_pivot[("headcount", POVERTY_LINES[i])] - tb_pivot[("headcount", POVERTY_LINES[i - 1])]
            )
            tb_pivot[varname_pct] = 100 * (tb_pivot[varname_n] / tb_pivot[("population", POVERTY_LINES[i])])

    # Now, only keep headcount_between and headcount_ratio_between, and headcount_above and headcount_ratio_above
    tb_pivot = tb_pivot.loc[
        :,
        tb_pivot.columns.get_level_values(0).isin(
            [
                "country",
                "year",
                "headcount_between",
                "headcount_ratio_between",
            ]
        ),
    ]

    # Stack table
    tb_pivot = (
        tb_pivot.reset_index()
        .set_index(["country", "year"])  # Set the desired index, including the additional columns
        .stack(level=["poverty_line"], future_stack=True)  # Stack the MultiIndex columns
        .reset_index()  # Reset the index to flatten the table
    )

    # Make poverty_line a string column
    tb["poverty_line"] = tb["poverty_line"].astype(str)

    # Merge with tb
    tb = pr.merge(tb, tb_pivot, on=["country", "year", "poverty_line"], how="outer")

    # Copy metadata to recover origin
    tb["headcount_between"] = tb["headcount_between"].copy_metadata(tb["headcount"])
    tb["headcount_ratio_between"] = tb["headcount_ratio_between"].copy_metadata(tb["headcount_ratio"])

    return tb


def calculate_alternative_method_with_population_dataset(tb_poverty: Table) -> Tuple[Table, Table]:
    """
    Calculate an alternative method with our population dataset, to compare results in Grapher. It calculates headcount_ratio_omm using population_omm from Our World in Data and also saves a population table with population differences.
    """
    # First, add population_omm column, the population of the world from Our World in Data
    tb_poverty = paths.regions.add_population(
        tb=tb_poverty,
        population_col="population_omm",
        warn_on_missing_countries=True,
        interpolate_missing_population=True,
    )

    # Calculate headcount_ratio_omm
    tb_poverty["headcount_ratio_omm"] = tb_poverty["headcount"] / tb_poverty["population_omm"] * 100

    # Create a different table to keep population estimates
    tb_population = tb_poverty[["country", "year", "poverty_line", "population", "population_omm"]].reset_index(
        drop=True
    )

    # Select first poverty line in POVERTY_LINES to avoid duplicates
    tb_population = tb_population[tb_population["poverty_line"] == str(POVERTY_LINES[0])].reset_index(drop=True)

    # Drop poverty_line column
    tb_population = tb_population.drop(columns=["poverty_line"])

    # Add population differences columns
    tb_population["population_diff"] = tb_population["population_omm"] - tb_population["population"]
    tb_population["population_diff_pct"] = tb_population["population_diff"] / tb_population["population_omm"] * 100

    # Add population as a share of population_omm
    tb_population["population_share_of_omm"] = tb_population["population"] / tb_population["population_omm"] * 100

    # Drop population columns from tb_poverty
    tb_poverty = tb_poverty.drop(columns=["population", "population_omm"])

    return tb_poverty, tb_population


def prepare_pip_data(tb_pip: Table, tb_thousand_bins: Table) -> Table:
    """
    Prepare World Bank PIP data to use it in extrapolations.
    I want to have the consolidated Gini and the extrapolated mean here.
    """

    tb_pip = tb_pip.copy()
    tb_thousand_bins = tb_thousand_bins.copy()

    # Keep only relevant columns
    tb_pip = tb_pip[
        [
            "country",
            "year",
            "ppp_version",
            "poverty_line",
            "welfare_type",
            "decile",
            "table",
            "survey_comparability",
        ]
        + PIP_INDICATORS
    ]

    # As Argentina is only available in PIP as "Argentina (urban)", rename it to "Argentina" to match with thousand_bins
    tb_pip["country"] = tb_pip["country"].replace({"Argentina (urban)": "Argentina"})

    missing_in_thousand_bins, missing_in_pip = compare_countries_available_in_two_tables(
        tb_1=tb_thousand_bins, tb_2=tb_pip, name_tb_1="thousand_bins", name_tb_2="pip"
    )

    # Drop countries missing in thousand_bins from tb_pip
    tb_pip = tb_pip[~tb_pip["country"].isin(missing_in_thousand_bins)].reset_index(drop=True)

    # Check if all categories I want to filter are present
    for filled_or_survey, category_filters in PIP_CATEGORIES.items():
        for column, category in category_filters.items():
            unique_values = tb_pip[column].unique()
            assert category in unique_values, (
                f"prepare_pip_data: Type of data '{filled_or_survey}' - Category '{category}' for column '{column}' not found in PIP data. "
                f"Available values: {unique_values}"
            )

    # Filter data for each category and concatenate
    # I need two tables, tb_pip_survey and tb_pip_filled, for both mean and gini
    tb_pip_survey = tb_pip[
        (tb_pip["ppp_version"] == PIP_CATEGORIES["survey"]["ppp_version"])
        & (tb_pip["poverty_line"] == PIP_CATEGORIES["survey"]["poverty_line"])
        & (tb_pip["welfare_type"] == PIP_CATEGORIES["survey"]["welfare_type"])
        & (tb_pip["table"] == PIP_CATEGORIES["survey"]["table"])
        & (tb_pip["survey_comparability"] == PIP_CATEGORIES["survey"]["survey_comparability"])
        & (tb_pip["decile"].isna())
    ].reset_index(drop=True)

    tb_pip_filled = tb_pip[
        (tb_pip["ppp_version"] == PIP_CATEGORIES["filled"]["ppp_version"])
        & (tb_pip["poverty_line"] == PIP_CATEGORIES["filled"]["poverty_line"])
        & (tb_pip["welfare_type"] == PIP_CATEGORIES["filled"]["welfare_type"])
        & (tb_pip["table"] == PIP_CATEGORIES["filled"]["table"])
        & (tb_pip["survey_comparability"] == PIP_CATEGORIES["filled"]["survey_comparability"])
        & (tb_pip["decile"].isna())
    ].reset_index(drop=True)

    # Merge both tables
    tb_pip = pr.merge(
        tb_pip_survey[["country", "year", "gini", "mean"]],
        tb_pip_filled[["country", "year", "mean"]],  # There is no gini in filled data
        on=["country", "year"],
        how="outer",
        suffixes=("_survey", "_filled"),
    )

    return tb_pip


def create_ginis_from_thousand_bins_distribution(tb_thousand_bins: Table, tb_pip: Table) -> Table:
    """
    Create Gini coefficients from the 1000-binned income distribution data and merge with World Bank PIP data.

    The Gini coefficient is calculated using the trapezoidal approximation of the Lorenz curve:
    Gini = 1 - sum((cumulative_income_share[i] + cumulative_income_share[i-1]) * pop_share[i])

    Where:
    - cumulative_income_share is the cumulative share of total income at each quantile
    - pop_share is the population share for each quantile (1/1000 for each bin)

    Args:
        tb_thousand_bins: Table with 1000 quantiles per country-year containing 'avg' (income) and 'pop' (population)
        tb_pip: Table with World Bank PIP Gini data to merge with

    Returns:
        Table with Gini coefficients calculated from thousand bins distribution, merged with PIP data for comparison
    """

    # Create a copy to avoid modifying the original
    tb = tb_thousand_bins.copy()

    # Keep only necessary columns
    tb = tb[["country", "year", "quantile", "avg", "pop"]].copy()

    # Sort by country, year, and quantile to ensure correct ordering
    tb = tb.sort_values(["country", "year", "quantile"]).reset_index(drop=True)

    # Calculate total income for each quantile (income per capita * population)
    tb["total_income"] = tb["avg"] * tb["pop"]

    # Calculate total income and population by country-year
    country_year_totals = (
        tb.groupby(["country", "year"])
        .agg(total_income_sum=("total_income", "sum"), total_pop_sum=("pop", "sum"))
        .reset_index()
    )

    # Merge totals back to main table
    tb = pr.merge(tb, country_year_totals, on=["country", "year"], how="left")

    # Calculate cumulative income share for each quantile
    tb["cumulative_income"] = tb.groupby(["country", "year"])["total_income"].cumsum()
    tb["cumulative_income_share"] = tb["cumulative_income"] / tb["total_income_sum"]

    # Calculate population share (should be uniform 1/1000 for each quantile, but we calculate it to handle edge cases)
    tb["pop_share"] = tb["pop"] / tb["total_pop_sum"]

    # Calculate Gini using trapezoidal rule
    # Gini = 1 - sum of areas under Lorenz curve
    # For each segment: area = (y[i] + y[i-1]) / 2 * (x[i] - x[i-1])
    # Where y is cumulative income share and x is cumulative population share

    # Get previous cumulative income share (for trapezoidal calculation)
    tb["cumulative_income_share_prev"] = tb.groupby(["country", "year"])["cumulative_income_share"].shift(1).fillna(0)

    # Calculate area under Lorenz curve for each segment
    # Area = average of two heights * width
    tb["lorenz_area"] = (tb["cumulative_income_share"] + tb["cumulative_income_share_prev"]) * tb["pop_share"] / 2

    # Sum areas by country-year to get total area under Lorenz curve
    gini_calc = tb.groupby(["country", "year"]).agg(lorenz_area_total=("lorenz_area", "sum")).reset_index()

    # Gini coefficient = 1 - 2 * (area under Lorenz curve)
    # The factor of 2 comes from the fact that the area under the perfect equality line is 0.5
    gini_calc["gini"] = 1 - 2 * gini_calc["lorenz_area_total"]

    # Drop intermediate calculation column
    gini_calc = gini_calc.drop(columns=["lorenz_area_total"])

    # Sanity check: Gini should be between 0 and 1
    invalid_gini = gini_calc[(gini_calc["gini"] < 0) | (gini_calc["gini"] > 1)]
    if len(invalid_gini) > 0:
        log.warning(
            f"create_ginis_from_thousand_bins_distribution: Found {len(invalid_gini)} country-years with invalid Gini coefficients (outside [0,1] range)"
        )
        # Clip to valid range
        gini_calc["gini"] = gini_calc["gini"].clip(0, 1)

    # Merge with PIP data for comparison
    tb_pip = pr.merge(
        tb_pip,
        gini_calc[["country", "year", "gini"]],
        on=["country", "year"],
        how="outer",
        suffixes=("_survey", "_filled"),
    )

    # Calculate difference between thousand bins Gini and PIP Gini
    tb_pip["gini_difference"] = tb_pip["gini_filled"] - tb_pip["gini_survey"]
    tb_pip["gini_difference_pct"] = (tb_pip["gini_difference"] / tb_pip["gini_survey"]) * 100

    # Log summary statistics
    if SHOW_WARNINGS:
        comparison_valid = tb_pip.dropna(subset=["gini_filled", "gini_survey"])
        if len(comparison_valid) > 0:
            mean_diff = comparison_valid["gini_difference"].abs().mean()
            max_diff = comparison_valid["gini_difference"].abs().max()
            log.info(
                f"create_ginis_from_thousand_bins_distribution: Comparison with PIP data - "
                f"Mean absolute difference: {mean_diff:.4f}, Max absolute difference: {max_diff:.4f}"
            )

    # Keep relevant columns for output
    tb_pip = tb_pip[["country", "year", "mean_survey", "mean_filled", "gini_survey", "gini_filled"]]

    return tb_pip


def add_ginis_from_van_zanden(tb_pip: Table, tb_van_zanden: Table) -> Table:
    """
    Add Gini coefficients from Van Zanden et al. (2014) to the PIP data table for historical comparison.
    """

    # Merge tb_pip with tb_van_zanden on country and year
    tb = pr.merge(
        tb_pip,
        tb_van_zanden[["country", "year", "gini"]],
        on=["country", "year"],
        how="outer",
    )

    # Rename gini column to gini_van_zanden
    tb = tb.rename(columns={"gini": "gini_van_zanden"}, errors="raise")

    return tb


def compare_countries_available_in_two_tables(
    tb_1: Table, tb_2: Table, name_tb_1: str, name_tb_2: str
) -> Tuple[Set[str], Set[str]]:
    """
    Compare countries available in two tables and log warnings if there are discrepancies (if SHOW_WARNINGS is True).
    Returns two sets: countries missing in tb_2 compared to tb_1, and countries missing in tb_1 compared to tb_2.
    """
    countries_tb_1 = set(tb_1["country"].unique())
    countries_tb_2 = set(tb_2["country"].unique())

    missing_in_tb_2 = countries_tb_1 - countries_tb_2
    missing_in_tb_1 = countries_tb_2 - countries_tb_1

    if SHOW_WARNINGS:
        if len(missing_in_tb_2) > 0:
            sorted_missing = ", ".join(sorted(missing_in_tb_2))
            log.warning(
                f"The following {len(missing_in_tb_2)} countries are in '{name_tb_1}' but missing in '{name_tb_2}': "
                f"{sorted_missing}"
            )
            calculate_population_of_missing_countries(missing_in_tb_2)

        if len(missing_in_tb_1) > 0:
            sorted_missing = ", ".join(sorted(missing_in_tb_1))
            log.warning(
                f"The following {len(missing_in_tb_1)} countries are in '{name_tb_2}' but missing in '{name_tb_1}': "
                f"{sorted_missing}"
            )

    return missing_in_tb_1, missing_in_tb_2


def prepare_mean_gini_data(tb_gini_mean: Table, tb_gdp: Table) -> Table:
    """
    Prepare mean income and Gini coefficient data for extrapolation.
    It consolidates mean and Gini columns based on priority rules, interpolates missing values, and extrapolates means using GDP growth factors.
    """
    tb_gini_mean = tb_gini_mean.copy()
    tb_gdp = tb_gdp.copy()

    # Check countries missing in either table
    missing_in_gini_mean, missing_in_gdp = compare_countries_available_in_two_tables(
        tb_1=tb_gini_mean,
        tb_2=tb_gdp,
        name_tb_1="gini_mean",
        name_tb_2="gdp",
    )

    # Filter tb_gini_mean to drop missing_in_gini_mean and missing_in_gdp
    tb_gini_mean = tb_gini_mean[
        ~tb_gini_mean["country"].isin(missing_in_gdp) & ~tb_gini_mean["country"].isin(missing_in_gini_mean)
    ].reset_index(drop=True)

    # Filter tb_gini_mean to show data from EARLIEST_YEAR only
    tb_gini_mean = tb_gini_mean[tb_gini_mean["year"] >= EARLIEST_YEAR].reset_index(drop=True)

    # Filter tb_gdp to show data from EARLIEST_YEAR to LATEST_YEAR_PIP_FILLED only
    tb_gdp = tb_gdp[(tb_gdp["year"] >= EARLIEST_YEAR) & (tb_gdp["year"] <= LATEST_YEAR_PIP_FILLED)].reset_index(
        drop=True
    )

    # Generate mean column using priority: survey > filled
    tb_gini_mean[["mean", "mean_origin"]] = tb_gini_mean.apply(select_mean, axis=1)
    tb_gini_mean["mean"] = tb_gini_mean["mean"].astype("Float64")

    # Generate gini column using priority: survey > filled > van_zanden
    tb_gini_mean[["gini", "gini_origin"]] = tb_gini_mean.apply(select_gini, axis=1)
    tb_gini_mean["gini"] = tb_gini_mean["gini"].astype("Float64")

    # Keep only relevant columns
    tb_gini_mean = tb_gini_mean[["country", "year", "mean", "gini"]]

    # Separate mean and gini tables, to interpolate differently
    tb_gini = tb_gini_mean[["country", "year", "gini"]].copy()
    tb_mean = tb_gini_mean[["country", "year", "mean"]].copy()

    # Also create a table with ginis to only extrapolate (repeat values from min year to earliest value)
    tb_gini_outside_extrapolation = tb_gini_mean[["country", "year", "gini"]]

    # Interpolate mean and gini separately

    tb_gini = interpolate_table(
        tb_gini,
        entity_col="country",
        time_col="year",
        time_mode="full_range",  # Interpolate for the full range of years
        method="linear",
        limit_direction="both",
        limit_area=None,  # Interpolate/extrapolate everywhere, including outside existing data ranges (repeating first/last known value)
    )

    tb_mean = interpolate_table(
        tb_mean,
        entity_col="country",
        time_col="year",
        time_mode="full_range",  # Interpolate for the full range of years
        method="linear",
        limit_direction="both",
        limit_area="inside",  # Only interpolate inside existing data ranges
    )

    # Also interpolate tb_gini_outside_extrapolation, to replicate values from 1820 to earliest observation
    tb_gini_outside_extrapolation = interpolate_table(
        tb_gini_outside_extrapolation,
        entity_col="country",
        time_col="year",
        time_mode="full_range",  # Interpolate for the full range of years
        method="linear",
        limit_direction="both",
        limit_area="outside",  # Only extrapolate outside existing data ranges
    )

    # Merge back both tables
    tb_gini_mean_interpolated = pr.merge(tb_mean, tb_gini, on=["country", "year"], how="outer")

    # Add original columns back
    tb_gini_mean = pr.merge(
        tb_gini_mean_interpolated,
        tb_gini_mean[["country", "year", "mean"]],
        on=["country", "year"],
        how="left",
        suffixes=("", "_original"),
    )

    # Add "original" gini (original data + extrapolation repeating values up to 1820)
    tb_gini_mean = pr.merge(
        tb_gini_mean, tb_gini_outside_extrapolation, on=["country", "year"], how="left", suffixes=("", "_original")
    )

    # Separate data in two parts: before (or equal to) and after LATEST_YEAR_PIP_FILLED
    tb_before_pip = tb_gini_mean[tb_gini_mean["year"] <= LATEST_YEAR_PIP_FILLED].reset_index(drop=True)
    tb_after_pip = tb_gini_mean[tb_gini_mean["year"] > LATEST_YEAR_PIP_FILLED].reset_index(drop=True)

    # Calculate growth factors for mean in tb_before_pip
    tb_before_pip = tb_before_pip.sort_values(["country", "year"])
    tb_before_pip["growth_factor"] = tb_before_pip.groupby("country")["mean"].transform(lambda x: x / x.shift(1))

    # From tb_gdp, sort values and shift growth_factor to align with calculation in mean
    tb_gdp = tb_gdp.sort_values(["country", "year"])
    tb_gdp["growth_factor"] = tb_gdp.groupby("country")["growth_factor"].shift(1)

    # Merge both tables
    tb_before_pip = pr.merge(
        tb_before_pip,
        tb_gdp[["country", "year", "growth_factor"]],
        on=["country", "year"],
        how="left",
        suffixes=("_mean", "_gdp"),
    )

    # Select growth factor for mean using priority: original > gdp
    tb_before_pip[["growth_factor", "growth_factor_origin"]] = tb_before_pip.apply(
        select_growth_factor_for_mean, axis=1
    )

    # Shift growth_factor down by one year to align with the starting year of extrapolation
    tb_before_pip["growth_factor"] = tb_before_pip.groupby("country")["growth_factor"].shift(-1)
    tb_before_pip["growth_factor_origin"] = tb_before_pip.groupby("country")["growth_factor_origin"].shift(-1)

    # Calculate the cumulative growth factor product from LATEST_YEAR_PIP_FILLED to each year
    # Make growth_factor Float64 to avoid issues with cumprod
    tb_before_pip["growth_factor"] = tb_before_pip["growth_factor"].astype("Float64")

    # For years before 1981: Calculate cumulative growth factor going backwards from 1981
    # Sort by country and year (descending) and apply cumprod
    tb_before_pip = tb_before_pip.sort_values(["country", "year"], ascending=[True, False])
    tb_before_pip["cumulative_growth_factor"] = tb_before_pip.groupby("country")["growth_factor"].cumprod()

    # Sort values back to original order
    tb_before_pip = tb_before_pip.sort_values(["country", "year"]).reset_index(drop=True)

    # For each country, add mean_reference, which is the mean at LATEST_YEAR_PIP_FILLED
    tb_before_pip = pr.merge(
        tb_before_pip,
        tb_before_pip[tb_before_pip["year"] == LATEST_YEAR_PIP_FILLED][["country", "mean"]].rename(
            columns={"mean": "mean_reference"}
        ),
        on="country",
        how="left",
    )

    # Extrapolate mean backwards using mean_reference and cumulative_growth_factor
    tb_before_pip["mean"] = tb_before_pip["mean_reference"] / tb_before_pip["cumulative_growth_factor"]

    # As cumulative_growth_factor can be NaN for the last year (if no growth factor is available), fill mean with mean_reference in that case (only if year == LATEST_YEAR_PIP_FILLED)
    tb_before_pip.loc[tb_before_pip["year"] == LATEST_YEAR_PIP_FILLED, "mean"] = tb_before_pip["mean_reference"]

    # Concatenate back together
    tb_gini_mean = pd.concat([tb_before_pip, tb_after_pip], ignore_index=True)
    tb_gini_mean = tb_gini_mean.sort_values(["country", "year"]).reset_index(drop=True)

    # Keep only relevant columns
    tb_gini_mean = tb_gini_mean[["country", "year", "mean", "gini", "mean_original", "gini_original"]]

    # Check if there are any remaining NaN values in mean or gini
    remaining_nans = tb_gini_mean[tb_gini_mean["mean"].isna() | tb_gini_mean["gini"].isna()]
    assert (
        len(remaining_nans) == 0
    ), f"prepare_mean_gini_data: There are {len(remaining_nans)} remaining NaN values in mean or gini after interpolation and extrapolation."
    f'{remaining_nans[["country", "year", "mean", "gini"]]}'

    return tb_gini_mean


def select_mean(row):
    """
    Select mean on priority: survey > filled.
    This way, we have the longest country-specific mean series possible.
    """
    if not pd.isna(row["mean_survey"]):
        return pd.Series({"mean": row["mean_survey"], "mean_origin": "survey"})
    else:
        return pd.Series({"mean": row["mean_filled"], "mean_origin": "filled"})


def select_gini(row):
    """
    Select Gini on priority: survey > filled > van_zanden.
    This way, we have the longest country-specific Gini series possible.
    """
    if not pd.isna(row["gini_survey"]):
        return pd.Series({"gini": row["gini_survey"], "gini_origin": "survey"})
    elif not pd.isna(row["gini_filled"]):
        return pd.Series({"gini": row["gini_filled"], "gini_origin": "filled"})
    else:
        return pd.Series({"gini": row["gini_van_zanden"], "gini_origin": "van_zanden"})


def select_growth_factor_for_mean(row):
    """
    Select growth factor for mean on priority: original > gdp.
    This way, we have the longest country-specific mean growth series possible.
    """
    if not pd.isna(row["growth_factor_mean"]):
        return pd.Series({"growth_factor": row["growth_factor_mean"], "growth_factor_origin": "mean"})
    else:
        return pd.Series({"growth_factor": row["growth_factor_gdp"], "growth_factor_origin": "gdp"})


def expand_means_and_ginis_to_thousand_bins(
    tb_gini_mean: Table, tb_thousand_bins: Table, mean_column: str, gini_column: str
) -> Table:
    """
    Expand mean and Gini data to a 1000-binned income distribution table.
    This is done by assuming a log-normal distribution for income within each country-year and using the mean and Gini to parameterize the distribution.

    For a log-normal distribution:
    - Gini = 2 * Φ(σ/√2) - 1, where Φ is the standard normal CDF and σ is the std dev of log(income)
    - Mean income relates to the log-normal parameters through: mean = exp(μ + σ²/2)

    Returns:
        Table with columns: country, year, quantile (1-1000), avg (income), pop (population)
    """

    if KEEP_ORIGINAL_THOUSAND_BINS:
        # Filter tb_gini_mean to only country-years not in tb_thousand_bins
        existing_country_years = set(tb_thousand_bins[["country", "year"]].drop_duplicates().apply(tuple, axis=1))
        tb_new = tb_gini_mean[
            ~tb_gini_mean[["country", "year"]].apply(tuple, axis=1).isin(existing_country_years)
        ].copy()
    else:
        tb_new = tb_gini_mean.copy()

    # Drop rows with missing mean or gini
    tb_new = tb_new.dropna(subset=[mean_column, gini_column]).reset_index(drop=True)

    if len(tb_new) == 0:
        # No new country-years to add, return original tb_thousand_bins
        return tb_thousand_bins

    # Calculate sigma for each row
    tb_new["sigma"] = tb_new[gini_column].apply(gini_to_sigma)

    # Drop rows where sigma couldn't be calculated
    tb_new = tb_new.dropna(subset=["sigma"]).reset_index(drop=True)

    if len(tb_new) == 0:
        return tb_thousand_bins

    # Calculate mu parameter: mean = exp(μ + σ²/2), so μ = log(mean) - σ²/2
    tb_new["mu"] = np.log(tb_new[mean_column]) - (tb_new["sigma"] ** 2) / 2

    # Create expanded table with 1000 quantiles per country-year
    expanded_rows = []

    for _, row in tb_new.iterrows():
        country = row["country"]
        year = row["year"]
        mu = row["mu"]
        sigma = row["sigma"]

        # Generate 1000 quantiles
        quantiles = np.arange(1, 1001)

        # For each quantile, calculate the income level
        # Use percentile points of the log-normal distribution
        percentiles = (quantiles - 0.5) / 1000  # Midpoint of each bin

        # Income at each percentile from log-normal distribution
        incomes = stats.lognorm.ppf(percentiles, s=sigma, scale=np.exp(mu))

        for quantile, income in zip(quantiles, incomes):
            expanded_rows.append(
                {
                    "country": country,
                    "year": year,
                    "quantile": quantile,
                    "avg": income,
                }
            )

    # Create expanded table
    tb_expanded = Table(pd.DataFrame(expanded_rows))

    # Add population
    tb_expanded = paths.regions.add_population(
        tb=tb_expanded,
        population_col="pop",
        warn_on_missing_countries=True,
        interpolate_missing_population=True,
        expected_countries_without_population=COUNTRIES_WITHOUT_POPULATION,
    )

    # Divide population equally among 1000 quantiles
    tb_expanded["pop"] /= 1000

    # Add region and region_old columns, from tb_thousand_bins
    # Create a mapping of country to region and region_old
    country_region_map = tb_thousand_bins[["country", "region", "region_old"]].drop_duplicates()
    tb_expanded = pr.merge(
        tb_expanded,
        country_region_map,
        on="country",
        how="left",
    )

    # Concatenate with original thousand_bins
    tb_thousand_bins_from_mean_gini = pr.concat([tb_thousand_bins, tb_expanded], ignore_index=True)
    tb_thousand_bins_from_mean_gini = tb_thousand_bins_from_mean_gini.sort_values(
        ["country", "year", "quantile"]
    ).reset_index(drop=True)

    return tb_thousand_bins_from_mean_gini


def gini_to_sigma(gini):
    """
    Convert Gini coefficient to sigma parameter of log-normal distribution.
    """
    if gini <= 0 or gini >= 1:
        return np.nan
    try:
        # Gini = 2 * Φ(σ/√2) - 1
        # Solve for σ: Φ(σ/√2) = (Gini + 1) / 2
        target_cdf = (gini + 1) / 2
        # Use inverse CDF to find σ/√2
        z_value = stats.norm.ppf(target_cdf)
        sigma = z_value * np.sqrt(2)
        return sigma
    except Exception:
        return np.nan


def interpolate_quantiles_in_thousand_bins(tb_thousand_bins_from_interpolated_mean: Table) -> Table:
    """
    Interpolate missing values in the 1000-binned income distribution table.
    This function interpolates missing 'avg' values for each country-year across quantiles.

    We do this to complete country series where Gini is not available for all years, but mean is.
    """

    tb_thousand_bins_from_interpolated_mean = tb_thousand_bins_from_interpolated_mean.copy()

    # Separate PIP-based data from extrapolated data
    # I am keeping LATEST_YEAR in both tables so I can interpolate propertly. Then I will reinstate the original bins
    tb_thousand_bins = tb_thousand_bins_from_interpolated_mean[
        tb_thousand_bins_from_interpolated_mean["year"] >= LATEST_YEAR
    ].reset_index(drop=True)

    tb_expanded = tb_thousand_bins_from_interpolated_mean[
        tb_thousand_bins_from_interpolated_mean["year"] <= LATEST_YEAR
    ].reset_index(drop=True)

    # Also have one table with data before LATEST_YEAR_PIP_FILLED so we can drop countries without data
    tb_expanded_before_pip_filled = tb_expanded[tb_expanded["year"] < LATEST_YEAR_PIP_FILLED].reset_index(drop=True)

    # Find missing countries in tb_expanded_before_pip_filled
    missing_countries_in_thousand_bins, missing_countries_in_pip_filled = compare_countries_available_in_two_tables(
        tb_1=tb_thousand_bins,
        tb_2=tb_expanded_before_pip_filled,
        name_tb_1="thousand_bins",
        name_tb_2="extrapolated_before_pip_filled",
    )

    # Remove countries missing in pip_filled from tb_expanded
    tb_expanded = tb_expanded[~tb_expanded["country"].isin(missing_countries_in_pip_filled)].reset_index(drop=True)

    # Drop pop column
    tb_expanded = tb_expanded.drop(columns=["pop"])

    # Make table wide
    tb_expanded = tb_expanded.pivot_table(index=["country", "year"], columns="quantile", values="avg").reset_index()

    # Interpolate missing values across quantiles for each country-year
    tb_expanded = interpolate_table(
        tb_expanded,
        entity_col="country",
        time_col="year",
        time_mode="full_range",  # All the years between min and max year of the table for each country
        method="linear",
        limit_direction="both",
        limit_area="inside",
    )

    # Make the table long again
    tb_expanded = tb_expanded.melt(id_vars=["country", "year"], var_name="quantile", value_name="avg")

    # Add population
    tb_expanded = paths.regions.add_population(
        tb=tb_expanded,
        population_col="pop",
        warn_on_missing_countries=True,
        interpolate_missing_population=True,
        expected_countries_without_population=COUNTRIES_WITHOUT_POPULATION,
    )

    # Divide population equally among 1000 quantiles
    tb_expanded["pop"] /= 1000

    # Drop data for the year LATEST_YEAR from tb_expanded to reinstate original bins
    tb_expanded = tb_expanded[tb_expanded["year"] < LATEST_YEAR].reset_index(drop=True)

    # Concatenate both tables back together
    tb = pr.concat([tb_thousand_bins, tb_expanded], ignore_index=True)

    # Sort by country, year, and quantile
    tb = tb.sort_values(["country", "year", "quantile"]).reset_index(drop=True)

    return tb
