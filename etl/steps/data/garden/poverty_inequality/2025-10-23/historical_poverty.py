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

from typing import Tuple

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.data_helpers.misc import interpolate_table
from etl.helpers import PathFinder

# Initialize logger
log = get_logger()

# Get paths and naming conventions for current step
paths = PathFinder(__file__)

# Poverty lines (daily income in 2021 PPP$)
POVERTY_LINES = [3, 10, 30]

# Earliest year for extrapolation
EARLIEST_YEAR = 1820

# Latest year for extrapolation
LATEST_YEAR = 1990

# Show warnings
SHOW_WARNINGS = False

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

# Historical entities mapping
HISTORICAL_ENTITIES = {
    "USSR": {
        "successor_states": [
            "Russia",
            "Ukraine",
            "Belarus",
            "Uzbekistan",
            "Kazakhstan",
            "Georgia",
            "Azerbaijan",
            "Lithuania",
            "Moldova",
            "Latvia",
            "Kyrgyzstan",
            "Tajikistan",
            "Armenia",
            "Turkmenistan",
            "Estonia",
        ],
        "end_year": 1991,
    },
    "Yugoslavia": {
        "successor_states": [
            "Serbia",
            "Croatia",
            "Slovenia",
            "Bosnia and Herzegovina",
            "North Macedonia",
            "Montenegro",
            "Kosovo",
        ],
        "end_year": 1992,
        "region": "Eastern Europe",
    },
    "Czechoslovakia": {"successor_states": ["Czechia", "Slovakia"], "end_year": 1992},
    "Former Sudan": {"successor_states": ["Sudan", "South Sudan"], "end_year": 2011, "region": "Sub Saharan Africa"},
}


def run() -> None:
    #
    # Load inputs
    #
    ds_thousand_bins = paths.load_dataset("thousand_bins_distribution")
    ds_maddison = paths.load_dataset("maddison_project_database")
    ds_population = paths.load_dataset("population")

    tb_thousand_bins = ds_thousand_bins["thousand_bins_distribution"].reset_index()
    tb_maddison = ds_maddison["maddison_project_database"].reset_index()

    # Prepare GDP data
    tb_gdp = prepare_gdp_data(tb_maddison)

    # Perform backward extrapolation
    tb_extended = extrapolate_backwards(tb_thousand_bins=tb_thousand_bins, tb_gdp=tb_gdp, ds_population=ds_population)

    # Calculate poverty measures
    tb, tb_population = calculate_poverty_measures(tb=tb_extended, ds_population=ds_population)

    # # Data quality checks
    # run_data_quality_checks(tb)

    tb = tb.format(["country", "year", "poverty_line"], short_name="historical_poverty")
    tb_population = tb_population.format(["country", "year"], short_name="population")
    tb_extended = tb_extended.format(
        ["country", "year", "region", "region_old", "quantile"], short_name="historical_income_distribution"
    )

    # Create dataset
    ds_garden = paths.create_dataset(
        tables=[tb, tb_population], check_variables_metadata=True, default_metadata=ds_thousand_bins.metadata
    )

    # Save dataset
    ds_garden.save()


def prepare_gdp_data(tb_maddison: Table) -> Table:
    """
    Prepare GDP per capita data for extrapolation, creating growth factors from country, historical entity, and region levels.
    """

    # Select relevant columns
    tb_gdp = tb_maddison[["country", "year", "gdp_per_capita", "region"]].copy()

    # Restrict data to years EARLIEST_YEAR to LATEST_YEAR
    tb_gdp = tb_gdp[(tb_gdp["year"] >= EARLIEST_YEAR) & (tb_gdp["year"] <= LATEST_YEAR)].reset_index(drop=True)

    # Remove rows with missing GDP per capita
    tb_gdp = tb_gdp.dropna(subset=["gdp_per_capita"])

    # Assert that HISTORICAL_ENTITIES keys and successor states are in tb_gdp
    all_countries = set(tb_gdp["country"].unique())
    for entity_name, entity_data in HISTORICAL_ENTITIES.items():
        if entity_name not in all_countries:
            log.error(f"prepare_gdp_data: Historical entity '{entity_name}' not found in GDP data")
        for successor in entity_data["successor_states"]:
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
                            "region": [entity_data.get("region", pd.NA)],
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
    tb_gdp = interpolate_table(
        tb_gdp,
        entity_col="country",
        time_col="year",
        time_mode="full_range",
        method="linear",
        limit_direction="both",
        limit_area="inside",
    )

    # Restore region information
    tb_gdp["region"] = tb_gdp["country"].map(regions_map)

    # Create year-over-year growth factors for countries
    tb_gdp = create_growth_factor_column(tb=tb_gdp)

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
    for entity_name, entity_data in HISTORICAL_ENTITIES.items():
        tb_entity = tb_gdp[(tb_gdp["country"] == entity_name)].reset_index(drop=True)
        tb_entity["successor_states"] = [list(entity_data["successor_states"]) for _ in range(len(tb_entity))]
        tb_historical_entities.append(tb_entity)

    # Concatenate all historical entities data
    tb_historical_entities = pr.concat(tb_historical_entities, ignore_index=True)

    # Expand successor_states into multiple rows and rename columns
    tb_historical_entities = tb_historical_entities.explode("successor_states").rename(
        columns={"country": "historical_entity", "successor_states": "country"}
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
    tb_gdp = tb_gdp.drop(columns=["country_region"])

    # Merge with historical entities
    tb_gdp = pr.merge(
        tb_gdp,
        tb_historical_entities,
        on=["country", "region", "year"],
        how="outer",
        suffixes=("", "_historical_entity"),
    )

    # Rename growth factor columns
    tb_gdp = tb_gdp.rename(
        columns={"growth_factor": "growth_factor_country"},
    )

    # Generate growth_factor column using priority: country > historical_entity > region
    tb_gdp["growth_factor"] = tb_gdp.apply(select_growth_factor, axis=1)

    # Copy metadata from growth_factor_country to growth_factor
    tb_gdp["growth_factor"] = tb_gdp["growth_factor"].copy_metadata(tb_gdp["growth_factor_country"])

    # Shift growth_factor down by one year to align with the starting year of extrapolation
    tb_gdp["growth_factor"] = tb_gdp.groupby("country")["growth_factor"].shift(-1)

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
            "cumulative_growth_factor",
        ]
    ]

    # Check for extreme growth rates
    if SHOW_WARNINGS:
        extreme_growth = tb_gdp[(tb_gdp["growth_factor"] > 2.0) | (tb_gdp["growth_factor"] < 0.5)]
        if len(extreme_growth) > 0:
            log.warning(
                f"prepare_gdp_data: Found {len(extreme_growth)} instances of extreme growth "
                f"(>100% or <-50% in a single year)"
            )
            # Show some examples
            sample = extreme_growth.head(10)[["country", "year", "growth_factor"]]
            log.warning(f"prepare_gdp_data: Examples:\n{sample}")

    return tb_gdp


def extrapolate_backwards(tb_thousand_bins: Table, tb_gdp: Table, ds_population: Dataset) -> Table:
    """
    Extrapolate income distributions backwards from 1990 to 1820, using the cumulative GDP growth factors in the 1000-binned income distribution data.
    """

    # Create tb_thousand_bins_to_extrapolate
    tb_thousand_bins_to_extrapolate = tb_thousand_bins[tb_thousand_bins["year"] == LATEST_YEAR].reset_index(drop=True)

    # Assert that countries coincide in both tables
    countries_bins = set(tb_thousand_bins_to_extrapolate["country"].unique())
    countries_gdp = set(tb_gdp["country"].unique())
    missing_countries = countries_bins - countries_gdp
    if SHOW_WARNINGS:
        if len(missing_countries) > 0:
            sorted_missing_countries = ", ".join(sorted(missing_countries))
            log.warning(
                f"extrapolate_backwards: The following countries are in thousand_bins but missing in GDP data: "
                f"{sorted_missing_countries}"
            )

    # Filter tb_gdp to only countries present in tb_thousand_bins_to_extrapolate
    tb_gdp = tb_gdp[tb_gdp["country"].isin(countries_bins)].reset_index(drop=True)

    # For tb_thousand_bins_to_extrapolate, column year, assign a list of years from EARLIEST_YEAR to LATEST_YEAR - 1 and then explode
    tb_thousand_bins_to_extrapolate = (
        tb_thousand_bins_to_extrapolate.assign(year=lambda df: [list(range(EARLIEST_YEAR, LATEST_YEAR))] * len(df))
        .explode("year")
        .reset_index(drop=True)
    )

    # Drop pop column, we will add other data on population later
    tb_thousand_bins_to_extrapolate = tb_thousand_bins_to_extrapolate.drop(columns=["pop"])

    # Merge with tb_gdp to get growth factors
    tb_thousand_bins_to_extrapolate = pr.merge(
        tb_thousand_bins_to_extrapolate,
        tb_gdp[["country", "year", "cumulative_growth_factor"]],
        on=["country", "year"],
        how="left",
    )

    # Divide avg income columns by cumulative_growth_factor to extrapolate backwards
    tb_thousand_bins_to_extrapolate["avg"] = (
        tb_thousand_bins_to_extrapolate["avg"] / tb_thousand_bins_to_extrapolate["cumulative_growth_factor"]
    )

    # Add population data for this table
    tb_thousand_bins_to_extrapolate = geo.add_population_to_table(
        tb=tb_thousand_bins_to_extrapolate,
        ds_population=ds_population,
        population_col="pop",
        warn_on_missing_countries=True,
        interpolate_missing_population=True,
    )

    # Divide pop into quantiles (1000 quantiles)
    tb_thousand_bins_to_extrapolate["pop"] /= 1000

    if SHOW_WARNINGS:
        # Check empty values in pop column
        # NOTE: I am checking this because it could be the case that some countries don't have population data for some years
        missing_pop = tb_thousand_bins_to_extrapolate[tb_thousand_bins_to_extrapolate["pop"].isna()]

        # Select only one quantile to review
        missing_pop = missing_pop[missing_pop["quantile"] == 1]

        # Define countries with missing population values
        missing_countries = missing_pop["country"].unique()
        if not missing_pop.empty:
            log.warning(f"extrapolate_backwards: Missing population values for countries:\n{missing_countries}")

    # Drop cumulative_growth_factor column, as it's no longer needed
    tb_thousand_bins_to_extrapolate = tb_thousand_bins_to_extrapolate.drop(columns=["cumulative_growth_factor"])

    # Concatenate with original tb_thousand_bins to get the 1000-binned distribution from EARLIEST_YEAR to present
    tb_thousand_bins = pr.concat([tb_thousand_bins, tb_thousand_bins_to_extrapolate], ignore_index=True)

    # Sort values
    tb_thousand_bins = tb_thousand_bins.sort_values(["country", "year", "quantile"]).reset_index(drop=True)

    return tb_thousand_bins


def apply_backward_extrapolation(
    country: str, baseline_year: int, baseline_quantiles: Table, growth_data: Table, method: str
) -> Table:
    """Apply backward extrapolation to income distribution.

    Parameters
    ----------
    country : str
        Country name
    baseline_year : int
        Starting year
    baseline_quantiles : Table
        Baseline income distribution (1000 quantiles)
    growth_data : Table
        GDP growth factors
    method : str
        Extrapolation method used

    Returns
    -------
    Table
        Extended income distribution (EARLIEST_YEAR to baseline_year-1)
    """
    extended_rows = []

    # Sort growth data by year (descending, going backwards)
    growth_data = growth_data.sort_values("year", ascending=False)

    # Initialize with baseline values
    current_income = baseline_quantiles.set_index("quantile")["avg"].to_dict()
    current_pop = baseline_quantiles.set_index("quantile")["pop"].to_dict()

    # Get region if available
    region = baseline_quantiles["region"].iloc[0] if "region" in baseline_quantiles.columns else None

    # Go backwards year by year
    for year in range(baseline_year - 1, EARLIEST_YEAR - 1, -1):
        # Get growth factor for year+1 to year
        growth_row = growth_data[growth_data["year"] == year + 1]

        if len(growth_row) == 0 or pd.isna(growth_row["growth_factor"].iloc[0]):
            # No growth data for this year, skip
            continue

        growth_factor = growth_row["growth_factor"].iloc[0]

        # Apply backward extrapolation to all quantiles
        # income(t-1) = income(t) / growth_factor(t)
        for quantile in current_income.keys():
            current_income[quantile] = current_income[quantile] / growth_factor

            # Floor negative values at 0
            if current_income[quantile] < 0:
                current_income[quantile] = 0
                log.warning(
                    f"apply_backward_extrapolation: {country} year {year} quantile {quantile} - negative income, floored at 0"
                )

        # Create rows for this year
        for quantile in current_income.keys():
            extended_rows.append(
                {
                    "country": country,
                    "year": year,
                    "quantile": quantile,
                    "avg": current_income[quantile],
                    "pop": current_pop[quantile],
                    "region": region,
                    "extrapolation_method": method,
                }
            )

    return Table(extended_rows)


def calculate_poverty_measures(tb: Table, ds_population: Dataset) -> Tuple[Table, Table]:
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
        # Filter rows where avg is less than poverty line
        tb_poverty_line = tb[tb["avg"] < poverty_line].reset_index(drop=True)

        # Get the last row for each year (highest quantile below poverty line)
        tb_poverty_line = tb_poverty_line.groupby("year").tail(1).reset_index(drop=True)

        # Add poverty_line column
        tb_poverty_line["poverty_line"] = poverty_line

        # Append to tb_poverty
        tb_poverty.append(tb_poverty_line)

    # Concatenate all poverty lines
    tb_poverty = pr.concat(tb_poverty, ignore_index=True)

    # Keep relevant columns
    tb_poverty = tb_poverty[["year", "poverty_line", "global_population", "cum_pop", "percentage_global_pop"]]

    # Rename columns
    tb_poverty = tb_poverty.rename(
        columns={"cum_pop": "headcount", "percentage_global_pop": "headcount_ratio", "global_population": "population"}
    )

    # Copy metadata from avg to headcount
    tb_poverty["headcount"] = tb_poverty["headcount"].copy_metadata(tb["avg"])
    tb_poverty["headcount_ratio"] = tb_poverty["headcount_ratio"].copy_metadata(tb["avg"])

    # Add country column
    tb_poverty["country"] = "World"

    # Create stacked variables for stacked area/bar charts
    tb_poverty = create_stacked_variables(tb_poverty)

    # Calculate an alternative method with our population dataset
    # First, add population_omm column, the population of the world from Our World in Data
    tb_poverty = geo.add_population_to_table(
        tb=tb_poverty,
        ds_population=ds_population,
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

    # Drop population columns from tb_poverty
    tb_poverty = tb_poverty.drop(columns=["population", "population_omm"])

    return tb_poverty, tb_population


def select_growth_factor(row):
    """
    Select growth factor based on priority: country > historical_entity > region.
    This way, we have the longest country-specific growth series possible.
    """
    if not pd.isna(row["growth_factor_country"]):
        return row["growth_factor_country"]
    elif not pd.isna(row["growth_factor_historical_entity"]):
        return row["growth_factor_historical_entity"]
    else:
        return row["growth_factor_region"]


def create_growth_factor_column(tb: Table) -> Table:
    """
    Create growth_factor column, dividing GDP values by the value in the previous year.
    """

    tb = tb.sort_values(["country", "year"])

    # Calculate growth factor
    tb["growth_factor"] = tb.groupby("country")["gdp_per_capita"].transform(lambda x: x / x.shift(1))

    return tb


def create_stacked_variables(tb: Table) -> Table:
    """
    Create stacked variables from the indicators to plot them as stacked area/bar charts
    """

    tb = tb.copy()

    # Define headcount_above and headcount_ratio_above variables
    tb["headcount_above"] = tb["population"] - tb["headcount"]
    tb["headcount_ratio_above"] = tb["headcount_above"] / tb["population"]

    # Make headcount_ratio_above a percentage
    tb["headcount_ratio_above"] = tb["headcount_ratio_above"] * 100

    # Define stacked variables as headcount and headcount_ratio between poverty lines
    # Select only the necessary columns
    tb_pivot = tb[
        [
            "country",
            "year",
            "poverty_line",
            "headcount_ratio",
            "headcount",
            "population",
        ]
    ].copy()

    # Pivot
    tb_pivot = pr.pivot(
        data=tb,
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
            tb_pivot[varname_pct] = tb_pivot[varname_n] / tb_pivot[("population", POVERTY_LINES[i])]

            # Multiply by 100 to get percentage
            tb_pivot[varname_pct] = tb_pivot[varname_pct] * 100

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
