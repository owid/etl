"""Generate historical poverty estimates by extrapolating income distributions backwards using GDP growth.

This step combines:
- thousand_bins_distribution: Modern income distribution data (1990+) with 1000 quantiles per country
- maddison_project_database: Historical GDP per capita data (back to 1820)

The approach:
1. Start from the earliest available year in thousand_bins (typically 1990)
2. Use Maddison GDP growth rates to extrapolate income distributions backwards to 1820
3. Calculate poverty headcounts for $3, $10, and $30 per day poverty lines
4. Generate estimates for individual countries and regional aggregates
"""

from typing import Dict, Optional

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
    },
    "Czechoslovakia": {"successor_states": ["Czechia", "Slovakia"], "end_year": 1992},
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
    tb = calculate_poverty_measures(tb=tb_extended)

    # # Data quality checks
    # run_data_quality_checks(tb)

    tb = tb.format(["country", "year", "poverty_line"], short_name="historical_poverty")

    # Create dataset
    ds_garden = paths.create_dataset(
        tables=[tb], check_variables_metadata=True, default_metadata=ds_thousand_bins.metadata
    )

    # Save dataset
    ds_garden.save()


def prepare_gdp_data(tb_maddison: Table) -> Table:
    """Prepare GDP per capita data for extrapolation.

    Steps:
    1. Fill missing years using interpolation (geometric mean growth)
    2. Calculate year-over-year growth factors

    Parameters
    ----------
    tb_maddison : Table
        Maddison Project Database with gdp_per_capita

    Returns
    -------
    Table
        GDP data with interpolated values and growth factors
    """
    # Select relevant columns
    tb_gdp = tb_maddison[["country", "year", "gdp_per_capita", "region"]].copy()

    # Restrict data to years EARLIEST_YEAR to LATEST_YEAR
    tb_gdp = tb_gdp[(tb_gdp["year"] >= EARLIEST_YEAR) & (tb_gdp["year"] <= LATEST_YEAR)].reset_index(drop=True)

    # Remove rows with missing GDP per capita
    tb_gdp = tb_gdp.dropna(subset=["gdp_per_capita"])

    # Store region information separately (categorical column can't be interpolated)
    regions_map = tb_gdp.groupby("country")["region"].first().to_dict()

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

    # Merge with tb_maddison to show original vs interpolated values
    tb_gdp = pr.merge(
        tb_gdp,
        tb_maddison[["country", "year", "gdp_per_capita"]],
        on=["country", "year"],
        how="left",
        suffixes=("", "_original"),
    )

    # Restore region information
    tb_gdp["region"] = tb_gdp["country"].map(regions_map)

    # Create year-over-year growth factors for countries
    tb_gdp = create_growth_factor_column(tb=tb_gdp)

    # Remove (Maddison) from country column if present
    tb_gdp["country"] = tb_gdp["country"].str.replace(" (Maddison)", "", regex=False)

    # Create tb_gdp_regions table, selecting only regions, which are the countries including (Maddison) and World
    region_entities = pd.Index(list(regions_map.values()) + ["World"]).dropna().unique().tolist()
    tb_gdp_regions = tb_gdp[tb_gdp["country"].isin(region_entities)].reset_index(drop=True)

    # Assert that HISTORICAL_ENTITIES keys and successor states are in tb_gdp
    all_countries = set(tb_gdp["country"].unique())
    for entity_name, entity_data in HISTORICAL_ENTITIES.items():
        if entity_name not in all_countries:
            log.warning(f"prepare_gdp_data: Historical entity '{entity_name}' not found in GDP data")
        for successor in entity_data["successor_states"]:
            if successor not in all_countries:
                log.warning(f"prepare_gdp_data: Successor state '{successor}' of '{entity_name}' not found in GDP data")

    # Create a historical entities table
    tb_historical_entities = []
    for entity_name, entity_data in HISTORICAL_ENTITIES.items():
        tb_entity = tb_gdp[(tb_gdp["country"] == entity_name)].reset_index(drop=True)
        tb_entity["successor_states"] = [list(entity_data["successor_states"]) for _ in range(len(tb_entity))]
        tb_historical_entities.append(tb_entity)

    # Concatenate all historical entities data
    tb_historical_entities = pr.concat(tb_historical_entities, ignore_index=True)

    # Rename country to historical_entity
    tb_historical_entities = tb_historical_entities.rename(columns={"country": "historical_entity"})
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

    # Shift growth_factor down by one year to align with the starting year of extrapolation
    tb_gdp["growth_factor"] = tb_gdp.groupby("country")["growth_factor"].shift(-1)

    # Calculate the the cumulative growth factor product from LATEST_YEAR to each year
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

    # Show country, year, region, and historical_entity columns first.
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

    # # Check for extreme growth rates
    # extreme_growth = tb_gdp[(tb_gdp["growth_factor"] > 2.0) | (tb_gdp["growth_factor"] < 0.5)]
    # if len(extreme_growth) > 0:
    #     log.warning(
    #         f"prepare_gdp_data: Found {len(extreme_growth)} instances of extreme growth "
    #         f"(>100% or <-50% in a single year)"
    #     )
    #     # Show some examples
    #     sample = extreme_growth.head(10)[["country", "year", "gdp_per_capita", "growth_factor"]]
    #     log.warning(f"prepare_gdp_data: Examples:\n{sample}")

    return tb_gdp


def extrapolate_backwards(tb_thousand_bins: Table, tb_gdp: Table, ds_population: Dataset) -> Table:
    """Extrapolate income distributions backwards from 1990 to 1820.

    Uses a 3-tier fallback strategy:
    1. Direct country GDP growth
    2. Historical entity GDP growth (USSR, Yugoslavia, Czechoslovakia)
    3. Regional GDP growth

    Parameters
    ----------
    tb_bins : Table
        Thousand bins distribution data (1990+)
    tb_gdp : Table
        Prepared GDP data with growth factors

    Returns
    -------
    Table
        Extended table with historical estimates (1820-present)
    """

    # Create tb_thousand_bins_to_extrapolate
    tb_thousand_bins_to_extrapolate = tb_thousand_bins[tb_thousand_bins["year"] == LATEST_YEAR].reset_index(drop=True)

    # Assert that countries coincide in both tables
    countries_bins = set(tb_thousand_bins_to_extrapolate["country"].unique())
    countries_gdp = set(tb_gdp["country"].unique())
    missing_countries = countries_bins - countries_gdp
    if len(missing_countries) > 0:
        log.warning(
            f"extrapolate_backwards: The following countries are in thousand_bins but missing in GDP data: "
            f"{missing_countries}"
        )

    # Filter tb_gdp to only countries present in tb_thousand_bins_to_extrapolate
    tb_gdp = tb_gdp[tb_gdp["country"].isin(countries_bins)].reset_index(drop=True)

    # For tb_thousand_bins_to_extrapolate, column year, assign a list of years from EARLIEST_YEAR to LATEST_YEAR - 1 and then explode
    tb_thousand_bins_to_extrapolate = (
        tb_thousand_bins_to_extrapolate.assign(year=lambda df: [list(range(EARLIEST_YEAR, LATEST_YEAR))] * len(df))
        .explode("year")
        .reset_index(drop=True)
    )

    # Drop pop column
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

    # Drop cumulative_growth_factor column
    tb_thousand_bins_to_extrapolate = tb_thousand_bins_to_extrapolate.drop(columns=["cumulative_growth_factor"])

    # Concatenate with original tb_thousand_bins
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


def calculate_poverty_measures(tb: Table) -> Table:
    """Calculate poverty headcount ratios and counts for all poverty lines.

    Parameters
    ----------
    tb_extended : Table
        Extended income distribution data (1820-present)

    Returns
    -------
    Table
        Poverty measures by country and year
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

    # Add country column
    tb_poverty["country"] = "World"

    return tb_poverty


def run_data_quality_checks(tb: Table) -> None:
    """Run data quality checks on the final dataset.

    Parameters
    ----------
    tb : Table
        Final poverty measures table
    """
    log.info("run_data_quality_checks: Starting data quality checks")

    # Check for negative values
    for poverty_line in POVERTY_LINES:
        ratio_col = f"headcount_ratio_poverty_{poverty_line}_dollars"
        count_col = f"headcount_poverty_{poverty_line}_dollars"

        negative_ratios = tb[tb[ratio_col] < 0]
        if len(negative_ratios) > 0:
            log.error(f"run_data_quality_checks: Found {len(negative_ratios)} negative values in {ratio_col}")

        negative_counts = tb[tb[count_col] < 0]
        if len(negative_counts) > 0:
            log.error(f"run_data_quality_checks: Found {len(negative_counts)} negative values in {count_col}")

    # Check monotonicity: poverty should increase with higher poverty lines
    violations = []
    for _, row in tb.iterrows():
        ratio_3 = row["headcount_ratio_poverty_3_dollars"]
        ratio_10 = row["headcount_ratio_poverty_10_dollars"]
        ratio_30 = row["headcount_ratio_poverty_30_dollars"]

        if not (ratio_3 <= ratio_10 <= ratio_30):
            violations.append(f"{row['country']} ({row['year']})")

    if len(violations) > 0:
        log.warning(f"run_data_quality_checks: Found {len(violations)} monotonicity violations")

    # Check ratios are between 0 and 1
    for poverty_line in POVERTY_LINES:
        ratio_col = f"headcount_ratio_poverty_{poverty_line}_dollars"
        invalid = tb[(tb[ratio_col] < 0) | (tb[ratio_col] > 1)]
        if len(invalid) > 0:
            log.error(f"run_data_quality_checks: Found {len(invalid)} invalid ratios (not in [0,1]) in {ratio_col}")

    log.info("run_data_quality_checks: Data quality checks completed")


def select_growth_factor(row):
    """Select growth factor based on priority: country > historical_entity > region."""
    if not pd.isna(row["growth_factor_country"]):
        return row["growth_factor_country"]
    elif not pd.isna(row["growth_factor_historical_entity"]):
        return row["growth_factor_historical_entity"]
    else:
        return row["growth_factor_region"]


def create_growth_factor_column(tb: Table) -> Table:
    """
    Create growth_factor column, dividing GDP values by the value in LATEST_YEAR.
    """

    tb = tb.sort_values(["country", "year"])

    # Calculate growth factor
    tb["growth_factor"] = tb.groupby("country")["gdp_per_capita"].transform(lambda x: x / x.shift(1))

    return tb
