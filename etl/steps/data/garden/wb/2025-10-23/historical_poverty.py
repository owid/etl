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

from pathlib import Path
from typing import Dict, Optional

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table
from ruamel.yaml import YAML
from structlog import get_logger

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

# Historical entities file
HISTORICAL_ENTITIES_FILE = Path(__file__).parent / "historical_entities.yml"


def run() -> None:
    #
    # Load inputs
    #
    ds_thousand_bins = paths.load_dataset("thousand_bins_distribution")
    ds_maddison = paths.load_dataset("maddison_project_database")

    tb_bins = ds_thousand_bins["thousand_bins_distribution"].reset_index()
    tb_maddison = ds_maddison["maddison_project_database"].reset_index()

    #
    # Prepare GDP data
    #
    tb_gdp = prepare_gdp_data(tb_maddison)

    #
    # Load historical entities mapping
    #
    log.info("historical_poverty: Loading historical entities mapping")
    historical_entities = load_historical_entities()

    #
    # Perform backward extrapolation
    #
    log.info("historical_poverty: Starting backward extrapolation from 1990 to 1820")
    tb_extended = extrapolate_backwards(tb_bins=tb_bins, tb_gdp=tb_gdp, historical_entities=historical_entities)

    log.info(f"historical_poverty: Extended dataset has {len(tb_extended)} rows")
    log.info(f"historical_poverty: Extended date range: {tb_extended['year'].min()}-{tb_extended['year'].max()}")

    #
    # Calculate poverty measures
    #
    log.info("historical_poverty: Calculating poverty measures")
    tb_poverty = calculate_poverty_measures(tb_extended)

    log.info(f"historical_poverty: Poverty table has {len(tb_poverty)} rows")

    #
    # Calculate regional aggregates
    #
    log.info("historical_poverty: Calculating regional aggregates")
    tb_poverty_regional = calculate_regional_poverty(tb_extended, tb_bins)

    log.info(f"historical_poverty: Regional poverty table has {len(tb_poverty_regional)} rows")

    #
    # Combine country and regional data
    #
    log.info("historical_poverty: Combining country and regional data")
    tb_final = pr.concat([tb_poverty, tb_poverty_regional], ignore_index=True)

    #
    # Data quality checks
    #
    log.info("historical_poverty: Running data quality checks")
    run_data_quality_checks(tb_final)

    #
    # Add origins to all poverty indicator columns
    #
    log.info("historical_poverty: Adding origins metadata")
    # Get origins from source tables
    bins_origins = []
    for col in tb_bins.columns:
        if hasattr(tb_bins[col].metadata, "origins") and tb_bins[col].metadata.origins:
            bins_origins.extend(tb_bins[col].metadata.origins)
            break  # Just need one column's origins

    maddison_origins = []
    for col in tb_maddison.columns:
        if hasattr(tb_maddison[col].metadata, "origins") and tb_maddison[col].metadata.origins:
            maddison_origins.extend(tb_maddison[col].metadata.origins)
            break  # Just need one column's origins

    # Combine origins from both source datasets
    combined_origins = bins_origins + maddison_origins

    # Add origins to all poverty indicator columns
    for col in tb_final.columns:
        if col not in ["country", "year", "extrapolation_method"]:
            tb_final[col].metadata.origins = combined_origins

    #
    # Format and save
    #
    log.info(f"historical_poverty: Final table has {len(tb_final)} rows")
    log.info(f"historical_poverty: Countries/regions: {tb_final['country'].nunique()}")

    tb_final = tb_final.format(["country", "year"], short_name="historical_poverty")

    # Create dataset
    ds_garden = paths.create_dataset(
        tables=[tb_final], check_variables_metadata=True, default_metadata=ds_bins.metadata
    )

    # Save dataset
    ds_garden.save()
    log.info("historical_poverty: Execution completed successfully")


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
        time_mode="full_range_entity",  # Fill complete range for each country
        method="linear",
        limit_direction="both",
    )

    # Restore region information
    tb_gdp["region"] = tb_gdp["country"].map(regions_map)

    log.info(f"prepare_gdp_data: After interpolation: {len(tb_gdp)} rows")

    # Calculate year-over-year growth factors
    log.info("prepare_gdp_data: Calculating year-over-year growth factors")
    tb_gdp = tb_gdp.sort_values(["country", "year"])

    # Growth factor = GDP(t) / GDP(t-1)
    tb_gdp["growth_factor"] = tb_gdp.groupby("country")["gdp_per_capita"].transform(lambda x: x / x.shift(1))

    # Check for extreme growth rates
    extreme_growth = tb_gdp[(tb_gdp["growth_factor"] > 2.0) | (tb_gdp["growth_factor"] < 0.5)]
    if len(extreme_growth) > 0:
        log.warning(
            f"prepare_gdp_data: Found {len(extreme_growth)} instances of extreme growth "
            f"(>100% or <-50% in a single year)"
        )
        # Show some examples
        sample = extreme_growth.head(10)[["country", "year", "gdp_per_capita", "growth_factor"]]
        log.warning(f"prepare_gdp_data: Examples:\n{sample}")

    return tb_gdp


def load_historical_entities() -> Dict:
    """Load historical entities mapping from YAML file.

    Returns
    -------
    dict
        Mapping of historical entities to their successor states
    """
    if not HISTORICAL_ENTITIES_FILE.exists():
        log.warning(f"Historical entities file not found: {HISTORICAL_ENTITIES_FILE}")
        return {}

    yaml = YAML(typ="safe")
    with open(HISTORICAL_ENTITIES_FILE, "r") as f:
        entities = yaml.load(f)

    log.info(f"load_historical_entities: Loaded {len(entities.get('historical_entities', {}))} historical entities")
    return entities.get("historical_entities", {})


def extrapolate_backwards(tb_bins: Table, tb_gdp: Table, historical_entities: Dict) -> Table:
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
    historical_entities : dict
        Mapping of historical entities to successor states

    Returns
    -------
    Table
        Extended table with historical estimates (1820-present)
    """
    # Get list of unique countries in bins data
    countries_bins = sorted(tb_bins["country"].unique())
    log.info(f"extrapolate_backwards: Processing {len(countries_bins)} countries")

    # Prepare baseline data (earliest year for each country)
    baseline_data = get_baseline_data(tb_bins)

    # Create list to store extended data
    extended_tables = []

    # Keep original data
    tb_bins_copy = tb_bins.copy()
    tb_bins_copy["extrapolation_method"] = "original"
    extended_tables.append(tb_bins_copy)

    # Track statistics for summary logging
    stats = {"direct": 0, "historical_entity": 0, "regional": 0, "skipped": 0}

    # Process each country
    for country in countries_bins:
        # Get baseline year and data for this country
        baseline_year = baseline_data[country]["year"]
        baseline_quantiles = baseline_data[country]["data"]

        # Try tier 1: Direct country extrapolation
        growth_data = get_country_growth_data(tb_gdp, country, baseline_year)

        if growth_data is not None:
            stats["direct"] += 1
            tb_extended = apply_backward_extrapolation(
                country=country,
                baseline_year=baseline_year,
                baseline_quantiles=baseline_quantiles,
                growth_data=growth_data,
                method="direct",
            )
            extended_tables.append(tb_extended)
            continue

        # Try tier 2: Historical entity
        historical_entity = find_historical_entity(country, historical_entities)
        if historical_entity:
            growth_data = get_country_growth_data(tb_gdp, historical_entity, baseline_year)
            if growth_data is not None:
                stats["historical_entity"] += 1
                tb_extended = apply_backward_extrapolation(
                    country=country,
                    baseline_year=baseline_year,
                    baseline_quantiles=baseline_quantiles,
                    growth_data=growth_data,
                    method="historical_entity",
                )
                extended_tables.append(tb_extended)
                continue

        # Try tier 3: Regional growth
        region = (
            tb_bins[tb_bins["country"] == country]["region"].iloc[0]
            if len(tb_bins[tb_bins["country"] == country]) > 0
            else None
        )
        if region:
            growth_data = get_regional_growth_data(tb_gdp, tb_bins, region, baseline_year)
            if growth_data is not None:
                stats["regional"] += 1
                tb_extended = apply_backward_extrapolation(
                    country=country,
                    baseline_year=baseline_year,
                    baseline_quantiles=baseline_quantiles,
                    growth_data=growth_data,
                    method="regional",
                )
                extended_tables.append(tb_extended)
                continue

        stats["skipped"] += 1
        log.warning(f"extrapolate_backwards: {country} - No growth data available, skipping extrapolation")

    # Log summary statistics
    log.info(
        f"extrapolate_backwards: Summary - Direct: {stats['direct']}, "
        f"Historical entity: {stats['historical_entity']}, "
        f"Regional: {stats['regional']}, "
        f"Skipped: {stats['skipped']}"
    )

    # Combine all extended data
    tb_final = pr.concat(extended_tables, ignore_index=True)

    return tb_final


def get_baseline_data(tb_bins: Table) -> Dict:
    """Get baseline year and data for each country (earliest available year).

    Parameters
    ----------
    tb_bins : Table
        Thousand bins distribution data

    Returns
    -------
    dict
        Dictionary mapping country to baseline year and data
    """
    baseline = {}

    for country in tb_bins["country"].unique():
        country_data = tb_bins[tb_bins["country"] == country].sort_values("year")
        baseline_year = country_data["year"].min()
        baseline_quantiles = country_data[country_data["year"] == baseline_year]

        baseline[country] = {"year": baseline_year, "data": baseline_quantiles}

    return baseline


def get_country_growth_data(tb_gdp: Table, country: str, baseline_year: int) -> Optional[Table]:
    """Get GDP growth data for a specific country.

    Parameters
    ----------
    tb_gdp : Table
        GDP data with growth factors
    country : str
        Country name
    baseline_year : int
        Baseline year (starting point for backward extrapolation)

    Returns
    -------
    Table or None
        GDP growth data from EARLIEST_YEAR to baseline_year, or None if not available
    """
    country_gdp = tb_gdp[tb_gdp["country"] == country].copy()

    if len(country_gdp) == 0:
        return None

    # Filter to years we need (EARLIEST_YEAR to baseline_year)
    country_gdp = country_gdp[
        (country_gdp["year"] >= EARLIEST_YEAR) & (country_gdp["year"] <= baseline_year)
    ].sort_values("year")

    # Check if we have reasonable coverage
    if len(country_gdp) < (baseline_year - EARLIEST_YEAR) * 0.3:  # At least 30% coverage
        return None

    return country_gdp


def get_regional_growth_data(tb_gdp: Table, tb_bins: Table, region: str, baseline_year: int) -> Optional[Table]:
    """Calculate regional aggregate GDP growth.

    Parameters
    ----------
    tb_gdp : Table
        GDP data
    tb_bins : Table
        Bins data (for region mapping)
    region : str
        Region name
    baseline_year : int
        Baseline year

    Returns
    -------
    Table or None
        Regional GDP growth data
    """
    # Get countries in this region from bins data
    countries_in_region = tb_bins[tb_bins["region"] == region]["country"].unique()

    # Get GDP data for these countries
    regional_gdp = tb_gdp[tb_gdp["country"].isin(countries_in_region)].copy()

    if len(regional_gdp) == 0:
        return None

    # Calculate regional aggregate GDP per capita
    # For each year: sum(GDP) / sum(Population)
    # We need population from Maddison (it's in the dataset)
    # For simplicity, we'll use weighted average of GDP per capita by population

    # Group by year and calculate weighted average
    regional_agg = (
        regional_gdp.groupby("year")
        .agg(
            {
                "gdp_per_capita": "mean"  # Simple average for now
            }
        )
        .reset_index()
    )

    regional_agg["country"] = region

    # Calculate growth factors
    regional_agg = regional_agg.sort_values("year")
    regional_agg["growth_factor"] = regional_agg["gdp_per_capita"] / regional_agg["gdp_per_capita"].shift(1)

    # Filter to years we need
    regional_agg = regional_agg[(regional_agg["year"] >= EARLIEST_YEAR) & (regional_agg["year"] <= baseline_year)]

    if len(regional_agg) < (baseline_year - EARLIEST_YEAR) * 0.3:
        return None

    return regional_agg


def find_historical_entity(country: str, historical_entities: Dict) -> Optional[str]:
    """Find if a country corresponds to a historical entity.

    Parameters
    ----------
    country : str
        Country name
    historical_entities : dict
        Mapping of historical entities

    Returns
    -------
    str or None
        Historical entity name if found, None otherwise
    """
    for entity_name, entity_data in historical_entities.items():
        if country in entity_data.get("successor_states", []):
            return entity_name

    return None


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


def calculate_poverty_measures(tb_extended: Table) -> Table:
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
    poverty_rows = []

    # Group by country and year
    for (country, year), group in tb_extended.groupby(["country", "year"]):
        # Sort by income (avg)
        group = group.sort_values("avg")

        # Get total population
        total_pop = group["pop"].sum()

        # Get extrapolation method (should be same for all quantiles in a country-year)
        method = group["extrapolation_method"].iloc[0]

        # Calculate for each poverty line
        poverty_measures = {"country": country, "year": year, "extrapolation_method": method}

        for poverty_line in POVERTY_LINES:
            # Find quantile where income exceeds poverty line
            above_line = group[group["avg"] >= poverty_line]

            if len(above_line) == 0:
                # Everyone is below poverty line
                headcount_ratio = 1.0
                headcount = total_pop
            else:
                # Get first quantile above line
                first_above_quantile = above_line["quantile"].iloc[0]

                # Share in poverty = (quantile_index - 1) / 1000
                headcount_ratio = (first_above_quantile - 1) / 1000
                headcount = headcount_ratio * total_pop

                # For more precision, use linear interpolation if we have the previous quantile
                # Reset index to use positional indexing
                group_reset = group.reset_index(drop=True)
                above_line_reset = group_reset[group_reset["avg"] >= poverty_line]

                if len(above_line_reset) > 0:
                    first_above_pos = above_line_reset.index[0]

                    if first_above_pos > 0:
                        prev_row = group_reset.iloc[first_above_pos - 1]
                        curr_row = above_line_reset.iloc[0]

                        # Linear interpolation between the two points
                        if curr_row["avg"] != prev_row["avg"]:
                            # Fraction = (poverty_line - prev_income) / (curr_income - prev_income)
                            fraction = (poverty_line - prev_row["avg"]) / (curr_row["avg"] - prev_row["avg"])
                            # Adjust headcount ratio
                            prev_quantile = prev_row["quantile"]
                            interpolated_quantile = prev_quantile + fraction * (first_above_quantile - prev_quantile)
                            headcount_ratio = (interpolated_quantile - 1) / 1000
                            headcount = headcount_ratio * total_pop

            # Store results
            poverty_measures[f"headcount_ratio_poverty_{poverty_line}_dollars"] = headcount_ratio
            poverty_measures[f"headcount_poverty_{poverty_line}_dollars"] = headcount

        poverty_rows.append(poverty_measures)

    return Table(poverty_rows)


def calculate_regional_poverty(tb_extended: Table, tb_bins: Table) -> Table:
    """Calculate regional poverty aggregates including World.

    Parameters
    ----------
    tb_extended : Table
        Extended income distribution data
    tb_bins : Table
        Original bins data (for region mapping)

    Returns
    -------
    Table
        Regional poverty measures
    """
    regional_rows = []

    # Get World Bank regions
    wb_regions = tb_bins["region"].unique()

    # Process World Bank regions
    for region in wb_regions:
        # Get countries in this region
        countries_in_region = tb_bins[tb_bins["region"] == region]["country"].unique()

        # Filter extended data to these countries
        regional_data = tb_extended[tb_extended["country"].isin(countries_in_region)]

        if len(regional_data) == 0:
            continue

        # Group by year
        for year, year_group in regional_data.groupby("year"):
            regional_measures = _calculate_poverty_for_pooled_data(year_group, region_name=region, year=year)
            regional_rows.append(regional_measures)

    # Calculate World aggregate (all countries)
    log.info("calculate_regional_poverty: Calculating World aggregate")
    all_countries = tb_bins["country"].unique()
    world_data = tb_extended[tb_extended["country"].isin(all_countries)]

    if len(world_data) > 0:
        for year, year_group in world_data.groupby("year"):
            world_measures = _calculate_poverty_for_pooled_data(year_group, region_name="World", year=year)
            regional_rows.append(world_measures)

    return Table(regional_rows)


def _calculate_poverty_for_pooled_data(year_group: Table, region_name: str, year: int) -> dict:
    """Calculate poverty measures for pooled country data.

    Parameters
    ----------
    year_group : Table
        Data for all countries in a region for a specific year
    region_name : str
        Name of the region or "World"
    year : int
        Year for this calculation

    Returns
    -------
    dict
        Dictionary with poverty measures
    """
    # Pool all quantiles from all countries
    # Sort by income
    pooled = year_group.sort_values("avg")

    # Calculate cumulative population
    pooled["cumul_pop"] = pooled["pop"].cumsum()
    total_pop = pooled["pop"].sum()

    # Calculate for each poverty line
    regional_measures = {"country": region_name, "year": year, "extrapolation_method": "regional_aggregate"}

    for poverty_line in POVERTY_LINES:
        # Find where cumulative population crosses poverty line
        above_line = pooled[pooled["avg"] >= poverty_line]

        if len(above_line) == 0:
            # Everyone is below poverty line
            headcount = total_pop
            headcount_ratio = 1.0
        else:
            # Population below line is cumulative population just before crossing
            first_above_idx = above_line.index[0]
            if first_above_idx == pooled.index[0]:
                headcount = 0
            else:
                prev_idx = pooled.index[pooled.index.get_loc(first_above_idx) - 1]
                headcount = pooled.loc[prev_idx, "cumul_pop"]

            headcount_ratio = headcount / total_pop if total_pop > 0 else 0

        regional_measures[f"headcount_ratio_poverty_{poverty_line}_dollars"] = headcount_ratio
        regional_measures[f"headcount_poverty_{poverty_line}_dollars"] = headcount

    return regional_measures


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
