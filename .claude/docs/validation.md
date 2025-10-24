# Data Validation and Sanity Checks

When adding data validation to ETL steps, implement comprehensive sanity checks following these patterns. Good validation prevents bad data from propagating through the pipeline and helps catch data quality issues early.

## OWID-Specific Data Conventions

### Regional Classifications

Our World in Data uses these standard regions:
- **Africa**
- **Europe**
- **Asia**
- **Oceania**
- **North America**
- **South America**

**Important**: These six regions should aggregate to the **World** total for additive variables (raw counts, totals) but NOT for rates, averages, or intensive variables. Use the metadata file to determine if a variable should be additive.

### Income Group Classifications

We also use World Bank income groups:
- **High-income countries**
- **Upper-middle-income countries**
- **Lower-middle-income countries**
- **Low-income countries**

**Important**: These four income groups should:
- **For additive variables**: Sum to the **World** total (like regional aggregation)
- **For rates/averages**: The **World** value should fall within the range of these income groups (not necessarily be their average, as it's population-weighted)

## Validation Best Practices

- **Use Metadata Files**: Always check for `*.meta.yml` files in the step directory and use them to inform validation logic
- **Fail Fast**: Put validation checks as early as possible in the pipeline
- **Clear Error Messages**: Include actual values in assertion messages for debugging
- **Domain Knowledge**: Use subject matter expertise to set realistic bounds and expectations
- **Warnings vs Errors**: Use `log.warning()` for concerning but non-fatal issues, `assert` for fatal problems
- **Progressive Validation**: Start with basic checks, then add domain-specific validation
- **Test Edge Cases**: Include validation for empty datasets, missing countries, extreme years
- **Document Assumptions**: Comment why specific thresholds or patterns are expected

## Validation Function Structure

Create dedicated validation functions that follow this pattern:

```python
from structlog import get_logger

log = get_logger()

def _validate_input_datasets(*datasets) -> None:
    """Validate input datasets for basic integrity checks."""
    # Check datasets aren't empty
    # Verify required columns exist
    # Check data types are expected
    # Validate key constraints

def _validate_output_data(tb_final: Table) -> None:
    """Validate final output data for sanity checks."""
    # Check value ranges are reasonable
    # Verify no duplicates where unexpected
    # Validate temporal consistency
    # Check completeness requirements

def _validate_domain_specific_logic(tb: Table) -> None:
    """Domain-specific validation based on subject matter expertise."""
    # Regional/country-specific validation
    # Historical pattern validation
    # Cross-variable consistency checks
    # Outlier detection and flagging
```

## Using Metadata Files for Validation

Always check for and utilize metadata files (`*.meta.yml`) when available, as they contain valuable information about expected data characteristics:

```python
import yaml
from pathlib import Path

def _load_metadata_if_available(step_dir: Path) -> dict:
    """Load metadata file if it exists in the step directory."""
    metadata_files = list(step_dir.glob("*.meta.yml"))
    if metadata_files:
        with open(metadata_files[0], 'r') as f:
            return yaml.safe_load(f)
    return {}

def _validate_against_metadata(tb: Table, metadata: dict) -> None:
    """Validate table against metadata specifications."""
    if not metadata or 'tables' not in metadata:
        return

    for table_name, table_meta in metadata['tables'].items():
        if 'variables' not in table_meta:
            continue

        for var_name, var_meta in table_meta['variables'].items():
            if var_name not in tb.columns:
                continue

            # Check units match expectations
            if 'unit' in var_meta:
                log.info(f"Expected unit for {var_name}: {var_meta['unit']}")

            # Validate against specified ranges
            if 'display' in var_meta and 'numDecimalPlaces' in var_meta['display']:
                expected_decimals = var_meta['display']['numDecimalPlaces']
                # Validate decimal precision matches expectation

            # Use description to inform validation logic
            if 'description' in var_meta:
                desc = var_meta['description'].lower()
                if 'percentage' in desc or 'rate' in desc:
                    # Apply percentage validation (0-100)
                    assert tb[var_name].min(skipna=True) >= 0, f"{var_name}: Negative rate found"
                    assert tb[var_name].max(skipna=True) <= 100, f"{var_name}: Rate exceeds 100%"
```

## Standard Validation Categories

### 1. Basic Data Integrity

```python
# Dataset completeness
assert len(tb) > 0, "Dataset is empty"
assert not tb.duplicated(subset=["country", "year"]).any(), "Duplicate country-year pairs found"

# Required columns exist
required_cols = ["country", "year", "indicator_value"]
missing_cols = [col for col in required_cols if col not in tb.columns]
assert not missing_cols, f"Missing required columns: {missing_cols}"

# Data types are correct
assert tb["year"].dtype in ["int64", "Int64"], "Year column should be integer"
assert pd.api.types.is_numeric_dtype(tb["indicator_value"]), "Indicator should be numeric"
```

### 2. Value Range Validation

```python
# Reasonable bounds based on domain knowledge
assert tb["life_expectancy"].min() >= 20, f"Unrealistically low life expectancy: {tb['life_expectancy'].min()}"
assert tb["life_expectancy"].max() <= 120, f"Unrealistically high life expectancy: {tb['life_expectancy'].max()}"

# Percentage indicators
assert tb["mortality_rate"].min() >= 0, "Negative mortality rate found"
assert tb["mortality_rate"].max() <= 100, "Mortality rate exceeds 100%"

# Year ranges
min_year, max_year = 1750, 2030
assert tb["year"].min() >= min_year, f"Year too early: {tb['year'].min()}"
assert tb["year"].max() <= max_year, f"Year too late: {tb['year'].max()}"
```

### 3. Temporal Consistency

```python
# Check for reasonable temporal trends
world_data = tb[tb["country"] == "World"].sort_values("year")
if len(world_data) >= 10:
    # Life expectancy should generally increase over time
    recent_avg = world_data.tail(5)["life_expectancy"].mean()
    earlier_avg = world_data.head(5)["life_expectancy"].mean()
    assert recent_avg > earlier_avg * 0.95, "Life expectancy hasn't improved as expected over time"
```

### 4. Regional/Geographic Validation

```python
# Region-specific validation based on historical patterns
for region in ["Africa", "Europe", "Asia", "World"]:
    region_data = tb[tb["country"] == region]
    if len(region_data) == 0:
        continue

    rates = region_data["child_mortality_rate"].dropna()
    if region == "Europe":
        # Europe should have lower recent mortality rates
        recent_data = region_data[region_data["year"] >= 1990]
        if len(recent_data) > 0:
            recent_rates = recent_data["child_mortality_rate"].dropna()
            assert recent_rates.max() <= 30, f"Europe: Unexpectedly high recent mortality: {recent_rates.max()}%"
```

### 5. Cross-Variable Consistency

```python
# Related variables should be mathematically consistent
if "survival_rate" in tb.columns and "mortality_rate" in tb.columns:
    expected_survival = 100 - tb["mortality_rate"]
    rate_diff = abs(tb["survival_rate"] - expected_survival)
    assert rate_diff.max() < 0.1, f"Survival/mortality rates inconsistent (max diff: {rate_diff.max()}%)"
```

## OWID-Specific Validation Functions

### Regional Aggregation Validation

```python
def _validate_regional_aggregation(tb: Table, metadata: dict) -> None:
    """Validate that additive variables aggregate correctly from regions to World."""
    regions = ["Africa", "Europe", "Asia", "Oceania", "North America", "South America"]

    if not metadata or 'tables' not in metadata:
        return

    for table_name, table_meta in metadata['tables'].items():
        if 'variables' not in table_meta:
            continue

        for var_name, var_meta in table_meta['variables'].items():
            if var_name not in tb.columns:
                continue

            # Check if variable should be additive based on metadata
            is_additive = _is_additive_variable(var_meta)

            if is_additive:
                # Validate regional aggregation for additive variables
                for year in tb["year"].unique():
                    year_data = tb[tb["year"] == year]

                    regional_data = year_data[year_data["country"].isin(regions)]
                    world_data = year_data[year_data["country"] == "World"]

                    if len(regional_data) == len(regions) and len(world_data) == 1:
                        # Check if any regions have NaN values
                        has_nan_regions = regional_data[var_name].isna().any()
                        if has_nan_regions:
                            log.warning(f"Some regions have NaN values for {var_name}, skipping aggregation validation")
                        else:
                            # Only validate when all regions have data
                            regional_total = regional_data[var_name].sum()
                            world_total = world_data[var_name].iloc[0]

                            # Allow small tolerance for rounding differences
                            tolerance = abs(world_total * 0.01) if world_total != 0 else 1.0
                            assert abs(regional_total - world_total) <= tolerance, \
                                f"{var_name} regional totals don't match World in {year}: {regional_total:.1f} vs {world_total:.1f}"

def _is_additive_variable(var_meta: dict) -> bool:
    """Determine if a variable should aggregate additively based on metadata."""
    # Check unit for clues about additivity
    unit = var_meta.get('unit', '').lower()
    description = var_meta.get('description', '').lower()

    # Additive indicators (should sum across regions)
    additive_keywords = ['total', 'number', 'count', 'deaths', 'cases', 'population', 'people']
    # Non-additive indicators (rates, averages, percentages)
    non_additive_keywords = ['rate', 'per capita', 'percentage', 'percent', '%', 'ratio', 'average', 'mean', 'expectancy']

    # Check for non-additive keywords first (more specific)
    if any(keyword in unit for keyword in non_additive_keywords) or \
       any(keyword in description for keyword in non_additive_keywords):
        return False

    # Check for additive keywords
    if any(keyword in unit for keyword in additive_keywords) or \
       any(keyword in description for keyword in additive_keywords):
        return True

    # Default to non-additive if unclear (safer assumption)
    return False
```

### Income Group Aggregation Validation

```python
def _validate_income_group_aggregation(tb: Table, metadata: dict) -> None:
    """Validate that income groups aggregate correctly to World."""
    income_groups = ["High-income countries", "Upper-middle-income countries",
                    "Lower-middle-income countries", "Low-income countries"]

    if not metadata or 'tables' not in metadata:
        return

    for table_name, table_meta in metadata['tables'].items():
        if 'variables' not in table_meta:
            continue

        for var_name, var_meta in table_meta['variables'].items():
            if var_name not in tb.columns:
                continue

            # Check if variable should be additive based on metadata
            is_additive = _is_additive_variable(var_meta)

            for year in tb["year"].unique():
                year_data = tb[tb["year"] == year]

                income_data = year_data[year_data["country"].isin(income_groups)]
                world_data = year_data[year_data["country"] == "World"]

                if len(world_data) == 0:
                    continue

                world_value = world_data[var_name].iloc[0]

                if is_additive:
                    # For additive variables: income groups should sum to World
                    if len(income_data) == len(income_groups):
                        income_total = income_data[var_name].sum()
                        tolerance = abs(world_value * 0.01) if world_value != 0 else 1.0

                        assert abs(income_total - world_value) <= tolerance, \
                            f"{var_name} income group totals don't match World in {year}: {income_total:.1f} vs {world_value:.1f}"
                else:
                    # For rates/averages: World should be within range of income groups
                    if len(income_data) >= 2:  # Need at least 2 income groups for range check
                        income_values = income_data[var_name].dropna()
                        if len(income_values) >= 2:
                            min_income = income_values.min()
                            max_income = income_values.max()

                            # Allow some tolerance beyond the range for edge cases
                            range_tolerance = (max_income - min_income) * 0.1  # 10% of range

                            if not (min_income - range_tolerance <= world_value <= max_income + range_tolerance):
                                log.warning(
                                    "World value outside income group range",
                                    variable=var_name,
                                    year=year,
                                    world_value=world_value,
                                    income_min=min_income,
                                    income_max=max_income
                                )
```

### Country Coverage and Completeness

```python
def _validate_country_coverage(tb: Table, metadata: dict) -> None:
    """Validate reasonable country coverage for global datasets."""
    regions = ["Africa", "Europe", "Asia", "Oceania", "North America", "South America"]
    countries = set(tb["country"].unique()) - {"World"} - set(regions)

    # Check minimum country coverage for global datasets
    min_countries = 50  # Adjust based on dataset expectations
    assert len(countries) >= min_countries, f"Only {len(countries)} countries found, expected at least {min_countries}"

    # Check for major countries that should typically be present
    major_countries = {"United States", "China", "India", "Germany", "United Kingdom", "France", "Japan"}
    missing_major = major_countries - countries
    if missing_major:
        log.warning("Missing major countries", missing_countries=list(missing_major))

    # Validate country names against expected format (no obvious typos)
    for country in countries:
        assert len(country) >= 2, f"Country name too short: '{country}'"
        assert not country.isdigit(), f"Country name appears to be numeric: '{country}'"
```

### Data Source and Origin Validation

```python
def _validate_data_sources(tb: Table) -> None:
    """Validate data source information is present and reasonable."""
    if "source" in tb.columns:
        # Check no empty/null sources
        assert not tb["source"].isna().any(), "Missing source information found"
        assert not (tb["source"] == "").any(), "Empty source strings found"

        # Log source distribution
        source_counts = tb["source"].value_counts()
        log.info("Data sources found", sources=dict(source_counts))

    # Validate origins metadata exists for key columns
    numeric_cols = tb.select_dtypes(include=['number']).columns
    for col in numeric_cols:
        if hasattr(tb[col], 'metadata') and hasattr(tb[col].metadata, 'origins'):
            assert len(tb[col].metadata.origins) > 0, f"No origins metadata for column {col}"
```

### Time Series Continuity and Gaps

```python
def _validate_time_series_continuity(tb: Table) -> None:
    """Check for suspicious gaps or discontinuities in time series."""
    for country in tb["country"].unique():
        country_data = tb[tb["country"] == country].sort_values("year")

        if len(country_data) < 2:
            continue

        years = country_data["year"].values
        year_gaps = np.diff(years)

        # Flag large gaps (more than 10 years) in time series
        large_gaps = year_gaps[year_gaps > 10]
        if len(large_gaps) > 0:
            log.warning("Large time series gaps found",
                       country=country,
                       max_gap=int(large_gaps.max()),
                       gap_positions=years[np.where(year_gaps > 10)[0]].tolist())
```

### Outlier Detection for Key Variables

```python
def _validate_outliers(tb: Table, metadata: dict) -> None:
    """Detect statistical outliers that may indicate data quality issues."""
    numeric_cols = tb.select_dtypes(include=['number']).columns

    for col in numeric_cols:
        if col in ["year"]:  # Skip non-indicator columns
            continue

        values = tb[col].dropna()
        if len(values) < 10:  # Need sufficient data for outlier detection
            continue

        # Use IQR method for outlier detection
        Q1 = values.quantile(0.25)
        Q3 = values.quantile(0.75)
        IQR = Q3 - Q1

        lower_bound = Q1 - 3 * IQR  # Using 3*IQR for extreme outliers
        upper_bound = Q3 + 3 * IQR

        outliers = tb[(tb[col] < lower_bound) | (tb[col] > upper_bound)]

        if len(outliers) > 0:
            outlier_countries = outliers[["country", "year", col]].to_dict('records')
            log.warning("Statistical outliers detected",
                       variable=col,
                       count=len(outliers),
                       examples=outlier_countries[:5])  # Show first 5 examples
```

### Population-Based Validation

```python
def _validate_population_consistency(tb: Table) -> None:
    """Validate indicators that should be consistent with population data."""
    # If we have both total counts and rates, check they're consistent
    for col in tb.columns:
        if 'rate' in col.lower() or 'per capita' in col.lower():
            # Look for corresponding total column
            base_name = col.replace('_rate', '').replace('_per_capita', '').replace('rate_', '').replace('per_capita_', '')
            potential_total_cols = [c for c in tb.columns if base_name in c and 'total' in c.lower()]

            for total_col in potential_total_cols:
                # Basic consistency check - totals should be higher than rates for same entities
                for country in tb["country"].unique():
                    country_data = tb[tb["country"] == country]
                    if len(country_data) > 0:
                        max_rate = country_data[col].max()
                        min_total = country_data[total_col].min()
                        if pd.notna(max_rate) and pd.notna(min_total) and max_rate > min_total:
                            log.warning("Rate higher than total - possible units mismatch",
                                      country=country, rate_col=col, total_col=total_col,
                                      max_rate=max_rate, min_total=min_total)
```

### Historical Plausibility Checks

```python
def _validate_historical_plausibility(tb: Table) -> None:
    """Check that historical trends make sense for development indicators."""

    # Life expectancy should generally increase over time (with some exceptions)
    life_exp_cols = [col for col in tb.columns if 'life_expectancy' in col.lower()]
    for col in life_exp_cols:
        world_data = tb[tb["country"] == "World"].sort_values("year")
        if len(world_data) >= 20:  # Need sufficient historical data
            # Check that recent decades show improvement
            early_period = world_data[world_data["year"] <= 1970][col].mean()
            recent_period = world_data[world_data["year"] >= 2000][col].mean()

            if pd.notna(early_period) and pd.notna(recent_period):
                if recent_period <= early_period:
                    log.warning("Life expectancy hasn't improved globally over time",
                              early_avg=early_period, recent_avg=recent_period)

    # GDP per capita should generally increase (inflation-adjusted)
    gdp_cols = [col for col in tb.columns if 'gdp' in col.lower() and 'capita' in col.lower()]
    for col in gdp_cols:
        # Similar check for reasonable long-term economic growth
        pass  # Implementation similar to life expectancy
```

## Validation Integration Pattern

### Separate Validation File Approach (Recommended)

**Best Practice**: Create a separate validation step file alongside your main ETL step. This approach:
- Keeps validation logic separate from ETL logic
- Automatically included in checksum (validation re-runs when code OR data changes)
- Makes validation code reusable and easier to maintain
- Allows validation to run independently for testing

**File Structure and Naming:**
```
etl/steps/data/garden/namespace/version/
├── [short_name].py              # Main ETL step
├── [short_name].meta.yml        # Metadata
└── [short_name]_validation.py   # Validation step
```

**Important**: The validation file must start with `[short_name]` (the same prefix as the main step file) to be automatically included in the checksum. The ETL system includes all files matching `[short_name]*` in the step's checksum calculation.

**Validation Step File** (`[short_name]_validation.py`):
```python
"""Validation step for [dataset_name]."""

from pathlib import Path
import pandas as pd
from structlog import get_logger
from owid.catalog import Table

from etl.helpers import PathFinder

log = get_logger()
paths = PathFinder(__file__)


def run() -> None:
    """Run validation checks on the dataset."""
    # Load the dataset to validate
    ds = paths.load_dataset("[short_name]")

    # Load metadata if available
    metadata = _load_metadata_if_available(Path(__file__).parent)

    # Get the main table
    tb = ds["[table_name]"].reset_index()

    # Run all validations
    log.info("Running validation checks")
    _validate_input_datasets(ds)
    _validate_output_data(tb)
    _validate_against_metadata(tb, metadata)
    _validate_regional_aggregation(tb, metadata)
    _validate_income_group_aggregation(tb, metadata)
    _validate_country_coverage(tb, metadata)
    _validate_data_sources(tb)
    _validate_time_series_continuity(tb)
    _validate_outliers(tb, metadata)
    _validate_population_consistency(tb)
    _validate_historical_plausibility(tb)

    log.info("All validation checks passed")


def _load_metadata_if_available(step_dir: Path) -> dict:
    """Load metadata file if it exists in the step directory."""
    import yaml

    # Look for metadata file with the dataset name
    metadata_files = list(step_dir.glob("*.meta.yml"))
    if metadata_files:
        with open(metadata_files[0], 'r') as f:
            return yaml.safe_load(f)
    return {}


def _validate_input_datasets(ds) -> None:
    """Validate input datasets for basic integrity checks."""
    # Check dataset isn't empty
    assert len(ds.table_names) > 0, "Dataset has no tables"

    for table_name in ds.table_names:
        tb = ds[table_name]
        assert len(tb) > 0, f"Table {table_name} is empty"


def _validate_output_data(tb: Table) -> None:
    """Validate final output data for sanity checks."""
    # Add your domain-specific validation here
    pass


# Add other validation functions here following patterns from this guide:
# - _validate_against_metadata(tb, metadata)
# - _validate_regional_aggregation(tb, metadata)
# - _validate_income_group_aggregation(tb, metadata)
# - _validate_country_coverage(tb, metadata)
# - _validate_data_sources(tb)
# - _validate_time_series_continuity(tb)
# - _validate_outliers(tb, metadata)
# - _validate_population_consistency(tb)
# - _validate_historical_plausibility(tb)
# - _is_additive_variable(var_meta)
```

**Adding Validation Step to DAG:**

Add the validation step to your DAG file with a dependency on the main step:

```yaml
steps:
  # Main ETL step
  data://garden/namespace/version/short_name:
    - data://meadow/namespace/version/short_name

  # Validation step depends on main step
  data://garden/namespace/version/short_name_validation:
    - data://garden/namespace/version/short_name
```

The validation file (`[short_name]_validation.py`) is **automatically included in the validation step's checksum** because it follows the naming convention `[short_name]*`. The ETL system's `_step_files()` method uses glob patterns to find all related files:
- `[short_name].py` → main step file ✓
- `[short_name]_validation.py` → validation file ✓
- `[short_name].meta.yml` → metadata file ✓

**No manual checksum configuration needed in the DAG** - just follow the naming pattern!

The validation step will automatically:
- Run when the input dataset checksum changes (data changed)
- Run when the validation file itself changes (automatically in checksum)
- Be skipped if neither has changed
- Fail the pipeline if validation checks fail

**Benefits of Separate Validation Files:**
1. **Performance**: Validation only runs when data OR validation code changes, not on unrelated code changes
2. **Modularity**: Keeps ETL logic and validation logic separate
3. **Testing**: Can test validation independently
4. **Reusability**: Validation functions can be shared across datasets
5. **Clarity**: Easier to maintain and understand validation rules
6. **Automatic Checksum**: Naming convention handles inclusion - no DAG configuration needed

### Inline Validation Pattern (Alternative)

If you prefer to keep validation in the main step file, validation should be performed **before** calling the `.format()` method, as formatting sets indexes and changes the table structure:

```python
def run() -> None:
    # Load metadata if available
    metadata = _load_metadata_if_available(Path(__file__).parent)

    # Load inputs
    ds_input = paths.load_dataset("input_data")

    # Process data
    tb = process_data(ds_input)
    tb = geo.harmonize_countries(tb, countries_file=paths.country_mapping_path)

    # CRITICAL: Validate outputs BEFORE formatting (before .format() call)
    log.info("Validating processed data")
    _validate_output_data(tb)
    _validate_against_metadata(tb, metadata)
    _validate_regional_aggregation(tb, metadata)

    # Format table (sets indexes) - do this AFTER validation
    tb = tb.format(["country", "year"])

    # Save outputs
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
```

**Note**: The inline pattern is simpler but runs validation on every execution, even when only code (not data) changes.

## Using Structured Logging for Validation

```python
# Warning for concerning but non-fatal issues
if missing_regions:
    log.warning("Missing regions in dataset", regions=missing_regions, dataset_name=name)

# Info for validation progress
log.info("Regional data validation completed", regions_validated=len(expected_regions))

# Error context before assertions
if not (min_regional - tolerance <= world_rate <= max_regional + tolerance):
    log.warning(
        "World rate outside regional bounds",
        world_rate=world_rate,
        regional_min=min_regional,
        regional_max=max_regional,
        year=year
    )
```

## Common Validation Patterns by Data Type

**Health/Mortality Data:**
- Rates between 0-100%, life expectancy 20-120 years
- Historical improvement trends expected
- Regional patterns based on development levels

**Economic Data:**
- GDP/income should be positive, reasonable growth rates
- Inflation rates can be negative but extreme values need checking
- Currency units and scale factors validated

**Population Data:**
- Population counts positive, reasonable growth rates
- Age distributions sum correctly
- Migration data balances between countries

**Environmental Data:**
- Temperature, precipitation within physical bounds
- Emission factors positive with reasonable country totals
- Time series consistency for monitoring data

Always adapt these patterns to your specific dataset's domain and expected data characteristics.
