# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

# Individual Preferences

- @~/.claude/instructions/etl.md

## Architecture Overview

This is Our World in Data's ETL system - a content-addressable data pipeline with DAG-based execution. The system processes global development data through a multi-stage pipeline with rich metadata and automatic dependency resolution.

### Core Pipeline Stages

**snapshot** → **meadow** → **garden** → **grapher** → **export**

- **snapshot**: DVC-tracked raw files with rich metadata (`snapshots/`)
- **meadow**: Basic cleaning and format standardization (`etl/steps/data/meadow/`)
- **garden**: Business logic, harmonization, indicator creation (`etl/steps/data/garden/`)
- **grapher**: MySQL database ingestion for visualization (`etl/steps/data/grapher/`)
- **export**: Final outputs - explorers, collections, APIs (`etl/steps/data/export/`)

### Step Execution System

Steps are content-addressable with automatic dirty detection:

```python
# Standard garden step pattern
from etl.helpers import PathFinder, create_dataset
from etl.data_helpers import geo

paths = PathFinder(__file__)

def run(dest_dir: str) -> None:
    ds_input = paths.load_dataset("input_dataset")
    tb = ds_input["table_name"].reset_index()
    tb = geo.harmonize_countries(tb, countries_file=paths.country_mapping_path)
    tb = tb.format(short_name=paths.short_name)
    ds_garden = create_dataset(dest_dir, tables=[tb])
    ds_garden.save()
```

### Catalog System

Built on **owid.catalog** library:

- **Dataset**: Container for multiple tables with shared metadata
- **Table**: pandas.DataFrame subclass with rich metadata per column
- **Variable**: pandas.Series subclass with variable-specific metadata
- Content-based checksums for change detection
- Multiple formats (feather, parquet, csv) with automatic schema validation

### DAG Dependencies

YAML-based dependency graphs in `dag/` directory:

- Content-based dirty detection skips unchanged steps
- Topological sorting ensures proper execution order
- Supports partial execution with `--only`, `--downstream` flags

## Development Commands

### Essential Commands

```bash
# Core ETL operations
make etl                    # Run garden steps only
make full                   # Run complete pipeline
make grapher               # Include grapher upsert to DB
make sync.catalog          # Download ~10GB catalog from R2

# Development workflow
make test                  # All tests + linting + type checking
make unittest              # Unit tests only
make test-integration      # Integration tests
make watch                 # Watch for changes and re-run tests
make format                # Format code with ruff
make check                 # Format, lint & typecheck changed files

# Services
make wizard                # Start Wizard UI (port 8053)
make api                   # Start ETL API (port 8081)
make lab                   # Start Jupyter Lab
```

### ETL CLI Commands

```bash
# Main execution
etl run [steps...]         # Run specific ETL steps
etl run --dry-run         # Preview execution plan
etl run --force           # Force re-run ignoring checksums
etl run --only garden     # Run only garden steps
etl run --downstream      # Include downstream dependencies

# Development tools
etl harmonize country     # Interactive country harmonization
etl diff dataset1 dataset2   # Compare datasets
etl graphviz             # Generate dependency graph
etl d version-tracker    # Validate dataset versions
etl d reindex           # Rebuild catalog index
etl d prune             # Remove orphaned datasets

# Specific step types
pytest tests/test_etl_step_code.py::test_step_name  # Test single step
```

## Key Development Patterns

### Geographic Harmonization

Use `geo.harmonize_countries()` for standardization:

```python
from etl.data_helpers import geo
tb = geo.harmonize_countries(tb, countries_file=paths.country_mapping_path)
```

### Metadata Management

Tables inherit and propagate metadata:

```python
tb = tb.format(short_name="table_name")  # Sets table metadata
tb["column"] = tb["column"].replace_metadata(unit="percent", short_unit="%")
```

### Creating New Steps

1. Use Wizard UI (`make wizard`) for guided creation
2. Or follow existing patterns in `etl/steps/data/[stage]/[namespace]/`
3. Add dependencies to appropriate DAG file in `dag/`
4. Steps follow URI pattern: `data://[stage]/[namespace]/[version]/[name]`

### Testing Steps

```bash
# Test specific step
etl run --dry-run data://garden/namespace/version/dataset
pytest tests/test_etl_step_code.py -k "test_namespace_version_dataset"

# Test DAG integrity
pytest tests/test_dag_utils.py

# Integration test
pytest tests/test_steps.py -m integration
```

## Configuration

### Environment Variables

- `OWID_ENV`: dev/staging/production environment
- `.env`: Local environment configuration
- Database connections managed via `etl.config.OWID_ENV`

### Key Files

- `dag/main.yml`: Main DAG dependencies
- `dag/[topic].yml`: Topic-specific dependencies
- `etl/config.py`: Runtime configuration
- `pyproject.toml`: Dependencies and tool configuration

## Libraries Structure

### Core Libraries (`lib/`)

- **catalog**: Dataset/table/variable management with metadata
- **datautils**: Data processing utilities and helpers
- **repack**: Data packaging and compression tools

These are installed as editable packages (`owid-catalog`, `owid-datautils`, `owid-repack`).

### Apps vs Core ETL

- **Core ETL** (`etl/`): Step execution engine, catalog management, DAG processing
- **Apps** (`apps/`): Extended functionality - Wizard, chart sync, anomaly detection, maintenance tools

## Important Development Notes

- Always use `geo.harmonize_countries()` for geographic data
- Follow the `PathFinder` pattern for step inputs/outputs
- Using `--force` is usually unnecessary - the step will be re-run if the code changes
- Test steps with `etl run --dry-run` before execution
- Use `make sync.catalog` to avoid rebuilding entire catalog locally
- Check `etl d version-tracker` before major changes
- VS Code extensions available: `make install-vscode-extensions`
- Never run --force alone, if you want to force run a step, use --force --only together.
- When running ETL steps, always use --private flag
- When running grapher:// step in ETL, always add --grapher flag

## Debugging ETL Data Quality Issues

When ETL steps fail due to data quality issues (NaT values, missing data, missing indicators), always trace the problem upstream through the pipeline stages rather than patching symptoms downstream:

### Systematic Debugging Approach

1. **Check the Snapshot First**: Root cause often lies at the data source level, not ETL logic

   - Compare snapshot file sizes and date ranges - external providers may truncate or discontinue data feeds
   - Examine snapshot history: `git log --oneline --follow snapshots/dataset.csv.dvc`
   - Verify the upstream data source is still providing complete data
   - Time-based indicators (e.g., `last12m`) will be correctly set to NaN if source data is too old

2. **Trace Through Pipeline Stages**: Work backwards if snapshot data is complete:
   - **garden** → **meadow** → **snapshot** → **source data**
   - Load and inspect data at each stage to isolate where the issue originates
   - Fix at the earliest possible stage

### Example Debugging Commands

```python
# Examine snapshot data first
from etl.snapshot import Snapshot
snap = Snapshot('namespace/version/dataset.csv')
df = snap.read()
print(f"Date range: {df.DATE.min()} to {df.DATE.max()}")
print(f"Null values: {df.DATE.isnull().sum()}")

# Then examine meadow dataset if needed
from owid.catalog import Dataset
ds = Dataset('/path/to/meadow/dataset')
tb = ds['table_name'].reset_index()
print(f"Garden null values: {tb.date.isnull().sum()}")
```

### Common Data Quality Issues

- **NaT/null dates**: Often caused by malformed dates in source data or incorrect `dropna()` logic in meadow steps
- **Missing countries**: Check country mapping files and harmonization logic
- **Invalid data types**: Verify data conversion and cleaning steps at each stage
- **Duplicate records**: Examine index formation and deduplication logic

### Best Practices

- **Never patch symptoms**: Don't add workarounds in downstream steps for upstream data issues
- **Check external data sources first**: ETL logic may be working correctly with stale/incomplete data
- **Add assertions**: Include data quality checks that fail fast with clear error messages
- **Document data issues**: Log warnings about data quality problems found during processing
- **Fix meadow steps**: Most data cleaning should happen in meadow, not garden steps

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

## Data Validation and Sanity Checks

When adding data validation to ETL steps, implement comprehensive sanity checks following these patterns. Good validation prevents bad data from propagating through the pipeline and helps catch data quality issues early.

### Validation Best Practices

- **Use Metadata Files**: Always check for `*.meta.yml` files in the step directory and use them to inform validation logic
- **Fail Fast**: Put validation checks as early as possible in the pipeline
- **Clear Error Messages**: Include actual values in assertion messages for debugging
- **Domain Knowledge**: Use subject matter expertise to set realistic bounds and expectations
- **Warnings vs Errors**: Use `log.warning()` for concerning but non-fatal issues, `assert` for fatal problems
- **Progressive Validation**: Start with basic checks, then add domain-specific validation
- **Test Edge Cases**: Include validation for empty datasets, missing countries, extreme years
- **Document Assumptions**: Comment why specific thresholds or patterns are expected

### Validation Function Structure

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

### Using Metadata Files for Validation

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

### Standard Validation Categories

**1. Basic Data Integrity**

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

**2. Value Range Validation**

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

**3. Temporal Consistency**

```python
# Check for reasonable temporal trends
world_data = tb[tb["country"] == "World"].sort_values("year")
if len(world_data) >= 10:
    # Life expectancy should generally increase over time
    recent_avg = world_data.tail(5)["life_expectancy"].mean()
    earlier_avg = world_data.head(5)["life_expectancy"].mean()
    assert recent_avg > earlier_avg * 0.95, "Life expectancy hasn't improved as expected over time"
```

**4. Regional/Geographic Validation**

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

### OWID-Specific Validation Functions

**Regional Aggregation Validation**

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
                        regional_total = regional_data[var_name].sum(skipna=True)
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

### Validation Integration Pattern

**Important**: Always perform final validation **before** calling the `.format()` method, as formatting sets indexes and changes the table structure, making validation more complex.

```python
def run() -> None:
    # Load metadata if available
    metadata = _load_metadata_if_available(Path(__file__).parent)

    # Load inputs
    ds_input = paths.load_dataset("input_data")

    # Validate inputs immediately after loading
    log.info("Validating input datasets")
    _validate_input_datasets(ds_input)

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

### Common Validation Patterns by Data Type

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
