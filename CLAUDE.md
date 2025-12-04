# Agent Guide

This file provides guidance to automation agents working with code in this repository.

# Individual Preferences
- @~/.claude/instructions/etl.md

## Critical Rules

- When running `etl` command, **ALWAYS** use the `.venv/bin/etl` binary (same for `python`, `pytest`, etc.)
⚠️ **NEVER mask problems - fix them systematically:**
- **NEVER** return empty tables, comment out failing code, or create workarounds
- **NEVER** catch and ignore exceptions without fixing the root cause
- **ALWAYS** trace issues upstream through the pipeline: snapshot → meadow → garden → grapher
- **ALWAYS** provide full error tracebacks - don't truncate diagnostic information
- **If unsure, ASK THE USER** - don't guess or mask issues
- **Never** push or commit unless explicitly told to do so

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
from etl.helpers import PathFinder
from etl.data_helpers import geo

paths = PathFinder(__file__)

def run() -> None:
    ds_input = paths.load_dataset("input_dataset")
    tb = ds_input["table_name"].reset_index()
    tb = geo.harmonize_countries(tb, countries_file=paths.country_mapping_path)
    tb = tb.format(short_name=paths.short_name)
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
```

### Ad-hoc Data Exploration

For exploring snapshot data outside of ETL steps (e.g., in notebooks or debugging):
```python
from etl.snapshot import Snapshot

# Load snapshot using short path (namespace/version/filename)
snap = Snapshot("who/latest/fluid.csv")
tb = snap.read_csv()
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

## Additional Tools

Get `--help` for details on any command.

### etl archive

Archive old datasets.


## Key Development Patterns

### CLI Tools
Always use Click instead of ArgumentParser for CLI scripts:
```python
import click

@click.command()
@click.option("--dry-run", is_flag=True, help="Preview changes without applying them")
def main(dry_run: bool, output: str):
    """Brief description of what the CLI does."""
    # Implementation here

if __name__ == "__main__":
    main()
```

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

### YAML File Editing
Always use `ruamel_load` and `ruamel_dump` from `etl.files` to preserve comments and formatting when editing YAML files:
```python
from etl.files import ruamel_load, ruamel_dump

# Load YAML while preserving comments and formatting
data = ruamel_load(file_path)

# Modify data as needed
data['some_key'] = new_value

# Save back to file with original formatting preserved
with open(file_path, 'w') as f:
    f.write(ruamel_dump(data))
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

### Python Environment

⚠️ **CRITICAL: Virtual Environment Usage**

This project uses a Python virtual environment (`.venv/`). **ALL Python commands must use the virtual environment binaries:**

```bash
# CORRECT - Use .venv binaries
.venv/bin/python script.py
.venv/bin/etl run step
.venv/bin/pytest tests/

# WRONG - Global commands will fail
python script.py        # ❌ Don't use
etl run step           # ❌ Don't use
pytest tests/          # ❌ Don't use
```

**Throughout this document, when you see commands like `etl`, `python`, or `pytest`, always prefix them with `.venv/bin/`**

- **Package Management**: Always use `uv` package manager instead of `pip`
  ```bash
  uv add package_name     # Add a new package
  uv remove package_name  # Remove a package
  uv sync                 # Sync dependencies
  ```
- **IMPORTANT**: Never install packages with `pip install` - always ask first, then use `uv` if approved

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

## Running ETL Steps
Use `.venv/bin/etlr` to run ETL steps:

### Basic Usage
- Run steps matching a pattern: `etlr biodiversity/2025-06-28/cherry_blossom`
- Run with grapher upload: `etlr biodiversity/2025-06-28/cherry_blossom --grapher`
- Dry run (preview): `etlr biodiversity/2025-06-28/cherry_blossom --dry-run`

### Key Options
- `--grapher/-g`: Upload datasets to grapher database (OWID staff only)
- `--dry-run`: Preview steps without running them
- `--force/-f`: Re-run steps even if up-to-date
- `--only/-o`: Run only selected step (no dependencies)
- `--downstream/-d`: Include downstream dependencies
- `--exact-match/-x`: Steps must exactly match arguments

## Git Workflow
Create PR first, then commit files:

1. **Create PR**: Use `etl pr` CLI (creates new branch)
2. **Check status**: `git status` to see modified/untracked files
3. **Add files**: `git add .` or `git add <specific-files>`
4. **Commit**: `git commit -m "Description of changes"`

⚠️ **ALWAYS use `etl pr` command to create pull requests** - never use `git checkout -b` + `gh pr create` manually.

Note: The `etl pr` creates a new branch but does NOT automatically commit files - you must commit manually after creating the PR.

### Commit Messages
When creating git commits, refer to [.claude/commands/commit.md](.claude/commands/commit.md) for commit message formatting guidelines including emoji usage.

## Important Development Notes

- Always use `geo.harmonize_countries()` for geographic data
- Follow the `PathFinder` pattern for step inputs/outputs
- Using `--force` is usually unnecessary - the step will be re-run if the code changes
- Test steps with `etl run --dry-run` before execution
- Use `make sync.catalog` to avoid rebuilding entire catalog locally
- Check `etl d version-tracker` before major changes
- VS Code extensions available: `make install-vscode-extensions`
- Never run --force alone, if you want to force run a step, use --force --only together
- When running ETL steps, always use --private flag
- When running grapher:// step in ETL, always add --grapher flag
- **ALWAYS run `make check` before committing** - formats code, fixes linting issues, and runs type checks
- SQL queries enclose in triple quotes for readability

### Exception Handling
- **NEVER** catch, log, and re-raise exceptions (`except Exception: log.error(e); raise`)
- Let exceptions propagate naturally with their original stack traces
- Only catch specific exceptions when you can meaningfully handle them
- Avoid `except Exception` - it masks real problems

### Never Mask Underlying Issues
- **NEVER** return empty tables or default values to "fix" data parsing failures
- **NEVER** silently skip errors or missing data without clear explanation
- **NEVER** comment out code to temporarily bypass problems - fix the underlying issue instead
- **BAD**: `return Table(pd.DataFrame({'col': []}))` - hides the real problem
- **BAD**: `try: parse_data() except: return empty_table` - masks what's broken
- **BAD**: `# return extract_data()  # Commented out due to format change` - commenting out code to avoid errors
- **GOOD**: Let the error happen and provide clear diagnostic information
- **GOOD**: `raise ValueError("Sheet 'Fig 3.2' format changed - skiprows needs updating from 7 to X")`
- **GOOD**: Update the code to handle the new data format correctly
- **If you don't know what to do - ASK THE USER instead of masking the issue**
- Silent failures make debugging exponentially harder and create technical debt

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

## Performance Profiling and Optimization

### Memory Profiling

Use `etl d profile` to identify memory bottlenecks:

```bash
# Profile memory usage line-by-line
etl d profile --mem garden/namespace/version/dataset

# Profile specific functions for cleaner output
etl d profile --mem garden/namespace/version/dataset -f function_name

# Profile CPU usage
etl d profile --cpu garden/namespace/version/dataset
```

### Common Memory Issues and Solutions

#### 1. Object vs Categorical Dtypes

**Problem**: String columns stored as `object` dtype consume 10-100x more memory than `category`.

**Solution**: Always load datasets with `safe_types=False` and ensure categorical dtypes are preserved:

```python
# Load with categorical dtypes preserved
ds = paths.load_dataset("dataset_name")
tb = ds.read("table_name", safe_types=False)

# Verify dtypes
assert isinstance(tb["country"].dtype, pd.CategoricalDtype), "country must be categorical"

# If categorical is lost, convert it back
if not isinstance(tb["country"].dtype, pd.CategoricalDtype):
    tb["country"] = tb["country"].astype("category")
```

**Common causes of categorical → object conversion:**
- Using `np.asarray()` on categorical columns (use `.array` instead)
- `.apply()` operations that return new values
- Some merge/concat operations (usually preserved with `owid.catalog.processing`)

**Memory savings**: 96-99% reduction for string columns (e.g., 21 MB → 0.8 MB)

#### 2. Vectorization vs Row-by-Row Operations

**Problem**: Using `.apply(func, axis=1)` is 100x+ slower than vectorized operations.

**Bad** (slow):
```python
tb["result"] = tb.apply(lambda row: func(row), axis=1)
tb["log_value"] = tb["value"].apply(lambda x: np.log(x) if pd.notna(x) else x)
```

**Good** (fast):
```python
# Use np.select for conditional logic
conditions = [tb["col1"].notna(), tb["col2"].notna()]
choices = [tb["col1"], tb["col2"]]
tb["result"] = np.select(conditions, choices, default=tb["col3"])

# Use vectorized functions with proper NaN handling
def safe_log(values):
    arr = np.asarray(values, dtype=float).copy()
    valid_mask = np.isfinite(arr) & (arr > 0)
    np.log(arr, out=arr, where=valid_mask)
    return arr

tb["log_value"] = safe_log(tb["value"])
```

**Performance gain**: 100-1000x speedup for large datasets

#### 3. Large DataFrame Expansion

When expanding dataframes (e.g., creating 1000 quantiles per row):

**Expected behavior**: Output size scales linearly with input
- 14,000 rows → 14M rows (1000x expansion) = ~1-2 GB output
- This is **unavoidable** if you need all the data

**Optimization strategies**:
- Use categorical dtypes for repeated values (country, region)
- Use appropriate numeric types (Float32 instead of Float64 when precision allows)
- Process in batches if memory constrained
- Use `del` to free intermediate variables immediately

### Profiling Workflow

1. **Run initial profile** to identify hotspots:
   ```bash
   etl d profile --mem garden/namespace/version/dataset
   ```

2. **Analyze memory spikes**:
   - Look for large increments (>100 MB jumps)
   - Check if output size is inherent or wasteful
   - Verify categorical dtypes are preserved

3. **Create isolated test** to verify optimizations:
   ```python
   # Test with small representative data
   # Measure before/after memory usage
   # Verify output is identical
   ```

4. **Apply optimizations**:
   - Preserve categorical dtypes
   - Vectorize row-by-row operations
   - Add assertions to prevent regressions

5. **Re-profile** to confirm improvements

### Memory Estimation

Quick estimation for DataFrame memory:
```python
# Rough memory estimate
n_rows = 14_000_000
n_numeric_cols = 3  # Float64 = 8 bytes each
n_categorical_cols = 2  # Category ~0.1 MB per million rows

numeric_mb = (n_rows * n_numeric_cols * 8) / (1024**2)  # ~320 MB
categorical_mb = n_categorical_cols * (n_rows / 1_000_000) * 0.1  # ~3 MB
total_mb = numeric_mb + categorical_mb  # ~323 MB

print(f"Expected memory: {total_mb:.0f} MB")

# Actual memory (with pandas overhead ~30-50%)
actual_mb = total_mb * 1.4
print(f"Actual memory: {actual_mb:.0f} MB")
```

### Best Practices

- **Profile first, optimize later**: Don't guess where bottlenecks are
- **Measure impact**: Verify optimizations actually help
- **Document expectations**: Add comments explaining expected memory usage
- **Add assertions**: Catch dtype regressions early
- **Use appropriate dtypes**: Category for strings, UInt16/Float32 when possible

## Database Access

### MySQL Connection
Can execute SQL queries directly using the staging database:
```bash
mysql -h staging-site-[branch] -u owid --port 3306 -D owid -e "SELECT query"
```

Example queries:
```sql
-- Find datasets by shortName
SELECT id, catalogPath, name FROM datasets WHERE shortName = 'dataset_name' AND NOT isArchived;

-- Check variables in dataset
SELECT id, name FROM variables WHERE datasetId = 12345;
```
