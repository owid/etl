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
- **Virtual Environment**: This project uses a Python virtual environment (`.venv/`)
- **Activation**: Always activate the virtual environment before running commands:
  ```bash
  source .venv/bin/activate  # Activate virtual environment
  ```
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
Use `etlr` to run ETL steps:

### Basic Usage
- Run steps matching a pattern: `etlr biodiversity/2025-06-28/cherry_blossom`
- Run with grapher upload: `etlr biodiversity/2025-06-28/cherry_blossom --grapher`
- Dry run (preview): `etlr biodiversity/2025-06-28/cherry_blossom --dry-run`
- Force re-run: `etlr biodiversity/2025-06-28/cherry_blossom --force`

### Key Options
- `--grapher/-g`: Upload datasets to grapher database (OWID staff only)
- `--dry-run`: Preview steps without running them
- `--force/-f`: Re-run steps even if up-to-date
- `--only/-o`: Run only selected step (no dependencies)
- `--downstream/-d`: Include downstream dependencies
- `--exact-match/-x`: Steps must exactly match arguments

## Updating ETL Steps

When asked to update a step or steps, use the `etl update` command:

### Update Workflow
1. **Check available options**: Run `etl update --help` first to understand available options
2. **Create update command**: Build the command based on user input and requirements
  - If user requests "direct", use `--direct-only` flag
3. **Dry run first**: Always run with `--dry-run` flag initially to preview changes
4. **CRITICAL: Get approval**: **ALWAYS** stop and ask user for approval before proceeding with actual update. **NEVER** execute `etl update` commands without explicit user approval.
5. **Execute update**: If approved, run the update command and continue with any follow-up tasks


## Git Workflow
Create PR first, then commit files:

1. **Create PR**: Use `etl pr` CLI (creates new branch)
2. **Check status**: `git status` to see modified/untracked files
3. **Add files**: `git add .` or `git add <specific-files>`
4. **Commit**: `git commit -m "Description of changes"`

Note: The `etl pr` creates a new branch but does NOT automatically commit files - you must commit manually after creating the PR.

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


## Important Development Notes

- Always use `geo.harmonize_countries()` for geographic data
- Follow the `PathFinder` pattern for step inputs/outputs
- Using `--force` is usually unnecessary - the step will be re-run if the code changes
- Test steps with `etl run --dry-run` before execution
- Use `make sync.catalog` to avoid rebuilding entire catalog locally
- Check `etl d version-tracker` before major changes
- VS Code extensions available: `make install-vscode-extensions`
- **ALWAYS run `make check` before committing** - formats code, fixes linting issues, and runs type checks
- SQL queries enclose in triple quotes for readability
- When running **etlr**, always use PREFER_DOWNLOAD=1 prefix (don't use it for **etls** command)
