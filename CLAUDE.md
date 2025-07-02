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
- Use content-based checksums - avoid `--force` unless necessary
- Test steps with `etl run --dry-run` before execution
- Use `make sync.catalog` to avoid rebuilding entire catalog locally
- Check `etl d version-tracker` before major changes
- VS Code extensions available: `make install-vscode-extensions`
