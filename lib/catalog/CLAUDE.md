# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This is **owid-catalog** - a Python library (version 0.4.3) that provides core data types for Our World in Data's data catalog system. It's published as a PyPI package and serves as the foundation for OWID's ETL system.

The library provides pandas-enhanced data structures with rich metadata support:
- **Dataset**: Container for multiple tables with shared metadata
- **Table**: pandas.DataFrame subclass with column-level metadata
- **Variable**: pandas.Series subclass with variable-specific metadata
- **RemoteCatalog/LocalCatalog**: APIs for querying and loading datasets

This library is part of the larger `etl` repository under `lib/catalog/` and is installed as an editable package called `owid-catalog`.

## Development Commands

### Virtual Environment

**CRITICAL:** This project uses a Python virtual environment (`.venv/`). All Python commands must use the virtual environment binaries:

```bash
# CORRECT - Use .venv binaries
.venv/bin/python script.py
.venv/bin/pytest tests/

# WRONG - Global commands will fail
python script.py        # ❌
pytest tests/          # ❌
```

### Package Management

**Always use `uv` package manager instead of `pip`:**

```bash
uv add package_name     # Add a new package
uv remove package_name  # Remove a package
uv sync                 # Sync dependencies
```

### Common Development Tasks

```bash
# Run all tests and checks
make test               # Runs format check, linting, type checking, and unit tests

# Watch for changes and re-run tests
make watch

# Format, lint, and typecheck changed files only
make check

# Individual checks
make format             # Format code with ruff
make lint               # Lint and auto-fix with ruff
make check-linting      # Check linting without fixing
make check-formatting   # Check formatting without fixing
make check-typing       # Type check with pyright
make unittest           # Run unit tests only
make coverage           # Run tests with coverage report
```

### Running Tests

```bash
# Run all tests
.venv/bin/pytest tests/

# Run specific test file
.venv/bin/pytest tests/test_tables.py

# Run specific test
.venv/bin/pytest tests/test_tables.py::test_create

# Run with coverage
.venv/bin/pytest --cov=owid --cov-report=term-missing tests/
```

## Architecture

### Core Data Structures

The library follows a layered metadata architecture:

```
Dataset (folder with index.json)
├── metadata: DatasetMeta (title, description, sources, licenses)
└── Tables (feather/parquet/csv files)
    ├── metadata: TableMeta (table-level metadata)
    └── Variables (columns)
        └── metadata: VariableMeta (unit, description, sources, etc.)
```

### Key Modules

- **`catalogs.py`**: Catalog querying interface (LocalCatalog, RemoteCatalog)
- **`datasets.py`**: Dataset container and serialization logic
- **`tables.py`**: Table class with metadata-aware operations
- **`variables.py`**: Variable (Series) class with metadata
- **`meta.py`**: All metadata dataclasses (DatasetMeta, TableMeta, VariableMeta, Source, Origin, License)
- **`processing.py`**: Pandas-like functions that propagate metadata (concat, merge, melt, pivot)
- **`processing_log.py`**: Processing log for tracking data transformations
- **`yaml_metadata.py`**: YAML metadata file handling with dynamic templates
- **`charts.py`**: API for fetching chart data from ourworldindata.org
- **`utils.py`**: Utility functions for metadata handling

### Metadata System

Metadata propagates through operations using pandas' `_metadata` attribute:

1. **Table operations** (slicing, filtering) preserve both table and column metadata
2. **Column operations** propagate VariableMeta to resulting columns
3. **Processing functions** (in `processing.py`) explicitly handle metadata propagation
4. **`@keep_metadata` decorator** preserves metadata when wrapping pandas functions

### File Formats

Supports multiple formats with automatic detection:
- **feather** (default/preferred): Fast binary format with schema
- **parquet**: Compressed columnar format
- **csv**: Human-readable text format

Metadata stored separately in JSON sidecar files (`{tablename}.meta.json`).

## Important Development Patterns

### Working with Tables

```python
from owid.catalog import Table, TableMeta, VariableMeta

# Create table with metadata
meta = TableMeta(short_name="gdp", title="GDP by country")
tb = Table(df, metadata=meta)

# Set column metadata
tb["gdp"] = tb["gdp"].replace_metadata(unit="current US$", short_unit="$")

# Format table (underscore column names, set index, sort)
tb = tb.format(["country", "year"])
```

### Metadata Propagation

Use `processing.py` functions instead of pandas directly to preserve metadata:

```python
from owid.catalog import processing as pr

# Use these instead of pandas functions
tb_merged = pr.merge(tb1, tb2, on="country")
tb_concat = pr.concat([tb1, tb2])
tb_melted = pr.melt(tb, id_vars=["country"])
```

### Custom Functions with Metadata

Use `@keep_metadata` decorator for custom operations:

```python
from owid.catalog.tables import keep_metadata

@keep_metadata
def my_operation(tb: Table) -> Table:
    # Your pandas operations here
    return tb.groupby("country").sum()
```

### Reading Data Files

```python
from owid.catalog import processing as pr

# Read with automatic metadata propagation
tb = pr.read_csv("data.csv")
tb = pr.read_excel("data.xlsx", sheet_name="Sheet1")
tb = pr.read_feather("data.feather")
tb = pr.read_parquet("data.parquet")

# R data files
tb = pr.read_rds("data.rds")
tb = pr.read_rda("data.rda")
```

## Testing Guidelines

- Test files follow `test_*.py` naming convention
- Use fixtures from `conftest.py` for common test setup
- Mock data generation utilities in `tests/mocking.py`
- Test both functionality and metadata preservation
- Run type checking with pyright - must pass before committing

## Dependencies

### Core Dependencies
- **pandas** (>=2.2.3): Base DataFrame functionality
- **pyarrow** (>=10.0.1): Feather/Parquet support
- **structlog**: Structured logging
- **PyYAML** + **dynamic-yaml**: Metadata file handling
- **dataclasses-json**: Metadata serialization

### Internal Dependencies
- **owid-datautils**: Data processing utilities (from `lib/datautils/`)
- **owid-repack**: Data compression and type optimization (from `lib/repack/`)

## Build and Release

This package is built with `hatchling` and published to PyPI as `owid-catalog`.

```bash
# Build package
uv build

# Package will be in dist/ directory
```

## Configuration Files

- **pyproject.toml**: Package dependencies, tool configuration (ruff, pyright, hatch)
- **Makefile**: Development command shortcuts (includes `../../default.mk`)
- **.pre-commit-config.yaml**: Pre-commit hooks configuration

## Important Notes

- Python 3.10+ required (supports 3.10, 3.11, 3.12, 3.13)
- This library is experimental - APIs may change
- Extends ruff configuration from parent `../../pyproject.toml`
- Always run `make check` before committing changes
- Type checking is mandatory - must pass pyright checks
