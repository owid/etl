# Experimental Features for owid-catalog API

This module contains experimental features that provide ergonomic shortcuts for common data access patterns. These features may change or be removed in future versions.

## ⚠️ Stability Notice

All features in this module are marked as **experimental** and follow these rules:

- **API may change** without notice in minor versions
- **Features may be removed** if they don't prove useful
- **Successful features will graduate** to the main API (with the `experimental_` prefix removed)
- **Use at your own risk** in production code

## Features

### 1. Quick Access Functions

#### `quick(name, kind="table", **filters)` - Smart search and download

One-liner to search and download data with fuzzy matching:

```python
from owid.catalog.api.experimental import quick

# Tables (default) - returns Table
table = quick("population")
table = quick("gdp", namespace="worldbank", latest=True)

# Indicators - returns single-column Table
indicator = quick("population", kind="indicator")

# Charts - returns DataFrame
chart_data = quick("life-expectancy", kind="chart")
```

**Features:**
- Fuzzy matching by default (typo-tolerant)
- Automatically returns latest version
- Supports all three APIs (tables, indicators, charts)
- Consistent return types (Tables everywhere except charts)

#### `get(path)` - Direct access with auto-detection

Direct path access that auto-detects what you're requesting:

```python
from owid.catalog.api.experimental import get

# Tables - regular catalog paths
table = get("garden/un/2024-07-12/un_wpp/population")

# Indicators - paths with # fragment
indicator_table = get("garden/un/2024-07-12/un_wpp/population#population")

# Charts - use chart: prefix
chart_data = get("chart:life-expectancy")
```

**Features:**
- Auto-detects type from path format using `CatalogPath`
- Validates prefixes (only `chart:` is allowed)
- Indicators return single-column Tables (not Variables)
- Clean error messages for invalid paths

### 2. Preview Methods on TableResult

Preview data before downloading full datasets:

```python
from owid.catalog import Client

client = Client()
result = client.tables.fetch("garden/un/2024-07-12/un_wpp/population")

# Preview first 10 rows (lightweight)
preview = result.experimental_preview(n=10)
print(preview.head())

# Get summary without loading data
print(result.experimental_summary())
# Output:
# Table: population
# Dataset: un_wpp (garden/un/2024-07-12)
# Dimensions: ['country', 'year']
# Columns: 3
# Dtypes: int64(2), float64(1)
# Memory: Unknown (call .data to load and measure)

# Pretty-print metadata
print(result.experimental_describe_metadata())
# Output:
# === Table Metadata ===
# Title: Population by country and year
# Description: Total population by country...
# Sources: UN World Population Prospects (2024)
# ...
```

**Note:** `experimental_preview()` currently loads the full table then returns first N rows. Future optimization may add true partial loading.

### 3. Bulk Download on ResponseSet

Download multiple tables in parallel:

```python
from owid.catalog import Client

client = Client()
results = client.tables.search("worldbank/wdi", match="contains")

# Download all results in parallel with progress bar
tables = results.experimental_download_all(
    parallel=True,
    max_workers=4,
    show_progress=True
)

# Check results
successful = {k: v for k, v in tables.items() if not isinstance(v, Exception)}
failed = {k: v for k, v in tables.items() if isinstance(v, Exception)}

print(f"Downloaded {len(successful)} tables, {len(failed)} failed")
```

**Features:**
- Parallel downloads using ThreadPoolExecutor
- Progress bars (requires `tqdm`)
- Graceful error handling (returns exceptions, doesn't fail entire batch)
- Returns `dict[str, Table | Exception]`

## Installation

The experimental module is included with `owid-catalog`:

```bash
uv add owid-catalog
```

For progress bars in bulk downloads:

```bash
uv add tqdm
```

## Usage Patterns

### Pattern 1: Quick data exploration

```python
from owid.catalog.api.experimental import quick

# Get latest population data quickly
table = quick("population")
print(table.head())

# Fuzzy search with filters
table = quick("populaton", namespace="un", match="fuzzy")  # Typo-tolerant!
```

### Pattern 2: Preview before loading

```python
from owid.catalog import Client

client = Client()
results = client.tables.search("population")

# Check metadata before downloading
for result in results[:5]:
    print(result.experimental_summary())

# Download only the one you want
table = results[0].data
```

### Pattern 3: Bulk processing

```python
from owid.catalog import Client

client = Client()
results = client.tables.search("worldbank", match="contains")

# Download top 10 in parallel
top_10 = results.first(10)
tables = top_10.experimental_download_all(parallel=True, max_workers=4)

# Process all successful downloads
for name, table in tables.items():
    if not isinstance(table, Exception):
        print(f"Processing {name}: {table.shape}")
```

### Pattern 4: Charts and indicators

```python
from owid.catalog.api.experimental import quick, get

# Quick chart access
chart = quick("life-expectancy", kind="chart")  # Returns DataFrame

# Or with explicit path
chart = get("chart:life-expectancy")

# Indicators return single-column Tables
indicator = quick("gdp", kind="indicator")  # Table with one column
indicator = get("garden/.../dataset/table#variable")  # Also a Table
```

## Migration Path

When experimental features graduate to stable API:

1. **Prefix removal**: `experimental_preview()` → `preview()`
2. **Import path change**: May move from `experimental` to main module
3. **Deprecation warnings**: Old experimental names will warn before removal
4. **Documentation update**: Will be documented in main API reference

### How to prepare for graduation

```python
# Current experimental usage
from owid.catalog.api.experimental import quick
table = quick("population")

# When graduated (hypothetical future)
from owid.catalog.api import quick  # Main import
table = quick("population")  # Same API

# Or stay on main API to avoid changes
from owid.catalog import Client
client = Client()
results = client.tables.search("population", match="fuzzy")
table = results.latest().data  # Explicit but stable
```

## Feedback

Experimental features are shaped by user feedback! Please:

- **Report bugs**: [GitHub Issues](https://github.com/owid/etl/issues)
- **Request features**: Describe your use case
- **Share usage patterns**: Help us understand what works

## Changelog

### v0.1.0-experimental (2024)

- ✨ Initial release with `quick()` and `get()` functions
- ✨ Added `experimental_preview()`, `experimental_summary()`, `experimental_describe_metadata()` to TableResult
- ✨ Added `experimental_download_all()` to ResponseSet
- 🎯 Unified API across tables, indicators, and charts
- 🔧 Auto-detection in `get()` using CatalogPath
- ⚡ Parallel bulk downloads with progress bars

## Technical Notes

### Return Type Consistency

- **Tables**: All table operations return `Table` objects
- **Indicators**: Return single-column `Table` objects (via `.to_frame()`)
- **Charts**: Return `DataFrame` objects (already processed data)

This design ensures you work with rich metadata wherever possible (Tables have `.metadata`), while charts remain simple DataFrames.

### Path Format Detection

`get()` uses these rules to auto-detect data type:

1. **Prefix check**: If path contains `:` before `/`, validate prefix (only `chart:` allowed)
2. **Fragment check**: If path contains `#`, it's an indicator (parse with CatalogPath)
3. **Default**: Regular catalog table path

### Parallel Download Safety

`experimental_download_all()` uses ThreadPoolExecutor (not multiprocessing):
- ✅ Safe for I/O-bound downloads
- ✅ Respects GIL (Global Interpreter Lock)
- ✅ Works across platforms
- ⚠️  Limited by network bandwidth, not CPU

### Performance Expectations

- **Quick access**: +0-100ms overhead vs. direct API calls
- **Bulk downloads**: 3-4x speedup with 4 workers (network-dependent)
- **Preview**: Currently loads full table (no speedup), future optimization planned
- **Summary**: ~10-50ms (only loads metadata, no data rows)
