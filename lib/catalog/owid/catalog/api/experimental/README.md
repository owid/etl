# Experimental Features for owid-catalog API

This module contains experimental features that provide ergonomic shortcuts for common data access patterns. These features may change or be removed in future versions.

## ⚠️ Stability Notice

All features in this module are marked as **experimental** and follow these rules:

- **API may change** without notice in minor versions
- **Features may be removed** if they don't prove useful
- **Successful features will graduate** to the main API (with the `experimental_` prefix removed)
- **Use at your own risk** in production code

## Features

### 1. Data Discovery and Retrieval

The experimental API separates discovery (browsing) from download (retrieval) for better cost awareness:

- **`show()`** - Browse available data without downloading (lightweight exploration)
- **`get()`** - Download specific data by path (explicit retrieval)

This design ensures you know exactly what you're downloading before spending bandwidth on large datasets.

#### `show(name, kind="table", **filters)` - Browse available data

Display what's available in the catalog without downloading:

```python
from owid.catalog.api.experimental import show

# Tables (default) - shows matching paths
show("population")

# With filters
show("wdi", namespace="worldbank_wdi")

# Indicators - semantic search
show("gdp", kind="indicator")

# Charts - search published charts
show("life-expectancy", kind="chart")

# Exact match (no fuzzy tolerance)
show("population", match="exact")
```

**Features:**
- Display-only (no data download)
- Fuzzy matching by default (typo-tolerant)
- Supports tables, indicators, and charts via `kind=` parameter
- Shows full catalog paths for use with `get()`
- Helpful error messages and tips

**Output example:**
```
Showing tables matching 'population' (fuzzy):
Found 15 results.

garden/un/2024-07-12/un_wpp/population
garden/un/2023-05-15/un_wpp/population
garden/worldbank/2024-01-10/wdi/population_total
...

Tip: Copy a path and use get(path) to download
```

#### `get(path)` - Download data by path

Direct download with auto-detection of data type:

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

**Typical workflow:**
```python
# 1. Browse to find what you need
show("population")

# 2. Copy a path from the results
# 3. Download it
table = get("garden/un/2024-07-12/un_wpp/population")
```

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

## Installation

The experimental module is included with `owid-catalog`:

```bash
uv add owid-catalog
```

## Usage Patterns

### Pattern 1: Data discovery workflow

```python
from owid.catalog.api.experimental import show, get

# Browse available data (no download)
show("population")

# Copy a path from the results and download
table = get("garden/un/2024-07-12/un_wpp/population")
print(table.head())

# Fuzzy search with filters (typo-tolerant!)
show("populaton", namespace="un", match="fuzzy")
```

### Pattern 2: Indicators and charts

```python
from owid.catalog.api.experimental import show, get

# Browse indicators using semantic search
show("gdp per capita", kind="indicator")

# Download specific indicator
indicator_table = get("garden/un/2024-07-12/un_wpp/population#population")

# Browse charts
show("life-expectancy", kind="chart")

# Download chart data
chart_data = get("chart:life-expectancy")
```

### Pattern 3: Preview before downloading

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

### Pattern 4: Precise filtering

```python
from owid.catalog.api.experimental import show, get

# Use filters to narrow results
show("wdi", namespace="worldbank_wdi", version="2024-01-10")

# Exact match (no fuzzy tolerance)
show("population", match="exact")

# Then download the specific version you need
table = get("garden/worldbank/2024-01-10/wdi/population_total")
```

## Migration Path

### Breaking Changes in v0.2.0

**⚠️ `quick()` has been removed** and replaced with `show()` + `get()` workflow.

**Old code (v0.1.0):**
```python
from owid.catalog.api.experimental import quick
table = quick("population")  # One-liner search + download
```

**New code (v0.2.0):**
```python
from owid.catalog.api.experimental import show, get

# Two-step workflow: browse then download
show("population")  # Display available data
table = get("garden/un/2024-07-12/un_wpp/population")  # Explicit download
```

**Why the change?**
- Clearer separation between discovery (lightweight) and download (expensive)
- Users explicitly choose what to download after seeing options
- Better cost awareness for large datasets

### When experimental features graduate to stable API

1. **Prefix removal**: `experimental_preview()` → `preview()`
2. **Import path change**: May move from `experimental` to main module
3. **Deprecation warnings**: Old experimental names will warn before removal
4. **Documentation update**: Will be documented in main API reference

### How to prepare for graduation

```python
# Current experimental usage
from owid.catalog.api.experimental import show, get
show("population")
table = get("garden/un/2024-07-12/un_wpp/population")

# When graduated (hypothetical future)
from owid.catalog.api import show, get  # Main import
show("population")
table = get("garden/un/2024-07-12/un_wpp/population")  # Same API

# Or stay on main API to avoid changes
from owid.catalog import Client
client = Client()
results = client.tables.search("population", match="fuzzy")
table = results[0].data  # Explicit but stable
```

## Feedback

Experimental features are shaped by user feedback! Please:

- **Report bugs**: [GitHub Issues](https://github.com/owid/etl/issues)
- **Request features**: Describe your use case
- **Share usage patterns**: Help us understand what works

## Changelog

### v0.2.0-experimental (2024)

- 🔥 **BREAKING**: Removed `quick()` function
- ✨ Added `show()` function for data discovery without downloading
- 💡 Separated discovery (show) from download (get) for better cost awareness
- 📝 Updated all documentation and examples
- 🎯 Kept `get()` unchanged for precise downloads

**Migration from v0.1.0:**
- Replace `quick("name")` with `show("name")` followed by `get(path)`
- See Migration Path section for details

### v0.1.0-experimental (2024)

- ✨ Initial release with `quick()` and `get()` functions
- ✨ Added `experimental_preview()`, `experimental_summary()`, `experimental_describe_metadata()` to TableResult
- ✨ Removed `experimental_download_all()` from ResponseSet (had type issues)
- 🎯 Unified API across tables, indicators, and charts
- 🔧 Auto-detection in `get()` using CatalogPath

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

### Performance Expectations

- **show()**: ~100-500ms for catalog search (no data download)
- **get()**: Variable depending on dataset size (downloads full table/indicator)
- **Preview**: Currently loads full table (no speedup), future optimization planned
- **Summary**: ~10-50ms (only loads metadata, no data rows)
