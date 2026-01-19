# owid-catalog: Data Structures and Processing

Enhanced pandas data structures with rich metadata support for OWID's data processing pipelines.

## Quick Reference

```python
from owid.catalog import Dataset, Table, Variable
from owid.catalog import processing as pr

# Create a table with metadata
tb = Table(df, metadata={"short_name": "population"})
```

### Metadata Hierarchy

```
Dataset
├── metadata: DatasetMeta (sources, licenses, title)
└── Tables
    ├── metadata: TableMeta (table-level info)
    └── Variables (columns)
        └── metadata: VariableMeta (unit, description, sources)
```

### Metadata Propagation
As the table is processed, metadata is preserved and propagated to resulting tables and variables.

```python
# Slicing
tb_filtered = tb[tb["year"] > 2000]  # Keeps metadata
# Filtering
tb_loc = tb.loc[tb["country"] == "USA"]  # Keeps metadata
# Sorting
tb_sorted = tb.sort_values("gdp_per_capita")  # Keeps metadata
# Column operations
tb["gdp_per_capita_usd"] = tb["gdp_per_capita"] * 2

# Merging
tb_merged = pr.merge(tb1, tb2, on="country")  # Merges metadata
# Concatenating
tb_concat = pr.concat([tb1, tb2])  # Combines metadata
# Pivoting
tb_pivot = pr.pivot(tb, index="year", ...)  # Adjusts metadata
# Melting
tb_melted = pr.melt(tb, ...)
```

### File Formats

Tables support multiple formats with automatic detection: feather, parquet, and CSV. Metadata is stored separately in `.meta.json` files.

## Reference

Metadata-aware alternatives to pandas functions.

::: owid.catalog.core.processing
    options:
      heading_level: 3
      filters: ["!^_"]
      members_order: alphabetical

Container for multiple tables with shared metadata.

::: owid.catalog.core.datasets
    options:
      heading_level: 3
      filters: ["!^_"]
      members_order: alphabetical


pandas DataFrame with column-level metadata.

::: owid.catalog.core.tables
    options:
      heading_level: 3
      filters: ["!^_"]
      members_order: alphabetical


pandas Series with metadata.

::: owid.catalog.core.variables
    options:
      heading_level: 3
      filters: ["!^_"]
      members_order: alphabetical


::: owid.catalog.core.meta
    options:
      heading_level: 3
      filters: ["!^_"]
      members_order: alphabetical


::: owid.catalog.core.utils
    options:
      heading_level: 3
      filters: ["!^_"]
      members_order: alphabetical

::: owid.catalog.core.s3_utils
    options:
      heading_level: 3
      filters: ["!^_"]
      members_order: alphabetical
