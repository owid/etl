# Catalog Library

The `owid-catalog` library is the foundation of Our World in Data's data management system. It serves two main purposes:

1. **[Data API](api.md)**: Access OWID data through unified client interfaces. We provide a reference for the most important objects and methods.
2. **[Data Structures](structures.md)**: Enhanced pandas DataFrames with rich metadata support

## Installation

```bash
pip install owid-catalog
```

## Quick Start

### Accessing Data via API

```python
from owid.catalog import Client

client = Client()

# Get chart data
df = client.charts.get_data("life-expectancy")

# Search for indicators
results = client.indicators.search("renewable energy")
variable = results[0].data

# Query catalog tables
tables = client.tables.search(table="population", namespace="un")
tb = tables[0].data
```

### Working with Data Structures

```python
from owid.catalog import Table

# Tables are pandas DataFrames with metadata
tb = Table(df)
tb.metadata.short_name = "population"

# Metadata propagates through operations
tb_filtered = tb[tb["year"] > 2000]  # Keeps metadata
tb_grouped = tb.groupby("country").sum()  # Preserves metadata
```
