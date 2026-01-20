# owid-catalog: Data APIs

The Data API provides unified access to OWID's published data through a simple client interface.

!!! tip "[Jump to the reference :octicons-arrow-down-16:](#api-reference)"

!!! warning "This library is under active development"

    This documentation reflects the latest version, v1.0.0rc2, which is currently in Release Candidate stage. To install it run `pip install owid-catalog==v1.0.0rc2`.

    We are continuously working to enhance its functionality and performance, and expect to release the stable v1.0.0 soon.

## Quick Reference
The API library is centered around the `Client` class, which provides quick access to different data APIs: `IndicatorsAPI`, `TablesAPI`, and `ChartsAPI`. Each API provides methods `search()` and `fetch()` for discovering and retrieving data, respectively.

For example to fetch a table by its path:

```python
from owid.catalog import Client

client = Client()
tb = client.tables.fetch("garden/un/2024-07-12/un_wpp/population")
```

For convenience, the library provides functions for the most common use cases:

```python
from owid.catalog import search, fetch
# Search and fetch
results = search("population")
tb = results[0].fetch()
# Direct fetch
tb = fetch("garden/un/2024-07-12/un_wpp/population")
```

### Lazy Loading

All `fetch()` methods return `Table`-like objects, which resemble pandas.DataFrame with the addition of metadata attributes that describe the data.
```python
tb = client.charts.fetch("life-expectancy")
tb.metadata  # Available immediately
tb["life_expectancy_0"].metadata  # Column metadata available
```

Optionally, you can defer data loading until it's actually needed, by using the `load_data=False` parameter in `fetch()` methods.


### Path Formats

Different APIs use different path conventions:

- **Charts**: `"life-expectancy"` (simple slug)
- **Tables**: `"garden/un/2024-07-12/un_wpp/population"` (channel/namespace/version/dataset/table)
- **Indicators**: `"garden/un/2024-07-12/un_wpp/population#population"` (table path + #column)

## API Reference

::: owid.catalog.api.quick
    options:
      heading_level: 3
      show_root_heading: true
      members_order: source
      filters:
        - "!^_"

::: owid.catalog.api.Client
    options:
      heading_level: 3
      show_root_heading: true
      members_order: source
      filters:
        - "!^_"


::: owid.catalog.api.charts.ChartsAPI
    options:
      heading_level: 3
      show_root_heading: true
      members_order: source
      filters:
        - "!^_"

::: owid.catalog.api.tables.TablesAPI
    options:
      heading_level: 3
      show_root_heading: true
      members_order: source
      filters:
        - "!^_"

::: owid.catalog.api.indicators.IndicatorsAPI
    options:
      heading_level: 3
      show_root_heading: true
      members_order: source
      filters:
        - "!^_"


### API result types

Result objects returned by `fetch()` and `search()` methods.

::: owid.catalog.api.models.ResponseSet
    options:
      heading_level: 4
      show_root_heading: true
      filters:
        - "!^_"
        - "!model_post_init"

::: owid.catalog.api.charts.ChartResult
    options:
      heading_level: 4
      show_root_heading: true
      members_order: source
      filters:
        - "!^_"

::: owid.catalog.api.indicators.IndicatorResult
    options:
      heading_level: 4
      show_root_heading: true
      members_order: source
      filters:
        - "!^_"
        - "!model_post_init"

::: owid.catalog.api.tables.TableResult
    options:
      heading_level: 4
      show_root_heading: true
      members_order: source
      filters:
        - "!^_"

