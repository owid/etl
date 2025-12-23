# owid-catalog: Data APIs

The Data API provides unified access to OWID's published data through a simple client interface.

!!! tip "[Jump to the reference :octicons-arrow-down-16:](#api-reference)"

!!! warning "This library is under active development"

    This documentation reflects the latest version, v1.0.0rc1, which is currently in Release Candidate stage. To install it run `pip install owid-catalog==v1.0.0rc1`.

    We are continuously working to enhance its functionality and performance, and expect to release the stable v1.0.0 soon.

## Quick Reference

```python
from owid.catalog import Client

client = Client()
table = client.tables.get_data("garden/un/2024-07-12/un_wpp/population")
```


There are three main APIs available via the `Client` class: `IndicatorsAPI`, `TablesAPI`, and `ChartsAPI`. All of them provide `search()`, `fetch()`, and `get_data()` methods.


### Lazy Loading

All `fetch()` methods return result objects with a `.data` property that loads data on first access:

```python
# Metadata only - fast
chart = client.charts.fetch("life-expectancy")
print(chart.title)

# First access downloads data
df = chart.data

# Subsequent access uses cache
df2 = chart.data  # Instant

# Or preload immediately
chart = client.charts.fetch("life-expectancy", load_data=True)
```

### Path Formats

Different APIs use different path conventions:

- **Charts**: `"life-expectancy"` (simple slug)
- **Tables**: `"garden/un/2024-07-12/un_wpp/population"` (channel/namespace/version/dataset/table)
- **Indicators**: `"garden/un/2024-07-12/un_wpp/population#population"` (table path + #column)

## API Reference


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

::: owid.catalog.api.models.ChartResult
    options:
      heading_level: 4
      show_root_heading: true
      members_order: source
      filters:
        - "!^_"

::: owid.catalog.api.models.IndicatorResult
    options:
      heading_level: 4
      show_root_heading: true
      members_order: source
      filters:
        - "!^_"

::: owid.catalog.api.models.TableResult
    options:
      heading_level: 4
      show_root_heading: true
      members_order: source
      filters:
        - "!^_"
