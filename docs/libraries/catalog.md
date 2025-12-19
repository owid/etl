# Catalog Library

The `owid-catalog` library is the foundation of Our World in Data's data management system. It provides enhanced data structures built on top of pandas that include rich metadata support.

You can install it via pip:

```bash
pip install owid-catalog
```

## Client API
The unified client for accessing OWID data through various APIs.

::: owid.catalog.api.Client
    options:
      heading_level: 3
      show_root_heading: true

::: owid.catalog.api.charts
    options:
      heading_level: 3
      show_root_heading: true
      members_order: source


::: owid.catalog.api.indicators
    options:
      heading_level: 3
      show_root_heading: true


::: owid.catalog.api.tables
    options:
      heading_level: 3
      show_root_heading: true

::: owid.catalog.api.models
    options:
      heading_level: 3
      show_root_heading: true

<!-- ::: owid.catalog.api.models.IndicatorResult
    options:
      heading_level: 3
      show_root_heading: true

::: owid.catalog.api.models.TableResult
    options:
      heading_level: 3
      show_root_heading: true

::: owid.catalog.api.models.PageSearchResult
    options:
      heading_level: 3
      show_root_heading: true

::: owid.catalog.api.models.ResultSet
    options:
      heading_level: 3
      show_root_heading: true -->

<!-- Note: The old DatasetsAPI has been renamed to TablesAPI for better clarity.
For backwards compatibility, client.datasets still works as an alias to client.tables.
The client/ directory has been renamed to api/ for clarity, though owid.catalog.client
imports still work via backwards compatibility. -->

## Legacy API
<!-- LEGACY BELOW  -->
<!-- Catalog -->
::: owid.catalog.catalogs
    options:
      heading_level: 3
      filters: ["!^_", "!^Catalog"]

## Data processing
<!-- Processing -->

::: owid.catalog.processing
    options:
      heading_level: 3
      filters: ["!^_"]
      members_order: alphabetical

<!-- Dataset -->
::: owid.catalog.datasets
    options:
      heading_level: 3
      filters: ["!^_"]
      members_order: alphabetical

<!-- Table -->
::: owid.catalog.tables
    options:
      heading_level: 3
      filters: ["!^_"]
      members_order: alphabetical

<!-- Variable -->
::: owid.catalog.variables
    options:
      heading_level: 3
      filters: ["!^_"]
      members_order: alphabetical

<!-- Metadata -->
::: owid.catalog.meta
    options:
      heading_level: 3
      filters: ["!^_"]
      members_order: alphabetical

<!-- Utils -->
::: owid.catalog.utils
    options:
      heading_level: 3
      filters: ["!^_"]
      members_order: alphabetical

::: owid.catalog.s3_utils
    options:
      heading_level: 3
      filters: ["!^_"]
      members_order: alphabetical
