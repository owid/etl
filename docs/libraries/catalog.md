# Catalog Library

The `owid-catalog` library is the foundation of Our World in Data's data management system. It provides enhanced data structures built on top of pandas that include rich metadata support.

You can install it via pip:

```bash
pip install owid-catalog
```

<!-- Catalog -->
::: owid.catalog.catalogs
    options:
      heading_level: 2
      filters: ["!^_", "!^Catalog"]

<!-- Processing -->

::: owid.catalog.processing
    options:
      heading_level: 2
      filters: ["!^_"]
      members_order: alphabetical

<!-- Dataset -->
::: owid.catalog.datasets
    options:
      heading_level: 2
      filters: ["!^_"]
      members_order: alphabetical

<!-- Table -->
::: owid.catalog.tables
    options:
      heading_level: 2
      filters: ["!^_"]
      members_order: alphabetical

<!-- Variable -->
::: owid.catalog.variables
    options:
      heading_level: 2
      filters: ["!^_"]
      members_order: alphabetical

<!-- Metadata -->
::: owid.catalog.meta
    options:
      heading_level: 2
      filters: ["!^_"]
      members_order: alphabetical

<!-- Utils -->
::: owid.catalog.utils
    options:
      heading_level: 2
      filters: ["!^_"]
      members_order: alphabetical

::: owid.catalog.s3_utils
    options:
      heading_level: 2
      filters: ["!^_"]
      members_order: alphabetical
