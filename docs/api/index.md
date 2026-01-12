---
tags:
  - API
icon: material/api
---

# APIs

!!! tip "For convenience, we provide a Python library that wraps all our APIs: [owid-data](../libraries/catalog/api/)."


Our World in Data offers a curated collection of charts on our website, datasets and indicators via our ETL catalog, etc. To this end, we are working to consolidate our different data APIs and provide clear documentation for each of them.

For now, we provide four main data APIs:

1. [Charts API](chart-api): Primary API for accessing chart data and metadata.
2. [Tables API](catalog-api): API for searching tables (similar to a dataset) in our catalog. Use it to access the data coming straight from ETL.
3. [Indicators API](semantic-search-api): API for searching indicators in our catalog. It relies on semantic similarity to find relevant indicators based on a text query.
4. [Search API](search-api): API for searching charts and datasets. This is equivalent to using the website's [search box](https://ourworldindata.org/search/).


!!! warning

    These APIs are under active development. We are continuously working to improve their functionality, performance, and documentation. If you have any questions or feedback, please don't hesitate to [reach out to us](mailto:info@ourworldindata.org).
