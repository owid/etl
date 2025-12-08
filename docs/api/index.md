---
tags:
  - API
icon: material/api
---

# Data APIs

Our World in Data offers a curated collection of charts on our website, datasets and indicators via our ETL catalog, etc.

To this end, we are working to consolidate our different data APIs and provide clear documentation for each of them.

For now, we provide three main data APIs:

1. [Charts API](chart-api): Primary API for accessing chart data and metadata.
2. [Search API](search-api): API for searching charts and datasets. This is equivalent to using the [search box](https://ourworldindata.org/search/) on our website.
3. [Indicators API](semantic-search-api): API for searching indicators in our catalog. It relies on semantic similarity to find relevant indicators based on a text query.

We also maintain a larger [data catalog](catalog-api) within our ETL system, where we fetch, process, and prepare data used for our charts. This catalog contains significantly more data, though its level of curation varies across different sections. It also has an API, albeit one that is currently less accessible; at this time, we only offer a Python client for interacting with it.


!!! warning

    These APIs are under active development. We are continuously working to improve their functionality, performance, and documentation. If you have any questions or feedback, please don't hesitate to reach out to us at info@ourworldindata.org.
