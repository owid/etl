# Data APIs

Our World in Data offers a curated collection of charts on our website, with data and metadata accessible via our [Public Chart API](chart-api.md). The API provides data in CSV and JSON formats over HTTP, enabling seamless integration with any programming language. It is specifically designed to support the creation of interactive charts.

We also maintain a larger [data catalog](catalog-api.md) within our ETL system, where we fetch, process, and prepare data used for our charts. This catalog contains significantly more data, though its level of curation varies across different sections. It also has an API, albeit one that is currently less accessible; at this time, we only offer a Python client for interacting with it.

!!! note
    Unlike the Public Chart API, which exclusively provides time series data by time (typically year) and entity (typically country), our ETL catalog includes larger datasets with additional dimensions, such as age group and gender breakdowns.

!!! tip "Learn more on how to use our APIs by [examples](example-usage.md)"
