---
icon: material/api
---
# Catalog API

The Catalog API makes it possible to access the dataframes our data scientists use to prepare the data for our public charts.

When using this API, you have access to the public catalog of data processed by our data team. The catalog indexes _tables_ of data, rather than datasets or individual indicators. To learn more, read about our [data model](../architecture/design/common-format.md).


!!! warning "Our ETL API is in beta"

    We currently provide Python, DuckDB, and Datasette access to our ETL catalog. Our hope is to extend this to other languages in the future. Please [report any issue](https://github.com/owid/etl) that you may find.

=== "Python"

    (see [example notebook](python.html))

{python_content}

=== "DuckDB"

    The ETL catalog is stored in two efficient formats—Feather and Parquet—making it easy to query data with tools like [:octicons-link-external-16: DuckDB](https://duckdb.org/). To access a table, use the URL pattern:

    ```
    https://catalog.ourworldindata.org/[channel]/[dataset]/[version]/[dataset]/[table].parquet
    ```

    For example, to fetch the first 100 rows of the Cherry Blossom dataset:

    ```sql
    SELECT
      *
    FROM
      'https://catalog.ourworldindata.org/garden/biodiversity/2025-04-07/cherry_blossom/cherry_blossom.parquet'
    LIMIT
      100;
    ```

=== "Datasette"

    [:octicons-link-external-16: Datasette](https://datasette.io/) lets you explore data in SQLite databases. By installing the [:fontawesome-brands-github: datasette-parquet plugin](https://github.com/owid/datasette-parquet), you can also query Parquet files via DuckDB. We maintain a public instance at https://datasette-public.owid.io where you can query the ETL catalog. You can run queries directly in the [:octicons-link-external-16: Datasette UI](https://datasette-public.owid.io/owid?sql=…) or export results as CSV by clicking the "CSV" button. For example:

    ```
    https://datasette-public.owid.io/owid.csv?sql=SELECT%20*%20FROM%20'https://catalog.ourworldindata.org/garden/biodiversity/2025-04-07/cherry_blossom/cherry_blossom.parquet'%20LIMIT%20100&_size=max
    ```
