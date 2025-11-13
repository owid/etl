# Querying data with DuckDB

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

# Querying data with Datasette

[:octicons-link-external-16: Datasette](https://datasette.io/) lets you explore data in SQLite databases. By installing the [:fontawesome-brands-github: datasette-parquet plugin](https://github.com/owid/datasette-parquet), you can also query Parquet files via DuckDB. We maintain a public instance at https://datasette-public.owid.io where you can query the ETL catalog. You can run queries directly in the [:octicons-link-external-16: Datasette UI](https://datasette-public.owid.io/owid?sql=…) or export results as CSV by clicking the “CSV” button. For example:

```
https://datasette-public.owid.io/owid.csv?sql=SELECT%20*%20FROM%20'https://catalog.ourworldindata.org/garden/biodiversity/2025-04-07/cherry_blossom/cherry_blossom.parquet'%20LIMIT%20100&_size=max
```
