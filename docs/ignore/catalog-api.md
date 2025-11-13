
# ETL catalog API

The ETL catalog API makes it possible to access the dataframes our data scientists use to prepare the data for our public charts.

When using this API, you have access to the public catalog of data processed by our data team. The catalog indexes _tables_ of data, rather than datasets or individual indicators. To learn more, read about our [data model](../architecture/design/common-format.md).

At the moment, this API only supports [Python](python.ipynb).


!!! warning "Our ETL API is in beta"

    We currently only provide a python API for our ETL catalog. Our hope is to extend this to other languages in the future. Please [report any issue](https://github.com/owid/etl) that you may find.

=== "Python"

    (see [example notebook](python.ipynb))
