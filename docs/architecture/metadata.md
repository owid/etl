# Metadata

!!! Warning

    Our metadata formats are still in flux, and are likely to change substantially over the coming year.

A key principal of our data pipeline is that the data that flows through needs to be properly described and attributed to the correct source. This is done using a metadata system that is built into the pipeline.

## Snapshot metadata 

Snapshots are the raw data provided by upstream data providers. At minimum, they must capture:

- The URL of the upstream data source
- The license the data was provided under
- A human readable description of the data

See the [`SnapshotMeta`](https://github.com/owid/etl/blob/master/etl/snapshot.py#L81) class for supported fields.

## Dataset metadata

In our [data model](common-format.md), datasets contain tables of data, and those tables contain variables. Each of these levels supports metadata.

See the classes `DatasetMeta`, `TableMeta` and `VariableMeta` in the [`owid.catalog.meta`](https://github.com/owid/owid-catalog-py/blob/master/owid/catalog/meta.py) module for more details on the available fields.