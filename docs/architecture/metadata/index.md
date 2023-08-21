!!! warning "This is still being written."

    Our metadata formats are still in flux, and are likely to change substantially over the coming months.


    We are currently working on version 2 of our metadata, for which you can find most up-to-date documentation [on Notion :octicons-arrow-right-24:](https://www.notion.so/owid/Metadata-guidelines-29ca6e19b6f1409ea6826a88dbb18bcc)


In our [data model](../design/common-format.md), we have various data objects: _Snapshots_, _datasets_ that contain _tables_ with _variables_, etc.


At each step, you can tweak, change and add new metadata. However, the most standard places to have metadata defined are in [Snapshot](workflow/snapshot.md) and in [Garden](workflow/garden.md).


## Snapshot
In Snapshot we define metadata attributes for the data source product. We make sure that all the different files, datasets and publications that we ingest to our system are properly documented. This includes making sure that these have licenses, descriptions, titles and other information assigned.


[Learn about the fields available in Snapshot :octicons-arrow-right-24:](fields.md)

## Garden
In Garden we focus on the metadata of the finished product. After all necessary ETL steps, the initial source file (or files) has (or have) been transformed into a curated dataset. This dataset may have multiple tables, each of which will have various indicators.

In this step we add more metadata content that describe this dataset, these tables and these indicators. Here, we focuss on the output of ETL (and not the input, i.e. the source). This means, that we may elaborate on the processing that a specific indicator has undergone (or how it has been created), how do we want these indicators to be called, etc.

[Learn about the fields available in Garden :octicons-arrow-right-24:](fields.md)


## Propagation of metadata
We are building a logic into ETL to automatically propagate metadata fields forward (Snapshot → Meadow → Garden → Grapher).

[Learn more :octicons-arrow-right-24:](propagation.md)


## Using metadata
### Metadata and Data pages
We automatically create data pages from an indicator using its metadata fields. Learn how the metadata fields are mapped to a data page with our demo app.

[Try the demo :octicons-arrow-right-24:](../../tutorials/metadata-play.md)


### Other uses
Users can consume the metadata programatically using the [`owid-catalog`](https://github.com/owid/etl/tree/master/lib/catalog).
