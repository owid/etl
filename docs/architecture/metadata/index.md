<!-- !!! warning "This is still being written."

    Our metadata formats are still in flux, and are likely to change over the coming weeks. -->
!!! note "Questions about the metadata?"

    If you have questions about the metadata, you can share these in our [discussion](https://github.com/owid/etl/discussions/categories/metadata). This greatly helps us keep track of the questions and answers, and makes it easier for others to find answers to similar questions.

    :material-chat-question: Additionally, we have added a [FAQs](faqs) section to this entry.

One of the main values of our work is the careful documentation that we provide along with our data and articles. In the context of
 data, we have created a metadata system in ETL that allows us to describe the data that we are working with.


In our [data model](../design/common-format.md) there are various data objects (_snapshots_, _datasets_ that contain _tables_ with _indicators_, etc.), each of them with different types of metadata.



The metadata is ingested into ETL in the form of [YAML files](./structuring-yaml.md), which live next to the scripts. Metadata can be ingested at any ETL step to tweak, change and add new metadata. However, the most standard places to have metadata defined are in Snapshot and in Garden.


## Snapshot
In Snapshot we define metadata attributes for the data source product. We make sure that all the different files, datasets and publications that we ingest to our system are properly documented. This includes making sure that these have licenses, descriptions, titles and other information assigned.


!!! info "Learn more"

    - [Learn about the fields available in `origin` :octicons-arrow-right-24:](reference/origin)
    - [Learn about our workflow in Snapshot :octicons-arrow-right-24:](../workflow/snapshot#metadata)

## Garden
In Garden we focus on the metadata of the finished product. After all necessary ETL steps, the initial source file (or files) has (or have) been transformed into a curated dataset. This dataset may have multiple tables, each of them with various indicators.

In this step we add metadata that describes this dataset, these tables and these indicators. We focuss on the output of ETL (and not the input, i.e. the origin). This means, for instance, adding details on the processing that a specific indicator has undergone (or how it has been created), how do we want these indicators to be called, etc.

!!! info "Learn more"

    - [Learn about the fields available in `indicator` :octicons-arrow-right-24:](reference/indicator)
    - [Learn about the fields available in `table` :octicons-arrow-right-24:](reference/tables)
    - [Learn about the fields available in `dataset` :octicons-arrow-right-24:](reference/dataset)
    - [Learn about our workflow in Garden :octicons-arrow-right-24:](../workflow/garden#metadata)


## Propagation of metadata
We have built a logic into ETL to automatically propagate metadata fields forward (Snapshot → Meadow → Garden → Grapher).

[Learn more :octicons-arrow-right-24:](propagation.md)


## Using metadata
### Metadata and Data pages
We automatically create data pages from an indicator using its metadata fields. Learn how the metadata fields are mapped to a data page with our demo app.

[Try the demo :octicons-arrow-right-24:](../../tutorials/metadata-play.md)

### Other uses
Users can consume the metadata programmatically using the [`owid-catalog`](https://github.com/owid/etl/tree/master/lib/catalog).

## Updating metadata using ChatGPT

We have developed a tool, `etlcli metadata-upgrader`, that uses OpenAI's GPT model to help us update metadata files. This tool supports two types of metadata files: 'snapshot' and 'grapher'. It reads the metadata file, generates an updated version, and saves it either in the specified output directory or overwrites the original file.

!!! info "Learn more"

    - [Learn about how to use the `etlcli metadata-upgrader` tool :octicons-arrow-right-24:](metadata_chatgpt_update.md)
