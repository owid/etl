<!-- !!! warning "This is still being written."

    Our metadata formats are still in flux, and are likely to change over the coming weeks. -->
!!! note "Questions about the metadata?"

    If you have questions about the metadata, you can share these in our [discussion](https://github.com/owid/etl/discussions/categories/metadata). This greatly helps us keep track of the questions and answers, and makes it easier for others to find answers to similar questions.

One of the main values of our work is the careful documentation that we provide along with our data and articles. In the context of
 data, we have created a metadata system in ETL that allows us to describe the data that we are working with.


In our [data model](../design/common-format.md) there are various data objects (_snapshots_, _datasets_ that contain _tables_ with _indicators_, etc.), each of them with different types of metadata.



The metadata is ingested into ETL in the form of [YAML files](./structuring-yaml.md), which live next to the scripts. Metadata can be ingested at any ETL step to tweak, change and add new metadata. However, the most standard places to have metadata defined are in Snapshot and in Garden.


## Snapshot
In Snapshot we define metadata attributes for the data source product. We make sure that all the different files, datasets and publications that we ingest to our system are properly documented. This includes making sure that these have licenses, descriptions, titles and other information assigned.


!!! info "Learn more"

    - [Learn about our workflow in Snapshot :octicons-arrow-right-24:](../workflow/snapshot#metadata)
    - [Learn about the fields available in `origin` :octicons-arrow-right-24:](reference/origin)

## Garden
In Garden we focus on the metadata of the finished product. After all necessary ETL steps, the initial source file (or files) has (or have) been transformed into a curated dataset. This dataset may have multiple tables, each of them with various indicators.

In this step we add metadata that describes this dataset, these tables and these indicators. We focuss on the output of ETL (and not the input, i.e. the origin). This means, for instance, adding details on the processing that a specific indicator has undergone (or how it has been created), how do we want these indicators to be called, etc.

!!! info "Learn more"

    - [Learn about our workflow in Garden :octicons-arrow-right-24:](../workflow/garden#metadata)
    - [Learn about the fields available in `dataset` :octicons-arrow-right-24:](reference/dataset)
    - [Learn about the fields available in `tables` :octicons-arrow-right-24:](reference/tables)


## Propagation of metadata
We have built a logic into ETL to automatically propagate metadata fields forward (Snapshot → Meadow → Garden → Grapher).

[Learn more :octicons-arrow-right-24:](propagation.md)


## Using metadata
### Metadata and Data pages
We automatically create data pages from an indicator using its metadata fields. Learn how the metadata fields are mapped to a data page with our demo app.

[Try the demo :octicons-arrow-right-24:](../../tutorials/metadata-play.md)

### Indicator titles
We currently have many fields related to an indicator's title, namely `indicator.title`, `display.name`, `presentation.grapher_config.title`, `presentation.title_public`, and `presentation.title_variant`. Here's a short clarification of how to define them:

- `indicator.title` must always be given.
  - For 'small datasets', it can be the publicly displayed title of the indicator in all places.
  - For 'big datasets' with many dimensions, it can include text fragments to communicate breakdowns, like  "- Gender: male - Age: 10-19". In such cases, `indicator.title` is mostly useful for internal searches, and a human-readable `display.name` should be given.
- `display.name` is our most versatile human-readable title, shown in many public places. It must be used to replace `indicator.title` when the latter has complex breakdowns.
- `presentation.grapher_config.title` should be used when a chart requires a specific title, different from the indicator's title (or `display.name`).
- `presentation.title_public` is only used for the title of a data page. It must be an excellent, human-readable title.
  - `presentation.title_variant` is an additional short text that accompanies the title of the data page. It is only necessary when the indicator needs to be distinguished from others, or when we want to emphasize a special feature of the indicator, e.g. "Historical data".

In an ideal world, we could define all previous fields for all indicators, but in practice, we need to minimize our workload when creating metadata. For this reason, most of the fields are optional, and publicly displayed titles follow a hierarchy of choices. In general, the hierarchy is:
`presentation.title_public > grapher_config.title > display.name > indicator.title`
The following places on our (internal/public) website will be populated using this hierarchy:
- Admin: `indicator.title`
- Sources and Table tab (and possibly other public places): `display.name > indicator.title`
- Chart title: `grapher_config.title > display.name > indicator.title`
- Data page title: `presentation.title_public > grapher_config.title > display.name > indicator.title`

### Other uses
Users can consume the metadata programmatically using the [`owid-catalog`](https://github.com/owid/etl/tree/master/lib/catalog).
