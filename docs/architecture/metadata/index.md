---
tags:
  - Metadata
---

<!-- !!! warning "This is still being written."

    Our metadata formats are still in flux, and are likely to change over the coming weeks. -->

# Metadata

!!! tip "Check the [metadata reference](reference) for a complete list of metadata fields."


One of the main values of our work is the careful documentation that we provide along with our data and articles. In the context of
 data, we have created a metadata system in ETL that allows us to describe the data that we are working with.


In our [data model](../design/common-format/#data-model){data-preview} there are various data objects (_snapshots_, _datasets_ that contain _tables_ with _indicators_, etc.), each of them with different types of metadata.



The metadata is ingested into ETL in the form of [YAML files](./structuring-yaml.md), which live next to the scripts. Metadata can be ingested at any ETL step to tweak, change and add new metadata. However, the most standard places to have metadata defined are in Snapshot and in Garden.


!!! note "Questions about the metadata?"

    If you have questions about the metadata, you can share these in our [:fontawesome-brands-github: discussion](https://github.com/owid/etl/discussions/categories/metadata). This greatly helps us keep track of the questions and answers, and makes it easier for others to find answers to similar questions.

    :material-chat-question: Additionally, we have added a [FAQs](faqs) section to this entry.

## Snapshot
In Snapshot we define metadata attributes for the data source product. We make sure that all the different files, datasets and publications that we ingest to our system are properly documented. This includes making sure that these have licenses, descriptions, titles and other information assigned.

