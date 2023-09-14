!!! warning "This is still being written."

    Our metadata formats are still in flux, and are likely to change over the coming weeks.


Metadata is added to ETL in the form of YAML files. We usually only add metadata in two steps: Snapshot and Garden. Below, we describe how this is done for these two steps.

## Metadata in Snapshot
The entrypoint to ETL is the Snapshot step. This is where we define metadata attributes for the data product of the origin. This is fundamental to ensure that the data is properly documented and that the metadata is propagated to the rest of the system.

The metadata in Snapshot consists mainly of one object: `meta.origin`. To learn more about it, please refer to [the reference](../reference/origin).

!!! note "To do"

    Show an example of a YAML file with metadata in Snapshot (also link to an example on Github).


## Metadata in Garden
After adapting and processing the origin's data, we have a curated dataset. This dataset, contains indicators (maybe not present in the origin) that we need to properly document. There are other fields that need documentation too.

The metadata in Garden consists mainly of two objects: `dataset` and `tables`. To learn more about it, please refer to [the reference for dataset](../reference/dataset) and [the reference for tables](../reference/tables).

!!! note "To do"

    Show an example of a YAML file with metadata in Garden (also link to an example on Github).
