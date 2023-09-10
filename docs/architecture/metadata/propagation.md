!!! warning "This is still being written."

    Our metadata formats are still in flux, and are likely to change substantially over the coming months.


    We are currently working on version 2 of our metadata, for which you can find most up-to-date documentation [on Notion :octicons-arrow-right-24:](https://www.notion.so/owid/Metadata-guidelines-29ca6e19b6f1409ea6826a88dbb18bcc)


We are defining an internal ETL logic so that the metadata is propagation in a smart way from Snapshot to Grapher.

- We keep information about the origins throughout the data pipeline.
- We track how much a dataset has been processed and signal this in the metadata (minor/major processing).
- We track different operations applied to an indicator. For instance, "indicator `A` was obtained from indicator `B` divided by population size".
