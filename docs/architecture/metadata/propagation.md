!!! warning "This is still being written."

    Our metadata formats are still in flux, and are likely to change over the coming weeks.


We are defining an internal ETL logic so that the metadata is propagation in a smart way from Snapshot to Grapher.

- We keep information about the origins throughout the data pipeline.
- We track how much a dataset has been processed and signal this in the metadata (minor/major processing).
- We track different operations applied to an indicator. For instance, "indicator `A` was obtained from indicator `B` divided by population size".
