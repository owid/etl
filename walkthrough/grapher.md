# Walkthrough - Grapher

## Grapher step

**Grapher step can be only executed by OWID staff**

Grapher step is the last phase before upserting the dataset into the database. It works in the same way as the other steps, but the transformations made there are meant to get the data ready for the database (and not be consumed by users of catalog).

After the dataset is generated using the standard `etl` command, we can run `etl ... --grapher` which will load the dataset and its tables and upsert them into `datasets`, `variables` and S3.
