## Importing local CSV

**Importing local CSV should be only used for exploration and draft datasets.** Datasets that should be public should be either imported from Google Sheets or go through ETL.

1. CSV should have columns `country` (or `entity`) and `year` then any number of additional columns. Column names don't have to be underscored, their names will be used as titles. Example of [valid layout](https://ourworldindata.org/uploads/2016/02/ourworldindata_multi-var.png)

2. Select your file in `Use local CSV file` field and click `Submit`.

3. Some metadata **is not editable** after importing to grapher (but you can still edit chart configs). Let me know if this turns out to be a problem.
