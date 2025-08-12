---
tags:
  - 👷 Staff
status: new
---

For some metrics we compile multiple sources in order to create a single variable, AKA an OWID Maintained Metric (OMM). In these cases we should create a publicly available Google Sheet that contains for each data point the sources used, the values, and any notes that are relevant to the data point.

To this end, we have implemented the function `etl.data_helpers.misc.export_table_to_gsheet`, which exports a table with all the source details to a Google Sheet.

Learn how to use it from the existing examples in ETL:

| Example                  | ETL step                                                                                                                                                 | Output GSheet                                                                                                  |
| ------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| Child mortality          | [GitHub](https://github.com/owid/etl/blob/9c12fb1ba48dd2a3fbef557224be0d27842b9fee/etl/steps/data/grapher/un/2025-04-25/long_run_child_mortality.py#L27) | [GSheet](https://docs.google.com/spreadsheets/d/1n-WO7yEbi6sXPpeWrorSEVu8w_Yu5dM0n97q1h16L0g/edit?gid=0#gid=0) |
| Life expectancy at birth | [GitHub](https://github.com/owid/etl/blob/9c12fb1ba48dd2a3fbef557224be0d27842b9fee/etl/steps/data/grapher/demography/2024-12-03/life_expectancy.py#L29)  | [GSheet](https://docs.google.com/spreadsheets/d/1LnrU1V3p2wq7sAPY4AHRdH1urol3cKev7prEvlLfSU4/edit?gid=0#gid=0) |

Note, that in order to use it you need to [Google API credentials set up](../configure-google-api.md).

All output GSheets should be stored in [this shared folder](https://drive.google.com/drive/folders/1qH0uBtO5KLvdew8X6u-lF75E4uKHSrjp), which only GCDL can access, but the public have read access to individual sheets. **By default, the function will automatically save to this folder**.

!!! warning "This function is only run locally"
The `export_table_to_gsheet` function is not run in staging or production environments. It is only run locally, so you must run it on your local machine to create the GSheet.
