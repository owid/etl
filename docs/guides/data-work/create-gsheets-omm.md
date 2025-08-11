---
tags:
  - 👷 Staff
---

For some metrics we compile multiple sources in order to create a single variable. In these cases we should create a publicly available Google Sheet that contains for each data point the sources used, the values, and any notes that are relevant to the data point.

We can do this using the `export_table_to_gsheet` function, within `etl.data_helpers`.

You must have the Google API configured in order to use this function. See the [Google API guide](configure-google-api.md) for instructions on how to do this.

There are a couple of examples of how to use it here:

- [Child mortality](https://github.com/owid/etl/blob/9c12fb1ba48dd2a3fbef557224be0d27842b9fee/etl/steps/data/grapher/un/2025-04-25/long_run_child_mortality.py#L27)
    - [Output GSheet](https://docs.google.com/spreadsheets/d/1n-WO7yEbi6sXPpeWrorSEVu8w_Yu5dM0n97q1h16L0g/edit?gid=0#gid=0)
- [Life expectancy at birth](https://github.com/owid/etl/blob/9c12fb1ba48dd2a3fbef557224be0d27842b9fee/etl/steps/data/grapher/demography/2024-12-03/life_expectancy.py#L29)
    - [Output GSheet](https://docs.google.com/spreadsheets/d/1LnrU1V3p2wq7sAPY4AHRdH1urol3cKev7prEvlLfSU4/edit?gid=0#gid=0)

All output GSheets should be stored in [this shared folder](https://drive.google.com/drive/folders/1qH0uBtO5KLvdew8X6u-lF75E4uKHSrjp), which only GCDL can access, but the public have read access to individual sheets. The function will automatically save to this folder.

The code can only be run locally, it will just get skipped on staging or prod, so to create the GSheet you must run it locally.
