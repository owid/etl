Here's a summary of the steps, you don't have to manually execute anything, all of it will be done automatically after submitting a form below

1. **Create an ingest script** (e.g. `snapshots/namespace/version/dummy.py`) to download the data from its original source and upload it as a new data snapshot into `s3://owid-catalog/snapshots`. This step can also be done manually (although it is preferable to do it via script, to have a record of how the data was obtained, and to be able to repeat the process in the future, for instance if another version of the data is released).

2. **Create metadata YAML file** (e.g. `snapshots/namespace/version/dummy.xlsx.dvc`) with all metadata under `meta` key. We should have some minimum level of metadata there (e.g. including the license for data), but you should be able to add a snapshot in 15 minutes without thinking too hard about it, even if you later come back to improve it.
