# Walkthrough - Walden

Here's a summary of this walkthrough, you don't have to manually execute anything, all of it will be done automatically after submitting a form below

1. **Create an ingest script** (e.g. `etl/vendor/walden/ingests/example_institution.py`) to download the data from its original source and upload it as a new data snapshot into the S3 `walden` bucket. This step can also be done manually (although it is preferable to do it via script, to have a record of how the data was obtained, and to be able to repeat the process in the future, for instance if another version of the data is released).

    Keep in mind that, if there is additional metadata, it should also be ingested into `walden` as part of the snapshot. If the data is in a single file for which you have a download link, this script may not be required: you can add this link directly in the index file (see next point). There is guidance on how to upload to `walden` manually in the [`walden` README](https://github.com/owid/walden#manually).

2. **Create an index file** `etl/vendor/walden/index/example_institution/YYYY-MM-DD/example_dataset.json` for the new dataset. You can simply copy another existing index file and adapt its content. This can be done manually, or, alternatively, the ingest script can also write one (or multiple) index files.


`walden` is for snapshots, and we should have some minimum level of metadata there (e.g. including the license for data), but you should be able to add a snapshot in 15 minutes without thinking too hard about it, even if you later come back to improve it.
