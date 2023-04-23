!!! warning "This tutorial might be partial or incomplete. Please check with the team if you have questions."

Before the existance of the ETL, datasets were directly uploaded to our Grapher admin site. Sometimes, we want to use these datasets in ETL. However, they are not available by default as they were never imported via the ETL. To make them available in ETL we use _backport_.


The first step is getting them to Snapshot using:

```
bulk_backport
```

(specify `--limit` to make it process only a subset of datasets). It goes through all public datasets with at least one variable used in a chart and uploads them to Walden catalog (or skip them if they're already there and have the same checksum). If you set `--skip-upload` flag, it will only persist the datasets locally. **You need S3 credentials to upload them to Snapshot.**

Backported snapshot (and walden) datasets can be processed with ETL using

```
etl --backport
```

(or `etl backport --backport`). This will transform original datasets from long format to wide format, optimize their data types, convert metadata and add them to the catalog. Then you can run `publish` to publish the datasets as usual.

