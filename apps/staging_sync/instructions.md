# Instructions

1. Merge your ETL branch to master and wait for the ETL to rebuild your dataset.
2. Fill `Config` section and tick `Dry run` first to see what charts will be updated and hit `Sync charts`.
3. Check the output, look for warnings that might indicate unexpected changes to charts in target server.
4. Untick `Dry run` and hit `Sync charts` again to apply changes to target.
5. New charts were created as drafts, make sure to **publish them in target**.
6. Chart updates were added as chart revisions, you still have to manually approve them.


# Notes

Staging servers are **destroyed 3 days after merging to master**, so this app should be
run before that, but after the dataset has been built by ETL.


## Charts:
- Only **published charts** from source are synced.
- New charts are synced as **drafts** in target (unless you set the checkbox).
- Existing charts (with the same slug) are added as chart revisions in target (unless you set the checkbox).
- You get a warning if the chart **has been modified in target** after source server was created.
- Deleted charts are **not synced**.

## Chart revisions:
- Approved chart revisions in source are automatically applied in target, assuming the chart has not been modified.

## Tags:
- Tags are synced only for **new charts** in the source server, any edits to tags in existing charts are ignored.
