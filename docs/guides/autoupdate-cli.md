---
tags:
  - 👷 Staff
---

# Enabling Automatic Updates via Metadata

Our ETL system includes an automatic update system that runs daily to update snapshots that are marked for automatic updating. This system identifies snapshots by looking for the `autoupdate` metadata field in their `.dvc` files, runs the corresponding Python scripts to fetch the latest data, and creates pull requests when data has changed.

It is then up to you to review, optionally edit and merge that PR. Use `chart-diff` and enable **Show all charts** in Options to check all affected charts. `data-diff` can also be helpful. Once you merge the PR, autoupdate will automatically create a new PR when there's new data available.

## Overview

The automatic update process:

1. Runs daily via a scheduled job
2. Identifies snapshots with `autoupdate` metadata in their `.dvc` files
3. Executes these snapshots' Python scripts to fetch the latest data
4. Checks if the data has actually changed (by comparing MD5 hashes and file sizes)
5. Creates a GitHub pull request when changes are detected for review and merging

!!! warning "Caution: Overwrites Existing Versions"
    The autoupdate workflow creates new snapshots in R2, but keeps all ETL versions the same. This means ETL and grapher datasets are overwritten on every update. If your updated steps are dependencies for other steps, those will be updated automatically as well—without a version bump.
    **Recommendation:** Use autoupdate only for "isolated", regularly updated datasets that use the "latest" version.
    If an update cascades unexpectedly, you can review all data changes using chart diff.

!!! info "Work in Progress"
    We are still working on a generalized autoupdate workflow for larger datasets that properly respects versioning. :face_with_monocle:

## Enabling Autoupdate for a Snapshot

To enable automatic updates for a snapshot, you need to add the `autoupdate` field to its `.dvc` file:

```yaml
autoupdate:
  name: Update name
meta:
  # Origin metadata...
outs:
  # Outputs...
```

The `name` field is used to group related snapshots together. Multiple snapshots can share the same update name, which means they will be processed together and included in the same pull request.


## Comparison with Scheduled Script Updates

The automatic update system provides an alternative approach to the scheduled update scripts described in [Auto Regular Updates](auto-regular-updates.md). Here's how they compare:

| Feature | Automatic Updates via Metadata | Scheduled Scripts |
|---------|-----------------|-------------------|
| Update mechanism | Uses `autoupdate` field in `.dvc` files | Dedicated bash script for each dataset |
| PR creation | Always creates PRs for review | Creates commit directly on master |
| Grouping | Groups related snapshots in single PR | One script per dataset update |
| Discovery | Automatic discovery of autoupdate-enabled snapshots | Manual maintenance of update scripts |
| Integration | Runs automatically daily | Scheduled individually via Buildkite |
