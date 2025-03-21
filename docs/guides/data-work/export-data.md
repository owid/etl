---
status: new
---

!!! warning "Export steps are a work in progress"

Export steps are defined in `etl/steps/export` directory and have similar structure to regular steps. They are run with the `--export` flag:

```bash
etlr export://explorers/minerals/latest/minerals --export
```

The `def run(dest_dir):` function doesn't save a dataset, but calls a method that performs the action. For instance `create_explorer(...)` or `gh.commit_file_to_github(...)`. Once the step is executed successfully, it won't be run again unless its code or dependencies change (it won't be "dirty").

## Exporting data to GitHub

One common use case for the `export` step is to commit a dataset to a GitHub repository. This is useful when we want to make a dataset available to the public. The pattern for this looks like this:

```python
if os.environ.get("CO2_BRANCH"):
    dry_run = False
    branch = os.environ["CO2_BRANCH"]
else:
    dry_run = True
    branch = "master"

gh.commit_file_to_github(
    combined.to_csv(),
    repo_name="co2-data",
    file_path="owid-co2-data.csv",
    commit_message=":bar_chart: Automated update",
    branch=branch,
    dry_run=dry_run,
)
```

This code will commit the dataset to the `co2-data` repository on GitHub if you specify the `CO2_BRANCH` environment variable, i.e.

```bash
CO2_BRANCH=main etlr export://co2/latest/co2 --export
```
