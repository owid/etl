---
tags:
    - 👷 Staff
---
# Data Pages Workflow

## Live Reloading of a Data Page

When modifying a YAML file for a Data page, executing the `etl ... --grapher` command to visualize updates can be inefficient. The following steps outline a streamlined approach for rapid development and testing.

### Example Step
For illustration, the step `grapher/gcp/2023-07-10/global_carbon_budget` is considered. The corresponding YAML file, `global_carbon_budget.meta.yml`, is assumed to be edited within the grapher channel and previewed on a Data page from a [staging server](../../guides/staging-servers/).

### Initial Configuration
- Add `DEBUG=1` to your `.env.myname` file and any other `.env.*` files in use to expedite ETL operations. `DEBUG=1` runs all steps in a single process and removes 2s overhead from running each step.
- Add `PREFER_DOWNLOAD=1` to your environment to use cached downloads of files instead of redownloading them each time, which can significantly speed up steps involving large file downloads.

### File Monitoring
- Execute the ETL command with the `--watch` flag. This monitors the YAML file for changes and automatically re-executes the corresponding step. This might not be as useful for long-running steps.

### Data Filtering
- If the dataset contains numerous indicators, consider using `SUBSET=consumption_emissions_per_capita` to filter the data to only relevant variables. This is optional for smaller datasets.

### Instant YAML Updates
- Use `INSTANT=1` environment variable to enable instant YAML updates, which bypasses the normal ETL pipeline for metadata changes. This makes YAML changes appear almost immediately in the grapher without re-running the entire ETL process.
- Example usage: `INSTANT=1 etl grapher/gcp/2023-07-10/global_carbon_budget --grapher`
- Note: Instant mode only works for metadata changes. Any changes to data processing in Python will still require a full ETL run.

#### Limitations
This might fail, be slow or give unexpected results in the following cases:

- Garden step doesn't call `paths.create_dataset` as the last command (e.g. when it programmatically post-processes metadata)
- Grapher code is doing some heavy lifting or flattening

### Optional Optimization
- Include the `--only` flag in the command to further improve performance by avoiding dependency checks.

### Command Summary
```bash
# Basic command with watch mode
ENV_FILE=.env.myname SUBSET=consumption_emissions_per_capita etl grapher/gcp/2023-07-10/global_carbon_budget --grapher --watch --only
```
!!! note

    Working on your local grapher instead of a remote grapher will also reduce upload times. One option is to work on local grapher to begin with, and move to your staging server at a later point to be able to share the result with other colleagues.

### Usage
After initiating the above command, changes made to the YAML file can be reviewed by refreshing the staging Data page. The updates should be reflected promptly.

!!! info

    If editing the YAML file in the **garden channel** and the step execution time is long, refresh latency will be dependent on the step's runtime. In such cases, consider developing the YAML file in the **grapher channel** before moving it into the garden channel (if the table and indicator names are identical).

### Workflow Best Practices

1. **Start with INSTANT mode:** Begin with `INSTANT=1` for rapid YAML changes to test metadata updates without waiting for full ETL runs.

2. **Use PREFER_DOWNLOAD wisely:** Enable this for steps with large downloads to avoid repeatedly downloading the same files.

3. **Local development first:** Develop on local grapher before moving to staging for faster iteration cycles.

4. **Switch to full ETL runs:** Once metadata is finalized, run a complete ETL process to ensure all changes are properly integrated.

5. **Combine optimization flags:** For the fastest development experience, combine `DEBUG=1`, `PREFER_DOWNLOAD=1`, and `INSTANT=1` as shown in the command summary.
