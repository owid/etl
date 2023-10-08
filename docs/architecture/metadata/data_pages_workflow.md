# Data Pages Workflow

## Live Reloading of a Data Page

When modifying a YAML file for a Data page, executing the `etl ... --grapher` command to visualize updates can be inefficient. The following steps outline a streamlined approach for rapid development and testing.

### Example Step
For illustration, the step `grapher/gcp/2023-07-10/global_carbon_budget` is considered. The corresponding YAML file, `global_carbon_budget.meta.yml`, is assumed to be edited within the grapher channel and previewed on a [staging Data page](http://staging-site-mojmir/admin/datapage-preview/738081).

### Initial Configuration
- Add `DEBUG=1` to your `.env.myname` file and any other `.env.*` files in use to expedite ETL operations. `DEBUG=1` runs all steps in a single process and removes 2s overhead from running each step.

### File Monitoring
- Execute the ETL command with the `--watch` flag. This monitors the YAML file for changes and automatically re-executes the corresponding step. This might not be as useful for long-running steps.

### Data Filtering
- If the dataset contains numerous indicators, consider using `GRAPHER_FILTER=consumption_emissions_per_capita` to filter the data to only relevant variables. This is optional for smaller datasets.

### Optional Optimization
- Include the `--only` flag in the command to further improve performance by avoiding dependency checks.

### Command Summary
```bash
ENV=.env.myname GRAPHER_FILTER=consumption_emissions_per_capita etl grapher/gcp/2023-07-10/global_carbon_budget --grapher --watch --only
```
!!! note
    Working on your local grapher instead of a remote grapher will also reduce upload times. One option is to work on local grapher to begin with, and move to your staging server at a later point to be able to share the result with other colleagues.

### Usage
After initiating the above command, changes made to the YAML file can be reviewed by refreshing the [staging Data page](http://staging-site-mojmir/admin/datapage-preview/738081). The updates should be reflected promptly.

!!! info
    If editing the YAML file in the **garden channel** and the step execution time is long, refresh latency will be dependent on the step's runtime. In such cases, consider developing the YAML file in the **grapher channel** before moving it into the garden channel (if the table and indicator names are identical).
