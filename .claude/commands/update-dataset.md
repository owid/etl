# Dataset Update

### Create PR and update steps

1. First create a draft PR using `etl pr` command with a proper branch name and `data` category
2. Use the `etl update snapshot://` command with `--include-usages` to copy the dataset to the new version. For instance
   ```bash
   etl update snapshot://#$ARGUMENTS --include-usages
   ```
3. Run snapshot with `etls`, e.g. `etls #$ARGUMENTS`
4. Run `etlr` with `--grapher` to execute the updated steps. It's ok if it fails, don't fix it!
5. Commit changes and push to the PR branch
6. Ask user for review and permission to continue with fixing steps

### Fix steps

1. If `etlr` failed, investigate the error and try to fix it.
2. Update the metadata YAML file to match the new column structure if necessary.
3. Summarise the changes in the PR description.

### Common issues when data structure changes

- **Column name changes**: If columns are renamed/split (e.g., single cost → local currency + PPP), update:
  - Python code references in garden step
  - Garden metadata YAML (`food_prices_for_nutrition.meta.yml`)
  - Grapher metadata YAML (if exists)
- **Index issues**: Check for unwanted `index` columns from `reset_index()` - ensure proper indexing with `tb.format(["country", "year"])`
- **Metadata validation**: Use error messages as guide - they show exactly which variables to add/remove from YAML files
