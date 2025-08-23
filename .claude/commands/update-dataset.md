# Dataset Update

## Phase 2: Fix Issues (REQUIRED - Execute automatically)

### Phase 2A: Snapshot and Initial Processing

Run snapshot step, compare it with the old snapshot, ask user if they want to proceed and if yes, commit and update PR with a collapsable section "Snapshot difference".

1. **Run snapshot step**
   ```bash
   etls [namespace]/[version]/[short_name]
   ```

2. **Compare snapshots and document changes** - Analyze differences between old and new data files
   ```bash
   # First ensure old snapshot is loaded (don't use etls!)
   etlr snapshot://[old-version]/[dataset-name]

   # Then compare both snapshot files programmatically
   # Read both old and new snapshot files to understand structural changes
   ```
   - Document key differences (sheet names, column headers, data ranges, format changes)
   - Summarize structural and content changes found in raw data
   - **⚠️ STOP HERE and ask user**: "Here are the key snapshot differences found: [summary]. Do you want to proceed with fixing the ETL pipeline for these changes?"
   - **Only proceed if user explicitly confirms the changes look reasonable**
   - This helps understand what ETL fixes will be needed in subsequent phases

3. **Fix meadow step** - Run and fix parsing errors until meadow step succeeds
   ```bash
   etlr data://meadow/[namespace]/[version]/[dataset]
   ```

4. **Compare meadow data differences** - Analyze changes from old version
   ```bash
   etl diff REMOTE data/ --include "meadow/[namespace]/.*/[dataset]" --verbose
   ```

5. **Review and summarize differences** - Analyze the data changes:
   - Summarize key differences: new data points, removed data, changed values, metadata updates
   - Present summary to user and ask for approval before proceeding
   - **⚠️ STOP HERE and ask user**: "Here are the key differences found in the data update: [summary]. Do you want to proceed with committing these changes?"
   - **Only proceed if user explicitly confirms the changes look reasonable**

6. **Document approved differences and commit** - If user approves:
   - Add summary of key changes to PR description in collapsed block
   - Include: new data points, removed data, changed values, metadata updates

7. **Commit snapshot and meadow fixes**
   ```bash
   git add .
   git commit -m "Phase 2A: Fix snapshot download and meadow parsing"
   git push origin [branch-name]
   ```

### Phase 2B: Garden Step Fixes

1. **Fix garden step** - Run and fix any garden-specific errors
   ```bash
   etlr data://garden/[namespace]/[version]/[dataset]
   ```

2. **Compare garden data differences** - Analyze processed data changes
   ```bash
   etl diff REMOTE data/ --include "garden/[namespace]/.*/[dataset]" --verbose
   ```

3. **Review and summarize garden differences** - Verify processing logic is working correctly
   - Check that transformations are applied properly to new data structure
   - Ensure indicator names and metadata are consistent
   - Summarize key processing changes and data transformations
   - **⚠️ STOP HERE and ask user**: "Here are the key garden processing differences found: [summary]. Do you want to proceed with committing these changes?"
   - **Only proceed if user explicitly confirms the changes look reasonable**

4. **Commit garden fixes** - If user approves:
   ```bash
   git add .
   git commit -m "Fix garden step processing for updated dataset"
   git push origin [branch-name]
   ```

### Phase 2C: Complete Pipeline

1. **Test full pipeline** until `etlr --grapher` succeeds
   ```bash
   etlr [dataset] --grapher
   ```

2. **Update metadata YAML files** if necessary to match new column structure

3. **Final commit and PR update**
   ```bash
   git add .
   git commit -m "Complete dataset update - full pipeline working"
   git push origin [branch-name]
   ```

### Common issues when data structure changes

- **⚠️ SILENT FAILURES WARNING**: Never return empty tables as workarounds! If data parsing fails, the function should either:
  - Fix the parsing logic to handle the new format, OR
  - Raise a clear error with specific details about what changed, OR
  - Add a clear log warning explaining why data is missing
  - **BAD**: `return Table(pd.DataFrame({'col': []}))` - silently masks the real issue
  - **GOOD**: `raise ValueError("Sheet 'Fig 3.2' format changed - need to update skiprows from X to Y")`
  - Empty tables hide data quality issues and make debugging much harder for future updates

- **Column name changes**: If columns are renamed/split (e.g., single cost → local currency + PPP), update:
  - Python code references in garden step
  - Garden metadata YAML (`food_prices_for_nutrition.meta.yml`)
  - Grapher metadata YAML (if exists)
- **Index issues**: Check for unwanted `index` columns from `reset_index()` - ensure proper indexing with `tb.format(["country", "year"])`
- **Metadata validation**: Use error messages as guide - they show exactly which variables to add/remove from YAML files
