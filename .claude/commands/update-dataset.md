# Dataset Update

1. Create a PR and update steps with dataset-update-pr subagent & checkpoint
2. Run snapshot-updater subagent & checkpoint
3. Run step-fixer subagent on meadow step & checkpoint
4. Run step-fixer subagent on garden step & checkpoint
5. Run step-fixer subagent on grapher step & checkpoint

## Checkpoint
- Present summary to user and ask for approval before proceeding
- **⚠️ STOP HERE and ask user**: "Here are the key differences found in the data update: [summary]. Do you want to proceed with committing these changes?"
- **Only proceed if user explicitly confirms the changes look reasonable**
- Commit changes and push
- Post a summary of key changes to PR description in collapsed block
- Continue



### Common issues when data structure changes

- **⚠️ SILENT FAILURES WARNING**: Never return empty tables or comment code as workarounds!
- **Column name changes**: If columns are renamed/split (e.g., single cost → local currency + PPP), update:
  - Python code references in garden step
  - Garden metadata YAML (`food_prices_for_nutrition.meta.yml`)
  - Grapher metadata YAML (if exists)
- **Index issues**: Check for unwanted `index` columns from `reset_index()` - ensure proper indexing with `tb.format(["country", "year"])`
- **Metadata validation**: Use error messages as guide - they show exactly which variables to add/remove from YAML files
