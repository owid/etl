---
name: remapping-ghost-variables
description: Fix ghost variable warnings by remapping charts from old to new variable IDs. Use when ETL grapher step shows "Variables used in charts will not be deleted automatically", when indicator short_names were renamed and charts still reference old variables, or when the user mentions ghost variables, orphaned indicators, or chart variable remapping.
metadata:
  internal: true
---

# Remapping Ghost Variables

Fix charts that reference old (ghost) variable IDs after indicator short_names were renamed in a garden/grapher step.

## When This Happens

After running a grapher step, you see a warning like:

```
[warning] Variables used in charts will not be deleted automatically.
  rows=   chartId  variableId
0     1234      56789
  variables=[56780, 56781, ..., 56789]
```

This means someone renamed indicator `shortName`s in the ETL step, creating new variables while charts still reference the old ones. The ETL can't delete the old variables because charts depend on them.

## Workflow

### Step 1: Create a staging PR

Create a PR first — all investigation and changes happen against the staging DB.

```bash
.venv/bin/etl pr "Remap chart <chart_id> ghost variables for <dataset>" data
```

Wait for the staging server to come up.

### Step 2: Identify the affected chart and old variables

From the warning, extract:
- **Chart IDs** from the `rows` table (e.g., chart 1234)
- **Ghost variable IDs** from the `variables` list

All queries below use `make query` which automatically targets the staging DB for the current branch.

Query the old variables to understand what was renamed:

```bash
make query SQL="SELECT id, name, shortName FROM variables WHERE id IN (<ghost_variable_ids>)"
```

### Step 3: Find what the chart currently uses

```bash
make query SQL="SELECT variableId FROM chart_dimensions WHERE chartId = <chart_id>"
```

Then get details on each:

```bash
make query SQL="SELECT id, name, shortName FROM variables WHERE id IN (<chart_variable_ids>)"
```

### Step 4: Find the new replacement variables

The new variables are in the same dataset but with different shortNames. Get the dataset ID first:

```bash
make query SQL="SELECT datasetId FROM variables WHERE id = <any_ghost_variable_id>"
```

Then search for the new variables. Look for the pattern difference between old and new shortNames:

```bash
make query SQL="SELECT id, shortName FROM variables WHERE datasetId = <dataset_id> AND shortName LIKE '<pattern_matching_new_name>' AND id NOT IN (<ghost_variable_ids>)"
```

Compare old vs new shortNames to confirm the mapping. Common rename patterns:
- Encoding changes (e.g., `gte_40` → `gt__40`)
- Prefix/suffix changes
- Restructured naming conventions

### Step 5: Remap the chart via Admin API

Use the `AdminAPI` to update the chart config on staging:

```python
from apps.chart_sync.admin_api import AdminAPI
from etl.config import OWIDEnv

env = OWIDEnv.from_staging('<branch_name>')
api = AdminAPI(env)

# Get current chart config
config = api.get_chart_config(<chart_id>)

# Remap old variable IDs to new ones
REMAP = {
    <old_var_id>: <new_var_id>,
    # ... add more mappings if multiple variables need remapping
}

for dim in config.get('dimensions', []):
    if dim['variableId'] in REMAP:
        old = dim['variableId']
        dim['variableId'] = REMAP[old]
        print(f'Remapped {old} -> {REMAP[old]}')

# Update the chart
result = api.update_chart(<chart_id>, config)
assert result['success'], f"Update failed: {result}"
print('Chart updated successfully')
```

## Guidelines

- **Always use staging** — never update charts directly on production
- **Verify the data is identical** — the new variable should contain the same data as the old one, just with a different shortName
- **Handle multiple charts** — the warning may list multiple chartId rows; remap all of them
- **Handle multiple variables per chart** — a chart may reference several ghost variables; remap all in one update
- **Check for narrative charts** — ghost variables may also be referenced by narrative charts (check `chart_dimensions` isn't the only reference)
