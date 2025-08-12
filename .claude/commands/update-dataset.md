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

### Upgrade indicators (Optional)

**Ask user first**: "Do you want to upgrade chart indicators to use the new dataset version? This will update all existing charts that use indicators from the old dataset to reference the new dataset indicators."

If the user confirms, proceed with indicator upgrade:

1. **Find dataset pairs** using their shortName:
   ```sql
   mysql -h staging-site-data-wb-foodprices-nutrition -u owid --port 3306 -D owid -e "SELECT id, catalogPath, name FROM datasets WHERE shortName = 'dataset_short_name' AND NOT isArchived ORDER BY id;"
   ```

2. **Analyze indicator mappings** using the corrected SQL from the Indicator Upgrader section below.

3. **Add variable mappings** using the 3-step process:
   - Step 1: Create table structure
   - Step 2: Insert perfect matches with SQL  
   - Step 3: Add manual mappings if needed (see detailed process below)

4. **Test with dry-run**:
   ```bash
   python apps/wizard/app_pages/indicator_upgrade/charts_update.py --dry-run
   ```

5. **Execute upgrade** if dry-run looks good:
   ```bash
   python apps/wizard/app_pages/indicator_upgrade/charts_update.py
   ```

6. **Verify results**: Check that all charts were successfully updated.

### Common issues when data structure changes

- **Column name changes**: If columns are renamed/split (e.g., single cost → local currency + PPP), update:
  - Python code references in garden step
  - Garden metadata YAML (`food_prices_for_nutrition.meta.yml`)
  - Grapher metadata YAML (if exists)
- **Index issues**: Check for unwanted `index` columns from `reset_index()` - ensure proper indexing with `tb.format(["country", "year"])`
- **Metadata validation**: Use error messages as guide - they show exactly which variables to add/remove from YAML files
- **Snapshot metadata**: Check if version info needs updating in `.dvc` file:
  - Update `version_producer`, `date_published`, and years in citations if newer version available
  - Keep your own `date_accessed` but align version info with the actual data

## Indicator Upgrader

The indicator upgrader tool helps systematically update charts when datasets are replaced with new versions. It maps indicators from old datasets to new datasets and updates all affected charts automatically.

### Finding Dataset Pairs

First, identify the old and new dataset versions using their shortName:

```sql
mysql -h staging-site-data-wb-foodprices-nutrition -u owid --port 3306 -D owid -e "SELECT id, catalogPath, name FROM datasets WHERE shortName = 'dataset_short_name' AND NOT isArchived ORDER BY id;"
```

This will show all versions of the dataset with their IDs and catalog paths.

### Analyzing Indicator Mappings

Use this SQL to find perfect matches and unmapped indicators between datasets:

```sql
mysql -h staging-site-data-wb-foodprices-nutrition -u owid --port 3306 -D owid -e "
SELECT
    v_new.id AS new_id,
    v_old.id AS old_id,
    COALESCE(v_new.name, v_old.name) AS title
FROM
    (SELECT DISTINCT v.id, v.name
     FROM variables v
     INNER JOIN chart_dimensions cd ON v.id = cd.variableId
     WHERE v.datasetId = [OLD_DATASET_ID]) v_old
LEFT JOIN
    (SELECT v.id, v.name
     FROM variables v
     WHERE v.datasetId = [NEW_DATASET_ID]) v_new
ON v_old.name = v_new.name

UNION

SELECT
    v_new.id AS new_id,
    v_old.id AS old_id,
    COALESCE(v_new.name, v_old.name) AS title
FROM
    (SELECT v.id, v.name
     FROM variables v
     WHERE v.datasetId = [NEW_DATASET_ID]) v_new
LEFT JOIN
    (SELECT DISTINCT v.id, v.name
     FROM variables v
     INNER JOIN chart_dimensions cd ON v.id = cd.variableId
     WHERE v.datasetId = [OLD_DATASET_ID]) v_old
ON v_new.name = v_old.name
WHERE v_old.id IS NULL

ORDER BY title;
"
```

Replace `[NEW_DATASET_ID]` and `[OLD_DATASET_ID]` with actual dataset IDs.

### Interpreting Results

The query results show:
- **Perfect matches**: Rows where both `new_id` and `old_id` are not NULL
- **Unmapped new indicators**: Rows where `old_id` is NULL (new indicators used in charts with no old counterpart)
- **Unused old indicators**: Rows where `new_id` is NULL (old indicators not used in current charts)

### Finding Manual Mappings

For unmapped new indicators, analyze the titles manually to find conceptual matches:

- Look for similar concepts with different wording (e.g., "share" vs "percentage", "cost" vs "price")
- Consider unit changes (e.g., "number of people" vs "share of population")
- Check for renamed categories (e.g., "animal-source foods" vs "animal-sourced foods")
- Match by subject matter even if exact phrasing differs

### Adding Variable Mappings to Database (3-Step Process)

Use this improved 3-step process to add variable mappings systematically:

#### Step 1: Create Table Structure

```sql
mysql -h staging-site-data-wb-foodprices-nutrition -u owid --port 3306 -D owid -e "
CREATE TABLE wiz__variable_mapping (
    id_old INT NOT NULL,
    id_new INT NOT NULL,
    timestamp DATETIME NOT NULL,
    dataset_id_old INT NOT NULL,
    dataset_id_new INT NOT NULL,
    comments TEXT,
    PRIMARY KEY (id_old, timestamp),
    INDEX idx_dataset_old (dataset_id_old),
    INDEX idx_dataset_new (dataset_id_new)
);
"
```

#### Step 2: Insert Perfect Matches with SQL

```sql
mysql -h staging-site-data-wb-foodprices-nutrition -u owid --port 3306 -D owid -e "
INSERT INTO wiz__variable_mapping (id_old, id_new, timestamp, dataset_id_old, dataset_id_new, comments)
SELECT 
    v_old.id AS id_old,
    v_new.id AS id_new,
    NOW() AS timestamp,
    [OLD_DATASET_ID] AS dataset_id_old,
    [NEW_DATASET_ID] AS dataset_id_new,
    'Perfect name match - automated' AS comments
FROM 
    (SELECT DISTINCT v.id, v.name
     FROM variables v
     INNER JOIN chart_dimensions cd ON v.id = cd.variableId
     WHERE v.datasetId = [OLD_DATASET_ID]) v_old
INNER JOIN
    (SELECT v.id, v.name
     FROM variables v
     WHERE v.datasetId = [NEW_DATASET_ID]) v_new
ON v_old.name = v_new.name;
"
```

#### Step 3: Add Manual Mappings (if needed)

Check for unmapped indicators:

```sql
mysql -h staging-site-data-wb-foodprices-nutrition -u owid --port 3306 -D owid -e "
SELECT 
    v_old.id AS old_id,
    v_old.name AS old_name
FROM 
    (SELECT DISTINCT v.id, v.name
     FROM variables v
     INNER JOIN chart_dimensions cd ON v.id = cd.variableId
     WHERE v.datasetId = [OLD_DATASET_ID]) v_old
LEFT JOIN 
    wiz__variable_mapping vm ON v_old.id = vm.id_old
WHERE vm.id_old IS NULL
ORDER BY v_old.name;
"
```

If any indicators need manual mapping, use the WizardDB method:

```python
from apps.wizard.utils.db import WizardDB

# Manual mappings for indicators that need brain matching
manual_mapping = {
    old_id: new_id,  # Add manually matched pairs here
}

WizardDB.add_variable_mapping(
    mapping=manual_mapping,
    dataset_id_old=OLD_DATASET_ID,
    dataset_id_new=NEW_DATASET_ID
)
```

### Running the CLI Upgrade

After adding mappings to the database, use the CLI tool to perform the upgrade:

```bash
# Dry run (preview changes)
python apps/wizard/app_pages/indicator_upgrade/charts_update.py --dry-run

# Apply the upgrade
python apps/wizard/app_pages/indicator_upgrade/charts_update.py
```

The CLI tool will:
1. Read variable mappings from the database
2. Find all affected charts
3. Update chart configurations with new indicator IDs
4. Show progress and results
