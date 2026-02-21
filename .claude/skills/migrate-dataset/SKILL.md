---
name: migrate-dataset
description: Migrate a legacy OWID dataset (no catalogPath) into the ETL pipeline. Use when user wants to migrate, backport, or convert a legacy dataset by ID, or mentions datasets without catalogPath.
---

# Migrate Legacy Dataset

Migrate a legacy OWID dataset from MySQL into the ETL pipeline using `etl b migrate`. This gives the dataset a proper `catalogPath` and moves it under ETL management.

## Inputs

Required:
- `dataset_id` - The MySQL dataset ID (integer)
- `namespace` - Target namespace (e.g., `plastic_pollution`, `trade`, `food`)

Optional:
- `version` - Dataset version date (default: `latest`). Prefer using the source's retrieval date (YYYY-MM-DD format).
- `short_name` - Override the auto-generated short name

## Workflow

### 1. Look up the dataset

Query MySQL to understand the dataset before migrating:

```bash
make query SQL="SELECT id, name, isPrivate FROM datasets WHERE id = <dataset_id>"
make query SQL="SELECT id, name, catalogPath FROM variables WHERE datasetId = <dataset_id>"
make query SQL="SELECT id, name, description FROM sources WHERE datasetId = <dataset_id>"
```

Present to the user:
- Dataset name and privacy status
- Number of variables and whether they have `catalogPath` (should be NULL for legacy datasets)
- Source info — use the source's `retrievedDate` or publication date to suggest a `version`

### 2. Confirm namespace and version with user

Get the list of existing namespaces to pick from:

```bash
ls etl/steps/data/garden/
```

Based on the dataset topic and existing namespaces, suggest an appropriate:
- **namespace**: pick an existing namespace if one fits, otherwise propose a new one
- **version**: from the source's retrieved date in YYYY-MM-DD format

Ask the user to confirm or adjust before proceeding.

### 3. Run the migration

```bash
.venv/bin/etl b migrate --dataset-id <dataset_id> --namespace <namespace> --version <version> --run
```

The `--run` flag handles the full pipeline automatically:
1. Backports data from MySQL to S3 snapshots
2. Generates ETL step files (snapshot, garden, grapher)
3. Adds entries to `dag/migrated.yml`
4. Runs the snapshot step
5. Runs ETL + grapher step (creates new dataset in MySQL with `catalogPath`)
6. Matches indicators between old and new datasets (perfect match, non-interactive)
7. Upgrades charts to point at new variables

## Important notes

- The indicator upgrader DB table is **cumulative** — mappings from previous migrations persist. This is harmless but means the upgrade step may show mappings from earlier migrations.
- Only variables used in charts need to match. Unused variables with no chart references can safely be unmatched.
- If variable names changed significantly (human-readable -> underscored), perfect matching at 100% threshold may miss some. Check if unmatched variables appear in any charts.
- Legacy datasets that are **private** (`isPrivate=1`) will use private S3 buckets automatically.

## Troubleshooting

- **"No such dataset"**: Verify the dataset ID exists with `make query SQL="SELECT * FROM datasets WHERE id = <id>"`
- **Indicator match finds 0 matches**: Variable names likely changed. Consider running `etl indicator-upgrade match` manually with a lower `--auto-threshold` (e.g., 80).
- **Grapher step fails**: Check that the staging DB is accessible and `GRAPHER_USER_ID` is set.

## Example

```
User: migrate dataset 5291
Agent: [looks up dataset] "Plastic ocean pollution (Meijer et al. 2021)" - 8 variables, 9 charts, source retrieved 2021-04-30
       Suggested: namespace=plastic_pollution, version=2021-04-30
User: looks good
Agent: .venv/bin/etl b migrate --dataset-id 5291 --namespace plastic_pollution --version 2021-04-30 --run
       -> Dataset 5291 -> 7360, 6/8 variables matched, 8 charts upgraded
```
