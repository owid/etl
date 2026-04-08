# Debugging ETL Data Quality Issues

When ETL steps fail due to data quality issues (NaT values, missing data, missing indicators), **always trace upstream** rather than patching symptoms downstream.

## Systematic Approach

### 1. Check Snapshot First

Root cause often lies at the data source level:

```python
from etl.snapshot import Snapshot
snap = Snapshot('namespace/version/dataset.csv')
df = snap.read()
print(f"Date range: {df.DATE.min()} to {df.DATE.max()}")
print(f"Null values: {df.DATE.isnull().sum()}")
```

- Compare snapshot file sizes and date ranges
- External providers may truncate or discontinue data feeds
- Check history: `git log --oneline --follow snapshots/dataset.csv.dvc`

### 2. Trace Through Pipeline

Work backwards: **garden** → **meadow** → **snapshot** → **source**

```python
from owid.catalog import Dataset
ds = Dataset('/path/to/meadow/dataset')
tb = ds['table_name'].reset_index()
print(f"Null values: {tb.date.isnull().sum()}")
```

Fix at the earliest possible stage.

## Common Issues

| Issue | Common Cause |
|-------|--------------|
| NaT/null dates | Malformed dates in source, incorrect `dropna()` in meadow |
| Missing countries | Country mapping files, harmonization logic |
| Invalid data types | Conversion/cleaning steps at each stage |
| Duplicate records | Index formation, deduplication logic |

## Best Practices

- **Never patch symptoms** - don't add workarounds downstream
- **Check external sources first** - ETL may be working correctly with stale data
- **Add assertions** - data quality checks that fail fast with clear messages
- **Fix in meadow** - most cleaning should happen there, not garden
