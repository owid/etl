# Performance Profiling

## Memory Profiling

```bash
.venv/bin/etl d profile --mem garden/namespace/version/dataset
.venv/bin/etl d profile --mem garden/namespace/version/dataset -f func_name  # Specific function
.venv/bin/etl d profile --cpu garden/namespace/version/dataset  # CPU profiling
```

## Common Issues

### 1. Object vs Categorical Dtypes (96-99% memory savings)

```python
# Load with categorical preserved
tb = ds.read("table", safe_types=False)
assert isinstance(tb["country"].dtype, pd.CategoricalDtype)
```

Common causes of dtype conversion:
- `np.asarray()` on categorical (use `.array` instead)
- `.apply()` operations

### 2. Vectorization (100x+ speedup)

```python
# Bad
tb["result"] = tb.apply(lambda row: func(row), axis=1)

# Good - use np.select()
conditions = [tb["col1"].notna(), tb["col2"].notna()]
tb["result"] = np.select(conditions, [tb["col1"], tb["col2"]], default=tb["col3"])
```

### 3. Workflow

1. Run profile, identify large memory spikes (>100 MB)
2. Check if inherent (row expansion) or wasteful (object dtypes)
3. Apply fixes, add assertions, re-profile

## Database Access

```bash
mysql -h staging-site-[branch] -u owid --port 3306 -D owid -e "SELECT query"
```

Example queries:
```sql
-- Find datasets by shortName
SELECT id, catalogPath, name FROM datasets WHERE shortName = 'name' AND NOT isArchived;

-- Check variables in dataset
SELECT id, name FROM variables WHERE datasetId = 12345;
```
