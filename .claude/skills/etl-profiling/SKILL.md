---
name: etl-profiling
description: Profile and optimize ETL step performance — CPU time, memory usage, and I/O bottlenecks. Use when an ETL step is slow, uses too much memory, or when the user asks to profile, optimize, or speed up a step. Covers profiling commands, categorical dtype optimization, vectorization, SUBSET filtering for fast dev runs, and iterative diagnose→fix→reprofile workflow.
metadata:
  internal: true
---

# ETL Step Profiling & Optimization

## Quick Start

```bash
# CPU profile — shows time per line in run()
.venv/bin/etl d profile --cpu garden/namespace/version/dataset

# Memory profile — shows memory per line in run()
.venv/bin/etl d profile --mem garden/namespace/version/dataset

# Profile a specific function
.venv/bin/etl d profile --cpu garden/namespace/version/dataset -f process_data
```

## Workflow

1. **Check feather schemas first** — before profiling, inspect the on-disk types of large tables:
   ```python
   import pyarrow.feather as pf
   for field in pf.read_table("data/meadow/.../table.feather").schema:
       print(f"{field.name}: {field.type}")
   # large_string → should be dictionary (categorical)
   ```
2. **Profile** — measure, never guess
3. **Identify the bottleneck** — read the `%` column, focus on the top 3 lines
4. **Diagnose** — is it I/O, dtype waste, or algorithmic?
5. **Fix one thing** — apply the smallest targeted fix
6. **Re-profile** — verify improvement, repeat

## Reading Profile Output

The profiler outputs a table with columns:

```
Line   Hits   Time          Per Hit     % Time    Line Contents
  46      1   1.46e+10     1.46e+10     41.7      tb = ds_meadow.read("population")
```

- **% Time** — focus here. Sort by this mentally; anything >5% is worth investigating.
- **Hits** — number of times the line executed. High hits on simple ops = loop to vectorize.
- **Per Hit** — time per call. High per-hit on a single call = the operation itself is slow.

**Caveat**: `line_profiler` adds overhead. Functions that run fast but are called from many instrumented lines can appear inflated. If a line shows >20% but the step runs fast in practice, the profiler overhead is distorting results. Verify with wall-clock timing:

```python
import time; t0 = time.time()
# ... suspect code ...
print(f"Took {time.time() - t0:.2f}s")
```

## Common Bottlenecks & Fixes

### 1. String Columns That Should Be Categoricals

**Symptom**: `ds.read()` takes many seconds; memory in GB for a table with <1000 unique values per column.

**Diagnose**:
```python
import pyarrow.feather as pf
arrow_table = pf.read_table("data/meadow/namespace/version/dataset/table.feather")
for field in arrow_table.schema:
    print(f"{field.name}: {field.type}")
# If you see `large_string` or `string` for country/variant/age/sex → problem
```

Check unique counts:
```python
for col in ['country', 'variant', 'age', 'sex']:
    unique = arrow_table.column(col).unique()
    print(f"{col}: {len(unique)} unique values out of {arrow_table.num_rows:,} rows")
```

**Fix upstream (meadow step)** — convert to categorical before `.format()`:
```python
for col in ["country", "variant", "sex", "age"]:
    if col in tb.columns:
        tb[col] = tb[col].astype("category")
tb = tb.format(index_columns, short_name="table_name")
```

**Fix downstream (garden step)** — read with `safe_types=False` to preserve categoricals:
```python
tb = ds_meadow.read("table_name", safe_types=False)
```

`safe_types=True` (the default) converts categoricals back to `string[pyarrow]`, losing all the savings.

**Impact**: Typically 90-99% memory reduction and 5-30x faster reads for tables with >1M rows.

### 2. Slow Reads Despite Categoricals

**Symptom**: `ds.read()` is fast with `safe_types=False` but slow with default `safe_types=True`.

**Fix**: Use `safe_types=False` when you don't need the type safety guarantees. Be aware that categorical columns behave slightly differently (e.g., `.replace()` may warn about deprecated behavior — use `.cat.rename_categories()` instead).

### 3. Row-by-Row Operations

**Symptom**: `.apply(lambda row: ..., axis=1)` or Python loops over rows showing high time.

**Fix with `np.select`**:
```python
# Bad — iterates row by row
tb["result"] = tb.apply(lambda row: row["a"] if row["a"] > 0 else row["b"], axis=1)

# Good — vectorized
import numpy as np
conditions = [tb["a"] > 0]
choices = [tb["a"]]
tb["result"] = np.select(conditions, choices, default=tb["b"])
```

**Note on origins**: `np.where` and `np.select` strip OWID metadata origins. To preserve them:
```python
tb["result"] = tb["b"]  # default
tb.loc[tb["a"] > 0, "result"] = tb.loc[tb["a"] > 0, "a"]
```

### 4. Expensive Groupby on Large Tables

**Symptom**: `.groupby().sum()` or `.groupby().agg()` taking seconds on millions of rows.

**Fixes**:
- Ensure groupby columns are categorical (much faster hashing)
- Use `observed=True` to skip unused category combinations
- Use `as_index=False` to avoid expensive multi-index creation
- **Never mix lambdas with string aggregations** in `.agg()` — a single callable forces pandas off its fast C path, causing ~10× slowdown on ALL aggregations (including the string ones like `"sum"`). Split into two separate groupby calls instead.

```python
# Good
tb.groupby(["country", "year", "sex"], as_index=False, observed=True)["value"].sum()

# Bad — lambda poisons the entire agg call
tb.groupby(cols).agg({"value": "sum", "country": lambda x: check(x)})

# Good — separate the fast and slow aggregations
result = tb.groupby(cols).agg({"value": "sum"})
checks = tb.groupby(cols)["country"].apply(lambda x: check(x))
```

**Known issue**: `geo.add_region_aggregates()` (deprecated) injects a per-group lambda to check `countries_that_must_have_data`. When that list is empty (common case), the lambda is a no-op but still causes the slowdown. This was fixed in 2026-03 to skip the lambda when no checks are needed. The newer `paths.regions.add_aggregates()` API doesn't have this issue.

### 5. Unnecessary Full-Table Reads

**Symptom**: Reading a large table but only using a few columns or a subset of rows.

**Fix**: Filter early. Add a SUBSET env var pattern for dev runs:
```python
import os
SUBSET = os.environ.get("SUBSET")

def run():
    tb = ds_meadow.read("big_table", safe_types=False)
    if SUBSET:
        countries = [c.strip() for c in SUBSET.split(",")]
        tb = tb[tb["country"].isin(countries)]
    # ... rest of processing
```

Usage: `SUBSET='France,Germany' .venv/bin/etlr namespace/version/dataset --private`

### 6. Expensive `create_dataset` or `ds.add`

**Symptom**: `paths.create_dataset(tables=..., check_variables_metadata=True)` showing high time in profiler.

**Diagnosis**: Often this is **profiler overhead**, not real time. Verify with wall-clock:
```python
t0 = time.time()
ds = paths.create_dataset(tables=tables, ...)
print(f"create_dataset: {time.time() - t0:.2f}s")
```

If it's genuinely slow, the cost is usually in `update_metadata` (YAML parsing) or `ds.add` (feather serialization for large tables). These are typically fixed costs and not worth optimizing unless the tables themselves are unnecessarily large.

## Memory-Specific Profiling

```bash
.venv/bin/etl d profile --mem garden/namespace/version/dataset
```

Look for:
- **Spikes >100 MB** on a single line — likely creating a large intermediate copy
- **Cumulative growth** that never drops — objects not being freed

**Quick memory check** in code:
```python
print(f"Memory: {tb.memory_usage(deep=True).sum() / 1e6:.0f} MB")
print(tb.dtypes)  # object dtype = memory hog
```

## Iteration Tips

- **Always use SUBSET** for profiling iterations. Never run full data until you've confirmed the fix works.
- **Use `etl d profile`** for measuring, not `etlr` — the latter has overhead from change detection, dependency resolution, and dataset saving that drowns out the signal.
- **Small SUBSET for correctness** (2-3 values), **medium SUBSET for timing** (10-15 values). Only go bigger if the bottleneck doesn't show up at small scale.
- **`-f function_name`** to drill into specific functions. Only works for functions defined in the step's main module, not imported ones.

## Checklist Before Optimizing

- [ ] Profiled with actual data (not guessing)
- [ ] Identified top 3 bottleneck lines by % time
- [ ] Checked feather schema for string vs dictionary columns
- [ ] Checked `safe_types` setting on large table reads
- [ ] Verified with wall-clock timing (not just profiler)
- [ ] Re-profiled after each fix to confirm improvement
