# Running ETL Steps

## Quick Reference

```bash
# Run steps matching a pattern
.venv/bin/etlr namespace/version/dataset

# With grapher upload
.venv/bin/etlr namespace/version/dataset --grapher

# Preview without running
.venv/bin/etlr namespace/version/dataset --dry-run

# Force re-run (use with --only)
.venv/bin/etlr namespace/version/dataset --force --only
```

## Key Options

| Flag | Description |
|------|-------------|
| `--grapher/-g` | Upload to grapher database |
| `--dry-run` | Preview steps without running |
| `--force/-f` | Re-run even if up-to-date |
| `--only/-o` | Run only selected step (no dependencies) |
| `--downstream/-d` | Include downstream dependencies |
| `--private` | Use private flag (always recommended) |

## Make Commands

```bash
make etl           # Run garden steps only
make full          # Complete pipeline
make grapher       # Include grapher upsert
make sync.catalog  # Download catalog from R2 (~10GB)
make check         # Format, lint & typecheck changed files
make wizard        # Start Wizard UI (port 8053)
```

## Testing

```bash
# Dry run specific step
.venv/bin/etl run --dry-run data://garden/namespace/version/dataset

# Run specific test
.venv/bin/pytest tests/test_etl_step_code.py -k "test_namespace_version_dataset"

# DAG integrity
.venv/bin/pytest tests/test_dag_utils.py
```

## Important Notes

- Using `--force` alone is discouraged - always pair with `--only`
- Steps re-run automatically when code changes (no --force needed)
- Always use `--private` flag when running ETL steps
- For grapher:// steps, always add `--grapher` flag
