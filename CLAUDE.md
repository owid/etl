# Agent Guide

Our World in Data's ETL system - a content-addressable data pipeline with DAG-based execution.

## Critical Rules

- **Always use `.venv/bin/`** for all Python commands (`etl`, `python`, `pytest`)
- **Never mask problems** - no empty tables, no commented-out code, no silent exceptions
- **Trace issues upstream**: snapshot → meadow → garden → grapher
- **Never push/commit** unless explicitly told to
- **Ask the user** if unsure - don't guess
- Use `gh` CLI to interact with GitHub issues (e.g. `gh issue view <url>`) instead of browser.

## Pipeline Overview

**snapshot** → **meadow** → **garden** → **grapher** → **export**

| Stage | Location | Purpose |
|-------|----------|---------|
| snapshot | `snapshots/` | DVC-tracked raw data |
| meadow | `etl/steps/data/meadow/` | Basic cleaning |
| garden | `etl/steps/data/garden/` | Business logic, harmonization |
| grapher | `etl/steps/data/grapher/` | MySQL ingestion |

## Quick Commands

```bash
.venv/bin/etlr namespace/version/dataset --private      # Run step
.venv/bin/etlr namespace/version/dataset --grapher      # Upload to grapher
.venv/bin/etl pr "Title" data                           # Create PR
make check                                               # Before committing
```

## Key Files

- `dag/` - YAML dependency graphs
- `etl/config.py` - Runtime configuration
- `.claude/docs/` - Detailed documentation

## Documentation

See `.claude/docs/` for detailed guides:
- `pipeline-stages.md` - Pipeline architecture
- `running-etl.md` - CLI commands and options
- `code-patterns.md` - PathFinder, geo.harmonize, etc.
- `debugging.md` - Data quality debugging
- `git-workflow.md` - PRs, commits, package management
- `performance.md` - Profiling and optimization

## Individual Preferences

- @~/.claude/instructions/etl.md
