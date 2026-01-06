# ETL Pipeline Stages

## Core Pipeline Flow

**snapshot** → **meadow** → **garden** → **grapher** → **export**

### Snapshot (`snapshots/`)
DVC-tracked raw files with rich metadata. Source data downloaded from external providers.

### Meadow (`etl/steps/data/meadow/`)
Basic cleaning and format standardization. Minimal transformations - mostly loading and reshaping.

### Garden (`etl/steps/data/garden/`)
Business logic layer:
- Country harmonization via `geo.harmonize_countries()`
- Indicator calculations and derivations
- Metadata enrichment
- Data validation

### Grapher (`etl/steps/data/grapher/`)
MySQL database ingestion for OWID visualization platform.

### Export (`etl/steps/data/export/`)
Final outputs - explorers, collections, APIs.

## Step URI Pattern

Steps follow: `data://[stage]/[namespace]/[version]/[name]`

Example: `data://garden/who/2024-01-15/ghe`

## DAG Dependencies

YAML-based dependency graphs in `dag/` directory:
- Content-based dirty detection skips unchanged steps
- Topological sorting ensures proper execution order
- Supports partial execution with `--only`, `--downstream` flags
