# ETL Pipeline Stages

## Core Pipeline Flow

**snapshot** → **meadow** → **garden** → **grapher** → **viz** / **export**

### Snapshot (`snapshots/`)
DVC-tracked raw files with rich metadata. Source data downloaded from external providers.

### Meadow (`etl/steps/data/meadow/`)
Basic cleaning and format standardization. Minimal transformations - mostly loading and reshaping.

### Garden (`etl/steps/data/garden/`)
Business logic layer:
- Country harmonization via `paths.regions.harmonize_names()`
- Indicator calculations and derivations
- Metadata enrichment
- Data validation

### Grapher (`etl/steps/data/grapher/`)
MySQL database ingestion for OWID visualization platform.

### Viz (`etl/steps/viz/`)
Visualizations, addressed as `viz://<channel>/...`: `chart` (charts and MDIMs; a chart is an MDIM with `dimensions: []`), `explorer`, `static` (PNG/SVG images), `bespoke` (data feeds of bespoke interactive visualizations). Chart, explorer and bespoke steps run with `--grapher`; static steps only write local files and need no flag.

### Export (`etl/steps/export/`)
Files shipped to external destinations (R2, GitHub). Addressed as `export://...`, not `data://export/...`; need `--export`.

## Step URI Pattern

Steps follow: `data://[stage]/[namespace]/[version]/[name]`

Example: `data://garden/who/2024-01-15/ghe`

## DAG Dependencies

YAML-based dependency graphs in `dag/` directory:
- Content-based dirty detection skips unchanged steps
- Topological sorting ensures proper execution order
- Supports partial execution with `--only`, `--downstream` flags
