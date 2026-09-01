---
name: check-chart-preview
description: Check chart or multidim preview on the staging server using a browser. Use when user wants to visually verify a chart renders correctly on staging, take a screenshot of a chart, or QA a chart/mdim preview.
metadata:
  internal: true
---

# Check Chart/Mdim Preview on Staging

Visually verify that a chart or multidimensional indicator page renders correctly on the staging server.

## Prerequisites

- A staging server must be running for the current git branch (check via `curl -s -o /dev/null -w "%{http_code}" http://<container>/`)
- The chart/mdim must have been pushed to staging already (either via `etlr --watch` in VSCode or manually)
- For non-trivial UI checks (MDIM dropdowns, link/unlink chips, etc.) the `agent-browser` skill is needed; for plain PNG snapshots it isn't

## Getting the Staging URL

### Option 1: Helper script (recommended)

```bash
.venv/bin/python .claude/skills/check-chart-preview/get_staging_url.py <slug-or-path>
```

Accepts chart slugs, mdim slugs, or file paths:
```bash
# Chart slug (most common)
.venv/bin/python .claude/skills/check-chart-preview/get_staging_url.py life-expectancy

# Mdim slug
.venv/bin/python .claude/skills/check-chart-preview/get_staging_url.py energy/latest/energy_prices#energy_prices

# Export multidim file path
.venv/bin/python .claude/skills/check-chart-preview/get_staging_url.py etl/steps/export/multidim/energy/latest/energy_prices.config.yml

# Chart file path
.venv/bin/python .claude/skills/check-chart-preview/get_staging_url.py etl/steps/graph/covid/latest/covid-cases.chart.yml
```

### Option 2: Construct manually

1. Get the git branch: `git branch --show-current`
2. Compute container name: replace `/._` with `-` in branch name, strip any `staging-site-` prefix, prepend `staging-site-`, truncate the normalized part to 28 chars, strip trailing `-`
3. Determine chart type and URL:

| File type | URL pattern |
|---|---|
| `.chart.yml` (simple, no `#` in slug) | `http://<container>/grapher/<slug>` |
| `.chart.yml` (mdim, `#` in slug) | `http://<container>/admin/grapher/<catalogPath>` |
| `export/multidim/*.config.yml` or `*.py` | `http://<container>/admin/grapher/<namespace>/<version>/<shortName>%23<shortName>` |

For `.chart.yml`: the `slug` is in the YAML `slug:` field. For mdim slugs containing `#`, the catalogPath inserts the version from the file path.

For export multidim: derive from path `etl/steps/export/multidim/<namespace>/<version>/<shortName>.*` → catalogPath is `<namespace>/<version>/<shortName>#<shortName>`.

## Checking the Preview

### Recommended: PNG via `/grapher/by-uuid/<UUID>.png` (works for drafts)

The fastest, most reliable way to grab a chart's rendered image — works for **drafts as well as published charts**, no browser, no auth required. Uses the same render pipeline OWID's public site uses for `/grapher/<slug>.png`, but addresses the chart by its `chart_configs.id` UUID, which is published to R2's `byUUID/` directory for every chart (whether or not the chart itself is `isPublished`).

```bash
# Resolve slug → by-uuid PNG URL, then fetch
URL=$(.venv/bin/python .claude/skills/check-chart-preview/get_chart_png_url.py <slug-or-id>)
curl -o ai/chart_preview.png "$URL"
```

Pass extra grapher query params with `--<key>=<value>`:

```bash
.venv/bin/python .claude/skills/check-chart-preview/get_chart_png_url.py <slug> --tab=map
.venv/bin/python .claude/skills/check-chart-preview/get_chart_png_url.py <slug> --tab=chart --time=earliest..2020
```

The helper queries the staging MySQL via `etl.config.OWID_ENV.read_sql` (which auto-routes to `staging-site-<branch>`), so it works as long as the staging server is up and the chart has been pushed.

Use the resulting PNG by reading it directly with the `Read` tool (Claude can view images).

### Fallback: browser preview for `mdim` pages / interactive UI

The by-uuid PNG route renders a single chart frame. For MDIM data pages with dropdowns, or for verifying interactive UI (controls, link/unlink chips, etc.), use the `agent-browser` skill to navigate to the staging URL from `get_staging_url.py` and take an in-browser screenshot.

### Quick PNG check (published charts only)

For charts that are already published, the slug-based PNG route also works:

```bash
curl -o ai/chart_preview.png "http://<container>/grapher/<slug>.png?nocache"
```

This 404s for drafts — use the by-uuid route above instead.

## Pushing to Staging First

If the chart hasn't been pushed yet:

```bash
# For graph step charts
.venv/bin/etlr graph://<namespace>/<version>/<slug> --graph --graph-push --private

# For export multidim
.venv/bin/etlr export://multidim/<namespace>/<version>/<shortName> --export --private
```
