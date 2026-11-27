---
name: query-grapher-db
description: Query the grapher MySQL database (local dev, a branch's staging server, or production via the internal Datasette, over Tailscale) and verify indicators and charts on a staging server. Use when you need to run SQL against grapher, find which charts use an indicator, assess the blast radius of a data fix, or check that a chart/indicator renders correctly on staging-site-<branch>.
metadata:
  internal: true
  owner: Marigold
---

# Querying grapher MySQL and verifying charts on staging

## Quick queries (staging)

```bash
make query SQL="SELECT COUNT(*) FROM variables WHERE catalogPath IS NULL"
```

Automatically connects to `staging-site-{branch}` based on current git branch.

## Python (for more control)

```python
from etl.config import OWID_ENV
df = OWID_ENV.read_sql("SELECT * FROM datasets LIMIT 10")
```

**Prefer Python when the SQL contains `%` (LIKE patterns, JSON_EXTRACT paths) or single-quoted strings — `make query` re-interprets those via shell + make and breaks unpredictably.** Use `params={...}` for `%`/quoted values to dodge pymysql's own `%`-format-string parsing.

**`OWID_ENV` targets your local dev DB even when you're on a branch.** To query the branch's staging DB from Python, use `OWIDEnv.from_staging('<branch>')` (`from etl.config import OWIDEnv`) — e.g. `OWIDEnv.from_staging('my-branch').read_sql(...)`. Also note `make query` shells out to the `mysql` CLI, which may not be installed; if it errors with `mysql: command not found`, use the Python `from_staging(...).read_sql(...)` path instead.

## Production queries via the internal Datasette

When you need production data (which charts use an indicator, chart configs, gdoc links) and local/prod MySQL isn't reachable, query the internal Datasette at `http://analytics.owid.io/owid`. **It is reachable only over Tailscale** (plain HTTP, no login), and holds a copy of production's public tables: published charts only, lagging production by up to a few days.

```bash
curl -s -G http://analytics.owid.io/owid.json -d _shape=array \
  --data-urlencode "sql=SELECT id, slug FROM charts WHERE isPublished LIMIT 5"
```

- **The SQL runs in DuckDB, not MySQL**, after the datasette-parquet plugin rewrites it from SQLite's dialect. JSON columns (`config`, `content`, `descriptionKey`) are text: read them with `->>'$.path'` or `json_extract_string`.
- **Don't write `date(`.** The rewrite currently makes any query containing it return no rows and no error; use `cast(x AS date)`. And use a `VALUES` list rather than a `[...]` list literal, which the rewrite turns into a quoted identifier.
- **A query returns at most 1,000 rows and must finish within 1 s.** The default shape flags a cut-off result with `"truncated": true` (`_shape=array` drops the flag), so page longer results with `ORDER BY ... LIMIT 1000 OFFSET n`. A query over the time limit fails with `SQL Interrupted`.
- **A failed query still returns `rows: []`** (with HTTP 400 and `"ok": false`), so check the flag, not just the rows. From Python, `_query` in `.claude/skills/fact-check-dataset/scripts/collect_dods.py` pages and checks errors; the base URL is `OWID_DATASETTE_JSON_URL` in `etl/analytics/config.py`.

### Off the tailnet: `public.duckdb`

Without Tailscale (a cloud sandbox, say), read `https://files.ourworldindata.org/public.duckdb`, the nightly public export of the same tables. DuckDB reads it in place over HTTP range requests, so a query fetches a few MB, not the ~1 GB file (`uvx --from duckdb-cli duckdb` without a local `duckdb`):

```bash
duckdb -json -c "ATTACH 'https://files.ourworldindata.org/public.duckdb' AS owid (READ_ONLY);
SET enable_external_access=false; SET lock_configuration=true; USE owid;
SELECT id, slug FROM charts WHERE isPublished LIMIT 5"
```

- **Keep the two `SET`s right after every `ATTACH`**, a local file's included. `READ_ONLY` doesn't stop a view or macro inside the file from reading local files (`read_text('~/.ssh/...')`) or fetching URLs, so a tampered export could pull them into what you see; the `SET`s turn that off for the session and lock it. They must come after the `ATTACH`, which needs httpfs to load.
- **Never add a query string to the URL** (`?nocache` included): the CDN answers a range request with an unseen query string with the whole ~1 GB file.
- **On first use DuckDB downloads its `httpfs` extension from `extensions.duckdb.org`**, so a sandbox with a host allow-list needs that host as well as `files.ourworldindata.org`. Without it, download `https://files.ourworldindata.org/public.duckdb.gz` (~80 MB), gunzip it (~1 GB) and `ATTACH` the local file instead.

`chart_dimensions` + `charts` + `chart_configs` answer "which charts use variable X"; `narrative_charts` and `posts_gdocs_links` cover derived charts and article references — together they answer the full "what does this dataset affect?" question when assessing the blast radius of a data fix.

## Verifying charts on staging

- **Indicator data/metadata API**: `https://api-staging.owid.io/staging-site-<branch>/v1/indicators/<id>.data.json` (and `.metadata.json`). The path prefix is `staging-site-<branch>`, **not** the bare branch name — a wrong prefix silently serves data from a different environment instead of 404ing, which looks exactly like "my fix didn't take". When in doubt, grep the staging chart page (`http://staging-site-<branch>/grapher/<slug>`) for `data.json` to get the exact URLs it loads.
- **Rendered chart without a browser**: `http://staging-site-<branch>/grapher/<slug>.svg` returns a server-side render — grep it for axis labels / entity names to verify a fix end-to-end (e.g. `grep -oE '>[0-9]+ [a-z]+[^<]*<'` to read the y-axis ticks).
