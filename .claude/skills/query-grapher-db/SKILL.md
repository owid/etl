---
name: query-grapher-db
description: Query the grapher MySQL database (local dev, a branch's staging server, or production via the public Datasette) and verify indicators and charts on a staging server. Use when you need to run SQL against grapher, find which charts use an indicator, assess the blast radius of a data fix, or check that a chart/indicator renders correctly on staging-site-<branch>.
metadata:
  internal: true
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

## Production queries via public Datasette

When you need production data (which charts use an indicator, chart configs, gdoc links) and local/prod MySQL isn't reachable, query the public Datasette over HTTP:

```bash
curl -s "https://datasette-public.owid.io/owid.json?sql=<url-encoded SQL>"
```

`chart_dimensions` + `charts` + `chart_configs` answer "which charts use variable X"; `narrative_charts` and `posts_gdocs_links` cover derived charts and article references — together they answer the full "what does this dataset affect?" question when assessing the blast radius of a data fix.

## Verifying charts on staging

- **Indicator data/metadata API**: `https://api-staging.owid.io/staging-site-<branch>/v1/indicators/<id>.data.json` (and `.metadata.json`). The path prefix is `staging-site-<branch>`, **not** the bare branch name — a wrong prefix silently serves data from a different environment instead of 404ing, which looks exactly like "my fix didn't take". When in doubt, grep the staging chart page (`http://staging-site-<branch>/grapher/<slug>`) for `data.json` to get the exact URLs it loads.
- **Rendered chart without a browser**: `http://staging-site-<branch>/grapher/<slug>.svg` returns a server-side render — grep it for axis labels / entity names to verify a fix end-to-end (e.g. `grep -oE '>[0-9]+ [a-z]+[^<]*<'` to read the y-axis ticks).
