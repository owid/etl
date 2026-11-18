# Decisions: check-chart-preview

Calls still in force, with the reason and the change that settled each. Read before editing the skill.

- **The recommended preview is a PNG from `/grapher/by-uuid/<chart_configs.id>.png`, not a browser screenshot.** It renders through the same pipeline as the public site, needs no browser or login, and works for drafts: `/grapher/<slug>.png` 404s for unpublished charts, while every chart's config is written to R2's `byUUID/`. The browser stays the fallback for MDim data pages and interactive UI, which a single PNG frame can't show. Settled in #6511.
- **`get_chart_png_url.py` looks up the UUID and builds the URL from one `OWIDEnv.from_staging(...)`**, so both always point at the same server. The script used to take the host from the git branch and the UUID from `OWID_ENV`, which is the `.env` DB unless `STAGING` is set; without `STAGING=1` it printed a staging URL holding a UUID from another DB. Both helpers target the current branch's server by default and honor `STAGING=<branch>`. Settled in the PR that added this file.
- **No `.chart.yml` input.** The `graph://` step type it belonged to no longer exists (charts are `viz://chart` steps), so `get_staging_url.py` takes slugs, MDim catalog paths and `viz/chart/` paths only.
- **The helpers import `get_container_name` from `etl.config` rather than porting it.** The container-name rule is already duplicated in the ops repo (see the note on `etl.config.get_container_name`), and a third copy here would drift. Importing `etl.config` adds under a second to each call.
