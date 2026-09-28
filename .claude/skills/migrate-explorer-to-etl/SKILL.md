---
name: migrate-explorer-to-etl
description: >-
  Migrate a non-ETL explorer into ETL end to end, as a `viz://explorer` step. Opens the
  branch and draft PR with `etl pr`, reads the live TSV from the production `explorers`
  table, resolves the `graphers` block's variable and chart IDs to catalog paths, translates
  the settings, `graphers` and `columns` tables into the dimensions and views
  `/create-explorer` needs, hands off to it, and sets the PROD/STAGING comparison body on the
  PR. Every explorer still outside ETL is grapher-based (variable IDs or chart IDs), so this is
  the one migration skill. Trigger on "/migrate-explorer-to-etl SLUG", "migrate the SLUG
  explorer to ETL", "bring SLUG explorer into ETL", or "port explorer SLUG". Temporary: retire
  when no explorer remains outside ETL; global-health (#6513) is the last tracked one.
metadata:
  internal: true
  status: temporary
  sunset: "retire when owid/etl#6513 closes and no explorer remains outside ETL"
  owner: lucasrodes
---

# Migrate an explorer into ETL

An explorer that is not yet in ETL lives only in the MySQL `explorers` table (its TSV is mirrored,
often stale, in `owid-grapher/explorers/<slug>.explorer.tsv`). This skill takes it to a
`viz://explorer/<ns>/latest/<short>` step: the boilerplate around a migration (branch, draft PR,
PR body), then the translation half (IDs to catalog paths, legacy tables to dimensions and views),
then a hand-off to `/create-explorer`, which owns the export-step authoring (Python skeleton, YAML
schema, FAUST upstream, post-processing, DAG, verification). Don't duplicate that content here.

Every explorer still outside ETL is **grapher-based**: its `graphers` block references variable IDs
(`yVariableIds`, optionally `xVariableId`, `colorVariableId`, `sizeVariableId`) or chart IDs
(`grapherId`). The CSV-backed route (`tableSlug` with `table` blocks) and the legacy-step
modernisation route were retired when their last explorers were done; if you meet either shape,
stop and tell the user rather than improvising.

## Inputs

- `<slug>`: the explorer slug, as in `https://ourworldindata.org/explorers/<slug>` and the `slug` column of `explorers`.
- `<ns>` and `<short>`: target namespace and short name under `etl/steps/viz/explorer/<ns>/latest/`. Ask if not obvious; past migrations used the topic namespace and the slug with hyphens turned into underscores.

If `etl/steps/viz/explorer/**/<short>.py` already exists, the explorer is in ETL and this skill does
not apply; edit it through `/create-explorer` instead.

## Step 1: branch and draft PR

Use `etl pr`, never `git checkout -b` plus `gh pr create`. Migrations are usually stacked, one PR
per explorer on top of the previous one, so branch off the current branch:

```bash
CURRENT_BRANCH=$(git branch --show-current)
.venv/bin/etl pr 'migrate `<slug>`' refactor --scope explorers --base-branch "$CURRENT_BRANCH" --no-llm 2>&1 | tee /tmp/etl_pr_out.txt
PR_URL=$(grep -oE 'https://github\.com/owid/etl/pull/[0-9]+' /tmp/etl_pr_out.txt | head -1)
PR_NUMBER=${PR_URL##*/}
NEW_BRANCH=$(git branch --show-current)
```

The title comes out as `🔨 explorers: migrate \`<slug>\`` (the 🔨 is the `refactor` category); quote the
backticks with single quotes. `etl pr` switches the working tree to the new branch itself, so do not
`git switch` again; if the user's editor still shows the old branch, it is a UI refresh, not a branch
problem. To keep working on the current branch in parallel, add `--worktree` and continue in the
sibling directory it prints.

If a tracking issue exists for the explorer (global-health: #6513), add the PR link to its checklist
row with `gh issue edit`. The umbrella issue #6028 is closed; do not edit it.

## Step 2: read the live explorer config from production

The `explorers` table is the source of truth. Do **not** read `owid-content/explorers/*.tsv`; that
repo is a year stale and mis-routes migrations. Priority:

1. **Production**, read-only, through the `.env.prod` the repo ships. It is reachable when `.env`
   declares `ENV_FILE_PROD` (see `etl/config.py`):
   ```python
   from etl.config import ENV_FILE_PROD, OWIDEnv
   PROD = OWIDEnv.from_env_file(ENV_FILE_PROD)
   row = PROD.read_sql("SELECT tsv, config FROM explorers WHERE slug = %s", params=("<slug>",)).iloc[0]
   ```
   (Same pattern as `apps/chart_approval/cli.py`.)
2. **The PR's staging server**, `make query SQL="SELECT tsv FROM explorers WHERE slug = '<slug>'"`,
   which routes by the current branch. `etl pr` provisions it asynchronously, so DNS for
   `staging-site-$NEW_BRANCH` can take a few minutes; hostnames are also truncated for long
   branch names.
3. Your own staging server (`DB_HOST=staging-site-<you>`) as a last resort.

The TSV has three sections:

- **Settings rows** at the top (key/value): `explorerTitle`, `explorerSubtitle`, `isPublished`,
  `hasMapTab`, `selection`, `pickerColumnSlugs`, `subNavId`, `subNavCurrentId`, `wpBlockId`,
  `entityType`, `originUrl`, `googleSheet`, `downloadDataLink`, `hideAlertBanner`, `thumbnail`,
  `yScaleToggle`, `yAxisMin`, `hideAnnotationFieldsInTitle`, … They map verbatim into the explorer's
  top-level `config:` block (schema in `/create-explorer`).
- **`graphers` table**: one row per view. Columns are the dimension widgets (`Metric Dropdown`,
  `Source Radio`, `Per capita Checkbox`, …), the ID column(s), and per-view chart config (`title`,
  `subtitle`, `type`, `hasMapTab`, `minTime`, `yAxisMin`, …).
- **`columns` table** (optional): per-(view, indicator) display overrides.

## Step 3: classify the `graphers` block

```bash
cat <<'PY' > /tmp/classify_explorer.py
import json, sys
from etl.config import ENV_FILE_PROD, OWIDEnv
slug = sys.argv[1]
env = OWIDEnv.from_env_file(ENV_FILE_PROD)
raw = env.read_sql("SELECT config FROM explorers WHERE slug = %s", params=(slug,))["config"].iloc[0]
cfg = json.loads(raw) if isinstance(raw, str) else raw
blocks = cfg.get("blocks", [])
print("blocks:", [b.get("type") for b in blocks])
gb = next((b for b in blocks if b.get("type") == "graphers"), None)
if gb:
    cols = set().union(*(r.keys() for r in gb.get("block", [])))
    print("graphers cols:", sorted(cols))
PY
.venv/bin/python /tmp/classify_explorer.py <slug>
```

| `graphers` columns | Case | What to do |
|---|---|---|
| `yVariableIds` (plus optional `xVariableId`, `colorVariableId`, `sizeVariableId`) | Indicator-based | Direct ID to catalogPath mapping (step 5). |
| `grapherId` only | Chart-based | Two-step lookup (step 5); the chart's stored config (title, subtitle, type, hasMapTab, map colours, …) becomes per-view config. |
| `tableSlug` with `table` blocks | CSV-backed | Retired route; none should remain. Stop and report. |
| `grapherId` and `tableSlug` mixed | Hybrid | Rare (`natural-disasters`, `food-footprints` were). Stop and ask the user. |

Do not trust type labels in old issues; check the columns.

## Step 4: invisible-character prefixes on choice names

Some production TSVs carry invisible Unicode prefixes on dropdown display names, almost always SOFT
HYPHEN (`\xad`), occasionally ZWSP or ZWNJ: a curator's hack to coerce Grapher's first-appearance
dropdown ordering when sub-collections share choice names. Democracy is the canonical case
(issue #6060); the prefixes are preserved in
`etl/steps/viz/explorer/democracy/latest/democracy.*.config.yml`.

**Preserve them byte for byte.** Grapher uses the choice's display name, not its slug, as the URL
query parameter, so stripping a prefix silently breaks every shared link that targeted the value.

```python
INVISIBLE = {"SOFT HYPHEN": "\xad", "ZWSP": "​", "ZWNJ": "‌", "ZWJ": "‍", "BOM": "﻿"}
counts = {name: tsv.count(ch) for name, ch in INVISIBLE.items() if tsv.count(ch)}
```

If `counts` is empty, move on. Otherwise build a map `(sub-config key, dimension slug, clean choice
name) -> prefix count` by stripping leading invisibles per cell across the dropdown columns, and
carry it to step 8: `/create-explorer` must prepend the right number of prefix characters to the
matching `name:` of each affected `dimensions[].choices[]` entry.

In the YAML, write the escape (`name: "\xad\xad\xadElectoral democracy"`), never the raw character:
they load identically, and the escape is visible in editors and diffs. `ruamel.yaml` re-emits `\xad`
as the raw character on round-trip, so if you scaffold the YAML programmatically, post-process the
dump to swap raw soft hyphens back to `\xad` inside double-quoted strings. When prefixes diverge
across sub-configs that share a slug, `combine_collections` renames the conflicting slugs internally
(`_update_choice_slugs_in_views` in `etl/viz/chart/core/combine.py`); slug renames are invisible to
URL parameters, so that is fine.

## Step 5: map IDs to catalog paths

```bash
# yVariableIds, xVariableId, colorVariableId, sizeVariableId: direct lookup
make query SQL="SELECT id, catalogPath FROM variables WHERE id IN (...)"

# grapherId: the chart's config lives in chart_configs, joined through charts.configId
make query SQL="SELECT c.id, cc.config FROM charts c JOIN chart_configs cc ON c.configId = cc.id WHERE c.id IN (...)"
# then look up every variableId in config.dimensions[] with the variables query above
```

Variables without a `catalogPath` are not in ETL; their datasets have to be brought into ETL first
(`/create-dataset`). **Stop and report which datasets are missing rather than guessing.**

For multi-indicator charts (stacked bars, for instance) read every entry of `config.dimensions`, not
just the first, and make each one an item of `view.indicators.y[]`.

## Step 6: identify the upstream grapher datasets

Each catalog path `<ns>/<v>/<dataset>/<table>#<short>` belongs to the step
`data://grapher/<ns>/<v>/<dataset>`. The unique set becomes the explorer step's DAG dependencies,
handed to `/create-explorer` in step 8.

Mental model for chart-based explorers: each `grapherId` is a thin wrapper around one or a few
indicators. The migration unwraps the chart, recovers its indicators, and rebuilds the explorer from
them. Whatever the chart stored (title, subtitle, colour scale, map config) either flows from the
indicator's garden metadata, preferred for single-indicator views (see `/create-explorer` step 5),
or has to be restated in the explorer YAML.

## Step 7: translate the legacy tables

| Legacy TSV element | Goes to (in the YAML `/create-explorer` writes) |
|---|---|
| Settings rows | top-level `config:` block, keys verbatim |
| `graphers` row | one `views:` entry |
| Dimension widget columns (`Metric Dropdown`, …) | `dimensions:` entries (slug = snake case of the widget name without the `Dropdown`/`Radio`/`Checkbox` suffix) and the `view.dimensions:` map |
| `yVariableIds` (space-separated) | `view.indicators.y[]`, one item per ID, each with `catalogPath:` |
| `xVariableId` | `view.indicators.x[]` |
| `colorVariableId` | `view.indicators.color[]` |
| `sizeVariableId` | `view.indicators.size[]` |
| Per-view chart settings (`title`, `subtitle`, `type`, `hasMapTab`, `minTime`, `yAxisMin`, …) | `view.config`; for single-indicator views prefer pushing `title`/`subtitle`/`note` into the indicator's garden `presentation.grapher_config` |
| `columns` table row | `view.indicators.<axis>[i].display` (colour scales, tolerance, units, …) |

For chart-based explorers, merge each chart's stored config (`title`, `subtitle`, `type`,
`hasMapTab`, `yAxis`, `map.colorScale`, …) into `view.config`, or into the indicator's garden
metadata for single-indicator views.

## Step 8: hand off to `/create-explorer`

Invoke `/create-explorer` with: the top-level `config:` settings, the dimensions list (slugs, names,
presentation types, choices, with the prefix map from step 4 applied), the views list (dimension
tuples, catalog paths, per-view overrides), and the `data://grapher/...` dependencies. It writes the
`etl/steps/viz/explorer/<ns>/latest/<short>.{py,config.yml}` pair and the DAG entry, and owns the
conventions: block-style YAML, `definitions.common_views` rather than anchors, hyphens in the URL
slug, conditional dimensions, FAUST upstream, `sort_choices`/`group_views`, verification. Do not
write the step by hand here.

## Step 9: set the PR body

The body is only the PROD/STAGING quick-link table: no summary, no @-mentions. `gh pr edit` fails
on a GraphQL deprecation, so use the REST API:

```bash
cat > /tmp/pr_body.json <<JSON
{"body":"| PROD | STAGING |\n|--------|--------|\n| [link](https://ourworldindata.org/explorers/<slug>) | [link](http://staging-site-${NEW_BRANCH}/admin/explorers/preview/<slug>) |"}
JSON
gh api -X PATCH repos/owid/etl/pulls/${PR_NUMBER} --input /tmp/pr_body.json --jq '.body' | head -3
```

Check the staging link resolves; long branch names are truncated in the hostname.

## Step 10: hand back

Tell the user what was produced, the PR URL, and how to verify:
`.venv/bin/etlr viz://explorer/<ns>/latest/<short>` plus the staging preview URL. Leave staging and
committing the generated files to the user unless asked; `etl pr` already pushed its empty commit.

## Gotchas

- **Block-style YAML only.** Every mapping and list in the config (`dimensions:`, `choices:`,
  per-view `dimensions:`, `selection:`, …) one key or item per line. Flow style collapses view
  blocks and makes review diffs unreadable. Markdown links inside quoted subtitles are content, fine.
- **Share view config through `definitions.common_views`, not YAML anchors.** Entries take a
  `config:` and an optional `dimensions:` filter; per-view `config:` overrides. `/create-chart`
  documents the same schema.
- **Missing catalog paths mean missing datasets.** Halt and list them; never guess an equivalent.
- **Staging is asynchronous.** DNS for the new staging host can lag by minutes; the PR and issue
  work do not need the database.

## Reference

- Tracking: #6513 (global-health, the last explorer outside ETL). The umbrella #6028 is closed.
- PRs to model the body on: #6029, #6031, #6032.
- Migrations that came through this route: `food_footprints`, `fertilizers`, `countries_in_conflict_data`, `democracy` (with soft-hyphen prefixes).
- After the explorer is in ETL it is a candidate for the port to an MDIM (`viz://chart`), umbrella #6014.

## Retire when

No explorer remains outside ETL: #6513 closed and `SELECT slug FROM explorers` on production lists
only slugs that have a `viz://explorer` step. Then delete this directory and repoint the
"When to use this skill" bullet in `/create-explorer`.
