---
name: find-chart-references
description: >-
  Find every OWID surface that references a chart, indicator, MDIM, or explorer —
  articles (links vs embeds), explorers, narrative charts, data insights, static viz,
  key-chart slots, MDIM views, WordPress posts. Answers "what breaks or goes stale if
  this changes or goes away", and distinguishes surfaces a URL redirect fixes from
  surfaces that render the object themselves. Read-only, pure SQL. Trigger when the
  user asks "what references this chart/indicator", "where is this chart embedded",
  "what's the blast radius of this change", "which articles link to X", "what do I
  have to update if I retire this chart", or when another skill needs the surface
  sweep (map-charts-to-mdim, check-empty-entities, check-hardcoded-years,
  update-dataset).
metadata:
  internal: true
---

# Find what references a chart, indicator, MDIM, or explorer

One question — *what would break, go stale, or need editing if this object changed
or went away?* — asked the same way for every grapher object, so each skill that
needs it stops re-deriving its own surface list.

For how to point at the right database, see `query-grapher-db`. Everything here is
read-only SQL and needs no `ADMIN_API_KEY`.

## The `kind` field is the point

Every finding carries a `kind` that decides what a fix costs:

| kind | meaning | does a URL redirect fix it? |
|---|---|---|
| `render` | the surface resolves the object and draws it — a chart on an indicator, an MDIM view, an explorer view, a key-chart slot | n/a — the object *is* the content |
| `embed` | the surface holds it by id/slug and renders its config directly — article chart blocks, data insights, static viz, explorers, narrative charts pinned to an MDIM view | **No.** Must be migrated by hand |
| `link` | a hyperlink in prose or a raw URL | Yes — but the href is still worth updating |

The discriminator for articles is `posts_gdocs_links.componentType`: a `span-*`
value is a hyperlink inside body text; anything else is a block-level component
that renders the chart. Skills that only count rows in `posts_gdocs_links` cannot
tell these apart, and will report an embed as if a redirect covered it.

## Usage

```bash
ENV_FILE=<creds> DATA_API_ENV=production .venv/bin/python \
  .claude/skills/find-chart-references/scripts/find_references.py \
  --chart-slugs life-expectancy,child-mortality \
  --json ai/refs.json --csv ai/refs.csv
```

Subjects (combinable): `--chart-ids` · `--chart-slugs` · `--variable-ids` ·
`--dataset-id` · `--mdim <slug|catalogPath>` · `--explorer <slug>`.

`--transitive` adds a second hop for indicator subjects: after finding the charts
that render an indicator, sweep the articles referencing those charts. Off by
default — it multiplies the work on a widely-charted dataset.

Output rows: `subject_type, subject, subject_id, surface, kind, where, where_path,
context, query_string, text, published`. `query_string` is what makes a
replacement URL reconstructable (`country=`, `time=`, `tab=`), so don't drop it.

## Surface catalog

What is swept, per subject type. Anything not on this list is not covered — say so
rather than implying full coverage.

**Chart subjects** (expanded to every old slug that still reaches the chart, since
references written before a rename point at the old one):

| surface | source | kind |
|---|---|---|
| articles | `posts_gdocs_links` (`grapher`, `guided-chart`) + `linkType='url'` scan | `embed` or `link` by `componentType` |
| explorers | `explorer_charts` (by chart id) | `embed` |
| narrative charts | `narrative_charts.parentChartId` | `link` (renders its own config) |
| data insights | `posts_gdocs.content->>'$."grapher-url"'` | `embed` |
| static viz | `static_viz.grapherSlug` | `embed` |
| key charts | `chart_tags` where `keyChartLevel > 0` | `render` |
| WordPress | `posts_links` (legacy; skipped if absent) | `link` |

**Indicator subjects**: charts (`chart_dimensions`), MDIM views
(`multi_dim_x_chart_configs` **plus** a config scan — that column records only the
first y indicator, so multi-indicator views are invisible to the join alone),
explorers (`explorer_variables`, aggregated per explorer).

MDIM findings are keyed by **(mdim, view, indicator)**, not by view: one view can
render several of the requested indicators, and each one is its own reference. The
config scan resolves every stored indicator shape (an id, a `{id: …}` dict, a
`{catalogPath: …}` dict, or a bare catalog-path string), so a view holding a
catalog path is not silently skipped.

**MDIM subjects**: article links/embeds, narrative charts pinned to a view
(`parentMultiDimXChartConfigId`), and inbound `multi_dim_redirects`.

**Explorer subjects**: article links/embeds (`linkType='explorer'`).

## Known gaps

State these when reporting; silence reads as full coverage.

- Non-ETL explorers whose config lives in the `explorers` TSV are not parsed.
- Data insights are matched on `grapher-url`; one storing the reference elsewhere
  is missed.
- Article sweeps cover what `posts_gdocs_links` recorded — charts nested inside
  layout containers may not produce a row.
- Public Datasette's `posts_gdocs_links` lags; verify article fixes against the
  live page, not the mirror.

## Notes for skills that consume this

- **A narrative chart survives its parent chart being unpublished.** It owns a
  materialized full config written at creation and renders from that; the parent is
  joined in only to build the "Explore the data" href from its slug. So it is a
  `link`, not an `embed`, and a redirect covers it — don't gate a migration on it.
  Do check the href's query params, which ride along to the target.
  There is still **no API to repoint a narrative chart**
  (`parentChartId`/`parentMultiDimXChartConfigId` are written only at creation), so
  the *parent pointer* stays stale. That matters for a narrative chart pinned to an
  **MDIM view** — that one is a genuine `embed`, and it can block the MDIM's next
  re-publish via an unguarded FK.
- **Query-param collisions matter for redirects.** Grapher merges the incoming
  URL's params *over* the redirect target's, so a link carrying `?metric=…` will
  override an MDIM dimension of the same name. If you build replacement URLs,
  compare each reference's `query_string` against the target's dimension slugs.
- **Cost control**: keep sweeps subject-scoped rather than site-wide, prefer the
  aggregate counts this script already returns over per-view rows, and treat a
  failed lookup as *unknown*, never as *none*.

## Lessons

When a run reveals a surface this catalog misses, add it here — this file is the
shared list, and a gap fixed here fixes it for every skill that reads it.
