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
| `embed` | the surface holds it by id/slug and renders its config directly — article chart blocks, narrative charts, data insights, static viz, explorers | **No.** Must be migrated by hand |
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
surface_id, config_id, context, query_string, text, published`.

Two fields carry the weight for callers:

- **`config_id`** — the surface's `chart_configs.id`, present for every
  config-bearing surface (charts, MDim views, narrative charts). This is what lets a
  caller inspect configs without re-deriving any joins: a single
  `SELECT ... FROM chart_configs WHERE id IN (...)` covers charts, MDim views and
  explorer views alike. **One exception:** for a narrative chart read
  `AdminAPI.get_narrative_chart(id)["configFull"]` instead — the stored row lags a
  parent edit until the child is re-saved.
- **`query_string`** — the reference's own URL params (`country=`, `time=`, `tab=`),
  where article-level pins live and what makes a replacement URL reconstructable.

`surface_id` identifies the surface object itself (chart id,
`multi_dim_x_chart_configs.id`, narrative chart id, explorer slug, tag id), for when
you need to edit it rather than read it. **For any gdoc-backed surface (articles,
data insights) it is the Google Doc id** — `posts_gdocs.id` is literally the Doc id,
so `https://docs.google.com/document/d/<surface_id>/edit` opens the source document.

### `--markdown` — the report to hand a human

`--markdown ai/refs.md` renders **one table per surface**, grouped by `kind`, so a
long list stays scannable. For every article reference the table gives three ways to
reach it:

- 📄 the **Google Doc** to edit,
- 🔎 the **anchor text** to search for inside that doc,
- 🔗 a **scroll-to-reference link** into the published article (a `#:~:text=` fragment,
  built the same way as `chart_diff/citations.py:create_text_fragment_url`), which
  opens the page scrolled to and highlighting the exact sentence.

Block embeds have no anchor text, so those fall back to the plain article URL.

Indicator subjects are labelled with the indicator's name (not a bare variable id),
cells are truncated and pipe-escaped so the tables can't break, and drafts are marked
⚠️. For spreadsheet work use `--csv`, which carries the untruncated values.

## Surface catalog

What is swept, per subject type. Anything not on this list is not covered — say so
rather than implying full coverage.

**Chart subjects** (expanded to every old slug that still reaches the chart, since
references written before a rename point at the old one):

| surface | source | kind |
|---|---|---|
| articles | `posts_gdocs_links` (`grapher`, `guided-chart`) + `linkType='url'` scan | `embed` or `link` by `componentType` |
| explorers | `explorer_charts` (by chart id) | `embed` |
| narrative charts | `narrative_charts.parentChartId` | `embed` |
| data insights | `posts_gdocs.content->>'$."grapher-url"'` | `embed` |
| static viz | `static_viz.grapherSlug` | `embed` |
| key charts | `chart_tags` where `keyChartLevel > 0` | `render` |
| WordPress | `posts_links` (legacy; skipped if absent) | `link` |

**Indicator subjects**: charts (`chart_dimensions`), MDIM views
(`multi_dim_x_chart_configs` **plus** a config scan — that column records only the
first y indicator, so multi-indicator views are invisible to the join alone),
explorers (`explorer_variables`, aggregated per explorer).

**MDIM subjects**: article links/embeds, narrative charts pinned to a view
(`parentMultiDimXChartConfigId`), and inbound `multi_dim_redirects`.

**Explorer subjects**: article links/embeds (`linkType='explorer'`).

## Who uses this, and what stays theirs

This skill answers *which surfaces reference the object*. It deliberately does not
interpret them — each caller keeps the analysis only it can do:

| Skill | Uses the sweep for | Keeps |
|---|---|---|
| `check-hardcoded-years` | the surface list for a dataset/indicator, plus each reference's `query_string` (article `time=` pins) | reading configs for `minTime`/`maxTime`/`map.time`, grading pins against the data's latest time, the where-the-fix-goes table |
| `check-empty-entities` | the same list, plus `query_string` (`country=` pins) and old-slug expansion | entity-selection vs entities-with-data checks, grading findings against production |
| `update-dataset` (step 7) | one sweep shared by both audits above | the update workflow around them |
| `review-data-pr` (§8d) | a cheap "which surfaces carry this dataset" check | judging whether the author's audit was complete |
| `map-charts-to-mdim` | the sweep for the charts being redirected | replacement URLs, redirect severity, param-collision detection |
| `edit-faust-metadata` | — (keeps `blast_radius.py`) | per-field inheritance analysis: which surfaces are *shielded* by their own patch override, which have no inheritance path. A generic sweep can't answer that |

When a caller needs a surface this doesn't cover, add it here rather than locally —
that's the point of the split.

## Known gaps

State these when reporting; silence reads as full coverage.

- Non-ETL explorers whose config lives in the `explorers` TSV are not parsed.
- **Legacy CSV-backed explorers** (`data://explorers/...` wide tables — e.g. the
  poverty explorer) appear in no DB table: their data and selections live in the
  explorer TSV, outside grapher configs. Report them as a coverage caveat rather
  than letting them pass silently.
- `linkType='url'` rows pointing at `archive.ourworldindata.org` are dropped as
  frozen by design. As of 2026-07 every url-typed grapher row was an archive
  snapshot — don't bet an audit on that classification continuing to hold.
- Indicator-level `presentation.grapher_config` lives in garden/grapher
  `.meta.yml`, not the DB. It is invisible here and needs a repo grep, and it fans
  out to every thin MDim/explorer view that inherits it.
- Data insights are matched on `grapher-url`; one storing the reference elsewhere
  is missed.
- Article sweeps cover what `posts_gdocs_links` recorded — charts nested inside
  layout containers may not produce a row.
- Public Datasette's `posts_gdocs_links` lags; verify article fixes against the
  live page, not the mirror.

## Notes for skills that consume this

- **Narrative charts survive the parent's death, visibly.** The config is fetched
  by UUID, so the chart keeps rendering — but "Explore the data" is built from the
  parent's slug, and there is **no API to repoint a narrative chart**
  (`parentChartId`/`parentMultiDimXChartConfigId` are written only at creation).
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
