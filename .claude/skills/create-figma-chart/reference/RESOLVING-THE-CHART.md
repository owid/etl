# Step 1 — Resolve the chart and gather its text

> Read at Step 1.  Part of [`/create-figma-chart`](../SKILL.md); the spine has the step order.


Get an SVG URL for the chart, whatever form the reference takes:

| Input | SVG URL |
|---|---|
| Slug or default grapher link | `https://ourworldindata.org/grapher/<slug>.svg` |
| Customized grapher link (query params) | insert `.svg` before the `?`, keep the query verbatim: `https://ourworldindata.org/grapher/<slug>.svg?country=USA~CHN&time=1990..latest` — `country`, `time`, `tab`, `stackMode`, `region`, `focus`, … are all honored, and slug redirects work |
| MDim view | same — the dimension params select the view: `.../energy-mix.svg?metric=per_capita&source=coal` |
| **Explorer view** | `https://ourworldindata.org/explorers/<slug>.svg?<view params>` — `EXPLORER_DYNAMIC_THUMBNAIL_URL` in `settings/clientSettings.ts`. **Carry the view's full param set:** requested bare it returns an axis and nothing else, at HTTP 200 (2 texts, no series). Verified on the `imType=thumbnail` route; untested for the other `imType`s. |
| **Bespoke component** (no slug, no `.svg`) | there is no endpoint — render and serialize the component yourself. See [BESPOKE-SVG.md](../BESPOKE-SVG.md). |
| Admin link `/admin/charts/<id>/edit` | **`/admin/charts/<id>.svg` does not exist** (it returns the admin SPA shell). Resolve the chart's `configId` — `SELECT configId FROM charts WHERE id = <id>` on the public Datasette (see the `query-grapher-db` skill), or `GET /admin/api/charts/<id>.config.json` — then use `https://ourworldindata.org/grapher/by-uuid/<configId>.svg`. Works for unpublished drafts too. |
| Narrative chart (**name**) | name → uuid via the unauthenticated map `https://admin.owid.io/api/narrative-chart-map`, then `https://ourworldindata.org/grapher/by-uuid/<uuid>.svg`. Being unauthenticated, this one route works from a cloud sandbox, while the *authenticated* `admin.owid.io` routes are Access-blocked there — test a specific route rather than assuming the host |
| Narrative chart (**admin link with a numeric id**, `/admin/narrative-charts/<id>/edit`) | **Try the direct lookup first** — `select id, name, chartConfigId from narrative_charts where id = <id>` on the public Datasette hands you the uuid outright (note the column is `chartConfigId`, not `configId`). Only when the id isn't mirrored yet do you need the guessing route below. |
| … the same, when the id is **newer than the Datasette mirror** | there is no id→uuid endpoint and the mirror lags production by days. Diff the live name-keyed map against `select name from narrative_charts` for the unmirrored names, then order them by uuid — **uuidv7, so lexical order is creation order** — and count up from the mirror's highest id. That is a *candidate*, not an answer: ids have gaps where charts were deleted, so **always render it and have the user confirm before building**. The **name is the stronger signal** — these are named after the piece they serve (`share-of-women-in-parliament-di`), so an unmirrored name matching the DI's topic and carrying the highest uuid is near-certain. Note a published DI can have `linkedNarrativeCharts: {}` because it ships a hand-made PNG, so the chart may be newer than the post; its embedded JSON still gives `grapher-url`, `authors` and the body text Step 2 needs. |
| Description only | find candidates via site search (`https://ourworldindata.org/search?q=...`) or a Datasette title match; show the candidates and confirm before proceeding |
| **Local SVG on disk** (from an `export://static_viz` step) | nothing to resolve — the file *is* the export. Skip the whole texts table below: an ETL step bakes its title, subtitle, `Note:`, `Data source:` and license line into the SVG, building the source string from the indicator's `origins` rather than from `chart.citation`. Read the strings straight out of the file if you need them (`grep -o '<text[^>]*>[^<]*' <file>.svg`), and take the frame's target template from the step, which already sized the figure to it. |

Then pull the chart's texts, which seed the template texts in Step 6. Read them from **`.metadata.json`**, not `.config.json`, and **keep the view's query params on the request** — `.../energy-mix.metadata.json?metric=per_capita&source=coal` resolves the selected MDim view exactly as the `.svg` request does:

| Template text | Where it comes from |
|---|---|
| Title | `chart.title` |
| Subtitle | `chart.subtitle`; when that's absent the chart is inheriting the indicator's `description_short` — take `columns[<column>].descriptionShort` |
| Note | `chart.note` (absent when the chart has none) |
| Data source | `chart.citation` — verbatim what grapher prints after `Data source:` |
| Topic page | **`.config.json` → `originUrl`** — the one field `.metadata.json` doesn't give (its `chart.originalChartUrl` is the grapher URL, not the topic page). Often null; fall back to asking the user in Step 2. |

`.config.json` is not a substitute: it never carries the source attribution, it carries `subtitle`/`note` only when a chart overrides them manually, and **for an MDim slug it 404s** — per-view configs aren't exposed under that path. Pass *every* dimension a view needs; a partial MDim param set returns an empty payload (`title: null`, no columns). If a text is ever in doubt, the rendered SVG is the tie-breaker — it shows exactly what the footer will say.

These texts also arrive **render-ready**: the endpoint unwraps grapher's detail-on-demand links across the whole payload, so a `description_short` written as `[lower secondary](#dod:lower-secondary-education)` reaches you as plain `lower secondary` — paste it as-is, and don't hand-strip anything. `.config.json` hands back the raw markup instead, which is one more reason not to take texts from it.

> **The one exception: `by-uuid` has no `.metadata.json`** — that route serves only `.config.json`, `.png` and `.svg`, so the request 404s. For a **narrative chart**, therefore, take the texts from `by-uuid/<uuid>.config.json`, which is complete for that case: a narrative chart stores its own `title`/`subtitle`/`note`/`originUrl` overrides rather than inheriting them. What it still won't give you is the source attribution — read that off the rendered SVG's footer instead, and re-derive nothing (see the producer-name rule below).
>
> **In that case read *every* text off the SVG, not just the source.** `.config.json` hands back raw detail-on-demand markup, so a note stored as `expressed in [international-$](#dod:int_dollar_abbreviation)` arrives with the brackets attached and you are one hand-edit away from pasting them into the template. The rendered SVG carries the same strings already unwrapped, in `<g id="header">` (title, subtitle) and `<g id="footer">` (sources, note) — one fetch, render-ready, and it is the file you are importing anyway:
>
> ```bash
> .venv/bin/python -c "import html,re,sys; s=open(sys.argv[1]).read(); cut=lambda a,b: html.unescape(re.sub(r'\s+',' ', re.sub(r'<[^>]*>?',' ', s[s.find('>',s.find(a))+1:b]))).strip(); print(cut('id=\"header\"', s.find('id=\"chart-area\"')), '\n\n', cut('id=\"footer\"', None))" chart.svg
> ```
>
> Two details the command has to get right: the footer group sits **after** `chart-area` in the document, so a slice that stops at the plot returns the title and subtitle only and quietly loses the source and note; and the SVG carries XML entities (`&#x27;` for an apostrophe), so unescape before pasting.

**Never shorten the producer's name to make it fit.** The footer string is the producer's official name — verify it against `rg "producer: .*<name>" snapshots/` if you're unsure — and "Food and Agriculture Organization of the United Nations" does not become "UN Food and Agriculture Organization" because the line is too long. When it overruns the CC BY text, wrap it (Step 7) rather than editing it.

**Check that every selected entity actually renders.** The failure is not always a missing *latest* year — an entity can be selected and have **no data at all**: `cereal-yield` carries both `North America` and `Northern America` in its selection and the latter has zero rows, so seven selected entities render six lines, and have done for as long as the chart has existed. Two near-identical region names in one selection is the signature; check the count of drawn series against the selection every time, and report a permanent absence as a chart-config bug (not a regression) so someone removes it. Grapher also silently drops an entity whose data doesn't reach the displayed year, with no warning anywhere — a chart pinned to 2023 quietly showed ten of its eleven countries because one stopped at 2022, and the DI text still discussed the missing one. Compare the **effective** selection against the entity labels in the exported SVG, and if they differ, say so before building: the fix is the narrative chart's tolerance setting (or pinning the year), and it belongs to whoever owns the chart.

  "Effective" is the catch. `selectedEntityNames` in `.config.json` is the *saved* chart's selection, so it is the wrong baseline for exactly the inputs this skill takes most often:

  | Input | Baseline to compare the SVG against |
  |---|---|
  | Bare slug, or `by-uuid` | `selectedEntityNames` from `.config.json` |
  | Link carrying `country=` (or `focus=`) | the **URL's** list — it overrides the saved selection entirely |
  | MDim view | the URL's `country=` if present, otherwise the view's resolved `selectedEntityNames` read from the grapher DB — `multi_dim_x_chart_configs mx JOIN chart_configs cc ON cc.id = mx.chartConfigId` (`/query-grapher-db`) |

  `life-expectancy.config.json` lists `World, Americas, Europe, Africa, Asia, Oceania`, but `life-expectancy.svg?country=USA~CHN` contains only `China` and `United States` — take the config as the baseline there and the check reports six entities missing and two unexpected, on a chart where nothing is wrong. And note the two sides speak different languages: `country=` takes **ISO codes** while the SVG prints **names**, so resolve the codes before comparing rather than diffing the strings.

  **Never use the rendered labels as their own baseline.** For an MDim view with no `country=` it is the tempting shortcut — `.config.json` 404s on MDim slugs (see Gotchas), so the SVG is the only thing in reach. But Step 8c then compares the SVG against the SVG: an entity grapher silently dropped for having no data in this view is missing from **both** sides, so the must-pass completeness check reports success precisely when it should fail. That is the ten-of-eleven defect further down this page, dressed as a green check. Read the resolved selection from the DB, and if you cannot, **report the completeness check as unavailable** for that frame. An honest gap in coverage is worth more than a check that cannot fail.
