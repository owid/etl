---
name: create-figma-chart
description: Turn an OWID chart — a grapher slug, a customized grapher link, an MDim view, an explorer view, an admin link, a narrative chart, a bespoke (client-rendered React) visualization, or just a description — into a templated chart in the design team's yearly "Charts (YYYY)" Figma file. Exports the chart SVG, creates a new page named "YYYYMMDD Title (Creator)", places the original chart and an adapted template side by side, replicates title/subtitle/data source/note in the template's styles, fits the chart into the template, proposes better labeling (direct line/bar labels instead of legends) and annotations with the file's curvy arrows, and names the final frame with the kebab-case slug used for the website PNG. Also builds the 302-wide "small" and "pull" chart thumbnails that sit in an article's chart-rows and pull-chart blocks, including inside a guided chart. Trigger when the user asks to "create a figma chart", "make a static chart in Figma", "prepare this chart for Instagram / as a data insight image", "put this grapher chart into the Charts file", "make a small chart / pull chart / chart thumbnail", "make a static chart from this bespoke viz", or pastes a grapher/admin/narrative-chart link asking for a designed static version.
metadata:
  internal: true
---

# Create a templated Figma chart from a grapher chart

This skill takes any OWID grapher chart and produces a designed static version in the design team's yearly **Charts (YYYY)** Figma file, following the team's DI Charts Guidelines and the Good Data Viz Checklist.

**The defining principle:** the template is law. You adapt the chart's content *into* the template — you never restyle what the template provides (fonts, colors, spacing, logo, footer layout). Anything you add on top (annotations, direct labels, arrows) uses the file's shared text styles and the Chart colors library, nothing else.

**Model check, before anything else:** the session context names the running model. On **Fable**,
stop before the first Figma call and recommend re-running on **Opus** (or **Sonnet** for a
mechanical re-export or a single text fix); continue only on the user's say-so — this skill is long
chains of design judgment, and a build on the wrong model wastes the shared file's review cycle.

**Load the Figma tool schemas in one `ToolSearch`, before the first Figma call.** Where the
`mcp__Figma__*` tools arrive deferred — a cloud session serves them that way — discovering them one at
a time costs a model turn each, ~7 over a run:

```
select:mcp__Figma__use_figma,mcp__Figma__get_screenshot,mcp__Figma__get_metadata,mcp__Figma__upload_assets,mcp__Figma__search_design_system
```

Add `get_design_context` or `download_assets` when the route needs them; harmless where they are
already loaded.

**In a cloud session `admin.owid.io` never resolves**, and the refusal arrives as a `403` that reads
like an auth failure — so discovering it mid-run costs retries plus a credential hunt that could not
have helped. Two steps reach for it, and
[cloud-sandbox.md](../../docs/cloud-sandbox.md) has the fallbacks: **Step 1's** narrative-chart-by-name
map, which the Datasette row in that same table replaces, and **Step 9's** 3× PNG export and upload,
which move to the user's machine. That export is optional for a full-size chart, but for a **302-wide
small or pull chart the PNG _is_ the deliverable** — a cloud session can build the frame and not ship
it, so say which at delivery.

Nothing else here is slower in a cloud session: the connector's own latency and concurrency measure
the same either way. The wall clock goes on the **turn** around each call — see the Round-trip budget.

**The single checkpoint rule:** the Charts file is a shared design file other people work in. Nothing is written to it before the user has seen the full proposal (page name, template choice, texts, planned label/annotation edits) and explicitly approved. Reading the file to check conventions needs no permission.

Read [GUIDELINES.md](GUIDELINES.md) (sibling file) before editing any chart — it distills the DI Charts Guidelines per chart type and the Good Data Viz Checklist.

> **Paired skill — an update here may oblige an update there, and the reverse.**
> [`/create-static-viz`](../create-static-viz/SKILL.md) writes the `export://static_viz` matplotlib
> step whose SVG this skill picks up, so the two share a contract that lives half in each file. **When
> you change something on this list, check the other skill in the same session and update it too —
> or state explicitly that you checked and no change was needed.** Neither side is allowed to drift
> silently; a stale cross-skill fact is how a run re-derives geometry by trial and error.
>
> | Shared fact | Owner | Consumed by |
> |---|---|---|
> | Template geometry — node ids, sizes, band top, footer starts | [`TEMPLATES.md`](../create-static-viz/TEMPLATES.md) | both |
> | The content box and the band a chart is fitted into | TEMPLATES.md, re-verified here each run | both |
> | Node naming (`gid`s) the step emits, and frame proportions | `/create-static-viz` | this skill's Steps 1/3/7–8 |
> | Which text slots the step fills vs. leaves to the template | `/create-static-viz` | this skill's Step 6 |
> | Type and palette — the step sets neither, this page owns both | this skill | `/create-static-viz` defers to it |
> | The design vocabulary (per chart type, labeling, colors) | [GUIDELINES.md](GUIDELINES.md) | both |
> | How Figma MCP calls are batched, and what is serial | this skill (**Round-trip budget**) | both |
>
> The asymmetry worth remembering: **that skill owns the data, the geometry and the proportions; this
> one owns the type and the palette.** A change that crosses that line belongs in both files.

Two more sibling files own a route each, and both replace rather than supplement the steps below:

| File | When |
|---|---|
| [SMALL-CHARTS.md](SMALL-CHARTS.md) | the output is a **302-wide small or pull chart** — an article thumbnail for a `chart-rows` or `pull-chart` block. Different templates, a free frame height, its own export mode, no fit, an 11px floor, a PNG-to-Cloudflare delivery. |
| [BESPOKE-SVG.md](BESPOKE-SVG.md) | the input is a **bespoke visualization** — a client-rendered React viz with no `.svg` endpoint. Covers getting a chart-only SVG out of one; after that this page applies unchanged. |

This page is the **spine**: the step order, the checkpoints, and the routing. The detail for each
step lives in [`reference/`](reference/) and is read *at* that step, not up front — the whole set is
~180 KB and no run needs all of it.

| Read | When | Covers |
|---|---|---|
| [reference/NODE-MAP.md](reference/NODE-MAP.md) | Step 5, before cloning anything | The yearly Charts file's node ids, the ten templates, per-family slot sizes, header sizing and the band table. Run `scripts/verify_templates.js` from here **every run** — a `DRIFT` verdict stops the run. |
| [reference/TEXTS.md](reference/TEXTS.md) | Step 6 | Filling the template's text slots, and the header reflow that makes the band measurable. |
| [reference/FITTING.md](reference/FITTING.md) | Step 7 | Measuring the band, importing the embed, unwrapping and scaling. The local-SVG restyle route. |
| [reference/LABELING.md](reference/LABELING.md) | Step 8, 8b, and any re-export | Direct labels, highlighting, the palette and its bound styles, annotations and arrows. What to replay after a re-import — `scripts/replay_chart_edits.js` does it in one call, in the right order. |
| [reference/CHECKS.md](reference/CHECKS.md) | Step 8c, before showing anyone | The gate. Every check, and the rule to re-run the pass after the *last* change. `scripts/verify_page.js` runs the mechanical rows in one call and declares what it cannot judge; `scripts/diff_against_template.js` checks the finished frame back **against the template it was cloned from**. |
| [reference/GOTCHAS.md](reference/GOTCHAS.md) | On an error, or grep by symptom | Every known pitfall. Worth one skim before your first `use_figma` call. |

[GUIDELINES.md](GUIDELINES.md) stays eagerly read — it is pointed into from all over this page — but
its per-chart-type conventions are now one file each under
[reference/per-chart-type/](reference/per-chart-type/). **Read only the one for the chart in hand.**

**This file has a size budget: keep the spine under 60 KB and GUIDELINES.md under 80 KB.** They are
read on every run, so a paragraph added here costs every future chart. New detail belongs in the
reference file for its step. After editing any doc in this skill:

```bash
.venv/bin/python .claude/skills/create-figma-chart/scripts/verify_docs.py --structure
# and after moving text between files, prove nothing was dropped:
.venv/bin/python .claude/skills/create-figma-chart/scripts/verify_docs.py --against <ref-before-the-move>
```

`--against` normalizes away heading levels, link depth and pointer rewrites, so anything it reports
as `LOST` is a real instruction that went missing. `REWORDED` findings are for you to accept or reject.

Three sibling skills do the text work this one depends on, and Step 8c calls them: **`/adversarial-data-review`** (is the FAUST true of the indicator, and is the data), **`/check-metadata-style`** (the Writing and Style Guide) and **`/check-metadata-typos`** (codespell). Anything they turn up is an upstream fix in the garden step, not a Figma edit.

## Round-trip budget

**A call costs twice, and the second cost is the one that gets forgotten.** The *call* is a network
hop to Figma's hosted connector — **7–10 s for `use_figma`, ~10 s for `get_screenshot`**, flat
regardless of how big the script is. The *turn* around it — issuing the call and reading its result —
is a model turn, measured at a **~12 s median** (min 0.4 s, mean 19.5 s across 23 consecutive calls).
So an unbatched call costs **~20 s, not ~10 s**, and a run's 120–190 of them come to **~50 minutes**
when nothing overlaps. Nothing else is close: not the SVG exports (0.24 s each locally, 0.9–2.6 s
through a cloud sandbox's egress proxy) and not the response payloads (~1.5 KB per `use_figma`).

**So the unit to minimize is messages, not calls.** A batch collapses both costs at once — the
connector serves the calls concurrently *and* they share one turn. Measured across five sessions by
sweeping each one's call intervals for peak simultaneous in-flight calls: **two sessions never
batched at all** (peak 1, nothing overlapping) and two more barely did (peak 2 and 5, 1–9% of calls
overlapping). One batched heavily — and paid for it; see the ceiling below.

**Fan out independent calls — one message, 4–6 at a time.** The connector serves them concurrently: eight screenshots issued together came back **4.1× faster** than serially (79.7 s of work in 19.4 s of wall clock), and six came back **3.85×** faster (53.1 s in 13.8 s). It admits about four or five at once, and per-call latency inflates past that — 8.2 s for the first of eight, 13.2 s for the last — so **4–6 per message is the sweet spot and more just queues.** This is the `figma-use` skill's own instruction too: issue the N calls in one message, and don't await one before issuing the next.

Reads fan out freely. **Writes only when they target different pages** — a script may switch pages only once, so two `use_figma` writes aimed at the same page in one message race each other.

What is independent — **the batch manifest, keyed by the step that owes it.** Issue each row's calls
in one message, so batching is mechanical rather than a fresh judgment call every run:

- **Step 5 — the page survey.** The page enumeration and `verify_templates.js` go together. Checking N pages means N calls — `page.children` on a page you have not switched to is lazily loaded — and they fan out.
- **Step 8c — the checks.** `verify_page.js` and `diff_against_template.js` are one read-only call each; issue them together, with every pixel probe the pass needs.
- **Step 9 — the delivery renders.** One screenshot per delivered frame, all in one message.
- **The palette harvest.** `search_design_system` caps at ~14 results against a 24-fill palette, so it takes one group query plus ~11 by-name queries. Every one of them is independent.
- **Screenshots of different frames or pages.** Issue them together, then `curl` all the returned URLs in one bash call — **in parallel**, `printf '%s\n' "$U1" … | xargs -P6 -I{} curl -sSL -o …` (six serially is 2.7 s through a cloud sandbox's egress proxy, 0.8 s in parallel) — then Read each. A screenshot is otherwise three tool calls, and a run takes 14–70 of them.
- **Every format in a multi-format run**, and every frame of a `chart-rows` set — separate pages and frames, so the writes fan out as well as the reads.
- **Any survey of N nodes** — but at 4–6, not more. The pass that wrote GUIDELINES.md screenshotted 272 chart-library nodes at **peak 14 in flight**, and its calls averaged **35.5 s** against ~10 s everywhere else. That is the ceiling being exceeded, not the connector being slow: 8.2 s at one in flight, 13.2 s at eight queued, 35.5 s at fourteen.
- **`upload_assets` takes a `count`.** One call returns N single-use `submitUrl`s and the POSTs parallelize, so a two-format run uploads both originals in one call rather than two — and both embeds in one more.
- **The Step 8c property sweeps** — font sizes, stroke weights, dash patterns, fills, polylines. Those are reads of a single page, so they collapse into *one* `use_figma` returning one JSON. `scripts/verify_templates.js` already does exactly this for ten templates.
- **The arrow probe's baseline render.** Only the FULL render is shared across arrows: the other three states of the four-render protocol (no-arrow, no-target, both-hidden) each hide *that pair's* nodes, so they are pair-specific and cannot be reused. N arrows cost `3N + 1` screenshots, not `4N` — and not `N + 2`, which under-collects and produces masks containing another pair's target.

**Measured on one template, end to end: 18 Figma calls.** Of those, 3 went on the footer-conversion
bug now fixed in TEXTS.md and 1 on a wrong guess about the footer's layout, so the same build now
costs ~14 — against ~21 per template on the previous run and 124–188 for a whole chart before any of
this. The two-pass export is where the saving is concentrated: it replaces "export, eyeball, re-export"
with one probe and one solved re-export.

**What is serial for a reason — don't collapse these:**

| Sequence | Why |
|---|---|
| trim → position → read height | `leadingTrim` does not update `height` within the call that sets it (Step 7) |
| original → clone → fill texts → measure band → export embed → fit | the band is not knowable until the real title and subtitle have reflowed the header (Step 3) |
| one page per `use_figma` call | `page.children` on a page you have not switched to returns a short list *without erroring* (Gotchas) |

And a bigger batch is a bigger loss: `use_figma` is atomic, so a script that throws on its last line reverts the whole pass. Stay inside the plugin's ~10-logical-operations-per-call guidance.

**Measuring whether any of this happened: use interval overlap, never a calls-per-message count.** The transcript writes one entry per tool call whether or not the calls were batched, so a calls-per-assistant-message histogram reports `{1: N}` for a provably concurrent run — checked against an 8-call probe that measured 4.12×, which the histogram scored as eight singletons. Sweep `tool_use` → `tool_result` timestamps for peak simultaneous in-flight calls instead, and count how many calls start before the previous one finished.

## Inputs

- **A chart reference**, in any of the forms of the Step 1 table. If the user only describes the chart ("the life expectancy chart with just the US and China"), resolve candidates first and confirm.
- **Or a local SVG already on disk** — typically `etl/steps/export/static_viz/<ns>/<version>/<name>.svg`, emitted by an `export://static_viz` step and handed over by [`/create-static-viz`](../create-static-viz/SKILL.md). Its texts are already baked in and its frame already matches a template, so Step 1's text sourcing and Step 3's export both fall away. See the local-SVG notes in those steps.
- **Or a bespoke visualization** — a client-rendered React viz from `owid-grapher`'s `bespoke/projects/*`, which has **no** `.svg` endpoint at all. [BESPOKE-SVG.md](BESPOKE-SVG.md) covers getting a chart-only SVG out of one; after that it behaves like grapher's `uncaptioned` embed and every step here applies.
- Optionally, **the DI/article text** the chart accompanies — the best source for annotation content. Ask for it if annotations are wanted and it exists.
- Optionally, **a link to a finished page in the file to work like** (see below).
- Everything else (formats, credit, slug, topic link) is collected once in Step 2.

**One output format has its own file.** A **small or pull chart** — the 302-wide thumbnail that sits in an article's `chart-rows` or `pull-chart` block — diverges from everything below at almost every step: a different pair of templates, a free frame height, its own export mode (`imType=thumbnail`), no fit, an 11px text floor and a PNG-to-Cloudflare delivery. Read [SMALL-CHARTS.md](SMALL-CHARTS.md) instead of improvising from this page, and take only the shared conventions (GUIDELINES.md, the Step 8c checks it doesn't override) from here.

### When you're pointed at a finished page as the model

**Read [reference/REFERENCE-PAGE.md](reference/REFERENCE-PAGE.md) for this mode.** A designer's own
page is a better spec than this file, and reading it answers most of Step 2 in measurements. That
file covers which template and export route the page used, what was done by hand, the trap of
reporting *their* numbers as your own, reading small details off a rendered crop rather than the
vector geometry, re-rendering the chart yourself before assuming it reproduces, treating the page as
stale (on a five-page run every one of them was), and how to read a designer's rework — or a loose
TEXT node dropped beside your frame — as the feedback it is.

## Step 1 — Resolve the chart and gather its text

Get an SVG URL for the chart, whatever form the reference takes:

| Input | SVG URL |
|---|---|
| Slug or default grapher link | `https://ourworldindata.org/grapher/<slug>.svg` |
| Customized grapher link (query params) | insert `.svg` before the `?`, keep the query verbatim: `https://ourworldindata.org/grapher/<slug>.svg?country=USA~CHN&time=1990..latest` — `country`, `time`, `tab`, `stackMode`, `region`, `focus`, … are all honored, and slug redirects work |
| MDim view | same — the dimension params select the view: `.../energy-mix.svg?metric=per_capita&source=coal` |
| **Explorer view** | `https://ourworldindata.org/explorers/<slug>.svg?<view params>` — `EXPLORER_DYNAMIC_THUMBNAIL_URL` in `settings/clientSettings.ts`. **Carry the view's full param set:** requested bare it returns an axis and nothing else, at HTTP 200 (2 texts, no series). Verified on the `imType=thumbnail` route; untested for the other `imType`s. |
| **Bespoke component** (no slug, no `.svg`) | there is no endpoint — render and serialize the component yourself. See [BESPOKE-SVG.md](BESPOKE-SVG.md). |
| Admin link `/admin/charts/<id>/edit` | **`/admin/charts/<id>.svg` does not exist** (it returns the admin SPA shell). Resolve the chart's `configId` — `SELECT configId FROM charts WHERE id = <id>` on the public Datasette (see the `query-grapher-db` skill), or `GET /admin/api/charts/<id>.config.json` — then use `https://ourworldindata.org/grapher/by-uuid/<configId>.svg`. Works for unpublished drafts too. |
| Narrative chart (**name**) | name → uuid via the unauthenticated map `https://admin.owid.io/api/narrative-chart-map`, then `https://ourworldindata.org/grapher/by-uuid/<uuid>.svg`. **In a cloud session this host never resolves** — take the Datasette route in the row below instead |
| Narrative chart (**admin link with a numeric id**, `/admin/narrative-charts/<id>/edit`) | **Try the direct lookup first** — `select id, name, chartConfigId from narrative_charts where id = <id>` on the public Datasette hands you the uuid outright (note the column is `chartConfigId`, not `configId`). Only when the id isn't mirrored yet do you need the guessing route below. |
| … the same, when the id is **newer than the Datasette mirror** | there is no id→uuid endpoint, and the mirror lags production by days (it once stopped at 338 while 341 existed). Diff the live name-keyed map against `select name from narrative_charts` to get the unmirrored names, then order them by uuid — they are **uuidv7, so lexical order is creation order** — and count up from the mirror's highest id. That gives a *candidate*, not an answer: ids have gaps where charts were deleted. **Always render the candidate and have the user confirm it before building.** In practice the **name is a far stronger signal than the id arithmetic** — these are named after the piece they serve (`share-of-women-in-parliament-di`), so an unmirrored name matching the DI's topic, *and* carrying the highest uuid, is near-certain. Note the DI page itself is not a reliable route: an older published DI can have `linkedNarrativeCharts: {}` because it ships a hand-made PNG, so the narrative chart you were handed may be newer than the post. Its embedded JSON is still worth reading for `grapher-url`, `authors` and the body text you need in Step 2. |
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

## Step 2 — Ask the run options, all at once

One `AskUserQuestion` batch — don't drip-feed:

1. **Output format(s)** (multi-select — several deliverables from one run are normal). Constraint from the design team: **Instagram and DI images are always square/mobile**; a static chart (for the OWID website) can be desktop and/or mobile:
   - Instagram post (square 540×540) or portrait (560×700)
   - Data insight image (DI_Template, 540×540)
   - Static chart — mobile/square (540×540 or 540×824) and/or desktop (Horizontal 850×638 / Vertical 850×1095; Vertical when the chart needs height — rankings, long bar lists)
   - **Guided / `chart-rows` thumbnail** (302 wide, free height, no source row) or **pull chart** (302 wide, free height, mandatory source) — both go to [SMALL-CHARTS.md](SMALL-CHARTS.md), which owns the rest of the run. The answer picks the template, so ask which block the image is for rather than inferring it from the size.
2. **Who is building the chart** — this is the page-name credit, and it is **not** the author of the DI or article the chart accompanies. Default to the user; don't infer it from the gdoc, which names the writer rather than whoever does the design work.
   - **First names only**, matching the file's existing pages: `(Charlie)`, `(Hannah)`, `(Bertha)`.
   - **Disambiguate a shared first name with the last initial** — `(Pablo A)` for Pablo Arriagada, `(Pablo R)` for Pablo Rosado. Both are in use, so a bare `(Pablo)` is ambiguous.
   - **Several people, comma-separated**: `(Bastian, Charlie)`.
   - An organization instead of a person when there is no individual: `(Our World in Data - Global Change Data Lab)`.
3. **The author of the piece**, separately, and only when a **static template — desktop *or* mobile** — is among the formats. Those carry a `Licensed under CC-BY by the author <Name>` line, and that name is the writer being credited for the work, which is often *not* the person building the chart. Mobile gained this line on 2026-08-13 along with its second footer row, so a mobile-only run needs the question too; skip it for DI and Instagram, whose footers say only `CC BY`, and for the 302-wide formats, which have no footer at all.
4. **The DI's own title — or the claim the image is meant to make.** Ask for this whenever a DI or Instagram image is among the formats, and ask *independently of annotations*: grapher's descriptive title must not survive into those images (GUIDELINES.md → Titles), and the story is not yours to invent. If there's no title written yet, ask for the sentence the image supports and derive a candidate from it for approval in Step 4.
5. **Annotations** — should the chart carry annotations replicating what the accompanying text says? If yes, ask for that text (DI draft, article paragraph).
6. **Topic page** for the `OurWorldinData.org/[Topic]` footer line — default from the config's `originUrl`. Instagram only; no other template carries the line.
7. **Slug** for the final frame — short, kebab-case (`child-mortality-asia-decline`). It becomes the PNG filename when the frame is exported for the website. Propose one; let the user override. For a 302-wide thumbnail it is instead `<grapher-slug>-thumbnail`, which has to be unique across every OWID image — see SMALL-CHARTS.md → Delivery.
8. **Entity names or values?** — 302-wide formats only. `imMinimal=1` replaces the entity labels with their values, which reads well when the surrounding prose already names the entities and badly when it doesn't. Default to keeping the names (SMALL-CHARTS.md → `imMinimal`).

## Step 3 — Export the SVGs

> **Local SVG on disk: there is nothing to export.** When the input is a file from an
> `export://static_viz` step, skip this whole step. The step chose its own `figsize` to match a
> template's proportions, so none of the `imType` / `imFontSize` / `imWidth` aspect solving below
> applies, and there is no chart-only "embed" to export — the file already *is* the framed chart,
> carrying its own title, subtitle, `Note:`, `Data source:` and license at that template's own slot
> positions. So the two assets below come from the step's own output rather than from a `curl`: the
> **PNG** it emits beside the SVG is the flat reference copy for the page, and the **SVG** is what
> goes into the template clone. `upload_assets` takes a local path unchanged. Then follow the
> local-SVG route in Steps 5 and 7 — it replaces the measure-solve-export-fit ordering entirely,
> because a frame that already matches the template has nothing left to solve.

Two exports per format family: the **original** (placed on the page as the reference copy) and the **embed** (chart area only, placed inside the template).

**Export the original now; export the embed only after the template texts are in (Step 6).** The embed's aspect ratio has to match the band between the header and the footer, and the header's height is not known until the real title and subtitle have reflowed it — a three-line title moves the band by ~30px, which is twice the whole gap budget. The order that avoids re-exports is: original → clone the template → fill the texts → **measure the band** → export the embed → import and fit. Every page in this run that skipped ahead needed a second export.

```bash
DIR=/tmp/figma-chart && mkdir -p $DIR   # or the session scratchpad

# Original — desktop templates (850×600 default render)
curl -sL "https://ourworldindata.org/grapher/<slug>.svg?<params>&nocache" -o $DIR/original.svg
# Original — square/mobile templates (grapher's own square re-layout, 540×540)
curl -sL "https://ourworldindata.org/grapher/<slug>.svg?<params>&imType=square&nocache" -o $DIR/original_square.svg

# Embed — chart area only, no grapher header/footer, at the template chart-area aspect ratio
curl -sL "https://ourworldindata.org/grapher/<slug>.svg?<params>&imType=uncaptioned&imWidth=<W>&imHeight=<H>&nocache" -o $DIR/embed.svg
```

`imWidth`/`imHeight` set the **aspect ratio only** — the server renormalizes the SVG to ~510k px², so you cannot request a bigger SVG (irrelevant: it's a vector; you scale it in Figma). Sanity-check what came back:

> **This is true of the default and `uncaptioned` routes only.** `extractOptions` (`functions/_common/imageOptions.ts`) **returns early** for `imType=thumbnail` and `imType=square`, so neither the `MIN/MAX_ASPECT_RATIO` clamp nor the ~510k normalization runs on them. On the thumbnail route `imWidth`/`imHeight` set the size outright — `staticBounds` becomes `imWidth/4 × imHeight/4` — which is what lets a 302-wide small chart arrive at exactly 302px and skip the Figma rescale entirely (SMALL-CHARTS.md → The export).

```bash
head -c 300 $DIR/embed.svg   # expect <svg ... width="..." height="...">, no <html
```

> **[`scripts/solve_export.py`](scripts/solve_export.py) does this arithmetic — don't do it by hand.**
> Run it from the repo root through the venv — `.venv/bin/python .claude/skills/create-figma-chart/scripts/solve_export.py …`;
> it is committed non-executable like the rest of that directory.
> `--band 508x371 --slug <slug>` returns the solved `imFontSize`, the `imWidth`/`imHeight` to
> request, the predicted content box, the **height-first** scale into the band, the leftover width
> the x-map has to close, the final label size, and the finished `curl`. Two things to read it by:
> it reports the leftover width rather than a predicted gap, because the gap is exact by
> construction once you fit the height (Step 7) — that leftover is the same quantity
> `measure_fit.js` reports as `xMapShortfall`, and it is the aspect miss expressed in px; and every
> number comes from the **rounded** `imFontSize`, since that is what the URL carries, so the label
> size quoted is the one the `curl` will actually produce (it prints the ideal font alongside when
> rounding moves it). It is a TWO-PASS tool: `--band` alone is pass 1, a probe under the symmetric
> `1.4 × imFontSize` inset model; pass 2 re-runs with `--declared`/`--ink`/`--im-font-size` measured
> off the probe's import and is exact, because the real inset is per-axis, not symmetric (see Step 7
> and reference/FITTING.md). It also carries its own `--self-test` (the worked examples, the band
> round-trip, and a real run's two measured-inset passes) and the `--thumbnail` route for a 302-wide
> chart.
>
> **It solves for `band − 2×--gap`, not for the band** — the gap below is a requirement of the fit,
> so a solve that ignores it lands the chart edge to edge and you re-export. `--gap` defaults to 14
> and takes 30 for the Instagram portrait; a 508×371 band makes the target 508×343, 14px at each
> end. The canvas model is confirmed against the real renderer — a `--gap 0` solve predicted 828×616
> and grapher returned **829×616**, landing labels at exactly 13.5px — so it is the target fed into
> it that the gap changes. After you have measured a real import, run the `nextPass` command that
> `measure_fit.js` prints — with its `CONFIG.declared` and `CONFIG.imFontSize` set from the probe,
> it is the exact measured-inset second pass rather than another guess — see Step 7.

**The aspect you request is the *canvas*, not the chart — solve for the padding or you will re-export every page.** Grapher insets the drawing inside the SVG it hands back, so the group Figma imports is smaller than the declared size, and it is the *group* that has to fill the template band. Measured on this file's charts, the inset is close to **1.4 × `imFontSize` on each axis** (at `imFontSize=32`: declared 901×566 → content 857×520; at 30: 862×591 → 818.9×550). So don't request the aspect you want — request the aspect that *yields* it, by solving

```
(W − 1.4F)/(H − 1.4F) = contentWidth / contentHeight      with   W·H ≈ 510000
```

for `H`, then `W = 510000/H`, and pass `imWidth=round(W/H × 1000)&imHeight=1000`. Taking the declared aspect at face value produced a 336.9px chart where 343 was needed — a 17px gap against a 14px target, on the first page of this run. The model is approximate (±3px), so **measure the imported group and expect at most one correction**; a naive request costs one re-export *per page*.

**Then be ready for the target itself to move: hiding furniture changes the group's aspect.** `connectors` extend to the right of the plot, so hiding them (Step 8) narrows the group and makes it relatively *taller* — the same export that was solved for a 1.6026 content aspect measured 1.5558 once the elbows were hidden, turning a 14px gap into 9.5px. Hide the connectors and the year markers **before** you measure and scale, not after, and re-read the aspect from the group you are actually going to fit.

> **Square charts, second route:** grapher's `imType=square` render re-lays out the chart for a square canvas (legend placement, font sizing tuned by the web team). When that layout is better than the uncaptioned crop — commonly for maps and charts with big legends — import the full square SVG instead and delete its `header` and `footer` groups in Figma after import. Offer both routes; pick per chart.
>
> **For a 540-wide template this is often the route to prefer, not the fallback — export both and measure before choosing.** The square render is already sized for the frame you are filling, and that can remove two whole steps: on one chart its chart area came out ≈505×328 with every label at exactly **15px** — a value on the annotation ladder — so there was no `imFontSize` to tune and no rescale at all (see Step 7 on why not rescaling is worth engineering for). Reaching the same 15px through `imType=uncaptioned` took `imFontSize≈36`, and that export also spent more of the frame on furniture: the same chart came back with a plot **279px** tall against the square route's **294.6px**, and a wider reserved right margin. Compare the two on three numbers — final font size at the template width, plot height, and plot width — rather than on which one is nominally "the embed".
>
> **The check that decides it is the plot's height against the band, and the square route often loses it.** The square export lays the chart out under grapher's *own* header and footer, which are not the template's — so its chart area is sized for a band you are not filling. Across five DI pages the square route came back **314.9px** tall for a **371px** band: a 28px gap at each end, twice the 12–16px target, with no way to close it except a rescale that then breaks the width. The square route wins when its chart area happens to fill your band (short template header, or a map/big-legend chart whose square re-layout is genuinely better); the solved uncaptioned aspect wins whenever it does not. **Measure the band first (Step 7), then pick** — and note the band is only knowable *after* the template texts are in, which is why Step 6 comes before the embed export.

**Size the text at export time with `imFontSize` — scaling in Figma cannot fix it.** Grapher picks a base font for the canvas it renders (`max(10, height/25)`, so ~24 for the default uncaptioned export), and every label is derived from it — the segment values and country names land at about **0.75 × the base**. Placing that export at 508px wide shrinks all of it by the same factor, so a default export ends up with ~12px labels: legal, but on the floor of the 12px minimum. Ask for a bigger base instead — `imFontSize=28` gives ~13.5px labels and ~14px legend text in a 540 frame, which matches the template's own 14px source line. Check the export before importing:

```bash
grep -oE 'font-size="[0-9.]+"' chart.svg | sort | uniq -c | sort -rn | head -3
# multiply the most common value by (508 / the export's content width) to get the final size
```

Bigger text needs more room, so this trades against how much fits — see the axis rule in Step 8 and, failing that, the entity count.

**`tab=` FALLS BACK SILENTLY when the chart does not declare that type — check the mark group, not the HTTP status.** `co-emissions-per-capita?tab=marimekko` returns 200 and a plausible 9 KB SVG containing `lines`: a line chart. `tab=stacked-discrete-bar` on the same slug returns `stacked-areas`. Nothing errors and the file looks right, so a whole "chart-type sweep" can be built on two charts that are not the types you asked for — which is what happened here. Two defences: find charts that actually declare the type (`json_extract_string(cc.full, '$.chartTypes')` on the public Datasette — `/query-grapher-db`), and assert the mark group in the returned SVG:

| type | mark group `id` |
|---|---|
| line | `lines` |
| discrete bar | `bars` + `entity-labels` + `value-labels` |
| stacked discrete bar | `bars` |
| stacked area | `stacked-areas` |
| slope | `slopes` |
| scatter | `points` |
| marimekko | `marimekko-chart` |
| map | `map` |

Caveats: `?tab=table` is silently ignored (renders the default tab); `imSquareSize` affects PNG only; add `nocache` when re-exporting after a config change.

## Step 4 — Propose, then get the go-ahead

> **If the title changes later, rename the page too.** The page name carries the *final* title, and a
> title that gets corrected mid-run — because it misread the data, or because it wrapped to a line too
> many — leaves the page still asserting the superseded claim. It is the one place the old wording
> survives a retitle, since nothing renders it.

Before touching the file, show the user in one message: the page name **`YYYYMMDD <Title> (<Creator>)`** (today's date, the *final* — possibly rewritten — title), the chosen template(s), every text that will go into the template, the labeling changes you propose (Step 8), and the annotations with their content. **Wait for explicit approval.** This is the single checkpoint; after it, iterate freely on the same page without re-asking.

### Two things to ask about, not decide

Both change what the reader sees, both are cheap to do and awkward to undo, and neither has a right
answer you can work out from the data. Ask them **with the numbers from the chart in hand**, and ask
in plain words — no "antimeridian straddler", no "content width", no pixel arithmetic. The person
answering needs to know what changes and what it costs, nothing else.

**1. Small islands on a map.** A world map often sits small in its space because a few tiny islands
reach out to the far left and right edges. Leaving them out lets the map fill the width.

> *"The map is sitting small because a few tiny Pacific islands stretch out to the edges. I can leave
> them out — Hawaii, Fiji, Kiribati, Samoa, Tonga, Tuvalu, Nauru and the Marshall Islands — which makes
> the map about 14% bigger. Those places would no longer be drawn; most are under 2px across here, so
> they are barely visible either way. Leave them out, or keep everything and accept the smaller map?"*

Details, and what it measured last time, in [reference/per-chart-type/maps.md](reference/per-chart-type/maps.md).

**2. Long country names.** This applies to **any chart that labels entities**, not just bar charts —
what differs is where the space goes. On a bar chart the names sit in a column on the left that is as
wide as the **longest** name, so shortening anything *but* the longest one gains nothing. On a slope or
line chart the labels eat into the plot at the end they sit on. On a scatter or a marimekko there is no
column at all and the gain is fewer collisions, so a shorter name can be worth it even when no edge
moves. Work out what the labels are actually costing before you ask
([reference/LABELING.md](reference/LABELING.md) has the per-type version).

`US` and `UK` are settled: the Writing and Style Guide rules on those two, without periods
([STYLE_GUIDE.md](../check-metadata-style/STYLE_GUIDE.md)). For any other name you may **propose** a
short form, provided it is one a reader would already recognise — and propose it, never apply it. The
line to hold is between a *common* abbreviation and an *invented* one: a reader cannot tell which is
which, so anything they would not recognise reads as data rather than as our layout choice. If you are
not confident it is in common use, say so in the question and let the user decide.

Measure first, then ask. When the two settled ones are enough:

> *"The names on the left take up a lot of room, and the widest one decides how much. I can shorten
> 'United States' to 'US' and 'United Kingdom' to 'UK', which makes the bars about 5% longer. Shorten
> them, or keep the full names?"*

When the widest name is a third country, name the short form you have in mind and own that it is a
suggestion:

> *"The widest name is what sets that column, so shortening 'United States' to 'US' alone would not
> make the bars any longer. If we also shorten the widest one — I'd suggest <short form>, but tell me if
> that is not how we write it — the bars get about 5% longer. Shorten them, or keep the full names?"*

The measurements, and the four-step re-layout that keeps the bar lengths proportional to the data, are
in [reference/per-chart-type/bar.md](reference/per-chart-type/bar.md).

## Step 5 — Create the page and place the pieces

> Template ids, sizes and the band table are in [reference/NODE-MAP.md](reference/NODE-MAP.md) — and `scripts/verify_templates.js` runs from there before you clone.

> **Load the `figma-use` skill before any `use_figma` call** — hard prerequisite. It covers `loadFontAsync` before text edits and the other plugin-API rules.

1. **Enumerate pages with `use_figma`**, not `get_metadata` — the MCP page listing is unreliable (it returns only "Cover" for this file). Dated chart pages sort newest-first after the instructions/templates pages; insert the new page at the top of the dated block, matching the existing order. **The dated block starts *below* a divider page named `-----------------------------------------`** — find it (`/^-{10,}$/` on the trimmed name) and insert after it rather than at a counted index, or the new pages land among the instruction pages. Watch the off-by-one when you move several: `insertChild(i, p)` removes before it inserts, so re-inserting pages that are already at those indices is a no-op — the reliable fix is to place the pages and then move the *divider* to its own index once.

```js
const pages = figma.root.children.map((p, i) => `${i}: ${p.name}`)
const page = figma.createPage()
page.name = "20260810 Child mortality in Asia (Pablo A)"   // creator, first name + last initial if ambiguous
// move it to the right index with figma.root.insertChild(index, page)
await figma.setCurrentPageAsync(page)
```

2. **Clone the template frame(s)** onto the new page: `(await figma.getNodeByIdAsync("<template-id>")).clone()`, then `page.appendChild(clone)` and position it.

   For a **302-wide small or pull chart**, clone `25344:1357` (guided) or `25344:1391` (pull) — the choice is the Step 2 answer, not a judgement. Both now carry a real visible white frame fill and no background vector, so there is nothing to repair before you start; check that still holds rather than assuming, since a hidden fill plus a fixed-size backing vector is exactly what a taller frame under-covers. A `chart-rows` block is 3–5 rows, so expect a *set* of frames on one page. SMALL-CHARTS.md → In Figma has the rest.

3. **Import the original SVG with `upload_assets`** — never `createNodeFromSvg` (the `use_figma` code param caps at 50k chars; a grapher SVG is ~165 KB). `upload_assets` takes a **`count`** and returns that many single-use `submitUrl`s — pass `count: 2` for a two-format run and POST both in parallel. Keep each returned `placedOnNodeId`. **Only the original at this stage** — the embed has not been exported yet (Step 3), and it arrives in Step 7 once the band is measurable:

```bash
curl -s -X POST "<submitUrl>" -F "file=@$DIR/original.svg;type=image/svg+xml"
# → {"success":true, ..., "placedOnNodeId":"<id>"}
```

4. **Lay out the page**: the original chart on the left; the adapted template **to its right** (~100 px gap). If several formats were requested, keep one original and line the templates up to its right. Move imported nodes with `use_figma` (`page.appendChild(node)`, set `node.x/node.y`). This page-level parenting is for the **original** reference chart only — the embed gets reparented into the template clone in Step 7.

> **Imported SVGs arrive at their natural size** (850×600 / 540×540). Scale with `node.rescale(factor)` — never `resize()`, see Step 7.

**Bin the import frame — on every import.** `upload_assets` wraps the SVG in a FRAME that OWID's charts don't have and that causes two failures of its own: it carries a **white fill** that paints over the footer as soon as the frame overhangs, and `resize()` on it *stretches its children through their constraints* — which silently rewraps every text box in the chart, because grapher's exported labels are sized to their glyphs with no slack ("Brazil" becomes "Bra zil"). Move the frame's children out to their real parent and delete the frame; a plain group is what the finished pages use. Only the destination differs: the **original** unwraps onto the page, here in Step 5; the **embed** unwraps into the template clone in Step 7, after its own export.

```js
const unwrap = (imported, parent, name) => {        // parent = the page (original) or templateClone (embed)
  const kids = [...imported.children];
  for (const k of kids) parent.appendChild(k);
  imported.remove();
  const node = kids.length === 1 ? kids[0] : figma.group(kids, parent);
  node.name = name;
  return node;
};
```

> **Local-SVG route.** Two imports, and neither is an embed: place the step's **PNG** on the page as
> the left-hand reference copy, and unwrap the **SVG** straight into the template clone here in Step 5
> rather than waiting for a band measurement in Step 7. Waiting buys nothing — the SVG's frame already
> carries the template's *aspect*, so there is no aspect to solve against the header and footer. It
> does not arrive at the template's *size*, though: Step 7 still owes it one uniform rescale to the
> clone's width. Keep the clone, too: the SVG's text is matplotlib's, not in the file's bound
> Lato/serif styles, so Step 6 still fills the template's own slots and Step 7 then drops the SVG's
> duplicate text nodes.

## Step 6 — Fill the template texts

> **Read [reference/TEXTS.md](reference/TEXTS.md) for this step.**

Replace the cloned template's lorem-ipsum text nodes with the chart's title, subtitle, `Note:` and `Data source:`, in the template's own bound styles. Filling them **reflows the header**, which is what makes the band measurable in Step 7 — so this comes before the embed export.

## Step 7 — Fit the chart into the template

> **Read [reference/FITTING.md](reference/FITTING.md) for this step.**

Measure the band off the *filled* clone, export the embed to that aspect, import it, unwrap the frame, and scale it in. Covers the local-SVG restyle route and `scripts/restyle_static_import.js`.

## Step 8 — Improve the labeling and annotate

> **Read [reference/LABELING.md](reference/LABELING.md) for this step.**

Direct labels instead of legends, the highlight treatment, the palette and its bound styles, annotations and the file's curvy arrows — plus what to replay when a re-import wipes the chart-local edits.

## Step 8c — The checks that must pass before you show it

> **Read [reference/CHECKS.md](reference/CHECKS.md) for this step.**

The checks are a gate, not a formality: **re-run the whole pass after the last change**, not after each one, and treat "I already checked that" as false after any re-export, reorder, rescale or restyle.

## Step 9 — Checklist pass, review, deliver

1. Run the **Good Data Viz Checklist** (GUIDELINES.md, final section) against the composed frame; fix what fails.
2. `get_screenshot` the new page and show the user — original and adapted version side by side. Iterate on feedback (no re-approval needed within the approved page).
3. **Rename the final frame to the slug** from Step 2 (`child-mortality-asia-decline`) — Figma uses the frame name as the export filename for the website PNG. **Exactly one frame carries the bare slug**; variants get a suffix (`-palette-a`). Two frames with the same name export two files with the same name.

   **When the user picks a variant, move the bare slug onto it in the same breath — never leave the rename as an open item.** It reads like a one-line loose end and it is not: the page ends up with a single finished frame still called `…-palette-a`, and the PNG the website gets is named after a trial. Renaming is free while the choice is being made and invisible afterwards.
4. **Clear the rejected variants off the page.** Proposal frames accumulate fast — a palette trial, a labeling trial, a layout trial — and a page with four near-identical charts makes the reader work out which one is live. When the user picks, delete what they didn't pick and keep what they asked to keep; a variant kept deliberately is fine, one left behind by accident is not.
5. Do **not** export a PNG by default — the designer usually keeps editing. On request, let the user export from Figma, or use the admin's Figma endpoint below. **`get_screenshot` cannot do it:** `maxDimension` only ever *downscales* and clamps at the node's natural size, so a 540 frame returns 540px however large a number you pass, and a 302 frame returns 302px. There is no way to get the 4× (2160×2160) DI export through it.

   For a **302-wide thumbnail** the export is part of the deliverable rather than optional, and it has its own route: `GET /api/figma/image?fileId=<key>&nodeId=<node>` on the OWID admin (`adminSiteServer/apiRoutes/figma.ts`) calls the Figma API at `scale: 3`, then `POST /api/images` uploads it to Cloudflare Images. PNG only — `ACCEPTED_IMG_TYPES` rejects SVG. See SMALL-CHARTS.md → Delivery for the naming rules and the retina reason for 3×. **Neither call reaches `admin.owid.io` from a cloud session**, so there the export and upload move to the user's machine — say that when you deliver instead of leaving the deliverable half-finished.
6. **Give the user a clickable link to the frame — once, when you first create it.** `https://www.figma.com/design/<fileKey>/<FileName>?node-id=<node-id>`, with the node id's colon written as a hyphen (`24977:6` → `node-id=24977-6`). Deep-link the **frame**, not the page: it opens with the chart on screen rather than wherever the canvas was last parked. A first delivery without the link is not delivered — making someone hunt for a page in a 180-page file is pure friction. But **don't repeat it on every iteration**: they already have the tab open, and a link at the top of every reply is noise. Re-send it only if the frame moves to a new page or they ask.
7. Report what was created (page name, frames, edits made) and what remains manual: the Flags plugin if it was used, and any design review — **you cannot read Figma comments via MCP, so never report the design review as clean.** Deviations and open items go in the report and the handover doc; put them on the Figma page **only if the user asks** — an unrequested note is clutter in someone else's design file.
