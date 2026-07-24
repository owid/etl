---
name: edit-faust-metadata
description: Edit user-facing chart and indicator text (FAUST — Title, Subtitle, Footnote — plus description_short, description_key, units, display.name, attribution_short, entity selection, and any other user-facing metadata) from a conversational request in the terminal. Accepts a chart or MDim referenced by live link, staging preview link, admin link, bare slug, chart id, or indicator catalogPath. Routes each edit to the right layer (garden .meta.yml, MDim yaml/py, or chart config via the admin API — ALWAYS on staging, never production), reports the blast radius on other charts/MDim views/explorers before applying shared-metadata changes, and ships via a PR with an automated @codex review loop. Trigger on "change the subtitle of <link> to …", "fix the footnote on this MDim view", "edit the units / description_key / selected countries of …", or any pasted grapher/staging/admin link plus an edit request. Also covers the legacy audit mode ("dump/audit the FAUST for dataset X", "review the text of all views in this MDim") — a Markdown dump + compare workflow for massive changes, entered ONLY on explicit request.
metadata:
  internal: true
---

# Edit FAUST & metadata

Edit the user-facing text of charts, MDims, and indicators from a plain request in the terminal, iterating **always on a staging server** — production is never written to directly. The skill is designed for both data scientists and non-coders: the target can be referenced by any link the user has at hand.

Two modes:

- **Edit mode (default)** — conversational edits: resolve the reference, route the edit to the right layer, check the blast radius, apply, verify on staging, and ship through a PR with an automated Codex review loop.
- **Dump + compare mode (only on explicit request — never offer it)** — generate a Markdown FAUST report for one or many charts/indicators, let the user edit it as the desired target state, then reconcile the live config against it. See [Dump + compare mode](#dump--compare-mode-explicit-request-only).

## Field scope and critical inheritance rules

**Chart title / subtitle / footnote** resolve ONLY from `presentation.grapher_config.{title, subtitle, note}`. Do NOT fall back to `variable.title`, `presentation.title_public`, `display.name`, or `description_short` — those are data-page fields and produce text that does not match what Grapher actually renders.

**description_short / description_key** resolve from the namesake top-level fields on `VariableMeta` — not from `grapher_config`.

Not every chart has `presentation.grapher_config` populated: some charts are edited only in the admin DB, so the ETL metadata looks empty. Treat those fields as chart-level (see routing below) rather than inventing a fallback. See `.claude/projects/-Users-parriagadap-etl/memory/feedback_chart_faust_inheritance.md` for the full rule.

Field mapping — user vocabulary → where the field can live:

| User says | Config key(s) | Chart-config-expressible? | Indicator-level (ETL meta.yml)? |
|---|---|---|---|
| title | `title` | yes (patch) | `presentation.grapher_config.title` |
| subtitle | `subtitle` | yes (patch) | `presentation.grapher_config.subtitle` |
| footnote / note | `note` | yes (patch) | `presentation.grapher_config.note` |
| description / "About this data" | `description_short`, `description_key` | no | yes (top-level VariableMeta) |
| WYSK / "What you should know about this indicator" | `description_key` | no | yes (top-level VariableMeta) |
| unit / short unit | `unit`, `short_unit`, `display.unit` | display-only via dimensions | yes |
| legend / series label | `dimensions[i].display.name` | yes (patch) | `display.name` |
| public title (data page) | `presentation.title_public` | no | yes |
| source shorthand | `presentation.attribution_short` | no | yes (does NOT inherit from origin — set explicitly) |
| selected countries / default view | `selectedEntityNames`, `selectedEntityColors` | yes (patch) | `presentation.grapher_config.selectedEntityNames` |
| axis labels, map settings, colors | `xAxis`/`yAxis`, `map.*`, `baseColorScheme` | yes (patch) | `presentation.grapher_config.*` |

"Chart-config-expressible" = the field can be set on an individual chart's config (its `patch`). Indicator-only fields (description_short/key, units, title_public, attribution_short) are read by every surface that uses the variable — editing them always has a potential blast radius.

## Step 0 — resolve the reference

Run the resolver on whatever the user pasted:

```
.venv/bin/python .claude/skills/edit-faust-metadata/scripts/resolve_target.py <reference> [--branch <b>] [--json] [--no-db]
```

Accepted references:

| Input | Example | Resolves to |
|---|---|---|
| Live chart URL | `https://ourworldindata.org/grapher/life-expectancy?country=FRA` | chart (slug) |
| Staging chart URL | `http://staging-site-my-branch/grapher/life-expectancy` | chart (slug) |
| Admin chart edit URL | `https://admin.owid.io/admin/charts/104/edit` | chart (id) |
| Admin collection/MDim preview URL | `.../admin/grapher/wb%2Flatest%2Fincomes_pip%23incomes_pip?indicator=mean` | mdim / mdim-view |
| Bare slug | `life-expectancy` | chart or mdim |
| Chart id | `104` | chart |
| Indicator catalogPath | `grapher/wb/2026-03-24/world_bank_pip/incomes#mean__...` | indicator |
| Explorer URL | `.../explorers/poverty-explorer` | out of scope → point to `/create-explorer` |

The resolver needs the branch's staging DB for slug/id lookups (`--no-db` does parse-only identification, useful before the staging server exists). It reports: kind, chart id/slug/published state, `isInheritanceEnabled`, which top-level keys are in the chart's `patch`, the variables on the chart (with whether each has an ETL grapher config), matched MDim view + its overrides, the candidate ETL files to edit, and ready-made staging/admin URLs.

Notes baked into the resolver — don't re-derive them by hand:

- MDims are served at `/grapher/<slug>` too — slug lookups check `multi_dim_data_pages` after `charts`.
- Old slugs resolve through `chart_slug_redirects`; the canonical slug is reported.
- Duplicate slugs prefer the published chart; editing an unpublished chart gets a warning.
- MDim choice values can carry deliberate trailing spaces — dims are matched stripped but written back raw (memory: `reference_mdim_choice_name_trailing_space`).
- Never hand-build `staging-site-<branch>` hostnames — branch names get normalized and truncated to 28 chars (`etl.config.get_container_name`); a wrong name silently serves a different environment.
- When a `<short_name>.meta.override.yml` exists next to the meta.yml, the resolver lists it first — that's the manual-curation surface (the main meta.yml is likely auto-generated; see the route (a) note below).

## Edit routing — which layer gets the edit

Primitives:

- **Explicitly set at chart level** ⇔ the key exists in `chart_configs.patch`. Never judge this from `full` — `full` has every inherited value merged in, so an inherited title looks identical to an override there (`etl/indicator_upgrade/indicator_update.py:207`).
- **Inheritable** ⇔ the chart's primary y variable has an ETL grapher config (`variables.grapherConfigIdETL IS NOT NULL`) and that config carries the field.

Three routes:

- **(a) Indicator ETL metadata** — edit the garden `.meta.yml` → rebuild garden+grapher → `STAGING=1 etlr grapher://grapher/<ns>/<ver>/<ds> --grapher` to upsert to staging. **Check for a `<short_name>.meta.override.yml` next to the meta.yml first** — the ETL merges it on top of the built metadata automatically (`etl/steps/__init__.py`), and datasets that carry one (WDI is the flagship: `wdi.meta.override.yml`) auto-generate their main `.meta.yml`, so manual curation MUST go into the override file — an edit to the auto-generated file builds fine but is silently lost on the next regeneration. The resolver lists the override file first when it exists.
- **(b) MDim step files** — edit the MDim `.config.yml` / `.py` → `STAGING=1 .venv/bin/etlr export://multidim/<ns>/<ver>/<name> --export --private`.
- **(c) Chart config on staging** — `scripts/update_chart_config.py` (guarded, staging-only; see below). Reaches production only via chart-diff approval + chart-sync after merge.

**Target = chart, field F:**

1. F is indicator-only (description_short/key, unit/short_unit, title_public, attribution_short, indicator-level display.name) → **route (a)**. Blast radius is mandatory first. Exception: the user wants a legend/series name changed on *this chart only* → `dimensions[i].display.name` via **route (c)** — offer both, default to fixing the source.
2. F ∈ {title, subtitle, note}:
   - Key present in the chart's `patch` → **route (c)** (the patch wins regardless of inheritance).
   - Key absent + inheritance enabled + single y indicator + inheritable → the rendered text IS the indicator's → **route (a)** if the fix should apply everywhere; **route (c)** (set an explicit patch value) if scoped to this chart. Run the blast radius and let its counts frame the question.
   - Key absent + inheritance disabled, or multi-y-indicator chart (inheritance baseline ambiguous — same conservatism as `indicator_update.py`), or no ETL grapher config → **route (c)**.
3. Entity selection / colors / axis / map settings → chart-config-only → **route (c)**. For selection edits, check the entities actually have data in the indicator (see the `check-empty-entities` skill's availability lookup).

**Target = MDim view, field F:**

1. Overridden at view level (`config.*` for chart fields, `metadata.*` for indicator fields in the yaml, or programmatic writes in the `.py` — grep for `view.metadata[...]`, `_assert_and_replace`, `_replace_*`) → **route (b)**. Mind mirror constants: MDim `.py` files hard-copy garden bullet texts under `OLD_*`/`NEW_*` assertions — every garden text edit needs the matching constant edit; grep the repo for fragments of any text you change.
2. Not overridden → inherited from the view's primary y indicator → **route (a)** (the grapher upsert refreshes the view; nothing extra needed on the MDim), or scope down to a new view-level override (**route b**) if the blast radius shows the indicator is shared.
3. Never write `multi_dim_x_chart_configs` or PUT MDim configs directly — they're rebuilt from the step files on every export.

**Target = MDim (whole collection):** top-level `title`, `default_selection`, `common_view_config`, config-level `definitions` → **route (b)**.

**Target = indicator:** → **route (a)**; blast radius on its variable ids.

**Narrative charts** (rare): their config is a patch over the parent chart. Edit via `AdminAPI(OWIDEnv.from_staging(branch)).get_narrative_chart(id)` / `update_narrative_chart(id, cfg)` — and audit the **merged** config (`configFull` from the API), since the stored `full` can be stale.

## Blast radius — notify and ask first

Before applying an edit, report every other surface it would change:

```
.venv/bin/python .claude/skills/edit-faust-metadata/scripts/blast_radius.py --branch <b> \
    (--variable-id N ... | --catalog-path 'grapher/...#col' ... | --anchor NAME --meta-file PATH | --chart-id N) \
    [--field subtitle] [--json]
```

Run it whenever:

- the route is **(a)** — always (indicator fields feed every surface using the variable);
- the route is **(b)** and the edit touches a shared block (`common_view_config`, config-level `definitions`, a garden definition consumed by several views);
- the route is **(c)** and the chart has narrative-chart children or gdoc embeds (the reporter checks).

It sweeps: **charts** (with `--field`, charts shielded by their own patch override of that field are listed separately — they will NOT change; for the chart-text fields title/subtitle/note, charts with no inheritance path — variable not a y series, several y series, or inheritance disabled — are also listed separately and excluded from the beyond-target count, since grapher only inherits chart config from a single-y, inheritance-enabled parent), **MDim views**, **explorer views** (legacy CSV explorers are invisible to these tables — caveat is printed), **narrative charts**, and **article references** (informational: embeds don't break, but the displayed text changes).

Decision rule: if surfaces **beyond the one the user pointed at** are affected (count > 0), STOP and ask the user before applying:

1. **Proceed broadly** — the text is wrong everywhere; fix at the source.
2. **Scope down** — name the concrete alternative: a view-level override in the MDim (route b) or an explicit chart-level value (route c), leaving other surfaces untouched.
3. **Abort.**

If the beyond-target count is zero, skip the ask and proceed.

## Workflow (edit mode)

**The single checkpoint rule: nothing is committed or pushed before the user's explicit go-ahead.** Everything up to the checkpoint happens on the branch + staging server only.

1. Parse the request; run `resolve_target.py --no-db` for instant identification feedback to the user.
2. Create the branch + draft PR: `.venv/bin/etl pr "<title, no emoji>" data` (never manual branching). This spins up the staging server the whole workflow depends on.
3. Wait for staging readiness: retry `OWIDEnv.from_staging(branch).read_sql("SELECT 1")` (builds take a few minutes).
4. Run `resolve_target.py` with the DB; pick the route via the decision tree.
5. Run `blast_radius.py` per the rules above; ask the user if other surfaces are affected.
6. Apply the edit:
   - route (a): edit the garden `.meta.yml`, following the style rules below;
   - route (b): edit the MDim yaml/py (mind mirror constants);
   - route (c): `update_chart_config.py --branch <b> --chart-id <id> --set ... [--dry-run first]`.
7. Reflect on staging **without committing**:
   - route (a): `.venv/bin/etlr garden/<ns>/<ver>/<ds> grapher/<ns>/<ver>/<ds> --private` then `STAGING=1 .venv/bin/etlr grapher://grapher/<ns>/<ver>/<ds> --grapher` (the MySQL upsert takes ~50 s+/dataset — warn the user; the automatic rebuild after the eventual push re-does it harmlessly). Needed because the staging auto-rebuild only sees *pushed* code.
   - route (b): `STAGING=1 .venv/bin/etlr export://multidim/<ns>/<ver>/<name> --export --private`.
   - route (c): already live on staging.
8. Run the metadata quality checks scoped to the edit (next section); fix findings and re-run the affected steps.
9. Verify on staging (section after); show the user the preview links.
10. **CHECKPOINT** — show: the `git diff` (routes a/b) and/or the chart-patch JSON diff (route c), staging preview links (strip the `.tail6e23.ts.net` suffix from admin links), the blast-radius summary, and any unresolved check findings. **Wait for the user's explicit go-ahead.**
11. After the go-ahead, hands-off:
    - `make check`;
    - commit `🔨🤖 <description>` with `Co-Authored-By: Claude <model name> <noreply@anthropic.com>`;
    - first push needs the upstream: `git push -u origin <branch>`, then verify `gh pr view --json files` is non-empty;
    - PR description via `gh pr edit` — first line is the attribution blockquote (`> _Written by Claude <model name> — @<handle> at the wheel._`), then: what changed and why (public facts only), the blast-radius summary, any route-(c) DB-only edits (they have **no file diff** — describe them explicitly and note they ride to production via chart-diff approval), and any `#dod:` follow-ups ("create in admin");
    - if the PR has committed files: post a bare `@codex review` comment, record its exact timestamp, and spawn the `pr-babysitter` skill's background agent to watch CI, judge/fix findings, reply + resolve threads;
    - if the PR is DB-only (zero committed files): skip Codex entirely and tell the user the path to production is chart-diff approval in the Wizard + merge.

## Metadata quality checks (before the checkpoint)

**Style rules for writing text** live in `.claude/skills/owid-metadata-generation/SKILL.md` — follow its field-by-field guidelines whenever composing new text (description_short must not repeat the title; plain language, expand acronyms; description_key ordered data-specific → methodology → caveats; curly apostrophes; American English; per-field guidance in `schemas/definitions.json`).

**The check suite** is also defined there (see "Metadata quality checks" in that SKILL — the canonical list, mirroring `/update-dataset` §6b/§6c): typos (`/check-metadata-typos`), Jinja spacing (`/check-metadata-spacing`), style guide (`/check-metadata-style`), the manual clarity checklist, link + `#dod:` verification, and adversarial claims verification (`/adversarial-data-review`).

Scoping rules specific to this skill:

- **Adversarial claims verification is MANDATORY here, but only on the metadata being added or edited — never on the data.** Run `/adversarial-data-review` scoped to the new/changed text: treat every added or edited sentence as a claim and verify it against the producer's documentation (fetch what's behind the links in the edited text and the dataset's snapshot `.dvc` — the link check only proves URLs resolve; this reads what they say). Skip the skill's data-value cross-checks, anomaly scans, and indicator prioritization entirely — no data changed. Unedited metadata is out of scope too. This keeps the pass cheap (a handful of web calls) while catching the failure mode nothing else covers: text that is well-formed, well-styled, and factually wrong (stale methodology attributions, scope overclaims, misread units in prose).

- For a one-field conversational edit, run the checks against **the edited text/step only** — don't re-audit the whole dataset. For target-report or mass edits, run the full skills as `/update-dataset` does.
- Route (c) chart-config text has no `.meta.yml` — apply the style guide, the clarity checklist, and a typo pass directly to the new text.
- If a check rewrites a `.meta.yml`, re-run the affected step (grapher steps with `--grapher`) and re-run the check to confirm zero remaining violations.
- New `[term](#dod:term)` links: check the `dods` table via public Datasette (`SELECT name FROM dods WHERE name LIKE ...`) before shipping; if missing, keep the link and list it in the PR body as a "create in admin" follow-up.

## Verifying on staging

- **Chart text without a browser**: `curl -s http://staging-site-<branch>/grapher/<slug>.svg | grep -o '<new text fragment>'` — the server-side render carries title/subtitle/note.
- **Indicator fields**: `https://api-staging.owid.io/staging-site-<branch>/v1/indicators/<id>.metadata.json` (path prefix is the full container name, not the bare branch — a wrong prefix silently serves another environment).
- **MDim views**: the resolver's per-view collection-preview URL (`/admin/grapher/<urlquoted catalogPath>?dim=choice...`).
- **Visual QA**: hand off to the `check-chart-preview` skill for a screenshot.
- **Big text changes**: re-run the report scripts in indicator-list mode and diff against the previous output (see dump mode below).
- **Jinja-templated definitions**: after editing shared `definitions`, rebuild garden AND grapher before reading anything — the report scripts and ad-hoc reads use the grapher channel, and a stale channel shows pre-edit metadata. Spot-check several rendered variants; dimension comparisons are type-sensitive (`decile == 5` vs `decile == "5"` — copy the comparison form from a working definition in the same file).

## Path to production

- Route (a)/(b) file edits deploy when the PR merges (normal ETL deploy).
- Route (c) staging chart edits appear in **chart-diff**; they are synced to production by chart-sync only after approval in the Wizard + merge. Remind the user of the pending approval.
- Never point a write at `admin.owid.io` or the production DB. The guard in `update_chart_config.py` enforces this; don't work around it.

## The guarded chart editor (route c)

```
.venv/bin/python .claude/skills/edit-faust-metadata/scripts/update_chart_config.py \
    --branch <b> --chart-id <id> \
    [--set subtitle='New subtitle'] [--unset note] \
    [--set-json selectedEntityNames='["France","Japan"]'] \
    [--dry-run]
```

- Hard-coded staging guard: refuses master/main, asserts the resolved env is staging, prints the target host before writing. There is no production escape hatch — by design.
- It GETs the chart's **patch** config, applies `--set` (dot-paths, string values), `--set-json` (typed values/arrays), `--unset` (deletes the key — the server re-derives inheritance on PUT, so unsetting restores the inherited value), prints the JSON diff, and PUTs back. `--dry-run` stops after the diff.
- Run `--dry-run` first, show the user the diff, then apply.

## Dump + compare mode (explicit request only)

Produce a Markdown audit of the user-facing chart text for a set of indicators or MDim views, for editorial review or as the target file of a mass edit. **Never suggest this mode proactively** — enter it only when the user asks for a dump/report/audit.

Scripts (shared helpers in `scripts/_common.py`: grapher-channel metadata loader, inheritance resolvers, `BulletLibrary`, auto-slugs, preview URL):

- `scripts/generate_mdim_text_report.py` — MDim view mode (supports `collapse_dims` and placeholder parametrization).
- `scripts/grapher_dataset_mode.py` — grapher-dataset mode (iterates every indicator column) and indicator-list mode (`--indicators <cp> <cp> ...` or `--indicators-file <path>`).

Rebuilding the MDim `.config.json` is done via `etlr <mdim> --export --private` — there is no DB-bypass helper. Change detection handles the common case: nothing changed → ~2 s; garden `.meta.yml`, garden data, or MDim yaml/py changed → etlr rebuilds only the affected steps.

Do **not** add `--grapher` unless you specifically need to re-upload indicator data/metadata to MySQL — it triggers a `grapher://grapher/<dataset>` upload step that can take ~50 s per dataset and isn't needed for the report (the script reads metadata directly from the local grapher-channel feather files). Do **not** add `--only` when you want garden/MDim edits to take effect — it skips upstream rebuilds by design; use `--only --force` only to re-run just the MDim step without touching anything upstream.

### Fields reported

Only user-facing text. Six fields, two groups:

| Group | Fields | Where they come from |
|---|---|---|
| Chart-level FAUST | `Title`, `Subtitle`, `Footnote` | `presentation.grapher_config.{title, subtitle, note}` |
| Indicator-level metadata | `description_short`, `description_key` | top-level `VariableMeta` fields |

Never report Axis titles or Units in the default output (keep the report skimmable). Never include `description_processing`.

### Inputs supported

| Input kind | Example | Source of per-entity text |
|---|---|---|
| MDim export | `wb/latest/incomes_pip#incomes_pip` | `export/multidim/<ns>/<ver>/<name>/<name>.config.json`, plus grapher-channel inheritance for each view's primary `y` indicator |
| Grapher/garden dataset | `data/grapher/wb/2026-03-24/world_bank_pip` | iterate columns across all tables; all text is `[inherited]` |
| Hand-picked indicators | `grapher/wb/2026-03-24/world_bank_pip/incomes#share__...` | same, filtered to the listed columns |

**Always load indicator metadata from the GRAPHER channel**, not garden. The grapher channel flattens dimensional indicators into one column per combination and renders the Jinja metadata templates with those specific dimension values — that's what Grapher actually shows.

### Required output format

```
# <mdim_name or dataset_name> — <top title>

**Preview:** [<catalog_path>](<admin_url>)

Total views: **N**   (for MDims)

## How to read this file
- [override], [inherited], [missing] explanation

## Description-key bullet legend
- **<slug>** — <full bullet text>   (one row per unique bullet)

## <view or indicator heading — uses chart Title when resolvable>

**<Dim name>:** <Choice name> · **<Dim name>:** ...   (human-readable dims)

**Preview:** [...](...)                                (view-level link)

- **Title** [source] ...
- **Subtitle** [source] ...
- **Footnote** [source] ...
- **description_short** [source] ...
- **description_key** [source]
  - slug-1
  - slug-2
```

### Key implementation features (all required)

1. **Grapher-channel metadata loading**: `Dataset(data/grapher/<ns>/<ver>/<ds>).read(<table>, safe_types=False)[<col>].metadata`.

1a. **`description_key` arrives as a markdown STRING, not a list**: the grapher channel serializes it via `owid.catalog.core.meta.description_key_to_string` — multiple bullets become one string joined as `"- b1\n- b2\n…"`, a single bullet becomes plain prose (datasets built before the change still carry lists). `scripts/_common.py:description_key_as_list()` normalizes both forms back into a bullet list; both report modes route through it. The same trap hits **MDim step code** that asserts/replaces bullets from `tb[col].metadata.description_key`: `OLD_TEXT in list(dk)` silently iterates characters on the string form and the assertion fails (or, worse, a `for b in dk` loop explodes bullets into characters). Normalize first (see `_description_key_bullets` in `incomes_pip.py` / `gini_lis.py` / `gini_wid.py`), then do list-membership asserts and per-bullet swaps; setting either a list or a markdown string back on `view.metadata["description_key"]` is accepted (`Collection` converts lists via `_convert_description_key_lists`).

2. **Rebuilding the MDim `.config.json`**: use `etlr export://multidim/<ns>/<ver>/<name> --export --private`. This runs `Collection.save()` (`validate_indicators_in_db` + `save_config_local` + `upsert_to_db` — admin-API upsert, not a big data push). If the command errors with a MySQL connection-refused trace, surface that to the user and stop — don't monkey-patch around it.

3. **Description-key dedup with auto slugs**: collect unique bullets into a per-file legend, auto-generate a short slug from the first ~3 non-stopword content words of each bullet (kebab-case), disambiguate collisions with `-2`/`-3` suffixes. Each view references bullets by their slugs, rendered as sub-bullets.

4. **Dimension collapse (MDim only)**: accept a `collapse_dims: list[str]` per MDim. Group views whose non-collapsed dims match, render one section per group, show variant previews on separate links labelled by the collapsed dim's value.

5. **Placeholder parametrization**: when the Title / Subtitle / description_short / description_key vary across collapsed variants only by a simple substitution, collapse the text to a single `{dim}` placeholder. Try the raw value first (`day` in `per day`), then snake → space (`before_tax` → `before tax`), then snake → hyphen (`before-tax`); case-insensitive regex. If all variants collapse to the same placeholder-bearing string, use it; else fall back to sub-bullets.

6. **Global placeholder legend**: when one or more dims are parametrized, include a header line listing `` `{dim}` ∈ {val1, val2, ...} `` once at the top of the file.

7. **Human-readable dim selections subheader**: directly under each view heading, render the dim selections using the dimension `name` and choice `name` from the MDim config. Filter out `nan` sentinel values.

7a. **Heading disambiguation when views share a title**: when two or more groups collapse to the same `## <Title>` heading, append `(Dim name: Choice name)` built from the non-collapsed dim(s) whose values differ across the colliding groups. Dim order follows the MDim config; only the differentiating dim(s) are appended.

8. **Preview URLs**: main MDim URL is `https://admin.owid.io/admin/grapher/<urlquote(catalog_path)>`. Per-view URL appends `?dim1=slug1&dim2=slug2` from the view's `dimensions` dict.

9. **Override / inherited / missing tagging**: `[override]` = text explicitly set on the view (MDim `config.*` or `metadata.*`); `[inherited]` = resolved from the primary y-indicator's ETL metadata; `[missing]` = absent in both. For grapher-dataset and indicator-list inputs, every tag is `[inherited]` or `[missing]`.

10. **`ai/` directory output** (per project convention). One Markdown file per entity the user asked about.

### Dump-mode workflow

1. Confirm the input kind: one MDim, several MDims, a dataset's indicators, or a hand-picked list.
2. For MDim input, confirm which dimensions (if any) to collapse — `period` is a classic candidate.
3. For MDims, rebuild the `.config.json` exports using `etlr` (full ETL path). For grapher/garden input, rely on the already-built dataset folder.
4. Run the appropriate script:
   - **MDim config rebuild**:
     ```
     .venv/bin/etlr export://multidim/wb/latest/incomes_pip --export --private
     ```
   - **MDim mode** — edit the `MDIMS` list at the top of `scripts/generate_mdim_text_report.py` or pass `--config <json>`:
     ```
     .venv/bin/python .claude/skills/edit-faust-metadata/scripts/generate_mdim_text_report.py
     ```
   - **Dataset mode**:
     ```
     .venv/bin/python .claude/skills/edit-faust-metadata/scripts/grapher_dataset_mode.py \
         --dataset data/grapher/wb/2026-03-24/world_bank_pip
     ```
   - **Indicator-list mode**:
     ```
     .venv/bin/python .claude/skills/edit-faust-metadata/scripts/grapher_dataset_mode.py \
         --indicators 'grapher/wb/2026-03-24/world_bank_pip/incomes#thr__...' \
                      'grapher/wb/2026-03-24/world_bank_pip/incomes#share__...'
     ```
5. Show the user the output file paths and wait for feedback — the user almost always wants iterative tweaks to format. Dataset mode has no collapse/parametrization; if the user wants dataset views grouped by a shared dim, fall back to the MDim-style code path.

### Comparing the live config to a target FAUST report

A common workflow: the user shares a FAUST report that represents the **desired** end state (their edited copy of an earlier auto-generated report) and asks "does the live MDim match this?". **Treat the report as the source of truth by default** — when the live config differs, the fix lands in the metadata to make the live match the report.

Two cases warrant a confirmation before silently editing the metadata to match:

- **Text-content drift in inherited bullets.** If the report shows older / shorter wording while the live config has newer longer wording, surface the diff side-by-side and confirm before reverting — sometimes the user rewrote the definition *after* generating the report and the live config is the up-to-date target.
- **View-count mismatch.** If the report has more or fewer sections than the live config, list the missing/extra sections explicitly and confirm before adding/removing views.

Before doing the field-by-field comparison, refresh everything the live config depends on. Skipping a step leaves a stale catalog, which produces phantom drift that isn't real:

```
.venv/bin/etlr garden/<ns>/<ver>/<ds> grapher/<ns>/<ver>/<ds> --private --force --only
.venv/bin/etlr multidim/<ns>/<ver>/<mdim> --export --only --private --force
```

Run both upstream steps — `garden --only` alone does NOT refresh the grapher channel, and the FAUST scripts read from grapher, not garden.

Then audit:

1. **Spot-check several view types**, not just one — overrides, `before_vs_after`, single-decile, all-decile (multi-indicator), share-vs-non-share. Different code paths populate different fields.
2. **Override fields live on the view; inherited fields don't.** A view's `metadata.description_key` in the `.config.json` only contains bullets the MDim explicitly set. Empty array / missing key means the bullets come from the underlying y-indicator — read those via `Dataset(<grapher_path>).read(<table>, load_data=False)[<col>].metadata.description_key`.
3. **Programmatic display.name overrides on indicators within multi-indicator views** live on `view['indicators']['y'][i]['display']['name']`, not on the view's text fields. Inspect them per-indicator.
4. **Slug collisions in the report (`Income-share-decile` vs `income-share-decile`) are tooling artefacts** — ignore capital/lowercase slug differences during audits.
5. **Check punctuation around markdown links specifically.** `[Economic Inequality.](url)` (period inside) vs `[Economic Inequality](url).` (period outside) is a common copy-edit issue and easy to miss.
6. **Common drift you'll see:** hyphenation removed from welfare_type bullets; qualifiers removed from subtitle / description_short overrides; `description_key[1:]` drops removed; new indicator-specific bullets added.
7. **If the live and target diverge, the fix usually lands in one of three places:** the garden meta.yml `definitions.description_key_*` blocks (text content); the MDim `.py` (override via `_assert_and_replace`, `_replace_welfare_type_bullet`, or `view.metadata[...] = ...`); rarely, the indicator's `presentation.grapher_config` block.
8. **After every fix push**, re-run garden + grapher + MDim export and re-verify against the report.

### Target-driven description_key restructuring across sibling MDims

A recurring large-scale workflow: the user pastes an edited FAUST report as the *desired* state for one dataset's MDims, then repeats it for sibling datasets ("now do the same for LIS / WID"). Lessons that generalize:

1. **The delta concentrates in `description_key`.** Chart-level FAUST and description_short almost always already match the target — verify that first and scope the work to bullet texts, per-variable list ordering, and the MDim mirror constants. Apply the new bullet ordering to the *whole dataset*, not just the MDim's indicators, unless told otherwise.
2. **Audit the target's legend↔views cross-references before editing.** Slugs referenced by views but missing from the legend usually map to an existing garden bullet — keep it unchanged. Legend bullets referenced by no view get skipped (confirm once with the user).
3. **Fact-check target texts against each dataset's actual data** — hand-edited targets propagate copy-paste from the first dataset: "income or consumption" onto income-only datasets, "country or region" where no regional aggregates exist. Verify empirically (count non-null values for region entities per indicator family) rather than trusting either the target or the old metadata.
4. **Bullets describing UI affordances must match the view's actual UI.** A bullet like "this chart gives the option to show breaks" is wrong on grouped views that exist only for one choice of that dimension — drop the bullet or strip the affordance sentence via a view-level override. Beware MDims that keep their own config-level `definitions.description_key_*` overrides — garden edits don't reach those views; align the config-local copies separately.
5. **New `#dod:…` links in a target may not exist.** Check the `dods` table via public Datasette before shipping; if missing, keep the link + list it in the PR body as a "create in admin" follow-up.
6. **Shared definitions serve more variants than the target shows.** Add Jinja branches so the target's wording doesn't leak onto other variants (poverty vs inequality, wealth vs income), and check the untouched-variant MDims in the regenerated reports.
7. **Jinja dimension comparisons: match the value type used elsewhere in the same file.** Dimension values can be int in one dataset and str in another — a wrong-type comparison renders the else-branch silently; copy the comparison form from a working definition and spot-check the affected view.
8. **Bulk list edits with Edit/replace_all: order by containment.** Reorder the anchored/longer per-variable blocks first and the bare short-tail patterns last; then verify all lists at once with a small parser script over the meta.yml.
9. **Mirror constants change in lockstep.** Every garden text edit needs the matching `OLD_*`/`NEW_*` constant edit in the MDim `.py`; the rebuild's assertion pass is the drift check.
10. **Tag placement is cosmetic in target comparisons**: a view the target marks `[inherited]` may only be implementable as `[override]` (and vice versa) — identical bullet content is what matters.

### Regression diff: prove a refactor didn't change user-facing text

When you change an MDim `.py` (reorder indicators, flip a choice order, change which y-indicator is primary) and need to prove the rendered FAUST is **unchanged except for the intended diff**, diff two auto-generated reports instead of eyeballing one. This is the right check whenever a change shifts the **primary y-indicator** (`y[0]`), because that's what drives inheritance.

`config_path` accepts **any** JSON path, not just the live `export/multidim/.../<name>.config.json` — so point two runs at two config snapshots:

1. Build the baseline config (e.g. `git checkout origin/master -- <step>.py && etlr <mdim> --export --grapher`) and copy its `<name>.config.json` to `/tmp/cfg_before/`. Restore your branch, rebuild, copy to `/tmp/cfg_after/`. (Note: `git checkout … -- a.py b.py` won't word-split an unquoted `$files` var in zsh — pass the paths literally or use an array.)
2. Run the report against each snapshot:
   ```
   echo '[{"name":"gini_lis_BEFORE","config_path":"/tmp/cfg_before/gini_lis.json","collapse_dims":[]}]' > /tmp/fb.json
   echo '[{"name":"gini_lis_AFTER","config_path":"/tmp/cfg_after/gini_lis.json","collapse_dims":[]}]'  > /tmp/fa.json
   .venv/bin/python .claude/skills/edit-faust-metadata/scripts/generate_mdim_text_report.py --config /tmp/fb.json
   .venv/bin/python .claude/skills/edit-faust-metadata/scripts/generate_mdim_text_report.py --config /tmp/fa.json
   ```
3. Diff, stripping the BEFORE/AFTER name token: `diff <(sed 's/BEFORE//g' ai/gini_lis_BEFORE.md) <(sed 's/AFTER//g' ai/gini_lis_AFTER.md)`. Byte-identical = all six fields render the same.

The FAUST diff only covers user-facing **text**. It will NOT catch indicator-order-only changes (e.g. a Dumbbell arrow direction or a series-color swap that follows column order) — pair it with a structural diff of the two `.config.json` files when order matters.

## Things to avoid

- Do NOT fall back to `title` / `title_public` / `display.name` / `description_short` when resolving chart Title / Subtitle / Footnote. Use `grapher_config` only.
- Do NOT report `description_processing` in dump mode; the user explicitly doesn't care about it for FAUST review.
- Do NOT load metadata from the garden channel; it exposes pre-template Jinja text and unflattened dimensions. Always use the grapher channel.
- Do NOT judge "explicitly set at chart level" from `chart_configs.full` — only `patch` distinguishes overrides from inherited values.
- Do NOT write to production — no `admin.owid.io` writes, no prod DB writes, ever. All chart edits go to the branch's staging server and ride chart-diff to production.
- Do NOT call `AdminAPI.put_grapher_config` or `put_mdim_config` by hand — the ETL files are the source of truth and the next rebuild overwrites DB-side edits.
- Do NOT hand-build `staging-site-<branch>` hostnames — use `get_container_name` / `OWIDEnv.from_staging`.
- Do NOT monkey-patch around a MySQL outage by calling `Collection.save_config_local()` directly or stubbing out `validate_indicators_in_db` / `upsert_to_db`. If MySQL is down, stop and tell the user.
- Do NOT produce HTML `<details>` blocks or tables in dump-mode reports — the preferred format is a flat Markdown outline with bullet fields.
- Do NOT suggest dump + compare mode — only enter it on explicit request.

## Related memories, skills, and references

- `.claude/projects/-Users-parriagadap-etl/memory/faust_definition.md` — FAUST = Footnote, Axis titles, Units, Subtitle, Title.
- `.claude/projects/-Users-parriagadap-etl/memory/feedback_chart_faust_inheritance.md` — the inheritance rule, with the caveat about `grapher_config` not being universally populated.
- `.claude/skills/owid-metadata-generation/SKILL.md` — writing style rules + the canonical metadata-check suite.
- `.claude/skills/pr-babysitter/SKILL.md` — the Codex review→fix→resolve background loop (step 11).
- `.claude/skills/check-chart-preview/SKILL.md` — visual QA on staging.
- `.claude/skills/check-empty-entities/SKILL.md` — entity-availability lookups for selection edits.
- `apps/chart_sync/admin_api.py` — the AdminAPI client the scripts build on.
