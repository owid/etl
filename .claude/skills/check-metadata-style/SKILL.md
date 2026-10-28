---
name: check-metadata-style
description: >-
  Check user-facing indicator and chart metadata (titles, subtitles, descriptions,
  description_key/WYSK, display names) against OWID's Writing and Style Guide and the
  harmonized FAUST for common metrics (money in national currency or international-$, % of
  GDP, GDP, poverty, inequality, death rates and others), after a mechanical pass for
  whitespace artifacts left by Jinja templates (double spaces, stray newlines, leading or
  trailing whitespace). Use when the user mentions the style guide, writing guide, harmonized
  FAUST, chart copy quality, title/subtitle/WYSK review, units, inflation or living-cost
  wording, spacing, whitespace or Jinja rendering, or after editing any .meta.yml under
  etl/steps/data/, garden or grapher. Most
  user-facing text is authored in garden and inherited by grapher; this skill reads the
  resolved metadata off the built grapher dataset, so garden-authored text is in scope.
metadata:
  internal: true
  owner: paarriagadap
---

# Check Metadata Style

Audit a dataset's user-facing text in three passes: a mechanical one for whitespace artifacts left by Jinja templates, a rule-driven one against OWID's Writing and Style Guide, and one against the harmonized FAUST for metrics OWID uses many times over (money measures above all). Flags fields that break the rules and offers to rewrite them.

The text itself is usually authored in the **garden** `.meta.yml` (`description_key`/WYSK, `description_short`, `title`, `display.name`) and inherited by grapher; a smaller share is set or overridden in the grapher step. This skill reads the **resolved** metadata off the built grapher dataset, so it covers both — you point it at the grapher step, and any fix it proposes goes back to whichever layer authored the field.

Rules live in two files next to this one, each a committed snapshot that step 1 keeps fresh:

- [STYLE_GUIDE.md](STYLE_GUIDE.md): the [Writing and Style Guide](https://app.notion.com/p/owid/Writing-and-style-guide-d51a3739ff8542ca90297fa8de40437c) (Notion).
- [HARMONIZED_FAUST.md](HARMONIZED_FAUST.md): [Harmonizing FAUST across OWID](https://docs.google.com/document/d/1tcknTryJAxOR0yv8x38s3jkONq-TY3T4HNCEcZQ_c-s/edit?tab=t.0) (Google Doc), the title, subtitle, footnote and units to use for common metrics.

## When to use

- After editing a `.meta.yml` under `etl/steps/data/` — **garden or grapher**.
- After editing Jinja templates in a `.meta.yml` (`<%- if %>`, `<<variable>>`, `{definitions.xxx}`), or when the user asks to check for spacing or whitespace issues in metadata.
- When the user asks to check chart copy / titles / subtitles / descriptions / WYSK against the style guide.
- After writing metadata for an indicator measured in money, or one of the metrics in `HARMONIZED_FAUST.md`.
- As part of pre-PR QA for a dataset update.

## Scope

**Current dataset only.** Ask for the step path if it's not obvious from context. Do not walk all active steps — keep the skill focused on the one dataset the user is working on. If the user asks to audit the whole catalog, say the skill is scoped to one step and suggest running it per dataset.

Read the **grapher** step even when the edit was to a garden `.meta.yml` (e.g. you edited `etl/steps/data/garden/un/2026-04-08/child_labor_report.meta.yml` → read `etl/steps/data/grapher/un/2026-04-08/child_labor_report`). Only the grapher dataset carries the resolved, inherited text a reader actually sees. If the grapher build is stale relative to your garden edit, rebuild it first (step 3) or you will be auditing the old copy. Check rather than assume: `.venv/bin/etlr <grapher step> --dry-run` prints `All datasets up to date!` only when the build matches the current files, uncommitted edits included. For a few published indicators you haven't edited locally, in a dataset too large to rebuild cheaply, audit the published text from `https://api.ourworldindata.org/v1/indicators/<id>.metadata.json` instead, and say which copy you audited.

---

## Implementation

### 1. Check the rulebooks are fresh

Each rulebook carries its own sync date, so check [STYLE_GUIDE.md](STYLE_GUIDE.md) and [HARMONIZED_FAUST.md](HARMONIZED_FAUST.md) independently: a fresh one never excuses refreshing the other.

Read the `> Last synced from Notion: YYYY-MM-DD` line in the header of [STYLE_GUIDE.md](STYLE_GUIDE.md).

- If the date is **less than two weeks** before today, no fetch is needed: move on to `HARMONIZED_FAUST.md` below.
- If the line is missing or the date is **two weeks old or more**, refresh the snapshot, trying in order:

1. **Notion MCP.** Load the Notion tools with `ToolSearch` (query `+notion fetch`) and fetch the [Notion page](https://app.notion.com/p/owid/Writing-and-style-guide-d51a3739ff8542ca90297fa8de40437c). Use the result only if it looks complete: all sections present (Branding, Capitalization, Grammar and syntax, Short citations for charts, Writing in GDoc, Descriptions, Dates, Punctuation, Spelling) with the ❌/✅ examples intact. Note: authorizing the connector **mid-session doesn't help** — MCP tool lists are fixed at session start (subagents inherit the same registry, and resuming the session doesn't refresh it either), so a newly authorized connector is only reachable from a brand-new session.
2. **Manual export.** If the Notion MCP is unavailable or unauthenticated, or the fetched content is incomplete or mangled, ask the user to download a markdown export of the page (open the Notion link → `•••` menu → **Export** → format **Markdown & CSV**) and provide the file path.
3. **Skip.** If neither works, warn the user that the committed guide may be stale (state its `Last synced` date) and continue the audit with the committed copy. Never block the audit on the refresh.

On a successful fetch/export:

- Convert the content to the existing file's structure (same heading hierarchy, ❌/✅ example formatting) and keep the 3-line header block.
- **Preserve sections marked `NOTE: local addition`** — rules agreed in PRs that aren't on the Notion page yet. Their absence from the fetched content is not a removal; drop one only when Notion covers the same rule (then remove the note too).
- Diff against the committed copy and summarize any rule changes to the user in-conversation.
- Rewrite the body only if the content changed; stamp today's date on the `Last synced from Notion:` line either way.
- Remind the user that the modified `STYLE_GUIDE.md` should be committed via a PR. Do **not** commit or push as part of this skill.

> Plain `WebFetch`/`curl` does **not** work for the refresh: `app.notion.com` serves a JavaScript shell with no page content. Don't waste time trying it.

Then, whatever the date of `STYLE_GUIDE.md`, read the `> Last synced from Google Doc: YYYY-MM-DD` line in the header of [HARMONIZED_FAUST.md](HARMONIZED_FAUST.md) and apply the same two-week rule. When it is stale, refresh it with the Google Docs connector (`ToolSearch` query `select:mcp__claude_ai_Google_Docs__read_doc`, document id `1tcknTryJAxOR0yv8x38s3jkONq-TY3T4HNCEcZQ_c-s`); the JSON is too large to read inline, so extract the text from the saved result with a short script. Otherwise ask the user for a download (**File → Download → Markdown**), or skip with a warning. When converting:

- Keep the file's structure: one section per metric, with `Title`, `Subtitle`, `Footnote` and unit bullets, and the `## Measures in various currencies and prices` section first.
- Leave out the doc's bulk-editor chart links, its history of earlier documents, and the names of people. This repo is public.
- Preserve rules marked `NOTE: local addition`, as for `STYLE_GUIDE.md`.
- Diff, summarize and stamp the date as for `STYLE_GUIDE.md`.

### 2. Read the rules

Read [STYLE_GUIDE.md](STYLE_GUIDE.md) and [HARMONIZED_FAUST.md](HARMONIZED_FAUST.md) in this skill's folder. These files are the source of truth the skill evaluates against — do not fall back to memory.

If either file is missing, rebuild it from its source via the refresh flow in step 1 (treat it as stale).

### 3. Collect user-facing strings from the step

Prefer the **rendered** (post-Jinja) metadata from the built catalog, so template artifacts are included in the check. The same collected strings feed every pass (steps 4, 5 and 5b).

Accept the step path in any of these forms and normalize it before loading:

- `etl/steps/data/grapher/un/2026-04-08/child_labor_report` (matches the Scope example)
- `data://grapher/un/2026-04-08/child_labor_report`
- `grapher/un/2026-04-08/child_labor_report`

```bash
.venv/bin/python -c "
from etl.paths import DATA_DIR
from owid.catalog import Dataset

raw = '<step path as the user gave it>'
step_path = raw.removeprefix('data://').removeprefix('etl/steps/data/').strip('/')
# step_path is now '<channel>/<namespace>/<version>/<dataset>', e.g. grapher/un/2026-04-08/child_labor_report
ds = Dataset(DATA_DIR / step_path)

import json
rows = []
for table_name in ds.table_names:
    tb = ds[table_name]
    for col in tb.columns:
        m = tb[col].metadata
        entry = {'table': table_name, 'variable': col, 'fields': {}}

        def put(key, val):
            if val:
                entry['fields'][key] = val

        put('title', getattr(m, 'title', None))
        put('unit', getattr(m, 'unit', None))  # harmonized-FAUST pass only
        put('short_unit', getattr(m, 'short_unit', None))  # harmonized-FAUST pass only
        put('description_short', getattr(m, 'description_short', None))
        put('description_processing', getattr(m, 'description_processing', None))  # whitespace pass only

        dk = getattr(m, 'description_key', None) or []
        if isinstance(dk, str):  # modern format: one free-form markdown string
            dk = [dk]
        for i, v in enumerate(dk):
            put(f'description_key[{i}]', v)

        display = getattr(m, 'display', None) or {}
        put('display.name', display.get('name'))
        put('display.unit', display.get('unit'))  # harmonized-FAUST pass only; Grapher shows it over unit
        put('display.shortUnit', display.get('shortUnit'))  # harmonized-FAUST pass only; Grapher shows it over short_unit

        pres = getattr(m, 'presentation', None)
        if pres is not None:
            put('presentation.title_public', getattr(pres, 'title_public', None))
            put('presentation.title_variant', getattr(pres, 'title_variant', None))
            put('presentation.attribution', getattr(pres, 'attribution', None))
            put('presentation.attribution_short', getattr(pres, 'attribution_short', None))
            gc = getattr(pres, 'grapher_config', None) or {}
            put('presentation.grapher_config.title', gc.get('title'))
            put('presentation.grapher_config.subtitle', gc.get('subtitle'))
            put('presentation.grapher_config.note', gc.get('note'))

        # The chart footer resolves presentation.attribution > origin.attribution >
        # origin.producer (year), so the visible credit usually comes from the origin.
        for i, o in enumerate(getattr(m, 'origins', None) or []):
            put(f'origins[{i}].attribution', getattr(o, 'attribution', None))
            put(f'origins[{i}].producer', getattr(o, 'producer', None))
            put(f'origins[{i}].description', getattr(o, 'description', None))

        if entry['fields']:
            rows.append(entry)

# Step 4: whitespace artifacts in the rendered strings
# A source description and a markdown description_key are multi-paragraph by design, so newlines don't count as
# artifacts there.
multi_paragraph = lambda k: k.endswith('.description') or k.startswith('description_key')
ws = [(r['table'], r['variable'], k, v) for r in rows for k, v in r['fields'].items()
      if isinstance(v, str) and ('  ' in v or v != v.strip() or ('\n' in v and not multi_paragraph(k)))]
print(f'WHITESPACE ISSUES: {len(ws)}')
for t, var, k, v in ws:
    print(f'  {t}.{var}.{k}: {v[:150]!r}')
print('---')

# Step 5b: indicators measured in money, for the currency rules of HARMONIZED_FAUST.md.
import re
money = re.compile(r'\\\$|€|£|¥|currency|dollar|euro|pound|yen|yuan|rupee|peso', re.I)
try:
    from babel.numbers import list_currencies  # every ISO 4217 code; babel ships with the full dev venv
    codes = list_currencies() | {'LCU'}
except ImportError:
    print('WARNING: babel is not installed, so units written only as an ISO currency code (SEK, DKK, ...) are missed')
    codes = {'USD', 'EUR', 'GBP', 'JPY', 'CNY', 'INR', 'LCU'}
code = re.compile(r'\b(?:' + '|'.join(sorted(codes)) + r')\b')  # case-sensitive: codes are upper case
# Grapher shows display.unit and display.shortUnit over unit and short_unit, so scan what it shows.
shown = lambda f: (f.get('display.unit') or f.get('unit', ''), f.get('display.shortUnit') or f.get('short_unit', ''))
money_rows = [r for r in rows if any(money.search(u) or code.search(u) for u in shown(r['fields']))]
print(f'MONEY INDICATORS: {len(money_rows)}')
for r in money_rows:
    f = r['fields']
    u, s = shown(f)
    under = (f.get('unit', ''), f.get('short_unit', ''))
    note = '' if (u, s) == under else f'  (display overrides unit={under[0]!r} short_unit={under[1]!r})'
    print(f\"  {r['table']}.{r['variable']}: unit={u!r} short_unit={s!r}{note}\")
print('---')

# Step 5: strings for the style audit
print(json.dumps(rows, indent=2, ensure_ascii=False))
"
```

**Fields checked:**

| Field | Why it matters |
|---|---|
| `title` | Variable short title; shows in the catalog and some chart views |
| `unit`, `short_unit` | Axis and tooltip units; checked only in the harmonized-FAUST pass (e.g. `current US$`, `% of GDP`) |
| `display.unit`, `display.shortUnit` | Override `unit` and `short_unit` in Grapher when set, so the harmonized-FAUST pass checks these instead |
| `description_short` | One-liner rendered under chart titles |
| `description_processing` | Processing note on data pages; collected for the whitespace pass only, the style rules skip it |
| `description_key[i]` | Key information on chart data pages (markdown string, or legacy bullet list) |
| `display.name` | Series label in chart legends |
| `presentation.title_public` | Public-facing chart title |
| `presentation.title_variant` | Disambiguator ("Historical", "WHO estimate", …) |
| `presentation.attribution` | Full source credit under the chart (`producer – data product (year)`) |
| `origins[i].attribution` | Where that credit usually comes from — the footer falls back to it when `presentation.attribution` is unset |
| `origins[i].producer` | The last fallback: with no attribution set, the footer renders `producer (year)` |
| `origins[i].description` | Source description in the Sources tab. `etl update` copies it from the previous version unchanged, so nothing else rereads it. Audit it once per origin, not once per variable |
| `presentation.attribution_short` | Short source credit under the chart |
| `presentation.grapher_config.title` | Overrides chart title when set |
| `presentation.grapher_config.subtitle` | Chart subtitle |
| `presentation.grapher_config.note` | Chart footnote |

**Fields deliberately skipped:**

- `description_from_producer` — verbatim text from the source, not OWID copy.
- `citation_full` — follows the producer's requested citation, so its wording and capitalization are theirs, not ours to restyle. (Dash typography is the one exception: a spaced hyphen separating producer from data product is an en dash everywhere, per the style guide.)
- `processing_level`, internal names — not user-facing prose.
- `description_long` — technical, de-prioritized (re-enable later if needed).

**Fallback if the dataset isn't built:**

If `DATA_DIR / step_path` does not exist, parse the `.meta.yml` directly with `etl.files.ruamel_load` and pull the same field names from the `tables → <name> → variables → <var>` tree.

Origin fields (`origins[i].attribution`, `origins[i].producer`, `origins[i].description`) live in the snapshot `.dvc`, not the `.meta.yml`, so this fallback does not cover them — say so in the report.

**Parse the garden `.meta.yml`, not just the grapher one.** Most datasets author their user-facing text in garden and have a thin or absent grapher `.meta.yml`, so reading only the grapher layer here finds nothing and reports a clean bill of health on unaudited text. Read both (`etl/steps/data/garden/<ns>/<version>/<dataset>.meta.yml` and the grapher one if it exists) and note that grapher values override garden ones on the same field.

Warn the user that Jinja templates (`<<var>>`, `{definitions.xxx}`, `<%- ... -%>`) and garden→grapher inheritance are **not** resolved in this fallback path, so template-generated violations will be missed and the whitespace pass (step 4) has nothing to check. Suggest building the step first:

```bash
.venv/bin/etlr grapher/<namespace>/<version>/<dataset> --grapher
```

Drop `--only` here on purpose: when the catalog is missing, upstream meadow/garden outputs are usually missing too, and `--only` would skip them and fail on missing inputs.

### 4. Mechanical pass: whitespace artifacts

Read the `WHITESPACE ISSUES` block the step-3 script printed. Every hit is a template defect that only shows in the rendered text, never in the YAML:

| Pattern | Example | Why it's a problem |
|---------|---------|-------------------|
| Double space | `Share of  children` | Jinja `if/else` block left extra whitespace |
| Leading whitespace | ` Share of children` | Template newline rendered as leading space |
| Trailing whitespace | `Share of children ` | Template block left trailing space |
| Embedded newline | `Share of\nchildren` | Multi-line Jinja block not properly trimmed |

Report them grouped by table and field, showing the rendered value with `repr()` so the whitespace is visible. The fix is in the template, not in the rendered string:

- Use `<%-` and `-%>` trim markers instead of `<%` and `%>` to strip whitespace around control blocks.
- Use a `|-` YAML block scalar for multi-line definitions to control trailing newlines.
- Check `{definitions.xxx}` references: the definition itself may carry leading or trailing whitespace.

The template usually lives in the **garden** `.meta.yml` even when you read the grapher dataset; edit the layer that authored it. If the block is empty, say so and move on.

### 5. Evaluate each string against the guide

Claude reads `STYLE_GUIDE.md` and checks every collected string. Keep the evaluation **rule-driven**: cite the specific section/heading of the guide, not a generic "doesn't sound right".

Focus on rules the guide actually states (e.g. sentence case vs. title case, acronym expansion on first use, number/unit formatting, banned phrases, punctuation, tone). Do not invent rules the guide doesn't cover — if `STYLE_GUIDE.md` doesn't say something, don't flag it. Prefer false negatives over false positives.

Report format, one block per violation:

```
[<table>.<variable>.<field>] "<offending text>"
  Rule: <section name / short rule>
  Why:  <one line rationale>
  Fix:  "<suggested rewrite>"
```

Group the blocks by variable for readability. End with a summary count.

**Exception:** when the producer explicitly asked for a specific short citation, `schemas/definitions.json` says to follow it. Report a matching `origins[i].attribution` as producer-prescribed rather than as a violation, and don't offer a fix.

### 5b. Evaluate against the harmonized FAUST

Check each indicator that is one of the metrics in `HARMONIZED_FAUST.md` against that metric's section, field by field: title, subtitle (`grapher_config.subtitle`, else `description_short`), footnote (`grapher_config.note`), `unit` and `short_unit`. For the units, check what Grapher shows: `display.unit` and `display.shortUnit` when set, else `unit` and `short_unit`, and put any fix in the field that is shown. Match an indicator to a metric only when it clearly is that metric (a GDP series, an extreme poverty share, a death rate, a "% of GDP" spending share); when unsure, don't flag. Use the same report format as step 5, with `Rule:` naming the `HARMONIZED_FAUST.md` section.

**Every indicator in the `MONEY INDICATORS` block gets the currency rules**, whatever metric it is. Read its `description_key` and `description_processing`, and the producer's documentation where they don't settle it, to place it in one of the four cases (current or constant national currency, current or constant international-$). A unit converted to US dollars at market exchange rates is a national currency (US$), not international-$. Then check:

- The subtitle says whether the data is adjusted for inflation when it covers several years, and whether it's adjusted for differences in living costs between countries when it covers several countries. Check the years and countries in the built data, not in the text. A subtitle that mentions inflation for single-year data, or living costs for single-country data, is a finding too.
- The subtitle has no index year and no "constant", "current" or "international-$". Those go in the footnote.
- The footnote gives the technical unit (e.g. "constant 2019 US$", "[international-$](#dod:int_dollar_abbreviation) at 2021 prices") and, when OWID adjusted for inflation, the deflator.
- The title mentions none of it.
- `unit` and `short_unit` follow the case: `current US$` / `$`, `constant 2019 US$` / `$`, `international-$ in 2017 prices` / `$`, `current international-$` / `$`. These are the currency part only: keep any scale, denominator or per-capita qualifier the unit already carries (`million current US$`, `constant 2024 US$ per kilowatt-hour`, `current US$ per capita`), in `short_unit` too, and flag only the currency part.

Also flag any indicator whose unit is a share of GDP but is written as `%` alone: the long unit must be `% of GDP`.

If no violations are found, say so and list the fields that were inspected — the user should know what was checked, not just that nothing came up.

### 6. Offer to fix

Match the pattern in [check-metadata-typos](../check-metadata-typos/SKILL.md) §4–5:

- **Fix all** — apply every suggested rewrite.
- **Review each** — step through one at a time, user confirms/rejects/edits each.
- **Cancel** — exit without changes.

A fix that changes what the text claims, not just how it is worded, has to rest on the producer's documentation (`.claude/rules/producer-documentation.md`). Check it there before applying it, and ask the user when the documentation is missing or inaccessible.

Apply fixes to the `.meta.yml` file with `ruamel_load` / `ruamel_dump` so comments and key ordering are preserved (see `CLAUDE.md` → *YAML Editing*):

```python
from etl.files import ruamel_load, ruamel_dump
data = ruamel_load(meta_yml_path)
# ...edit the tables → variables → <var> → <field> tree...
with open(meta_yml_path, 'w') as f:
    f.write(ruamel_dump(data))
```

Origin fields are set in the snapshot `.dvc` (`meta.origin.*`) — or, occasionally, by the step's own code after it loads the snapshot. Check which before editing, and edit that one. Producer-prescribed attributions stay out of **Fix all**.

If a violation only shows up in the rendered output because of a Jinja definition (e.g. the issue is inside `{definitions.foo}`), flag it for manual fix — don't auto-rewrite the definition without asking. Whitespace artifacts from step 4 always go this route: propose the trim marker or block-scalar change in the template and let the user confirm.

### 7. Verify

After fixes:

1. Rebuild the step if metadata text was changed in a way that affects rendering:
   ```bash
   .venv/bin/etlr grapher/<namespace>/<version>/<dataset> --grapher --force --only
   ```
2. Re-run steps 3–5b of this skill on the same step. Expect zero whitespace issues and zero violations.
3. Run `make check` to confirm no lint/format regressions from the YAML edits.

---

## Notes

- **No persistent output.** Analysis results stay in-conversation: no report `.md`, no scripts under `scripts/`. The only persistent file tied to this skill is `STYLE_GUIDE.md` (the committed rulebook).
- **Between syncs (step 1), always audit against the committed `STYLE_GUIDE.md`**, so the skill stays deterministic and offline-capable.
- **Archive-aware.** If the provided step path is under `dag/archive/*.yml`, point that out and confirm the user still wants to check it.
