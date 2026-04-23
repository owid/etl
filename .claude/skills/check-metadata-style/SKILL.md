---
name: check-metadata-style
description: Check grapher chart metadata (titles, subtitles, descriptions, display names) against OWID's Writing and Style Guide. Use when the user mentions the style guide, writing guide, chart copy quality, title/subtitle review, or after editing .meta.yml files under etl/steps/data/grapher/.
metadata:
  internal: true
---

# Check Metadata Style

Audit the user-facing text in a grapher step's metadata against OWID's Writing and Style Guide. Flags fields that break the rules and offers to rewrite them.

Rules live in [STYLE_GUIDE.md](STYLE_GUIDE.md) next to this file — a committed snapshot of the [OWID Notion page](https://www.notion.so/owid/Writing-and-style-guide-d51a3739ff8542ca90297fa8de40437c). Keep `STYLE_GUIDE.md` in sync via a PR whenever the Notion page changes.

## When to use

- After editing a `.meta.yml` under `etl/steps/data/grapher/`.
- When the user asks to check chart copy / titles / subtitles / descriptions against the style guide.
- As part of pre-PR QA for a dataset update.

## Scope

**Current step only.** Ask for the step path if it's not obvious from context (e.g. `etl/steps/data/grapher/un/2026-04-08/child_labor_report`). Do not walk all active steps — keep the skill focused on the one dataset the user is working on.

---

## Implementation

### 1. Read the Writing and Style Guide

Read [STYLE_GUIDE.md](STYLE_GUIDE.md) in this skill's folder. That file is the source of truth the skill evaluates against — do not fall back to memory, and do not invent rules that aren't in the file.

If the file is missing, stop and tell the user to restore it from the [Notion page](https://www.notion.so/owid/Writing-and-style-guide-d51a3739ff8542ca90297fa8de40437c).

### 2. Collect user-facing strings from the step

Prefer the **rendered** (post-Jinja) metadata from the built catalog, so template artifacts are included in the check.

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
        put('description_short', getattr(m, 'description_short', None))

        dk = getattr(m, 'description_key', None) or []
        for i, v in enumerate(dk):
            put(f'description_key[{i}]', v)

        display = getattr(m, 'display', None) or {}
        put('display.name', display.get('name'))

        pres = getattr(m, 'presentation', None)
        if pres is not None:
            put('presentation.title_public', getattr(pres, 'title_public', None))
            put('presentation.title_variant', getattr(pres, 'title_variant', None))
            put('presentation.attribution_short', getattr(pres, 'attribution_short', None))
            gc = getattr(pres, 'grapher_config', None) or {}
            put('presentation.grapher_config.title', gc.get('title'))
            put('presentation.grapher_config.subtitle', gc.get('subtitle'))
            put('presentation.grapher_config.note', gc.get('note'))

        if entry['fields']:
            rows.append(entry)

print(json.dumps(rows, indent=2, ensure_ascii=False))
"
```

**Fields checked:**

| Field | Why it matters |
|---|---|
| `title` | Variable short title; shows in the catalog and some chart views |
| `description_short` | One-liner rendered under chart titles |
| `description_key[i]` | Bullet list on chart data pages |
| `display.name` | Series label in chart legends |
| `presentation.title_public` | Public-facing chart title |
| `presentation.title_variant` | Disambiguator ("Historical", "WHO estimate", …) |
| `presentation.attribution_short` | Short source credit under the chart |
| `presentation.grapher_config.title` | Overrides chart title when set |
| `presentation.grapher_config.subtitle` | Chart subtitle |
| `presentation.grapher_config.note` | Chart footnote |

**Fields deliberately skipped:**

- `description_from_producer` — verbatim text from the source, not OWID copy.
- `unit`, `short_unit`, `processing_level`, internal names — not user-facing prose.
- `description_long`, `description_processing` — technical, de-prioritized (re-enable later if needed).

**Fallback if the dataset isn't built:**

If `DATA_DIR / step_path` does not exist, parse the `.meta.yml` directly with `etl.files.ruamel_load` and pull the same field names from the `tables → <name> → variables → <var>` tree. Warn the user that Jinja templates (`<<var>>`, `{definitions.xxx}`, `<%- ... -%>`) are **not** resolved in this fallback path, so template-generated violations will be missed. Suggest building the step first:

```bash
.venv/bin/etlr grapher/<namespace>/<version>/<dataset> --grapher --private
```

Drop `--only` here on purpose: when the catalog is missing, upstream meadow/garden outputs are usually missing too, and `--only` would skip them and fail on missing inputs.

### 3. Evaluate each string against the guide

Claude reads `STYLE_GUIDE.md` and checks every collected string. Keep the evaluation **rule-driven**: cite the specific section/heading of the guide, not a generic "doesn't sound right".

Focus on rules the guide actually states (e.g. sentence case vs. title case, acronym expansion on first use, number/unit formatting, banned phrases, punctuation, tone). Do not invent rules the guide doesn't cover.

Report format, one block per violation:

```
[<table>.<variable>.<field>] "<offending text>"
  Rule: <section name / short rule>
  Why:  <one line rationale>
  Fix:  "<suggested rewrite>"
```

Group the blocks by variable for readability. End with a summary count.

If no violations are found, say so and list the fields that were inspected — the user should know what was checked, not just that nothing came up.

### 4. Offer to fix

Match the pattern in [check-metadata-typos](../check-metadata-typos/SKILL.md) §4–5:

- **Fix all** — apply every suggested rewrite.
- **Review each** — step through one at a time, user confirms/rejects/edits each.
- **Cancel** — exit without changes.

Apply fixes to the `.meta.yml` file with `ruamel_load` / `ruamel_dump` so comments and key ordering are preserved (see `CLAUDE.md` → *YAML Editing*):

```python
from etl.files import ruamel_load, ruamel_dump
data = ruamel_load(meta_yml_path)
# ...edit the tables → variables → <var> → <field> tree...
with open(meta_yml_path, 'w') as f:
    f.write(ruamel_dump(data))
```

If a violation only shows up in the rendered output because of a Jinja definition (e.g. the issue is inside `{definitions.foo}`), flag it for manual fix — don't auto-rewrite the definition without asking.

### 5. Verify

After fixes:

1. Rebuild the step if metadata text was changed in a way that affects rendering:
   ```bash
   .venv/bin/etlr grapher/<namespace>/<version>/<dataset> --grapher --private --force --only
   ```
2. Re-run steps 2–3 of this skill on the same step. Expect zero violations.
3. Run `make check` to confirm no lint/format regressions from the YAML edits.

---

## Notes

- **No persistent output.** Analysis results stay in-conversation: no report `.md`, no scripts under `scripts/`. The only persistent file tied to this skill is `STYLE_GUIDE.md` (the committed rulebook).
- **Current step only.** If the user asks to audit the whole catalog, say the skill is scoped to one step and suggest running it per dataset.
- **Keep `STYLE_GUIDE.md` in sync with Notion.** If the Notion page changes, update the committed file via a PR — that way guide changes are reviewed and the skill stays deterministic and offline-capable.
- **Archive-aware.** If the provided step path is under `dag/archive/*.yml`, point that out and confirm the user still wants to check it.
- **Don't hallucinate rules.** If `STYLE_GUIDE.md` doesn't say something, don't flag it. Prefer false negatives over false positives.
