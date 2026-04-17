---
name: check-metadata-style
description: Check grapher chart metadata (titles, subtitles, descriptions, display names) against OWID's Writing and Style Guide. Use when the user mentions the style guide, writing guide, chart copy quality, title/subtitle review, or after editing .meta.yml files under etl/steps/data/grapher/.
metadata:
  internal: true
---

# Check Metadata Style

Audit the user-facing text in a grapher step's metadata against OWID's Writing and Style Guide (Notion). Flags fields that break the rules and offers to rewrite them.

Style guide URL: `https://www.notion.so/owid/Writing-and-style-guide-d51a3739ff8542ca90297fa8de40437c`

## When to use

- After editing a `.meta.yml` under `etl/steps/data/grapher/`.
- When the user asks to check chart copy / titles / subtitles / descriptions against the style guide.
- As part of pre-PR QA for a dataset update.

## Scope

**Current step only.** Ask for the step path if it's not obvious from context (e.g. `etl/steps/data/grapher/un/2026-04-08/child_labor_report`). Do not walk all active steps — keep the skill focused on the one dataset the user is working on.

---

## Implementation

### 1. Fetch the Writing and Style Guide from Notion

The guide lives behind Notion auth, so use the Notion MCP tools.

1. Try fetching the page with the Notion MCP fetch/search tool first (names vary — look for `mcp__claude_ai_Notion__*` tools beyond `authenticate` once auth is complete).
2. If the fetch fails with an authentication error, call `mcp__claude_ai_Notion__authenticate`, tell the user to complete the OAuth flow, wait, then call `mcp__claude_ai_Notion__complete_authentication` and retry the fetch.
3. Keep the guide content in-conversation for the rest of the skill run — do **not** write it to disk.

If the Notion MCP is not available in this Claude Code session, stop and tell the user: the skill needs Notion access to read the rules. Do not guess at rules from memory.

### 2. Collect user-facing strings from the step

Prefer the **rendered** (post-Jinja) metadata from the built catalog, so template artifacts are included in the check.

```bash
.venv/bin/python -c "
from etl.paths import DATA_DIR
from owid.catalog import Dataset

step_path = '<channel>/<namespace>/<version>/<dataset>'  # e.g. grapher/un/2026-04-08/child_labor_report
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
.venv/bin/etlr grapher/<namespace>/<version>/<dataset> --grapher --private --force --only
```

### 3. Evaluate each string against the guide

Claude reads the fetched Notion content and checks every collected string. Keep the evaluation **rule-driven**: cite the specific section/heading of the guide, not a generic "doesn't sound right".

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

Match the pattern in [check-metadata-typos](../.claude/skills/check-metadata-typos/SKILL.md) §4–5:

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

- **No persistent files.** All analysis is in-conversation: no report `.md`, no scripts under `scripts/`, no cached copy of the guide on disk.
- **Current step only.** If the user asks to audit the whole catalog, say the skill is scoped to one step and suggest running it per dataset.
- **Fresh rules each run.** The guide can change in Notion; fetching it at invocation time is intentional.
- **Archive-aware.** If the provided step path is under `dag/archive/*.yml`, point that out and confirm the user still wants to check it.
- **Don't hallucinate rules.** If the guide doesn't say something, don't flag it. Prefer false negatives over false positives.
