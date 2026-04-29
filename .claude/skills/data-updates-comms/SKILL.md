---
name: data-updates-comms
description: Draft answers for OWID's data-updates-comms Slack template using snapshot DVC + garden metadata + staging DB queries. Use when the user wants to announce a dataset update, fill the "Message about new data update" form, or generate the FAQ-style Slack post after an ETL update. Mechanical fields (producer, dates, coverage, chart count, search URL) are filled directly; editorial fields (why it matters, caveats, what's interesting about this update) get prompts seeded with extracted context for the user to refine.
metadata:
  internal: true
---

# Data update communications draft

Generate a draft for the #data-updates-comms Slack form. The skill inspects the **current branch** of the ETL repo to fill the mechanical fields and seeds the editorial fields with context the user then rewrites in their own voice.

## Why this channel exists (read before drafting)

OWID's #data-updates-comms channel is **not** an internal "FYI I did X" log. It gives information to Charlie (OWID's Communications & Outreach Manager), who turns this input into public-facing posts on social media (Instagram, LinkedIn, X) and in newsletters.

So the form's editorial fields are written for Charlie *and indirectly for the general public* — not for the data team. A big mistake is writing them in an internal/engineer voice.

**The reframing that matters most:**

> Don't tell Charlie what you'd say to your colleagues ("I updated all the WDI charts").
> Tell him what you'd say to a friend who asks what you did this week ("I updated hundreds of our charts to the latest release of the World Bank's largest dataset, called the World Development Indicators. It's a core dataset with hundreds of indicators across global development. This update added new data up to 2025 for dozens of our most-viewed charts…").

What Charlie needs from each field is a **reader-centric view**: what work was done, what it changes or enables, what's interesting about the source, what it helps people understand about the world, and why anyone should care that it's been updated.

**This skill must surface this framing to the user** — both at the top of the output draft file and inside each editorial-field prompt block — so the user has it in front of them when they sit down to write. Do not delete or paraphrase the framing.

## When to use

- The user says "draft the data-update comms post for X", "fill out the Slack form for X", or similar.
- After an `update-dataset` run finishes (step 9 of that skill should delegate here rather than re-implement).
- After a manual update where the user wants help drafting the announcement.

## Inputs

- `<namespace>/<new_version>/<short_name>` — the updated dataset (garden path).
- Optional: `<old_version>` — for "what changed since last release".
- Optional: branch name — defaults to current branch (used for the staging-site hostname).

If the user only gives a branch or no input at all, infer the dataset(s) from `git diff master...HEAD --name-only` filtered to `etl/steps/data/grapher/**` or `snapshots/**`. If multiple new datasets are touched, ask the user which one(s) the announcement covers.

## What's mechanical vs. editorial

| Slack field | Source | Skill output |
|---|---|---|
| Dataset name | `meta.origin.title` + `meta.origin.producer` from snapshot DVC | filled |
| Release date | `meta.origin.date_published` | filled |
| Next release | producer's page (web fetch of `url_main`) — best effort | candidate + `[verify]` tag |
| Data source | `meta.origin.producer`, `attribution_short`, `citation_full` | filled |
| Coverage (years + countries) | garden table: `year.min()..year.max()`, distinct `country` count, presence of regions | filled |
| Charts affected | staging MySQL query on `chart_dimensions` joined to `variables.catalogPath` (filter `publishedAt IS NOT NULL`) | filled, with size qualifier (handful/moderate/large/massive) |
| Why this matters | seeded from `meta.origin.description` + dataset `description` + top indicator `description_short` | **prompt** with extracted snippets — user rewrites |
| Caveats | seeded from indicator `description_key` bullets, sanity-check workarounds (`notes_to_check.md` from update-dataset workbench), `meta.origin.description` paragraphs that mention "limitations" / "caution" | **prompt** with extracted snippets — user rewrites |
| Anything interesting | seeded from PR commit messages, `notes_to_check.md` resolutions, snapshot diff summary if available in `workbench/<short_name>/` | **prompt** — user rewrites |
| Chart views (1–3) | reuse update-dataset step 8 picker logic OR query staging directly using the criteria below | filled with rationale, user confirms |
| Search URL | `https://ourworldindata.org/search?datasetProducts=<urlquote(producer)>` | filled |

**The editorial fields are deliberately not auto-prosed.** Slack posts in the editorial voice ("Why we have this dataset on OWID") read flat when LLM-written; the value is in the human framing. The skill's job is to surface the relevant snippets so the user doesn't have to grep for them.

## Workflow

1. **Resolve the dataset.**
   - Parse `<namespace>/<new_version>/<short_name>` or infer from git diff.
   - Locate snapshot DVC: `snapshots/<namespace>/<new_version>/*.dvc`.
   - Locate garden step: `etl/steps/data/garden/<namespace>/<new_version>/<short_name>.py` and `.meta.yml`.
   - Locate built garden dataset: `data/garden/<namespace>/<new_version>/<short_name>` (must have been run; if missing, tell the user to run the garden step first).

2. **Extract mechanical fields.**
   - Read DVC origin block via `etl.files.ruamel_load` (preserves comments if we ever write back) or just `yaml.safe_load`.
   - Load the garden dataset to get coverage:
     ```python
     from owid.catalog import Dataset
     ds = Dataset("data/garden/<ns>/<ver>/<sn>")
     tb = ds.read("<table>", safe_types=False).reset_index()
     year_min, year_max = int(tb["year"].min()), int(tb["year"].max())
     n_countries = tb["country"].nunique()
     has_regions = tb["country"].isin(REGION_NAMES).any()  # use etl.helpers.regions
     ```
   - Year/country coverage: report the range, the distinct-country count, and whether OWID-defined regions are included. Flag if the most recent year has notably fewer countries than the overall median (sparse-recent caveat).

3. **Query staging for affected published charts.**
   ```bash
   make query SQL="
     SELECT COUNT(DISTINCT c.id) FROM charts c
     JOIN chart_configs cc ON cc.id = c.configId
     JOIN chart_dimensions cd ON cd.chartId = c.id
     JOIN variables v ON cd.variableId = v.id
     WHERE v.catalogPath LIKE '%<ns>/<ver>/<sn>%'
       AND c.publishedAt IS NOT NULL"
   ```
   - Map the count to a qualifier: 1–10 = "handful", 10–50 = "moderate", 50–200 = "large", 200+ = "massive".
   - **Only count published charts.** Drafts are excluded by design — the Slack audience cares about user-facing impact.

4. **Seed the editorial fields with context.**
   For each of "why it matters", "caveats", "anything interesting", do not write prose — write a short prompt block followed by extracted snippets. Example:
   ```markdown
   **What does this dataset help our users understand about the world?**
   _Rewrite in your own voice. Snippets to draw from:_
   - From `meta.origin.description` (BTI snapshot): "BTI puts development and transformation policies to the test… in-depth assessments of 137 countries…"
   - Garden dataset description: "…"
   - Top indicator `description_short`: "…"
   ```

5. **Pick chart views (reuse update-dataset step 8 logic).**
   - Query published charts on staging (same SQL as step 3 but selecting `c.id, cc.slug, cc.full->>'$.title', cc.full->>'$.type', cc.full->>'$.hasMapTab'`).
   - Rank by: `hasMapTab=true` > `type=StackedArea` global views > standalone-headline titles. Skip population-weighted variants and country-specific views.
   - Output 1–3 with slug + rationale.
   - Prefer the most-viewed / most-linked charts (e.g. the `analytics_pageviews` table on staging or the equivalent admin endpoint)

6. **Build the search URL.**
   - `producer` from snapshot origin → `urllib.parse.quote_plus(producer)` → `https://ourworldindata.org/search?datasetProducts=<encoded>`.
   - Always include this — even if it returns zero results today, it'll resolve once the new version is deployed.

7. **Best-effort next-release date.**
   - Fetch `url_main` (WebFetch) and extract any phrase like "next release", "annual update", "updated yearly" near the page header. Tag the answer `[verify]` so the user knows to confirm.
   - If nothing extractable, output `[unknown — please confirm with the producer's release schedule]`.

8. **Write the draft.**
   - Output path: `ai/data-update-comms.md` by default, or `workbench/<short_name>/slack-announcement.md` when invoked from `update-dataset` step 9.
   - Use the format in `.claude/skills/update-dataset/slack-announcement-template.md`.
   - Mark each field with one of: `[filled]`, `[prompt — user rewrites]`, `[verify]`, `[missing]`.

9. **Show the user the file path** and stop. Do **not** post to Slack — that's a human action. The user copy-pastes from the Markdown file into the Slack form.

## Output format

**The output file must follow the Slack form's wording verbatim.** Use the exact prompt strings and example strings shown in `slack-form-verbatim.md`. The user copy-pastes from this file directly into the Slack form, so each Slack field gets its own `## <verbatim prompt>` heading, the Slack-provided example sits unchanged underneath as a quoted line ("E.g.: …"), and our drafted answer goes below in a fenced ```text block``` (so the user can copy the answer cleanly without dragging the prompt or example with it).

The verbatim Slack prompts and examples (do **not** rephrase, abbreviate, or change punctuation):

| # | Prompt heading (verbatim) | Example line (verbatim, kept as-is) |
|---|---|---|
| 1 | `What dataset(s) did you update?` | `E.g.: UN World Population Prospects.` |
| 2 | `When was this data released? When is the next scheduled release / our plan for next update?` | `E.g.: released: Jul 2024; next: Jul 2027, and we'll update that month.` |
| 3 | `Who is the data source(s)? Is there anything our users should know about them?` | `E.g.: Maintained by researchers at the University of California and the Max Planck Institute for Demographic Research.` |
| 4 | `What's the coverage of the data in terms of years and countries/regions?` | `E.g.: Covers the years 1991–2024, global total only.` |
| 5 | `How many charts did this update affect?` | `Was this a small update affecting a handful of charts, or a massive one affecting tens or even hundreds?` |
| 6 | `What does this dataset help our users understand about the world, and why is it important they know that?` | `In other words: Why do we have this data on OWID at all? What is unique about this dataset compared to similar ones?` |
| 7 | `Any important caveats or pitfalls in interpretation that users should know about this data? (optional)` | `E.g.: note that saying "Country X is more productive than Country Y" is often taken as "people in Country X work harder".` |
| 8 | `Anything interesting to note about this update, including what you had to do? Anything else you'd like to add? (optional)` | `E.g.: you worked with the data provider to improve the data somehow.` |
| 9 | `Add 1–3 chart views we might use in the public announcement` | `Pick chart views that represent the whole dataset, rather than, e.g., something very specific about a single country.` |
| 10 | `Link to the updated charts as a search result (not a chart collection anymore). Ask Charlie if you need help with this. (optional)` | `E.g.: https://ourworldindata.org/search?datasetProducts=World%20Development%20Indicators.` |

The table above is the single source of truth — if the Slack form's wording changes, update it here and nowhere else.

The exact draft file structure:

````markdown
# Data update comms draft — <dataset name>

Source: `<ns>/<new_version>/<short_name>` · Branch: `<branch>` · Generated: <iso datetime>

> Each section below mirrors the #data-updates-comms Slack form verbatim. Copy the answer block (inside ```text``` fences) into the matching field in the Slack form. `[filled]` answers are mechanical and safe to use as-is. `[prompt — user rewrites]` answers are seeded with snippets — rewrite them in your own voice.
>
> **Before you write the editorial fields (#6, #7, #8):** these go to Charlie, who turns them into public-facing posts on social media and in newsletters — not into an internal team log. Don't write what you'd say to your colleagues ("I updated all the WDI charts"). Write what you'd say to a friend who asks what you did this week ("I updated hundreds of our charts to the latest release of the World Bank's largest dataset, called the World Development Indicators…"). Try to take a reader-centric view: what work was done, what it changes/enables, what's interesting about the source, what it helps people understand about the world, why anyone should care it's been updated.

---

## What dataset(s) did you update?

> E.g.: UN World Population Prospects.

[filled]
```text
<Dataset title — Producer>
```

## When was this data released? When is the next scheduled release / our plan for next update?

> E.g.: released: Jul 2024; next: Jul 2027, and we'll update that month.

[filled] / [verify]
```text
Released: <date_published>. Next: <best-effort guess>.
```

## Who is the data source(s)? Is there anything our users should know about them?

> E.g.: Maintained by researchers at the University of California and the Max Planck Institute for Demographic Research.

[filled]
```text
<producer>. <citation_full or attribution_short, trimmed>.
```

## What's the coverage of the data in terms of years and countries/regions?

> E.g.: Covers the years 1991–2024, global total only.

[filled]
```text
Covers <year_min>–<year_max>, <n_countries> countries<, plus OWID regions if applicable>. <Sparse-recent-year flag if applicable.>
```

## How many charts did this update affect?

> Was this a small update affecting a handful of charts, or a massive one affecting tens or even hundreds?

[filled]
```text
<N> published charts (<size qualifier>).
```

## What does this dataset help our users understand about the world, and why is it important they know that?

> In other words: Why do we have this data on OWID at all? What is unique about this dataset compared to similar ones?

[prompt — user rewrites]

_This is the most important field. Write it as you'd describe the dataset to a curious friend, not to a colleague. What question does this data help answer? What's unique about this source vs. alternatives? Why should someone outside OWID care?_

_Snippets to draw from:_
- snapshot description: "…"
- garden description: "…"
- top indicator description_short: "…"

```text
<your draft answer here>
```

## Any important caveats or pitfalls in interpretation that users should know about this data? (optional)

> E.g.: note that saying "Country X is more productive than Country Y" is often taken as "people in Country X work harder".

[prompt — user rewrites]

_Candidate caveats from indicator `description_key` bullets, sanity-check workarounds, and methodology notes:_
- "…"
- "…"

```text
<your draft answer here, or leave empty if no caveats>
```

## Anything interesting to note about this update, including what you had to do? Anything else you'd like to add? (optional)

> E.g.: you worked with the data provider to improve the data somehow.

[prompt — user rewrites]

_Frame this from the reader's perspective, not the engineer's. "We flagged three issues to the source" is too technical and internal. "We spotted several issues with the data on vaccination in Sub-Saharan Africa and emailed the WHO team, who has now corrected them" is much more helpful for Charlie. Skip routine engineering work; lead with anything a non-OWID reader would find genuinely interesting._

_Context from this PR:_
- <commit subject lines unique to this PR>
- <resolved sanity_checks workarounds, if any>
- <snapshot row delta vs old version, if available>

```text
<your draft answer here, or leave empty if nothing notable>
```

## Add 1–3 chart views we might use in the public announcement

> Pick chart views that represent the whole dataset, rather than, e.g., something very specific about a single country.

[filled]

1. **<title>** — `<slug>` — <rationale>
2. **<title>** — `<slug>` — <rationale>

## Link to the updated charts as a search result (not a chart collection anymore). Ask Charlie if you need help with this. (optional)

> E.g.: https://ourworldindata.org/search?datasetProducts=World%20Development%20Indicators.

[filled]
```text
https://ourworldindata.org/search?datasetProducts=<urlencoded producer or dataset title>
```
````

**Strict rules for the verbatim text:**
- Do not paraphrase the prompt headings — they must match the Slack form character-for-character.
- Keep the example line as a `> E.g.: …` blockquote directly under each heading. Do not delete it (the user reads it for tone calibration), do not paste it into our answer.
- Each answer that is meant to be copy-pasted goes in a ```text``` fenced block. Editorial prompts may have unfenced snippet bullets above the fenced block, but the actual draft answer always sits inside fences.
- Optional sections (#7, #8, #10) keep their `(optional)` suffix.
- Notes about scope corrections, source-of-truth caveats (e.g. "queried live DB instead of staging"), or branch-version mismatches go **below the form** under a `## Pending mechanical follow-ups` section — never inside the verbatim Slack fields.

## Critical rules

- **Never invent dates, producer names, or chart counts.** If the source is missing or stale, mark `[missing]` and stop on that line — don't paper over a gap.
- **Published charts only** for the chart count. Same rule as `update-dataset` step 8.
- **Don't write the editorial fields as prose.** The user explicitly does not want LLM-voiced "Why we have this data" text — that's exactly the part the human wants to write themselves. Output snippets, not prose.
- **Don't post to Slack from the skill.** Output a Markdown file and stop. Posting is a human action.
- **Use `urllib.parse.quote_plus`** (not `quote`) for the search URL — Slack's input expects `+` for spaces in `datasetProducts`.
- **Always run from a green garden build.** If `data/garden/<ns>/<ver>/<sn>` doesn't exist, tell the user to run the garden step before retrying. Don't fabricate coverage from the snapshot alone — the garden output is what matters for the Slack post.

## Things to avoid

- Don't auto-write the optional caveats / interesting-notes fields with generic prose ("This dataset offers important insights into…"). Leave them as prompts.
- Don't query the staging DB without the `publishedAt IS NOT NULL` filter — drafts in the count would mislead.
- Don't use the producer's homepage URL as the search link. The Slack template specifically wants `ourworldindata.org/search?datasetProducts=…`.
- Don't fold this skill into `update-dataset` — keep it standalone so users can invoke it after manual updates too. `update-dataset` step 9 should delegate here, not duplicate the logic.

## Related

- `.claude/skills/update-dataset/slack-announcement-template.md` — the template this skill fills.
- `.claude/skills/update-dataset/SKILL.md` step 9 — the orchestrator entry point that should call this skill.
- `.claude/skills/chart-text-report/SKILL.md` — reuses the same grapher-channel metadata patterns for chart-view selection.
