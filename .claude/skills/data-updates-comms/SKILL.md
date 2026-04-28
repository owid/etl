---
name: data-updates-comms
description: Draft answers for OWID's #data-updates-comms Slack template using snapshot DVC + garden metadata + staging DB queries. Use when the user wants to announce a dataset update, fill the "Message about new data update" form, or generate the FAQ-style Slack post after an ETL update. Mechanical fields (producer, dates, coverage, chart count, search URL) are filled directly; editorial fields (why it matters, caveats, what's interesting about this update) get prompts seeded with extracted context for the user to refine.
metadata:
  internal: true
---

# Data update communications draft

Generate a draft for the #data-updates-comms Slack form. The skill inspects the **current branch** of the ETL repo to fill the mechanical fields and seeds the editorial fields with context the user then rewrites in their own voice.

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

The draft file should look like:

```markdown
# Data update comms draft — <dataset name>

Source: `<ns>/<new_version>/<short_name>` · Branch: `<branch>` · Generated: <iso datetime>

---

## What dataset(s) did you update?
[filled] <Dataset title — Producer>

## When was this data released? When is the next scheduled release?
[filled] Released: <date_published>.
[verify] Next: <best-effort guess from url_main, or "unknown">.

## Who is the data source(s)?
[filled] <producer>. <citation_full or attribution_short, trimmed>.

## What's the coverage?
[filled] Covers <year_min>–<year_max>, <n_countries> countries<, plus OWID regions if applicable>.
<Sparse-recent-year flag if applicable.>

## How many charts did this update affect?
[filled] <N> published charts (<size qualifier>).

## What does this dataset help our users understand?
[prompt — user rewrites]
Snippets to draw from:
- snapshot description: "…"
- garden description: "…"
- top indicator description_short: "…"

## Caveats?
[prompt — user rewrites]
Candidate caveats from indicator description_key bullets:
- "…"
- "…"
<sanity-check workarounds, if any, from notes_to_check.md>

## Anything interesting about this update?
[prompt — user rewrites]
Context from this PR:
- <commit subject lines unique to this PR>
- <resolved sanity_checks workarounds, if any>
- <snapshot row delta vs old version, if available>

## Chart views to feature
1. **<title>** (`<slug>`) — <rationale>
2. **<title>** (`<slug>`) — <rationale>

## Search URL
[filled] https://ourworldindata.org/search?datasetProducts=<urlencoded producer>
```

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
