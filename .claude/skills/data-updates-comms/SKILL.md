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
- After an `update-dataset` run finishes (step 9 of that skill should delegate here with `workbench/<short_name>/update-context.yml` rather than re-implement).
- After a manual update where the user wants help drafting the announcement.

## Inputs

- Preferred after `update-dataset`: `workbench/<short_name>/update-context.yml` — reuse gathered facts first, and only gather missing fields directly.
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
| Chart views (1–3) | `update-context.yml` candidates OR query staging directly using the criteria below | filled with rationale, user confirms |
| Search URL | `https://ourworldindata.org/search?datasetProducts=<urlquote(producer)>` | filled |

**The editorial fields are deliberately not auto-prosed.** Slack posts in the editorial voice ("Why we have this dataset on OWID") read flat when LLM-written; the value is in the human framing. The skill's job is to surface the relevant snippets so the user doesn't have to grep for them.

## Workflow

0. **Reuse update context if available.**
   - If `workbench/<short_name>/update-context.yml` is provided or exists, read it first and use its values for mechanical fields, chart count/views, and editorial snippets.
   - Continue with the steps below only for fields missing from that context. This keeps `update-dataset` responsible for gathering context during the update while preserving this skill as a standalone fallback.

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
   - Map the count to a qualifier: 1–9 = "handful", 10–49 = "moderate", 50–199 = "large", 200+ = "massive".
   - **Only count published charts.** Drafts are excluded by design — the Slack audience cares about user-facing impact.

4. **Seed the editorial fields with snippet bullets.**
   For each of "why it matters", "caveats", "anything interesting": don't write prose, write 2–6 substantive bullets above an empty `text` fenced block. Bullets are clean reader-facing prose — **no `Snapshot description:` / `Garden description:` prefixes** in the output. Use those as internal source pointers only.

5. **Pick chart views.**
   - Reuse `charts.selected_views` from `update-context.yml` if present.
   - Otherwise query published charts on staging (same SQL as step 3 but selecting `c.id, cc.slug, cc.full->>'$.title', cc.full->>'$.type', cc.full->>'$.hasMapTab'`).
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
   - Use the canonical format in the Output format section below — no example lines, no `[filled]` / `[prompt]` tags, no inline instructions. If a field can't be filled mechanically, write `[missing — <what's needed>]` inside the fenced block and stop.

9. **Show the user the file path** and stop. Do **not** post to Slack — that's a human action. The user copy-pastes from the Markdown file into the Slack form.

## Output format

**The output file must use the Slack form's prompt wording verbatim as section headings.** The user copy-pastes the answer text into the matching Slack fields, so each prompt is its own `## <verbatim heading>`.

**Keep the file lean.** No "E.g.:" example lines, no `[filled]` / `[prompt — user rewrites]` tags, no inline framing instructions, no `_Snippets to draw from:_` / `_Candidate caveats:_` / `_Context from this PR:_` label preambles, no ```text``` code fences around the answers. The skill keeps the framing reminders for itself (see "Editorial framing" below); the file is just headings → answer prose (for mechanical fields) or snippet bullets (for editorial fields).

The verbatim Slack prompt headings (do **not** rephrase, abbreviate, or change punctuation):

| # | Prompt heading (verbatim) |
|---|---|
| 1 | `What dataset(s) did you update?` |
| 2 | `When was this data released? When is the next scheduled release / our plan for next update?` |
| 3 | `Who is the data source(s)? Is there anything our users should know about them?` |
| 4 | `What's the coverage of the data in terms of years and countries/regions?` |
| 5 | `How many charts did this update affect?` |
| 6 | `What does this dataset help our users understand about the world, and why is it important they know that?` |
| 7 | `Any important caveats or pitfalls in interpretation that users should know about this data? (optional)` |
| 8 | `Anything interesting to note about this update, including what you had to do? Anything else you'd like to add? (optional)` |
| 9 | `Add 1–3 chart views we might use in the public announcement` |
| 10 | `Link to the updated charts as a search result (not a chart collection anymore). Ask Charlie if you need help with this. (optional)` |

The table above is the single source of truth — if the Slack form's wording changes, update it here and nowhere else.

### Editorial framing (internal — do **not** copy into the output file)

Before drafting fields #6, #7, #8, remember they go to Charlie, who turns them into public-facing posts — not into an internal team log. Snippets should sound like what you'd tell a curious friend, not a colleague: reader-centric, with a concrete number where possible, and what's interesting about the source. The agent uses this framing to *select* and *phrase* the snippets; the framing itself is never written into the output.

### Snippet selection per editorial field

- **Why does this matter (#6)** — pull 3–5 short bullets that, taken together, answer "why do we have this data on OWID at all?". Draw from `meta.origin.description`, garden dataset description, and top indicator `description_short`; rephrase into clean prose. Do **not** prefix each bullet with its source label (e.g. don't write `Snapshot description: "…"`); just write the substantive content.
- **Caveats (#7)** — pull 2–4 bullets surfacing real interpretation pitfalls. Draw from indicator `description_key` bullets, sanity-check workarounds, and methodology notes. Skip if there are no load-bearing caveats.
- **Interesting (#8)** — pull 3–6 bullets describing concrete findings or noteworthy events captured in this update (new policies, reversals, changed countries). Draw from `editorial_context.interesting_update_snippets` in `update-context.yml`, commit messages, and resolved workarounds. Phrase them as reader-facing facts, not engineering notes.

### Exact draft file structure

```markdown
# Data update comms draft — <dataset name>

Source: `<ns>/<new_version>/<short_name>` · Branch: `<branch>` · Generated: <iso datetime>

---

## What dataset(s) did you update?

<Dataset title — Producer>

## When was this data released? When is the next scheduled release / our plan for next update?

Released: <date_published>. Next: <best-effort or "unknown — …">.

## Who is the data source(s)? Is there anything our users should know about them?

<producer>. <citation_full or attribution_short, trimmed>.

## What's the coverage of the data in terms of years and countries/regions?

Covers <year_min>–<year_max>, <n_countries> countries<, plus OWID regions if applicable>. <Sparse-recent-year flag if applicable.>

## How many charts did this update affect?

<N> published charts (<size qualifier>).

## What does this dataset help our users understand about the world, and why is it important they know that?

- <substantive snippet>
- <substantive snippet>
- <substantive snippet>

## Any important caveats or pitfalls in interpretation that users should know about this data? (optional)

- <caveat snippet>
- <caveat snippet>

## Anything interesting to note about this update, including what you had to do? Anything else you'd like to add? (optional)

- <interesting snippet>
- <interesting snippet>

## Add 1–3 chart views we might use in the public announcement

1. **<title>** — `<slug>` — <one-line rationale>
2. **<title>** — `<slug>` — <one-line rationale>

## Link to the updated charts as a search result (not a chart collection anymore). Ask Charlie if you need help with this. (optional)

https://ourworldindata.org/search?datasetProducts=<urlencoded dataset title>

---

## Pending mechanical follow-ups

- <only if any — e.g. "next release date is best-effort", "verify producer's release cadence", source-of-truth caveats>
```

**Strict rules:**
- Do not paraphrase the prompt headings — they must match the Slack form character-for-character.
- No code fences around answers — they sit as plain prose under each heading.
- Editorial fields (#6, #7, #8) carry snippet bullets only; the user writes their own answer when filling the Slack form.
- Snippet bullets must be substantive prose, not labelled quotes. Drop source prefixes like "Snapshot description:" — the user shouldn't see them.
- Optional sections (#7, #8, #10) keep their `(optional)` suffix.
- Notes about scope corrections, source-of-truth caveats, or branch-version mismatches go **below the form** under a `## Pending mechanical follow-ups` section — never inside the verbatim Slack fields.

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
- Don't fold this skill into `update-dataset` — keep it standalone so users can invoke it after manual updates too. `update-dataset` should gather reusable facts in `update-context.yml` and step 9 should delegate here, not duplicate the Slack rendering logic.

## Related

- `.claude/skills/update-dataset/SKILL.md` step 9 — the orchestrator entry point that should call this skill.
- `.claude/skills/faust-metadata-audit/SKILL.md` — reuses the same grapher-channel metadata patterns for chart-view selection.
