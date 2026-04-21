---
name: create-analysis-doc
description: Draft a technical companion document for an OWID analysis, visualization, or data product in the `docs/analyses/` style. Use when the user wants to write technical documentation that explains the methodology, data sources, and limitations of an analysis — typically as a companion page for an article or a bespoke visualization. Trigger phrases include "write a technical documentation", "methodology doc", "companion doc for the ... article", "explain this analysis".
metadata:
  internal: true
---

# Create technical analysis documentation

Produce a companion methodology document in the style of the existing publications under [docs/analyses/](../../../docs/analyses/) (e.g. `biofuels_land_use/index.md`, `media_deaths/methodology.md`, `slavery_historical_data/index.md`).

The goal is a document a reader can use as the "technical appendix" to a public-facing article: it describes the data sources, the processing applied, the methodological choices and their justification, known limitations, and links to both the source code (ETL steps, visualization components) and the external datasets.

## 1. Gather inputs from the user

Before writing anything, ask the user for the following. Present them as a checklist so they can answer in one go:

**Required**

- **Title** of the analysis / visualization (will become the `# H1` of the doc).
- **Short name** — snake_case, used as the folder name under `docs/analyses/` (e.g. `inequality_visualization`). If not provided, suggest one based on the title.
- **ETL scripts** — paths to the meadow / garden / grapher / external steps that process the data. Read each one to understand what transformations are applied.
- **External data source URLs** — the canonical pages for every upstream dataset used (e.g. World Bank PIP, FAOSTAT, WDI indicator pages). These populate the "Data sources" section.

**Optional but strongly encouraged**

- **Companion article URL** — the OWID article this doc supports. If not yet published, leave `[TODO: article URL]` as a placeholder in the intro.
- **Visualization code paths** — if the analysis is backed by a bespoke component in `owid-grapher` (e.g. `bespoke/projects/<name>/`), ask for the entry point and any key utility files. Read them to describe display choices and methodological details at the visualization level.
- **Author name(s)** — default to the current git user if not given, with today's date in the header.
- **Known limitations** — things the user wants to flag explicitly (e.g. survey coverage gaps, interpolation assumptions).

Confirm the collected inputs back to the user before drafting — don't silently proceed if something is missing or ambiguous.

## 2. Read the scripts and external pages

Before writing a single section, actually **read the code** and the external dataset pages. Notes to take:

- **For each ETL step**: what does it take in, what does it output, what transformations are applied, what sanity checks are run, what the update cadence is. Pay particular attention to non-obvious decisions (manual override lists, hard-coded thresholds, filter criteria) — these are the things readers won't discover by themselves and that belong in the methodology section.
- **For each visualization script**: what computations are done at display time vs. upstream, what display choices (log vs. linear, smoothing parameters, color assignments) are being made, and why.
- **For each external source**: what welfare concept it measures, what year coverage, what the intended use case is, what its known caveats are.

If the user mentions an article, use `WebFetch` to read it so the doc's framing matches the article's framing.

## 3. Draft to `ai/<short_name>.md` first

Per project convention, initial drafts go to `ai/`. Once the user approves, the file is copied to `docs/analyses/<short_name>/index.md`.

Use this **document structure** as a starting skeleton. Adapt sections for the specific analysis — not every doc needs every section, and some analyses will need entirely new ones.

```markdown
# Title of the analysis

!!! info ""
    :octicons-person-16: **[Author](https://ourworldindata.org/team/author)** • :octicons-calendar-16: <date> *(last edit)* • [**:octicons-mail-16: Feedback**](mailto:info@ourworldindata.org?subject=Feedback%20on%20technical%20publication%20-%20<title>)

## Introduction

(2–3 short paragraphs. First: what the analysis/visualization is and what article it accompanies. Second: what this document covers and how it relates to the article. Third: a one-line summary of the data sources — this acts as a bridge to the next section.)

## Data sources

### <Source 1>

(Prose intro. Then sub-subsections for specific datasets within that source.)

#### <Specific dataset>
- What the file contains, at what granularity, in what units.
- How the source builds it (e.g. survey methodology, interpolation rules).
- Release cadence and coverage.
- Any caveats about the raw data (top-income coverage, welfare concept differences, etc.). Use `!!! warning "Title"` for hard caveats.
- File structure (columns) if relevant.
- Collapse long lists (country enumerations, indicator lists) into `??? quote "..."` blocks.

### <Source 2>

(Same pattern.)

## Methodology

### <Processing step 1>

(What the ETL step does. Link directly to the garden/meadow/external step file on GitHub.)

### <Analytical method 1 — e.g. KDE, smoothing, aggregation>

(Explain the method. Lead with intuition, then the arithmetic. Include a parameter table if relevant. Use `!!! info "Why X?"` to answer obvious "why did you choose this?" questions inline.)

## <Additional H2 sections — only as needed>

(Add one or more extra top-level sections here when the analysis has specific concerns that deserve their own walk-through, separate from the generic "Methodology" heading. Do not add placeholder sections just because another doc had them — only include headings whose content is genuinely warranted by this analysis.)

## Limitations

(Bullet list. Each point leads with a **bold term** and then a short paragraph. Prefer to include every limitation the user or the source materials have flagged.)

## References

**Our World in Data source code**

- Visualization component (with GitHub URL)
- ETL steps (meadow, garden, grapher), each with GitHub URL
- Snapshots (with GitHub URL)

**External datasets and references**

- Upstream data sources, each with a direct link
- Methodology pages published by the upstream providers
- Any related external literature
```

### When to add extra H2 sections

The canonical skeleton above (Introduction, Data sources, Methodology, Limitations, References) is the minimum. Add extra H2 sections only when the analysis has a concern substantive enough to deserve its own heading — never add a section just because another doc had it. Some signals that an extra section is warranted:

- **The analysis has a headline metric whose computation is non-trivial.** Give that computation its own H2 and walk through a concrete worked example (e.g. the "Share of population below a poverty line" section in the inequality-visualization doc, or "How we estimate land used for biofuels" in the biofuels-land-use doc).
- **The visualization lets the user toggle units, currencies, or time intervals, and those toggles involve non-obvious conversion logic.** Describe the base unit, the conversion factor, and where any manual decisions live.
- **A representation choice (log/linear axis, smoothing parameters, color palette, binning strategy) is substantive enough to affect interpretation.** Explain what the choice is and why it was made — but only if the reasoning is non-obvious.
- **There is an analysis-specific workflow that doesn't fit under "Methodology"** — e.g. query construction rules, sampling design, manual adjudication lists, validation procedures.

If the analysis is simple enough to cover in the canonical four sections, leave it at four.

## 4. Apply the style conventions

Consistency with the existing analyses docs is important — they deploy through Zensical (mkdocs-material-compatible) and rely on specific admonition syntax:

- **Header metadata**: `!!! info ""` (empty title) with person/calendar/feedback icons.
- **Callouts**: `!!! note`, `!!! warning`, `!!! info "Title"`, `!!! tip`.
- **Collapsible long content**: `??? quote "Title"` — used for country lists, full query listings, anything that would dominate the page when expanded inline.
- **Tables** for parameter lists, indicator codes, lookup mappings, rule summaries.
- **Worked examples with concrete numbers** — prefer "Brazil has 250 bins below the line, which is 25%" over abstract algebra.
- **Bold on first mention** for key terms (e.g. **International Dollars**, **PPP factor**, **bin**).
- **Mathematical typography**: use subscripts (log₂, not log2), proper minus signs, and Unicode for fractions only when they'd aid readability.
- **Links in markdown `[text](url)` syntax** — no bare URLs. Link GitHub files to their `owid/<repo>/blob/master/...` path so they stay valid as code changes.
- **Prefer prose over deeply nested bullets** — if a bullet list has three levels of nesting, rewrite as paragraphs with a short concluding list.

## 5. Writing principles (lessons from past sessions)

- **Lead with intuition, not implementation.** Readers want to understand the *idea* before the code. For a computation like "share below the line", first explain "each bin represents 1/1000 of the population; count bins below, divide by 1000" — then mention the `(k / 10) * pop` expression and *why* it collapses that way.
- **Explain "why" not just "what".** Every methodological choice (log scale, fixed KDE bandwidth, PPP-over-WDI preference, 3% reconciliation threshold) should come with a one-sentence justification. If you don't know why, ask the user — don't guess.
- **Be explicit about approximations and error bounds.** If the computation miscounts by ≤0.05 pp, say so. Readers trust a doc that surfaces its own limitations.
- **Separate the upstream decision from the downstream display.** When a module only formats pre-processed data (like `incomePlotUtils.ts` only formatting pre-converted currency values), call that out — readers shouldn't have to guess where the transformation happens.
- **Collapse long lists.** A 218-country enumeration in the body destroys readability. A `??? quote` block keeps the content available without forcing it on every reader.
- **Keep the References section structured.** Group OWID source code separately from external datasets — readers who want to inspect the code have different needs from those who want the upstream source.

## 6. Review with the user

Once the draft is in `ai/<short_name>.md`, ask the user to review. Expect multiple rounds of edits — technical documentation often benefits from iteration, especially:

- Corrections to methodological framing (a specialist reader may reframe approximations or point out the wrong level of detail).
- Filling in placeholders (article URLs, author names, exact data cadence).
- Expanding limitations or adding concrete country examples.

Only move to the deployment step once the user explicitly signs off.

## 7. Deployment (after user approval)

The file needs to move from `ai/` to `docs/analyses/` and be registered so it appears on `docs.owid.io`:

```bash
mkdir -p docs/analyses/<short_name>
cp ai/<short_name>.md docs/analyses/<short_name>/index.md
```

Then ask the user whether they also want the two registration steps done now or in a follow-up:

1. **Nav entry in [zensical.toml](../../../zensical.toml)** — add one line to the analyses block (search for the `"analyses/index.md"` entry):
   ```toml
   { "<User-facing title>" = "analyses/<short_name>/index.md" },
   ```
2. **Landing-page card in [docs/analyses/index.md](../../../docs/analyses/index.md)** — add a `!!! note ""` block following the existing pattern (short blurb, Methodology button linking to the doc).

Preview with `make docs.serve` at `http://localhost:9010/analyses/<short_name>/`.

## 8. PR workflow

If the user wants a PR for the doc, follow the project's standard flow ([CLAUDE.md → Git Workflow](../../../CLAUDE.md)):

```bash
.venv/bin/etl pr "<Title>" docs
git add docs/analyses/<short_name>/index.md [zensical.toml] [docs/analyses/index.md]
git commit -m "📜🤖 <Title>"
git push -u origin <branch>
gh pr edit <number> --body "..."
gh pr comment <number> --body "@codex review"
```

Use the **📜 docs emoji** (plus 🤖 for AI-assisted work). The PR body should include:

- A "Summary" section describing what the doc covers at a high level.
- A "Follow-ups" section listing anything not done in this PR (nav entry, landing-page card, unresolved placeholders).
- A "Test plan" checklist (local render check with `make docs.serve`, external link check, collapsible-block check).
