---
name: migrate-source-to-origin
description: Migrate a legacy `meta.source` block to a modern `meta.origin` block in an OWID snapshot DVC file. Use when the user asks to migrate, convert, or rewrite a snapshot's `source:` to `origin:`, or to fix an existing `origin` block per OWID style.
model: sonnet
metadata:
  internal: true
---

# Migrate Source → Origin

Migrate a legacy `meta.source` block to a modern `meta.origin` block in a single
OWID snapshot DVC file, following OWID's documented Origin style.

This skill is purely instruction-driven. You (the assistant) do the reasoning,
the file reads/writes, and the validation. There is no helper script.

## When to use

- The user asks to migrate, convert, or rewrite a snapshot's `source:` to `origin:`.
- The user asks to fix or clean up an existing `origin` block per OWID style.
- The user references a snapshot DVC path containing `meta.source` or a poorly-formed `meta.origin`.

## Inputs

- **Required:** path to a single snapshot DVC file (e.g. `snapshots/labor/2017-07-31/child_labor_in_us__long__1958.feather.dvc`).
- **Optional:** any user override for fields that are hard to infer (e.g. they may
  hand you the canonical `producer` or `title`).

If the user requests a batch (e.g. "all paper-cited snapshots"), apply this
skill once per file in a loop.

## Workflow

1. **Read the DVC file.** Note:
   - The legacy `meta.source` block (and any sibling `meta.name`,
     `meta.description`, `meta.license`). `meta.name` is OWID's internal
     label, NOT the producer's title.
   - If a prior `meta.origin` already exists, you're replacing it — but
     still treat the legacy `meta.source` (if present) as the source of truth.
   - The `outs:` block (and any other top-level YAML keys) — preserve verbatim.

2. **Apply STEP 1 (coincidence test)** — see below — to decide whether
   `title_snapshot` and `description_snapshot` should be set at all.

3. **Apply STEP 2 (field rules)** — see below — to fill each field of the
   proposed origin object. Skip optional fields rather than fabricate values.

4. **Apply OWID writing style** — see below — to all prose fields.

5. **Self-check against the lint rules** — see "Lint rules" below. Quietly
   fix any violations.

6. **Write the new `meta` block.** Replace the existing `meta:` block
   (whether it was `meta.source` or a prior `meta.origin`) with the new
   `meta.origin`. Leave `outs:` and any other top-level YAML keys untouched.
   See "YAML output conventions" below for shape.

   *Note:* the `Edit` tool drops YAML inline comments. If the legacy file
   has section comments like `# Data product / Snapshot`, `# Citation`,
   `# Files` and you want to preserve them, rewrite via `ruamel` in a Bash
   one-liner instead of `Edit`. Most files don't have such comments, so
   `Edit` is fine by default.

7. **Validate** the rewritten file:
   ```bash
   .venv/bin/python -c "from etl.snapshot import SnapshotMeta; SnapshotMeta.load_from_yaml('<dvc_path>')"
   ```
   If this fails, read the error, fix the YAML or the offending field, and retry.

---

## STEP 1: Does the data product coincide with this snapshot?

This is the central decision. Most snapshots coincide with their data product —
that means `title_snapshot` and `description_snapshot` are NULL (omitted).

**THEY COINCIDE** in all of these cases (the snapshot IS the data product):

- The source is a single paper, journal article, working paper, book, or thesis.
- The source is a one-off study with one accompanying dataset (no follow-up
  release series).
- The source is an OWID compilation of multiple raw sources combined into one
  snapshot — the "data product" IS the compilation.

When they coincide, **`title_snapshot` is NULL and `description_snapshot` is NULL**.
Every meaningful sentence of the legacy `source.description` (whether descriptive
of the data or describing OWID's processing) goes into `description`. The fact
that the legacy `meta.name` phrases the snapshot differently from the producer's
title does NOT mean they differ — `meta.name` is just an internal OWID label,
not a producer-defined slice.

**THEY DIFFER** (snapshot is a slice of a larger product) ONLY when BOTH:

- The producer publishes a named multi-product database or report series
  (Maddison Project DB, V-Dem, Penn World Table, FAO databases, OECD reports,
  Statistics Canada tables, Correlates of War, World Bank WDI, etc.), AND
- This snapshot picks one specific producer-defined named slice (e.g. one
  table, one indicator group, one named topic). The slicing is the producer's,
  not OWID's.

When they differ:

- `title` = the data product name (e.g. `Penn World Table`).
- `title_snapshot` = `<data product> - <slice>` (e.g. `Penn World Table - National Accounts`).
- `description` = the data product (the producer's whole database).
- `description_snapshot` = OWID's processing notes for this slice, when present.

If you cannot point to a producer-published name for the broader data product
and a producer-defined name for the slice, default to "they coincide".

## STEP 2: Field rules

### `producer` (required, ≤255 chars)
Institution or author(s).
- One author: `Williams`. Two: `Williams and Jones`. Three or more: `Williams et al.`
- Prefer well-known acronyms (`NASA`, `FAO`); else the full institution name.
- For OWID compilations of several distinct raw sources, use `Various sources`.
- Must NOT contain: years, semicolons, `&` (use `and`), `OWID` / `Our World in Data`,
  trailing period (except when value ends `et al.`).
- Strip OWID-derivation prefixes: `Our World in Data based on X` → `X`.

### `title` (required, ≤512 chars)
Sentence-case start, no trailing period, no semicolons, no producer/version unless
they're part of the canonical name (`Education at a Glance 2017`).

Use whichever string is the most informative producer-issued title that passes lint.
If `source.name` contains the producer name, year, or other lint-violating bits,
it's an OWID label, not a real title — go to `published_by` for the publication's
actual title. Strip any year suffix from a clean `source.name` before using it
(e.g. `Whale populations (Pershing et al. 2010)` → use the paper's actual title from
`published_by`; `Cherry Blossom Full Bloom Dates in Kyoto, Japan` → use as-is).

### `title_snapshot` (default omitted)
Set ONLY when the data product and snapshot differ (STEP 1). Format:
`<Data product> - <Slice>`. No year, no version, no producer, no period.

### `description` (default omitted when the legacy source has no data-product description)
Describes the producer's data. Sentence-case, end with period, 1–3 short paragraphs.
- DO NOT FABRICATE for papers/books/articles: a paper/book title alone is not a
  description. If the legacy has no descriptive content about the data, omit
  `description`.
- **Multi-product database exception**: if the legacy source has no description of
  the parent data product but the database is widely-known (FAO, World Bank, OECD
  reports, Sea Around Us, etc.), it is acceptable to write **one short factual
  sentence** describing what the database is, drawn from common knowledge of what
  the producer's own landing page would say. Do not extrapolate beyond that.
- Sentences belonging here: what the data is, who collected it, scope, producer
  methodology, pointers to the producer's own materials ("See the authors' data
  appendix").
- Preserve paragraph structure from the legacy `source.description` — use real
  newline-separated paragraphs in a YAML literal block (`|-`), not a single
  collapsed paragraph and not literal `\n\n` sequences.

### `description_snapshot` (default omitted)
Set ONLY when the data product and snapshot DIFFER (STEP 1).
If they coincide, `description_snapshot` is ALWAYS omitted even if the legacy
description contains OWID processing notes — fold those into `description`.

### `citation_full` (required)
Producer's preferred citation; long is OK. Start capital, end with a period,
include the publication year. Where legacy `source.published_by` is a full
citation, it goes here.

### `attribution_short` (default omitted, ≤512 chars)
Set ONLY when there is a well-known acronym or short brand strictly shorter and more
recognizable than `producer` (e.g. `FAO`, `WHO`, `V-Dem`). If `producer` is already
short (`Fouquin and Hugot`, `World Bank`, `NASA`), omit. No year, no period.

### `version_producer` (default omitted, ≤255 chars)
Set ONLY when the producer issues a series of releases AND uses a release identifier
that the legacy source mentions (e.g. `v14`, `Version 3`, `4.0.1.0`, `25.1`, or for
truly annual releases the year as identifier — Maddison Project DB, Total Economy DB).
Never set for a one-off paper/study. Working-paper numbers like `2016-14` are paper
IDs, not versions — never use them here.

### `date_published` (required)
`YYYY-MM-DD` or `YYYY` or `latest`. The CURRENT version's publication date. Never
pick a year that is part of a coverage range (`1827–2014`) or a projection
(`2030–2050`).

### `url_main` (default omitted when absent from legacy source)
Landing-page URL. Must appear VERBATIM in the legacy source — never invent one,
never swap a file extension or domain. Never use placeholders like `<UNKNOWN>`,
`N/A`, `TBD`. If no URL exists in the legacy source, omit.

### `url_download` (default omitted)
Direct-download URL. Must appear verbatim in the legacy source (typically
`source.source_data_url` or `source.url`). Otherwise omit.

### `date_accessed` (required)
`YYYY-MM-DD`, copied from the legacy source.

### `license` (default omitted)
Object with `name` and `url`. Omit if not present in the legacy source.

## Field order

Emit fields in this order (omit any that are null):

```
producer, title, description, title_snapshot, description_snapshot,
citation_full, attribution, attribution_short, version_producer,
url_main, url_download, date_accessed, date_published, license
```

## Anti-fabrication

- Never invent URLs (no extension swaps, no domain guesses).
- Never invent dates not implied by the legacy.
- Never invent a description from your own knowledge of the source (with the
  one-sentence multi-product-database exception above).
- Placeholders like `<UNKNOWN>`, `N/A`, `TBD` are forbidden — omit the field.
- When uncertain about an optional field, omit it.

### Minor cleanup is welcome

The following count as cleanup, not fabrication:

- Typo corrections in legacy text (e.g. fix `1980` → `1820` when context makes
  the intended year obvious).
- Expanding a clearly-implied institution from a URL clue (e.g. seeing
  `emp.lbl.gov` and adding `Lawrence Berkeley National Laboratory` to the
  citation).
- Curly quotes / smart quotes → straight quotes; non-breaking spaces → spaces.
- Trimming OWID-attribution prefixes (`Our World in Data based on X` → `X`).
- Sentence-casing producer-issued titles per OWID style.

Stop short of writing new content that wasn't implied by the legacy.

## OWID writing style

Apply to `description`, `description_snapshot`, `citation_full`, and any other
prose fields:

- American English (`analyzed`, not `analysed`).
- Sentence-case titles. "Data" is singular. Oxford comma in lists.
- En dashes (`–`) for year ranges (`1990–2020`); em dashes (`—`) with spaces
  for asides (` — like this — `).
- Double quotes; spell out 1–10 in prose.
- `US`, `UK`, `UN` without periods.
- Brand spelling: `Our World in Data`, `OWID`.
- Author surnames only in citations (`Williams et al.`, not `John Williams et al.`).

## Lint rules (self-check before writing)

Walk through these before writing the origin:

1. **`producer`** does not contain: a 4-digit year, `;`, `&`, `OWID` /
   `Our World in Data`, or a trailing period (unless ends `et al.`).
2. **`title`** does not end with a period.
3. **`citation_full`** ends with a period.
4. **`date_published`** matches `YYYY-MM-DD`, `YYYY`, or `latest`.
5. **DB lengths**: `producer` ≤ 255, `title` ≤ 512, `attribution_short` ≤ 512,
   `version_producer` ≤ 255 chars.
6. **`attribution_short`** is not identical to `producer`; does not contain a
   year; does not end with a period.
7. **`url_download`** is not identical to `url_main`.
8. **`url_main`** and `url_download`, if present, appear VERBATIM somewhere in
   the legacy source (in `source.url`, `source.source_data_url`,
   `source.published_by`, or `source.description`).
9. **`title_snapshot`** does not contain a 4-digit year. It does not include
   the producer's name UNLESS the producer is also part of `title`'s canonical
   name (e.g. `Education at a Glance 2017: OECD Indicators`).
10. **`version_producer`** does not match the working-paper pattern `YYYY-N`
    (e.g. `2016-14` is forbidden).
11. **STEP 1**: if data product = snapshot, `title_snapshot` and
    `description_snapshot` are both omitted.

## YAML output conventions

The rewritten DVC's `meta` block should look like this — clean, ordered,
no nulls, multi-paragraph descriptions in `|-` blocks, one quoting style:

```yaml
meta:
  origin:
    producer: Hegglin et al.
    title: 'Twenty questions and answers about the ozone layer: 2014 update'
    description: |-
      Figures represent emissions of ozone-depleting substances, with substances weighted by their potential to destroy ozone (their ozone-depleting potential). This gives a total value of emissions normalised to their CFC11-equivalents.

      Total emissions are inclusive of naturally-occurring and man-made emissions.

      Data is based on figure Q0-1 in "Twenty questions and answers about the ozone layer: 2014 update", published as the 2014 edition of the Scientific Assessment Panel of the Montreal Protocol.

      Data was extracted from the static figure Q0-1 using the extraction tool WebPlotDigitizer (https://apps.automeris.io/wpd/).
    citation_full: 'Hegglin, M. I., Fahey, D. W., McFarland, M., Montzka, S. A., and Nash, E. R. (2014). Twenty questions and answers about the ozone layer: 2014 update. World Meteorological Organization, UNEP, NOAA, NASA, and European Commission.'
    url_main: https://www.wmo.int/pages/prog/arep/gaw/ozone_2014/documents/2014%20Twenty%20Questions_Final.pdf
    date_accessed: '2026-03-10'
    date_published: '2014'
outs:
  - md5: 3d25db3427dbfc07cf8894638a7d54ec
    size: 3970
    path: ozone_depleting_emissions_since_1960__scientific_assessment__2014.feather
```

Key conventions:

- Two-space indentation throughout.
- Single quotes around any string that contains a colon (`title`,
  `citation_full`) so YAML doesn't misparse it.
- Quote dates as strings: `date_accessed: '2026-03-10'`, `date_published: '2014'`.
- Use `|-` block scalar for any multi-paragraph string. Indent paragraph
  content by 6 spaces (4 from `description` + 2 for the value). Separate
  paragraphs with one blank line.
- Drop the legacy `meta.source`, `meta.name`, `meta.description`,
  `meta.source_name`, `meta.published_by`, `meta.url`, etc. from the new
  `meta:` block. Only `meta.origin:` should remain inside `meta:`.
- Preserve `outs:` and any sibling top-level keys verbatim — only `meta:` is
  rewritten.

## Verification (recap)

```bash
.venv/bin/python -c "from etl.snapshot import SnapshotMeta; SnapshotMeta.load_from_yaml('<dvc_path>')"
```

A clean parse means the migration is good. If the parse fails, the most
common causes are: bad indentation, missing colons, unquoted strings
starting with a special character, or a malformed `date_published` (must
match `YYYY-MM-DD`, `YYYY`, or `latest`).

## Out of scope

- Batch driving via `dag/migrated.yml` — invoke this skill once per file and
  loop in your conversation.
- Indicator-level metadata (garden / grapher steps) — only snapshot DVC `meta.origin`.
