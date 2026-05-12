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

1. **Read the DVC file.** Identify the legacy shape:
   - **`meta.source:` block** (the common form). Sibling `meta.name`,
     `meta.description`, `meta.license` may also exist.
   - **Flat top-level fields under `meta:`** (older shape, no `source:`
     wrapper): `source_name`, `source_published_by`, `publication_year`,
     `publication_date`, `url`, `source_data_url`, `license_url`,
     `license_name`, `description`, `date_accessed`, `is_public`,
     `file_extension`, plus the internal `name`. Treat these as if they
     lived under `meta.source.*` — same field rules apply. Drop
     `namespace`/`short_name`/`version`/`file_extension`/`wdir` — they're
     derived from the file path.

   `meta.name` is OWID's internal label, NOT the producer's title — drop it.

   If a prior `meta.origin` already exists, you're replacing it — but
   still treat the legacy fields (if present) as the source of truth.

   The `outs:` block (and any other top-level YAML keys) — preserve verbatim.

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

7. **Audit the diff** before validating. Read every legacy field and
   confirm it maps somewhere in the new origin (or has a documented
   reason to drop). Watch for these common silent drops:
   - "Accessed YYYY-MM-DD" tail inside legacy `published_by` — drop from
     `citation_full` only because `date_accessed` carries the same fact.
   - Expanded methodology phrases in legacy description (e.g. "including
     journal articles, …") — keep verbatim in `description`, do not
     compress.
   - Downstream `dataset.description` that adds info beyond what the
     snapshot's description had — lift into `origin.description` (see
     "Downstream sweep" below).

8. **Validate** the rewritten file:
   ```bash
   .venv/bin/python -c "from etl.snapshot import SnapshotMeta; SnapshotMeta.load_from_yaml('<dvc_path>')"
   ```
   If this fails, read the error, fix the YAML or the offending field, and retry.

9. **Downstream sweep (when migrating snapshots in active chains).**
   Snapshots used by active garden/grapher steps often have matching
   `dataset.sources:`, `all_sources:`, or variable-level `sources:` /
   `descriptions:` blocks in the downstream meta.yml that duplicate the
   snapshot's source info. After the snapshot is migrated, scan each
   downstream meta.yml:
   - If `dataset.description` adds info beyond the snapshot's
     `description`, lift the extra into `origin.description`.
   - If a variable's `description_key` / `descriptions:` anchor carries
     per-source notes (e.g. "OWID uses WHO data for the following
     countries: …"), move those into the relevant snapshot's
     `origin.description_snapshot` (see the OWID-curated slice notes case
     under `description_snapshot`). The notes then travel with the origin
     to every chart that uses the snapshot.
   - **Variable-level `description:` → `description_short:` (do NOT lift
     to `origin.description`).** The legacy variable `description` field
     is a concept definition for the indicator (renders as the chart's
     subtitle/data-page short blurb), not source metadata. Rename it to
     `description_short:` at the same variable level. Lifting it into
     the snapshot's `origin.description` is wrong — it muddies the
     producer's data-product description with indicator-concept prose
     and removes the text from where charts actually display it.
   - Then strip the now-redundant `sources:`, `licenses:`, `descriptions:`,
     `all_sources:`, and `dataset.sources:` blocks from the downstream
     meta.yml.
   - The downstream `.py` may need refactoring too: `pd.read_csv(snap.path)`
     and `Table(df, ...)` patterns drop column origins. Use
     `snap.read_csv()` + `pr.concat` so origins flow through Table
     operations all the way to grapher.

   Optional for one-off snapshot migrations; mandatory when the
   downstream meta.yml carries rich source-level prose or when the
   chain's grapher step combines multiple snapshots.

---

## Core principle: restructure, don't rewrite

The purpose of this migration is to restructure information, not to
improve the writing. You're moving the legacy source's content into the
right fields of the new origin — splitting one description into
`description` + `description_snapshot`, extracting URLs and citations
from prose blobs into their own fields, fixing typos. **Not rewriting.**

Every fact, sentence, quoted passage, URL, and date in the legacy
`meta.source` must survive somewhere in the output. Verbose is fine;
long descriptions stay long. The only allowed prose changes are the
"Minor cleanup" list below. If the output is shorter than the legacy or
contains words you can't trace back, you've summarized.

## STEP 1: Does the data product coincide with this snapshot?

This is the central decision. Most snapshots coincide with their data product —
that means `title_snapshot` and `description_snapshot` are NULL (omitted).

**THEY COINCIDE** in all of these cases (the snapshot IS the data product):

- The source is a single paper, journal article, working paper, book, or thesis.
- The source is a one-off study with one accompanying dataset (no follow-up
  release series).
- The source is an OWID compilation of **multiple** raw sources combined into
  one snapshot — the "data product" IS the compilation. (Single-source-derived
  metrics are NOT compilations — see "differ" below.)

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
- `description` ← legacy paragraphs about the producer's broader database
  (what the database is, the producer's overall methodology, scope).
- `description_snapshot` ← legacy paragraphs about this specific slice
  (the indicator definition, OWID's processing notes for this slice).

Split the legacy `source.description` at its existing paragraph boundaries
and bucket each paragraph whole. Do **not** rewrite paragraphs to redistribute
information cleanly between buckets. If a paragraph straddles both topics,
leave it intact in whichever bucket fits more closely.

If the legacy has no paragraph describing the broader data product, **omit
`description`** rather than authoring one (see the strict one-sentence
exception under `description` below).

If you cannot point to a producer-published name for the broader data product
and a producer-defined name for the slice, default to "they coincide".

**OWID-defined slice exception.** If the legacy `meta.name` clearly
describes a topical slice that the producer doesn't publish under that
name (e.g. legacy `meta.name: "Perceptions of spending on health
expenditure - IPSOS (2016)"` vs. producer's title `Perils of Perception
Study`), use `title_snapshot: "<data product> - <slice descriptor>"` to
preserve the slice descriptor even though the slicing is OWID's. The
parent `title` is still the producer-issued title, and
`description_snapshot` stays empty unless the legacy has slice-specific
text. Without this, useful slice info from `meta.name` gets discarded
under the strict "meta.name is junk" rule.

## STEP 2: Field rules

### `producer` (required, ≤255 chars)
Institution or author(s).
- One author: `Williams`. Two: `Williams and Jones`. Three or more: `Williams et al.`
- Prefer well-known acronyms (`NASA`, `FAO`); else the full institution name.
- For an OWID compilation, name the producers explicitly when ≤3 are listed in
  the legacy (e.g. `PBL Netherlands Environmental Assessment Agency and FAO`).
  Reserve `Various sources` for compilations whose underlying sources are
  too numerous to enumerate, or where the legacy itself doesn't name them.
  In either compilation case, consider an `attribution_short` that gives the
  recognizable shorthand readers see on charts (often the dataset names, e.g.
  `HYDE and FAO`).
- **Paper-backed named products**: when a paper or research group has
  produced a recognizable named data product (a website, database, or
  project that a chart reader would identify — e.g. `Sea Around Us`,
  `V-Dem`, `World Inequality Database`, `Human Life-Table Database`),
  use the product name as `producer` rather than the author surnames.
  Keep the author names in `citation_full`. Acronyms (`V-Dem`, `WID`,
  `HLD`) usually belong in `attribution_short`, not `producer`. Apply
  this only when the product is clearly more chart-readable than the
  authors — for one-off papers with no public product, authors are
  correct.
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

### `description` (default omitted when legacy has no data-product description)
Verbatim from the legacy `source.description`. 1–3 short paragraphs in a
`|-` block. If the legacy has no descriptive content about the data,
omit. The one allowed addition is the **multi-product database
exception**: when STEP 1 says they differ AND the legacy has no paragraph
about the parent database AND the database is widely-known (FAO, World
Bank WDI, OECD reports, etc.), you MAY add one short factual sentence
identifying what the database is — never two sentences.

### `description_snapshot` (default omitted)
Set in two cases:
1. **Producer-defined slice (STEP 1 says they differ).** Verbatim from
   the legacy `source.description` — the paragraphs about this specific
   slice.
2. **OWID-curated slice notes.** When the snapshot feeds a combined
   indicator and the downstream grapher meta.yml has source-specific
   notes ("OWID uses WHO data for the following countries: …",
   per-source methodology asides), lift those notes here so they travel
   with the origin into every chart that uses the snapshot — instead of
   leaving them in `description_key` or `descriptions:` anchors at the
   variable level.

When the snapshot coincides with the data product AND has no OWID-curated
slice notes, omit `description_snapshot` and put everything in `description`.

**Source-property prose belongs on origin, not on variables.** When the
legacy `dataset.description` contains data-interpretation caveats that
are properties of how the producer reports the data (e.g. "Negative
values imply that quantities destroyed or exported exceeded the sum of
production and imports, so they came from stockpiles"), these are
source facts — they belong in `origin.description` (or
`description_snapshot`). Do NOT lift them into variable-level
`description_key` / `description_short`, which are for
indicator-concept text. Origin prose travels with the snapshot to every
chart that uses it; variable-level fields are indicator-specific and
don't follow the source.

### `citation_full` (required)
Producer's preferred citation; long is OK. Start capital, end with a period,
include the publication year. Where legacy `source.published_by` is a full
citation, it goes here.

### `attribution_short` (default omitted, ≤512 chars)
Set ONLY when there is a well-known acronym or short brand strictly shorter and more
recognizable than `producer` (e.g. `FAO`, `WHO`, `V-Dem`). If `producer` is already
short (`Fouquin and Hugot`, `World Bank`, `NASA`), omit. No year, no period.

**Pairing pattern.** When the institution has a well-known acronym, spell
out the full name as `producer` and put the acronym in `attribution_short`:
`producer: World Health Organization` + `attribution_short: WHO`;
`producer: Food and Agriculture Organization of the United Nations` +
`attribution_short: FAO`. This gives the data-source pane the full name
and the chart byline a compact acronym.

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

**Precision.** When the legacy has both `publication_year: 2022` and
`publication_date: 2022-09-01`, prefer the full date. Same goes for any
explicit month/day mentioned in the legacy description.

### `url_main` and `url_download` (default omitted when no URLs in legacy)
URLs the legacy provides — typically in `source.url`,
`source.source_data_url`, or as inline links in `source.description`.
Copy them verbatim (never invent, swap extensions, or guess domains). If
the legacy field is a prose blob with several URLs, extract each: the
landing-page URL goes to `url_main`, the direct-download URL to
`url_download`. URLs in the legacy must not be silently dropped.

### `date_accessed` (required)
`YYYY-MM-DD`. Plausibility test: any `date_accessed` from **2026-03 or
later** on an older snapshot is likely the legacy-migration tool
stamping the day it ran, not a real access date. Earlier dates — even
ones years after the snapshot's version directory — are real OWID
re-access dates and should be trusted.

Resolution order:
1. Use `source.date_accessed` if plausible by the test above.
2. Else, look for an explicit access date in the legacy description
   prose (`[accessed 18th July 2017]`).
3. Else, fall back to the snapshot's version directory: for `2020/` use
   `2020-01-01`; for `2017-09-27/` use `2017-09-27`. The snapshot file
   was committed on that date, so this is the best lower bound for when
   the data was pulled.

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
- Converting HTML in description prose to Markdown — `<a href="URL">text</a>`
  → `[text](URL)`, `<ul><li>x</li>…</ul>` → `- x` bullet lists, `<br>` → blank
  line. Grapher renders `origin.description` through a markdown-only path
  (`SimpleMarkdownText`), so raw HTML survives as escaped text on the chart's
  Sources tab. The legacy `source.description` flow used a permissive
  `HtmlOrSimpleMarkdownText` renderer, which is why HTML in legacy descriptions
  used to render fine — that crutch is gone for origins.

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

1. **`producer`**: no 4-digit year, no `;`, no `&`, no `OWID` /
   `Our World in Data`, no trailing period (unless ends `et al.`).
2. **`title`** does not end with a period; **`citation_full`** does.
3. **`date_published`** matches `YYYY-MM-DD`, `YYYY`, or `latest`.
4. **DB lengths**: `producer` ≤ 255, `title` ≤ 512, `attribution_short` ≤ 512,
   `version_producer` ≤ 255 chars.
5. **`attribution_short`** ≠ `producer`; no year; no trailing period.
6. **`url_download`** ≠ `url_main`. Both, if present, appear verbatim
   somewhere in the legacy source.
7. **`title_snapshot`** has no 4-digit year and no producer name (unless
   the producer is part of the canonical title, e.g. `Education at a Glance
   2017: OECD Indicators`).
8. **`version_producer`** is not a working-paper ID like `2016-14`.

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
- Authoring indicator-level metadata from scratch — only snapshot DVC
  `meta.origin` and the cleanup of duplicate source info in downstream
  garden/grapher meta.yml + `.py` (see "Downstream sweep" in Workflow
  step 9).
