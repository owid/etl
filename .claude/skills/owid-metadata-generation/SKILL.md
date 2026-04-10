---
name: owid-metadata-generation
description: Use when creating or enriching metadata for OWID ETL datasets - generates comprehensive YAML metadata from dataset inspection, data exploration, and web research following OWID metadata standards. Trigger when writing or editing *.meta.yml files, when a garden step has empty or minimal metadata, or when user asks to improve/add/enrich metadata.
---

# OWID Metadata Generation

Practical guidelines for writing high-quality metadata in `*.meta.yml` files.

**Core principle:** Metadata is for the public. Every field should help someone understand the data. Write in plain language -- if a layperson can't understand it, rewrite it.

## When to Use

- New garden/grapher datasets needing metadata
- Enriching incomplete or sparse metadata files
- Updating existing metadata for dataset refreshes

**Don't use for:** Snapshot metadata (different process) or quick single-field edits.

## Canonical YAML Structure

```yaml
definitions:
  common:
    processing_level: minor  # or major
    presentation:
      topic_tags:
        - <Topic>
      attribution_short: <Short Source Name>
    display:
      numDecimalPlaces: 1
  # Reusable text blocks
  my_note: &my_note |-
    Reusable text.

dataset:
  update_period_days: 365  # Must be accurate: 0 for datasets that will never update

tables:
  <table_name>:
    variables:
      <variable_name>:
        title: <Human-readable title>
        unit: <Full unit name>
        short_unit: <Symbol>
        description_short: |-
          <1-2 sentences>
        description_key:
          - <Bullet point>
        description_from_producer: |-
          <Original text from source>
        description_processing: |-
          <What OWID did to process this>
        display:
          name: <Legend label>
        presentation:
          title_public: <Public-facing title>
          title_variant: <Disambiguating label>
```

**Key rule from CLAUDE.md:** Omit the `dataset:` block entirely (inherited from origin) -- only set `update_period_days` when needed.

## Field-by-Field Guidelines

### Titles

| Field | Purpose | Length |
|-------|---------|--------|
| `title` | Primary identifier, always required | ~100 chars max |
| `presentation.title_public` | Human-readable public title | Must be excellent |
| `presentation.title_variant` | Disambiguator ("Historical data", "WHO") | Short phrase |
| `display.name` | Chart legend label | ~30 chars max |

**Rules:**
- Sentence-case, no trailing period
- Never include producer, year, or units in titles
- When `display.name` is set, also set `title_public` (see title hierarchy in `docs/architecture/metadata/faqs.md`)
- **Prefer NOT setting `title_public`** unless the `title` has dimension breakdowns or codes (e.g. SDG indicator numbers). The data page will show the curated chart title instead, which is usually better. Use `display.name` for cleaner legend/table labels.
- Prefer clear, reader-friendly titles over technically precise ones (e.g. "Share of people who think vaccines are safe" over "Share who strongly agree that...")
- `title_variant` disambiguates when multiple indicators share a similar title — use short phrases like "Historical data", "WHO", "Age-standardized", "Extrapolated". Watch for redundancy with `attribution_short` (avoid "V-Dem - V-Dem" duplication).

```yaml
# GOOD
title: Number of neutron star mergers in the Milky Way
display:
  name: Neutron star mergers
# BAD
title: Number of neutron star mergers (NASA, 2023)
```

### Units

| Field | Format | Examples |
|-------|--------|----------|
| `unit` | Lowercase, plural, "per" not "/" | `tonnes per hectare`, `%`, `""` |
| `short_unit` | SI abbreviation | `t/ha`, `%`, `""` |

- Always set `unit` explicitly, even to `""` for dimensionless indicators (scores, indexes)
- `short_unit` is only needed when there's an actual unit to abbreviate. Omit it for dimensionless indicators -- it defaults to `None` and grapher won't show a unit label.
- Use "person" not "capita" (`kilowatts per person`)
- Choose human-friendly scales -- if most values are below 1 tonne per person, use "kilograms per person" instead
- `short_unit` should use SI abbreviations (`g` not `grams`, `%` not `pct`)

**Decimal precision:** 0 for counts, 1 for percentages, 2 for economic/per-capita values. Be consistent across related variables. Always set `numDecimalPlaces` explicitly -- it's a frequent source of review feedback.

### Descriptions

**`description_short`** -- 1-2 sentences, ~200 characters ideally. Answers "What does this number measure?" Longer explanations belong in `description_key`.
- Use `|-` block scalar for multi-line; inline strings are fine for single sentences
- Don't mention units, sources, or processing (redundant or belongs elsewhere)
- Don't just repeat the title -- if it adds nothing beyond the title, omit it entirely
- Remove filler phrases like "Emissions are..." at the start
- Supports Markdown: `[text](url)` for links, `[term](#dod:term)` for OWID definition popups (e.g. `[stunted](#dod:stunting)`)

```yaml
# GOOD
description_short: |-
  The number of people living in extreme poverty, defined as living on less than $2.15 per day.
# BAD - repeats the title
description_short: |-
  Manufactured cigarettes sold in this country in this year.
# BAD - mentions sources (belongs in description_key)
description_short: |-
  The number of people living in extreme poverty, based on data and estimates from different sources.
```

**`description_key`** -- Array of self-contained bullet points for "About this data" panel.
- Plain language, no jargon (e.g. "livestock digestive processes" not "enteric fermentation"). Expand acronyms on first use.
- **Add concrete examples** for abstract scope descriptions (what countries are included, what events qualify)
- **Order**: data-specific points first, methodology second, caveats last
- Only describe data that actually exists in the indicator. State implications of limitations explicitly.

```yaml
# GOOD
description_key:
  - Extreme poverty is measured using the International Poverty Line of $2.15 per day in 2017 international dollars.
  - This metric uses household survey data adjusted for purchasing power parity (PPP).
# BAD - unexpanded acronyms, jargon
description_key:
  - Uses IPL of $2.15/day (2017 PPP).
```

**`description_from_producer`** -- Exact producer text, verbatim or minimally edited. Only if producer provides clear definitions. Can be set in `definitions.common` when the same producer description applies to all variables, with per-variable overrides as needed.

**`description_processing`** -- What OWID did. Only for major transformations (aggregations, per-capita calculations, combining sources). Don't document routine operations (country harmonization, dropping nulls).
- Document dropped data and aggregation choices. Keep in sync with code.
- Don't reference internal dataset names — describe what was done, not which datasets were used.
- Low-visibility field — surface important caveats in `description_short` or `description_key` too.

### Processing Level and License

- `minor`: data largely unchanged (reformatting, unit conversion, harmonization) -> use most restrictive origin license
- `major`: significant transformations (calculations, combinations, imputations) -> use CC BY 4.0
- The test: is the data identical to the original? If not (e.g. processing of historical regions, per-capita calculations), it's `major`.

Set in `definitions.common`, override per-variable as needed.

### Topic Tags

- 1-3 tags, most relevant first. First tag = primary (used in citations).
- Must match valid tags from the schema. To list them:
  ```bash
  .venv/bin/python -c "import json; tags=json.load(open('schemas/dataset-schema.json'))['properties']['tables']['additionalProperties']['properties']['variables']['additionalProperties']['properties']['presentation']['properties']['topic_tags']['items']['enum']; print('\n'.join(tags))"
  ```
- Set in `definitions.common.presentation.topic_tags`.

### Presentation & Display

- `presentation.attribution_short`: Short source name ("WHO", "World Bank"). Set in `common` when uniform.
- `presentation.grapher_config`: Only set when you want a specific default chart view. Common sub-fields:
  - `note`: Chart footnotes — methodology caveats, sample sizes, inflation adjustments. Keep to 1-2 sentences.
  - `selectedEntityNames`: Pre-select countries/regions for the default view (e.g. `["United States", "China", "Europe"]`)
  - `selectedEntityColors`: Map entity names to hex colors (e.g. `{"Africa": "#A2559C", "Asia": "#00847E"}`)
  - `map`: Map tab settings — `colorScale` with `baseColorScheme` (e.g. `"YlOrRd"`), `binningStrategy` (`"manual"`), and `customNumericValues` for bin thresholds
  - Set at variable level, or in `definitions.common.presentation` when all variables share the same chart defaults
- `display.numDecimalPlaces`: Set explicitly. Use `metadata-export --decimals auto` to auto-detect.
- `display.tolerance`: Number of years to allow gap-bridging on line charts (default 0). Set higher (e.g. 5-10) for sparse historical data where connecting distant points is acceptable.
- `display.roundingMode`: Use `"significantFigures"` with `numSignificantFigures` instead of `numDecimalPlaces` when values span many orders of magnitude.

## YAML Efficiency Patterns

**Use `definitions.common`** when 3+ variables share the same field values. Remember: `common` does NOT merge -- it completely overrides. Use `<<: *anchor` for partial overrides.

**Use anchors/aliases** for identical blocks shared by 2+ variables. Define in `definitions:` at the top. **Name anchors to indicate their target field** (e.g. `description_producer_refugee` not `description_refugee`) so reviewers can tell which metadata field the text will end up in.

**Use `<<: *anchor` merge** to extend a shared mapping while overriding specific keys (e.g. `<<: *common_display` then `numDecimalPlaces: 0`).

**Use Jinja templates** for dimensional datasets (age, sex, cause breakdowns). Custom delimiters: `<% %>` for blocks, `<< >>` for expressions.

```yaml
# Jinja example: dimensional variable with conditional descriptions
variables:
  time_spent:
    title: Time spent with <<who_category>> throughout life
    unit: hours per day
    short_unit: h
    description_short: |-
      <% if who_category == "Alone" %>
      Time spent alone, by gender and age.
      <% else %>
      Time spent with <<who_category.lower()>>, by gender and age.
      <%- endif -%>
    display:
      name: With <<who_category>>
```

**Use `{definitions.xxx}` string interpolation** for reusing text fragments inline (e.g. `'{definitions.methodology}'` in a `description_key` bullet). Unlike YAML anchors which substitute entire nodes, this inserts text within strings. Use anchors for whole fields/blocks, interpolation for composing text.

**Use `shared.meta.yml`** when multiple `.meta.yml` files in the same directory share definitions or macros. These files contain only Jinja macros and reusable definitions — no actual variable metadata. Step-level `.meta.yml` files then import and call these macros. Used in large multi-file datasets like IHME GBD.

For full syntax details, see `docs/architecture/metadata/structuring-yaml.md`.

## Common Variable Patterns

- **Per-capita variables**: `processing_level: major`, document the calculation in `description_processing`, use `international-$ per person` not "per capita"
- **Age-standardized variables**: Use `presentation.title_variant: Age-standardized`, explain standardization method in `description_key`
- **Absolute + share pairs**: Keep consistent naming; the share variable gets `unit: "%"`, absolute gets the count unit
- **Survey response breakdowns**: When variables represent response options (e.g. `very_worried`, `not_worried_at_all`), put all shared metadata in `definitions.common` (question text, methodology, unit, display) and give individual variables only a `title`. This avoids repetition and keeps the file compact.

## Workflow

1. **Generate skeleton**: `.venv/bin/etl metadata-export data/garden/<ns>/<ver>/<ds> --output /tmp/<ds>.meta.yml` (never run without `--show` or `--output` to avoid overwriting)
2. **Inspect the data**: Load dataset, check columns, value ranges, existing origin metadata
3. **Research sources**: Read snapshot `.dvc` for origin URLs, visit source docs for methodology and definitions
4. **Write top-down**: Start with `definitions:` and `common:` to identify shared patterns before individual variables
5. **Fill systematically**: For each variable: title -> units -> description_short -> description_key -> processing_level -> display
6. **Preview with INSTANT mode**: `INSTANT=1 .venv/bin/etlr data://grapher/<ns>/<ver>/<ds> --grapher --only`
7. **Check typos**: Run the `check-metadata-typos` skill on the step path

## Quality Checklist

Items that are easy to miss (obvious rules like "set title" are omitted — see field guidelines above):

- [ ] `description_short` adds value beyond the title — if it just repeats the title, delete it
- [ ] `description_key` includes concrete examples where scope is abstract
- [ ] `description_key` only describes data that actually exists in the indicator
- [ ] `description_processing` matches current code and doesn't reference internal dataset names
- [ ] Important caveats surfaced in `description_short` or `description_key`, not buried in `description_processing`
- [ ] `numDecimalPlaces` set and consistent across related variables
- [ ] `display.name` paired with `title_public` when set
- [ ] No TODO/FIXME, no copy-paste errors, no producer names/years in titles
- [ ] Repeated text uses anchors/aliases or `{definitions.xxx}`; Jinja for 10+ similar variables
- [ ] Typo check passed
