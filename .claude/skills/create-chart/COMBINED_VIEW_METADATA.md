# Data-page metadata for a view that combines different indicators

Reference for the "Data page of a view that combines different indicators" section of
[`SKILL.md`](SKILL.md).

## What the data page reads

In owid-grapher, `baker/MultiDimBaker.tsx` builds each view's data page from one merged metadata object:
the variable metadata of `view.indicators.y[0]`, then the multidim's `config.metadata`, then the view's
`metadata` (`MultiDimDataPageConfig.mergeViewMetadata`). Nested objects merge field by field and lists are
replaced whole, so any field the view leaves out still comes from the first y indicator.
`site/multiDim/README.md` states the rule: the first y indicator is the "main" one, and its metadata renders
the data page outside the chart area.

`site/dataPage.ts` (`getDatapageDataV2`) renders these fields from the merged object:

- `presentation.title_public`, the data-page title, together with `title_variant` and `attribution_short`;
- `description_short`, `description_key`, `description_processing` and `description_from_producer`;
- the unit (`display.unit` wins over `unit`);
- `origins`, the "Sources and processing" section;
- the time span.

The other indicators in the view contribute none of these. Without a view-level override, a reader of a
gross vs. net view sees only the gross rate's title and key information.

## When a view needs it

- **Needs it:** any view whose y (and x, for a scatter) are different indicators: a scatter, a dumbbell or
  line chart comparing two different measures, a stacked bar of several components.
- **Doesn't need it:** a view comparing one indicator across a dimension, such as men vs. women, when the
  indicator's text doesn't name the split value. If its text says "men", the women's line inherits that
  sentence too, so the view needs an override after all.

## Writing the text

- **Reuse the indicators' own sentences.** Build `description_short` and `description_key` from the
  `description_short` and `description_key` text of the indicators in the view. Trim repetition, but don't
  add claims that neither indicator's text makes.
- **Say which indicator each paragraph is about,** for example by opening it with the indicator's name in
  bold.
- **Title the whole view** in `presentation.title_public`, naming every measure it shows.
- **Check the rest of the page:** `description_processing`, `description_from_producer`, the unit and the
  origins. Override any whose first-indicator value is wrong or incomplete for the view.
- **Match the form of the indicators' text.** `description_key` can be a `|-` markdown string, which renders
  as paragraphs, or a YAML list, which renders as bullets.

## Where it goes

Under the view's `metadata:` key, next to `config:`. The `config:` block takes chart settings only and
rejects metadata fields (see Troubleshooting in `SKILL.md`).

```yaml
views:
  - dimensions:
      measure: a_vs_b
    indicators:
      y:
        - catalogPath: tb#indicator_a
        - catalogPath: tb#indicator_b
    config:
      chartTypes: ["Dumbbell"]
    # Copied from the garden .meta.yml text of indicator_a and indicator_b.
    # Keep in sync: when their text changes, update this block too.
    metadata:
      presentation:
        title_public: Indicator A and indicator B
      description_short: <one sentence covering both, built from their description_short>.
      description_key: |-
        **Indicator A:** <sentence from indicator_a's description_key>.

        **Indicator B:** <sentence from indicator_b's description_key>.
```

## Keeping it in sync

A chart config can't reference the garden `.meta.yml` definitions, so the view's text is a copy. Leave a
comment above the block that names the indicators it copies and asks to keep it in sync, as in the example.
Whoever edits those indicators' public text then updates the combined views that copy it
(`edit-faust-metadata` covers this in its blast-radius step).

## Checking it

Open the view's data page on staging (the multidim URL with the view's dimension query string) and read the
title, the key information and the "Sources and processing" section as a reader of that view.
