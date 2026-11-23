# Checking a static chart's texts

For an image about to be exported (a Figma frame from `/owid-staff:create-figma-chart`, a `/create-static-viz` render) or a single chart's own text. The image is frozen once it is exported, so a wrong sentence cannot be corrected in place: every string that says something about the data is a claim to refute against the indicator, its producer and the displayed values.

One agent, a handful of turns: a first check fits in about ten, a re-check of changed strings in four. The rules of [SKILL.md](../SKILL.md) apply (*Run it fast*, proportionality, tolerance). Spend the turns on the strings: a page, a source or a check that no string rests on is out of scope, however reachable.

## 1. Prepare, in the first message

```bash
uv run --no-sync --with duckdb python .claude/skills/fact-check-dataset/scripts/prepare.py --brief <path to check_facts.md>
# or, without a brief:
uv run --no-sync --with duckdb python .claude/skills/fact-check-dataset/scripts/prepare.py --chart <slug or link> --strings <file> [--data <csv>]
```

Send it together with the `ToolSearch` that loads `WebSearch` and `WebFetch`. The dossier holds the chart's own texts, the indicators behind it with their metadata, the steps and `.dvc` origins, the producer's documentation saved as text, the live check of the source files, the corrections on these indicators, each string with the numbers it states matched against the displayed data, and each series' first and last value with the change between them.

An image with no grapher chart behind it (a `viz://static` step that reads a garden dataset): run `prepare.py <ns>/<version>/<short_name> --context claims` instead, and check the strings against that dossier and the data the step plots.

A brief that says **"Changed since the last check"**: check those strings only. Carry the other verdicts over from the previous result, marked as carried over, and say so in the first lines.

## 2. What each text claims

| Text | The claim it makes |
|---|---|
| Title | the headline assertion: that the data shows this |
| Subtitle | what is measured, in what units, over what population |
| Note | the caveats, and that these are the ones the producer states |
| A year or period, wherever it is stated | that it is what the export shows, for every entity |
| Annotations | each number, comparison and superlative, transcribed or derived |
| Direct labels and value labels | that this number belongs to this mark |
| Legend and category labels | that the category contains what the label says it does |
| Axis labels and units | the scale, and whether it is a share, a rate or a count |
| Entity names | that the entity is the one the producer means (aggregates especially) |
| `Data source:` line | the producer, and the year of their release |

A shortened label is a factual edit: check it against the producer's own indicator title. An annotation that was drafted rather than transcribed (a derived percentage, a "more than half") has no upstream provenance: check it against the data.

## 3. The checks

Work out which of these the strings need, then run them in three steps at most:

1. **One message**: every WebSearch the strings need (external facts, the title's independent source), and the Grep calls into `docs/` for the definitions, units and caveats.
2. **One message**: one scratch script for every local check (recompute the derived claims, compare the displayed values with the snapshot), written with the Write tool into the dossier directory and run once; and every WebFetch of the search hits, beside it.
3. **The result.**

Go back for more only when a result contradicts a string.

- **Definitions, units, scope and caveats** (subtitle, note, axis and legend labels): against the indicator's metadata in the dossier and the producer's documentation in `docs/`. Quote the passage. Wording the harmonized FAUST requires or leaves out on purpose is not an omission (`/check-metadata-style` owns that), but when the title is a claim the subtitle must still say what the indicator is, and a note must not be dropped when it carries the unit, the price base or a definition.
- **Numbers the strings state** (labels, annotations, the title): the dossier shows where each one is in the displayed data. Confirm it is the mark the string is attached to (the brief gives the series and the x value). A number marked *not in the displayed data* is derived or external: recompute a derived one from the data, and treat an external one as a fact to verify online.
- **Derived claims** ("tenfold", "more than half", "the highest", "doubled since"): recompute from the displayed data, across every entity the claim covers. A superlative needs the whole comparison set, not the entities on the chart: check it against the full indicator.
- **Years and periods**: the dossier flags a stated year that is outside the data or missing for an entity. A release year in the source line is checked against the origin's `date_published`.
- **The source line and entity names**: the producer and release year against the `.dvc` origin; an aggregate's name against the producer's own definition of it.
- **The displayed values against the source.** Read *Source files, checked live*: a `changed` status means the producer has replaced the file since the snapshot, so say it and check whether the stated values moved. For the values the strings state, confirm them against the raw snapshot or the producer's own published figure, filtered to those entities and years. A value that differs from the source entered in our processing: see [source-errors.md](source-errors.md).
- **The number the title rests on**: one independent source (a different producer measuring the same quantity; never OWID, a mirror or a republisher), within the tolerance in SKILL.md. One search, and at most two pages opened; a second source only when the claim is a superlative or the first disagrees. Compare what the source publishes; don't build an estimate of your own from it (extrapolating a year it lacks, converting units). When nothing in reach measures the same quantity, say so: the claim is still verified against the producer's own data.
- **External facts** (an event and its date in an annotation): one authoritative page you opened, never a search summary. The organization's own announcement is enough; don't add a second for confirmation.
- **Corrections and anomalies**: a correction on these indicators that touches a displayed value is part of the story the image tells; say so. When the garden dataset is built, `scripts/anomaly_scan.py <ns>/<version>/<short_name> --columns <cols> --entities "<names>"` flags an odd value among the displayed ones.

Not part of this context: the indicator ranking, the dataset-wide anomaly scan, and the DoDs (an image has no hover; they are checked when the metadata is).

## 4. The result

Write it where the brief says (`check_facts.result.md` next to the brief). When there is a brief, the first line is `Brief: <id>` exactly as the brief gives it, and the second line is `ok` (no string is wrong or unverifiable) or `findings`. That is all the calling skill reads to record the check; there is nothing to look up about how it does so.

```
Brief: <id>
ok | findings

# Fact check: <frame or chart> (chart `<slug>`, <view>)

(two or three lines: the indicator and its release; which documents and which data the verdicts rest on, with
fetch dates; whether this is a re-check)

## Per string
- **<Text>**, "<the string>": **verified** | **wrong** | **could not be verified** (transcribed | derived | external; carried over)
  - the evidence: the quote and its document, or the values and where they come from
  - when wrong: what the source says, and the corrected text

## Transcribed or derived
(which labels and annotations are transcribed from the data, transcribed from an external source, or derived)

## Findings
(each wrong or unverifiable string with its severity 🔴/🟡/🟢, or "No findings; nothing needs to be held or fixed upstream.")
```

A finding is fixed upstream, where the chart's text lives, through `/edit-faust-metadata`, not only in the frame. Hold the image when the claim is load-bearing.
