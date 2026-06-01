---
name: add-ivs-indicators
description: Add new indicator codes (WVS/EVS question codes like C001, D059, H002_01, Y022) to the existing Integrated Values Surveys (IVS) pipeline WITHOUT bumping the version. Use when the user wants to add IVS/WVS/EVS questions to integrated_values_surveys, extend the IVS dataset with new survey items, or says "add these codes to IVS".
metadata:
  internal: true
---

# Add indicators to Integrated Values Surveys (no version bump)

Adds new WVS/EVS question codes to the existing `integrated_values_surveys` pipeline. The version is
**not** bumped — you extend the current `ivs/<version>` step in place.

## Why this pipeline is unusual

The IVS snapshot is **not** a downloaded file — it's a CSV produced by a **Stata** script
(`snapshots/ivs/<version>/ivs_create_file.do`) that collapses the WVS+EVS microdata
(`Integrated_values_surveys_1981-2022.dta`) into country×year response shares. So adding indicators
means editing the `.do` file, having the **user run it in Stata** (Claude cannot run Stata), then
flowing the new columns through meadow → garden → grapher.

End-to-end flow:

```
edit ivs_create_file.do  →  USER runs it in Stata → regenerates ivs.csv
  → re-snapshot (same version)  →  meadow VARS_DICT  →  garden constants + checks
  →  garden .meta.yml  →  etlr (meadow→garden→grapher)  →  [optional] Notion Available flags
```

Find the active version first: `ls etl/steps/data/garden/ivs/`. All paths below use `<v>` for it
(e.g. `2025-06-27`).

## Reference docs (kept out of git — `*.pdf`/`*.xlsx` are git-ignored)

Two documents make scale/wording verification easy. They normally sit in `snapshots/ivs/<v>/` during an
IVS update but are **git-ignored**, so they're not in the repo. **If they aren't in the snapshot folder,
ask the user for them** (or download from worldvaluessurvey.org → *Data and documentation → Data Download*):

- **Common EVS/WVS dictionary** (`…Common_EVS_WVS_Dictionary_IVS.xlsx`) — maps each IVS code to its WVS-7 /
  EVS variable name (the **Q-number**) and label (sheet `IVS_EVS_and_WVS_Variables`).
  Path: *Data Download → WVS/EVS Trend 1981-2022 → IVS documentation: IVS Common EVS-WVS dictionary*.
- **WVS-7 master questionnaire** (`…WVS-7_Master_Questionnaire_…English.pdf`) — verbatim question stems +
  answer-category wording (look up by Q-number).
  Path: *Data Download → Wave 7 (2017-2022)* (matches the current IVS version) *→ Questionnaire link*.

The 878 MB `.dta` is likewise git-ignored and must be present in `snapshots/ivs/<v>/` locally to
regenerate the CSV.

---

## Step 0 — Verify scales against the `.dta` (NEVER from memory)

Model-recalled survey wording/scales are unreliable — verify. The authoritative code→category mapping
for each variable lives in the `.dta` value labels. Read **every code you're adding in one pass**:

```python
import pandas as pd
path = "snapshots/ivs/<v>/Integrated_values_surveys_1981-2022.dta"
# .dta is row-major: each read_stata() streams the whole 878 MB file once. Read EVERY code you
# need in ONE pass, then inspect each from the in-memory frame — NOT one read_stata() per code
# (that re-streams the 878 MB file per code: ~24 full passes for 24 codes, ~10x slower).
codes = ["C001", "C002", "H002_01"]  # all the codes you're adding
df = pd.read_stata(path, columns=codes, convert_categoricals=True)
for c in codes:
    print(c, list(df[c].cat.categories))   # order == code 1, 2, 3, … (the IVS coding)
```

(`StataReader.value_labels()` mapping is unreliable for these vars — use `convert_categoricals=True`.)

Key gotchas:
- **IVS often collapses the raw WVS scale.** E.g. C001/C002 are 5-point in the WVS-7 questionnaire but
  **3 categories in the IVS `.dta`** (`1 Agree · 2 Disagree · 3 Neither`). Trust the `.dta` categories.
- **Look-alike questions can have different scales.** H002 neighborhood frequency =
  `Very / Quite / Not / Not at all frequently`; H008_02 ("felt unsafe at home") =
  `Often / Sometimes / Rarely / Never` — don't lump them into one block.
- Missing codes follow `.a` Don't know, `.b` No answer, `.c/.d/.e` excluded. Negative codes aren't used.
- Confirm the Q-number + verbatim wording from the **dictionary** + **questionnaire** (see Reference docs).

For the user-facing wording, prefer the fuller **questionnaire** text; for the answer/category names the
`.dta` labels and questionnaire may differ slightly ("some respect" vs "fairly much respect") — they mean
the same; pick per the user's preference.

## Step 1 — Stata: `snapshots/ivs/<v>/ivs_create_file.do`

The `.do` follows one idiom per question group: define a `global` listing codes → a `preserve`-scoped
block (a `foreach` loop for multi-item groups, or a custom block for a single question) that recodes to
0/1 dummies → `collapse (mean) … [w=S017], by(year country)` → `save` a tempfile → later
`merge 1:1 year country` in the "Combine all the saved datasets" section. New codes must also be added
to the master `keep S002VS S002EVS S003 S017 $questions` line.

**Reuse the closest existing structural twin** instead of inventing a block:

| New question shape | Clone this existing block |
|---|---|
| 4-pt with high/low aggregate (`1\|2` vs `3\|4`) + avg_score | `worries` loop (H006) |
| binary 0/1 (`keep if >= 0`) | `neighbors` loop (A124) |
| single 4-pt question | `happiness` block (A008) |
| 3-pt agree/disagree/neither | `political action` loop (E025) |
| 5-pt agree (with neutral) | `work` loop (C039/C041) |
| continuous 0–1 index | custom (see Y022 below) |

**Stata name limits (this WILL bite you):** local-macro / `tempfile` names are capped at **31 chars**
(variable names at 32). `tempfile neighborhood_frequency_\`var'_file` (35) errors with `r(198)`. Keep
tempfile macro names short, e.g. `nbhd_freq_\`var'_file`.

**Continuous indices (e.g. Y022 Welzel equality):** keep the native scale; `keep if Y022 < .`;
`gen avg_score_<name> = Y022`; `collapse (mean) …`; **no DK/NA, no aggregate**. Naming it `avg_score_*`
makes the final `replace \`var' = \`var'*100` step skip it (that step multiplies everything **except**
`avg_score*`).

Each block's generated columns are `{answer}_{CODE}` for loops (meadow renames the `CODE`), or directly
named for single-question custom blocks (e.g. `secure_neighborhood`, `very_secure_neighborhood`).

> The user runs the edited `.do` in Stata to regenerate `ivs.csv`. You can sanity-check by reading the
> `.dta` in pandas and recomputing a couple of weighted means to diff against the produced CSV.

## Step 2 — Re-snapshot (same version, no bump)

After the user regenerates `ivs.csv` (next to the `.dta`):

```bash
.venv/bin/etls ivs/<v>/integrated_values_surveys --path-to-file snapshots/ivs/<v>/ivs.csv
```

This overwrites the `.dvc` md5/size in place (commit that diff). Delete the local `ivs.csv` after.

## Step 3 — Meadow: `etl/steps/data/meadow/ivs/<v>/integrated_values_surveys.py`

Add entries to `VARS_DICT` (`"CODE": "Readable label"`) **only for codes used as column suffixes** —
i.e. the loop groups. The rename works by `column.endswith(code)`, then snake_cases. Custom
single-question blocks already produce final names, so they get **no** `VARS_DICT` entry. Check that no
new code is a suffix of another (e.g. `H002_01` vs `H002_1` — fine; just be deliberate).

## Step 4 — Garden: `etl/steps/data/garden/ivs/<v>/integrated_values_surveys.py`

Per categorical group add:
1. a suffix-list constant (the snake-cased item names),
2. a `replace_dont_know_by_null(...)` call — `answers` = the individual response categories (no aggregate, no DK/NA),
3. a `check_sum_100(...)` call — `answers` = individual categories + `dont_know` + `no_answer`.

`check_sum_100` is the **correctness gate**: it fails loudly if a recode is wrong (categories must sum to 100%).

Derive the **exact** garden column suffixes (so the constants match meadow output) with the same
transform meadow uses:

```python
from owid.catalog.utils import underscore
suffix = underscore("Didn't carry much money".lower().replace(" ", "_"))  # -> "didnt_carry_much_money"
```

(apostrophes are dropped, hyphens → `_`.)

**Continuous index:** no `replace_dont_know`/`check_sum`. Crucially, **exclude it from the
`replace(0, NaN)` step** (0 is a valid index value) — add its column to the difference set there.

## Step 5 — Metadata: `etl/steps/data/garden/ivs/<v>/integrated_values_surveys.meta.yml`

Every new column needs an entry: `title`, single-quoted `description_short` (quote the question stem +
answer options, double internal apostrophes for YAML), and `display.name` + `<<: *common-display`. Source
the wording from the questionnaire PDF; the code→category order from the `.dta`. Index/unit-less columns
override `unit: ""` / `short_unit: ""` (and e.g. `numDecimalPlaces: 2`). Generate the entries
programmatically (it's 100+ columns) and append to the `variables:` block.

**Audit gotcha:** verify any stated scale range against the actual `.do` recode — a pre-existing entry had
`"6 to 10"` where the recode was `>= 7`. Don't copy ranges blindly.

## Step 6 — Pre-flight cross-check (before running the pipeline)

Assert the set of new column names is **identical** across three places, or you get silent breakage:
(a) what Stata+meadow produce, (b) the garden `check_sum_100`/`replace_dont_know` groups, (c) the
`.meta.yml` keys. Script it:

```python
from etl.files import ruamel_load
meta = ruamel_load(open("etl/steps/data/garden/ivs/<v>/integrated_values_surveys.meta.yml").read())
metakeys = set(meta["tables"]["integrated_values_surveys"]["variables"])
expected = set()  # build from your group constants × answer prefixes, same logic as garden
print("missing in meta:", sorted(expected - metakeys) or "none ✓")
```

## Step 7 — Run & verify

```bash
.venv/bin/etlr integrated_values_surveys --private             # meadow→garden→grapher
.venv/bin/etlr integrated_values_surveys --grapher --private   # upload to staging grapher
```

- `check_sum_100` is the gate — green run = recodes sum to 100%.
- Spot-check ranges: share columns 0–100; continuous index on its native scale (e.g. 0–1).
- `make check` before committing.

## Step 8 — Git (per repo CLAUDE.md)

```bash
.venv/bin/etl pr "Add … indicators to Integrated Values Surveys" data   # no emoji in title with a category
git add … && git commit -m "✨🤖 …"   # 📊🤖 for the snapshot/.dvc regen
git push
```

Post `@codex review` as a **separate** PR comment; keep the PR body in sync with substantial pushes.

## Step 9 — Notion `Available` dictionary (optional follow-up)

The **Integrated Values Surveys dictionary** Notion DB (`collection://fd3a1354-9da6-48e7-91bb-a0665ddd0f0b`)
has an `Available` checkbox per code. Flip `Available = ✅` **only for codes that survive into the garden
dataset** = the `.do` keep list **minus** items excluded in `drop_indicators_and_replace_nans`. The
reliable way to get that set is to read the garden table columns and reverse-map to base codes — a `.do`-only
list over-counts (e.g. `ds E069*` keeps `E069_64`/"Elections" which produces no column, and ~8 confidence
items are dropped in garden). Beware substring false-positives when reverse-matching ("elections" matches
`..._free_elections`); verify the exact column exists before flipping. The Notion MCP has **no bulk row
query** — fetch/update per row (`notion-search` returns titles only; `notion-fetch` returns `Available`;
`notion-update-page` with `{"Available":"__YES__"}`). The row's `Answers` field is a handy independent
cross-check of your recode mapping.

## Worked example

A full reference implementation (20 codes across women's rights / ethnic groups / human rights / crime,
including a 3-pt agree group, a 4-pt agree group, two reused frequency/binary loops, two single-question
4-pt blocks, and the Y022 continuous index) is PR **owid/etl#6180** — read its diff for concrete patterns.
