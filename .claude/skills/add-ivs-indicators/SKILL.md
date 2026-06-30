---
name: add-ivs-indicators
description: Add new survey question codes (e.g. C001, D059, H002_01, Y022, E268, G055) to OWID's values-survey pipeline WITHOUT bumping the version — either the Integrated Values Surveys table (integrated_values_surveys, WVS+EVS merged) or the World Values Survey table (world_values_survey, WVS-only questions). Both tables live in the same ivs/<version> garden+grapher dataset. Use when the user wants to add IVS/WVS/EVS questions, extend integrated_values_surveys or world_values_survey, or says "add these codes to IVS" / "add these WVS questions".
metadata:
  internal: true
---

# Add indicators to the values-survey pipeline (IVS / WVS, no version bump)

Adds new question codes to OWID's values-survey pipeline **without bumping the version** — you extend the
current `ivs/<version>` step in place. The dataset holds **two tables**, and you pick which one you're
adding to:

- **`integrated_values_surveys`** — the IVS (WVS **+** EVS merged trend). Source: the IVS `.dta`.
- **`world_values_survey`** — WVS-only questions (asked in WVS but absent from IVS). Source: the WVS
  Time-Series `.dta`.

Both tables live in the **same** `ivs/<v>` garden + grapher dataset, so variable **titles must be unique
across both tables** (Step 5/6). The two paths differ in only a handful of places; everything else in this
skill is identical. **Pick the target first**, then read each step with the right column:

| Aspect | IVS → `integrated_values_surveys` | WVS → `world_values_survey` |
|---|---|---|
| Source `.dta` | `Integrated_values_surveys_1981-2022.dta` (`snapshots/ivs/<v>/`) | `WVS_Time_Series_1981-2022_stata_v5_0.dta` (`snapshots/wvs/<wv>/`) |
| Stata script | `snapshots/ivs/<v>/ivs_create_file.do` | `snapshots/wvs/<wv>/wvs_create_file.do` |
| Snapshot / meadow | `ivs/<v>/integrated_values_surveys` | `wvs/<wv>/world_values_survey` |
| Garden code | `drop_indicators_and_replace_nans` + `sanity_checks` | `process_wvs` + `sanity_checks_wvs` (same garden file) |
| **Missing codes** | extended-missing `.a/.b/.c/.d/.e` | **negative**: `-1` DK, `-2` NA, `-3` N/A, `-4` not asked, `-5` missing |
| **DK / NA** | `.a` / `.b` | `-1` / `-2` |
| **Keep rule** (Step 1) | `keep if var>=1` then drop `.c/.d/.e` | `keep if var>=-2 & var<.` (drops -5/-4/-3 + sysmiss) |
| **avg_score** | `gen avg_score=var` (ext-missing auto-excluded) | `gen avg_score=var if var>=0` (exclude negative DK/NA) |
| zero→null in garden | yes (IVS has spurious zeros) | **no** (WVS has none; absent country-years are NaN after merge) |
| Reference docs | IVS Common EVS/WVS dictionary | WVS Time-Series variable list (`F00003844…xlsx`) |
| Notion `Available` dict (Step 9) | yes | **n/a** — skip (WVS has no Notion tracker) |

Find the active versions: `ls etl/steps/data/garden/ivs/` (shared garden/grapher dataset → `<v>`) and
`ls snapshots/wvs/` (WVS snapshot/meadow → `<wv>`).

## Why this pipeline is unusual

Neither snapshot is a downloaded file — each is a CSV produced by a **Stata** script (`*_create_file.do`)
that collapses survey microdata into country×year response shares (we publish only the aggregated shares,
not the microdata — this also respects the WVS "no redistribution" license). So adding indicators means
editing the `.do`, having the **user run it in Stata** (Claude cannot run Stata), then flowing the new
columns through meadow → garden → grapher.

End-to-end flow (same for both targets):

```
edit <ivs|wvs>_create_file.do  →  USER runs it in Stata → regenerates the csv
  → re-snapshot (same version)  →  meadow VARS_DICT  →  garden constants + checks
  →  garden .meta.yml  →  etlr (meadow→garden→grapher)  →  [IVS only: Notion Available flags]
```

Paths below use `<v>` for the shared garden/grapher version (e.g. `2025-06-27`) and `<wv>` for the WVS
snapshot/meadow version (e.g. `2026-06-30`).

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
- **EVS 2017 field questionnaire** (`ZA7500_q_gb.pdf`, the Great Britain master) — the **fallback** for
  older EVS-only items that never made it into WVS-7 (so they're absent from the WVS-7 PDF). Look up by
  the EVS variable name / question number (e.g. the IVS `E158` "concern about humankind" = EVS **Q60,
  item v216** "the living conditions of all humans all over the world"). Source:
  europeanvaluesstudy.eu → *Methodology, data, documentation → Survey 2017 → full release EVS2017 →
  participating countries → questionnaires*. Ask the user for it if it's not in the snapshot folder.

To find which questionnaire a code lives in, check the dictionary's WVS-7 vs EVS columns: if the **WVS-7
variable name is blank** for that IVS code, it's EVS-only — go to the EVS questionnaire.

The 878 MB `.dta` is likewise git-ignored and must be present in `snapshots/ivs/<v>/` locally to
regenerate the CSV.

---

## Step 0 — Verify scales against the `.dta` (NEVER from memory)

Model-recalled survey wording/scales are unreliable — verify. The authoritative code→category mapping
for each variable lives in the `.dta` value labels. Read **every code you're adding in one pass**:

```python
import pandas as pd
# IVS: snapshots/ivs/<v>/Integrated_values_surveys_1981-2022.dta   → convert_categoricals=True
# WVS: snapshots/wvs/<wv>/WVS_Time_Series_1981-2022_stata_v5_0.dta → convert_categoricals=False, then
#      inspect raw codes: WVS codes missing as NEGATIVES (-1 DK, -2 NA, -4 not asked, -5 missing); the
#      positive codes (and 0 where it's a real category, e.g. immigration G05x) are the substantive answers.
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
- **Missing codes differ by survey.** IVS: `.a` Don't know, `.b` No answer, `.c/.d/.e` excluded (negatives unused). WVS: `-1` Don't know, `-2` No answer, `-3` not applicable, `-4` not asked, `-5` missing.
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

> **WVS target?** Edit `snapshots/wvs/<wv>/wvs_create_file.do` instead — same idiom, but the master keep is
> `keep S002VS S003 S017 $questions` (no `S002EVS`) and the missing-value handling differs: per question use
> `keep if \`var' >= -2 & \`var' < .` (drops -5/-4/-3 and system-missing; keeps substantive + DK + NA), then
> `dont_know_<q> = (\`var' == -1)`, `no_answer_<q> = (\`var' == -2)`, substantive dummies on the positive
> codes, and for avg_score questions `gen avg_score_<q> = \`var' if \`var' >= 0` (so negative DK/NA are
> excluded from the mean). The structural-twin table below still applies — only the keep / DK / NA lines change.

**Reuse the closest existing structural twin** instead of inventing a block:

| New question shape | Clone this existing block |
|---|---|
| 4-pt with high/low aggregate (`1\|2` vs `3\|4`) + avg_score | `worries` loop (H006) |
| binary 0/1 (`keep if >= 0`) | `neighbors` loop (A124) |
| single 4-pt question | `happiness` block (A008) |
| 3-pt agree/disagree/neither | `political action` loop (E025) |
| 5-pt agree (with neutral) | `work` loop (C039/C041) |
| **5-pt frequency** (Daily/Weekly/Monthly/Less than monthly/Never) | extend the `worries` 4-pt loop to 5 levels (no exact twin) |
| **10-pt agree / better-worse** | `income_equality` block (E035): aggregates `>=7` / `5\|6` neutral / `<=4`, **+ `avg_score` native 1–10** |
| **multinomial 1-of-N named choice** (respondent picks one option) | `environment_vs_econ` block (B008): one 0/1 dummy per option, **no aggregate, no avg_score** |
| continuous 0–1 index | custom (see Y022 below) |

For a **10-pt** block the three aggregates (`>=7`, `5\|6`, `<=4`) + `dont_know` + `no_answer` partition
to 100% — that's what `check_sum_100` checks (`avg_score` is extra). For **multinomial**, the N option
dummies + `dont_know` + `no_answer` sum to 100%. When several questions share the **same** option codes
(e.g. E001/E002 "aims of country: 1st/2nd choice" both map 1–4 to the same four goals), put them in one
loop.

**Aggregate-name collisions:** an aggregate must not collide with a category name. The closeness 4-pt
scale has a category `close` (code 2), so the high aggregate (`1|2`) must be named `feel_close`, not
`close`. Similarly check any `not_*` aggregate vs a `not_*` category before naming.

**Aggregates and `avg_score` are NOT part of the `check_sum_100` partition** — only the mutually-exclusive
categories + `dont_know` + `no_answer` sum to 100. Aggregates (`worry_`, `feel_close_`, `agree_`,
`at_least_weekly_`…) are derived extras layered on top.

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

After the user regenerates `ivs.csv` (next to the `.dta`), **first confirm the Stata run actually emitted
your new columns** before snapshotting — cheap insurance against a botched/partial `.do` run:

```python
import pandas as pd
cols = set(pd.read_csv("snapshots/ivs/<v>/ivs.csv", nrows=0).columns)
# pre-rename raw names, i.e. {prefix}_{CODE} for loops + literal names for custom blocks
assert {"at_least_weekly_E248B", "agree_E217", "better_off_science_world", "concerned_humankind"} <= cols
```

Then re-snapshot:

```bash
.venv/bin/etls ivs/<v>/integrated_values_surveys --path-to-file snapshots/ivs/<v>/ivs.csv
```

This overwrites the `.dvc` md5/size in place (commit that diff with a `📊🤖` message). Delete the local
`ivs.csv` after.

## Step 3 — Meadow: `etl/steps/data/meadow/ivs/<v>/integrated_values_surveys.py`

Add entries to `VARS_DICT` (`"CODE": "Readable label"`) **only for codes used as column suffixes** —
i.e. the loop groups. The rename works by `column.endswith(code)`, then snake_cases. Custom
single-question blocks already produce final names, so they get **no** `VARS_DICT` entry. Check that no
new code is a suffix of another (e.g. `H002_01` vs `H002_1` — fine; just be deliberate).

**Labels: avoid `:` and other punctuation.** `rename_vars` snake_cases in two steps — first a crude
`.str.lower().str.replace(" ", "_")`, then `tb.format()` applies the real `underscore()`. A label like
`"Information source: Daily newspaper"` ends up as `information_source__daily_newspaper` (the colon
becomes a **second** underscore). Use colon-free labels (`"Information source daily newspaper"`) so the
final suffix is clean single-underscore (`information_source_daily_newspaper`), and put the nicely
punctuated wording in the `.meta.yml` `title`/`description_short` instead. **Codes with trailing letters**
(e.g. `E248B`, `G007_18_B`) work fine with `endswith`.

## Step 4 — Garden: `etl/steps/data/garden/ivs/<v>/integrated_values_surveys.py`

Per categorical group add:
1. a suffix-list constant (the snake-cased item names),
2. a `replace_dont_know_by_null(...)` call — `answers` = the individual response categories (no aggregate, no DK/NA),
3. a `check_sum_100(...)` call — `answers` = individual categories + `dont_know` + `no_answer`.

`check_sum_100` is the **correctness gate**: it fails loudly if a recode is wrong (categories must sum to 100%).

Derive the **exact** garden column suffixes (so the constants match meadow output) with the same
transform meadow uses:

```python
from owid.catalog.core.utils import underscore   # owid.catalog.utils path is deprecated
suffix = underscore("Information source daily newspaper")  # -> "information_source_daily_newspaper"
```

(apostrophes are dropped, hyphens → `_`; a colon would leave a double `__`, so keep labels colon-free.)

**`avg_score` and the `replace(0, NaN)` step.** Only indices where **0 is a genuine value** need to be
excluded from the 0→null replacement (`WELZEL_EQUALITY_INDEX_COLUMNS`, the 0–1 Welzel index). An ordinary
`avg_score_*` on a 1–N scale (frequency 1–5, closeness 1–4, agree 1–10, …) **never equals 0**, so the
replacement is a no-op for it — do **not** add those to the exclusion set. A truly continuous block also
gets **no** `replace_dont_know`/`check_sum`.

## Step 5 — Metadata: `etl/steps/data/garden/ivs/<v>/integrated_values_surveys.meta.yml`

Every new column needs an entry: `title`, single-quoted `description_short` (quote the question stem +
answer options, double internal apostrophes for YAML), and `display.name` + `<<: *common-display`. Source
the wording from the questionnaire PDF; the code→category order from the `.dta`. Index/unit-less columns
(the `avg_score_*`) override `unit: ""` / `short_unit: ""` and `numDecimalPlaces: 2`; share columns inherit
`unit: "%"` from `&common-display`. Generate the entries programmatically (it's 100s of columns) and append
to the `variables:` block. To re-run the generator after a wording fix, splice cleanly: find the first new
key in the file, truncate from there, and re-append the regenerated block (don't blindly append twice).

**Battery questions — separate the prompt from the item.** Many WVS/EVS questions read a generic prompt
then list items (e.g. "…with the following statements? - Work is a duty…"). In `description_short` never
leave a raw `" - "` separator **and** never let the item dangle straight after the "?" (e.g.
`…statements? Work is a duty…`) — both read as two sentences mashed together. End the prompt's quote and
re-open a quote for the item, picking the connector by what the item *is*:

| Case | Pattern | Examples |
|---|---|---|
| **plural** "…the following **statements**?" (or stem ending on a scale phrase) | `…?", when the statement was "<statement>"` | gender-roles, **work**, justifiable |
| **singular** "…with the following **statement**?" | **no connector** — drop the dash, item follows directly: `…statement? <statement>` | jobs-scarce (C001/C002) |
| non-statement items (situations, actions, "things") | `…?", regarding "<item>"` — or a fitting noun where one reads cleanly | neighborhood frequency, security actions; **worries** already uses `…situations?", when the situation was "…"` |

Rationale: the deciding factor for statements is **singular vs plural** — a singular "the following
statement?" already names its one statement so a connector is redundant (just drop the dash), while a
plural battery needs the connector to say which item. The connector **noun must fit the item**: "when the
**thing** was" reads badly, so use **"regarding"** (or a clean noun like "situation"/"characteristic").
Always use the comma form `", when the … was "`, matching the pre-existing justifiable battery.

**YAML quoting in a Python generator:** keep RAW apostrophes in your strings and run them through one
single-quote helper that doubles `'`→`''`. Do **not** hand-write `'Don''t know'` in Python source — that's
adjacent-string-literal concatenation and silently yields `Dont know`. After appending, re-parse the file
with `ruamel_load` and assert the count + that a new `avg_score_*` entry shows `unit == ""` and the
`&common-display` anchor resolved (e.g. `numDecimalPlaces`).

**`title` must be UNIQUE across the whole dataset — and the dataset holds BOTH tables.** Because
`integrated_values_surveys` and `world_values_survey` share one grapher dataset, a WVS title must not clash
with any IVS title (or vice-versa). WVS shares many concepts with IVS (worries, confidence, justifiable,
…), so when you add a WVS question that duplicates an IVS concept, **suffix the WVS title** (e.g.
`… (World Values Survey)`) to keep it unique; `display.name` may still repeat. Grapher enforces this at the
`--grapher` upload (`_adapt_table_for_grapher`), *not* at the garden build, so a clash sails through
`check_sum_100` and only explodes at upload (`AssertionError: Variable titles are not unique`). The trap: an
**aggregate** and a **category** that describe the same thing collide on title even when their column names
differ. Real
example from this work — the closeness category `close_*` and the high aggregate `feel_close_*` both wanted
`title: "Feel close to X"`. Fix: disambiguate the aggregate `title` (`"Feel close to X (very close or
close)"`) and keep the short label in `display.name` (only `title` must be unique — `display.name` may
repeat). **Assert title uniqueness in the pre-flight** (Step 6), don't wait for the upload to catch it:
`titles=[e["title"] for e in vars.values()]; assert len(titles)==len(set(titles))`.

**Audit gotcha:** verify any stated scale range against the actual `.do` recode — a pre-existing entry had
`"6 to 10"` where the recode was `>= 7`. Don't copy ranges blindly.

### Topic tags — assign per indicator, from the curated enum

`presentation.topic_tags` is **not** free text. Valid values are the enum in
[`schemas/dataset-schema.json`](schemas/dataset-schema.json) (the ~128 OWID topic **pages** — e.g.
`Women's Rights`, `Human Rights`, `Migration`, `Democracy`, `Religion`, `Internet`, `Technological Change`,
`Loneliness & Social Connections`, `War & Peace`, `Terrorism`, `Corruption`, `Marriages & Divorces`,
`LGBT+ Rights`, `Happiness & Life Satisfaction`, `Economic Inequality`, `Economic Growth`, `Climate Change`,
`Time Use`, `Working Hours`). Tags that exist in the grapher `tags` table but have **no topic page**
(`Values`, `Crime`, `News`, `Communication Technology`) are **invalid** — they're silently dropped at upload
with a `create_links.missing_tags` warning. Check every tag against the enum before writing it (see the
Step 6 assertion).

IVS spans many topics, so **do not** leave the whole dataset on one tag (the legacy default was a single
`topic_tags: [Trust]` in `definitions.common`). Tag per indicator:

- **Anchor per distinct tag-set under `definitions`** (the `undp_hdr.meta.yml` idiom), referenced per
  variable — generate it programmatically by mapping each garden question-group → tag-set:
  ```yaml
  definitions:
    topic_tags_women_rights: &topic_tags_women_rights
      - Women's Rights
  # …
      gender_roles_var:
        presentation:
          topic_tags: *topic_tags_women_rights
  ```
  A **per-question override** dict handles splits within a group: justifiable `divorce`→`Marriages & Divorces`,
  `homosexuality`→`LGBT+ Rights`, `cheating_on_taxes`/`accepting_a_bribe`→`Corruption`; worries
  `war`/`civil_war`→`War & Peace`, `terrorist_attack`→`Terrorism`, `losing_job`→`Work & Employment`; media
  `internet`/`email`/`mobile_phone`→`Internet`; jobs-scarce `men_more_right_to_a_job`→`Women's Rights`,
  `priority_to_nationals`→`Migration`.
- **Multiple tags are allowed** — e.g. environment-vs-economy → `[Climate Change, Economic Growth]`.
- **No matching topic page → OMIT `topic_tags` entirely.** For the soft-attitude / crime-fear batteries
  (Schwartz, important-in-life, neighbors, most-serious-problem, crime fear, traditional media) there is no
  topic page. **Do not write `Uncategorized`** — it's a valid enum value but has no DB tag row, so it links to
  nothing *and* logs a `missing_tags` warning per variable. Omitting gives the same result with no noise.
  Drop the now-empty `presentation` block when `topic_tags` was its only key.
- **`attribution_short` survives.** Setting only `topic_tags` on a variable does **not** wipe a common
  `presentation.attribution_short` — the combiner merges `presentation` key-by-key
  (`_merge_variable_metadata` `merge_fields`), so the common value still inherits.
- **Sanity-check choices against what existing IVS charts already use** (chart tags can include non-topic-page
  tags, so still filter your final pick through the enum):
  ```sql
  SELECT t.name, COUNT(DISTINCT c.id) FROM charts c
  JOIN chart_dimensions cd ON cd.chartId=c.id JOIN variables v ON cd.variableId=v.id
  JOIN chart_tags ct ON ct.chartId=c.id JOIN tags t ON ct.tagId=t.id
  WHERE v.catalogPath LIKE '%ivs/<v>/%' GROUP BY t.name ORDER BY 2 DESC;
  ```

**Sweep the untagged pool against the FULL enum — many items have a non-obvious nearest-fit page.** Don't stop
at the obvious group→tag map. Print the whole `topic_tags` enum and walk each remaining untagged question
against it; surprisingly specific pages exist. Real matches found this way (2026-06-03): justifiable
`suicide`→`Suicides`, `political_violence`→`War & Peace`, `parents_beating_children`→`Violence Against Children
& Children's Rights`, `man_beating_wife`→`Women's Rights`, `invitro_fertilization`→`Fertility Rate`; neighbours
`immigrant_foreign_workers`→`Migration`, `aids`→`HIV/AIDS`, `drug_addicts`/`drug_sale_in_streets`→`Illicit Drug
Use`, `heavy_drinkers`→`Alcohol Consumption`, `racist_behavior`→`Human Rights`; important-in-life
`work`→`Work & Employment`, `politics`→`Democracy`, `leisure_time`→`Time Use`, `friends`/`family`→`Loneliness &
Social Connections`; worry `not_being_able_to_provide_good_education`→`Global Education`. Present the candidate
fits (clear vs. judgment-call) to the user/topic owner and let them confirm — they often know which existing
charts already carry a tag (e.g. "our political-violence charts use War & Peace").

**Add topic tags as SECONDARY tags to already-tagged items that also touch that topic** (multi-tag, primary
first). E.g. items mentioning religion that are primarily something else get `Religion` added alongside:
`religious_authorities_interpret_the_laws`→`[Democracy, Religion]`, `confidence_churches` /
`confidence_organization_of_the_islamic_world` / `trust_another_religion`→`[Trust, Religion]`, E220
science-vs-faith→`[Technological Change, Religion]`. Each distinct combo is its own anchor
(`topic_tags_trust_religion`, …); reuse the existing single-tag anchor object when a variable's tag-set matches it.

**Judge by what the indicator measures, not a keyword.** Literal keyword matching over-reaches: `family_victim_
of_a_crime` mentions "family" but is a crime-victimisation question, **not** a family/social-connections one —
don't tag it `Loneliness & Social Connections`. Same for "faith" inside a science item, etc. Flag the ambiguous
ones to the user instead of auto-tagging.

**Re-routing a tag → delete the orphan anchor.** If you move a variable from one tag to another
(`invitro_fertilization`: Medicine & Biotechnology → Fertility Rate), and the old anchor is now referenced by
nothing, remove it from `definitions` so no dead anchors linger.

**Tag edits are metadata-only — the `data://grapher` dataset must be rebuilt or the DB won't update** (see
Step 7's gotcha): `etlr grapher://… --grapher --force --only` alone re-uploads the **stale** built dataset and
every variable is `skipped_no_changes`. Rebuild `data://grapher` (delete its output dir if unsure), then upload,
then verify links landed in `tags_variables_topic_tags`. It's normal and fine to leave a large share untagged
when no page fits (values/morality batteries, generic crime-fear, traditional media) — don't force a bad tag.

## Step 6 — Pre-flight cross-check (before running the pipeline)

Assert the set of new column names is **identical** across three places, or you get silent breakage:
(a) what Stata+meadow produce, (b) the garden `check_sum_100`/`replace_dont_know` groups, (c) the
`.meta.yml` keys. Reconstruct (a) directly from the `.do` collapse lists — every column the loop emits is
`{prefix}_{underscore(VARS_DICT_label)}` for loop groups, or the literal name for custom blocks — then
diff against the meta keys:

```python
from owid.catalog.core.utils import underscore
from etl.files import ruamel_load

# the exact (prefix list) per group, mirroring each .do collapse line (incl. aggregates + avg_score)
GROUPS = {("E248B", "E254B", ...): ["at_least_weekly", "less_than_weekly", "daily", ..., "avg_score"], ...}
LABELS = {"E248B": "Information source daily newspaper", ...}  # == meadow VARS_DICT
stata = {f"{p}_{underscore(LABELS[c])}" for codes, prefs in GROUPS.items() for c in codes for p in prefs}
stata |= {f"{p}_humankind" for p in [...]}            # custom blocks: literal names, no rename

meta = ruamel_load(open("etl/steps/data/garden/ivs/<v>/integrated_values_surveys.meta.yml").read())
# Pick the table you're editing: "integrated_values_surveys" (IVS) or "world_values_survey" (WVS).
TABLE = "integrated_values_surveys"
metakeys = set(meta["tables"][TABLE]["variables"])
print("stata not meta:", sorted(stata - metakeys) or "none ✓")
print("meta not stata:", sorted(metakeys - stata) or "none ✓")   # restrict metakeys to the new ones
# also: assert no new code endswith another new code (rename ambiguity)
# and assert ALL titles are unique across BOTH tables (one shared grapher dataset — see Step 5):
titles = [v["title"] for t in meta["tables"].values() for v in t["variables"].values()]
assert len(titles) == len(set(titles)), "duplicate variable titles — grapher upload will fail"

# and: every topic_tag must be in the curated schema enum (else it's silently dropped at upload)
import json
schema = json.load(open("schemas/dataset-schema.json"))
TAG_ENUM = set(schema["properties"]["tables"]["additionalProperties"]["properties"]["variables"]
               ["additionalProperties"]["properties"]["presentation"]["properties"]["topic_tags"]["items"]["enum"])
for v in meta["tables"]["integrated_values_surveys"]["variables"].values():
    for t in (v.get("presentation") or {}).get("topic_tags", []):
        assert t in TAG_ENUM, f"invalid topic_tag {t!r} — not a topic page in dataset-schema.json"
```

A clean both-empty diff (counts equal) + unique titles means the Stata→meadow output, the garden groups,
and the meta keys agree, and the grapher upload won't trip the title-uniqueness assert — all before a
single pipeline step runs.

## Step 7 — Run & verify

```bash
.venv/bin/etlr integrated_values_surveys --private             # meadow→garden→grapher
.venv/bin/etlr integrated_values_surveys --grapher --private   # upload to staging grapher
```

- **Two separate gates:** the build (first command) runs `check_sum_100` in garden (green = recodes sum to
  100%). The `--grapher` upload (second command) runs additional grapher-only validations — notably
  **title uniqueness** — so always run it too; a title clash only surfaces here. Don't stop at the build.
- Spot-check ranges by loading the garden dataset: share columns 0–100; each `avg_score` on its native
  scale (frequency 1–5, closeness 1–4, agree 1–10, index 0–1).
- The benign `DisplayNameWarning` about `presentation.title_public` (1000+ columns) is expected — IVS
  doesn't set `title_public`; not an error.
- **Metadata-only changes (e.g. `topic_tags`) need the `data://grapher` dataset rebuilt — `grapher://… --only`
  is not enough.** `etlr grapher://… --grapher --force --only` forces only the **upload** step, and `--only`
  skips its `data://grapher` build dependency, so it re-uploads the **stale** grapher dataset (every variable
  `skipped_no_changes`) and the new tags never reach the DB. Rebuild the built dataset itself —
  `etlr data://grapher/ivs/<v>/integrated_values_surveys --private --force --only` (delete
  `data/grapher/ivs/<v>/integrated_values_surveys/` first if in doubt) — **then** run the `--grapher` upload.
  Confirm the links actually landed (and that omitted/`Uncategorized` columns produced **no**
  `create_links.missing_tags` warnings):
  ```python
  from etl.config import OWID_ENV
  print(OWID_ENV.read_sql("""SELECT t.name, COUNT(DISTINCT v.id) n FROM variables v
    JOIN tags_variables_topic_tags tv ON tv.variableId=v.id JOIN tags t ON t.id=tv.tagId
    WHERE v.catalogPath LIKE '%%ivs/<v>/%%' GROUP BY t.name ORDER BY n DESC""").to_string())
  ```
- `make check` before committing.

## Step 8 — Git (per repo CLAUDE.md)

```bash
.venv/bin/etl pr "Add … indicators to Integrated Values Surveys" data   # no emoji in title with a category
git add … && git commit -m "✨🤖 …"   # 📊🤖 for the snapshot/.dvc regen
git push
```

Post `@codex review` as a **separate** PR comment; keep the PR body in sync with substantial pushes.

## Step 9 — Notion `Available` dictionary (required — IVS only)

**WVS target: skip this step entirely** — there is no Notion `Available` tracker for `world_values_survey`.
For IVS this is **not** optional — always finish the job by flipping the flags so the dictionary reflects
what's actually in the dataset.

The **Integrated Values Surveys dictionary** Notion DB (`collection://fd3a1354-9da6-48e7-91bb-a0665ddd0f0b`)
has an `Available` checkbox per code. Flip `Available = ✅` **only for codes that survive into the garden
dataset** = the `.do` keep list **minus** items excluded in `drop_indicators_and_replace_nans`. The
reliable way to get that set is to read the garden table columns and reverse-map to base codes — a `.do`-only
list over-counts (e.g. `ds E069*` keeps `E069_64`/"Elections" which produces no column, and ~8 confidence
items are dropped in garden). Beware substring false-positives when reverse-matching ("elections" matches
`..._free_elections`); verify the exact column exists before flipping. The Notion MCP has **no bulk row
query** — fetch/update per row (`notion-search` returns titles only; `notion-fetch` returns `Available`;
`notion-update-page` with `command="update_properties"`, `properties={"Available":"__YES__"}`). The row's
`Answers` field is a handy independent cross-check of your recode mapping.

**Each row's title is exactly the code** (the `Variable` title property). Find a row by searching the data
source for the code (`notion-search` with `data_source_url=collection://…`), but **match the title
EXACTLY** — search is fuzzy and returns look-alikes (`E248B` query also returns `E248`; `G062` returns
`F062`/`D062`; `E001` returns `Y001`/`F001`). Updates are independent per page, so fire all of them in
parallel (one `notion-update-page` per code). Verify with one `notion-fetch` afterward that `Available`
reads `"__YES__"`.

## Step 10 — Indicator → admin-link table (share with reviewers / PR comment)

After the upload, produce a compact **one-row-per-question** table that links each response option to its
Grapher admin page — handy for a PR comment or a hand-off to a topic reviewer. A **generic** script lives
at `scripts/indicator_admin_table.py` (next to this skill) — it hardcodes **no** indicators. Edit only the
small CONFIG block (`VERSION`, `BRANCH`, `ENV`) and run it:

```bash
.venv/bin/python .claude/skills/add-ivs-indicators/scripts/indicator_admin_table.py
```

It works in three moves, all auto-derived:
1. **Which variables did the PR create?** Diff the garden `.meta.yml` variable keys between `master` and
   the PR branch (one entry per variable): `meta_keys(BRANCH) - meta_keys("master")`. Robust even when the
   working tree is on `master` (it uses `git show <ref>:<file>`).
2. **Resolve variable ids** from the Grapher DB and cluster columns into questions. Loop-group columns →
   IVS code via the **branch meadow `VARS_DICT`** (it reverses `{underscore(label): code}` and matches the
   longest column suffix, so `family_victim_of_a_crime` beats `victim_of_a_crime`). Custom single-question
   blocks (no `VARS_DICT` entry, so the column carries no code) are clustered by stripping a generic IVS
   recode-prefix vocabulary and keyed by their column **suffix**; their label is derived from the variable
   `name`s.
3. **Label and order each option link.** Link text is the option's **real category name** (strip the
   per-question shared prefix / a parenthetical suffix from the variable `name`; fixed text for
   dk/na/avg; humanised recode key as last resort) — so e.g. E124 shows "Fairly much respect", not the
   terse key `some`. Order within a question: **aggregates first** (positive rollup before negative),
   **Yes before No**, then the remaining categories, then Don't know / No answer / average score.
4. **Emit** `| IVS code | Question | option links |` rows — one flat table by default.

Three **optional** dicts in CONFIG add editorial polish without reintroducing hardcoded indicators:
`TOPICS = {"Human Rights": ["E124", ...], ...}` (group/order rows), `LABELS = {code_or_suffix: "…"}`
(nicer question wording), and `CODE_FOR = {custom_suffix: "IVS_CODE"}` (give custom blocks like
`humankind`→`E158`, `science_world`→`E234`, `secure_neighborhood`→`H001` their real code). PR #6180's
output used all three; with them empty you still get a complete, correct table.

**Staging vs production differ only in the DB connection + admin base** (the script switches on `ENV`):
- **Staging** (`ENV="staging"`): container = `staging-site-<branch normalised & truncated to 28 chars>`
  (via `get_container_name`); admin `http://<container>/admin/variables/<id>`. On `master`,
  `OWIDEnv.read_sql` falls back to local, so the script connects to `<container>.tail6e23.ts.net` MySQL
  **directly**. The `datasets.catalogPath` has **no** `grapher/` prefix (e.g.
  `ivs/<v>/integrated_values_surveys`) even though `variables.catalogPath` does — filter on the dataset row
  accordingly.
- **Production** (`ENV="production"`): admin base is `https://admin.owid.io/admin`. Reach the prod grapher
  DB **directly over Tailscale** at host **`prod-db`, port 3306** (check `tailscale status` for it) with the
  `live_grapher` creds from `.env`/`.env.live` — this is far more reliable than `.env`'s `127.0.0.1:3310`
  local SSH tunnel, which is usually down (`Connection refused`). Build the engine with
  `sqlalchemy.engine.URL.create(...)` so the special-char password is encoded correctly. The indicators
  exist in production **only after the PR is merged and the dataset is deployed**, and **production variable
  ids differ from staging** — always re-query prod (don't reuse staging ids). After merge, the
  branch-vs-`master` diff is empty, so set `NEW_COLS_FILE` (e.g. the saved `ai/ivs_pr_new_cols.txt`) or
  `BASE_REF` to the merge base. The `len(v) == len(NEW)` assert doubles as an "is it deployed yet?" check.

## Worked example

PR **owid/etl#6180** is a full reference diff — it spans nearly every shape above (3/4/5-pt agree, 4-pt
frequency & closeness, binary, 5-pt frequency, 10-pt agree + better/worse, multinomial 1-of-N, the Y022
0–1 index, and an EVS-fallback single-question block).
