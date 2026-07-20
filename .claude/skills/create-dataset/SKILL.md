---
name: create-dataset
description: Create a brand-new OWID dataset in ETL from a data file the user provides — a local CSV/Excel, a downloaded file, or a web link to data (snapshot → meadow → garden → grapher → PR → staging server). Use when someone has data they want in ETL so they can build charts. Designed for non-technical "Cloud co-work" users: infer aggressively, build a working dataset first, then ask the person to review and correct.
metadata:
  internal: true
---

# Create a dataset

Turn data the user provides into a fully wired-up OWID dataset — snapshot, meadow, garden, grapher, DAG entries, a draft PR, and a staging server where the user can build charts.

The input can be **anything**: a local CSV or Excel file, a file they just downloaded, or a link to a page/file on the web. Whatever the form, the job is the same — get the tabular data into a snapshot, then build the chain on top of it.

This skill is for **people who are not ETL experts** (typically working in Claude Code on the web / "Cloud co-work"). They may know some metadata (units, what the columns mean) or they may just have a link to the source page. They should **not** be quizzed field-by-field.

> **Paired skill — keep in sync.** [`/create-snapshot`](../create-snapshot/SKILL.md) is the canonical owner of the snapshot-creation conventions this skill consumes in Step 4: whenever snapshot conventions change here or there, mirror the change in the other file in the same commit — see the mirror note there. The update-side skills are part of the same family: this skill points into [`/update-dataset`](../update-dataset/SKILL.md)'s canonical sections (§5b-bis sanity bounds, §5c harmonization audit, §6b metadata quality, §6c metadata checklist + link verification, §6d scheduled issues), whose outcomes [`/review-data-pr`](../review-data-pr/SKILL.md) verifies — when those sections change, check whether this file needs a matching edit.

## Guiding principles

1. **Build first, review later.** Don't interview the user for every detail up front. Inspect the file, infer everything you reasonably can, fill sensible defaults for the rest, and **build a working dataset end-to-end**. Then hand the user a concise review so they correct a finished thing rather than imagine an abstract one.
2. **Ask rarely, and ask all at once.** There is exactly **one** required checkpoint with the user before building (the consolidated confirmation in Step 2), and exactly **one** after building (the review handoff in Steps 7–8, which also tells them how to publish to live). Don't drip-feed questions. If you can guess it, guess it and flag the guess for review.
3. **Never block on a missing detail.** If you can't infer a field, use a clearly-marked placeholder (e.g. `attribution_short: TBD`), note it in the review, and keep going. A dataset that's 80% right and on staging beats a perfect one that never ships.
4. **Surface every guess.** The review in Step 7 must list what you inferred vs. what the user gave you, so nothing silently ships wrong.

## Inputs

Required (one of):
- A **local data file** — absolute path to a CSV, Excel, etc.
- A **web link** — a URL pointing at a data file (CSV/Excel/JSON) or a page that links to one.

Optional:
- `metadata_url` — a link to the source page / documentation (if different from a data link). If given, fetch it for metadata (producer, citation, license, definitions).
- Anything the user volunteers (units, column meanings, namespace, title…).

---

## Workflow

### Step 0 — Pull the latest `master` first

Users are often not experienced with git and may be sitting on a stale checkout. Before doing anything, get the repo onto a fresh `master` so the new branch and PR are based on current code:

```bash
git switch master && git pull --ff-only origin master
```

If they have uncommitted local changes or a detached HEAD that blocks this, surface it plainly and ask how they want to proceed — don't force it. (When testing/iterating in an unusual git state, this step may be intentionally skipped — but for a real user, always start here.)

### Step 1 — Get the data into reach and inspect it (no questions yet)

If the input is a **web link**, first obtain the actual data file: download a direct CSV/Excel/JSON URL (use `etl.http`'s session for OWID hosts; plain download otherwise), or if it's a landing page, WebFetch it to find the download link. Save it locally so the snapshot can ingest it. If the input is already a **local file**, use it directly.

Then read the file and figure out its shape. Do **not** ask the user anything in this step.

1. **Load it.** Read the first ~50 rows with pandas to see headers, dtypes, and a sample. Then compute, per column: dtype, min/max (numeric), distinct count, % null. Use the `duckdb` skill or a short `.venv/bin/python` snippet — whatever's quickest.
2. **Identify the entity (country) column.** Look for a column named (case-insensitive) `entity`, `country`, `nation`, `location`, `geo`, `area`, or `region`. OWID-exported CSVs usually call it `Entity`. The right column holds country-like names, not codes. If there's both a name column and an ISO `code` column, use the name column and drop the code.
3. **Identify the time column.** Look for `year`, `date`, `time`, `period`. A column of 4-digit integers in ~1500–2100 is a year; ISO-date strings are a date. If the time dimension is a date (not a plain year), the garden step must keep it as `date` and `.format(["country", "date"])`.
4. **Everything else is an indicator column.** For each, record name, dtype, value range, and null %.
5. **Detect extra dimensions.** If a non-numeric column repeats per (country, year) — e.g. `sex`, `age`, `variant`, `fuel_type` — it's a dimension, not an indicator. The table is then long-format keyed by `["country", "year", <dim>...]`. Most simple CSVs have none; handle them only if present (see the long-format note in `/update-dataset`).
6. **Infer units from names + ranges** (these are guesses — flag them for review):
   | Signal in column name / values | Inferred `unit` / `short_unit` |
   |---|---|
   | `share`, `pct`, `percent`, `_rate`, and values mostly in 0–100 | `%` / `%` |
   | `share` / `proportion` with values in 0–1 | flag: is this a fraction (×100 → %) or already %? |
   | `usd`, `gdp`, `price`, `cost`, `$` | `US dollars` / `$` |
   | `population`, `count`, `number`, `_n`, integer counts | leave `unit` descriptive (e.g. `cars`), `short_unit` empty |
   | `tonnes`, `kg`, `kwh`, `gwh`, `co2`, `emissions` | physical unit from the name |
   | can't tell | leave `unit: ""`, flag in review |
7. **Derive naming defaults** from the filename. A filename like `Electric car sales (IEA, 2026) - data.csv` implies: title `Electric car sales`, producer `IEA`, year `2026`. Propose:
   - `short_name` — snake_case of the core title (`electric_car_sales`).
   - `namespace` — the producer's slug if one already exists under `snapshots/` (check `ls snapshots/`), else a sensible new one.
   - `version` — today's date (`date -u +"%Y-%m-%d"`).

Write a one-paragraph internal summary of what you found before moving on.

### Step 2 — One consolidated confirmation (the only pre-build checkpoint)

Now ask the user **once**, with all your best guesses pre-filled, using `AskUserQuestion` where it fits. Frame it as "here's what I figured out — correct anything that's wrong, otherwise I'll build it." Before listing the specifics, set expectations in one plain sentence so the end isn't a surprise — e.g. *"I'll build the dataset and put it on a staging server for you to review; once you're happy you merge the PR and it goes live."* Then keep the questions to the few things that genuinely can't be guessed or that would be expensive to get wrong:

- **Namespace + short_name + dataset title** (show your proposal; let them override).
- **What the data is / source** — confirm the producer and, if not already provided, ask for a `metadata_url` (the source page). If they give one, fetch it now for citation, license, and column definitions.
- **License** — show your best guess (default `CC BY 4.0` for academic/IGO sources if unknown) and let them correct.
- **Any column meanings you couldn't infer** — only ask about the genuinely ambiguous ones (e.g. "is `ev_sales_share` a percentage 0–100 or a fraction 0–1?"). Don't ask about columns you're confident on.

Everything else (units you inferred, descriptions, topic tags) you'll fill in and surface for review later — don't ask now.

If a `metadata_url` was provided, fetch it (WebFetch) and extract producer, `citation_full`, `attribution_short`, `date_published`, `license`, and any column definitions — same fields as `/create-snapshot` Step 1.

### Step 3 — Create a working branch

> **On PR permissions:** opening the draft PR and pushing to staging *is* this skill's deliverable. If a session-level rule says "don't open PRs unless explicitly asked," treat the user invoking this skill as that explicit request — go ahead and open the PR. The one exception is when you can't (e.g. the session pins you to a fixed pre-assigned branch you must not leave): in that case build on the current branch, open the PR from it if you can, and say clearly in the Step 7 handoff what you did and didn't do — don't silently skip the PR.

`etl pr` needs a branch (it fails on a detached HEAD). Create the PR scaffold up front so the rest of the work lands on a branch:

```bash
.venv/bin/etl pr "Add <dataset title> (<producer>)" data
```

This creates the branch + a draft PR and does **not** commit. (If the user is on a detached HEAD or the working tree is dirty in a way that blocks this, create a branch with `git checkout -b data-<short_name>` first and tell them you'll open the PR after the build.)

### Step 4 — Build the snapshot

This is a **manual-import** snapshot (you hand the snapshot a local file rather than relying on a stable download URL — even for web inputs, you've already saved the file locally in Step 1). Use the file's real extension in the `.dvc` name (`.csv`, `.xlsx`, …). Follow `/create-snapshot` conventions, with these specifics:

- `snapshots/<namespace>/<version>/<short_name>.<ext>.dvc` — fill `origin` from Step 2 (title, producer, citation_full, attribution_short, date_published, url_main, license). `description` must be the producer's verbatim text (page prose or paper abstract), not a paraphrase, and `citation_full` the producer's recommended citation verbatim when one exists (slight modifications only to fix typos or spacing in the source); for the license, check the documentation too and warn the user if none is stated anywhere (fall back to `© <producer> (<year>)`); if the file is one table/extract of a broader product, use `title_snapshot` + `description_snapshot` for the file specifics and any OWID-side notes (see `/create-snapshot`). Set `date_accessed: <version>`. Omit fields you don't have rather than leaving them blank; use `TBD` placeholders only where the review needs to flag them.
- `snapshots/<namespace>/<version>/<short_name>.py` — modern manual-import script. **No `click` decorators and no `if __name__ == "__main__"` block** — `etls` wraps a plain `run(upload, path_to_file)` and supplies the CLI itself:

```python
"""Script to create a snapshot of dataset.

The data file is provided manually. Run with:
  etls <namespace>/<version>/<short_name> --path-to-file <path>
"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run(upload: bool = True, path_to_file: str | None = None) -> None:
    snap = paths.init_snapshot()
    snap.create_snapshot(filename=path_to_file, upload=upload)
```

Run it against the user's file:

```bash
.venv/bin/etls <namespace>/<version>/<short_name> --path-to-file "<path_to_file>"
```

### Step 5 — Build meadow / garden / grapher steps + DAG

Scaffold the three steps with `/create-etl-steps` (DAG file = the topic that best fits, e.g. `energy`, `health`; ask in Step 2 if unclear). Then adapt:

- **Meadow** — load the snapshot, rename the entity column to `country` if needed, cast low-cardinality string columns (`country`, dims) to `category`, `tb.format(["country", "year"])` (or `["country", "date"]`, plus any dims). Keep it light.
- **Garden** — `paths.regions.harmonize_names(tb, country_col="country", countries_file=paths.country_mapping_path)`, then `tb.format(...)`. Add `sanity_check_inputs` / `sanity_check_outputs` if the step does more than load-and-format (see CLAUDE.md "Sanity checks"); ground every threshold in the built data and pick value bounds from the by-indicator-type table in `/update-dataset` §5b-bis, then negative-test the checks. Don't strip origins — follow the metadata-preserving patterns in CLAUDE.md (`pr.concat`, no `np.where`, etc.).
- **Grapher** — pass the garden table through unchanged.
- **Metadata** (`<short_name>.meta.yml` in garden) — generate it with `/owid-metadata-generation`, then verify it against the mandatory-fields checklist in `/update-dataset` §6c. Fill `title`, `unit`, `short_unit`, `description_short`, `description_key` (≥1 bullet), `display.name`, `display.numDecimalPlaces`, `display.tolerance` per indicator; `topic_tags` and `processing_level` in `definitions.common`; `presentation.attribution_short` explicitly under `definitions.common.presentation` — it does **not** inherit from the origin's `attribution_short`. In the `dataset` block, set `update_period_days` plus `owners`: the user is the new dataset's first owner — resolve their canonical OWID name from `git config user.name` via `etl.owners.resolve_owner` (must match the `schemas/dataset-schema.json` enum; add the mapping in `etl/owners.py` + an enum row if missing), mirroring `/update-dataset` step 1a-bis. Use the units you inferred in Step 1; mark anything uncertain so it shows up in the review.
- **Outdated-practices check** — run `/check-outdated-practices` on every new step file (including any helper modules) and fix findings before the first run, per `/update-dataset` step 1b — run the skill, don't eyeball the patterns.
- **DAG form** — write the new chain in the nested (compact) DAG form (grapher → garden → meadow → snapshot declared inline; example in `/update-dataset` "Removing the old version & reordering the DAG", step 4) and verify it parses: `.venv/bin/python -c "from etl.dag_helpers import load_dag; load_dag()"`.

### Step 6 — Run the chain and harmonize countries

```bash
.venv/bin/etlr <namespace>/<version>/<short_name> --private
```

Fix whatever breaks (trace upstream, never mask). The most common task is **country harmonization**: the garden run logs unmatched country names that need mapping into `<short_name>.countries.json`.

**Shortcut for OWID-exported CSVs:** if the file came out of OWID (entity column literally named `Entity`), the country names are almost always already canonical. Don't sit through an interactive `etl harmonize` — auto-build the mapping by matching each entity against the canonical regions (and their aliases), and only fall back to manual mapping for the leftovers:

```python
import json
from pathlib import Path
from owid.catalog import Dataset
import pandas as pd

# Needs the regions dataset built locally:
#   .venv/bin/etlr data://garden/regions/2023-01-01/regions --private
tb_regions = Dataset(str(sorted(Path("data/garden/regions").glob("*/regions"))[-1]))["regions"]
canonical = set(tb_regions["name"].dropna().astype(str))
alias_map = {}
for name, al in tb_regions[["name", "aliases"]].dropna(subset=["aliases"]).itertuples(index=False):
    for a in json.loads(al):
        alias_map[a] = name

entities = sorted(pd.read_csv("<path_to_csv>")["Entity"].unique())
mapping, unmatched = {}, []
for e in entities:
    if e in canonical: mapping[e] = e
    elif e in alias_map: mapping[e] = alias_map[e]
    else: unmatched.append(e)  # decide these by hand: typo, alias, custom aggregate, or exclude
```

Resolve `unmatched` by hand against canonical regions; follow the harmonization-audit guidance in `/update-dataset` (Step 5c). Residual aggregates the producer defines (e.g. `Rest of World`, `European Union (27)`) can be kept as custom entities (map to themselves) — they won't join with population/region data, so note that in the review. Don't silently drop countries — if you exclude any, list them.

After the garden step builds, also run the **garden-output entity check** from `/update-dataset` §5c (Python check #5): load the built garden tables and confirm every `country` value is canonical (regions + latest income groups). It catches what the mapping file can't see — inline `tb["country"] = ...` assignments and post-harmonization mutations. List any non-canonical entity (including the custom aggregates you kept) in the Step 7 review and the PR body as living outside the canonical system.

Then build and upload the grapher step to staging — target the `grapher/...` path (no `--only`, so the `grapher://` MySQL upsert step actually runs):

```bash
STAGING=<branch> .venv/bin/etlr grapher/<namespace>/<version>/<short_name> --grapher --private
```

Confirm the upsert actually succeeded before moving on: it should print the dataset's admin URL / id (`…/admin/datasets/<id>`). **Capture that `<id>`** — you'll hand it to the user in Step 7. If the upsert errored or printed no dataset, fix it now rather than handing over a link that won't resolve.

### Step 6b — Adversarial fact-check of data and metadata (optional — offer it in the handoff)

Optionally run [`/adversarial-data-review`](../adversarial-data-review/SKILL.md) on `garden/<namespace>/<version>/<short_name>`. It's not part of the default build because it can consume many tokens (~12–20 web calls even for a small dataset) — mention it as an offer in the Step 7 handoff ("I can also fact-check the data and metadata against the source's documentation and independent sources — say the word") and run it when the user opts in, or proactively when the build surfaced red flags (values that look implausible, a source page contradicting the file).

When it runs: since the dataset is brand-new (no charts yet, few indicators), review **all** indicators. This catches the two things the Step 7 review table can't: metadata you inferred that the source's own documentation contradicts (units, definitions, scope), and values the *source itself* got wrong (unit slips, wrong-year rows) — verified against independent sources online. Fold the findings into the handoff in plain language ("I double-checked the numbers against <independent source> — X and Y match; Z looks off, here's why"), and route confirmed source errors to `<short_name>.corrections.yml` per that skill's routing table rather than editing the data inline.

### Step 7 — Commit, push, and hand off for review

1. **Quality pass before handoff.** Run `/check-metadata-typos`, `/check-metadata-spacing`, and `/check-metadata-style` on the new garden + grapher `.meta.yml` files, and walk the general-audience clarity checklist — full procedure in `/update-dataset` §6b. Then run the link-verification loop from `/update-dataset` §6c on every URL in the new `.dvc` and `.meta.yml` files, including its anchor-fragment pass for URLs with `#…` (a curl non-2xx is a *signal*, not proof — escalate WebFetch → Wayback before trusting it). After any `.meta.yml` edit, re-run the affected step (`--grapher` for grapher) so the built catalog and staging reflect it.
2. Run `make check`, confirm you're still on the work branch (`git branch --show-current` — a branch switch in the user's IDE silently moves your shell too), then commit and push:
   ```bash
   git add .
   git commit -m "📊🤖 Add <dataset title> (<producer>)"
   git push
   ```
3. Update the PR description (attribution blockquote per CLAUDE.md "Team"; what the dataset is, source, coverage, indicator count). Then post `@codex review` as a separate PR comment; while the user reviews, address any valid findings and resolve the threads per `/update-dataset` step 10 — don't block the handoff on the review round.
4. **Hand the user a review** — this is the second and final checkpoint. Present a compact table they can scan and correct:

   | Indicator (column) | Title | Unit | Decimals | Inferred? |
   |---|---|---|---|---|

   Plus a short list of:
   - **Things I guessed** (units, descriptions, license, topic tags) — "tell me if any are wrong."
   - **Things I couldn't determine** (any `TBD` placeholders).
   - **Countries that didn't match / were excluded**, if any.

   Always tell them **where the metadata file lives locally**, so they (or you) can edit it directly:
   - **Metadata YAML:** `etl/steps/data/garden/<namespace>/<version>/<short_name>.meta.yml` — this is the file to edit for titles, units, descriptions, and decimals.
   - Country mapping: `etl/steps/data/garden/<namespace>/<version>/<short_name>.countries.json`

   Then give them the **staging links** so they can build charts. Paste the actual URLs directly into the chat — don't tell them to "open the PR" or "go to the staging admin"; non-experts won't know where those are, and they're unlikely to open the PR at all:
   - **Dataset in staging admin:** the dataset page printed by the grapher upsert (the `<id>` you captured in Step 6), `https://staging-site-<branch>/admin/datasets/<id>`. This is where they create charts from the new indicators.
   - **Set the right expectation about timing.** The staging server rebuilds for a few minutes after each push, so this link will **404 (or show a "site not found" page) until the build finishes — that's normal, not a broken link**. Tell them to wait ~5 minutes and refresh. Better yet, before you hand the link over, poll it yourself until the staging host responds (e.g. `curl -so /dev/null -w "%{http_code}" https://staging-site-<branch>/admin/` stops returning a connection error / `404` "site not found") so you only give them a link that already works. Handing over a link before the build is ready — with no signal that waiting will fix it — reliably reads as "it's broken / I don't see my dataset."

5. Ask for corrections in plain terms ("anything in the table look wrong? any column you'd describe differently?"). Apply their feedback by editing the `.meta.yml` / `.countries.json` and re-running the affected step (`--grapher` for grapher), then push again.

### Step 8 — Tell them how to go live (don't assume they know)

The dataset and any charts they build live on the **staging server**, not on ourworldindata.org. Getting them to live is a manual step the user has to take, and the workflow (approve charts in chart-diff, then merge the PR) is unfamiliar to non-experts — they will not discover it on their own. Spell it out explicitly in the handoff, with the real links pasted in:

1. **Charts must be approved in chart-diff before they sync to live — and this is a human step, not yours.** If the user creates any charts on the staging admin, those charts only reach production if they're **approved** in chart-diff first. **Never approve charts yourself** (no CLI, no script, no direct DB write) — chart-diff exists so a person eyeballs each chart before it goes live, and that review must stay human. Give them the direct link and tell them to click **Approve** on each chart themselves:

   ```
   http://staging-site-<branch>/etl/wizard/chart-diff
   ```

   Creating the charts is also something the user does in the admin — assume they'll need to visit the admin at some point regardless; your job is to make that the *only* thing they have to do there.

2. **Merging the PR is what publishes the dataset to live.** No external review is required for a data PR like this. The user can click **"Ready for review"** then **"Squash and merge"** on the PR themselves (link the PR URL) — but you can also **do the merge for them from the session, if they explicitly ask you to and the checks are green.** This is the one outward-facing action where, given an explicit "merge it" from the user, you should go ahead — it saves a non-expert a trip to GitHub. Do it carefully:

   ```bash
   gh pr checks <number>          # confirm checks are green first
   gh pr ready <number>           # take it out of draft
   gh pr merge <number> --squash  # publish
   ```

   - **Only merge on an explicit request** from the user ("merge it", "go live", "ship it") — never proactively, and never as the default end of the skill.
   - **Only merge if checks are green.** If a check is red or still running, say so and don't merge — surface what's failing and let them decide. (Don't merge a red PR just because they asked; tell them what's red first.)
   - If they'd rather click it themselves, that's fine — point them at the PR.
   - The dataset (and any approved charts) land on ourworldindata.org a few minutes after the merge.

3. **Offer a scheduled update issue if the data refreshes regularly.** If `update_period_days` is roughly in [2, 366] (updated at least yearly but less than daily), the dataset qualifies for a scheduled "Data update" issue in `owid/owid-issues`, so future refreshes don't depend on anyone's memory. Offer to create the workflow per `/update-dataset` §6d — cron timed shortly after the producer's expected release window, issue body pointing the next updater at `/update-dataset <short_name>` — and only create it with the user's sign-off (the commit goes straight to owid-issues `main`).

4. Make this a short, plain-language checklist at the end of your handoff — e.g. *"When you're happy: (1) approve your charts here «chart-diff link», (2) then either merge the PR yourself «PR link» or just tell me to merge it and I'll do it once the checks are green — it's live a few minutes later."* Paste the real URLs, not placeholders.

## Notes & gotchas

- **Snapshot is raw passthrough.** Don't sum, dedupe, relabel, or convert period labels in the snapshot — that's garden's job (see CLAUDE.md "Snapshot is raw passthrough only").
- **`Entity` → `country`.** OWID-exported files name the country column `Entity`; rename it in meadow.
- **Shares: 0–1 vs 0–100.** The single most common unit mistake. If a "share"/"%" column tops out near 1, it's a fraction (multiply by 100 in garden, or set the unit to fraction); if it tops out near 100, it's already a percentage. When unsure, this is worth one of your Step 2 questions.
- **Start on fresh `master` (Step 0).** Saves you from basing the branch/PR on a stale checkout — common with non-git-savvy users. `etl pr` and staging also need a real branch (not a detached HEAD).
- **Keep the user oriented.** They're not ETL experts. When you report progress, say what you did in plain language and what they can do next (review the table, build charts on staging) — not a wall of pipeline jargon.
