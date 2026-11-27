---
name: fact-check-dataset
description: >-
  Adversarially review an ETL dataset's data and metadata for factual accuracy — verifies metadata claims against the producer's own documentation (fetched from the links in snapshot .dvc files and metadata texts) and cross-checks anomalous plus anchor values against independent sources online, to catch unit errors, wrong-year values, and hard-to-detect mistakes made by the source itself. Also checks the texts of a static chart (a Figma frame or a static viz) against the indicator and its producer. Use when someone wants a dataset's values or metadata, or a static chart's text, checked against the producer and independent sources, or as the factual-accuracy step inside /update-dataset, /create-dataset, /review-data-pr, /create-static-viz and /owid-staff:create-figma-chart.
metadata:
  internal: true
  owner: paarriagadap
---

# Adversarial data review

Attack the dataset the way a hostile referee would: treat every metadata sentence and every data value as a claim that must survive verification against (a) the producer's own documentation and (b) independent sources — because the producer itself can be wrong.

Two failure classes to hunt:

1. **Our mistakes** — misread units, metadata content in the wrong field, stale producer text, scope overclaims, processing bugs that ship wrong numbers.
2. **The source's mistakes** — unit slips, wrong-year values, transcription errors, stale pre-revision values. These are hard to detect precisely because our pipeline faithfully reproduces them; only an *independent* source can expose them.

## Scope — what this skill is NOT

Factual accuracy only. Don't duplicate:

- Style-guide compliance → `/check-metadata-style`
- Spelling typos → `/check-metadata-typos`
- Jinja whitespace artifacts → `/check-metadata-style` (its mechanical first pass)
- Field-coverage / freshness / link-liveness audits → `/update-dataset` § 6c (that step checks the links *resolve*; this skill *reads what's behind them*)

Any rewrites you propose use American spelling.

## Inputs

- A step path `garden/<namespace>/<version>/<short_name>` (the `data://` URI form also works), or a bare `<short_name>`: `prepare.py` resolves it to the latest active version and stops when it is ambiguous — ask the user then.
- Optional: `--top N` (deep-review cap, default 10), `--full` (deep-review every indicator regardless of chart usage), `--indicators a,b` (name the ones to deep-review).
- A **single chart** (slug, link or id) or a **static chart's brief** (`check_facts.md` from `/owid-staff:create-figma-chart`): the review is scoped to that chart's indicators, its displayed values and its strings. Drafts count (`--staging <branch>`; they have no production row).
- Precondition for a dataset: the garden dataset is built locally (`.venv/bin/etlr data://garden/<ns>/<version>/<short_name>`; `PREFER_DOWNLOAD=1` is fine for already-published upstream deps). Without it the anomaly scan does not run, and the dossier says so. **Exceptions:** the `snapshot` context needs only the `.dvc`, and a chart needs no local build.

## Contexts

Where the table says **mandatory**, the calling skill always runs it, in the background and with effort scaled to the dataset; elsewhere it is optional.

| Context | `prepare.py` | Agents | Scope |
|---|---|---|---|
| `/update-dataset` § 6c-bis (**mandatory**) | `<step> --context update` | two lanes | New/changed metadata text + newly added data values (latest wave/year); deep review = top-N + anomalies |
| `/update-dataset` § 7, Anomalist pass (**mandatory**) | none: `anomalist_candidates.py <step> --staging <branch>` on the 6c-bis dossier | one | Only the Anomalist candidates the first pass did not cover; then `assemble_report.py` again |
| `/create-dataset` Step 6b (**mandatory**) | `<step> --context create` | two lanes | All indicators if there are few; otherwise the main ones, with a few value cross-checks each |
| Standalone | `<step>` | two lanes | Top-N + anomalies (or `--full`) |
| `/edit-faust-metadata`, `/generate-metadata` (**mandatory**) | `<step> --context claims` | one | Claims-only, on the added/edited metadata text exclusively, plus the DoDs it links — verify each new/changed sentence against the producer docs. NO data-value cross-checks, anomaly scans or indicator prioritization (no data changed), and unedited metadata is out of scope |
| `/create-snapshot` § 5 (optional) | `<step> --context snapshot` | one | The `.dvc` claims against the fetched producer docs only (no built dataset yet, so no data cross-checks) |
| `/owid-staff:create-figma-chart`, `/create-static-viz` (**mandatory**) | `--brief <check_facts.md>`, or `--chart <slug> --strings <file> [--data <csv>]` | one | Every string the image carries and the values it displays. No ranking, no dataset-wide anomaly scan, no DoDs (an image has no hover) |
| `/review-data-pr` § 10b | none: read the author's report | one | Only if the author ran it: verify outcomes and independently spot-check 2–3 findings and 2–3 anchor values |

## Run it fast

A review's time goes on turns, not on the web: each turn re-reads the whole context. Spend turns on judgement only.

1. **Start with one message holding three calls**: `prepare.py` for your context (table above), the `ToolSearch` that loads `WebSearch` and `WebFetch`, and a Read of the reference file your context names (*How to run* below). Read each skill file once, with the Read tool, not `cat`.
   ```bash
   .venv/bin/python .claude/skills/fact-check-dataset/scripts/prepare.py <target and options>
   ```
   It reads production from the internal Datasette (`/query-grapher-db`), so it needs Tailscale.
   It resolves the steps, reads the `.dvc` origins, renders the metadata, fetches the producer's documentation in parallel as plain text, re-checks the source files, and (for a dataset) ranks the indicators, scans for anomalies and lists the DoDs. It prints a dossier and says what to do next. Any section it marks `FAILED` or `NOT RUN` is yours to do by hand; never skip one.
2. **Read the producer's documentation from the dossier's `docs/` folder** with Grep (Read only the passage you need: the files can be long), and quote from that text with the file's fetch date. Fetch a page yourself only when a claim under review rests on it and the table marks it `unreachable`, `thin` or `not-fetched`, or when a claim needs a source the table lacks.
3. **Batch what is independent.** Every WebSearch you already know you need goes in one message; then every WebFetch of the hits in one message. Same for file reads and local checks. One call per message only when the next call depends on the last result.
4. **Web access.** `curl` always with `--max-time 20`. Escalate curl → WebFetch → the Wayback Machine (only through `curl -sG "https://archive.org/wayback/available" --data-urlencode "url=<url>"`: WebFetch cannot reach `web.archive.org`) → offer the user their Chrome (Claude in Chrome extension) before calling a page unreachable.
5. **Local work.** Python goes in a file under the dossier directory, written with the Write tool (a Bash heredoc can be refused in a worktree) and run with `.venv/bin/python <file>` (never `python3 -c`). One script that answers several questions beats several one-liners. `anomaly_scan.py --columns` takes garden or grapher column names. Read data from the built garden or grapher dataset; open the raw snapshot only for the attribution of a bad value, filtered to the rows in question.
6. **Don't redo.** Don't re-read a file the dossier quotes, and reuse what an earlier review listed in the dossier established about the source.
7. **Hand back short.** The report or result file holds the detail. The message back is the verdict, the count by severity and the file's path.

## How to run

- **Two lanes (update, create, standalone).** A subagent cannot start subagents, so the fan-out belongs to whoever has the Agent tool: the calling session. It runs `prepare.py`, starts the two briefs the dossier names (`lane_claims.md`, `lane_values.md`) as background agents **in one message**, each with the prompt `Read <brief> and do what it says.`, and goes on with its own work. When both are back it reads each result's *Notes for the other lane*, settles any contradiction between them, and runs `assemble_report.py <dossier> --reconciled`, which writes `ai/adversarial-review-<short_name>-<YYYY-MM-DD>.md`. If you are a subagent asked for a full review, run both lanes yourself, claims first, and assemble. Procedure: [reference/dataset-review.md](reference/dataset-review.md).
- **The Anomalist pass (update).** Anomalist, which owidbot runs on every push, stores old-vs-new and Gaussian-process anomalies in the branch's staging DB. After the indicator upgrade, `scripts/anomalist_candidates.py` reads them, ranked as the Wizard ranks them, marks those the first pass already checked or flagged, says what Anomalist left out, and writes `lane_anomalist.md` into the same dossier. The caller starts one background agent on it, and `assemble_report.py` folds its result into the report. These are extra candidates: the first pass's scan, anchors and cross-checks still run in full. Procedure: the *Anomalist candidates* section of [reference/dataset-review.md](reference/dataset-review.md).
- **One agent (claims, snapshot).** The *Claims lane* of [reference/dataset-review.md](reference/dataset-review.md), limited to the context's scope. The report keeps the same parts, with Part 2 saying which checks the context leaves out.
- **Chart texts.** [reference/chart-texts.md](reference/chart-texts.md).
- **A mismatch, a suspected source error, a correction.** [reference/source-errors.md](reference/source-errors.md), before routing anything.

## HARD RULE — proportionality

The severity of any provenance or factual critique must be proportional to the depth of verification you achieved. If you read the documentation and confirmed a gap, make a strong claim. If the docs were unreachable after the full escalation, cap the language at "I was unable to verify … — worth checking before merge" and the severity at 🟢. Never assert an error you couldn't check.

## Tolerance

Rounding, vintage/revision drift, and methodology gaps of a few percent are *not* findings. The targets are magnitude errors (×10/×100/×1000), wrong-year values, sign errors, entity mix-ups, and stale pre-revision values. Declaring a **confirmed source error** requires one of two routes: **(a) ≥2 independent sources that agree with each other and disagree with ours** beyond methodology tolerance, or **(b) an accounting-identity contradiction internal to the producer's own release**, under the guards in [reference/source-errors.md](reference/source-errors.md).

## Routing findings

| Finding | Author flows (update/create/standalone) | Review flow |
|---|---|---|
| Metadata contradicts producer docs (unit/definition/scope; content in the wrong field per the `.dvc`-vs-`description_processing` split) | Edit `.meta.yml`/`.dvc`, re-run the step (`--grapher` for grapher channel) | 🔴/🟡 with quote + doc link |
| Value wrong in our output but correct in the raw snapshot | Fix the step code — never corrections.yml, never mask | 🔴 |
| Value confirmed wrong **at the source** (raw snapshot carries it; confirmed via route (a) or route (b) of the Tolerance gate — for (b), *with the erroneous term identified*) | Add `<short_name>.corrections.yml` next to the garden step + `tb = paths.apply_corrections(tb)` (format: `etl/data_corrections.py`); fill `reason`/`producer`/`status`, add an `expect` guard; tell the user to notify the producer and record the `reported:` date | 🔴 if confirmed and uncorrected |
| Suspicious but unconfirmed (independent sources disagree with each other, or methodology plausibly explains the gap) | "Verify manually" item in the report — do **not** add a correction | 🟡 |
| Docs/data unreachable after curl → WebFetch → Wayback → the user's Chrome (offered and declined, or failed too) | "Unable to verify — worth checking" (proportionality cap) | 🟢 |
| Producer-doc vs. shipped-file discrepancy | Preserve the data as shipped; flag for producer follow-up | 🟢 |
| A static chart's string is wrong | Fixed upstream, where the chart's text lives, through `/edit-faust-metadata`; hold the image when the claim is load-bearing | 🔴/🟡 |

Severity rubric (aligned with `/review-data-pr`): 🔴 = confirmed factual error (metadata contradicted by the producer's own docs, or a value confirmed wrong via route (a) or (b), with snapshot-level attribution); 🟡 = likely issue needing confirmation; 🟢 = informational or unverifiable.

In `/update-dataset`, the caller also logs every source-attributed finding for its message to the producer (`update-dataset/reference/producer-issues.md`).

In author flows, apply the 🔴 fixes immediately (they're why the skill ran before commit); leave 🟡/🟢 as report items for the user to triage. A lane agent only reports: the calling session applies the fixes.

## Key constraints

- **Always fetch and search — never judge data plausibility from memory or training data.** The whole point is a fresh look at what the source and the wider literature actually say today. A saved copy of a document counts as fetched while the dossier lists it; say its fetch date when you quote it.
- **Never cap silently.** Every indicator is reviewed or listed as SKIPPED with its rank; every anomaly is checked or listed as unchecked. A truncated review that reads as complete is itself a factual error.
- Establish source-vs-OWID attribution before critiquing either layer; every finding names whose layer it targets.
- Severity proportional to verification depth — the HARD RULE above.
- Don't manufacture objections. A clean report is a valid outcome; if uncertain whether something is wrong, say "verify this" rather than asserting it.
- Separate data-level from text-level findings — a legitimate metric can sit under overclaiming text, and carefully hedged text can sit on top of broken data.
- Methodology differences are not errors. Name the specific mechanism before calling a mismatch an error.
- Factual accuracy only — no style/typo/spacing duplication (see Scope).
- No persistent files beyond the `ai/` report and the dossier under `ai/fact-check/` — plus, in author flows, the metadata/corrections edits themselves. Ad-hoc analysis scripts stay in the dossier directory and are not committed.
