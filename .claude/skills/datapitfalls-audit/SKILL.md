---
name: datapitfalls-audit
description: Audit an OWID dataset/ETL pipeline for data-analysis mistakes using the datapitfalls CLI (a Claude-powered tool built on Ben Jones' "Avoiding Data Pitfalls" 8-domain taxonomy). Scans the pipeline's code, metadata, published charts, and a plain-English description of its method, then synthesizes one consolidated report cross-referenced against the pipeline's existing sanity checks. Trigger when the user wants to "analyze/audit a dataset with datapitfalls", "run datapitfalls on <pipeline>", "scan <dataset> for data pitfalls", or "check <pipeline> for pitfalls".
metadata:
  internal: true
---

# datapitfalls audit

Run the [`datapitfalls`](https://github.com/bjonesdataliteracy/datapitfalls) CLI against an OWID dataset/pipeline and produce one consolidated, **tool-driven** report. The findings must come from the tool; the value this skill adds on top is (a) feeding it the *right* inputs (code + metadata + real charts + a prose description of the method), and (b) cross-referencing each finding against the pipeline's existing `sanity_checks()` so the reader can tell "already guarded" from "genuine gap".

`datapitfalls` is a Claude-powered npm CLI (`@anthropic-ai/sdk`) grounded in the 8-domain taxonomy from Ben Jones' *Avoiding Data Pitfalls* (Epistemic Errors, Technical Trespasses, Mathematical Miscues, Statistical Slip-Ups, Analytical Aberrations, Graphical Gaffes, Design Dangers, Biased Baseline). It detects type by file extension and scans code / images (Vision) / PDFs / prose.

## Prerequisites

- **Node ≥18 + npx** (`node --version`). We run via `npx --yes datapitfalls@latest …` — **no global/local install**.
- **`ANTHROPIC_API_KEY`** — the CLI calls the Claude API. It usually isn't set in the shell, but a key lives in `etl/.env`. The scripts source it from there for the run only and **never print it**. Confirm it exists first (length/prefix only, value never echoed):
  ```bash
  grep -q '^ANTHROPIC_API_KEY=' /Users/parriagadap/etl/.env && echo "key present"
  ```
  If absent, ask the user how to provide one — do not proceed without a key.
- Verify the tool runs before scanning: `npx --yes datapitfalls@latest stats` (prints the 76-rule catalog).

## When to use

- The user wants a pitfalls/quality audit of a whole dataset pipeline (not a single line of code).
- Good targets: a garden step doing real business logic, plus its snapshot extraction script, metadata, and published charts.
- **Not** for: a quick lint of one function (scan that one file directly), or anything the `check-outdated-practices` / `faust-metadata-audit` / `review-data-pr` skills already cover better.

## Workflow

### 1. Resolve the target files
**Scope all outputs under a per-pipeline dir.** `dp_aggregate.py` tallies *every* `*.json` under the scan dir you point it at, so a flat shared `raw/` lets a previous audit (of a different pipeline) silently contaminate this run's counts and report. Put everything under `ai/datapitfalls/<short_name>/` from the start — `raw/`, `charts/`, `text/` — so each audit is self-contained and nothing needs deleting:
```bash
mkdir -p ai/datapitfalls/<short_name>/{raw,charts,text}
```
All example commands below use `ai/datapitfalls/raw` etc. for brevity — substitute your `ai/datapitfalls/<short_name>/raw`.

Given a dataset key (`namespace/version/short_name`) or pipeline name, find the active version and its files. Check the DAG (`grep -n <short_name> dag/*.yml`) for the current version, then collect:
- **Snapshot scripts** incl. any **real extraction helper** (e.g. `snapshots/wb/<v>/pip_api.py`, often much bigger than the 16-line `*.py` passthrough — find it with `ls snapshots/<ns>/<v>/`).
- **meadow / garden / grapher** `.py` steps for that version.
- **`.meta.yml`** metadata file(s).
- Pick **published chart slugs** (see step 4). Confirm scope with the user if the pipeline is large or has a legacy variant — they may want to exclude parts (e.g. "skip legacy metadata").

### 2. Scan code + metadata
Use the wrapper `scripts/dp_scan.sh OUTDIR SLUG <scan-args…>` (it sources the key, runs `scan … --json --all`, validates JSON, prints a one-line summary). Default model is Sonnet 4.6 — don't pass a model flag unless the user asked. **If the user asks for a specific model** (e.g. Opus 4.8), set `ANTHROPIC_MODEL` and the wrapper honors it — `export ANTHROPIC_MODEL=claude-opus-4-8` before the batch (confirm it took via the `model=…` field the wrapper prints). The CLI's own flags are `--thorough` (Opus 4.7) and `--fast` (Haiku 4.5); `ANTHROPIC_MODEL` overrides all. One scan per file:
```bash
scripts/dp_scan.sh ai/datapitfalls/raw garden_main etl/steps/data/garden/<ns>/<v>/<short>.py
scripts/dp_scan.sh ai/datapitfalls/raw meta_main  etl/steps/data/garden/<ns>/<v>/<short>.meta.yml
```
`.py/.sql/.r` scan as code; `.yml` falls through to text/code (fine). Run several files as a background batch — the big garden files take ~1–2 min each on Sonnet.

### 3. Capture + scan published charts
The Graphical-Gaffes / Design-Dangers / Biased-Baseline domains only fire on actual images. Pick 4–6 charts spanning the pipeline's pitfall-prone encodings (a map, a time-series line, a stacked/distribution chart, an inequality/ratio chart). Fetch each with `scripts/dp_fetch_chart.sh <slug> <outpath> [tab]` (GET on the grapher `.png` endpoint with a browser UA — **HEAD requests 404 behind Cloudflare**; it follows redirects and verifies the bytes are a PNG). Then scan each individually **and** all together (cross-chart pass catches inconsistent-scale/color across maps):
```bash
scripts/dp_fetch_chart.sh share-of-population-in-extreme-poverty ai/datapitfalls/charts/01_map.png
scripts/dp_scan.sh ai/datapitfalls/raw chart_01_map ai/datapitfalls/charts/01_map.png
scripts/dp_scan.sh ai/datapitfalls/raw charts_cross_all ai/datapitfalls/charts/01_map.png ai/datapitfalls/charts/02_line.png …
```
Note: a grapher PNG renders the chart's **default tab** and ignores `?tab=` for some charts — `Read` the PNG to confirm whether you got the map or the line, and grab the other view with `?tab=chart` / `?tab=map` (de-dupe by md5).

### 4. Scan a plain-English description of the method (`--text`)
This is what surfaces Epistemic/Statistical/Analytical pitfalls that pure code hides. Write 1–2 short prose files describing the *method* (PPP/vintage changes, mixing of measures, interpolation/nowcasting shown as data, gap-filling, hardcoded per-entity rules) to `ai/datapitfalls/text/`, then scan with `--text`:
```bash
scripts/dp_scan.sh ai/datapitfalls/raw text_method ai/datapitfalls/text/method.txt --text
```
0 findings on a well-caveated description is a *useful* signal: the stated method is sound; remaining gaps are in enforcement/visual encoding.

### 5. Aggregate + synthesize the report
Run `scripts/dp_aggregate.py ai/datapitfalls/raw` for counts (total, by severity, by domain, by rule) and a flagged list of likely **self-retracted false positives** (findings whose own explanation says "disregard"/"no actual"/"on review"). Then write **`ai/datapitfalls/<short_name>_report.md`** with:
- Run provenance (tool version, model, exact `npx` command, inputs, raw-JSON location) — make it reproducible.
- An at-a-glance severity × domain table and the most-repeated rules (deduped).
- **Errors first** (most actionable), then findings grouped by the 8 domains.
- A **"Guard?"** column on each: cross-reference the finding against the pipeline's existing `sanity_checks()` / `assert`s (read the garden step) — mark "already guarded", "partly", or "genuine gap".
- A short "genuine gaps worth a ticket" list (the net-new, actionable ones).
- A "reading the tool critically" section (see below).

### 6. (Optional) Deeper pass with `--thorough`
The tool returns a **top-N (~8) findings per scan**, so large files that hit 8 likely hide more. **Skip this step if the whole run already used Opus** via `ANTHROPIC_MODEL` (`--thorough` only swaps in Opus 4.7 and keeps the same 8-cap, so it adds nothing on top of an Opus-4.8 run — note that in the report instead). Otherwise re-run the biggest 1–2 files on Opus and record the diff (it tends to be *more selective*, not deeper):
```bash
scripts/dp_scan.sh ai/datapitfalls/raw garden_main_thorough etl/…/<short>.py --thorough
```
Fold genuine net-new findings in; **drop self-retracted ones**; on model disagreement (e.g. one flags an `error`, the other doesn't), keep the union and note the split.

## Scripts
- `scripts/dp_scan.sh OUTDIR SLUG <scan-args…>` — thin wrapper around `npx … datapitfalls scan … --json --all`. Sources `ANTHROPIC_API_KEY` from `$DP_ENV_FILE` (default `/Users/parriagadap/etl/.env`); honors `ANTHROPIC_MODEL`. Writes `OUTDIR/SLUG.json` + `.err`; prints `slug rc findings severities`.
- `scripts/dp_fetch_chart.sh SLUG OUTPATH [tab]` — download a grapher chart PNG (GET + browser UA + follow redirects + PNG-bytes check). Exits non-zero on 404/non-image.
- `scripts/dp_aggregate.py OUTDIR [--exclude slug …]` — tallies findings across `OUTDIR/*.json` and flags likely self-retracted false positives.

## Reading the tool critically (put a version of this in every report)
- **8-finding cap** — scans returning exactly ~8 findings are truncated top-N, not exhaustive. Use `--thorough` on the big files if depth matters.
- **`data-reality-gap` is the tool's reflex** — it fires on nearly every survey/estimate input. Fair framing, but it's one observation repeated, not N distinct problems.
- **Read the evidence, not the rule name** — the tool sometimes raises a rule and then retracts it in its own `explanation` ("…Disregard."). `dp_aggregate.py` flags these; don't count them.
- **Severity ≠ reality — check the `confidence` field.** A finding can be `severity: error` yet `confidence: low`, and on reading the evidence describe code that's already correct (e.g. `unweighted-rate-average` firing on a step that *does* `mean_weighted_by_population`). `dp_aggregate.py`'s retraction-marker heuristic won't catch these (it reported 0 self-retracted on the WDI run, yet 3 of 8 `error`s were false alarms). Triage every `error` against its `confidence` and its evidence before promoting it.
- **Chart findings are about rendering**, not the ETL code — several will be deliberate OWID conventions (shared OrRd ramp, map-tab default, 1990 start). Judge, don't auto-file.
- **`--text` findings only reflect the prose you wrote** — they audit your description of the method, not the code.
- **Don't suppress real findings; don't over-file framing ones.** The deliverable is a triaged report, not a raw dump.

## Output convention
Everything goes under a **per-pipeline** dir `ai/datapitfalls/<short_name>/`: `raw/*.json` (one per scan, kept), `charts/*.png` (scanned images), `text/*.txt` (prose inputs). The deliverable report sits one level up at `ai/datapitfalls/<short_name>_report.md` (kept at that depth so its relative links to repo source files resolve as `../../<path>`). Per-pipeline nesting keeps `dp_aggregate.py` from mixing two audits' findings — never share a flat `raw/`.
