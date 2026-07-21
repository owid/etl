---
name: adversarial-data-review
description: Adversarially review an ETL dataset's data and metadata for factual accuracy — verifies metadata claims against the producer's own documentation (fetched from the links in snapshot .dvc files and metadata texts) and cross-checks anomalous plus anchor values against independent sources online, to catch unit errors, wrong-year values, and hard-to-detect mistakes made by the source itself. Use when the user asks to "adversarially review a dataset", "fact-check this dataset", "verify the data against the source", "cross-check the values", or as the factual-accuracy step inside /update-dataset, /create-dataset, and /review-data-pr.
metadata:
  internal: true
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
- Jinja whitespace artifacts → `/check-metadata-spacing`
- Field-coverage / freshness / link-liveness audits → `/update-dataset` § 6c (that step checks the links *resolve*; this skill *reads what's behind them*)

Any rewrites you propose use American spelling.

## Inputs

- A step path `garden/<namespace>/<version>/<short_name>` (the `data://` URI form also works), a bare `<short_name>` — resolve via the DAG like `/update-dataset` does (`rg "/<short_name>:?$" dag/ -g "*.yml" | grep -v "^dag/archive"`, latest active version, ask the user if ambiguous).
- Optional: `--top N` (deep-review cap, default 10) or `--full` (deep-review every indicator regardless of chart usage).
- Also accepts a **single chart** (slug or id) as the target: scope the whole review to that chart's indicator(s) — verify every displayed country-category/value for the latest year, compare against the previous dataset version, and review the chart's own FAUST text as claims. Drafts count (find them in the staging DB; they have no production row).
- Precondition: the garden dataset is built locally (`.venv/bin/etlr data://garden/<ns>/<version>/<short_name> --private`; `PREFER_DOWNLOAD=1` is fine for already-published upstream deps). **Exception:** the `/create-snapshot` context runs Phase 0 only against the `.dvc` and the fetched docs — no meadow/garden step exists yet, so skip this precondition (and Steps 1, 3–5) there.

Scope by calling context:

In the workflow skills this review is an **optional, offered** step (it can consume many tokens — see the estimates in Step 1); when invoked from one of them, scope accordingly:

| Context | Scope |
|---|---|
| `/update-dataset` § 6c-bis (optional) | New/changed metadata text + newly added data values (latest wave/year); deep review = top-N + anomalies |
| `/create-dataset` Step 6b (optional) | Everything — all indicators (new datasets are small and have no charts yet) |
| `/create-snapshot` § 5 (optional) | Phase 0 only — verify the `.dvc` claims against the fetched producer docs (no built dataset yet, so no data cross-checks) |
| `/review-data-pr` § 10b | Only if the author ran it: verify outcomes and independently spot-check 2–3 findings and 2–3 anchor values |
| Standalone | Top-N + anomalies (or `--full`) |

## Step 1 — Prioritize indicators by chart views (heaviness control)

The deep per-indicator work (metadata claim review + online cross-checks) costs real time (~25–45 web calls on a typical dataset), so scope it: deep-review the **top N indicators (default 10) ranked by summed 365-day views of the charts that use them, plus every indicator the anomaly scan flags**. For a brand-new dataset with no charts, review all indicators.

**Never cap silently.** The report's Part 2 must list every skipped indicator with its rank — a truncated review that reads as complete is itself a factual error.

**Views rank prioritization; the DB defines coverage.** Analytics only sees *published* charts — when the task is "all charted indicators" (or the chart under review is a draft), inventory usage from the grapher/staging DB instead (`chart_dimensions` JOIN `variables` on catalogPath, no `publishedAt` filter), then rank the published subset by views.

**Map the ranking onto the NEW build before selecting.** Pre-merge, the ranked `catalog_path`s are the *old* version's — extract a version-independent identity (`<table>#<column>`, i.e. everything after the version segment) and join it onto the new garden/grapher build's actual table/column list. Old-charted identities missing from the new build are renames — resolve them via the indicator-upgrade mapping before ranking, or list them explicitly. New-build indicators absent from the ranking are uncharted (including newly added ones) — they are candidates for the anomaly-driven track and must appear in the reviewed-or-SKIPPED inventory, never silently dropped by the chart join.

```python
from etl.analytics.config import SEMANTIC_LAYER_SCHEMA as S  # tables must be schema-qualified for BigQuery
from etl.analytics.data import read_analytics  # Metabase; auto-falls back to analytics Datasette without creds

NS, SHORT = "<namespace>", "<short_name>"
ind = read_analytics(f"SELECT indicator_id, catalog_path FROM {S}.indicators WHERE catalog_path IS NOT NULL")
# Match namespace + short_name across ANY version — before the PR merges, live charts still point at the OLD version.
ind = ind[ind["catalog_path"].str.contains(f"/{NS}/") & ind["catalog_path"].str.contains(f"/{SHORT}/")]
cxi = read_analytics(f"SELECT chart_slug, indicator_id FROM {S}.charts_x_indicators")
charts = read_analytics(f"SELECT chart_slug, views_365d FROM {S}.charts").drop_duplicates("chart_slug")
usage = (
    # LEFT-join the views: a newly published or unvisited chart has no analytics row yet, and an
    # inner join would silently drop its indicators from the charted inventory (they rank at 0 instead).
    ind.merge(cxi, on="indicator_id").merge(charts, on="chart_slug", how="left")
    .groupby("catalog_path")
    .agg(n_charts=("chart_slug", "nunique"), views_365d=("views_365d", "sum"))
    .sort_values("views_365d", ascending=False)
)
```

If the analytics layer is entirely unreachable, fall back to the public grapher Datasette and rank by **number of charts** (it has no view counts):

```python
from etl.http import session  # OWID infra → tagged User-Agent

sql = (
    # NOTE: variables.catalogPath DOES carry the channel prefix ("grapher/<ns>/<version>/<short>/<table>#<col>");
    # it's datasets.catalogPath that is channel-less — don't mix the two conventions up.
    "SELECT v.catalogPath, COUNT(DISTINCT cd.chartId) AS n_charts "
    "FROM variables v JOIN chart_dimensions cd ON cd.variableId = v.id "
    "WHERE v.catalogPath LIKE 'grapher/<namespace>/%/<short_name>/%' GROUP BY 1 ORDER BY 2 DESC"
)
rows = session.get("https://datasette-public.owid.io/owid.json", params={"sql": sql}).json()["rows"]
```

## Step 2 — Phase 0: source verification (mandatory before any critique)

Establish what you're looking at before criticizing anything. Investigative, not adversarial.

1. **Collect the source's links.** From the snapshot `.dvc` (`url_main`, `url_download`, `license.url`) and every URL embedded in metadata texts and step files:
   ```bash
   rg --no-filename -No "https?://[^\"' )>]+" snapshots/<ns>/<version>/ etl/steps/data/{meadow,garden,grapher}/<ns>/<version>/
   ```
2. **Compare the source's file-modification dates/hashes against our snapshot's `date_accessed`/md5** (e.g. the OSF API lists `date_modified` and hashes per file). Producers replace files in place without bumping version labels — an unchanged version string proves nothing, and an in-place revision is the single highest-yield thing this phase can find. While at it, diff the source's **file inventory** against what we snapshot: a new companion file (pre-built index, summary table, construction script) is a new-indicator candidate that no within-file diff can surface — route it to the update workflow's "Surface new indicators" step rather than just cataloguing it.
3. **Fetch and READ the producer's own documentation** — methodology pages, indicator definitions, codebooks/data dictionaries, release notes. Not secondary commentary, not a blog post about the source: the source itself. Follow the links from `url_main` to the actual methodology document when the landing page is thin. Access escalation per repo convention: curl → WebFetch → Wayback Machine before treating a 4xx as real.
4. **Check for an existing `<short_name>.corrections.yml`** next to the garden step. Its entries are *known, already-handled* source errors — acknowledge them in Part 0 of the report; never re-flag them as new findings.
5. **Establish the pipeline.** For each metric under review: what does the source publish (exact indicator name, unit, definition, granularity, upstream data — official statistics, modeled, survey)? What does OWID add on top (read `description_processing`, the garden step code, and the corrections file)? Every later critique must state whose layer it targets.
   While reading, harvest the codebook's **worked examples as test vectors**: any country/value/date the documentation itself cites must match the data — a codebook example contradicting the shipped file is the strongest class of source error (provable entirely from the producer's own materials).
6. **Field-placement audit.** The `.dvc` `meta.origin.description` must carry *producer* content only; garden `description_processing` must carry *OWID* content only; `description_from_producer` must be **verbatim** producer text — diff it against the fetched docs (typography-only drift is fine, paraphrase is not). Beyond placement, each field must be factually consistent with what the docs actually say (units, scope, coverage, method).

**HARD RULE — proportionality.** The severity of any provenance or factual critique must be proportional to the depth of verification you achieved. If you read the documentation and confirmed a gap, make a strong claim. If the docs were unreachable after the full escalation, cap the language at "I was unable to verify … — worth checking before merge" and the severity at 🟢. Never assert an error you couldn't check.

## Step 3 — Phase 1: internal anomaly scan (local, cheap — runs before any web call)

Scan the built garden dataset for internal red flags. Paste-and-adapt sketch — adjust the key columns (extra dimensions like `sex`/`age` need to join the groupby keys), the thresholds, and which units are additive:

```python
from owid.catalog import Dataset
import numpy as np
import pandas as pd

ds = Dataset("data/garden/<ns>/<version>/<short_name>")
findings = []
for tname in ds.table_names:
    tb = ds.read(tname, safe_types=False)  # read() resets the index, so key columns are regular columns
    if tb.index.names != [None]:  # defensive: if keys sit in the index (e.g. the table came via ds[tname]), restore them
        tb = tb.reset_index()
    year_col = "year" if "year" in tb.columns else ("date" if "date" in tb.columns else None)
    has_country = "country" in tb.columns  # year-only tables exist (gravitational-wave counts), as do static ones (GWP factors)
    vals = [c for c in tb.columns if c not in ("country", year_col) and pd.api.types.is_numeric_dtype(tb[c])]
    for col in vals:
        unit = (tb[col].metadata.unit or "").lower()
        s = tb[(["country"] if has_country else []) + ([year_col] if year_col else []) + [col]].dropna(subset=[col])
        if s.empty:
            findings.append((tname, col, "EMPTY", "all-NaN column")); continue
        mx, mn = s[col].max(), s[col].min()
        # 1. Unit/magnitude sniffs
        if any(w in unit for w in ("%", "percent", "share")):
            if mx <= 1.5: findings.append((tname, col, "UNIT", f"unit is % but max={mx:.3g} — fraction stored?"))
            if mx > 150: findings.append((tname, col, "UNIT", f"% column max={mx:.3g} — can it exceed 100?"))
        if mn < 0 and any(w in unit for w in ("people", "number", "deaths", "tonnes", "count")):
            findings.append((tname, col, "SIGN", f"negative min={mn:.3g} in count-like unit"))
        if not has_country or year_col is None:
            continue  # year-only or static table: only the unit/magnitude sniffs apply; checks 2-6 need country + time
        # 2. Robust per-country outliers (median/MAD z-score; skip short series — MAD is unstable under ~8 points)
        g = s.groupby("country")[col]
        mad = g.transform(lambda x: (x - x.median()).abs().median()).replace(0, np.nan)
        z = ((s[col] - g.transform("median")) / mad).where(g.transform("count") >= 8)
        for _, r in s[z.abs() > 6].head(20).iterrows():
            findings.append((tname, col, "OUTLIER", f"{r['country']} {r[year_col]}: {r[col]:.4g} (|z|>6)"))
        # 3. Trend breaks with a unit-error signature (~×10/×100/×1000 jumps)
        ss = s.sort_values(["country", year_col])
        ratio = ss.groupby("country")[col].pct_change().add(1).abs()
        for _, r in ss[np.log10(ratio.replace(0, np.nan)).abs() >= 1].head(20).iterrows():
            findings.append((tname, col, "BREAK", f"{r['country']} {r[year_col]}: ≥×10 year-over-year jump"))
        # 4. Coverage drop in the most recent period
        cov = s.groupby(year_col)["country"].nunique()
        if len(cov) > 1 and cov.iloc[-1] < 0.7 * cov.iloc[-2]:
            findings.append((tname, col, "COVERAGE", f"{cov.index[-1]}: {cov.iloc[-1]} countries vs {cov.iloc[-2]}"))
        # 5. Suspicious constants: long runs of identical NON-ZERO values (source forward-fill?) —
        #    repeated zeroes are normal for sparse count/event indicators and must not count.
        runs = ss.groupby("country")[col].apply(lambda x: ((x == x.shift()) & (x != 0)).mean())
        for ctry in runs[runs > 0.5].index[:10]:
            findings.append((tname, col, "CONSTANT", f"{ctry}: >50% of series identical to previous period"))
        # 6. World vs sum of countries — ONLY for additive units (counts, tonnes, deaths; never rates/shares/indices)
        #    flag when |World − Σ countries| / World > 5% in a spot-checked year
```

The scan is a **candidate generator, not a verdict**. Review the raw `findings` yourself and discard the obviously legitimate ones (wars, pandemics, currency redenominations, real policy shocks) *with a stated reason each* before spending any web calls in Phase 2.

## Step 4 — Phase 2: independent online cross-check (anomaly-led + anchors)

This is the half that catches the *source's* mistakes — the ones invisible to every local check because our pipeline reproduces them faithfully.

**What to check:**

- Every anomaly that survived your Phase-1 triage. Cap the WebSearch effort at ~10 values; list anything beyond the cap as unchecked in Part 2.
- Fixed **anchors**, regardless of anomalies: the World total (if the dataset has one), 2–3 major or topic-relevant countries, the latest year, and one mid-series historical year.

**Independence rules (anti-circularity — read before searching).** An independent source is a *different producer measuring the same quantity* (WHO vs. IHME, IEA vs. Energy Institute, IMF vs. World Bank, UN WPP vs. a national statistics office), or the primary source the producer aggregates. **Never** count as independent: ourworldindata.org itself; sites that republish OWID (Wikipedia charts and infoboxes frequently cite us — check the citation); mirrors of the same producer (tradingeconomics and friends scrape WB/IMF); or the producer's own secondary pages.

**Procedure per value:** WebSearch the quantity + entity + year → open 1–2 authoritative hits with WebFetch → record source, value, and link in the Part 2 table. **Never cite a number straight from the search-results summary** — summaries blend several sources and lag living pages; every figure that reaches a finding, a PR body, or a producer question must be quoted from a page you actually opened (a stale search-summary count once shipped into a producer email as "21 of 32" when the opened page said 22, later 26).

**Measurement-artifact scrutiny (per source, not per value):** search `"<producer> completeness bias"`, `"<producer> coverage <region>"`, `"<indicator> revision history"` and read what comes back. When you flag a comparability problem, name the **specific mechanism** by which the data misleads (e.g. "death registration completeness below 60% in region X inflates apparent improvement"); a bare "comparisons should be made with care" is banned.

**Tolerance:** rounding, vintage/revision drift, and methodology gaps of a few percent are *not* findings. The targets are magnitude errors (×10/×100/×1000), wrong-year values, sign errors, entity mix-ups, and stale pre-revision values. Declaring a **confirmed source error requires ≥2 independent sources that agree with each other and disagree with ours** beyond methodology tolerance.

**Attribution before routing.** Before routing any confirmed bad value, read the raw snapshot (`from etl.snapshot import Snapshot; Snapshot("<ns>/<version>/<file>").read()` — `read()` picks the reader from the file's format; use the format-specific `read_csv`/`read_excel`/`read_json` only when auto-detection needs overriding) to determine where it entered: present in the source file → source error (corrections route); absent → our processing introduced it (trace snapshot → meadow → garden and fix the step).

## Step 5 — Phase 3: adversarial metadata review (top-N + anomalous indicators only)

Treat each prioritized indicator's user-facing text as a set of claims and attack them against the Phase-0 documentation:

- Does `unit` (and any `display.unit`/conversion) match the producer's stated unit? A `(mils)`/`(000)` marker in the source's column header demands a visible conversion in garden.
- Does the title / `description_short` overclaim scope — "global" when the source covers reporting countries only, "countries" when it's high-income countries?
- Does `description_key` state contested definitions as settled, or omit a caveat the producer's own docs (or your Phase-2 literature search) prominently state — coverage gaps, comparability breaks, denominator choices?
- Are causal or certainty words ("shows", "proves", "leads to", "drives") backed by the source's methodology, or do they smuggle in an interpretation?
- For categorical indicators built from label maps, list the distinct source labels and verify each maps **explicitly** — values routed to a fallback bucket ("unknown", "other") are silent misclassifications, because the fallback is an existing category and no validation fires. Recommend an `observed labels ⊆ map keys` assert where one is missing.
- For Jinja-templated metadata, spot-check the *rendered* text readers actually see: `Dataset("data/grapher/<ns>/<version>/<short_name>").read(t, load_data=False)[col].metadata`.

Lead with the concrete rewrite, not the objection. "Add a link" is a valid fix. Match the register and length of the original — prefer a word swap over an added clause.

## Routing findings

| Finding | Author flows (update/create/standalone) | Review flow |
|---|---|---|
| Metadata contradicts producer docs (unit/definition/scope; content in the wrong field per the `.dvc`-vs-`description_processing` split) | Edit `.meta.yml`/`.dvc`, re-run the step (`--grapher` for grapher channel) | 🔴/🟡 with quote + doc link |
| Value wrong in our output but correct in the raw snapshot | Fix the step code — never corrections.yml, never mask | 🔴 |
| Value confirmed wrong **at the source** (raw snapshot carries it; ≥2 independent sources agree against it) | Add `<short_name>.corrections.yml` next to the garden step + `tb = paths.apply_corrections(tb)` (format: `etl/data_corrections.py`); fill `reason`/`provider`/`status`, add an `expect` guard; tell the user to notify the producer and record the `reported:` date | 🔴 if confirmed and uncorrected |
| Suspicious but unconfirmed (independent sources disagree with each other, or methodology plausibly explains the gap) | "Verify manually" item in the report — do **not** add a correction | 🟡 |
| Docs/data unreachable after curl → WebFetch → Wayback | "Unable to verify — worth checking" (proportionality cap) | 🟢 |
| Producer-doc vs. shipped-file discrepancy | Preserve the data as shipped; flag for producer follow-up | 🟢 |

## Output report

Write to `ai/adversarial-review-<short_name>-<YYYY-MM-DD>.md`:

```
# Adversarial data review — <ns>/<version>/<short_name>

## TL;DR
(max 3 sentences: overall verdict + the one thing to fix first)

## Part 0 — Source verification
- Source(s) + documents accessed (URL, access status: read / paywalled / unreachable)
- What the source publishes (indicators, units, definitions, granularity, upstream data)
- What OWID adds (from description_processing + step code + corrections.yml)
- Field-placement check (.dvc description / description_processing / description_from_producer)
- Data-quality flags stated in the source's own docs
- Flags the docs SHOULD state but don't (from the artifact-literature search)
- Existing corrections.yml entries acknowledged

## Part 1 — Findings (numbered, ordered 🔴 → 🟡 → 🟢)
N. 🔴|🟡|🟢 [data-level|text-level] <one-line defect>
   Evidence: <quote or value + link>
   Fix: <concrete action + routing per the table above>
   Why: <one tight sentence>

## Part 2 — Cross-check appendix
- Values checked: indicator | entity | year | our value | independent value(s) | source link | verdict ✓/✗/~
  (mark each row as anchor or anomaly)
- SKIPPED indicators (name, views_365d rank, why skipped) — mandatory, never silent
- Anomalies beyond the WebSearch cap, listed as unchecked
```

Severity rubric (aligned with `/review-data-pr`): 🔴 = confirmed factual error (metadata contradicted by the producer's own docs, or a value confirmed wrong by ≥2 independent sources with snapshot-level attribution); 🟡 = likely issue needing confirmation; 🟢 = informational or unverifiable.

In author flows, apply the 🔴 fixes immediately (they're why the skill ran before commit); leave 🟡/🟢 as report items for the user to triage.

## Key constraints

- **Always fetch and search — never judge data plausibility from memory or training data.** The whole point is a fresh look at what the source and the wider literature actually say today.
- Establish source-vs-OWID attribution (Phase 0) before critiquing either layer; every finding names whose layer it targets.
- Severity proportional to verification depth — the HARD RULE in Step 2.
- Don't manufacture objections. A clean report is a valid outcome; if uncertain whether something is wrong, say "verify this" rather than asserting it.
- Separate data-level from text-level findings — a legitimate metric can sit under overclaiming text, and carefully hedged text can sit on top of broken data.
- Methodology differences are not errors. Name the specific mechanism before calling a mismatch an error.
- Factual accuracy only — no style/typo/spacing duplication (see Scope).
- corrections.yml `override` values on categorical columns must come from the source's *current* vocabulary — assigning a retired label fails with `Cannot setitem on a Categorical`; choose the current-vocabulary value that yields the same published output.
- No persistent files beyond the `ai/` report — plus, in author flows, the metadata/corrections edits themselves. Ad-hoc analysis scripts run from the session and are not committed.
