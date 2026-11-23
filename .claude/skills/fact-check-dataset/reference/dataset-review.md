# Reviewing a dataset

The procedure behind the dataset contexts of [SKILL.md](../SKILL.md). `prepare.py` has already done the mechanical part; this file says what each lane judges and what its result looks like. A single-agent context (claims, snapshot) follows the *Claims lane* within its scope.

## What the dossier gives you, and what is still yours

| Dossier section | Done by the script | Still yours |
|---|---|---|
| Pipeline, Origins | the chain from the DAG, every `.dvc` origin field | read the garden step code where a claim depends on what OWID does |
| Producer documentation | every link in the `.dvc`, the metadata and the step code, plus documents linked from the dataset page, saved as text in `docs/` | decide whether the set is complete and current; find and fetch what is missing |
| Source files, checked live | the bytes at `url_download` against the snapshot's md5 and size | the source's file inventory; snapshots marked `by-hand` or `too-large` |
| Corrections already in place | the entries of `<short_name>.corrections.yml` | acknowledge them in Part 0; never re-flag them |
| Metadata as readers see it | `metadata.md` (rendered, Jinja resolved) and `metadata_changes.diff` | the claims review |
| Indicators for the deep review | `ranking.csv`: views, rank, anomaly flags, selected or SKIPPED with a reason | override with `--indicators`, `--top`, `--full` when the selection misses the point of the review |
| Anomaly candidates | `anomalies.csv` | the triage: which are real |
| Details on Demand | `dods.md` | hold each relevant DoD to the documentation |
| Earlier reviews | their paths | reuse what they established about the source |

## How indicators are selected

The deep per-indicator work costs real time, so it is scoped: the **top N indicators (default 10) ranked by summed 365-day views of the charts that use them, plus the indicators the anomaly scan flags** (up to 20 more, most-viewed first; `--max-flagged` changes it). A brand-new dataset with no charts and at most 20 indicators is reviewed whole; a larger one needs the main indicators named with `--indicators`.

- **Never cap silently.** `ranking.csv` holds every indicator with its rank and, when it is skipped, why. `assemble_report.py` copies the SKIPPED list into Part 2; a single-agent report lists it by hand.
- **Views rank prioritization; the DB defines coverage.** Analytics only sees *published* charts. When the task is "all charted indicators", or the chart under review is a draft, inventory usage from the grapher or staging DB instead (`chart_dimensions` JOIN `variables` on catalogPath, no `publishedAt` filter), then rank the published subset by views.
- **The ranking is mapped onto the new build.** Before a PR merges, the charted catalog paths are the *old* version's, so the script joins on the version-independent identity `<table>#<column>`. Identities charted but missing from the new build are renames: resolve them via the indicator-upgrade mapping, or list them explicitly. New-build indicators absent from the ranking are uncharted (newly added ones included): they stay in the reviewed-or-SKIPPED inventory and are candidates for the anomaly track.
- **A "no charts use this indicator" clearance goes stale the moment charts are remapped.** Blast-radius checks are version-scoped; a chart stranded on an older version won't match a query filtered to the version you're updating, and will silently come into scope once the stranded-chart sweep runs. Re-run any such check after the indicator upgrade before relying on it.

## Claims lane

Establish what you're looking at before criticizing anything: the first half is investigative, not adversarial.

### Source verification (Part 0)

1. **The source's links.** The dossier's documentation table is the collection. Add what it lacks: a link the script could not read, or documentation not linked from the `.dvc`, the metadata or the dataset page.
2. **File dates and hashes.** Read *Source files, checked live*. A `changed` status means the producer replaced the file in place: an unchanged version string proves nothing, and an in-place revision is the single highest-yield thing this phase can find. For `by-hand` and `too-large`, compare the source's file-modification dates or hashes yourself against the snapshot's `date_accessed` and md5 (the OSF API lists `date_modified` and hashes per file). While at it, diff the source's **file inventory** against what we snapshot: a new companion file (pre-built index, summary table, construction script) is a new-indicator candidate that no within-file diff can surface — route it to the update workflow's "Surface new indicators" step rather than just cataloguing it.
3. **READ the producer's own documentation** — methodology pages, indicator definitions, codebooks/data dictionaries, release notes. Not secondary commentary, not a blog post about the source: the source itself. Follow the links from `url_main` to the actual methodology document when the landing page is thin. Cover the sources in `.claude/rules/producer-documentation.md` (dataset page, methodology, indicator definitions, codebook, main report, what's new) and confirm each is the edition matching the data. List any that are missing, insufficient or inaccessible in Part 0 for the caller to raise with the user.
4. **Establish the pipeline.** For each metric under review: what does the source publish (exact indicator name, unit, definition, granularity, upstream data — official statistics, modeled, survey)? What does OWID add on top (read `description_processing`, the garden step code, and the corrections file)? Every later critique must state whose layer it targets.
   While reading, harvest the codebook's **worked examples as test vectors**: any country/value/date the documentation itself cites must match the data — a codebook example contradicting the shipped file is the strongest class of source error (provable entirely from the producer's own materials). Pass each one to the values lane in your notes when you cannot check it against the data yourself.
5. **Field-placement audit.** The `.dvc` `meta.origin.description` must carry *producer* content only; garden `description_processing` must carry *OWID* content only; `description_from_producer` must be **verbatim** producer text — diff it against the saved docs (typography-only drift is fine, paraphrase is not). Beyond placement, each field must be factually consistent with what the docs actually say (units, scope, coverage, method). For each text that is wrong, stale or missing a caveat the docs give, propose a rewrite citing the passage it rests on.
6. **Measurement-artifact scrutiny (per source, not per value).** Search `"<producer> completeness bias"`, `"<producer> coverage <region>"`, `"<indicator> revision history"`, all in one message, and read what comes back. When you flag a comparability problem, name the **specific mechanism** by which the data misleads (e.g. "death registration completeness below 60% in region X inflates apparent improvement"); a bare "comparisons should be made with care" is banned.

### Adversarial metadata review

For the selected indicators (in an update: the new and changed text in `metadata_changes.diff` first), treat the user-facing text as a set of claims and attack them against the documentation:

- Does `unit` (and any `display.unit`/conversion) match the producer's stated unit? A `(mils)`/`(000)` marker in the source's column header demands a visible conversion in garden.
- Does the title / `description_short` overclaim scope — "global" when the source covers reporting countries only, "countries" when it's high-income countries?
- Does `description_key` state contested definitions as settled, or omit a caveat the producer's own docs (or the artifact search above) prominently state — coverage gaps, comparability breaks, denominator choices?
- Are causal or certainty words ("shows", "proves", "leads to", "drives") backed by the source's methodology, or do they smuggle in an interpretation?
- For categorical indicators built from label maps, list the distinct source labels and verify each maps **explicitly** — values routed to a fallback bucket ("unknown", "other") are silent misclassifications, because the fallback is an existing category and no validation fires. Recommend an `observed labels ⊆ map keys` assert where one is missing.
- Read the *rendered* text: `metadata.md` holds what readers actually see, with the Jinja resolved.
- **Attack the DoDs too.** `dods.md` lists every Details on Demand (DoD) linked with `#dod:` from the dataset's indicator metadata, its charts and narrative charts, the ETL viz configs, and the articles and data insights that show those charts, with each DoD's text and any link to a missing DoD. It reads production; `scripts/collect_dods.py <ns>/<version>/<short_name> --staging <branch>`, run the way SKILL.md runs `prepare.py` (`uv run ... --with duckdb`), reads a staging server instead, to re-check a DoD edited there. Hold each DoD to the documentation like any metadata: a DoD lives in the grapher DB, so no PR diff shows it, yet it is the definition readers get on hover. A DoD linked only from an article can be off-topic (articles link many topics); check it only when it defines something the dataset measures. Fix one as `.claude/rules/producer-documentation.md` says.

Lead with the concrete rewrite, not the objection. "Add a link" is a valid fix. Match the register and length of the original — prefer a word swap over an added clause.

### Result

`lane_claims.result.md` in the dossier directory, with exactly these headings (a single-agent context writes the same content straight into the report):

```
## Part 0 — Source verification
- Source(s) + documents accessed (URL, edition, fetch date, access status: read / paywalled / unreachable)
- What the source publishes (indicators, units, definitions, granularity, upstream data)
- What OWID adds (from description_processing + step code + corrections.yml)
- Field-placement check (.dvc description / description_processing / description_from_producer)
- Source files: unchanged, changed in place, or not checked (and why)
- Data-quality flags stated in the source's own docs
- Flags the docs SHOULD state but don't (from the artifact search)
- Existing corrections.yml entries acknowledged
- DoDs linked: each one checked, or why not

## Findings
N. 🔴|🟡|🟢 [text-level|data-level] <one-line defect>
   Evidence: <quote + document and page or section>
   Fix: <concrete rewrite or action + routing per the table in SKILL.md>
   Why: <one tight sentence>

## Notes for the other lane
(test vectors from the documentation to check against the data; units or definitions the values lane must use
when comparing with other sources; or "none")
```

## Values lane

This is the half that catches the *source's* mistakes.

### Triage the anomaly candidates

`anomalies.csv` is a **candidate generator, not a verdict**. Review the candidates of the selected indicators and discard the obviously legitimate ones (wars, pandemics, currency redenominations, real policy shocks) *with a stated reason each* before spending any web calls. A `COVERAGE` hit in the latest year where the aggregates (World, regions, income groups) still have values but most countries don't is never discarded: the aggregates may be the producer's estimates or nowcasts, or the country rows may just come out later or be withheld, so require an estimate label on the metadata and on anything quoting that year only when the producer's methodology confirms one.

The scan's thresholds are constants at the top of `scripts/anomaly_scan.py`; run it with `--columns` or `--entities` to look at one indicator or country alone. Its `WORLD_SUM` flag applies only to additive units (counts, tonnes, deaths; never rates, shares or indices).

### What to cross-check online

- **Our output against the producer's own figures**, first, because it needs no search: for the newly added period (and the anchors below), compare our values with what the producer publishes, in the saved documentation or the raw snapshot. A difference there entered in our processing.
- Every anomaly that survived your triage. Cap the WebSearch effort at ~10 values; list anything beyond the cap under *Anomalies not checked*.
- Fixed **anchors**, regardless of anomalies: the World total (if the dataset has one), 2–3 major or topic-relevant countries, the latest year, and one mid-series historical year.
- **One-year shifts in newly added years**, for sources that compile national-office figures (OECD, Eurostat, UN agencies). Compare the new years for 1–2 countries with the national office's own figures: our value for year Y matching its figure for Y−1 means the series is shifted.

**Independence rules (anti-circularity — read before searching).** An independent source is a *different producer measuring the same quantity* (WHO vs. IHME, IEA vs. Energy Institute, IMF vs. World Bank, UN WPP vs. a national statistics office), or the primary source the producer aggregates. **Never** count as independent: ourworldindata.org itself; sites that republish OWID (Wikipedia charts and infoboxes frequently cite us — check the citation); mirrors of the same producer (tradingeconomics and friends scrape WB/IMF); or the producer's own secondary pages.

**Procedure.** Write down every value to check first, then send all the searches in one message (quantity + entity + year each), then open 1–2 authoritative hits per value, all in one message. Record source, value and link in the *Values checked* table. **Never cite a number straight from the search-results summary** — summaries blend several sources and lag living pages; every figure that reaches a finding, a PR body, or a producer question must be quoted from a page you actually opened.

A mismatch beyond the Tolerance in SKILL.md, an accounting identity that does not hold, or two rows whose values match under swapped names: go to [source-errors.md](source-errors.md) before calling it an error or routing it.

### Result

`lane_values.result.md` in the dossier directory, with exactly these headings:

```
## Findings
N. 🔴|🟡|🟢 [data-level] <one-line defect>
   Evidence: <our value, the independent values, links>
   Fix: <concrete action + routing per the table in SKILL.md>
   Why: <one tight sentence>

## Values checked
| indicator | entity | year | our value | independent value(s) | source link | verdict ✓/✗/~ | anchor or anomaly |

## Anomalies not checked
(candidates discarded in triage, with the reason each; candidates beyond the WebSearch cap; or "none")

## Notes for the other lane
(a unit or definition the metadata gets wrong, seen while comparing; or "none")
```

## The report

`assemble_report.py <dossier> --reconciled` writes `ai/adversarial-review-<short_name>-<YYYY-MM-DD>.md` from the two results:

```
# Adversarial data review — <ns>/<version>/<short_name>

## TL;DR
(max 3 sentences: overall verdict + the one thing to fix first; pass it with --tldr)

## Part 0 — Source verification        ← the claims lane's Part 0

## Part 1 — Findings                    ← both lanes, numbered, ordered 🔴 → 🟡 → 🟢

## Part 2 — Cross-check appendix
### Values checked                      ← the values lane's table
### SKIPPED indicators                  ← from ranking.csv: name, rank, why — mandatory, never silent
### Anomalies not checked               ← the values lane's list
```

It refuses to write the report when a result or a heading is missing, and when a lane left notes for the other that the caller has not settled. Settling a note means deciding whether it changes a finding in the other result, and editing that result if it does. A finding both lanes report (the same defect seen from the text and from the data) goes in once: delete it from one result, and add that lane's evidence to the other if it adds something.

A single-agent context writes the same file by hand, and says in Part 2 which checks its context leaves out (a claims-only review has no *Values checked* table).
