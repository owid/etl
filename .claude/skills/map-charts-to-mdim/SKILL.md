---
name: map-charts-to-mdim
description: >-
  Propose redirects from (soon-to-sunset) grapher charts to the matching views of
  published MDIMs. Selects charts by tag, slug list, or dataset id; matches each
  chart to MDIM views by indicator IDs (auto-discovering ALL published MDIMs as
  targets by default); writes a proposal CSV, one redirect payload JSON per source
  chart, a side-by-side review HTML, and the `;`-delimited CSV that grapher's
  `createMultiDimRedirectsFromCsv` CLI consumes. Also audits every article, explorer,
  narrative chart, data insight and static viz that links or embeds each chart, with
  the replacement URL. Only charts with a confident match get a proposed redirect —
  the rest are reported. Creating the redirects is a separate, human-run step.
  Trigger when the user says "map charts tagged <X> to mdims", "propose chart -> MDIM
  redirects", "which charts are covered by the new multidim", "where do I replace the
  embedded charts", "we're sunsetting these charts in favour of the MDIM", or similar.
metadata:
  internal: true
---

# Map charts to MDIM views (redirect proposal)

When individual grapher charts are being retired in favor of MDIMs, each chart
needs a redirect to the equivalent MDIM view. Unlike explorers (see
`map-explorer-to-mdim`), no human mapping rules are needed: a chart is a single
view, and the match is computed from indicator IDs.

**This skill proposes and validates; it never writes.** Redirects are created by
a human running grapher's `createMultiDimRedirectsFromCsv` CLI, which also
unpublishes the source charts in the same transaction. Everything here builds
toward that one command and proves in advance that it will succeed.

The target mechanism is the `multi_dim_redirects` table (`source` path →
`multiDimId` + `viewConfigId`). Grapher's admin API can write it too, but **not
for these charts**: it rejects any source that already has an old slug pointing
at it, and its bulk endpoint accepts explorer sources only. The CLI
(`owid-grapher/devTools/createMultiDimRedirectsFromCsv.ts`) exists precisely for
the chart case.

## Inputs

Exactly one chart selection:

- `--tag "<tags.name>"` — published charts with that **exact** tag (the script
  prints nearby tag names, e.g. `Economic Inequality` vs `Economic Inequality by
  Gender`, so the user can widen the selection knowingly).
- `--slugs a,b,c` or `--slugs @file` — explicit chart slugs.
- `--dataset-id N` — published charts using any variable of that dataset.

Targets: **all published MDIMs by default** (auto-discovery); restrict with
repeatable `--mdim <catalogPath>` (as in `multi_dim_data_pages.catalogPath`).

## DB access (confirm this *before* running)

Charts and MDIMs are read from the grapher DB via `OWID_ENV` — read-only is
enough for the proposal phase. Point `OWID_ENV` at a DB that has both:

1. **Staging DB**: prefix with `STAGING=1` (the current branch's
   `staging-site-<branch>` DB; `STAGING=<name>` for another branch's). Being
   checked out on the branch is NOT enough by itself — without the prefix,
   `OWID_ENV` silently points at your local dev DB, which also passes the
   connection preflight.
2. **Production, read-only, via an env file**: prefix with
   `ENV_FILE=<prod creds file> DATA_API_ENV=production`. Don't assume the file
   name — check what exists (`ls .env*`); on some machines it's `.env.prod`, on
   others `.env.live`.
3. **Some other credentials file**: `ENV_FILE=<their file>`.

If no suitable env file exists and there's no staging server to point at, **stop
and ask the user** — never hardcode credentials.

The extractor prints the environment it resolved (`grapher DB: ...`) — check
that line matches what you intended before trusting the output. All links in
the artifacts (and the review HTML panes) default to that environment's site,
so a staging extraction is reviewed against staging; `--host` overrides.

```bash
ls .env* 2>/dev/null
ENV_FILE=<file> DATA_API_ENV=production .venv/bin/python -c \
  "from etl.config import OWID_ENV; print('DB OK:', OWID_ENV.read_sql('SELECT 1 AS x').iloc[0,0])"
```

## Workflow

### 1. Extract + match

```bash
ENV_FILE=<prod creds> DATA_API_ENV=production .venv/bin/python \
  .claude/skills/map-charts-to-mdim/scripts/extract_and_match.py \
  --tag "Economic Inequality" \
  --out ai/economic-inequality-charts-mdim-mapping
```

Writes into `--out`:

| file | contents |
|---|---|
| `charts.csv` | selected charts + their indicator slots |
| `multidim_views.csv` | every candidate MDIM view (row ids `A1…`, `B1…`) |
| `mapping_proposal.csv` | one row per chart: quality, target view, clickable URLs |
| `mapping.json` | combined machine record — confident, conflict-free matches only |
| `payloads/<chart_slug>.json` | **one JSON per source chart** — the copy-paste handoff unit |
| `redirects_for_cli.csv` | **the apply input** — `;`-delimited `source;target` for the grapher CLI |
| `migration_log_template.csv` | `(old_slug, mdim_slug, view_id, cutover_date)` — stamp the date at apply time |
| `unmatched.md` | everything not proposed, with candidates/near-miss detail |
| `_sources.json` | machine record of run inputs (don't hand-edit) |

**Handoff convention: one source per JSON.** Like the explorer→MDIM redirect
deliverables (`ai/explorer-mdim-redirects/`, one `admin_bulk_payload.json` per
explorer), each JSON handed over must describe exactly ONE source page. For
charts that means one file per chart, in `payloads/`. The combined
`mapping.json` is the machine record consumed by `apply_redirects.py` — don't
hand that one over as a copy-paste payload.

**Matching**: a chart matches a view when the y-variable-ID sets are equal AND
x/size/color agree (absent == absent). Several matching views → tiebreak on chart
type; still ambiguous → reported, never guessed. Indicator-set subset/superset →
`near_miss`, reported only. Quality labels:

- `exact` — one view matches (possibly via the `chart_type` tiebreak — check the
  `tiebreak` column).
- `forced` / `skipped` — set by `overrides.csv` (below).
- `ambiguous` — several views match; needs a human pick.
- `near_miss` — overlapping but unequal indicator sets; the diff is spelled out.
- `none` — no view shares the chart's indicators. **An accepted outcome** — only
  matched charts get redirects; don't force the rest.

**Conflicts vs CLI-required.** Matched charts are checked (read-only SQL) against
the same conditions the apply CLI validates. Two outcomes, and the distinction
matters:

- **`cli_required`** — the chart has old slugs redirecting into it, or site
  redirects pointing at it. **Not a blocker**: the CLI migrates both. (The admin
  API rejects exactly these as redirect chains, which is why it isn't the apply
  path.) The old slugs are listed in the proposal and carried into
  `payloads/*.json` as `oldSlugs`.
- **`conflict`** — a genuine blocker that would abort the CLI's transaction: the
  source is already a redirect source, the chart's own slug is itself an old
  slug, the target MDIM's `/grapher/<slug>` is a redirect source, a self-redirect,
  or one of the old slugs is already a redirect source elsewhere. These land in
  `mapping.json → conflicts[]` and are kept out of the CSV.

Charts already redirected to the same target are counted as `already_done` — no
redirect to create, but they are still published (the extractor only selects
published charts), so they still shadow their redirect and still need the CLI to
unpublish them.

Many charts → one view is legitimate (e.g. a line chart and its map twin) —
surfaced via `shared_target_chart_ids`, never collapsed.

### 2. Review

Side-by-side HTML (chart left, proposed MDIM view right; approve/flag with notes,
decisions persist in the browser):

```bash
.venv/bin/python .claude/skills/map-charts-to-mdim/scripts/build_review.py \
  --mapping-dir ai/economic-inequality-charts-mdim-mapping
```

Rows without a target show their candidates / near-miss diff instead of an
iframe, so ambiguous cases can be triaged into `overrides.csv` from the same UI.
The proposal CSV also works standalone in a spreadsheet (URLs are clickable).

When the review is done, export the decisions (⬇ JSON or ⬇ CSV) — the apply
step consumes that file so flagged charts are excluded.

### 3. Overrides + re-run (only if needed)

For ambiguous rows (or to force/suppress a match), write
`<out>/overrides.csv` — it is **never overwritten** by re-runs:

```csv
chart_id,action,note
1234,SKIP,keep this chart live
5678,poverty_inequality/latest/gini#gini|metric=gini__welfare=disposable,picked over the WID twin
9012,01981234-abcd-7def-8123-456789abcdef,forced by view_config_id
```

Then re-run step 1 (same command) — overridden rows become `forced`/`skipped`
and `mapping.json` regenerates.

### 4. Audit what references the charts (ALWAYS offer; run when the user says yes)

A redirect only rescues plain hyperlinks. Anything that **embeds** the chart —
explorers, narrative charts, data insights, static viz, article chart blocks —
resolves it by id or slug and keeps rendering the old config, so it breaks when
the source chart is unpublished (which the apply step always does).

```bash
ENV_FILE=<prod creds> DATA_API_ENV=production .venv/bin/python \
  .claude/skills/map-charts-to-mdim/scripts/audit_references.py \
  --mapping ai/economic-inequality-charts-mdim-mapping
```

The sweep itself is the shared `find-chart-references` skill; this script is the
redirect-specific consumer that adds the replacement URLs. It writes
`references.csv` + `references.md`, one row per reference. Severity: **🔴**
embed — the surface renders the chart's own config, so the redirect does not fix
it and it breaks on unpublish; migrate before applying · **🟡** hyperlink (the
301 covers it, update the href anyway) or key-chart slot (re-tag the MDIM) ·
**ℹ️** unpublished/draft.

Pure SQL, so read-only credentials are enough. It sweeps both the current slug
and every old slug that reaches the chart — references written before a rename
point at the old one.

**Watch for the param-collision warning.** Grapher merges the incoming URL's
params *over* the redirect target's, so a link carrying `?metric=…` overrides an
MDIM dimension of the same name and lands the reader on the wrong view. The
audit flags those explicitly.

Mention this step every time (like `/update-dataset` step 7); running it is the
user's call, since a wide sweep costs tokens.

**Narrative charts are a known dead end.** They keep rendering (the config is
fetched by UUID), but "Explore the data" points at the parent chart's slug and
survives only via the 301 — and there is no API to repoint one, so a real fix is
raw SQL by a Grapher dev. Report them; don't try to fix them here. Drafts to
escalate the gap live in `ai/narrative-charts-slack-post.md` and
`ai/narrative-charts-grapher-issue.md`.

### 5. Apply — the grapher CLI (GATED, production only)

**This skill never creates redirects.** Applying is `yarn
createMultiDimRedirectsFromCsv` in owid-grapher, run by a human. The skill's job
is to produce a CSV that survives it and to prove, beforehand, that it will.

First, preflight (read-only, safe to run any time):

```bash
ENV_FILE=<prod creds> DATA_API_ENV=production .venv/bin/python \
  .claude/skills/map-charts-to-mdim/scripts/preflight.py \
  --mapping ai/economic-inequality-charts-mdim-mapping \
  --decisions ai/economic_inequality_chart_mdim_review.json
```

It re-runs every validation the CLI performs, against the live DB, and re-checks
each source chart's slug + config MD5 (an edited or deleted chart comes back
`STALE`). Statuses: `OK` / `BLOCKER` / `EXISTS` / `DIFFERS` / `GONE` / `STALE`.
Non-zero exit means **do not run the CLI yet** — it runs a single transaction, so
one bad row aborts the entire migration. Pass `--decisions` whenever a review
happened; flagged charts are excluded (remove them from the CSV, or mark them
`SKIP` in `overrides.csv` and re-run step 1).

Then, from the **owid-grapher** repo:

```bash
yarn createMultiDimRedirectsFromCsv /abs/path/to/redirects_for_cli.csv --dry-run
```

What the CLI does, in one transaction: creates each `multi_dim_redirects` row,
**migrates every old `chart_slug_redirects` alias** into a redirect aimed
straight at the MDIM view (one hop, no chain), and **unpublishes each source
chart**. `--dry-run` rolls all of it back and skips unpublishing entirely.

Facts to surface to the user before the real run:

- **Production only.** Redirect tables never sync staging→prod, and a staging
  rehearsal leaves prod with the old charts still published and no redirects —
  the migration hasn't half-happened, it hasn't happened.
- **The CLI has no `--host`, no `--env`, and no production guard.** It reads
  `GRAPHER_DB_*` from owid-grapher's `.env` (or `$PRIMARY_ENV_FILE`). Whichever
  DB that points at is the one it writes.
- **Unpublishing is mandatory, not optional.** A grapher redirect is only
  consulted when the URL 404s, so a redirect over a still-published chart never
  fires. The CLI handles it; don't treat it as a separate decision.
- **Never hand-unpublish a source chart first.** Unpublishing deletes that
  chart's `chart_slug_redirects` rows, so its old slugs become hard 404s. The
  CLI's ordering (flatten aliases, then unpublish) is what preserves them.
- **The admin API is not an alternative for these charts.** It rejects any
  source with an existing slug redirect as a redirect chain, and its bulk
  endpoint only accepts explorer sources.
- **`chart_slug_redirects.target_query_param` is lost** in the flatten —
  `old-slug → new-slug?tab=map` becomes `old-slug → mdim?<view dims>`. The
  proposal flags which aliases carry one.
- **Check with the Grapher team before a large run.** The CLI was written for
  manual use by a Grapher dev and may lag recent redirect changes.

Right after the run, stamp `cutover_date` in `migration_log_template.csv` and
keep it. Analytics cannot reconstruct it later: `prod_semantic.redirects` holds
no `multi_dim_redirects` rows, and once a chart stops being published its whole
view history resolves to `chart_id = NULL` — retroactively, not just from the
cutover. To rebuild a continuous series, query `grapher_views_detailed` on the
raw `grapher` column for the pre-cutover period, the MDIM slug after it, and
union the two. Expect a ~1 week tail of real views on the old URL (redirects
fire only on 404 and Cloudflare serves the cached page meanwhile), and note that
query params are stripped in analytics, so every MDIM dimension collapses into
one slug.

## Gotchas

- **The CSV format is unforgiving.** `;`-delimited, exactly two columns, a header
  tolerated **only** on line 1, no comment lines anywhere, no duplicate sources,
  both fields must start with `/`. Any violation aborts the CLI's whole run. The
  extractor validates its own output and `preflight.py` re-validates it.
- **Never put query params on the source.** They pass the CLI's regex and get
  stored verbatim, but serving matches the bare path — so the redirect would
  simply never fire.
- **Targets carry every dimension or none.** The CLI resolves the target query
  string to exactly one view; a partial dimension spec matches several and
  throws. Our targets always carry the full dict, so don't hand-trim them.
- `viewConfigId` (a `chart_configs` UUID, = `views[].fullConfigId`) is what
  grapher validates a redirect against; the human-readable `view_id`
  (`dim=choice__dim=choice`, key-sorted) is for URLs and display. Both are in
  every artifact.
- Stored MDIM configs mostly carry `{id, catalogPath}` per indicator, but older
  ones may have catalogPath-only or bare entries — the extractor normalizes all
  shapes and batch-resolves against `variables`.
- Charts with no slug or no y indicators are excluded/reported, not guessed.
- A NULL `chart_type` means map-only (`chartTypes: []`); the generated column
  defaults to `LineChart` when the config omits `chartTypes` entirely, so
  NULL == NULL is a meaningful tiebreak, not a wildcard.
- Views without `fullConfigId` can't be redirect targets and are excluded from
  the pool (warned). Same for views with an indicator entry that can't be
  resolved to a variable id, and for views with several indicators in one
  x/size/color slot (a chart holds one per slot) — matching on a truncated
  indicator set could hit the wrong chart.
- Re-runs regenerate everything except `overrides.csv`; `mapping.json` is
  derived — never hand-edit it.
- Don't `ORDER BY` in SQL that selects `multi_dim_data_pages.config` — the
  multi-MB JSON travels with each sorted row and blows the server sort buffer
  (MySQL error 1038). Sort client-side (already handled in the extractor).
- A choice slug that looks like an artifact (e.g. `period=nan` in
  incomes-across-distribution-lis) can be **real** in the published config —
  the target URL works because that IS the dimension value. Don't "fix" it in
  the mapping; if it bothers anyone, that's an MDim-authoring fix, done
  separately (a redirect created before the fix would break when the slug
  changes).

## Lessons

After a real run, fold anything the matcher or these docs got wrong back into
this SKILL.md (and check whether the sibling `map-explorer-to-mdim` /
`review-explorer-mdim-mapping` skills need the same fix).
