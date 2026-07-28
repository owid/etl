---
name: map-charts-to-mdim
description: >-
  Propose redirects from (soon-to-sunset) grapher charts to the matching views of
  published MDIMs. Selects charts by tag, slug list, or dataset id; matches each
  chart to MDIM views by indicator IDs (auto-discovering ALL published MDIMs as
  targets by default); writes a proposal CSV + redirect payload JSON + side-by-side
  review HTML. Only charts with a confident match get a proposed redirect — the rest
  are reported. Applying redirects (grapher's multi_dim_redirects) is a separate,
  gated step. Trigger when the user says "map charts tagged <X> to mdims", "propose
  chart -> MDIM redirects", "which charts are covered by the new multidim", "we're
  sunsetting these charts in favour of the MDIM", or similar.
metadata:
  internal: true
---

# Map charts to MDIM views (redirect proposal)

When individual grapher charts are being retired in favor of MDIMs, each chart
needs a redirect to the equivalent MDIM view. Unlike explorers (see
`map-explorer-to-mdim`), no human mapping rules are needed: a chart is a single
view, and the match is computed from indicator IDs. This skill produces a
**proposal for human review** — creating the redirects is a separate, explicitly
gated step, because going live also implies unpublishing the source charts.

Grapher has a purpose-built mechanism for exactly this: the `multi_dim_redirects`
table (`source` path → `multiDimId` + `viewConfigId`), served by admin endpoints
(`POST /multi-dims/:id/redirects`). Reference implementation:
`owid-grapher/devTools/createMultiDimRedirectsFromCsv.ts`.

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
| `mapping.json` | combined redirect payload — confident, conflict-free matches only |
| `payloads/<chart_slug>.json` | **one JSON per source chart** — the copy-paste handoff unit |
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

**Conflicts**: matched charts are also checked (read-only SQL, mirroring
grapher's `validateMultiDimRedirect`) against existing `multi_dim_redirects`,
site `redirects`, and `chart_slug_redirects` chains. A common one: an old chart
with **incoming chart_slug_redirects** — the API hard-rejects that as a chain,
so it's surfaced now, not at apply time. Conflicted matches land in
`mapping.json → conflicts[]`, not in the POST-able `redirects[]`. Charts already
redirected to the same target are counted as `already_done` — no redirect to
create, but they are still published (the extractor only selects published
charts), so `apply_redirects.py` still includes them in the unpublish step.

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

### 4. Apply (GATED — separate user decision)

**Never run `--execute` or `--unpublish` unless the user explicitly asked in this
conversation.** Dry-run first, always:

```bash
.venv/bin/python .claude/skills/map-charts-to-mdim/scripts/apply_redirects.py \
  --mapping ai/economic-inequality-charts-mdim-mapping \
  --decisions ai/economic-inequality-charts-mdim-mapping/economic-inequality_chart_mdim_review.json
```

Whenever a review happened, pass the exported decisions file (step 2) via
`--decisions` — charts the reviewer flagged are excluded from both the redirect
creation and the unpublish step. Without it, review-HTML flags are silently
ignored (the script warns on `--execute`).

The dry-run re-checks existing redirects + chains fresh, re-verifies each
source chart against the DB (slug + config MD5 captured at extraction — an
edited or deleted chart comes back `STALE` and is skipped), and prints the
action table (`CREATE` / `EXISTS` / `DIFFERS` / `CONFLICT` / `STALE`);
non-zero exit means something needs attention before executing.

Facts to surface to the user before `--execute`:

- Redirect tables are **per-environment** (staging does not sync to production);
  production redirects need production admin creds (`ADMIN_API_KEY`). If the
  user can't run against prod admin, hand them `mapping.json` — the web team's
  `createMultiDimRedirectsFromCsv.ts` consumes the same identifiers.
- **Each created redirect triggers a static build** — mention the batch size.
- A redirect alone is cheap to reverse (DELETE endpoint exists). The
  destructive half is **unpublishing the source charts** (their pages/embeds
  disappear), which is what makes the redirect actually take effect — grapher's
  own reference script does both. That's why `--unpublish` is a separate flag,
  requires `--execute`, and asks for a typed confirmation.
- Before unpublishing, each chart's references are audited via the admin API:
  charts directly referenced by **explorers / narrative charts / data insights /
  static viz** are blocked and reported (a redirect does not update those —
  they need manual migration first). Gdoc/article links are fine: they go
  through the `/grapher/<slug>` URL, which the redirect covers.

## Gotchas

- `viewConfigId` (a `chart_configs` UUID, = `views[].fullConfigId`) is what the
  POST endpoint validates; the human-readable `view_id`
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
  resolved to a variable id — matching on a truncated indicator set could hit
  the wrong chart.
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
