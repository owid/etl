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
| `mdim_suggestions.md` | **what the MDIMs would need** for the unmatched charts to become redirectable |
| `HANDOFF.md` | standalone note for whoever runs the CLI — command, caveats, why the CLI |
| `_sources.json` | machine record of run inputs (don't hand-edit) |

**Handoff convention: one source per JSON.** Like the explorer→MDIM redirect
deliverables (`ai/explorer-mdim-redirects/`, one `admin_bulk_payload.json` per
explorer), each JSON handed over must describe exactly ONE source page. For
charts that means one file per chart, in `payloads/`. The combined
`mapping.json` is the machine record that `preflight.py` and
`audit_references.py` read — don't hand that one over as a copy-paste payload.

**Matching**: a chart matches a view when the y-variable-ID sets are equal AND
x/size/color agree (absent == absent) — after **decoration indicators** are
stripped from x/size/color on both sides: population as Marimekko width / bubble
size, and `regions#owid_region` as continent coloring (matched by catalogPath, so
version bumps don't matter). Charts and views carry these inconsistently without
changing what data is plotted — an MDIM view adds `x=population` for its Marimekko
tab, an editor adds continent colors to a line chart — and requiring them to agree
literally drops same-y pairs into `none` with no report at all (equal y sets means
they aren't even a near miss). A match made across such a difference says so in
the proposal's `note` column. Content indicators (a scatter's GDP-per-capita x, a
"political regime" coloring) are never stripped. Two guardrails keep the rule from
overreaching: a **scatter's x slot is never stripped** — on a `ScatterPlot`,
`x=population` is the plotted relationship, not decoration (size/color there still
are) — and the population pattern is **end-anchored** to the raw head-count columns
(`#population`, `#population_historical`, `#population_projection`), because the
same dataset also carries population *density* columns that are content. Several matching views → tiebreak
on chart type; still ambiguous → reported, never guessed. **Any** partial indicator overlap
that is not an exact match → `near_miss`, reported only — not just subset/superset:
a chart plotting `{A, B}` against a view plotting `{A, C}` shares `A`, so calling
it `none` would assert an overlap check that came out the other way. Quality labels:

- `exact` — one view matches (possibly via the `chart_type` tiebreak — check the
  `tiebreak` column).
- `forced` / `skipped` — set by `overrides.csv` (below).
- `ambiguous` — several views match; needs a human pick.
- `near_miss` — overlapping but unequal indicator sets; the diff is spelled out.
- `none` — no view shares the chart's indicators. **An accepted outcome** — only
  matched charts get redirects; don't force the rest.

**Twin suspects.** ID matching is blind to one real equivalence: a dataset that
publishes the same series in two tables (WID/LIS do — `inequality#share_top_10__…`
for the standalone charts vs `incomes#share__…quantile_10…` for the MDIM; verified
value-identical). The extractor flags these — unmatched charts whose y comes from
the same dataset as a slot-compatible view with a similar column name — in the run
report and in `mdim_suggestions.md` as **twin suspects**, with the exact
`overrides.csv` line to use. A suspect is NOT a match: verify first that the two
indicators' values are identical (fetch both
`api.ourworldindata.org/v1/indicators/<id>.data.json` and compare every
entity-year), then force it, citing the verification in the override note.

Either side carrying several indicators in one `x`/`size`/`color` slot has no
chart-shaped signature, so it is **excluded from matching** rather than truncated
to the first: a truncated signature can spuriously exact-match a counterpart that
lacks the rest. Charts land in `none` with the reason in `note`; views are dropped
from the target pool with a warning. `overrides.csv` can still force either.

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
  `mapping.json → conflicts[]` and are kept out of the CSV — and, because being
  absent from the CSV is not the same as being finished, they are also listed in
  `HANDOFF.md` with their blocker, next to the already-redirected rows.

Charts already redirected to the same target are counted as `already_done` — no
redirect to create, but they are still published (the extractor only selects
published charts), so they shadow their redirect and it never fires. **The CLI
cannot finish these**: their source is already a `multi_dim_redirects` source,
which its own validation rejects, so including them would abort the whole run.
Preflight reports them as `MANUAL`; unpublish each one in the grapher admin. The
exception is a chart carrying old slugs — hand-unpublishing deletes its
`chart_slug_redirects` rows and those slugs become hard 404s, so take those to
the Grapher team instead. That manual unpublish breaks embeds exactly as the
CLI's would, so `already_done` charts sit behind the same reference gate and
appear in the step-4 audit alongside the proposed redirects.

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
explorers, data insights, static viz, article chart blocks — resolves it by id or
slug and keeps rendering the old config, so it breaks when the source chart is
unpublished (which the apply step always does).

```bash
ENV_FILE=<prod creds> DATA_API_ENV=production .venv/bin/python \
  .claude/skills/map-charts-to-mdim/scripts/audit_references.py \
  --mapping ai/economic-inequality-charts-mdim-mapping
```

The sweep itself is the shared `find-chart-references` skill; this script is the
redirect-specific consumer that adds the replacement URLs. It writes
`references.csv` + `references.md`, one row per reference. The report is
organized by what the reader does, not by severity tier: **embedded charts and
text links sit adjacent in one "Google Doc edits" section** (one editing pass
per doc covers both — 🔴 embeds break on unpublish and gate the CLI, 🟡 links
stay functional behind the 301), with reader-facing section names ("Embedded
charts", "Text links", "Front-matter chart URLs") rather than raw ArchieML
tokens (those live in the CSV's `component` column). **Topic-page All charts
entries collapse to a per-page summary and need NO action**: the block lists
only published charts (`GdocPost.loadRelatedCharts` filters on `isPublished` —
verified in grapher), so entries drop out on their own at the next bake — and
no replacement is possible either, because the block is built from `charts` ×
`chart_tags` only and cannot list MDIMs; featuring the MDIM on a topic page is
a separate gdoc-authoring change. **Narrative charts get their own table
(before the All charts summary)**, one row per chart: the admin editor link and
parent chart; **"create from this view"** — the target view carrying that
narrative chart's stored controls, which is both the rendering to match and the
place to create from; **"text to re-apply"** (see below); **the pages that
actually embed it**, resolved by a second hop the sweep doesn't make
(`posts_gdocs_links` on `linkType='narrative-chart'`, both `narrative-chart` and
`key-insights` components), each with its doc link and the name to search for;
and the ordered steps. Each row carries only the order that applies to it, on
the rule above: a **published** page among the users makes create → repoint →
delete mandatory; with none, the row emits the delete → create shortcut that
reuses the same name (a draft reference then keeps resolving, since pages
reference the name). **ℹ️** unpublished/draft pages close the report.

**Create from the view, not from a create link.** Tell the operator to open the
target view and use the chart's own **"Create narrative chart"** admin control:
the MDIM page builds that control's target from whichever view is on screen
(`site/multiDim/MultiDim.tsx`), so the new chart is parented to the right view
and inherits the controls set on it. A bare
`/admin/narrative-charts/create?type=multiDim&chartConfigId=<viewConfigId>` link
looks equivalent but opens a copy of the MDIM's **default** view — verified in
practice — so never hand that out as the create step.

**Creating from an MDIM starts at its DEFAULT view — nothing carries over.**
Not the dimension selection, not the entity selection, not tab/time, not the
text. So the report's **"Set by hand after creating"** column lists all three
groups per chart, in the order they get applied in the editor:

1. **view dimensions** — the target view's own dimension values (from the
   proposal), because the new chart opens on the MDIM's default view;
2. **controls** — taken from the narrative chart's `chart_configs.patch` (the
   delta its author typed on top of the parent) plus its stored
   `queryParamsForParentChart`, and listed **chart type first** (it decides
   which other controls exist), **then the entity selection** (the most visible
   thing to get wrong), then the rest alphabetically. Entities are always shown
   by **name**, never as codes: a URL param spells them `ZWE~MDG`, so those are
   resolved against `entities` before display (unknown codes pass through). The
   patch and the params encode the same state, so a URL param is dropped when
   the patch already carries the equivalent config key (`country` ↔
   `selectedEntityNames`, `focus` ↔ `focusedSeriesNames`, `time` ↔
   `minTime`/`maxTime`) — otherwise the cell asks for one setting twice in two
   spellings, and the config form is what the editor's fields expose;
3. **FAUST text** — `title`, `subtitle`, `note`, `sourceDesc`,
   `hideAnnotationFieldsInTitle` from the patch. Miss these and the replacement
   silently renders the *view's* wording instead of the text the article was
   written around.

Every group is diffed against the target view's config, so only genuine
differences are asked for. `dimensions` and `$schema` are excluded from the patch
on purpose: the new parent view supplies them, and re-applying the old ones would
repoint the chart at the retired chart's indicators.

**Suggest the replacement's name, and check it is free.** When a published page
holds the original name the replacement needs a different one — and that name is
**permanent**, since `create` rejects an existing name and there is no rename, so
it is not a staging name to tidy up after the delete. The report suggests
`<original>-mdim` (falling back to `-mdim-2`, `-mdim-3`, …), validated against
every name in `narrative_charts` so the suggestion cannot be the one thing
`create` refuses; suggestions handed out within a run are reserved as they go. In
the delete-first case there is nothing to suggest — the row names the original,
which the delete frees for reuse.

**Admin routes need the admin origin.** A narrative chart's `where_path` is
`/admin/narrative-charts/<id>/edit`, and the public site does not serve
`/admin` — prefixing it with the site host yields a link that 404s. Both the
sweep and this consumer route such paths through a helper
(`admin_url` / `absolute_url`) that strips the admin root's own `/admin` suffix
before joining, or the result carries `/admin/admin/`.

Pure SQL, so read-only credentials are enough. It sweeps both the current slug
and every old slug that reaches the chart — references written before a rename
point at the old one.

**Watch for the param-collision warning.** Grapher merges the incoming URL's
params *over* the redirect target's, so a link carrying `?metric=…` overrides an
MDIM dimension of the same name and lands the reader on the wrong view. The
audit flags those explicitly.

Mention this step every time (like `/update-dataset` step 7); running it is the
user's call, since a wide sweep costs tokens — but preflight gates on the same
embedded references, so it becomes mandatory before applying.

**Narrative charts do not block a migration.** One parented to a chart owns a
materialized full config and renders from it, so unpublishing the parent leaves it
intact; only its generated "Explore the data" href uses the parent slug, and the
301 covers that. They are classified `link`, reported by preflight but never gated
on — gating would strand every such chart behind raw SQL for no reader-visible
gain. Do check the href's query params for collisions with the target view's
dimensions (step 4 flags them).

**To actually fix one, replace it — don't try to repoint it.** The parent columns
are INSERT-only, so there is no repointing API and never will be one by design; the
route the Grapher devs endorse is to create a *new* narrative chart from the
equivalent MDIM view and delete the old one. Three API facts set the order, and
getting it wrong strands you:

- **create** rejects a name that already exists, and requires kebab-case;
- **delete** refuses while a **published** post references the name;
- **update** writes only query params — there is **no rename**.

So the obvious sequence (delete, then recreate under the same name) is blocked in
precisely the case that matters — a narrative chart embedded in a published article.
Two paths, and the references audit (step 4) tells you which applies:

*No published post references it* → delete, then create the replacement with the
same name. The name is preserved and nothing else changes.

*A published post references it* (the usual case) → do it in this order:

1. **Create** the replacement from the MDIM view, under a new kebab-case name.
2. **Update the article(s)** to reference the new name.
3. **Delete** the old one — now unreferenced, so the delete succeeds.

Never delete first. The delete will fail, and unpublishing the article to force it
through breaks the page for readers.

**Do the create in the admin UI — it is deep-linkable to the right view:**

```
{admin_site}/narrative-charts/create?type=multiDim&chartConfigId=<target.viewConfigId>
```

That opens the narrative-chart editor already parented to the MDIM view, so you
set the name and the view's controls and save. Prefer this over the API: `AdminAPI`
has `get_narrative_chart` and `update_narrative_chart` but **no create or delete**,
so the API route means hand-rolled HTTP for both ends of the swap.

If you do script it, the endpoints are `POST {admin_api}/narrative-charts` and
`DELETE {admin_api}/narrative-charts/<id>`:

```jsonc
{
  "type": "multiDim",
  "name": "<kebab-case-name>",
  "parentChartConfigId": "<the MDIM view's chart_configs.id>",
  "config": { /* the OLD narrative chart's rendered full config */ }
}
```

**You already have `parentChartConfigId`: it is `target.viewConfigId` in the
redirect payload** for the chart this narrative chart hangs off
(`payloads/<chart_slug>.json`, or `target_view_config_id` in
`mapping_proposal.csv`). That is the MDIM view's `chart_configs.id` — the same
value `find-chart-references` reports as `config_id` on an `mdim view` row. So
"which MDIM view do I parent the replacement to" is answered by the proposal: it is
the view the chart was going to redirect to. Get the
`config` from `AdminAPI.get_narrative_chart(<old id>)["configFull"]`: the endpoint
derives the patch itself by diffing what you pass against the new parent, so pass
the rendered full config, not the old patch.

**Replacement is manual, and that is a deliberate call.** owid/owid-grapher#6872
asked for a repointing endpoint; it was **closed as not-planned** in favour of
manual replacement,
because the number of narrative charts that can ever be in this situation is tiny.
The population is not "all narrative charts" — it is narrative charts whose **parent
chart is itself a redirect candidate**, i.e. has an exact MDIM-view match. Measured
site-wide on production (2026-07) by applying this skill's own matching rule to every
published chart with narrative children: 249 narrative charts across 190 parent
charts, of which **5 parents match a published MDIM view — so 5 narrative charts
total**, and that is the ceiling if every matchable chart were migrated, not a
per-migration figure. (The Economic Inequality trial hit 1 of them.)

Re-measure rather than trusting that number: it grows as MDIMs are published, since
each new MDIM view can turn an existing chart into a candidate. `ai/` in this repo
has the query; the shape is — narrative charts on published parents → those parents'
indicator slots → compare against every published view's slot signature. **Revisit
the decision if a single migration would require replacing more than a handful**,
and say so in the PR rather than grinding through them silently.

**One problem from that issue is now untracked.** #6872 covered two things, and
closing it closed both. Manual replacement solves the repointing half; it does
nothing for the other — a narrative chart parented to an MDIM view blocks that
MDIM's next re-publish, because `cleanUpOrphanedChartConfigs` deletes
`multi_dim_x_chart_configs` rows with no narrative-chart guard and the FK refuses
(`ER_ROW_IS_REFERENCED_2`). Renaming a dimension or choice slug is enough to
trigger it, and it surfaces as a data update failing with an opaque FK error.
Replacement makes this *more* likely, not less: every replacement creates a new
MDIM-parented narrative chart. Production carries 1 today. If that count grows,
re-file it as its own issue — the write-up is in
`ai/narrative-charts-grapher-issue.md` (§2), alongside
`ai/narrative-charts-slack-post.md`.

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
both sides of each row against the proposal: the source chart's slug + config MD5,
and the target MDIM's slug + reviewed view (an edited, deleted, renamed or rebuilt
one comes back `STALE`). Statuses: `OK` / `BLOCKER` / `EXISTS` / `DIFFERS` /
`GONE` / `STALE` / `MANUAL`.

It also **gates on embedded references**. Explorers, data insights, static viz
and article chart blocks render the chart's own config, so
unpublishing the source breaks them with no error anywhere — the one failure mode
the CLI itself cannot detect. Preflight counts them (current *and* old slugs, for
proposed *and* `already_done` charts) and exits non-zero while any remain. Migrate
them (step 4 gives you each replacement URL), then re-run; `--no-references` skips
the gate once they are handled.

Non-zero exit means **do not run the CLI yet**. Pass `--decisions` whenever a
review happened; flagged charts are excluded (remove them from the CSV, or mark
them `SKIP` in `overrides.csv` and re-run step 1).

### Step 6 — hand the migration over

**The skill's output ends here.** Send `redirects_for_cli.csv` and `HANDOFF.md`
to a Grapher developer, who runs the CLI themselves from the owid-grapher repo.
Never run it — not even `--dry-run`, which still connects to the production DB.
`HANDOFF.md` is written for that developer and already carries the commands and
every caveat they need, so don't restate the procedure here or in chat; they
will not have this skill.

Two things stay on **our** side of the handoff, because the developer won't do
them:

- **Say what the run will destroy.** It unpublishes every source chart, in the
  same transaction as the redirects. Unpublishing is mandatory rather than
  tidy-up — a grapher redirect is only consulted when the URL 404s — which is
  exactly why step 4's embed gate has to be clear first. Anything that renders a
  source chart's own config (explorer, data insight, static viz, article chart
  block) breaks silently the moment it is unpublished, and no redirect repairs
  that.
- **Log the cutover.** Right after the developer confirms the run, stamp
  `cutover_date` in `migration_log_template.csv` and keep it. Analytics cannot
  reconstruct it later: `prod_semantic.redirects` holds no `multi_dim_redirects`
  rows, and once a chart stops being published its whole view history resolves
  to `chart_id = NULL` — retroactively, not just from the cutover. To rebuild a
  continuous series, query `grapher_views_detailed` on the raw `grapher` column
  for the pre-cutover period, the MDIM slug after it, and union the two. Expect
  a ~1 week tail of real views on the old URL (redirects fire only on 404 and
  Cloudflare serves the cached page meanwhile), and note that query params are
  stripped in analytics, so every MDIM dimension collapses into one slug.

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
- **`viewConfigId` does not change when the view's content does.** Re-exporting
  an MDIM looks each view up by its dimension-derived view id and updates that
  `chart_configs` row in place, so the id survives content edits and only ever
  changes together with `view_id`. To tell whether the reviewed *rendering* still
  holds, compare `viewConfigMd5` (`chart_configs.fullMd5`) — the target-side
  mirror of a source chart's `configMd5`. Both md5s are in the review
  fingerprint and in `preflight.py`'s staleness checks.
- **`charts.publishedAt` is not the live publication flag.** It records the first
  publish and stays set after an unpublish (308 production charts are in that
  state), so chart selection and reference grading both read `isPublished` from
  the config instead.
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

- **Same-y charts vanishing into `none` is the matcher's blind spot** — when a
  reviewer reports a "clear equivalent" the run missed, diff the two sides'
  x/size/color slots first (charts.csv vs multidim_views.csv): the y sets being
  equal means the miss can only be a slot disagreement, and if it's a decoration
  indicator the fix belongs in `DECORATION_PATTERN`, not in `overrides.csv`.
  (2026-08: population + owid_region cost 5 of 19 Economic Inequality matches.)
- **Preflight's embed gate must classify exactly like `find-chart-references`.**
  It re-implements the embed count in SQL for read-only use, so any component
  the sweep exempts (e.g. `all-charts` — a topic page's auto-generated index
  where a retired chart just drops out) must be exempted there too, or preflight
  blocks on a reference the audit rightly never lists.
- **Re-runs that add targets invalidate those rows' review decisions** — the
  review HTML fingerprints each decision on (target, both config md5s), so a row
  that gains or changes a target gets its saved approval/note pruned on next
  load. Before re-running the extractor mid-review, have the reviewer export
  (⬇ JSON); unchanged rows re-import cleanly.
- **A reviewer flagging an unmatched row with "the target should be X" is the
  twin-variable signal** (2026-08: 3 of 3 such flags were twins — same dataset,
  two tables, identical values). The workflow is: verify values via the
  indicators API, force via `overrides.csv`, re-run. The old flag then reads as
  stale in `preflight.py --decisions` (a decision exported with an empty target
  no longer matches the now-targeted row — deliberate, or the flag would silently
  drop the freshly forced redirect from the CSV); the forced rows just need a
  quick re-approve + re-export.
