---
name: map-explorer-to-mdim
description: >-
  Take (soon-to-sunset) OWID explorers to redirected MDIMs, end to end. Maps each
  explorer's views to the views of one or more replacement MDIMs, writes ONE
  apply-ready JSON payload per explorer for the admin bulk-redirect endpoint, audits
  every article that links or embeds each explorer (with the view each link will land
  on), preflights every validation the endpoint performs — including site redirects
  that would block it — and covers retiring the explorer's ETL step afterwards.
  Trigger when the user says "map explorer <slug> to mdim(s) <...>", "suggest
  explorer->MDIM redirects", "we're sunsetting the <slug> explorer, map its views to
  the new multidims", "redirect these explorers to MDIMs", or similar.
metadata:
  internal: true
---

# Map an explorer's views to MDIM views (redirect proposal)

When an explorer is being retired in favour of one or more MDIMs, every explorer
view needs a redirect to the equivalent MDIM view. This skill produces the input
for that: a CSV of explorer views, a CSV per target MDIM, and a **joint proposal**
mapping each explorer view to a target MDIM view (the suggestion is for human review).

The mapping itself is **explorer-specific** (how the explorer's dimensions translate
to MDIM dimension slugs, and — when there are multiple MDIMs — which MDIM each view
routes to). The skill automates everything mechanical (pulling views, the join, the
shared-target accounting, validation) and leaves only the per-explorer rules for you
to write, seeded with auto-suggested matches.

## Inputs

- **Explorer slug** — matches `explorers.slug` in the grapher DB (e.g. `natural-disasters`).
- **One or more MDIM catalogPaths** — as stored in `multi_dim_data_pages.catalogPath`,
  e.g. `natural_disasters/latest/deaths#deaths`. The MDIMs must be **published in the
  DB you connect to** (their fully-expanded views are read from `multi_dim_data_pages.config`).

## DB access (confirm this *before* running)

Both the explorer and the MDIMs are read from the grapher DB via `OWID_ENV`, so the
scripts only work where that DB actually contains both the explorer and the published
MDIMs. There are three ways to point `OWID_ENV` at such a DB — **figure out which one
applies before running, and don't assume `.env.prod` exists:**

1. **Staging branch** (often easiest): if you're on a `staging-site-<branch>` branch,
   `OWID_ENV` already points at that prod-clone DB — run the commands as-is, no prefix.
2. **Production, read-only, via `.env.prod`**: prefix commands with
   `ENV_FILE=.env.prod DATA_API_ENV=production`. **Only if `.env.prod` is present.**
3. **Some other credentials file**: the user may keep prod (or other) DB creds in a
   different env file — run with `ENV_FILE=<their file> [DATA_API_ENV=production]`.

**Preflight — check, then ask if needed:**

```bash
# Is .env.prod available?
ls -la .env.prod 2>/dev/null && echo "found .env.prod" || echo "NO .env.prod"

# Connectivity test (swap the ENV_FILE prefix for whatever applies; drop it on a staging branch):
ENV_FILE=.env.prod DATA_API_ENV=production .venv/bin/python -c \
  "from etl.config import OWID_ENV; print('DB OK:', OWID_ENV.read_sql('SELECT 1 AS x').iloc[0,0])"
```

If `.env.prod` is missing **and** you're not on a staging branch with the data, **stop
and ask the user which credentials / env file to use** (e.g. "I don't see `.env.prod` —
which env file holds DB credentials that can reach the explorer + MDIMs? Or should I run
this from a staging branch?"). Then use that file as the `ENV_FILE=` prefix for both
script invocations below. Don't hardcode credentials.

If the connection works but a query returns nothing, the scripts stop with a clear
message (explorer slug not found, or MDIM not published in this DB) — that means the DB
you reached doesn't have it, so re-check which DB you're pointed at.

## Workflow

### 1. Extract views + scaffold

```bash
.venv/bin/python .claude/skills/map-explorer-to-mdim/scripts/extract_views.py \
  --explorer <slug> \
  --mdim <ns/v/short#short> [--mdim <ns/v/short#short> ...] \
  --out ai/<slug>-mdim-mapping
```

Writes into the out folder:

- `explorer_views.csv` — `id` (1..N) + `dimension_1..M` (explorer **display** values).
- `multidim_<short>_views.csv` — one per MDIM; `id` is letter-prefixed by `--mdim` order
  (`A1…`, `B1…`, `C1…`) so ids are unique across MDIMs; columns are the MDIM dimension **slugs**.
- `_scaffold.md` — the explorer dimension legend (which `dimension_i` is which name),
  the distinct values per dimension, each MDIM's dims/choices, auto-suggested value
  matches (where a slugified explorer value equals a real MDIM choice slug), and a
  ready-to-edit `mapping_rules.py` template.
- `_sources.json` — machine-readable record of the explorer slug + dimension names and
  each MDIM's short/prefix/catalogPath/dim-slugs. Consumed by `build_mapping.py` to emit
  `mapping.json` (below); don't hand-edit it.

### 2. Write `mapping_rules.py`

Open `_scaffold.md`, then write `ai/<slug>-mdim-mapping/mapping_rules.py` defining:

- `EXPLORER_DIMENSIONS` — list naming `dimension_1..N` (copy from the scaffold; keep order).
- `MDIMS` — MDIM short names in the same order as `--mdim` (= prefixes `A`, `B`, `C`, …).
- `route(dims) -> str` — given a view's `{dimension name: value}`, return the target MDIM
  short name. For a single MDIM this is just `return "<short>"`. For several, it's a
  decision on some explorer dimension (e.g. natural-disasters routes on `Impact`:
  `Deaths`→deaths, `Economic damages (% GDP)`→economic_damages, the rest→affected).
- `translate(dims, mdim) -> dict` — return `{mdim_dim_slug: choice_slug}` for the target
  MDIM view, built from the `*_MAP` dicts. Only include slugs the MDIM actually has
  (e.g. economic_damages has no `metric` — single-choice dims are pruned from MDIM views).
- *(optional)* `DEFAULT_MDIM = "<short>"` — the catch-all target for the bare explorer URL
  (see `mapping.json` → `catchAll`). Omit it and the best-fitting MDIM is chosen automatically
  (the one receiving the most resolved views; tie-break = earliest in `MDIMS`). Set it only
  when the automatic pick isn't the MDIM you'd want a param-less explorer link to land on.

The scaffold seeds the `*_MAP` dicts with `slugify(value)` guesses. **Verify every entry** —
slugify won't catch label↔slug differences like `Decadal average`→`decadal`, `Injuries`→`injured`,
`Volcanoes`→`volcanic_activity`, or aggregate collapses like `All disasters`/`All disasters (by type)`→`all_stacked`.

### 3. Build the proposal

```bash
.venv/bin/python .claude/skills/map-explorer-to-mdim/scripts/build_mapping.py --out ai/<slug>-mdim-mapping
```

Writes `mapping_proposal.csv`, one row per explorer view:

| columns | meaning |
|---|---|
| `id`, `dimension_1..N` | the explorer view (same as `explorer_views.csv`) |
| `target_mdim`, `target_view_id` | the resolved target (`target_view_id` is the `A*`/`B*`/`C*` id) |
| `<mdim>_<dimslug>` … | wide block; only the **target** MDIM's columns are filled with the translated slugs |
| `shared_target_explorer_ids` | when >1 explorer view lands on the same MDIM view, the comma-joined list of all those explorer ids (e.g. `1,12`); empty when the target is unique |

It also writes **`mapping.json`** — the machine record — and
**`admin_bulk_payload.json`**, which is what you actually apply. The API exists:
`POST {admin}/api/multi-dim-redirects/bulk` (`handleBulkCreateMultiDimRedirects`), also
reachable from the *Bulk-create redirects from JSON* button on `/admin/multi-dim-redirects`.
Its Zod schema deliberately mirrors this file's `catchAll` + `redirects` shape, ignores keys
it doesn't know (`sourceViewId`, `viewId`, `mdim`, `stats`, `targets`), and reports
`target: null` entries as `skipped`.

> **Post `admin_bulk_payload.json`, never `mapping.json`.** The payload has empty-valued
> source dimensions stripped out. That is mandatory, not cosmetic: a condition is matched
> against the incoming URL's params, and an absent param is not an empty string — so a
> condition of `{"Period": ""}` can never match, and every view carrying one silently falls
> through to the catch-all instead of its intended target. `mapping.json` keeps them because
> it is the faithful record of the view grid.

Unlike the CSV (positional `dimension_N` columns, meant for a spreadsheet), the JSON carries
every identifier a redirect needs:

```jsonc
{
  "explorer": { "slug": "...", "dimensions": ["<name>", ...] },
  "targets":  [ { "mdim": "...", "catalogPath": "ns/v/short#short", "dimensions": ["<slug>", ...] } ],
  "stats":    { "total": N, "resolved": N, "unresolved": N },
  "catchAll": {                        // bare explorer URL (no query params) fallback
    "source": { "explorerSlug": "..." },
    "target": { "mdim": "...", "catalogPath": "ns/v/short#short",
                "viewId": null, "dimensions": {} }   // no params → the MDIM's default view
  },
  "redirects": [
    {
      "sourceViewId": 1,
      "source": { "explorerSlug": "...", "dimensions": { "<name>": "<value>", ... } },
      "target": {                       // null when unresolved
        "mdim": "...", "catalogPath": "ns/v/short#short",
        "viewId": "A2",                 // internal id, cross-references the CSVs
        "dimensions": { "<slug>": "<choiceSlug>", ... }
      },
      "sharedTargetSourceIds": [1, 29, 57],   // present only when >1 source shares this target
      "unresolvedReason": "..."               // present only when target is null
    }
  ]
}
```

The **source** view is identified by the explorer slug + dimension **name→display-value**
(the explorer URL query params); the **target** view by the MDIM catalogPath + dimension
**slug→choice-slug** (the MDIM URL query params). Unresolved views are kept with
`target: null` so the API can see the full picture; a consumer typically skips them.

**`catchAll` is always present**: it redirects the bare explorer URL (no query params) — and
serves as the sensible fallback for any view a consumer doesn't route individually — to the
best-fitting MDIM with **no query params**, which grapher renders as that MDIM's default view
(hence `viewId: null`, `dimensions: {}`). The best-fitting MDIM is the one most views resolve
to, or whatever `DEFAULT_MDIM` in `mapping_rules.py` overrides it to.

The script prints a validation report: how many explorer views resolved, distinct MDIM
views hit per MDIM, how many rows share a target, and **FLAGS** for any explorer view that
didn't resolve to a real MDIM view (fix the rules and re-run until there are no flags).

### 4. Review

Sanity-check the flagged rows and the judgment calls (approximate type matches, aggregate
collapses, MDIM choices with no explorer source). `/review-explorer-mdim-mapping` renders the
pairs side by side with approve/flag controls for a topic owner; corrections go through
`mapping_rules.py` and a rebuild, not the HTML.

### 5. Audit what references the explorers (ALWAYS offer; run when the user says yes)

```bash
ENV_FILE=<prod creds> DATA_API_ENV=production .venv/bin/python \
  .claude/skills/map-explorer-to-mdim/scripts/audit_references.py \
  --mapping ai/<a>-mdim-mapping --mapping ai/<b>-mdim-mapping \
  --out ai/<combined>-redirects
```

Writes `references.csv` + `references.md` across all the explorers in one pass. It resolves
each referencing URL through the rules the payload would create, so every row says which view
the reader lands on — and separates *the link had no params* from *the link names a choice the
explorer has since dropped*, which lands on the MDIM's default view and needs authoring
attention rather than a URL swap.

**The timeline differs from the chart skill's, and this is the thing to say out loud:** an
embedded explorer breaks the moment the redirect is **created**, not later at unpublish,
because the embed renders by fetching the explorer page and parsing it. So the 🔴 rows are
migrated *before* step 7, not after.

### 6. Preflight (read-only, gated)

```bash
ENV_FILE=<prod creds> DATA_API_ENV=production .venv/bin/python \
  .claude/skills/map-explorer-to-mdim/scripts/preflight.py \
  --mapping ai/<a>-mdim-mapping --mapping ai/<b>-mdim-mapping \
  --out ai/<combined>-redirects [--record ai/<combined>-redirects/bulk_redirects.json]
```

Mirrors every validation the endpoint performs, per explorer — because it memoizes its
source-side checks and re-throws the cached rejection, so **one source-level problem fails
every entry for that explorer**. Blockers: the explorer path is already a site-redirect
source, or the *target* of one (the chain case); the slug collides with a chart's old slug; a
target MDIM is missing or unpublished; the target `/grapher/<slug>` is itself a redirect
source; two views share a source condition; the view fingerprint no longer matches the live
explorer; redirects already exist that differ from the payload; or embedded references are
outstanding. A **missing** `references.csv` is a blocker too — "never looked" must not read
like "looked and found nothing".

A retired explorer whose redirects are all live reports `DONE` and exits 0. That is the
finished state, not an error.

Non-zero exit means **do not post anything**.

### 7. Apply — the admin bulk endpoint (GATED, production)

> [!WARNING]
> **Creating the redirect darkens the explorer immediately.** It is checked on *every*
> `/explorers/*` request, ahead of `env.ASSETS.fetch`, so it beats the baked explorer page and
> any `_redirects` entry, and fires while the explorer is still published. There is no staged
> rollout and no bulk undo — removal is one row at a time.

Paste each `admin_bulk_payload.json` into *Bulk-create redirects from JSON* at
`{admin}/multi-dim-redirects`, **one explorer at a time**. Read the response
**positionally**: every `results[i].source` is the same `/explorers/<slug>` string, so index 0
is the catch-all when present and index `i` is `redirects[i-1]`. Expect
`created + skipped + errors == entries`.

Then wait for the bake **plus ~2 minutes** (the redirect map is fetched with a 2-minute edge
TTL) and verify:

```bash
curl -sI "https://ourworldindata.org/explorers/<slug>?<one view's params>" | grep -i "^HTTP/\|^location:"
# expect 302 + location: /grapher/<mdim>?<view dims>
```

### 8. Retire the explorer's ETL step

Once the redirect is verified, remove the explorer's ETL footprint — and **never delete a step
without archiving it** (see `CLAUDE.md`). Delete the step's `.py` **and its sibling
`.config.yml`** (the periodic archive sweep only knows `.py`, which is why orphaned configs
exist on disk today), remove the `dag/*.yml` entry, `make check`, **commit**; then run
`etl archive-dag` and commit `dag/archive/*.yml` separately, since it reads *committed*
history. `git checkout` anything unrelated it sweeps in. Archive anything now orphaned
upstream — a garden step that existed only to feed this explorer — in a second round.

> **Grep before deleting a shared explorer data step.** Some are consumed **off-DAG**, by
> scripts fetching their published catalog CSVs by URL. Those consumers are invisible to
> `etl archive-dag`, so the step looks like a safe leaf when it is not. Search for its catalog
> URL first and keep it until every consumer is retired.

Track these steps with `TodoWrite` in the chat. **Do not generate a checklist file**, and
there is no `HANDOFF.md` for this skill — unlike the chart path there is no cross-team
handoff, since the same operator pastes the payload.

## What each run writes

| file | written by | contents |
|---|---|---|
| `explorer_views.csv` | extract | `id` 1..N + `dimension_1..M` (display values) |
| `multidim_<short>_views.csv` | extract | one per MDIM, ids `A1…`/`B1…` |
| `_scaffold.md` | extract | dimension legend, distinct values, auto-matches, `mapping_rules.py` template |
| `_sources.json` | extract | slugs, catalogPaths, MDIM ids/slugs/published, **`viewsFingerprint`**, `configMd5` — don't hand-edit |
| `mapping_rules.py` | **you** | routing + value translation |
| `mapping_proposal.csv` | build | one row per explorer view, wide target block |
| `mapping.json` | build | faithful machine record (empty source dims **kept**) |
| **`admin_bulk_payload.json`** | build | **the apply unit** — one per explorer, paste into the admin modal |
| `references.csv` / `references.md` | audit | combined across explorers, in `--out` |
| `bulk_redirects.json` | preflight `--record` | combined record; **not postable** |

## Notes & gotchas

- **MDIM views come from `multi_dim_data_pages.config`** (published, fully expanded). This
  already reflects code-generated views (`group_views` aggregates) and **pruned single-choice
  dimensions** — so e.g. a `metric` that has only one active choice won't appear as a column.
- **Many explorer views can redirect to one MDIM view** — that's expected (the explorer often
  splits a concept the MDIM merges, e.g. a single-line "All disasters" total and a
  stacked-by-type view both mapping to the `all_stacked` MDIM view). `shared_target_explorer_ids`
  surfaces these so the reviewer sees the collisions.
- **An MDIM may have choices with no explorer source** (e.g. an `…_excluding_extreme_temperature`
  aggregate). Nothing redirects to those — fine, just confirm.
- Explorer dimension columns stay `dimension_1..N` (compact, and joinable to the explorer CSV);
  the name legend lives in `_scaffold.md` and in `EXPLORER_DIMENSIONS`.
- Re-running `extract_views.py` overwrites the CSVs and `_sources.json` but **not** your `mapping_rules.py`.
- `mapping.json` needs `_sources.json`; if you extracted before this output existed, just
  re-run `extract_views.py` once (it preserves `mapping_rules.py`), then `build_mapping.py`.
- **One payload per explorer is a correctness requirement**, not a convention: the endpoint's
  schema has a single `catchAll`, so a merged file would silently drop all but one.
- **There is no bulk delete** — only `DELETE /api/multi-dims/:id/redirects/:redirectId`, one
  row at a time. A 460-row batch posted wrongly is expensive to undo, which is why the
  preflight exists.
- **Explorer redirects never reach `_redirects`** (`getRecentMultiDimRedirects` excludes
  non-`/grapher/` sources), so there is no static-redirect fallback and no one-week window —
  they live only in the baked redirect map.
- **Matched source params are deleted from the outgoing URL and unmatched ones leak through.**
  The target view's params win; `country=`, `time=`, `tab=` ride through untouched, which is
  what a reader following an old link wants.
- **An explorer view's id is positional row order** — a re-saved TSV renumbers every id that
  `mapping_proposal.csv`, the review HTML and `sourceViewId` key on. That is what
  `viewsFingerprint` detects; `configMd5` is only a secondary signal, since it flips on any
  FAUST edit and gating on it would force a needless re-review.
- **Algolia keeps indexing the explorer** until the next index build, so it can still appear
  in site search after the redirect exists.
- **Retiring the explorer row itself is separate** from the redirect: deleting it makes
  `linkedChart` unresolvable, so `Chart` blocks render nothing and tiles vanish. The five
  WB/WID inequality explorers migrated in July are the worked example.
