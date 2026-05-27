---
name: map-explorer-to-mdim
description: >-
  Suggest a redirect mapping from a (soon-to-sunset) OWID explorer's views to the
  views of one or more replacement MDIMs. Pulls explorer views and MDIM views from
  the grapher DB, writes a CSV per source/target plus a wide joint proposal that
  routes each explorer view to a target MDIM view, and flags when several explorer
  views land on the same MDIM view. Trigger when the user says "map explorer <slug>
  to mdim(s) <...>", "suggest explorer->MDIM redirects", "we're sunsetting the
  <slug> explorer, map its views to the new multidims", or similar.
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

## DB access

Both the explorer and the MDIMs are read from the grapher DB via `OWID_ENV`. Run either:

- from a **staging branch** (the usual case — `OWID_ENV` points at `staging-site-<branch>`, a prod clone), or
- against **production** (read-only) by prefixing commands with `ENV_FILE=.env.prod DATA_API_ENV=production`.

If a query returns nothing, the scripts stop with a clear message (explorer slug not
found, or MDIM not published in this DB).

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

The script prints a validation report: how many explorer views resolved, distinct MDIM
views hit per MDIM, how many rows share a target, and **FLAGS** for any explorer view that
didn't resolve to a real MDIM view (fix the rules and re-run until there are no flags).

### 4. Review

Sanity-check the flagged rows and the judgment calls (approximate type matches, aggregate
collapses, MDIM choices with no explorer source). The CSVs are typically pasted into a
spreadsheet for a human reviewer / topic owner to confirm before redirects are created.

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
- Re-running `extract_views.py` overwrites the CSVs but **not** your `mapping_rules.py`.
