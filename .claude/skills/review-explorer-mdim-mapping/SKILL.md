---
name: review-explorer-mdim-mapping
description: >-
  Generate a self-contained HTML to review an explorer → MDIM view mapping
  side-by-side, with approve/flag controls and browser-persistent decisions.
  Consumes the output of the `map-explorer-to-mdim` skill (mapping_proposal.csv
  + mapping_rules.py + multidim_*_views.csv) and renders a single shareable
  HTML in `ai/`. Trigger after `map-explorer-to-mdim` when a human needs to
  verify the proposed explorer→MDIM correspondence chart-by-chart, or when the
  user asks to "review the explorer→MDIM mapping", "build a review tool for
  the <slug> migration", or "make a side-by-side HTML so <reviewer> can sign off".
metadata:
  internal: true
---

# Review an explorer → MDIM mapping (side-by-side HTML)

When an explorer is being retired in favour of one or more MDIMs, `map-explorer-to-mdim`
produces a proposed mapping (one explorer view → one MDIM view per row). This skill turns
that proposal into a **shareable, self-contained HTML** where a human steps through every
pair, sees both charts side by side, and clicks **Approve** / **Flag** (with notes).
Everything is saved automatically in the reviewer's browser, with an optional file
auto-save and Import for cross-machine recovery.

The HTML is a single file — no server, no dependencies. Just send the `.html` to the
reviewer.

## When to trigger

- After `map-explorer-to-mdim` has produced `ai/<slug>-mdim-mapping/`.
- When the user asks for a review/verification UI for an explorer → MDIM migration.
- When the user mentions sharing the proposed redirects with a topic owner or colleague
  for sign-off before redirects are wired up.

If `ai/<slug>-mdim-mapping/mapping_proposal.csv` doesn't exist yet, **stop and tell the
user to run `map-explorer-to-mdim` first** (or do that as a prerequisite). This skill does
not produce the mapping itself.

## Inputs you'll need

1. **The mapping directory** — `ai/<slug>-mdim-mapping/`, the output of
   `map-explorer-to-mdim`. Must contain at least `mapping_proposal.csv` and
   `mapping_rules.py`. `multidim_<short>_views.csv` files are also used by the coverage
   report.
2. **The explorer slug** — matches the live URL, e.g. `natural-disasters` for
   `https://ourworldindata.org/explorers/natural-disasters`.
3. **The published Grapher slug per MDIM** — one per MDIM short name listed in
   `mapping_rules.MDIMS`. This is the slug that appears in `/grapher/<slug>` on the host,
   not the catalogPath. The skill cannot derive it without DB access, so ask the user (or
   look it up via `multi_dim_data_pages.slug` if you have DB access).

If the user hasn't told you the published MDIM slugs and you can't query the DB, **stop
and ask** — they're required to build the right-hand iframe URLs.

## Usage

```bash
.venv/bin/python .claude/skills/review-explorer-mdim-mapping/scripts/build_review.py \
    --mapping-dir ai/<slug>-mdim-mapping \
    --explorer-slug <slug> \
    --mdim-slug <short_1>=<grapher_slug_1> \
    --mdim-slug <short_2>=<grapher_slug_2> \
    [--host https://ourworldindata.org] \
    [--output ai/<slug>_view_review.html] \
    [--no-coverage]
```

Concrete example for `natural-disasters`:

```bash
.venv/bin/python .claude/skills/review-explorer-mdim-mapping/scripts/build_review.py \
    --mapping-dir ai/natural-disasters-mdim-mapping \
    --explorer-slug natural-disasters \
    --mdim-slug deaths=natural-disasters-deaths \
    --mdim-slug affected=natural-disasters-people-affected \
    --mdim-slug economic_damages=natural-disasters-economic-damages
```

The script prints a coverage summary (rows, distinct MDIM targets, many-to-one collapses,
unresolved rows, MDIM views never targeted) **before** writing the HTML. Read it: it's the
fastest way to spot a mapping gap *the reviewer can't see by eye*.

## What the reviewer sees

- **One pair at a time** — old explorer (left) vs proposed MDIM view (right), both rendered
  in iframes against the configured host.
- Selection chips above each chart (e.g. `Disaster Type: Floods · Impact: Deaths · …` and
  `type: flood · timespan: annual · …`) and the full URL underneath, with "open ↗".
- **Approve ✓ / Flag ⚠ / Clear**, optional note, prev/next + a jump dropdown, filters
  (All / To review / Approved / Flagged), live counts.
- Keyboard: `←`/`→` nav, `a` approve, `f` flag, `c` clear.
- Persistence: every action auto-saves to `localStorage` (survives refresh & close).
  **Auto-save to file** (Chrome/Edge File System Access) mirrors each change to a JSON
  on disk. **Import** restores/merges from any exported JSON — the cross-machine recovery
  path and the way two reviewers merge their decisions.
- **Export CSV / JSON** at any time for the verified mapping artifact.

## After the reviewer is done

They'll either:
- Send back the auto-saved JSON or an exported one, which contains every row with its
  `status` (`approved` / `flagged` / blank) and `note`, plus the explorer/MDIM URLs. Use it
  to finalize the redirects (rows with `status == "approved"` are ready to wire up;
  `flagged` rows need a second pass with the user).
- Or just signal "looks good" — at which point the verified mapping artifact is the
  exported JSON / CSV.

## Caveats / gotchas

- **Reviewer's `localStorage` is per-browser and per-file path.** Refreshes don't lose
  work, but switching browser or machine does — that's why Auto-save to file / Import
  exist. If the reviewer shares decisions, ask them to Export JSON and send it.
- **Single-choice MDIM dimensions are pruned** from URLs (and from the `mapping_proposal.csv`
  wide block) — the skill respects that, so you should *not* manually add e.g. a `metric`
  param for an MDIM whose `metric` has only one active choice.
- **Explorer URL params are display names**, not slugs (e.g. `Disaster Type=Floods`, not
  `disaster_type=floods`). `map-explorer-to-mdim`'s `dimension_1..N` columns already hold
  display values, and `EXPLORER_DIMENSIONS` names them in order.
- **`hideControls=true` is appended to both URLs** to hide each side's own selector chrome
  (the selection is shown as chips above the chart). Click "open ↗" to see the view with
  controls.
- **The MDIM must be reachable on the configured host.** Defaults to production; for a
  staging branch's MDIMs use `--host https://staging-site-<branch>` (or have the reviewer
  edit Settings in the HTML).
- **The colleague's CSV is not the same format.** If a mapping was hand-made in a custom
  schema (e.g. multi-block header CSV), it must be converted to `mapping_proposal.csv` +
  `mapping_rules.py` first, or you can write a quick one-off adapter script in `ai/` and
  point this skill at the produced folder.
