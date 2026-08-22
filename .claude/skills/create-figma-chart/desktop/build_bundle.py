"""Build the Claude-desktop-app (claude.ai skills) bundle of /create-figma-chart.

The desktop app has no ETL repo, no `.venv`, no guaranteed shell network and no admin
credentials, so this script generates an adapted copy of the skill and zips it in the
format claude.ai expects (the `create-figma-chart/` folder at the zip root, SKILL.md
frontmatter limited to `name` + `description` <= 200 chars).

Every adaptation is expressed as a marker-anchored operation on the repo files, so an
upstream edit that moves an anchor FAILS THE BUILD instead of silently shipping stale
or broken instructions. Content edits that don't touch an anchor flow through on the
next build with no work. See desktop/README.md for the operating instructions.

Usage (any python3, stdlib only):
    python3 .claude/skills/create-figma-chart/desktop/build_bundle.py
    python3 .claude/skills/create-figma-chart/desktop/build_bundle.py --out /tmp/somewhere
"""

from __future__ import annotations

import argparse
import re
import shutil
import sys
import zipfile
from pathlib import Path

SKILL_DIR = Path(__file__).resolve().parent.parent
REPO_ROOT = SKILL_DIR.parents[2]
BUNDLE_NAME = "create-figma-chart"
DEFAULT_OUT = REPO_ROOT / "ai" / "create-figma-chart-desktop"

TEXT_SUFFIXES = {".md", ".py", ".js", ".mjs", ".txt"}

# --------------------------------------------------------------------------- config

FRONTMATTER = """---
name: create-figma-chart
description: Turn an OWID grapher chart, MDim/explorer view, or narrative chart into a templated static chart in the design team's yearly Charts Figma file, with fitted layout, direct labels, and annotations.
---
"""

# Files that must not ship: ETL-repo-only routes and maintenance-time tooling.
DROP = [
    "BESPOKE-SVG.md",
    "scripts/bespoke_svg.mjs",  # needs puppeteer + Chrome + owid-grapher checkout
    "scripts/restyle_static_import.js",  # the export://static_viz route only
    "scripts/verify_docs.py",  # maintainer tool; shells out to git
    "scripts/test_diff_against_template.js",
    "scripts/test_measure_fit.js",
    "scripts/test_replay_chart_edits.js",
    "scripts/test_verify_page.js",
]

# (file, start_marker, end_marker, replacement). Cuts [start, end) — start included,
# end excluded; end=None cuts to EOF. Both markers must match exactly once.
CUTS = [
    # --- SKILL.md ---------------------------------------------------------------
    (
        "SKILL.md",
        "**Model check, before anything else:**",
        "**The single checkpoint rule:**",
        """**Environment check, before anything else.** This skill needs the **Figma connector**
(claude.ai → Settings → Connectors) — every read and write goes through its `use_figma` tool, and
the `figma-use` guidance that ships with it still applies. Code execution (the bash/python sandbox)
is optional but strongly recommended: it runs the helper scripts in `scripts/` and the `curl`
exports below. On the first shell command of a run, probe whether the sandbox can reach the
internet — `curl -sI "https://ourworldindata.org/grapher/life-expectancy.svg" | head -1` — and if
it cannot (or there is no sandbox at all), use the **manual import fallback** wherever a step
fetches or POSTs a file: give the user the exact URL as a clickable link, ask them to open it in a
browser, save the file, and drag it onto the Figma page you name. A dragged-in SVG arrives wrapped
in a FRAME exactly like an `upload_assets` import, so every unwrap/fit step below applies
unchanged. Say which route you're on in your first report. Prefer a strong model for a full build —
this skill is long chains of design judgment; a mechanical re-export or a single text fix can run
on a smaller one.

""",
    ),
    (
        "SKILL.md",
        "> **Paired skill — an update here may oblige an update there",
        "Two more sibling files own a route each",
        "",
    ),
    (
        "SKILL.md",
        "| [BESPOKE-SVG.md](BESPOKE-SVG.md) | the input is a **bespoke visualization**",
        "\nThis page is the **spine**",
        "",
    ),
    (
        "SKILL.md",
        "**This file has a size budget:",
        "Three sibling skills do the text work",
        """This bundle is generated from the `create-figma-chart` skill in OWID's `etl` repository
(its `desktop/build_bundle.py` builds it). Don't edit the bundle in place — report improvements to
the data team so they land upstream and every copy inherits them.

""",
    ),
    (
        "SKILL.md",
        "Three sibling skills do the text work",
        "## Round-trip budget",
        """Three text checks stand behind Step 8c: is every string **true of the indicator** (check each
claim against the producer's own documentation, reachable from the chart page's Sources tab), does
the wording follow **OWID's Writing and Style Guide**, and is the **spelling** American and
typo-free (`codespell`). Anything they turn up is an upstream fix in the chart's own metadata —
report it to the chart's owner rather than fixing it only in the image.

""",
    ),
    (
        "SKILL.md",
        "- **Or a local SVG already on disk** — typically",
        "- Optionally, **the DI/article text**",
        "",
    ),
    (
        "SKILL.md",
        "| **Bespoke component** (no slug, no `.svg`) | there is no endpoint",
        "| Admin link `/admin/charts/<id>/edit` |",
        "",
    ),
    (
        "SKILL.md",
        "| **Local SVG on disk** (from an `export://static_viz` step) | nothing to resolve",
        "\nThen pull the chart's texts",
        "",
    ),
    (
        "SKILL.md",
        "> **Local SVG on disk: there is nothing to export.**",
        "Two exports per format family:",
        "",
    ),
    (
        "SKILL.md",
        "> **Local-SVG route.** Two imports, and neither is an embed:",
        "## Step 6 — Fill the template texts",
        "",
    ),
    # --- reference/FITTING.md -----------------------------------------------------
    (
        "reference/FITTING.md",
        "> **Local-SVG route: nothing is *fitted*, but it still has to be scaled.**",
        "### The x-map is not optional",
        "",
    ),
    (
        "reference/FITTING.md",
        "### Restyling a local-SVG import to OWID's fonts and colors",
        "**Measure that band; don't hardcode it.**",
        "",
    ),
    # --- reference/GOTCHAS.md -------------------------------------------------------
    (
        "reference/GOTCHAS.md",
        "- **A local SVG from an `export://static_viz` step has none of those names.**",
        "- **Restyling an imported chart's text to Lato:",
        "",
    ),
    (
        "reference/GOTCHAS.md",
        "- **A step's desktop and mobile SVGs are meant to stay paired**",
        "- **Raising `imFontSize` makes grapher drop labels",
        "",
    ),
    (
        "reference/GOTCHAS.md",
        "- **A bespoke component mounted outside a Shadow DOM renders unstyled, silently.**",
        "- **",
        "",
    ),
    (
        "reference/GOTCHAS.md",
        "- **`verify_docs.py --against` takes a git ref, not a path.**",
        "## Re-checking a row outside its script",
        "",
    ),
    # --- reference/TEXTS.md ---------------------------------------------------------
    (
        "reference/TEXTS.md",
        "  So establish it in this order:",
        "  > **First check whether the template already gives the source its own row",
        """  Establishing which producer's data actually fills the window takes OWID's pipeline code (the
  garden step's splice logic and the snapshot scripts), which is not readable from here — and
  **matching values do not identify a source**: two producers reporting the same figure for the
  same country-year is common. So the rule in this bundle is: **don't drop a producer yourself.**
  If the citation looks over-inclusive for the cropped window, flag it to the chart's owner with
  the window and the reasoning, and keep the full citation until they confirm. If they confirm a
  drop, note that the interactive chart's footer will still list every producer, and re-measure the
  footer afterwards — a shorter line can collapse a planned two-row footer back to the template's
  single row, which is worth ~20px of chart.

""",
    ),
]

# (file, old, new) — `old` must match exactly once, before global replacements run.
REPLACEMENTS = [
    # --- SKILL.md -------------------------------------------------------------------
    (
        "SKILL.md",
        "Two more sibling files own a route each, and both replace rather than supplement the steps below:",
        "One sibling file owns a route of its own, and it replaces rather than supplements the steps below:",
    ),
    (
        "SKILL.md",
        "Measuring the band, importing the embed, unwrapping and scaling. The local-SVG restyle route. |",
        "Measuring the band, importing the embed, unwrapping and scaling. |",
    ),
    (
        "SKILL.md",
        "`scripts/verify_page.js` runs the mechanical rows in one call and declares what it cannot judge;",
        "`scripts/verify_page.js` runs the mechanical rows in one call and declares what it cannot judge"
        " (paste its comment-stripped twin `scripts/verify_page.min.js` — the commented original is over"
        " `use_figma`'s 50k cap);",
    ),
    (
        "SKILL.md",
        "Issue them together, then `curl` all the returned URLs in one bash call, then Read each.",
        "Issue them together, then fetch all the returned URLs in one bash call and read each"
        " (skip the fetch where the app already renders the screenshot inline).",
    ),
    (
        "SKILL.md",
        "on the public Datasette (see the `query-grapher-db` skill), or `GET /admin/api/charts/<id>.config.json` — then use",
        "on the public Datasette (recipe below the table), or `GET /admin/api/charts/<id>.config.json`"
        " (needs an admin login — ask the user to fetch it) — then use",
    ),
    (
        "SKILL.md",
        "Then pull the chart's texts, which seed the template texts in Step 6.",
        """> **The public Datasette, wherever a row above says so:** production's grapher database is
> mirrored read-only at `https://datasette-public.owid.io` — public, no auth. Fetch
> `https://datasette-public.owid.io/owid.json?sql=<url-encoded SQL>` (curl in the sandbox, or the
> web-fetch tool). It is DuckDB-backed: use `json_extract_string(col, '$.path')` for JSON fields
> and `TRY_CAST` for numerics. The mirror lags production by days — the narrative-chart rows above
> say what to do when an id isn't mirrored yet.

Then pull the chart's texts, which seed the template texts in Step 6.""",
    ),
    (
        "SKILL.md",
        "> Two details the command has to get right: the footer group sits **after** `chart-area` in the document, so a slice that stops at the plot returns the title and subtitle only and quietly loses the source and note; and the SVG carries XML entities (`&#x27;` for an apostrophe), so unescape before pasting.",
        """> Two details the command has to get right: the footer group sits **after** `chart-area` in the document, so a slice that stops at the plot returns the title and subtitle only and quietly loses the source and note; and the SVG carries XML entities (`&#x27;` for an apostrophe), so unescape before pasting.
>
> No sandbox? Step 4's proposal needs every one of these texts before anything touches Figma, so
> gather them pre-approval: fetch the same `.svg` URL with the web-fetch tool and read the strings
> straight from its markup — title and subtitle inside `<g id="header">`, sources and note inside
> `<g id="footer">`, with the same two details as above. If no fetch tool can reach the file, ask
> the user to open the chart in a browser and paste the four texts. After the Step 5 import the
> same strings are one `use_figma` read away (TEXT nodes under those two groups) — use that read
> to double-check what was pasted, not to gather.""",
    ),
    (
        "SKILL.md",
        'verify it against `rg "producer: .*<name>" snapshots/` if you\'re unsure',
        "verify it against the producer's own site (or the chart page's Sources tab) if you're unsure",
    ),
    (
        "SKILL.md",
        "the view's resolved `selectedEntityNames` read from the grapher DB — `multi_dim_x_chart_configs mx JOIN chart_configs cc ON cc.id = mx.chartConfigId` (`/query-grapher-db`)",
        "the view's resolved `selectedEntityNames` read from the public Datasette — `multi_dim_x_chart_configs mx JOIN chart_configs cc ON cc.id = mx.chartConfigId`",
    ),
    (
        "SKILL.md",
        "One `AskUserQuestion` batch — don't drip-feed:",
        "Ask everything in **one message** — don't drip-feed:",
    ),
    (
        "SKILL.md",
        "DIR=/tmp/figma-chart && mkdir -p $DIR   # or the session scratchpad",
        "DIR=figma-chart && mkdir -p $DIR   # any scratch dir in the sandbox working directory",
    ),
    (
        "SKILL.md",
        "`imWidth`/`imHeight` set the **aspect ratio only** — the server renormalizes",
        """**No sandbox network?** (the Environment check's probe failed): give the user those same URLs
as clickable links with the filenames to save (`original.svg`, and `original_square.svg` when a
square format is in the run — the embed URL comes later, in Step 7), and ask them only to
download the files for now: the page they will drag onto doesn't exist until Step 5 creates it,
after the Step 4 approval — request the drag there. The sanity checks below then run after that
import, as `use_figma` reads of the imported node — its size and child count — instead of
`head`/`grep` on a local file.

`imWidth`/`imHeight` set the **aspect ratio only** — the server renormalizes""",
    ),
    (
        "SKILL.md",
        """> Run it from the repo root through the venv — `.venv/bin/python .claude/skills/create-figma-chart/scripts/solve_export.py …`;
> it is committed non-executable like the rest of that directory.""",
        """> Run it with any `python3` — `python3 scripts/solve_export.py …` (stdlib-only, it ships in this
> bundle). No sandbox? The closed-form solution is documented in the script's own header — read it
> and do the arithmetic in-context.""",
    ),
    (
        "SKILL.md",
        "(`json_extract_string(cc.full, '$.chartTypes')` on the public Datasette — `/query-grapher-db`)",
        "(`json_extract_string(cc.full, '$.chartTypes')` on the public Datasette)",
    ),
    (
        "SKILL.md",
        """`US` and `UK` are settled: the Writing and Style Guide rules on those two, without periods
([STYLE_GUIDE.md](../check-metadata-style/STYLE_GUIDE.md)).""",
        "`US` and `UK` are settled: OWID's Writing and Style Guide rules on those two, without periods.",
    ),
    (
        "SKILL.md",
        """```bash
curl -s -X POST "<submitUrl>" -F "file=@$DIR/original.svg;type=image/svg+xml"
# → {"success":true, ..., "placedOnNodeId":"<id>"}
```""",
        """```bash
curl -s -X POST "<submitUrl>" -F "file=@$DIR/original.svg;type=image/svg+xml"
# → {"success":true, ..., "placedOnNodeId":"<id>"}
```

   **No sandbox network?** Skip `upload_assets` entirely: the user drags the saved SVG onto the
   page (Environment check). A dragged-in SVG also arrives wrapped in a FRAME named after the
   file — find it as the page's newest child, verify by name and size, and use its id wherever the
   steps below say `placedOnNodeId`.""",
    ),
    (
        "SKILL.md",
        "Measure the band off the *filled* clone, export the embed to that aspect, import it, unwrap the frame, and scale it in. Covers the local-SVG restyle route and `scripts/restyle_static_import.js`.",
        "Measure the band off the *filled* clone, export the embed to that aspect, import it, unwrap"
        " the frame, and scale it in. (No sandbox network? The solved embed URL goes to the user to"
        " download and drag in, like the original.)",
    ),
    (
        "SKILL.md",
        """   For a **302-wide thumbnail** the export is part of the deliverable rather than optional, and it has its own route: `GET /api/figma/image?fileId=<key>&nodeId=<node>` on the OWID admin (`adminSiteServer/apiRoutes/figma.ts`) calls the Figma API at `scale: 3`, then `POST /api/images` uploads it to Cloudflare Images. PNG only — `ACCEPTED_IMG_TYPES` rejects SVG. See SMALL-CHARTS.md → Delivery for the naming rules and the retina reason for 3×.""",
        """   For a **302-wide thumbnail** the export is part of the deliverable rather than optional, and it goes through the OWID admin, which needs admin credentials — so it is the **user's** step: the admin pulls the frame from Figma at `scale: 3` and uploads the PNG to Cloudflare Images (PNG only; SVG is rejected). See SMALL-CHARTS.md → Delivery for the naming rules, what to hand the user, and the retina reason for 3×.""",
    ),
    # --- reference/NODE-MAP.md --------------------------------------------------------
    (
        "reference/NODE-MAP.md",
        "The per-slot *positions* for the four static templates live in [`/create-static-viz`'s TEMPLATES.md](../../create-static-viz/TEMPLATES.md); these are the type sizes you are filling into, which Step 6 needs and which no other file records for the non-static families.",
        "The per-slot *positions* are not recorded here — measure them off your template clone when a"
        " step needs them; these are the type sizes you are filling into, which Step 6 needs and which"
        " no other file records.",
    ),
    # --- reference/FITTING.md -----------------------------------------------------------
    (
        "reference/FITTING.md",
        """asking what question the copy would answer; if you cannot name one, don't place it. In
`scripts/restyle_static_import.js` that decision is the job's `reference` field: omit it, with
`referenceGap`, and the pass places no copy — a *wrong* id there is still an error, so a typo cannot
pass itself off as this decision. The `original — <slug>` reference from Step 5 stays either way""",
        """asking what question the copy would answer; if you cannot name one, don't place it. The
`original — <slug>` reference from Step 5 stays either way""",
    ),
    (
        "reference/FITTING.md",
        "The table gives one number per template — the band you fit a chart into — and that is deliberately all it gives. **Per-slot geometry for the four static templates** (each text slot's own `y`/width/height, the derived positions, unit conversions, the exact footer strings) belongs to [`/create-static-viz`'s TEMPLATES.md](../../create-static-viz/TEMPLATES.md), which needs it to place text without opening Figma. Read it there rather than re-measuring into this file: two copies of a measurement drift, and the copy a session happens to read then decides which one was right.",
        "The table gives one number per template — the band you fit a chart into — and that is"
        " deliberately all it gives. **Per-slot geometry** (each text slot's own `y`/width/height) is"
        " deliberately not recorded here: two copies of a measurement drift, so measure it off your"
        " clone when a step needs it.",
    ),
    # --- reference/GOTCHAS.md -------------------------------------------------------------
    (
        "reference/GOTCHAS.md",
        """- **`use_figma`'s `code` parameter caps at 50,000 bytes**, and `verify_page.js` is ~59KB with its
  comments. It has to be comment-stripped to run: drop the header block and every whole-line `//`
  comment (never inline ones — a URL or a regex can contain `//`), which took it to ~37KB. Worth
  knowing before you plan a run around it, and worth remembering when adding to any of these scripts:
  past the cap, a script cannot be executed at all.""",
        """- **`use_figma`'s `code` parameter caps at 50,000 bytes**, and `verify_page.js` is well over it
  with its comments. This bundle ships the comment-stripped twin `verify_page.min.js` — paste that
  one, and read the commented original for the CONFIG documentation. Worth remembering for any
  script here: past the cap, a script cannot be executed at all.""",
    ),
    (
        "reference/GOTCHAS.md",
        """ The harnesses are the real gate — they wrap the source the same way the tool does —
  so run `node test_<name>.js`, never `node --check <name>.js`.""",
        """ They are made to be pasted into
  `use_figma`, not run locally; their test harnesses stay in the skill's source repo.""",
    ),
    (
        "reference/GOTCHAS.md",
        "This is the same failure the harnesses exist to prevent, arriving through the side door.",
        "This is the same failure the scripts' test harnesses (kept in the skill's source repo) exist"
        " to prevent, arriving through the side door.",
    ),
    # --- reference/CHECKS.md ------------------------------------------------------------------
    (
        "reference/CHECKS.md",
        "> **[`scripts/verify_page.js`](../scripts/verify_page.js) runs the MECHANICAL rows in ONE read-only",
        "> **[`scripts/verify_page.js`](../scripts/verify_page.js) — paste its comment-stripped twin"
        " [`verify_page.min.js`](../scripts/verify_page.min.js), the commented original is over"
        " `use_figma`'s 50k cap — runs the MECHANICAL rows in ONE read-only",
    ),
    (
        "reference/CHECKS.md",
        """colour-vision and grayscale (`color_audit.py`), spelling (`codespell`), the data-truth row
> (`/adversarial-data-review`), entity completeness""",
        """colour-vision and grayscale (`color_audit.py`), spelling (`codespell`), the data-truth row
> (checking every claim against the producer's documentation), entity completeness""",
    ),
    (
        "reference/CHECKS.md",
        """Its harness is
> [`scripts/test_diff_against_template.js`](../scripts/test_diff_against_template.js) (**49**
> assertions), which found""",
        """Its test harness (49
> assertions, kept in the skill's source repo) found""",
    ),
    (
        "reference/CHECKS.md",
        """then a stubbed-figma harness ([`scripts/test_verify_page.js`](../scripts/test_verify_page.js),
> `node` it after any edit) covering **137** assertions including the rows that are awkward to plant on a
> real page.""",
        """then a stubbed-figma harness (137 assertions,
> kept in the skill's source repo) covering the rows that are awkward to plant on a real page.""",
    ),
    (
        "reference/CHECKS.md",
        "| Spelling and prose | `.venv/bin/codespell` over the texts, plus a read against the style guide | American spelling (CLAUDE.md), no typos, no style-guide breaches — see below |",
        "| Spelling and prose | `codespell` over the texts (`pip install codespell` in the sandbox; a"
        " careful slow read if you can't), plus a read against the style guide | American spelling,"
        " always; no typos, no style-guide breaches — see below |",
    ),
    (
        "reference/CHECKS.md",
        "| The text is *true* of the indicator | `/adversarial-data-review` on the dataset behind the chart |",
        "| The text is *true* of the indicator | check every string against the producer's own"
        " documentation, reachable from the chart page's Sources tab |",
    ),
    (
        "reference/CHECKS.md",
        "a member missing its latest year is dropped silently (`/check-empty-entities` is the pipeline sweep) |",
        "a member missing its latest year is dropped silently |",
    ),
    (
        "reference/CHECKS.md",
        "Either way the source chart isn't pinned to an old year (`/check-hardcoded-years`) |",
        "Either way the source chart isn't pinned to an old year |",
    ),
    (
        "reference/CHECKS.md",
        "Run `.venv/bin/codespell` over the strings (it is a dev dependency; `/check-metadata-typos` covers the same ground on `.meta.yml` and `.dvc`). American spelling always, per CLAUDE.md, including in text copied out of a chart. For the wording itself, `/check-metadata-style` holds the Writing and Style Guide, whose FAUST rules govern exactly the strings this skill moves.",
        "Run `codespell` over the strings (`pip install codespell` in the sandbox), or read them"
        " slowly if you have none. American spelling always — OWID house style — including in text"
        " copied out of a chart. For the wording itself, OWID's Writing and Style Guide governs"
        " exactly the strings this skill moves; flag breaches to the chart's owner.",
    ),
    (
        "reference/CHECKS.md",
        "- **Whether the text is true.** Run **`/adversarial-data-review`** on the dataset behind the chart, over the data *and* every string in the frame that says something about it. That skill fetches the producer's own documentation from the snapshot's links and treats each sentence as a claim to be refuted, which is the right posture for text about to be published as an image. Its scope here is **everything, not just the FAUST**:",
        "- **Whether the text is true.** Check the data *and* every string in the frame that says"
        " something about it against the producer's own documentation — fetch it from the links on"
        " the chart page's Sources tab — and treat each sentence as a claim to be refuted, which is"
        " the right posture for text about to be published as an image. The scope is **everything,"
        " not just the FAUST**:",
    ),
    (
        "reference/CHECKS.md",
        "`/check-metadata-spacing` is the pipeline check for this; here it is enough to read the placed strings once for spacing,",
        "Read the placed strings once for spacing,",
    ),
    (
        "reference/CHECKS.md",
        "`/check-empty-entities` is the pipeline sweep for this class; the local version is Step 1's rule",
        "The check is Step 1's rule",
    ),
    (
        "reference/CHECKS.md",
        "**A pinned year, and a frozen image.** `/check-hardcoded-years` exists because a chart pinned to `maxTime: 2019` quietly stops showing new data.",
        "**A pinned year, and a frozen image.** A chart pinned to `maxTime: 2019` quietly stops showing new data.",
    ),
    (
        "reference/CHECKS.md",
        "Route the fix through `/edit-faust-metadata`, always, and don't pick the layer yourself: that skill decides which layer the field actually lives in (garden `.meta.yml`, an MDim's yaml, or the chart config on staging) and reports which *other* charts inherit the same string before anything changes. Editing the garden file directly because it looked like the obvious home is how a one-chart correction silently rewrites text on a dozen others. Report the finding, hand it over, and hold the image",
        "Report the finding to the chart's owner and let them route it through OWID's metadata"
        " pipeline — the same string is often inherited by other charts, so the fix has a blast"
        " radius only the pipeline side can see. Hand it over, and hold the image",
    ),
    (
        "reference/CHECKS.md",
        "If they accept the deviation, record it in the report; chart-side work goes in the handover doc and reusable mechanics go in this skill.",
        "If they accept the deviation, record it in the report; chart-side work goes in the handover"
        " note and reusable mechanics belong upstream in this skill's source — report them to the"
        " data team.",
    ),
    (
        "reference/FITTING.md",
        "`measure_fit.js` now keeps a zero-area node when any of its strokes is\n  visible; `test_measure_fit.js` case 8 is the regression. Note the asymmetry with `verify_page.js`,",
        "`measure_fit.js` now keeps a zero-area node when any of its strokes is\n  visible (its test harness, in the skill's source repo, pins the regression). Note the asymmetry with `verify_page.js`,",
    ),
    (
        "reference/FITTING.md",
        "The per-slot text positions behind these bands are `TEMPLATES.md`'s.",
        "The per-slot text positions behind these bands are deliberately unrecorded — measure them off your clone.",
    ),
    (
        "reference/NODE-MAP.md",
        " TEMPLATES.md carries the measured widths.",
        "",
    ),
    (
        "reference/LABELING.md",
        """  Harness: [`scripts/test_replay_chart_edits.js`](../scripts/test_replay_chart_edits.js) (49
  assertions, mostly asserting the ordering rather than the arithmetic). Then re-run Step 8c""",
        """  (Its test harness — 49 assertions, mostly asserting the ordering rather than the arithmetic —
  is kept in the skill's source repo.) Then re-run Step 8c""",
    ),
    (
        "scripts/measure_fit.js",
        """// TESTED by scripts/test_measure_fit.js — a stubbed-figma harness, since this file executes only
// inside Figma. Run it after any edit here:  node .claude/skills/create-figma-chart/scripts/test_measure_fit.js""",
        """// TESTED by a stubbed-figma harness kept in the skill's source repo, since this file executes
// only inside Figma.""",
    ),
    (
        "scripts/verify_page.js",
        "// `/adversarial-data-review`; the entity-completeness row needs the EFFECTIVE selection from",
        "// checking claims against the producer's documentation; the entity-completeness row needs the EFFECTIVE selection from",
    ),
    (
        "scripts/verify_page.js",
        '"/check-hardcoded-years"',
        '"the source chart\'s saved config (maxTime)"',
    ),
    # --- GUIDELINES.md ------------------------------------------------------------------------
    (
        "GUIDELINES.md",
        "**Where it goes: the report, and the handover doc when there is chart-side work to hand back (`ai/<topic>/…md`).**",
        "**Where it goes: the report, and the handover note when there is chart-side work to hand back.**",
    ),
    # --- SMALL-CHARTS.md ------------------------------------------------------------------------
    (
        "SMALL-CHARTS.md",
        """every text claim being true of the indicator (`/adversarial-data-review`, `/check-metadata-style`,
`/check-metadata-typos`).""",
        """every text claim being true of the indicator (checked against the producer's own documentation,
the OWID style guide, and codespell — see CHECKS.md → Checking the words).""",
    ),
    (
        "SMALL-CHARTS.md",
        """2. **Export at 3×.** The admin already has a Figma path —
   `GET /api/figma/image?fileId=<key>&nodeId=<node>` (`adminSiteServer/apiRoutes/figma.ts`) calls the
   Figma API at `scale: 3`, giving **906 × 3H**. This matters: `getSizes(302)` yields
   `[48, 100, 302]`, so the largest srcset candidate at 1× is 302w and a 2× display upscales it.
   Note `get_screenshot` **cannot** do this — `maxDimension` only ever downscales, and clamps at the
   node's natural size.
3. **Upload** via the admin (`POST /api/images`), then reference the filename in the block's `image:`
   field.""",
        """2. **Export at 3×.** The OWID admin has a Figma path —
   `GET /api/figma/image?fileId=<key>&nodeId=<node>` calls the Figma API at `scale: 3`, giving
   **906 × 3H**. This matters: the largest srcset candidate at 1× is 302w and a 2× display upscales
   it. It needs admin credentials, so **this step is the user's** — hand them the file key, the
   frame's node id and the filename rule above (exporting at 3× from Figma's own export panel works
   too). Note `get_screenshot` **cannot** substitute — `maxDimension` only ever downscales, and
   clamps at the node's natural size.
3. **Upload** via the admin's Images page (`POST /api/images` under the hood) — also the user's
   step — then have them reference the filename in the block's `image:` field.""",
    ),
    # --- scripts ------------------------------------------------------------------------------
    (
        "scripts/color_audit.py",
        "Usage (run from the repo root, with the repo interpreter):",
        "Usage (any python3 — stdlib only):",
    ),
    (
        "scripts/verify_page.js",
        '".venv/bin/codespell + /check-metadata-style"',
        '"codespell + the OWID style guide"',
    ),
    (
        "scripts/verify_page.js",
        '"/adversarial-data-review"',
        '"checking each claim against the producer\'s documentation"',
    ),
    (
        "scripts/verify_page.js",
        '"Step 1\'s table + /query-grapher-db"',
        '"Step 1\'s table + the public Datasette"',
    ),
]

# Applied to every text file, after CUTS and REPLACEMENTS; no match-count requirement.
GLOBAL_REPLACEMENTS = [
    (".venv/bin/python .claude/skills/create-figma-chart/scripts/", "python3 scripts/"),
    (".venv/bin/python", "python3"),
    (".claude/skills/create-figma-chart/scripts/", "scripts/"),
]

# Any of these left anywhere in the bundle fails the build. Keep patterns specific
# enough not to hit legitimate text (e.g. the word "test" alone).
FORBIDDEN = [
    r"\.venv",
    r"\.claude/skills",
    r"etl/steps",
    r"export://",
    r"create-static-viz",
    r"query-grapher-db",
    r"BESPOKE",
    r"[Bb]espoke",
    r"restyle_static_import",
    r"verify_docs",
    r"adversarial-data-review",
    r"check-metadata-style",
    r"check-metadata-typos",
    r"check-metadata-spacing",
    r"check-empty-entities",
    r"check-hardcoded-years",
    r"edit-faust-metadata",
    r"test_verify_page|test_measure_fit|test_replay_chart_edits|test_diff_against_template",
    r"TEMPLATES\.md",
    r"CLAUDE\.md",
    r"snapshots/",
    r"AskUserQuestion",
    r"puppeteer",
    r"ai/<topic>",
]

MIN_JS_SOURCE = "scripts/verify_page.js"
MIN_JS_TARGET = "scripts/verify_page.min.js"
USE_FIGMA_CODE_CAP = 50_000

# ------------------------------------------------------------------------- machinery


class BuildError(Exception):
    pass


def apply_cut(text: str, start: str, end: str | None, replacement: str, label: str) -> str:
    if text.count(start) != 1:
        raise BuildError(f"{label}: start marker matches {text.count(start)} times (need 1): {start[:80]!r}")
    i = text.index(start)
    if end is None:
        return text[:i] + replacement
    tail = text[i + len(start) :]
    if tail.count(end) < 1:
        raise BuildError(f"{label}: end marker not found after start: {end[:80]!r}")
    j = tail.index(end)
    return text[:i] + replacement + tail[j:]


def apply_replacement(text: str, old: str, new: str, label: str) -> str:
    n = text.count(old)
    if n != 1:
        raise BuildError(f"{label}: replacement source matches {n} times (need 1): {old[:80]!r}")
    return text.replace(old, new)


def strip_frontmatter(text: str, label: str) -> str:
    m = re.match(r"^---\n.*?\n---\n", text, flags=re.S)
    if not m:
        raise BuildError(f"{label}: no frontmatter block found to replace")
    return text[m.end() :]


def make_min_js(source: str) -> str:
    """Drop whole-line // comments (never inline ones) and collapse the blank runs left behind."""
    kept: list[str] = []
    for line in source.splitlines():
        if line.lstrip().startswith("//"):
            continue
        kept.append(line)
    out: list[str] = []
    for line in kept:
        if line.strip() == "" and out and out[-1].strip() == "":
            continue
        out.append(line)
    return "\n".join(out).strip() + "\n"


def bundle_files(staging: Path) -> list[Path]:
    return sorted(p for p in staging.rglob("*") if p.is_file())


def lint_forbidden(staging: Path) -> list[str]:
    findings = []
    for path in bundle_files(staging):
        if path.suffix not in TEXT_SUFFIXES:
            continue
        rel = path.relative_to(staging)
        for i, line in enumerate(path.read_text().splitlines(), start=1):
            for pat in FORBIDDEN:
                if re.search(pat, line):
                    findings.append(f"{rel}:{i}: /{pat}/ -> {line.strip()[:120]}")
    return findings


def lint_links(staging: Path) -> list[str]:
    findings = []
    for path in bundle_files(staging):
        if path.suffix != ".md":
            continue
        rel = path.relative_to(staging)
        for i, line in enumerate(path.read_text().splitlines(), start=1):
            for target in re.findall(r"\]\(([^)\s]+)\)", line):
                if target.startswith(("http://", "https://", "#", "mailto:")):
                    continue
                target_path = (path.parent / target.split("#")[0]).resolve()
                if not target_path.exists():
                    findings.append(f"{rel}:{i}: broken relative link -> {target}")
    return findings


def check_frontmatter(staging: Path) -> list[str]:
    findings = []
    text = (staging / "SKILL.md").read_text()
    m = re.match(r"^---\n(.*?)\n---\n", text, flags=re.S)
    if not m:
        return ["SKILL.md: no frontmatter"]
    fields = dict(
        (k.strip(), v.strip()) for k, v in (line.split(":", 1) for line in m.group(1).splitlines() if ":" in line)
    )
    if fields.get("name") != BUNDLE_NAME:
        findings.append(f"frontmatter name is {fields.get('name')!r}, expected {BUNDLE_NAME!r}")
    desc = fields.get("description", "")
    if not desc:
        findings.append("frontmatter has no description")
    elif len(desc) > 200:
        findings.append(f"description is {len(desc)} chars (claude.ai caps it at 200)")
    extra = set(fields) - {"name", "description", "dependencies"}
    if extra:
        findings.append(f"unexpected frontmatter fields for claude.ai: {sorted(extra)}")
    return findings


def build(out_dir: Path) -> Path:
    staging = out_dir / BUNDLE_NAME
    resolved = staging.resolve()
    if resolved.is_relative_to(SKILL_DIR) or SKILL_DIR.is_relative_to(resolved):
        raise BuildError(
            f"--out {out_dir} stages to {resolved}, which aliases the source skill at {SKILL_DIR} — "
            "the build would delete or re-copy its own source. Pick a directory outside the skill."
        )
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)

    # 1. copy the tree, minus drops, caches and this desktop/ dir itself
    dropped = set(DROP)
    for src in sorted(SKILL_DIR.rglob("*")):
        rel = src.relative_to(SKILL_DIR)
        parts = rel.parts
        if not src.is_file() or parts[0] == "desktop" or "__pycache__" in parts:
            continue
        if str(rel) in dropped:
            dropped.discard(str(rel))
            continue
        dst = staging / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, dst)
    if dropped:
        raise BuildError(f"DROP entries not found in the skill dir (renamed upstream?): {sorted(dropped)}")

    # 2. marker-anchored cuts, then one-shot replacements, on the copies
    for relpath, start, end, replacement in CUTS:
        path = staging / relpath
        path.write_text(apply_cut(path.read_text(), start, end, replacement, f"CUT {relpath}"))
    for relpath, old, new in REPLACEMENTS:
        path = staging / relpath
        path.write_text(apply_replacement(path.read_text(), old, new, f"REPLACE {relpath}"))

    # 3. global path rewrites on every text file
    for path in bundle_files(staging):
        if path.suffix not in TEXT_SUFFIXES:
            continue
        text = path.read_text()
        for old, new in GLOBAL_REPLACEMENTS:
            text = text.replace(old, new)
        path.write_text(text)

    # 4. desktop frontmatter on SKILL.md
    skill = staging / "SKILL.md"
    skill.write_text(FRONTMATTER + strip_frontmatter(skill.read_text(), "SKILL.md"))

    # 5. the pasteable verify_page twin, under the use_figma code cap
    min_js = make_min_js((staging / MIN_JS_SOURCE).read_text())
    if len(min_js.encode()) >= USE_FIGMA_CODE_CAP:
        raise BuildError(f"{MIN_JS_TARGET} is {len(min_js.encode())} bytes; must stay under {USE_FIGMA_CODE_CAP}")
    (staging / MIN_JS_TARGET).write_text(min_js)

    # 6. gates
    problems = lint_forbidden(staging) + lint_links(staging) + check_frontmatter(staging)
    if problems:
        raise BuildError("bundle failed its gates:\n  " + "\n  ".join(problems))

    # 7. zip with the folder at the root, as claude.ai expects
    zip_path = out_dir / f"{BUNDLE_NAME}.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for path in bundle_files(staging):
            zf.write(path, Path(BUNDLE_NAME) / path.relative_to(staging))
    return zip_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT, help=f"output directory (default: {DEFAULT_OUT})")
    args = parser.parse_args()
    try:
        zip_path = build(args.out)
    except BuildError as exc:
        print(f"BUILD FAILED\n{exc}", file=sys.stderr)
        return 1
    staged = args.out / BUNDLE_NAME
    n_files = len(bundle_files(staged))
    size_kb = zip_path.stat().st_size / 1024
    skill_lines = len((staged / "SKILL.md").read_text().splitlines())
    print(f"OK: {zip_path} ({size_kb:.0f} KB, {n_files} files, SKILL.md {skill_lines} lines)")
    print(f"Staged tree for inspection: {staged}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
