# Desktop-app bundle of /create-figma-chart

`build_bundle.py` generates a variant of this skill that runs in the **Claude desktop app /
claude.ai** (Settings → Capabilities → Skills → Upload skill), where there is no ETL repo, no
`.venv`, no guaranteed shell network and no admin credentials. It rewrites or removes what can't
work there and zips the result in the format claude.ai expects.

## Rebuild

```bash
python3 .claude/skills/create-figma-chart/desktop/build_bundle.py
# → ai/create-figma-chart-desktop/create-figma-chart.zip (+ the staged tree beside it)
```

Any `python3` works — the script is stdlib-only. Rebuild and re-share the zip after substantive
edits to the skill; routine content edits flow through untouched, and an edit that moves one of the
transform anchors **fails the build loudly** — update the anchor in `build_bundle.py` and re-run.
The build also fails if any ETL-only reference (`.venv`, `/query-grapher-db`, `export://`, sibling
skills, …) survives into the bundle, if a relative link breaks, or if the frontmatter drifts from
claude.ai's rules (`description` ≤ 200 chars; `name` = folder name).

## What the desktop variant changes

- **SVG import**: the Claude Code flow (`curl` → `upload_assets` submitUrl POST) needs shell
  network, which the desktop sandbox may not have. The bundle's SKILL.md opens with an
  *Environment check* that probes once and, without network, falls back to handing the user the
  export URLs to download and drag into Figma (a dragged-in SVG arrives frame-wrapped exactly like
  an `upload_assets` import).
- **`verify_page.min.js`** is generated at build time — the commented `verify_page.js` outgrew
  `use_figma`'s 50k `code` cap, so the bundle ships a comment-stripped twin to paste. The repo's
  `test_verify_page.js` harness passes against the stripped file (checked in the build session;
  re-check after edits to `verify_page.js` by copying the min file over `verify_page.js` in a temp
  dir next to the harness).
- **Removed routes**: local SVG from `export://static_viz` (needs the ETL repo and
  `/create-static-viz`), bespoke visualizations (needs puppeteer + Chrome + an `owid-grapher`
  checkout), `verify_docs.py` and the `test_*.js` harnesses (maintenance-time tooling).
- **Sibling-skill hand-offs** (`/query-grapher-db`, `/adversarial-data-review`,
  `/check-metadata-*`, `/edit-faust-metadata`) are inlined as plain instructions: the public
  Datasette recipe, producer-documentation checking, `pip install codespell`, and "report upstream
  fixes to the chart's owner".
- **Admin-credentialed steps** (the 302-wide PNG export/upload through the OWID admin) are
  rewritten as steps the *user* performs.
- `python3 scripts/…` replaces every `.venv/bin/python .claude/skills/…` invocation; the two
  Python helpers are stdlib-only and run in the desktop code-execution sandbox.

## Installing (for teammates)

1. Download `create-figma-chart.zip`.
2. Claude desktop app / claude.ai → **Settings → Capabilities → Skills → Upload skill** → pick the
   zip. Connect the **Figma connector** under Settings → Connectors (required). Code execution is
   optional but strongly recommended, matching the skill's own Environment check — without it the
   run falls back to manual routes (the user downloads and drags the SVGs).
3. In a new chat, ask e.g. *"create a figma chart from https://ourworldindata.org/grapher/life-expectancy"*.

Known limits in the desktop app: no bespoke-viz route, the 302-thumbnail Cloudflare upload stays a
human step, and without sandbox network the SVG import is a download-and-drag by the user.
