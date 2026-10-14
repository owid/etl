# MDim simulator

A single HTML page for sketching a multidimensional chart (MDim) before it exists in ETL, for people who don't work in the ETL. You set up the dropdowns, link an indicator to each combination of options, and edit the chart the way readers will see it, drawn by the real Grapher. When it looks right, "Copy MDim setup" gives a block of text in the shape of a `viz://chart` config, ready to paste to Claude, who turns it into the ETL step.

People normally get here through the `/sketch-mdim` skill, which builds the page (pre-filled from chart links, catalog paths or indicator IDs when the request names them) and opens it.

Prototype: the page and its scripts may change without notice.

## Build it

```bash
.venv/bin/python apps/mdim_simulator/build.py                                    # an empty page, in ai/mdim_simulator/index.html
.venv/bin/python apps/mdim_simulator/build.py path/to/page.html --setup setup.yml # pre-filled from a setup
.venv/bin/python apps/mdim_simulator/build.py --check                            # can this machine reach Grapher?
```

A setup is the YAML the page copies out, so a copied setup reopens exactly as it was. Each page keeps its own work in the browser: reopening the file brings it back, and a page rebuilt with a different setup starts fresh.

Open the file in a browser. **Charts need Tailscale**: Grapher loads from `owid-packages.tail6e23.ts.net` (the `@ourworldindata/grapher` package, pinned in `build.py`). Off Tailscale the page says so in place of the chart, and everything else still works.

## What you can set

In the panel at the bottom of the page (fold it to one row, or drag its top edge to resize it):

- **Title**, an optional title variant, and **topic tags**. The first tag is the main topic, and the chart footer links to its page.
- **Chart**, for every view: which tabs exist (line, bar, slope, dumbbell, Marimekko, map; the table is always there), whether to hide the time range after the title, and whether readers change the country instead of adding countries.
- **Dropdowns** and their options, each with an optional description; options can sit under a group heading.
- **Combinations**: one row per combination of options, where you link an indicator by catalog path or ID (chart links are refused, because the chart text comes from the indicator), and tick the view that opens first.

On the chart itself:

- The **countries ticked** in Grapher's own panel are the MDim's default selection, for every view.
- The **tab that is open** is the MDim's default tab.

With **"✎ Edit FAUST and WYSK"**, for the view on screen: title, subtitle and footnote (the chart updates as you type), "What you should know" edited in place (drag a point's handle to move it), number rounding, and, on the Map tab, the palette and an automatic binning strategy. Each edited field can be undone, or flagged as a suggestion to change the indicator everywhere.

**"Manage WYSK"** lists the "What you should know" points of every linked view, each point once with the views it's on. A point can be edited or deleted on all its views or only some (editing on some forks it), and a new point can be added to the end of the views you pick.

Every view's y-axis starts at zero.

## What the copied setup contains

The dropdowns, the views and their catalog paths, `default_dimensions`, `default_selection`, `topic_tags`, the shared settings under `definitions.common_views`, and each view's own text, rounding and map colours. Only what someone changed is exported. Comments at the end list combinations with no indicator yet, each text change against the indicator's original, and any changes suggested for the indicator itself, which Claude checks against other charts before making. Specific map bins and the MDim's web address are settled in that conversation too.

## How it works

- `template.html` holds the markup and styles; `simulator.js` holds the page's code.
- `build.py` inlines both, with the pre-filled indicators' data and metadata, OWID's fonts, the valid topic tags (from `schemas/dataset-schema.json`) and their topic pages, and the map colour schemes from the vendored grapher schema, so the page is one self-contained file.
- Indicators linked in the page are looked up live: a catalog path through the public Datasette, then data and metadata from `api.ourworldindata.org`.
- The setup is saved in the browser (`localStorage`), so it survives a reload.

## Check it in a browser

```bash
.venv/bin/python apps/mdim_simulator/check_in_browser.py
```

Builds its own pages (an empty one, and one pre-filled with the newest "Number of people using the Internet"), clicks through them in headless Chromium, prints one line per check, and exits with an error if any fails. It ends by loading the copied setup with ETL's own chart loader, and by reopening it in a new page to check nothing is lost. Screenshots and the setup land in `ai/mdim_simulator/`. It is not part of `make test`.
