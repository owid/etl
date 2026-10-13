# MDim simulator

A single HTML page for sketching a multidimensional chart (MDim) before it exists in ETL. You add dropdowns and their options, link an indicator to each combination, and see the result drawn by Grapher. When the setup looks right, "Copy MDim setup" gives a block of text in the shape of a `viz://chart` config, ready to hand to Claude.

Prototype: the page and its scripts may change without notice.

## Build it

```bash
.venv/bin/python apps/mdim_simulator/build.py              # writes ai/mdim_simulator/index.html
.venv/bin/python apps/mdim_simulator/build.py ~/Downloads/mdim_simulator.html
```

Open the file in a browser. **Charts need Tailscale**: Grapher loads from `owid-packages.tail6e23.ts.net`. Off Tailscale the page says so in place of the chart, and everything else still works.

## How it works

- `template.html` is the page: styles, markup and all the JavaScript.
- `build.py` fills the template's placeholders with the starting indicator's data and metadata, OWID's fonts and the Grapher package URL, so the page is one self-contained file.
- Indicators linked in the page are looked up live: a catalog path through the public Datasette, then data and metadata from `api.ourworldindata.org`. Chart links are refused, because the chart text comes from the indicator.
- The setup is saved in the browser (`localStorage`), so it survives a reload.

## Check it in a browser

```bash
.venv/bin/python apps/mdim_simulator/check_in_browser.py
```

Clicks through the page in headless Chromium (adding options, linking indicators, switching views, copying the setup, resizing and folding the panel) and saves screenshots to `ai/mdim_simulator/screenshots/`. It is not part of `make test`.
