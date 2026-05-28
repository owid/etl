---
name: data-report
description: Create a data report — a self-contained HTML write-up of analysis or takeaways from data exploration — and publish it to the owid/data-reports repo (served at data-reports.owid.io). Use when the user wants to turn an exploration, finding, or analysis into a shareable internal report.
metadata:
  internal: true
---

# Create a Data Report

A **data report** is a bit of analysis or a set of takeaways from data exploration,
written up as a self-contained HTML page and published to the internal
[`owid/data-reports`](https://github.com/owid/data-reports) site
(**data-reports.owid.io**). Reports are not necessarily evergreen — this is a
low-friction place to share findings.

This skill scaffolds the report folder, writes its `metadata.yaml` and
`index.html`, validates it, lets the user preview locally, and (on request)
commits and opens a PR.

## How the data-reports repo works

You must respect this structure exactly — the site discovers reports at request
time, with **no build step**:

```
data-reports/
  reports/
    <slug>/                # kebab-case, identifies the report
      metadata.yaml        # title, authors, date, summary, tags, status
      index.html           # the report — self-contained
      css/ js/ data/ ...   # any local images / css / js / json the report needs
      assets/              # OPTIONAL: notebooks, build scripts, raw exports
                           # — anything that explains how the report was made
                           # but isn't required for it to render
  server.py                # renders the landing page from each metadata.yaml
  Makefile                 # `make new`, `make serve`, `make check`
```

- The **landing page** (`/`) renders one card per report from its `metadata.yaml`.
- Each report is served as-is at **`/reports/<slug>/`**. The public share link is
  `http://data-reports.owid.io/reports/<slug>/`.
- `index.html` should be **self-contained**: inline CSS/JS, or assets stored in
  the report's own folder. Don't depend on the landing-page styles.
- **`assets/` is the conventional home for provenance** — Jupyter notebooks
  (`.ipynb`), build scripts, raw CSV/parquet exports, and anything else that
  documents *how* the report was produced. Files there are served by data-reports
  too (everything under `/reports/<slug>/` is static), so you can link to them
  from `index.html` (e.g. `<a href="assets/analysis.ipynb">Notebook</a>`). Put
  files the report **needs to render** in `data/` / `css/` / `js/`; put files
  that just explain the work in `assets/`.

## Locate (or clone) the data-reports repo

Reports live in the `data-reports` repo, not in `etl`. Find it:

1. Check for a sibling checkout: the directory `../data-reports` relative to the
   etl repo root (i.e. alongside `etl/`). Most OWID setups have it there.
2. If not present, clone it next to the etl repo:
   ```bash
   git clone git@github.com:owid/data-reports.git ../data-reports
   ```

Run all `make`/file operations below **inside the data-reports checkout**, not in `etl`.

## Workflow

### 1. Gather the report's metadata

Collect (infer sensible defaults from the conversation / current analysis, then confirm):

| Field | Notes |
|-------|-------|
| `slug` | kebab-case identifier, e.g. `child-mortality-revisions-2026`. Becomes the folder name and URL. |
| `title` | Short, descriptive. Shown on the card and as the page title. |
| `authors` | List. Default to the current git user (`git config user.name`); confirm. |
| `date` | `YYYY-MM-DD`. Default to today (`date -u +%Y-%m-%d`). |
| `summary` | 1–2 sentences for the landing-page card. |
| `tags` | Optional list, **1–3 tags**. Stay broad (`population`, `health`, `methodology`) — they help search/filtering on the landing page, but a wall of narrow tags makes the cards noisy. Don't add a "draft" tag — `status` covers that. |
| `status` | `draft` or `published`. Default `draft` until the user is happy. |

Confirm these with the user before writing files.

### 2. Scaffold the report folder

From the data-reports checkout, use the built-in scaffolder (preferred — it
creates the folder, a starter `metadata.yaml`, and a starter `index.html`):

```bash
cd ../data-reports
make new SLUG=<slug> TITLE="<title>"
```

If `make`/`uv` isn't available, create the files by hand under
`reports/<slug>/` matching the structure above.

Then fill in `reports/<slug>/metadata.yaml`:

```yaml
title: "<title>"
authors:
  - <author>
date: <YYYY-MM-DD>
tags:
  - <tag>
summary: >
  <one or two sentences for the landing-page card>
status: draft
```

### 3. Write the report content (`index.html`)

Turn the analysis into a clean, self-contained `index.html`. Keep the house
style: a centered column, a "← All reports" back-link, title + byline, then the
content. Starter scaffold:

```html
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title><title></title>
  <style>
    body { margin: 0; color: #1d1d1b; background: #fff;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
      line-height: 1.6; }
    .wrap { max-width: 820px; margin: 0 auto; padding: 48px 24px 80px; }
    a.back { color: #1d3d63; text-decoration: none; font-size: 0.9rem; }
    h1 { font-size: 2rem; letter-spacing: -0.02em; margin: 18px 0 6px; }
    .byline { color: #6b6b66; margin: 0 0 28px; font-size: 0.95rem; }
    figure { margin: 28px 0; } figure img { max-width: 100%; height: auto; }
    figcaption { color: #6b6b66; font-size: 0.85rem; margin-top: 6px; }
    table { border-collapse: collapse; width: 100%; font-size: 0.9rem; }
    th, td { border: 1px solid #e3e3df; padding: 6px 10px; text-align: left; }
  </style>
</head>
<body>
  <div class="wrap">
    <a class="back" href="/">← All reports</a>
    <h1><title></h1>
    <p class="byline"><author> · <pretty date></p>

    <!-- report content -->
  </div>
</body>
</html>
```

Guidance for content:
- **Plots**: save static images (PNG/SVG) into the report folder and reference
  them with relative paths (`<img src="fig1.png">`). Prefer committing the image
  over a live-generating script.
- **OWID charts**: embed with the grapher iframe, e.g.
  `<iframe src="https://ourworldindata.org/grapher/<slug>" loading="lazy" style="width:100%;height:600px;border:0"></iframe>`.
- **Tables**: plain HTML tables are fine for small results.
- **Data**: if you want to ship the underlying data, drop a CSV/JSON in the
  report folder and link to it.
- Keep everything **relative and self-contained** so the report works at
  `/reports/<slug>/`.
- **Don't add your own breadcrumb / "back to reports" link** — the data-reports
  server injects a small fixed-position breadcrumb pill into every report
  (top-left of the viewport, links back to `/`). Adding your own would
  duplicate it.

### 4. Validate and preview

From the data-reports checkout:

```bash
make check                       # validates every report's metadata.yaml
make serve                       # http://127.0.0.1:8080 — open the landing page and the report
```

Confirm the card shows the right title/authors/date/summary and the report opens
and renders correctly. Iterate until the user is happy, then set
`status: published` in `metadata.yaml` if appropriate.

### 5. Publish (only when the user asks)

Commit on a branch and open a PR against `owid/data-reports`:

```bash
cd ../data-reports
git switch -c report/<slug>
git add reports/<slug>
git commit -m "Add data report: <title>"
git push -u origin report/<slug>
gh pr create --title "Add data report: <title>" --body "<short description>"
```

Once merged to `main`, Buildkite redeploys and the report is live at
`http://data-reports.owid.io/reports/<slug>/`. Share that URL.

## Notes

- Required metadata fields: `title`, `authors`, `date`, `summary`. `make check`
  enforces them and that an `index.html` exists.
- The slug is permanent-ish: it's the URL. Choose something stable and descriptive.
- Reports are internal (Tailscale / data-reports.owid.io) — fine for internal
  data, but it is not a public site.
- Don't edit `server.py`, `static/`, or the Makefile from this skill — only add a
  folder under `reports/`.
- If the user only wants the report scaffolded locally (not published), stop
  after step 4 and don't create a branch or PR.
