---
name: create-snapshot
description: Create a new snapshot (DVC file, plus a Python script only when one is needed) from a url_main and optional url_download. Fetches the page, extracts metadata with AI, confirms with user, writes files, and runs the snapshot. Use when the user wants to add a new data source or create a snapshot from a URL.
metadata:
  internal: true
---

# Create Snapshot

Create a new ETL snapshot from a source URL: fetch the page, infer metadata, confirm with the user, write the `.dvc` file (plus a `.py` script only when one is genuinely needed), then run the snapshot.

> **Paired skill — keep in sync.** [`/create-dataset`](../create-dataset/SKILL.md) consumes the conventions defined here (its Step 4 builds the snapshot for a full dataset chain): whenever you change the `.dvc` field guidance, the script templates, or the workflow in this file, check whether `create-dataset/SKILL.md` needs a matching edit (and make it in the same commit if so). The reverse also holds — see the mirror note there. The update-side skills are part of the same family: the fields written here are exactly what [`/update-dataset`](../update-dataset/SKILL.md) §6c re-checks on every version bump and what [`/review-data-pr`](../review-data-pr/SKILL.md) §5 compares old-vs-new at review time — keep the field guidance consistent across all of them.

## Inputs

Required:
- `url_main` — the dataset landing page URL

Optional:
- `url_download` — direct download URL for the data file (if available)

## Workflow

### 1. Fetch and analyse the source page

Use WebFetch to fetch `url_main`. From the page content, extract as much metadata as possible:

| Field | Where to look |
|-------|---------------|
| `title` | Page `<title>`, main heading, dataset title |
| `description` | Page description, abstract, or "about" section — copy the producer's text **verbatim** (light copyedits only — typos, spacing, encoding artifacts), don't paraphrase. For academic papers, use the abstract verbatim. If the page prose spans several paragraphs, take all of them (skip only navigation/boilerplate) |
| `producer` | Organisation name, data owner, author |
| `citation_full` | The producer's recommended citation, copied **verbatim** (light copyedits only — fixing a typo, stray spacing, or encoding artifacts; never rephrasing) — look for "cite as" / "suggested citation" / "please make the following reference" blocks on the page, an "Original citation" on repository pages (e.g. WRAP), NBER's suggested citation, or "Reference:" headers inside the data files themselves. Sites often have a dedicated "how to cite" / citation page separate from the dataset landing page — browse the site's navigation for one. Only construct a citation in standard format when the producer provides none |
| `attribution_short` | Short org name / acronym |
| `date_published` | Publication date or last-updated date. Not on the landing page? Look on other pages of the site (news/release notes, documentation), in the paper itself (title page and abstract often carry the exact date, e.g. "27 September 2013"), or in the files (file names like `10sd_jan15_2014.xlsx` or `shna2025tablesiii.xlsx` encode the release, and notes/readme sheets or PDF metadata may state it). On a fully JS-rendered page that states no date, the download URL's HTTP `Last-Modified` header is a defensible source — corroborate it against a release-named filename (see `/update-dataset` §6c). Use a partial date ("YYYY" or "YYYY-MM") if that's all the source supports; never silently default it to `date_accessed` — ask the user if still unsure, and only if they confirm the producer publishes no date anywhere, fall back to `date_accessed` with a `#` comment in the `.dvc` documenting why (`/review-data-pr` §5 looks for exactly that comment) |
| `license_name` | License section (e.g. "CC BY 4.0", "Open Government Licence"). Not on the landing page? Also check the documentation (sources & methods documents, notes/readme sheets inside the data files, repository cover sheets like WRAP) and other pages within the same website — producers often have a dedicated licensing / terms-of-use / "about the data" section reachable from the site navigation. If no license is stated anywhere, **warn the user explicitly** and fall back to rights-reserved `© <producer> (<year>)` — never invent permissive terms like "Free to use" |
| `license_url` | Link to the license, or to the page/document where the terms are stated |
| `file_extension` | Infer from `url_download` if provided (csv, xlsx, xls, zip, json…); default `csv` |

Leave fields blank if they cannot be inferred — the user will fill them in.

### 2. Confirm metadata and path with the user

Present the inferred metadata and ask the user to fill in or correct:

**Required fields the user must provide:**
- `namespace` — e.g. `who`, `worldbank`, `un_igme` (suggest based on producer)
- `short_name` — snake_case file stem, e.g. `child_mortality_rates`
- `version` — YYYY-MM-DD (default: today's date from `date -u +"%Y-%m-%d"`)

**Pre-filled fields to confirm or correct:**
- `title` — dataset title
- `producer` — organisation name
- `citation_full` — full citation string
- `attribution_short` — short name / acronym (optional)
- `date_published` — YYYY-MM-DD or YYYY or YYYY-MM
- `description` — brief dataset description (optional)
- `license_name` — e.g. `CC BY 4.0`
- `license_url` — license page URL (optional)
- `file_extension` — inferred from download URL or `csv`
- `is_private` — default `false`
- `dataset_manual_import` — default `false` (set to `true` if there's no `url_download`)

Present this as a summary block so the user can quickly scan and correct individual fields. Wait for confirmation before proceeding.

### 3. Write the files

Once the user confirms, compute:
```
snapshot_dir = snapshots/<namespace>/<version>/
dvc_path     = snapshots/<namespace>/<version>/<short_name>.<file_extension>.dvc
py_path      = snapshots/<namespace>/<version>/<short_name>.py   # only when a script is needed — see below
```

Create the directory if it doesn't exist.

**Decide whether a `.py` script is needed** (CLAUDE.md, "No `.py` for simple downloads"):

- **Plain `url_download`, no custom logic** → write **only the `.dvc`**. `etls <namespace>/<version>/<short_name>` runs it straight from the `.dvc`. This is the default case — `/review-data-pr` §3/§7 treat the script as optional, so don't add one "for completeness".
- **Manual import** (`dataset_manual_import: true`) → write the manual-import script below.
- **Download needing custom code** (API pagination, auth, scraping, multi-file assembly, non-trivial parsing before storing) → write the automatic-download script below with the custom logic inside `run()`.

Either script is a plain `run()` — **no `click` decorators and no `if __name__ == "__main__":` block**. The `etls` CLI imports the module and invokes `run` itself, supplying `--path-to-file` for manual imports. (Many old scripts still carry the boilerplate; don't copy them.)

**Write the DVC file** (`<short_name>.<file_extension>.dvc`):

```yaml
# Learn more at:
# http://docs.owid.io/projects/etl/architecture/metadata/reference/
meta:
  origin:
    # Data product / Snapshot
    title: "<title>"
    title_snapshot: "<title> - <file specifics>"   # only when the file is one table/extract of a broader data product
    description: |-               # omit block if empty; verbatim producer text
      <description>
    description_snapshot: |-      # required whenever title_snapshot is set; own wording is fine here
      <what this specific file contains: table number, variables, units, years, plus any OWID-side
      context such as manual transcription or retrieval from an archived copy>
    date_published: "<date_published>"

    # Citation
    producer: <producer>
    citation_full: |-
      <citation_full>
    attribution_short: <attribution_short>   # omit if empty

    # Files
    url_main: <url_main>
    url_download: <url_download>             # omit if not provided
    date_accessed: <version>                 # use the snapshot version date

    # License
    license:
      name: <license_name>
      url: <license_url>                     # omit if empty

  is_public: false    # omit this line if is_private is false
outs:
  - md5: ""
    size: 0
    path: <short_name>.<file_extension>
```

**Write the Python script** (`<short_name>.py`) — only in the two cases above:

If the download runs automatically but needs custom code:
```python
"""Script to create a snapshot of dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run(upload: bool = True) -> None:
    """Create a new snapshot.

    Args:
        upload: Whether to upload the snapshot to S3.
    """
    snap = paths.init_snapshot()
    # ... custom fetch/assembly logic here ...
    snap.create_snapshot(upload=upload)
```

If `dataset_manual_import` is `true` (no direct download link) — same template as `/create-dataset` Step 4:
```python
"""Script to create a snapshot of dataset.

The data file is provided manually. Steps to obtain it:
  1. Go to <url_main>
  2. Download the data file and save it locally.
  3. Run: etls <namespace>/<version>/<short_name> --path-to-file <path>
"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run(upload: bool = True, path_to_file: str | None = None) -> None:
    snap = paths.init_snapshot()
    snap.create_snapshot(filename=path_to_file, upload=upload)
```

### 4. Run the snapshot

After writing the files, run:

```bash
.venv/bin/etls <namespace>/<version>/<short_name>
```

- If `dataset_manual_import` is `true`, tell the user to download the file manually and re-run with `--path-to-file <path>`.
- If the snapshot run fails, diagnose and fix the issue. Common problems:
  - Wrong `file_extension` — check what the download URL actually serves
  - Missing or wrong `url_download` — verify with the user
  - Auth/captcha required — first test a plain, honestly-identified User-Agent against the direct file URL (some hosts reject *browser-like* UAs only — see the inverse-UA note under Notes); if genuinely blocked, flag to user and switch to `dataset_manual_import = true`

### 5. Verify links and field consistency

- **Links**: run the HEAD-check loop from `/update-dataset` §6c ("Link verification") on every URL in the new `.dvc` (`url_main`, `url_download`, `license.url`, and any URL inside `description`). A curl non-2xx is a *signal*, not proof — Cloudflare-fronted hosts return false 404s to curl. Escalate with WebFetch, then the Wayback Machine, before treating a link as broken; never swap a link for an alternative on a curl-only failure. URLs carrying a `#fragment` also need §6c's anchor pass — HTTP status alone can't validate a fragment.
- **Citation year vs `date_published` year**: the year inside `citation_full` should normally match `date_published`'s year. A deliberate mismatch is fine when the producer labels the release by *edition* rather than publish date (e.g. a "2025 report" published 2026-03-17) — leave a one-line `#` comment in the `.dvc` so the next reviewer doesn't re-flag it (`/review-data-pr` §5 checks exactly this pair).
- **Optional deeper pass — adversarial source verification**: [`/adversarial-data-review`](../adversarial-data-review/SKILL.md) goes beyond "the links resolve" and *reads* the producer's documentation behind them, checking every `.dvc` claim (description verbatim-ness, counts, `date_published`, license, citation) against what the docs actually say — its Phase 0 is the slice that applies at snapshot stage (the data cross-checks need a built garden dataset, e.g. via `/create-dataset`). Don't run it by default — it fetches docs and runs web searches, so it can consume many tokens; offer it when the source looks unreliable (no version labels, self-published, or the page and file seem to disagree).

### 6. Report to the user

Show:
- The paths of the files created (`.dvc`, plus the `.py` if one was needed)
- Whether the snapshot ran successfully
- Next steps: "You can now create a meadow step for `<namespace>/<version>/<short_name>`"

## Notes

- `date_accessed` in the DVC file should always equal the snapshot `version` date (the date you ran the snapshot).
- If `url_download` is not provided and cannot be inferred, always set `dataset_manual_import = true`.
- **Data living in an embedded chart (Datawrapper and similar): parse the page's own fallback tables, never the chart platform's CDN endpoint.** The chart CDN's latest published version can trail the page's data by a full release. Producer pages server-render each embed's data as an HTML `<noscript>` table — parse that instead: whole-page `pd.read_html(io.StringIO(resp.text))`, select the table whose columns exactly match the expected header, assert exactly one match. See `/update-dataset` Guardrails, "Scraped chart embeds".
- **Before accepting manual import because "the site blocks downloads": test a plain UA.** Some hosts reject *browser-like* User-Agents while letting plain, honestly-identified clients through (the inverse of the usual bot-blocking). Test both directions against the direct file URL; if a plain UA works, keep `url_download` in the `.dvc` and pass `user_agent="owid-etl/1.0 (https://ourworldindata.org)"` to `snap.create_snapshot(...)` — and keep the `.py` in that case (the script-less path can't set a UA), saying so in the docstring. Also re-check the producer's download page/API for a stable direct endpoint before carrying a manual flow forward. See `/update-dataset` Guardrails.
- The `outs` block `md5` and `size` fields are filled in automatically by DVC when the snapshot runs — just set them to empty/zero in the template.
- Omit optional YAML fields entirely (don't leave them blank) to keep the DVC file clean.
- Never guess at citation text — if you can't find it on the page, leave a placeholder like `<TO BE FILLED>` and ask the user.
- Licenses and citations are often not on the landing page: check the documentation too (sources & methods PDFs, notes sheets inside the workbook, repository cover sheets), and look for dedicated pages elsewhere on the same site (licensing, terms of use, how-to-cite sections). A citation request ("please cite as...") is not a license — you may summarize it as the license name only when the page explicitly frames usage terms (e.g. "when using these data (for whatever purpose), please make the following reference"). If nothing is stated anywhere, warn the user and use `© <producer> (<year>)`.
- `citation_full` should be the producer's recommended citation **verbatim** whenever one exists (page "cite as" blocks, repository "Original citation", NBER suggested citations, "Reference:" lines inside the data files); slight modifications are fine to fix typos or spacing issues in the source (e.g. "mirror : a" → "mirror: a"), but never rephrase or reformat the citation style. If the recommended citation is for a working-paper version of a published work, keep it and append the published version (e.g. "Published as: …"). Construct a standard-format citation only when the producer recommends none.
- `description` must be the producer's own words, copied verbatim from the landing page or the paper's abstract — a fluent paraphrase is not acceptable. Slight modifications are fine when they fix a typo, spacing, or encoding artifact in the source text. Verify against the actual page text.
- When the snapshot is a single file/table of a broader data product, split the metadata: `title`/`description` describe the data product (verbatim), while `title_snapshot`/`description_snapshot` describe the specific file. Whenever `title_snapshot` is set, also write a `description_snapshot` — that one doesn't need to be verbatim. OWID-side context (hand-transcription notes, "retrieved from the Internet Archive", etc.) belongs in `description_snapshot`, never inside the producer's `description`.
