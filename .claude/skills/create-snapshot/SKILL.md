---
name: create-snapshot
description: Create a new snapshot (DVC file + Python script) from a url_main and optional url_download. Fetches the page, extracts metadata with AI, confirms with user, writes files, and runs the snapshot. Use when the user wants to add a new data source or create a snapshot from a URL.
---

# Create Snapshot

Create a new ETL snapshot from a source URL: fetch the page, infer metadata, confirm with the user, write the `.dvc` and `.py` files, then run the snapshot.

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
| `description` | Page description, abstract, or "about" section |
| `producer` | Organisation name, data owner, author |
| `citation_full` | Official citation or "how to cite" section |
| `attribution_short` | Short org name / acronym |
| `date_published` | Publication date or last-updated date |
| `license_name` | License section (e.g. "CC BY 4.0", "Open Government Licence") |
| `license_url` | Link to the license |
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
py_path      = snapshots/<namespace>/<version>/<short_name>.py
```

Create the directory if it doesn't exist.

**Write the DVC file** (`<short_name>.<file_extension>.dvc`):

```yaml
# Learn more at:
# http://docs.owid.io/projects/etl/architecture/metadata/reference/
meta:
  origin:
    # Data product / Snapshot
    title: "<title>"
    description: |-               # omit block if empty
      <description>
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

**Write the Python script** (`<short_name>.py`):

If `dataset_manual_import` is `false` (automatic download via `url_download`):
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
    snap.create_snapshot(upload=upload)
```

If `dataset_manual_import` is `true` (no direct download link):
```python
"""Script to create a snapshot of dataset.

Steps to download the data manually:
  1. Go to <url_main>
  2. Download the data file and save it locally.
  3. Run: python snapshots/<namespace>/<version>/<short_name>.py --path-to-file <path>
"""

from pathlib import Path

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run(upload: bool = True, path_to_file: str | None = None) -> None:
    """Create a new snapshot.

    Args:
        upload: Whether to upload the snapshot to S3.
        path_to_file: Path to local data file.
    """
    snap = paths.init_snapshot()
    snap.create_snapshot(filename=path_to_file, upload=upload)
```

### 4. Run the snapshot

After writing the files, run:

```bash
.venv/bin/etl snapshot <namespace>/<version>/<short_name>
```

- If `dataset_manual_import` is `true`, tell the user to download the file manually and re-run with `--path-to-file <path>`.
- If the snapshot run fails, diagnose and fix the issue. Common problems:
  - Wrong `file_extension` — check what the download URL actually serves
  - Missing or wrong `url_download` — verify with the user
  - Auth/captcha required — flag to user, switch to `dataset_manual_import = true`

### 5. Report to the user

Show:
- The paths of the two files created
- Whether the snapshot ran successfully
- Next steps: "You can now create a meadow step for `<namespace>/<version>/<short_name>`"

## Notes

- `date_accessed` in the DVC file should always equal the snapshot `version` date (the date you ran the snapshot).
- If `url_download` is not provided and cannot be inferred, always set `dataset_manual_import = true`.
- The `outs` block `md5` and `size` fields are filled in automatically by DVC when the snapshot runs — just set them to empty/zero in the template.
- Omit optional YAML fields entirely (don't leave them blank) to keep the DVC file clean.
- Never guess at citation text — if you can't find it on the page, leave a placeholder like `<TO BE FILLED>` and ask the user.
