---
name: create-snapshot
description: Create a new snapshot (DVC file, plus a Python script only when one is needed) from a url_main and optional url_download. Fetches the page, extracts metadata with AI, confirms with user, writes files, and runs the snapshot. Use when the user wants to add a new data source or create a snapshot from a URL.
metadata:
  internal: true
---

# Create Snapshot

Create a new ETL snapshot from a source URL: fetch the page, infer metadata, confirm with the user, write the `.dvc` file (plus a `.py` script only when one is genuinely needed), then run the snapshot.

> **Paired skill — keep in sync.** [`/create-dataset`](../create-dataset/SKILL.md) consumes the conventions defined here (its Step 4 builds the snapshot for a full dataset chain, reusing this skill's step 3 generator call with `dataset_manual_import: True`): whenever you change the `.dvc` field guidance, the cookiecutter context, or the workflow in this file, check whether `create-dataset/SKILL.md` needs a matching edit (and make it in the same commit if so). The reverse also holds — see the mirror note there. The update-side skills are part of the same family: the fields written here are exactly what [`/update-dataset`](../update-dataset/SKILL.md) §6c re-checks on every version bump and what [`/review-data-pr`](../review-data-pr/SKILL.md) §5 compares old-vs-new at review time — keep the field guidance consistent across all of them.

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
| `description` | Page description, abstract, or "about" section. Per the metadata reference: if the producer provides a *good, factual* description, use it exactly or conveniently rephrased. Academic abstracts and methodology summaries usually qualify — take them as-is. **Never paste promotional or first-person copy** ("our nation", "vital information", funding pitches): rewrite it as a neutral, factual description from OWID's point of view (who runs it, what it measures, coverage, cadence) |
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

### 3. Generate the files

Once the user confirms, generate both files from the wizard's snapshot cookiecutter — the same templates the wizard's snapshot page uses.

> **Never hand-write these files, and never copy the templates into this file.** `apps/wizard/etl_steps/cookiecutter/snapshot/` is the single source of truth. Hand-copied templates drift: an earlier version of this skill carried its own copies, and the manual-import one had already lost the canonical docstring. The template also gets details right that are easy to fluff by hand — most importantly `license` nested **inside** `origin` (CLAUDE.md's most-repeated snapshot mistake, and a schema `not`-constraint plus `test_snapshot_license_lives_under_origin` exist because of it), and `is_public: false` for a private snapshot.

Files produced:
```
snapshots/<namespace>/<version>/<short_name>.<file_extension>.dvc
snapshots/<namespace>/<version>/<short_name>.py    # removed again when no script is needed — see below
```

**Decide whether a `.py` script is needed** (CLAUDE.md, "No `.py` for simple downloads"):

- **Plain `url_download`, no custom logic** → set `dvc_only: True`, which deletes the generated script so only the `.dvc` remains. `etls <namespace>/<version>/<short_name>` runs it straight from the `.dvc`. This is the default case — `/review-data-pr` §3/§7 treat the script as optional, so don't keep one "for completeness".
- **Manual import** → set `dataset_manual_import: True`; the template emits the `path_to_file` variant of `run()`.
- **Download needing custom code** (API pagination, auth, scraping, multi-file assembly, non-trivial parsing before storing) → keep the generated script and add the custom logic inside `run()`, between `paths.init_snapshot()` and `snap.create_snapshot(...)`.

Either way the script is a plain `run()` — **no `click` decorators and no `if __name__ == "__main__":` block**. The `etls` CLI imports the module and invokes `run` itself, supplying `--path-to-file` for manual imports. The template already gets this right; the warning matters because many old scripts in the repo still carry the boilerplate — don't copy one of those as a model.

**Generate the files.** Every field confirmed in step 2 goes in as cookiecutter context. Pass **all** the keys below — there is no committed `cookiecutter.json` supplying defaults, so a missing key is a Jinja `UndefinedError`, and an empty string is how you say "omit this field" (the template's `{%- if %}` guards drop it from the output).

**Write the context as JSON with the `Write` tool, then run the generator against that file.** Do not interpolate the field values into a `python -c` program: `citation_full`, `title` and `description` are producer prose, and an apostrophe (`World Bank's`), a double quote, a backslash, or a newline in any of them either breaks the program or silently changes the value before cookiecutter sees it. Producer citations contain apostrophes routinely, so this is the normal case, not an edge case. JSON keeps the prose out of shell and Python literals entirely.

First write `/tmp/snapshot_context.json` (booleans are real JSON `true`/`false`, and `""` means "omit this field"):

```json
{
  "channel": "snapshots",
  "namespace": "<namespace>",
  "snapshot_version": "<version>",
  "short_name": "<short_name>",
  "file_extension": "<file_extension>",
  "is_private": false,
  "dataset_manual_import": false,
  "dvc_only": false,
  "title": "<title>",
  "description": "<description>",
  "title_snapshot": "",
  "description_snapshot": "",
  "origin_version": "",
  "date_published": "<date_published>",
  "producer": "<producer>",
  "citation_full": "<citation_full>",
  "attribution": "",
  "attribution_short": "<attribution_short>",
  "url_main": "<url_main>",
  "url_download": "<url_download>",
  "date_accessed": "<version>",
  "license_name": "<license_name>",
  "license_url": "<license_url>"
}
```

Then generate. The only string literal in this program is a fixed path, so no field value can break it:

```bash
.venv/bin/python -c "
import json
from apps.utils.files import generate_step
from apps.wizard.etl_steps.utils import COOKIE_SNAPSHOT
from etl.paths import SNAPSHOTS_DIR

with open('/tmp/snapshot_context.json') as f:
    data = json.load(f)
generate_step(cookiecutter_path=COOKIE_SNAPSHOT, data=data, target_dir=SNAPSHOTS_DIR)

# dvc_only: drop the script the template always writes.
if data['dvc_only']:
    py = SNAPSHOTS_DIR / data['namespace'] / data['snapshot_version'] / (data['short_name'] + '.py')
    py.unlink(missing_ok=True)
"
```

Note that `license` belongs to `origin` — the template nests `license_name` / `license_url` correctly, so there is nothing to move afterwards.

Which fields to fill, and when to leave them empty:

| Context key | Value |
|---|---|
| `title` / `description` | The data product. `description` is factual — the producer's own text when it is factual, never promotional copy |
| `title_snapshot` / `description_snapshot` | Both empty by default. Set them **only** when the file is one table/extract of a broader product; `description_snapshot` then becomes required, and carries the file specifics (table number, variables, units, years) plus any OWID-side context such as manual transcription or an archived copy |
| `attribution` | Empty unless `producer (year)` is genuinely uninformative |
| `attribution_short` / `origin_version` | Empty when the producer gives none |
| `url_download` | Empty for a manual import |
| `license_url` | Empty when the producer states no license anywhere — don't fall back to the landing page |
| `date_accessed` | The snapshot version date |

**After generating**, three things to do:

- **Verify the `.dvc` parses, and fix the quoting if it doesn't.** The template emits `title` and `date_published` as double-quoted scalars and `producer`, `title_snapshot`, `attribution`, `attribution_short` and `license_name` as *plain* (unquoted) ones, none of them escaped. So a `"` in the title, or a `: ` or leading `#` in any plain-scalar field, produces a file that is not valid YAML — a real case, since product titles carry both quotes and colons. `description`, `description_snapshot` and `citation_full` use block scalars and are safe. Always check, and re-quote the offending field by hand (single quotes with `''` doubling, or a `|-` block) when it fails:

  ```bash
  .venv/bin/python -c "
  import sys, yaml
  p = 'snapshots/<namespace>/<version>/<short_name>.<file_extension>.dvc'
  try:
      yaml.safe_load(open(p))
  except yaml.YAMLError as e:
      sys.exit(f'{p} is not valid YAML — re-quote the offending field:\n{e}')
  print(f'{p}: valid YAML')
  "
  ```

- Tidy the end of the `.dvc`. The template's final `{%- endif -%}` leaves a whitespace-only line (`"  "`) after the license block, and for a private snapshot the file also ends without a final newline. Both parse fine as YAML, but committed files shouldn't carry either: drop the whitespace-only line and make sure the file ends with exactly one `\n`.
- Add any `#` comments this skill calls for: the companion-files `# NOTE:` above `url_download` (step 6), and a one-line note when `citation_full`'s year deliberately differs from `date_published` (step 5).

There is deliberately **no `outs:` block** — `snap.create_snapshot()` writes it with the real md5 and size when step 4 runs. Don't add a placeholder.

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

- **Links**: run the HEAD-check loop from `/update-dataset` §6c ("Link verification") on every URL in the new `.dvc` (`url_main`, `url_download`, `license.url`, and any URL inside `description`). A curl non-2xx is a *signal*, not proof — Cloudflare-fronted hosts return false 404s to curl. Escalate with WebFetch, then the Wayback availability API, but per §6c no automated signal is decisive (a missing Wayback capture is non-evidence, and bot-blocked hosts fail curl *and* WebFetch while serving browsers fine): a link that fails every automated check gets reported with its evidence trail for the user to confirm in a browser — never mark it broken or swap it for an alternative on automated failures alone. URLs carrying a `#fragment` also need §6c's anchor pass — HTTP status alone can't validate a fragment.
- **Citation year vs `date_published` year**: the year inside `citation_full` should normally match `date_published`'s year. A deliberate mismatch is fine when the producer labels the release by *edition* rather than publish date (e.g. a "2025 report" published 2026-03-17) — leave a one-line `#` comment in the `.dvc` so the next reviewer doesn't re-flag it (`/review-data-pr` §5 checks exactly this pair).
- **Typos in the `.dvc`**: run [`/check-metadata-typos`](../check-metadata-typos/SKILL.md) on the new file (its "current step only" scope covers snapshot `.dvc` files as well as `.meta.yml`). The prose fields written in step 3 — `description`, `title`, `citation_full`, `attribution` — are user-facing, and nothing downstream spell-checks them: `/update-dataset` §6c only re-checks them on a *later* version bump. `citation_full` is the exception to fixing what it reports: it is verbatim producer text, so only correct a typo there if the producer's own page has it right (see the `citation_full` note above).
- **Outdated practices in the `.py`**, when step 3 wrote one: run [`/check-outdated-practices`](../check-outdated-practices/SKILL.md) on it. This is the mechanical check for the conventions already stated above — no `click` decorators, no `if __name__ == "__main__":` block — plus the metadata-preserving patterns from CLAUDE.md if the script parses before storing. Run the skill rather than eyeballing the file; it reads the detector extension as its source of truth and catches helper calls that look current but aren't. `/review-data-pr` treats a leftover `__main__` block in a snapshot as a 🔴 blocker, so this is cheaper to fix here than at review.

  Route the finding by where the pattern came from. The script templates in step 3 are variants of `apps/wizard/etl_steps/cookiecutter/snapshot/`, which is where the current practices are supposed to live — so a hit on a line that came from the template means the **template** is stale, and fixing only the generated file leaves every future snapshot carrying it. Fix it upstream in the cookiecutter (and in this skill's copy of it) as well as in the file you just wrote.
- **Optional deeper pass — adversarial source verification**: [`/adversarial-data-review`](../adversarial-data-review/SKILL.md) goes beyond "the links resolve" and *reads* the producer's documentation behind them, checking every `.dvc` claim (description accuracy, counts, `date_published`, license, citation) against what the docs actually say — its Phase 0 is the slice that applies at snapshot stage (the data cross-checks need a built garden dataset, e.g. via `/create-dataset`). Don't run it by default — it fetches docs and runs web searches, so it can consume many tokens; offer it when the source looks unreliable (no version labels, self-published, or the page and file seem to disagree).

### 6. Report to the user

Show:
- The paths of the files created (`.dvc`, plus the `.py` if one was needed)
- Whether the snapshot ran successfully
- The results of the step 5 checks — links, typos, and outdated practices — including "clean" results. Silence on a check reads as "not a problem" when it may mean "not run".
- **The source's other data files, if any** — when the landing page / repository ships several data files (companion indices, summary panels, codebooks with data), list the ones NOT snapshotted so the user can opt in now or skip deliberately. **Persist the inventory in the `.dvc`**, not just in chat — a `# NOTE:` comment above `url_download` listing the release's other data files as of `date_accessed` (e.g. `# NOTE: the release also ships Foo_Index.csv and codebook.pdf — not snapshotted (2026-07-20)`). That comment is the baseline the next `/update-dataset` cycle diffs against when the host has no file-history API; a new companion file is invisible to every within-file check (see `/update-dataset`, "Surface new indicators").
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
- `description` describes the data product factually. When the producer's own text is factual (abstracts, methodology summaries), prefer it, exactly or lightly rephrased — don't rewrite for the sake of rewriting. When the producer's page only offers promotional or first-person copy ("our nation", "vital information", how the data guides funding), do NOT paste it — write a neutral description from OWID's point of view instead: who produces it, what it measures, coverage, cadence. (Schema guideline: "If the producer provides a good description, use that, either exactly or conveniently rephrased.")
- When the snapshot is a single file/table of a broader data product, split the metadata: `title`/`description` describe the data product, while `title_snapshot`/`description_snapshot` describe the specific file. Whenever `title_snapshot` is set, also write a `description_snapshot` — that one doesn't need to be verbatim. OWID-side context (hand-transcription notes, "retrieved from the Internet Archive", etc.) belongs in `description_snapshot`, never inside the producer's `description`.
