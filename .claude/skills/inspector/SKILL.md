---
name: inspector
description: Inspect OWID public-facing content (charts, MDims, explorers, articles/posts) for typos and semantic issues. Gathers content bundles from the grapher DB, runs a zero-cost lint pass (codespell + rules), then an LLM pass that judges every view in full collection context, adversarially verifies candidate findings, and stores the confirmed ones in the staging `inspections` table. Trigger when the user says "inspect <slug>", "run the inspector", or "check this chart/mdim/explorer/article for typos or semantic issues"; also as the final step of skills that touch public-facing content (update-dataset, create-multidim, create-explorer, review-data-pr).
metadata: { internal: true }
---

# Inspector

Find mistakes in what OWID readers actually see: chart titles, subtitles, footnotes, indicator
descriptions, MDim/explorer dropdown labels, and article prose. Typos, nonsense
programmatically-generated view titles, unit mismatches, stale dates, leftover template
placeholders.

Findings are stored in the `inspections` table on the current branch's **staging** DB (created on
demand; production is never touched). Fingerprint-based dedup means re-runs update known findings
instead of repeating them, and dismissed findings stay suppressed until the underlying text
changes.

## Workflow

Work in a run directory: `ai/inspector/<slug-or-scope>/`.

### 1. Gather

```bash
.venv/bin/etl inspector gather -s <slug> -o ai/inspector/<run>/bundles
```

- `-s` can be repeated; it matches chart slugs, MDim slugs/short-names/namespaces, explorer
  slugs, and post slugs. `-t chart|multidim|explorer|post` restricts the type, `-l N` limits
  count per type.
- Omitting `-s` gathers **everything** (thousands of objects). Never do that unless the user
  explicitly asks for a full sweep, and warn them first.
- Output: one JSON bundle per content object. A bundle contains `config_fields` (collection-level
  texts), `views` (each with `dimensions`, `url`, and view-specific `fields`),
  `shared_view_fields` (fields identical across all views, listed once), `indicators` (each
  indicator's user-visible metadata, listed once no matter how many views use it), and for posts
  `markdown` + `embedded_charts`. Every field carries `origin_id` and `fix_location`.
- Each view also has `rendered_title` / `rendered_subtitle`: what Grapher actually displays.
  When the view's own `fields` contain no `title`/`subtitle`, these are resolved from the
  primary indicator (first of `indicator_ids`: titlePublic → display.name → name, and
  descriptionShort). A defect in a resolved rendered field belongs to that indicator's metadata:
  use the indicator's `origin_id`/`fix_location` and its field name when reporting it.

### 2. Lint (free, deterministic)

```bash
.venv/bin/etl inspector lint ai/inspector/<run>/bundles -o ai/inspector/<run>/lint_findings.json
```

Codespell plus rule checks (double spaces, unrendered `{{...}}`/`<<...>>` placeholders,
duplicated words). These findings go through the same verify pass as yours (codespell flags
proper nouns and technical terms).

### 3. Detect (you)

Read each bundle and judge it **as a reader sees it**:

- **Per view**: do title, subtitle, note, and the indicators' units tell one coherent story? Do
  the dimension choices match the title (a view with `{sector: transport}` whose title says
  "industry" is a finding)?
- **Across sibling views**: scan the views table for programmatically-generated combinations that
  produce nonsense or ungrammatical titles. This is the failure mode isolated checks miss; use
  the siblings as context for what the template intended.
- **Indicator legend**: typos and grammar in descriptions and description-key bullets. One
  finding per defect, no matter how many views use the indicator; put the variable's `origin_id`
  on the finding and the affected view URLs in `affected_views`.
- **Posts**: prose typos and grammar; claims that contradict the titles/subtitles of
  `embedded_charts`; stale phrasing ("as of 2023", "last year") relative to today.
- **Do NOT flag**: style preferences, tone, title-case conventions, subjective rewrites, British
  spellings in producer names or quoted material, or anything a careful editor would leave
  alone. If a bundle is clean, report no findings for it; that is a valid outcome.

Categories: `typo`, `grammar`, `semantic-mismatch`, `unit-mismatch`, `nonsense-combination`,
`stale-text`, `formatting-artifact`, `style`. Severity: `high` (visible on the chart/article
itself: title/subtitle typos, nonsense combinations, wrong units), `medium` (visible on data
pages or tooltips: description typos), `low` (minor, defensible).

**Scaling**: up to ~5 bundles, do it inline. More than that, spawn one subagent per bundle (or
per batch of ~20 chart bundles) with the bundle path and these detection instructions; each
returns a JSON list of findings.

### 4. Verify (adversarial)

For every candidate finding (yours **and** lint's): re-read the exact text and try to refute it.
Is the "typo" a proper noun, a technical term, a producer's own spelling? Is the "mismatch"
correct in context? Drop anything you cannot defend to an editor. For large finding sets, spawn
verifier subagents; when in doubt on severity keep the finding but downgrade it.

Write the survivors to `ai/inspector/<run>/agent_findings.json` (drop refuted lint findings from
`lint_findings.json` too). Each finding:

```json
{
  "content_type": "multidim",
  "slug": "energy",
  "url": "<view or object URL>",
  "field": "title",
  "origin_id": "<origin_id of the field, from the bundle>",
  "category": "nonsense-combination",
  "severity": "high",
  "context": "<the exact sentence/phrase containing the issue>",
  "explanation": "<why this is wrong>",
  "suggested_fix": "<the corrected text>",
  "affected_views": ["<view URLs, when the origin is shared text>"],
  "fix_location": "<fix_location of the field, from the bundle>",
  "content_hash": "<content_hash of the bundle>",
  "source": "agent"
}
```

Copy `origin_id`, `fix_location`, and `content_hash` verbatim from the bundle. Quote `context`
exactly and stably: the fingerprint that dedupes re-runs is computed from it.

### 5. Store and report

```bash
.venv/bin/etl inspector store ai/inspector/<run>/agent_findings.json ai/inspector/<run>/lint_findings.json \
    --bundles ai/inspector/<run>/bundles
```

`--bundles` lets the store mark previously-open findings that disappeared as `fixed`. The command
prints new / seen-again / reopened / still-dismissed counts.

Then report to the user, leading with the high-severity findings: what is wrong, where (URL), and
where to fix it (`fix_location`: an ETL catalogPath means the step's `.meta.yml`; an admin URL
means the chart editor; a Google Docs link means the article). Mention how many known findings
were skipped as already dismissed.

## Triage commands

```bash
.venv/bin/etl inspector list                     # open findings on this staging server
.venv/bin/etl inspector dismiss <fingerprint-prefix> -r "producer's own spelling"
```

When the user says a finding is a false positive, dismiss it with their reason; it will not
resurface unless the text changes.

## Notes

- `OWID_ENV` on a branch points at `staging-site-<branch>` automatically; that is where the table
  lives and dies with the branch. Check `etl inspector list` output header if unsure which DB.
- The LLM pass runs in this session (subscription); the only metered thing is nothing — gather,
  lint, and store are plain Python.
- Full-catalog sweeps are Phase 3 (scheduled); don't run them ad hoc.
