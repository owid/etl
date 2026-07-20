---
name: mdim-metadata-helper
description: Walk an author (often non-technical) through drafting, structuring, and shipping changes to MDIM metadata texts — e.g. the "What you should know about this data" bullets (description_key), titles, subtitles, footnotes — from a shared Google Doc of rules to an ETL PR with a staging preview. Trigger when a user wants to write or edit MDIM/WYSK/description-key texts, shares a "metadata rules" Google Doc, or asks to turn text rules into Jinja templates or YAML metadata.
metadata:
  internal: true
---

# mdim-metadata-helper

Help an author take metadata texts for an MDIM from rough notes to a reviewed PR. The author
may be non-technical: explain everything in plain language, never assume they know what
Jinja, YAML, garden steps, or staging servers are, and do all the technical lifting yourself.
The author owns the words; you own the plumbing.

The workflow is general to any metadata text field; the examples here use the
"What you should know about this data" field (`description_key`), which is where it has
been exercised most.

## The one principle that governs everything

**The rules document is the source of truth for wording. YAML is generated from it.**
Wording decisions happen in the doc (or in conversation, then written back to the doc) —
never by silently editing YAML. If the author asks for a wording tweak mid-implementation,
update the doc first (give them the text to paste), then regenerate.

## Phase 0 — Setup

The author works in a Google Doc with two tabs: **"Input to Claude"** (their notes and
rules) and **"Output from Claude"** (your structured restatement, pasted back by them).

- If they don't have one yet, point them at the team template and ask them to make a copy
  (File → Make a copy) and share the new doc's link with you:
  **TEMPLATE-DOC-URL-TO-BE-FILLED-IN**
- Read the doc directly via the Google Drive connector when the user pastes its link.
  If the connector isn't available in their session, ask them to either enable it or just
  paste the doc's text / attach an exported copy — the workflow is identical from there.

## Phase 1 — Understand (before writing anything)

1. Read their doc in full.
2. Read the current state of the texts they want to change:
   - the MDIM's config (`etl/steps/export/multidim/.../<name>.config.yml`) — dimensions,
     views, any `common_views` overrides, and any *duplicated text definitions* it carries;
   - the garden step's `*.meta.yml` — the Jinja-templated definitions the indicators use;
   - `grep` the exact current sentences across `etl/steps/` — the same text is often
     duplicated in several files, and all copies must be found before promising a change.
3. Work through their **open questions** first, then run the elicitation checklist:
   - **Coverage**: enumerate the MDIM's dimension choices; does a rule or the anchor text
     account for every view? Ask about any combination the doc is silent on.
   - **Ordering**: is the bullet/paragraph order fixed everywhere, or does it vary?
   - **Presentation**: bullets or paragraphs? (See "rich text" note below.)
   - **Links and defined terms**: which must be preserved (`#dod:` terms, article links)?
   - **Exceptions**: confirm each one names a specific view and states the full deviation.
   - **Style**: American spelling; follow the metadata reference guidelines
     (https://docs.owid.io/projects/etl/architecture/metadata/reference/) for the field.

Ask questions in batches, in plain language, quoting their own words back where possible.

## Phase 2 — Structure

Produce a **structured restatement** of the rules and ask the author to paste it into the
"Output from Claude" tab. Iterate until they say it's right; get an explicit "agreed"
before implementing. The restatement must be complete enough that a colleague could
implement from it without seeing this chat. Format:

```
SCOPE: <mdim catalog path> · <field(s)> · <views covered>
PRESENTATION: <bullets | paragraphs> · <count per view>

BASE TEXT (anchor view: <dimensions>):
  B1: <text>
  B2: <text>
  ...

VARIATION RULES:
  R1: WHEN <control> = <choice(s)> → <replace B1 with … / drop B3 / append …>
  R2: ...

EXCEPTIONS:
  E1: view <dimensions> → <full deviation>

DECISIONS RESOLVED: <one line per open question and its answer>
```

## Phase 3 — Implement

Only after the author says "make the PR" (or equivalent):

1. **Branch**: short descriptive name, no prefixes, ≤ 28 chars (it becomes
   `staging-site-<branch>`). Rename the auto-created `claude/...` branch in cloud sessions.
2. **Decide template vs. override** per rule:
   - Systematic changes (affect a class of views) → edit the shared definitions: the garden
     `*.meta.yml` Jinja blocks *and* any duplicated copies in the MDIM config's
     `definitions`. Change every copy `grep` found, deliberately.
   - One-off deviations (single views) → a dimension-filtered entry under
     `definitions.common_views` in the MDIM config, with a `metadata` override.
3. **Hard-won gotchas** — violating these produces wrong pages that *look* fine in the diff:
   - **Array overrides replace wholesale.** The site's merge overwrites arrays instead of
     merging them (`merge` in `@ourworldindata/utils`), so a view-level `description_key`
     override must repeat **the full list of bullets**, not just the changed one.
   - The `common_views` dimension filter uses **pre-snake-case choice values** as written
     in the config (`"420"`, `No spells`); the final URLs/slugs use the converted forms
     (`_420`, `no_spells`) — numeric slugs get a leading underscore at save time.
   - YAML plain scalars can't contain `": "` — use `|-` block scalars for prose.
   - In Jinja edits, reuse the file's existing definitions/macros patterns; don't invent a
     new mechanism when an existing block does the job.
   - `description_key` is historically a **list** rendered as bullets. The platform is
     moving to allow free rich text (paragraphs, explicit bullets). Follow the author's
     presentation choice, but verify the current schema/renderer actually supports non-list
     content before emitting it; when unsure, default to the list form and tell the author.
4. **Rules snapshot**: commit the agreed "Output from Claude" content as
   `<short_name>.rules.md` next to the MDIM config, headed by the Google Doc link and the
   date. Intent and implementation must travel in the same PR.
5. **Checks**: run `make check` before committing (and the unit tests if you touched
   `apps/`). Commit style and PR attribution per this repo's CLAUDE.md — every PR body
   starts with the `> _Written by Claude <model> — @<handle> at the wheel._` line.
6. **Predict before you push**: state the expected blast radius ("N of M views change,
   each with exactly <change>"). If you can, verify offline against the production config
   before pushing. A prediction that staging then contradicts is a bug to investigate,
   not to explain away.

## Handoff — reviewing the change

Give the author (plain language, real links):

- The PR link.
- The staging **Metadata Diff** tool: `http://staging-site-<branch>/etl/wizard/metadata-diff`
  — Blast Radius tree for "what changed where", View diff pages for word-level red/green.
- A live example view: `http://staging-site-<branch>/admin/grapher/<url-encoded catalog path>/?<dimension params>`
  (read the exact choice slugs from the stored config — never guess URLs).
- **Timing expectations**: garden-template edits re-run the whole garden step on staging
  (tens of minutes; watch the `buildkite/etl-automated-staging-environment` status on the
  commit). Config-only edits are much faster. The Metadata Diff tool caches for ~5 minutes.

Then iterate: author reviews on staging → wording changes go **doc-first** → regenerate →
push. The PR is reviewed and merged by a data manager as usual; nothing ships without that.

## When something looks wrong

- Diff shows changes the author didn't ask for → check for duplicated definitions you
  missed, and check the baseline (staging compares against production, which may already
  differ from master).
- A view kept old text → an existing view-level override is pinning it (overrides win
  over templates; remember they freeze the *entire* array).
- Staging shows stale state → the build may still be running (commit status), or you're
  inside the tool's cache window.
