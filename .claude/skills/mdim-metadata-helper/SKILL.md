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

**The author owns the words; you own the plumbing.** You are not the writer here. The
author drafts the texts themselves; your job is to understand the structure they have in
mind, surface gaps and ambiguities, and translate the agreed rules into templates and
config. Only draft wording when explicitly asked for help drafting — and then offer it as
a suggestion for the author to edit and accept into their doc, never as something you
quietly decide.

The workflow is general to any metadata text field; the examples here use the
"What you should know about this data" field (`description_key`), which is where it has
been exercised most.

## The one principle that governs everything

**The rules document is the source of truth for wording. YAML is generated from it.**
Wording decisions happen in the doc (or in conversation, then written back to the doc) —
never by silently editing YAML. If the author asks for a wording tweak mid-implementation,
update the doc first (give them the text to paste), then regenerate.

## Phase 0 — Setup

The author works in a Google Doc with two tabs: **"Rough input to Claude"** (their notes,
texts, and rules, in whatever structure suits them) and **"Clean output from Claude"**
(your structured restatement, pasted back by them once they accept it).

- If they don't have one yet, point them at the team template — the link opens a "Use
  template" page that creates their own copy — and ask them to share their new doc's link:
  https://docs.google.com/document/d/1Fbg_Ps4y86HoHkBswTKzTuVAuT74627OJ4n1AuDuwLE/template/preview
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
3. Work through their **open questions** first, then run the elicitation checklist. The
   "TEXTS & RULES" section is deliberately free-form: authors structure it however matches
   the structure in their head, and that structure can be complex — shared blocks composed
   differently across views, whole families of views with their own texts, rules that key
   on combinations of controls. Do not force it into a "one base text plus tweaks" shape.
   Your questions should *elicit* their structure, not impose yours:
   - **Coverage**: enumerate the MDIM's dimension choices; do the texts and rules account
     for every view? Ask about any combination the doc is silent on.
   - **Composition**: which pieces of text appear in which views, and in what order? Does
     the order vary?
   - **Presentation**: bullets or paragraphs? (See "rich text" note below.)
   - **Links and defined terms**: which must be preserved (`#dod:` terms, article links)?
   - **Exceptions**: confirm each one names a specific view and states the full deviation.
   - **Style**: American spelling; follow the metadata reference guidelines
     (https://docs.owid.io/projects/etl/architecture/metadata/reference/) for the field.
     Flag style issues as questions for the author — don't rewrite their words.

Ask questions in batches, in plain language, quoting their own words back where possible.

## Phase 2 — Structure

Produce a **structured restatement** of the author's texts and rules and ask them to paste
it into the "Clean output from Claude" tab. Iterate until they say it's right; get an
explicit "agreed" before implementing. Two hard requirements:

- **Verbatim texts.** Every piece of wording in the restatement is the author's, copied
  exactly (or their explicitly approved edit). Restructure freely; rewrite nothing.
- **Complete and self-contained.** A colleague could implement from the restatement alone,
  without seeing this chat.

A shape that often works is *named text blocks + composition rules* — it mirrors how the
templates work underneath:

```
SCOPE: <mdim catalog path> · <field(s)> · <views covered>
PRESENTATION: <bullets | paragraphs>

TEXT BLOCKS (author's wording, verbatim):
  [POVERTY-LINE-ABS]: <text>
  [INTL-DOLLARS]: <text>
  ...

COMPOSITION (which views get which blocks, in what order):
  WHEN <control> = <choice(s)> → [POVERTY-LINE-ABS] [INTL-DOLLARS] ...
  WHEN ... → ...

EXCEPTIONS:
  E1: view <dimensions> → <full deviation>

DECISIONS RESOLVED: <one line per open question and its answer>
```

But treat that as a starting point, not a straitjacket — adapt the sections to the
structure the author actually has. Fidelity to their rules beats tidiness of format.

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
4. **Rules snapshot**: commit the agreed "Clean output from Claude" content as
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
