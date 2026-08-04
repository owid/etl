# Metadata Diff — build plan

Internal plan for evolving the Metadata Diff Wizard app (`apps/wizard/app_pages/metadata_diff/`)
from a read-only reviewer into a review/sign-off surface that also covers individual charts.
Written for the assistant to execute when the user says go. Nothing here is built yet beyond
what's noted under "Current state".

## Current state (already shipped on `incomes-inequality-wysk`)

- Diffs the **rendered** user-visible texts of MDim views (Title, Description, WYSK/`description_key`,
  Processing notes, Description from producer) + per-view chart FAUST (title/subtitle/note), staging
  vs a selectable baseline (production / master).
- **Blast radius**: separates a change that lives in the shared **indicator** metadata (propagates to
  charts + other MDims) from an **MDim-only override** (contained). Per-view flag + affected-surface list.
- Affected-charts component: paginated (10/pp), hover-preview of the change, links to the chart's data
  page on staging, flags **multi-indicator charts with no data page** (WYSK not shown to readers there).
- Files: `core.py` (pure diff logic), `data.py` (DB reads), `usage.py` (blast-radius: charts + MDims),
  `tree.py` (blast-radius tree + affected-charts component), `app.py` (Streamlit UI).

## The design we converged on

**Two shared objects run the whole thing:**
1. **Distinct text** — the unit of change, review, and edit. A rendered indicator text (a garden
   template output) or an MDim view override. Each carries its *surfaces* (charts w/ data pages +
   MDim views) and its *scope options* (overridable in MDim views; indicator-only for charts).
2. **Anchored comment + snapshot** — feedback pinned to a distinct text, carrying the point-in-time
   value it was written against. Same object both directions: author-intent-in and reviewer-feedback-back.

**Roles are hats, not headcount** (author / data scientist / reviewer may be 1–3 people): attribute
"who authored" / "who approved" when they differ, don't care when they're the same.

**Gate is graded, not binary.** Level = *edit type × reach* (dropping/reordering bullets = structural =
big; reword = small; × how many charts+views it hits). Default: **block while any review flag is
unresolved** (an open flag = the disagreement). Small/low-reach changes need no mandatory review; a
"ship now, review later" bypass is allowed and recorded.

**Phase split.** Author iterates freely across many staging builds with **no snapshots**; hitting
**"send to review"** freezes a baseline; the reviewer then always sees **delta since that snapshot**
(solves staging-1-vs-2 without anything going live — the baseline toggle gains a "last reviewed" option).

**Gdoc stays.** Writing prose in a Gdoc is easier, so Gdoc remains a first-class **input** for both
author intent AND reviewer feedback. The assistant's job is to **ingest Gdoc prose → anchor it to the
distinct texts → ask clarifying questions on the ambiguous/high-level bits**. The in-tool anchored
comments are the canonical store; the Gdoc is an ergonomic front door, not the carrier of record.

---

## Phase A — individual charts (do this first; user wants to try it)

Goal: review a standalone chart's WYSK the same way we review an MDim view. A chart's data page IS its
indicator's WYSK, so this reuses almost everything; charts are just another surface on a distinct text.

**Entry points (both):**
- **Search box** — type a chart slug / id / name → resolve via `charts`/`chart_configs` (mirror the
  `edit-faust-metadata/scripts/resolve_target.py` slug/id/redirect handling).
- **Browse list** — charts whose indicator metadata changed between staging and baseline (reuse the
  `metadataChecksum` comparison that chart-diff already does in `chart_diff.py:_modified_data_metadata_on_staging`).
  This auto-surfaces the review set without needing slugs.

**Data path (new, small):** a chart → a single synthetic "view" bundle.
- Resolve the chart's primary y-indicator (and note total distinct variables → `wysk_shown`, already in `usage.py`).
- Build a `ViewBundle` with `base` = that indicator's `variables` row (per env), no MDim override,
  `chart` = the chart's own config title/subtitle/note. Reuse `core.build_view_bundle` (pass
  `config_metadata=None`, `view={"dimensions": {}, ...}`, the variable row, the chart config).
- `diff_views([chart_bundle], [baseline_bundle])` → the chart's changed fields. Reuse the existing
  side-by-side field renderer (`_render_text_html`, bullets-aware).
- Blast radius: reuse `usage.charts_using_indicators` / `mdims_using_indicators` on the indicator id →
  "this change also affects N other charts + M MDims".
- If the chart is multi-indicator (no data page) → show the existing "not shown to readers" flag prominently.

**UI:** add a type selector at the top of `app.py` (segmented control `MDim | Chart`, mirroring
chart-diff's `diff-type`). Chart mode: search/browse → render the single-view diff (no tree; the tree is
MDim-specific) + the affected-surfaces component.

**Deliberately deferred:** whole-dataset "review every indicator's WYSK" (the broadest lens). The
indicator/dataset entry can come after single-chart works; the dump-mode scripts already do dataset/
indicator iteration if we want a head start.

## Phase B — override generator (the push-all-vs-override boundary)

On the View diff page, per shared field, **"Generate MDim override"** → a copy-pasteable snippet, default
**pin-to-old** (keep baseline wording in this view while the indicator changes), with a custom-text option.
- Route by field: indicator fields → `view.metadata[...]`; `title_public` → `view.metadata["presentation"]["title_public"]`;
  chart fields → `view.config[...]`. Target the view via `view.matches(**dims)` in the MDim `.py`.
- Generates code, never applies (ETL files stay source of truth). For a **dimensional** misfit it instead
  says "clusters on `welfare_type` → garden Jinja" and points at the `edit-faust-metadata` workflow.

## Phase C — review tab (comments, snapshot, gate)

New "Review" mode/tab. Backed by a **staging DB table** (like chart-diff persists per-chart state) so
reviewer + author + two builds share the same records.

- **Anchored comment**: `(branch, distinct-text key = catalogPath#indicator or MDim+view+field,
  build-1 value snapshot, author, body, optional suggested_text, status: open/applied/resolved)`.
- **Change-set comment**: high-level note not tied to one text (assistant triages → specific texts, asks author).
- **Send-to-review** action → snapshot the current rendered texts (values only; tiny) keyed to the review.
- **Delta baseline**: add "last reviewed" to the baseline toggle (reads the snapshot instead of a live env).
- **Gate**: a required PR check reads the branch's review status (blocked while any flag is open; bypass =
  explicit deferred-review record). Level auto-computed (edit-type × reach), human-overridable both ways.
- **Gdoc ingest**: a paste/upload path where the assistant maps Gdoc prose → anchored comments + a
  "here's what I understood / what's ambiguous / what's uncovered" report before anything is applied.

## Output modes (once the diff/review is ready — keep ALL as options)

The diff/review is the shared front-end; the *outcome* can take several shapes, not mutually exclusive.
The user is not committing to one yet — the tool should be able to feed any of them, chosen by
magnitude (edit-type × reach) and by whether a DS review is wanted.

1. **Execute → PR (AI drives to mergeable).** For significant changes:
   - `etl pr "<title>"` (branch off latest master; never manual git+gh) → commit → draft PR.
   - Iterate CI via the `pr-babysitter` skill (Codex review + CI watch loop).
   - Surface the `staging-site-<branch>` link + the Metadata Diff view for verification.
   - Iterate on feedback (review loop / anchored comments).
   - Closeout = the gate at PR level: ask the human — **make live** (squash-merge, *only with explicit
     authorization*) **or** mark **ready-to-review** + assign a data scientist.
   - Guardrails: NEVER auto-merge without an explicit go-ahead; PR body carries the Claude attribution
     blockquote + `@handle`; public repo → no internal context (see CLAUDE.md Team section).

2. **Spec-doc handoff (re-ingestable).** A Markdown summary of proposed changes **by distinct text /
   by view** (surfaces + scope), structured so another Claude session with the skills can ingest it and
   reach the same place. Superset of the `edit-faust-metadata` dump report; decouples review from
   execution (good when executor ≠ reviewer).

3. **In-tool / interactive (the review tab).** Changes captured + approved in the tool; the tool drives
   the PR on approval. The "live" default of Phase C.

The **merge-vs-assign closeout is orthogonal** — it applies to modes 1 and 3 alike, and is the same
graded-gate decision (block on unresolved disagreement; bypass = ship-now-review-later).

## Storage

Prefer staging DB tables (new, additive): `metadata_review` (per branch/change-set: status, snapshot ref,
level, bypass), `metadata_review_comment` (anchored + change-set comments). Snapshots as JSON of
`{text-key: value}`. Mirror chart-diff's table patterns; nothing touches production.

## Open questions to resolve before/while building

- Exact **distinct-text key** (indicator catalogPath+dims for inherited; MDim+view+field for overrides) —
  needs to be stable across builds so comments/snapshots re-anchor cleanly.
- **Gate wiring**: real PR check vs. soft "approved-by/deferred-by" record honored by the merger.
- **Suggested-wording vs comment-only** for the reviewer (keep both; don't force).
- Whole-dataset indicator lens: build into this tool, or leave to dump mode.
