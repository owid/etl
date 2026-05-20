---
name: report-indicator-changes
description: Draft a short update message to a topic-owner reviewer after landing substantial dataset or chart changes on staging. The message lists the indicator changes with staging admin links, surfaces open design questions with option tables, and closes with a Chart Diff sign-off CTA. The output is markdown so the user can paste it into either Slack or a GitHub PR comment. Use after a dataset redesign or restructure when the iteration with the reviewer is back-and-forth and you need them to verify on staging before merge. Not for the canonical comms announcement — for that, see `data-updates-comms`.
metadata:
  internal: true
---

# Report indicator changes to a topic-owner reviewer

This skill drafts the kind of message you send to a topic owner when a dataset update or chart redesign needs their sign-off on staging — typically *after* the data work has landed and *before* merging to master. It's the iterative back-and-forth that mediates the final design. The draft is plain markdown so the user can post it in Slack or as a GitHub PR comment.

## When to use this skill

- A restructured / redesigned dataset is live on staging and a topic owner is the gatekeeper for chart-level decisions (categories, labels, titles, which charts to publish vs skip).
- You want to capture a clear status update + open questions in a single scannable message that the reviewer can answer in chat.
- The conversation will likely iterate over multiple rounds (round 1 = list changes, round 2 = reply to their feedback, round 3 = pick between alternatives, etc.).

**Don't use this skill for**:
- The canonical comms-manager Slack form (audience-facing announcement for the OWID communications channel) — use [`data-updates-comms`](../data-updates-comms/SKILL.md) for that. Different audience, different shape.
- Routine PR review handoffs — those just need a PR description.
- One-off questions that don't need a structured update.

## Inputs to gather before drafting

1. **Dataset path** — `namespace/version/short_name` (e.g. `lgbt_rights/2026-05-11/lgbti_national_policy_dataset`). Used to query the staging DB for the affected variables and to construct admin URLs.
2. **Staging site URL** — read from `OWID_ENV.site` (run `from etl.config import OWID_ENV; print(OWID_ENV.site)`), or infer from the current git branch (`staging-site-<branch>`).
3. **Reviewer handle** — Slack handle for a Slack post, or GitHub username for a PR comment. Check `workbench/<dataset>/` for prior drafts to confirm.
4. **What's changed since the last round** — concise list of indicator-level changes since the previous message: new indicators built, categories renamed or collapsed, alternative versions added, decisions taken. Tip: `git log --oneline <since>` plus the latest commit messages.
5. **One or two open design questions** — choices the reviewer needs to weigh in on (label wording, schema A vs B, include or skip a category, etc.). If there's nothing actionable for them, the message isn't ready.
6. **Chart Diff URL** — the wizard chart-diff for this branch, pre-filtered to the view the reviewer needs:

   ```
   http://staging-site-<branch>/etl/wizard/chart-diff?show_reviewed=&show-narrative-charts=False&show-article-citations=False
   ```

   The three query params are:
   - `show_reviewed=` (empty value) — keeps already-reviewed charts visible. Without it, the page hides them by default, which can make the diff list look empty after the reviewer's first pass and surprise them.
   - `show-narrative-charts=False` — hides charts that are parents of narrative charts. They show in their own grouped section by default; for a topic-owner pass we usually want the flat list.
   - `show-article-citations=False` — hides the article-citation list under each chart. Useful in the dense first pass; switch back on when the reviewer wants to weigh chart-impact.

## Output structure

Write the draft to `workbench/<dataset>/<reviewer-slug>-update-<N>.md`, picking a short reviewer slug (their first name, GitHub handle, or "reviewer" if it's ambiguous). Keep it short — ideally under ~600 words. Four sections:

### 1. Greeting + status one-liner

Friendly and brief. Examples:

- `Hi @<handle>! Quick update on where we are:`
- `Hi @<handle>! Reposting with the latest staging changes — here's where we landed:`

### 2. What's changed

Group indicator changes by type for scannability. Each group is a short bold heading + a bullet list of indicators with markdown links to the staging admin pages. **Headings should reflect what's actually in this round** — pick whatever buckets make the changes easy to read. Examples from past rounds: "New indicators", "Renamed categories", "Alternative versions to pick between", "Dropped indicators", "Existing indicators kept as-is". Skip groups that don't apply. If there's only one kind of change, a single flat list (no headings) is fine.

Each bullet uses the **staging admin variable URL** format:

```
http://staging-site-<branch>/admin/variables/<id>
```

Markdown link example:

```markdown
- [Same-sex sexual acts](http://staging-site-data-lgbti-policy-v2/admin/variables/1229636) — 5 categories incl. "Criminalized but not enforced"
```

Get IDs from staging:

```bash
make query SQL="SELECT id, shortName FROM variables WHERE catalogPath LIKE 'grapher/<namespace>/<version>/<short_name>/<table>%'"
```

### 3. Open questions / things to discuss

Numbered list. Each question is short. When a question has multiple options for the reviewer to pick between, use a markdown table showing what each option produces (categories, counts, downstream effect). Example shape:

```markdown
**1. Mixed enforcement category — keep or fold?**

| Option | Categories | Count |
|---|---|---|
| A | 5 (incl. "Banned but not enforced") | 71 countries with the new tier |
| B | 4 (folds enforcement back into "Banned") | cleaner legend |

Lean toward A — it surfaces the recent US 2025 story.
```

If a question is closed (yes/no), just ask it directly without the table.

### 4. CTA — Chart Diff sign-off

Single closing paragraph. Asks the reviewer to look at the [`Chart Diff`](http://staging-site-<branch>/etl/wizard/chart-diff?…) for any tweaks they want — FAUST text, colors, sort order, charts to add or drop. End with what unblocks the merge:

> Once you sign off there and pick X, I'll do Y and we can move to merge.

## Style guide

- Conversational, short paragraphs, no jargon the reviewer wouldn't know.
- No emojis unless the user explicitly requests them.
- All links cited inline as markdown — no bare URLs anywhere. Long staging admin URLs especially.
- Acknowledge corrections explicitly when the reviewer flags something we missed: "Good catch — I had forgotten X. Done now." Keeps trust through iteration.
- Multi-option comparisons go in markdown tables, not bullet lists — the reviewer needs to weigh trade-offs at a glance.
- When in doubt about whether to make a code change, propose it in the message and ask before doing it. Don't surprise the reviewer with changes they didn't ask for.
- **Don't use "chart" as a verb.** "We haven't built a chart for it yet" reads natural; "we haven't charted it yet" doesn't.
- **Avoid "mock up"** when offering to build a quick exploratory chart for the reviewer. "Happy to build one" lands the offer without the throwaway tone.
- **Don't pin the dataset name to a version date.** "X 2026-05-14 update" reads stiff. Prefer "X update" or "latest X update" — the date sits in the PR / commit log where it belongs.
- **Don't reference the PR number in the body** of a message that's being posted as a PR comment. The PR is implicit context; calling it out again ("on PR #6123") makes the prose read like an internal status report rather than a conversational ping.

## What NOT to put in the message

- Snake_case column names or internal slugs unless the reviewer is technical and asked to see them.
- Long YAML or code blocks — link to the file in GitHub instead.
- "We should probably do X" speculation — frame as a question or a proposed option with trade-offs.
- Internal-team jargon (CI run names, PR numbers without context, etc.).

## Iteration pattern across rounds

Each round of the back-and-forth has a slightly different shape. A typical sequence:

- **Round 1 — first update**: The opening message after a substantial garden + grapher landing. Long-ish: lists all indicators by group, surfaces the headline open questions, links to Chart Diff. Establishes the design framing.
- **Round 2 — reply to their feedback**: Point-by-point response to the reviewer's comments. Mirrors the reviewer's numbering. Owns mistakes, proposes options.
- **Round 3 — status check-in / pick a winner**: Shorter. Confirms what's landed, surfaces 1–2 remaining picks (often via a two-option pick: "version A or version B?"). Asks for Chart Diff re-pass.
- **Round N — Sign-off and merge prep** (last round): Confirms reviewer's picks are landed, links Chart Diff one more time, asks for final go-ahead.

## Examples

Canonical drafts from the LGBTI v2 redesign cycle (PR #6110) live under `workbench/lgbti_national_policy_dataset/` — `ls` that directory for files matching `*-update-*.md` and `*-reply-*.md`. The set covers:

- A full first-round update with grouped indicator lists, three open questions with option tables, and a Chart Diff CTA.
- A point-by-point reply to a numbered list of reviewer questions.
- A short status check-in with a two-option pick (e.g. "version A or version B?") and a Chart Diff re-pass CTA.

Read these before drafting a new round for a different dataset — the patterns transfer.

## Artifacts

- `workbench/<dataset>/<reviewer>-update-<N>.md` — the draft, ready to paste into Slack or a GitHub PR comment.

## Cross-references

- [`update-dataset`](../update-dataset/SKILL.md) — for the upstream pipeline work that produces the changes being reported here.
- [`data-updates-comms`](../data-updates-comms/SKILL.md) — for the canonical comms-manager-facing Slack announcement once everything is merged.
- [`check-chart-preview`](../check-chart-preview/SKILL.md) — for visually verifying a chart on staging before sending the message.
