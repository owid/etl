---
name: pr-babysitter
description: Spawn a background agent that babysits an open PR — triggers a Codex review, watches CI, judges and fixes valid findings, replies to and resolves review threads, and loops to a cap. Never merges. Trigger when the user asks to "babysit the PR", "ask for codex review(s) and watch CI", or after pushing a substantial chunk to a PR branch.
metadata: { internal: true }
---

# PR Babysitter

Run the full **review → wait → fix → re-review** loop for a PR hands-off, in a background agent, so the main session can keep working.

## When to use

- The user asks to get a PR reviewed and watched until green.
- You just pushed a substantial chunk of work to a PR branch.

Only ONE babysitter per PR. If one is already running, message it (SendMessage) instead of spawning another, and never duplicate its work from the main session: don't post extra `@codex review` triggers, don't run your own CI watchers, don't reply to or resolve review threads yourself. If you must act on the PR (e.g. the user asks directly), tell the agent exactly what you did, with timestamps.

## Setup (main session)

1. Post the trigger as a bare PR comment: `gh pr comment <n> --body "@codex review"`. Record the exact `created_at` timestamp of that comment (`gh api repos/owid/etl/issues/<n>/comments`).
2. Spawn a `general-purpose` background agent with the prompt template below, filled in. The agent works in the SAME checkout on the SAME branch — warn it that the main session may also push commits mid-loop.
3. When the completion notification arrives, relay the report. If the agent stops early (its notification says it is "waiting"), resume it with a message telling it to keep polling in short bash calls rather than ending its turn.

## Agent prompt template

Fill every <placeholder>. Keep all rules — each one exists because its absence caused a real failure.

---

You are babysitting PR #<n> on <repo> (branch <branch>) until CI is green and the Codex review is addressed. Work from <repo path>, already checked out on <branch>. A "@codex review" comment was posted at <exact UTC timestamp>. The main session may push its own commits to this branch while you work.

Loop (max <3> iterations, then stop and report):

1. **CI**: `gh pr checks <n> --watch --interval 60` (up to 30 min). On failure: read logs (`gh run view <id> --log-failed`), diagnose, fix.
2. **Wait for the review**: poll every 2-3 minutes for a Codex response LATER than the trigger timestamp above, checking BOTH endpoints — Codex submits a formal review (`pulls/<n>/reviews`) when it has findings, but posts a plain issue comment (`issues/<n>/comments`, e.g. "Didn't find any major issues") when it has none; watching only the reviews endpoint strands the loop forever on a clean review. Poll in SHORT bash calls (one `sleep 120` + both checks per call, repeated as separate tool calls) — never one long multi-minute loop, so that queued messages from the main session can reach you between calls. Give up after 30 minutes and say so in your report.

   **The polling happens by YOU making the next tool call, in this same turn.** There is no such thing as "arming a monitor", "watching for events", or waiting to be notified — nothing you set up keeps running once you stop, and phrases like "I'll act on events as they arrive" mean you have stopped. After every poll call that comes back empty, immediately make the next poll call. You end your turn exactly once: when the final report is written. Never before — not after CI passes, not after "setting up" anything, not while "waiting".
3. **Judge each finding.** Valid: real bugs, wrong data handling, broken asserts, metadata errors. Invalid: style nitpicks contradicting CLAUDE.md conventions, or suggestions to undo deliberate decisions listed in the PR description. When a finding touches a decision you know the main session made deliberately, rebut rather than fix.
4. **Fix valid findings**: `git pull --rebase` FIRST (the branch may have moved). Use `.venv/bin/` for everything. Verify with the relevant `etlr` steps and `make check`. Commit `🐛🤖`/`🔨🤖` + "Co-Authored-By: Claude <model name> <noreply@anthropic.com>", push.
5. **Reply to every finding's inline comment** (fixed → what you did + commit hash; rebutted → why). Every reply MUST start with this exact first line:
   `> _Written by Claude <model name> — @<handle> at the wheel._`
   This is a public repo: plain language, no internal context, no names of people.
6. **Resolve each thread you addressed** (replying does not resolve it): match the REST inline-comment id to `databaseId` of the thread's first comment in GraphQL —
   `gh api graphql -f query='query { repository(owner: "<owner>", name: "<repo>") { pullRequest(number: <n>) { reviewThreads(first: 50) { nodes { id isResolved comments(first: 1) { nodes { databaseId } } } } } } }'`
   then `gh api graphql -f query='mutation { resolveReviewThread(input: {threadId: "<id>"}) { thread { isResolved } } }'`.
   Leave threads you did not address open for the human.
7. **Re-trigger** a fresh bare `@codex review` comment ONLY if you pushed a substantial code fix (metadata-only tweaks don't count). Record its exact timestamp and use it as the new polling threshold. Then loop back to 1.
8. NEVER merge. Never force-push. Never edit `dag/archive/*`.

Final report: status of every CI check; each finding with verdict (fixed+commit / rebutted+why); threads resolved; commits pushed; anything left for the human.

---

## Lessons already folded in (do not relearn)

- The agent must keep polling within its turn (ending the turn "to wait" strands the loop until someone resumes it) — but in short bash calls, never one long multi-minute loop: messages from the main session can only be delivered between tool calls, so a long sleep makes the agent unreachable.
- Agents talk themselves into stopping with "monitors are armed, I'll act on events as they arrive" — there are no monitors; nothing runs after the turn ends. The prompt must say explicitly that polling means making the next tool call yourself, and that the turn ends exactly once, at the final report.
- Codex answers on two different surfaces: findings arrive as a formal review, a clean pass arrives as a plain issue comment ("Didn't find any major issues"). Poll both, or a clean review strands the loop until the deadline.
- Replying to a review comment does NOT resolve the thread; resolution is a separate GraphQL mutation.
- Every re-trigger resets the polling threshold; deleted trigger comments make timestamps lie — always pin the threshold to a comment that still exists.
- Main session and agent share the checkout: both must `git pull --rebase` before committing, and the main session must not run a parallel review/CI loop.
