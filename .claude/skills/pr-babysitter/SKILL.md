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

0. **A round in flight must reach a verdict — or be explicitly written off — before you start another one.** Checking the surfaces once is not enough: a single empty read means Codex has not answered *yet*, and posting a new trigger on the strength of it moves the polling threshold so the answer, when it comes ~4–9 minutes later, is discarded exactly as if you had never checked. So:

   - **Wait it out.** Poll the current trigger's surfaces — an `issues/<n>/comments` clean pass ("Didn't find any major issues"), a `+1` from the Codex bot on the trigger comment or the PR body, a new review with findings — until one of them fires. Then handle it, and only then trigger the next round. That handling is what makes the round *harvested*; reading a verdict and leaving its findings unaddressed is the same failure with extra steps.
   - **Or write the round off — but only if you can still tell its answer apart from the next one's.** Abandoning a round does not cancel the job: it can answer *after* your new trigger, and a timestamp-only filter will read that stale answer as the new round's verdict. What makes the write-off safe is the **`Reviewed commit:` discriminator** in step 2 — every findings review body and every clean-pass comment names the SHA it examined, so a late answer can be matched to the round it belongs to and discarded. If you are relying on a bare `+1` reaction (which names no SHA), you have no discriminator: wait instead. Either way, record in the report which SHA the abandoned round covered and that its verdict went unread — an abandoned round is an *unverified* item, never a silent gap.

   Never a third option. "I checked and there was nothing, so I re-triggered" is the starvation bug in the lesson below.
1. Post the trigger, capture its timestamp **from the same call**, and record the **head SHA the round covers** (`git rev-parse HEAD`, captured at trigger time). The creation response carries `created_at`, so there is no lookup to get wrong, but no SHA — and the recorded SHA is what step 2 matches Codex's own `Reviewed commit:` line against, which is the only way to attribute a verdict to a round when several are in flight or the head has moved. Pass all three to the agent:

   ```bash
   git rev-parse HEAD   # the SHA this round covers
   gh api repos/<owner>/<repo>/issues/<n>/comments --method POST \
     -f body="@codex review" --jq '{id, created_at}'
   ```

   Keep **all three**: the timestamp is the polling threshold, the id is what step 2 reads reactions from (`gh api repos/<owner>/<repo>/issues/comments/<id>/reactions`) to see the `+1` clean signal, and the SHA is what any verdict from this round actually covers. Pass all three to the agent. A findings review names its own commit, but a clean pass does not — so if the head moves mid-round, the recorded SHA is the only thing standing between "clean" and a claim about code Codex never saw.

   Don't post with `gh pr comment` and then search for the comment: that lookup is a paginated connection, and on a PR with more than 30 issue comments the trigger you just posted is not on the first page.
2. Spawn a `general-purpose` background agent with the prompt template below, filled in. The agent works in the SAME checkout on the SAME branch — warn it that the main session may also push commits mid-loop. Cleaner when available: give it a dedicated worktree of the branch, which sidesteps the shared-dirty-tree hazards in the lessons below.
2b. **Pace your own pushes.** One trigger per commit starves the loop: Codex takes ~4–9 minutes to answer, so a push every few minutes means every round is superseded before it reports, and nobody reads the verdicts. Measured on a real PR: eight commits and eight triggers in ~40 minutes produced two clean passes and four findings reviews that the loop never harvested — a human noticed the unread "Didn't find any major issues" comment. When the user is iterating rapidly, batch the edits into one round; if you cannot, step 0's write-off rule applies — an abandoned round gets named in the report, with its SHA, as unverified.
3. When the completion notification arrives, relay the report. If the agent stops early (its notification says it is "waiting", "the monitor will notify me", "the waiter is still looping", or anything short of a final report), resume it with a message telling it to keep polling in short bash calls rather than ending its turn. **Expect to do this 1–2 times per run** — agents routinely stop early despite the prompt's warnings, so treat every non-final-report notification as a stall and send the corrective immediately (state the current trigger timestamp and any commits the main session pushed meanwhile).

## Agent prompt template

Fill every <placeholder>. Keep all rules — each one exists because its absence caused a real failure.

---

You are babysitting PR #<n> on <repo> (branch <branch>) until CI is green and the Codex review is addressed. Work from <repo path>, already checked out on <branch>. A "@codex review" comment was posted at <exact UTC timestamp>, and the branch head at that moment was <SHA> — that is the revision this round's verdict covers, whatever the head is by the time you report. The main session may push its own commits to this branch while you work.

Loop (max <3> iterations, then stop and report):

1. **CI**: `gh pr checks <n> --watch --interval 60` run in the FOREGROUND of your own turn (up to 30 min) — never as a background task you then "wait on"; a backgrounded watcher does not resume you. On failure: read logs (`gh run view <id> --log-failed`), diagnose, fix.
2. **Wait for the review**: poll every 2-3 minutes for a Codex response LATER than the trigger timestamp above, checking ALL THREE surfaces Codex can respond on:
   - `pulls/<n>/reviews` — a formal review, submitted when Codex has findings.
   - `pulls/<n>/comments` — the inline review comments carrying the individual findings.
   - `issues/<n>/comments` — a plain issue comment (e.g. "Didn't find any major issues") posted when Codex has NONE. This is the clean-pass verdict and it is NOT a review; watching only the reviews endpoint strands the loop forever on a clean pass.

   **Do NOT treat a reaction on the trigger comment as the review arriving, unless it is a `+1` from the Codex bot.** Within seconds of the trigger, Codex adds an acknowledgment reaction (👀-style) to the `@codex review` comment, then submits its real review minutes later (~4–9 min observed). Exiting the wait on *any* codex reaction declares the PR "clean" while a review with findings is still in flight — this silently skipped two P2 findings once. So: a **new review (reviews count increased)** is the findings signal; a reaction with `content == "+1"` is the ONLY clean-signal reaction — and it counts ONLY when the reaction's `user.login` is the Codex bot itself. Mind the API split when matching: REST (which the reaction endpoints use) exposes the bot as `chatgpt-codex-connector[bot]`, while GraphQL strips the suffix to `chatgpt-codex-connector` — match with a prefix predicate (login starts with `chatgpt-codex-connector`), not an exact string from the other API. A human collaborator's 👍 is noise, not a verdict — timestamp alone cannot tell them apart, and the reviews-count cross-check does not catch this while the real review is still in flight (it hasn't increased the count yet). **The `+1` can land on the PR DESCRIPTION instead of the trigger comment** (`gh api repos/<owner>/<repo>/issues/<n>/reactions` — the issue-body reactions), with the trigger comment keeping only the 👀 acknowledgment forever — so poll BOTH reaction locations, and count a body `+1` only when it is stamped after the trigger timestamp AND reacted by the Codex bot. After any apparent "clean" verdict (from a `+1` in either location or an issue comment), cross-check that the reviews count did not also increase before you conclude there are no findings.

   **Attribute every verdict by its `Reviewed commit:` line, not by timestamp alone.** Codex
   stamps the SHA it examined into both kinds of verdict — a findings review's body and a
   clean-pass issue comment both contain `**Reviewed commit:** <sha>`. Parse it and require it
   to match this round's recorded head SHA (prefix match: the line is abbreviated, e.g.
   `240588d338` for `240588d33…`). This is the only reliable discriminator: a timestamp filter
   accepts a *late answer from a previous round* as this round's verdict, which on a clean pass
   would declare the current head reviewed when an older revision was. If the SHA does not
   match, the verdict belongs to another round — log it, do not act on it as this round's.
   A `+1` reaction carries no SHA and so cannot be attributed at all; treat it as a clean
   signal only when no other round could be answering, and prefer the issue comment whenever
   one exists.

   One paginated GraphQL call covers both `pulls/` surfaces with the fields you need to
   judge them — author and timestamp per finding, plus `totalCount` for the
   reviews-count cross-check. `reviews(last: 20)` returns the newest, so it needs no
   cursor of its own:

   ```bash
   gh api graphql --paginate -f query='
     query($endCursor: String) {
       repository(owner: "<owner>", name: "<repo>") {
         pullRequest(number: <n>) {
           reviews(last: 20) { totalCount nodes { databaseId author { login } submittedAt state } }
           reviewThreads(first: 100, after: $endCursor) {
             pageInfo { hasNextPage endCursor }
             nodes { id isResolved comments(first: 1) { nodes { databaseId author { login } createdAt body } } }
           }
         }
       }
     }'
   ```

   `databaseId` on each review is what the final sweep reports as its coverage boundary,
   so don't drop it. The bare thread query in step 6 is for *resolving* threads and carries
   neither author nor timestamp — polling with it cannot tell a new Codex response from a historical
   thread. `issues/<n>/comments` (the clean-pass verdict) and the trigger's reactions stay
   on REST; page the former with `--paginate`.

   **Every one of these is a paginated connection — page all of them.** Bare `gh api .../pulls/<n>/reviews` returns only the first 30 items, oldest first, so on a long-lived PR the newest review is not in the response and you will read a fresh review as silence (measured: page 1 fourteen hours stale, with 41 reviews and 60 inline comments present). GraphQL `reviewThreads` is the better route — it also hands you `isResolved` and the thread ids step 6 needs — **but a bare `first: N` truncates exactly the same way**, so page it too (query form in step 6). Either add `--paginate` to the REST calls or use the paginated GraphQL query; never a bare first page of anything.

   Poll in SHORT bash calls (one `sleep 120` + all checks per call, repeated as separate tool calls) — never one long multi-minute loop, so that queued messages from the main session can reach you between calls. Give up after 30 minutes and say so in your report.

   **Final sweep before the report:** Codex can post an *additional* findings review minutes after the first, with no fresh trigger — a loop that concludes after handling the first response walks past the second. Immediately before writing the final report, re-check all three surfaces one last time, record in the report the latest review id + timestamp you saw, and state explicitly that anything Codex posts after that timestamp is outside your run's coverage. The sweep is a snapshot, not a quiet period — a second review has landed ~25 minutes after the first, so the late-review window is handed off to the main session as unverified, never implicitly declared clean.

   **The polling happens by YOU making the next tool call, in this same turn.** There is no such thing as "arming a monitor", "watching for events", or waiting to be notified — nothing you set up keeps running once you stop, and phrases like "I'll act on events as they arrive" mean you have stopped. After every poll call that comes back empty, immediately make the next poll call. You end your turn exactly once: when the final report is written. Never before — not after CI passes, not after "setting up" anything, not while "waiting".
3. **Judge each finding.** Valid: real bugs, wrong data handling, broken asserts, metadata errors. Invalid: style nitpicks contradicting CLAUDE.md conventions, or suggestions to undo deliberate decisions listed in the PR description. When a finding touches a decision you know the main session made deliberately, rebut rather than fix.
4. **Fix valid findings**: `git pull --rebase` FIRST (the branch may have moved; if the shared tree is dirty with the main session's work, use `--autostash`). Use `.venv/bin/` for everything. Verify with the relevant `etlr` steps and `make check`. Stage ONLY the files you edited (never `git add -A`) — and if the main session has uncommitted changes in a file you need to fix, do NOT commit that file at all: staging is file-level, so `git add <file>` would sweep those unreviewed hunks into your commit. Leave it uncommitted, flag the collision in your report, and let the main session fold the fix into its own commit. Commit `🐛🤖`/`🔨🤖` + "Co-Authored-By: Claude <model name> <noreply@anthropic.com>", push.
5. **Reply to every finding's inline comment** (fixed → what you did + commit hash; rebutted → why). Every reply MUST start with this exact first line:
   `> _Written by Claude <model name> — @<handle> at the wheel._`
   This is a public repo: plain language, no internal context, no names of people.
6. **Resolve each thread you addressed** (replying does not resolve it): match the REST inline-comment id to `databaseId` of the thread's first comment in GraphQL —
   ```bash
   gh api graphql --paginate -f query='
     query($endCursor: String) {
       repository(owner: "<owner>", name: "<repo>") {
         pullRequest(number: <n>) {
           reviewThreads(first: 100, after: $endCursor) {
             pageInfo { hasNextPage endCursor }
             nodes { id isResolved comments(first: 1) { nodes { databaseId } } }
           }
         }
       }
     }'
   ```
   `--paginate` needs both the `$endCursor` variable and the `pageInfo` block — without them it silently returns one page, which is the failure this whole rule exists to prevent.
   then `gh api graphql -f query='mutation { resolveReviewThread(input: {threadId: "<id>"}) { thread { isResolved } } }'`.
   Leave threads you did not address open for the human.
7. **Re-trigger** a fresh bare `@codex review` comment ONLY if you pushed a substantial code fix (metadata-only tweaks don't count). Post it exactly as setup step 1 does, capturing **all three** fields:

   ```bash
   git rev-parse HEAD   # the SHA this NEW round covers — your fix commit, not the old head
   gh api repos/<owner>/<repo>/issues/<n>/comments --method POST \
     -f body="@codex review" --jq '{id, created_at}'
   ```

   Replace **all three** stored values — the threshold, the trigger comment id, *and* the head SHA. Keeping the old id means step 2 reads reactions from the previous trigger, where an existing `+1` declares the new round clean before Codex has answered it. Keeping the old SHA is subtler and worse: the round's verdict then gets reported against the revision you just replaced, so a clean pass reads as covering code Codex never saw, and the revision it did see goes unnamed. Then loop back to 1.
8. NEVER merge. Never force-push. Never edit `dag/archive/*`.

Final report: status of every CI check; each finding with verdict (fixed+commit / rebutted+why); threads resolved; commits pushed; **the SHA each verdict covers** — the SHA recorded for the round that produced it (refreshed on every re-trigger, per step 7), plus the head at report time when they differ, since a clean pass on an older revision says nothing about newer commits; anything left for the human.

---

## Lessons already folded in (do not relearn)

- The agent must keep polling within its turn (ending the turn "to wait" strands the loop until someone resumes it) — but in short bash calls, never one long multi-minute loop: messages from the main session can only be delivered between tool calls, so a long sleep makes the agent unreachable.
- Agents talk themselves into stopping with "monitors are armed, I'll act on events as they arrive" — there are no monitors; nothing runs after the turn ends. The prompt must say explicitly that polling means making the next tool call yourself, and that the turn ends exactly once, at the final report.
- The same stall wears other disguises: backgrounding `gh pr checks --watch` (or any "waiter") and ending the turn "until its notification arrives". A backgrounded watcher notifies no one who can act. Even with all warnings in the prompt, agents stall this way 1–2 times per run — the main session's SendMessage corrective ("poll yourself in short calls; end the turn only at the final report") reliably restarts them, so budget for it rather than treating it as exceptional.
- **Page every review surface, on both APIs.** `gh api repos/<o>/<r>/pulls/<n>/reviews` (and `/comments`) returns only the FIRST PAGE — 30 items, oldest first — so on a long-lived PR the newest review is not in the response at all and the loop reads a fresh review as silence. Measured on a real PR: page 1's newest review was 14 hours stale (41 reviews and 60 inline comments existed; 30 of each were visible). Use the `reviewThreads` GraphQL query — which also gives you `isResolved` and the thread ids you need to resolve them — or pass `--paginate`. This cost ~25 minutes of a run: the review arrived in 4.5 minutes and the poller never saw it. The GraphQL route has the same trap — a bare `reviewThreads(first: 50)` truncates just as silently, and a heavily-reviewed PR reaches that (one hit 30 threads across five rounds) — so page the connection with `$endCursor` + `pageInfo`, not just the REST calls. Where a value can be read from a creation response instead of looked up — the trigger comment's `created_at` — do that: it is O(1) and structurally cannot go stale.
- Codex answers on three surfaces: a formal review (`pulls/<n>/reviews`), the inline finding comments (`pulls/<n>/comments`), and — when there are no findings — a plain issue comment ("Didn't find any major issues", `issues/<n>/comments`). Poll all three, or a clean pass strands the loop until the deadline.
- A reaction on the trigger comment is NOT the review, unless `content == "+1"` **from the Codex bot**. Codex adds a fast 👀-style acknowledgment reaction within seconds, then submits the real review minutes later (~4–9 min). Exiting on any reaction declared "clean" and silently skipped two P2 findings (PR 6506). Use a review-count increase as the findings signal and a Codex-authored `+1` as the only clean-signal reaction; after any clean verdict cross-check the reviews count didn't also increase.
- **The clean-pass `+1` can land on the PR description instead of the trigger comment** (PR 6589: the only verdict signal was a `+1` on the issue body 4 min after the trigger — no issue comment, no review, and the trigger comment kept only the 👀). A loop watching only the trigger's reactions polls blind to its deadline; a human noticed the thumbs-up first. Poll the issue-body reactions too (`issues/<n>/reactions`), accept a `+1` there only when stamped after the trigger timestamp AND `user.login` is the Codex bot, and keep the reviews-count cross-check.
- **A `+1` verdict must be authored by the Codex bot, not just well-timed.** Humans routinely 👍 a PR description; content + timestamp alone would read that as a clean pass, and the reviews-count cross-check can't save you while the real findings review is still in flight (it hasn't been submitted yet, so the count hasn't moved). Always check the reaction's `user.login` before treating any `+1` as a verdict, in both reaction locations — and match it as a prefix (`chatgpt-codex-connector`), because REST renders the bot login as `chatgpt-codex-connector[bot]` while GraphQL renders it without the suffix; an exact-string check copied from the wrong API silently rejects every genuine verdict and stalls the loop to its deadline.
- Replying to a review comment does NOT resolve the thread; resolution is a separate GraphQL mutation.
- Every re-trigger resets the polling threshold; deleted trigger comments make timestamps lie — always pin the threshold to a comment that still exists.
- **Attribute verdicts by `Reviewed commit:`, never by timestamp alone.** Codex stamps the SHA it examined into both verdict kinds — the findings review body and the clean-pass issue comment (`**Reviewed commit:** 240588d338`). Without matching it against the round's recorded head SHA, a late answer from an abandoned or superseded round passes the timestamp filter and is read as the current round's verdict; on a clean pass that silently certifies a revision Codex never saw. The `+1` reaction is the one verdict with no SHA in it, which is why it cannot be the sole clean signal whenever another round might still answer.
- **Rapid re-triggering starves the loop — harvest before re-triggering.** Every new `@codex review` resets the polling threshold, so any verdict that arrived for the previous trigger is discarded unread. On a PR taking eight commits and eight triggers in ~40 minutes, Codex answered every round (two clean passes, four findings reviews) and the loop harvested none of them, because each answer landed after its trigger had already been superseded; the unread clean pass was spotted by a human, not by the loop. Two guards, both in Setup above: **step 0** — read every surface for the current trigger before posting a new one — and **pacing** — batch edits into one round instead of triggering per commit. Also state, in every relayed report, which SHA the verdict covers: with a moving head, "clean" without a SHA is unfalsifiable.
- **A concluded loop is not a closed review.** Codex posted a second findings review ~25 minutes after the first, with no new trigger and no new push, after the babysitter had legitimately finished its round (PR 6561: a P1 sat unaddressed until a human noticed). Hence the final-sweep rule in step 2 — and when relaying a babysitter's report, treat its "latest review id seen" as the coverage boundary, not as proof the review is over.
- Main session and agent share the checkout: both must `git pull --rebase` before committing, and the main session must not run a parallel review/CI loop. A dirty shared tree adds two hazards — plain `git pull --rebase` refuses to run (needs `--autostash`), and staging is file-level, so committing a file the main session also edited sweeps its unreviewed hunks into the agent's commit. Both guards are baked into step 4 of the prompt template — they must live INSIDE the template, since a rule listed only in this section never reaches the spawned agent. The clean way to sidestep both hazards is to hand the babysitter its own worktree of the branch.
