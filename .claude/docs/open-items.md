# What tends to be left open

A checklist for the closing lines of a report on multi-step work — updates, audits,
reviews, migrations. Not an output format: there is no block to fill in and no
headings to emit. The point is that these three things are the ones that get
dropped, so check for them before you call the work done, and mention the ones
that apply in whatever shape reads naturally.

- **Waiting on someone else.** Name who acts and what the ask is, and include a
  locator — a link, a file, an id. An item the next person has to re-derive from
  scratch hasn't really been handed off.
- **Waiting on a decision.** State the exact change, so that "yes" is a complete
  answer and doesn't need a follow-up round to pin down what was agreed.
- **Nobody checked it.** Skipped or failed checks, surfaces left uncovered, sweeps
  that were capped or truncated. This is the one that matters most: silence reads
  as "clean", and that is the only genuinely misleading thing a closing summary can
  do. If you bounded the work, say what fell outside the bound.

When work is revisited, carry the still-open items forward instead of reporting
only what changed since last time, and say when something clears rather than
letting it disappear from the list.

Over-applying this is its own failure. Three labeled headings on top of two loose
ends is scaffolding, not information — a chat report with one thing pending needs
one sentence. Reach for structure when the list is long enough that a reader would
otherwise lose track of an item, not because the categories exist.

## Where it's worth being formal

Two cases justify an explicit, structured list rather than a sentence:

- **A PR body.** Chat scrolls away; the PR description is what a reviewer and
  future-you actually read. Dataset updates in particular generate far more loose
  ends than they close.
- **Work that continues in a follow-up PR** — repointing downstream consumers,
  archiving an old version, charts left on retired indicators. That list *is* the
  follow-up PR's scope, so losing it means redoing the analysis. `/update-dataset`
  treats this as a fourth category alongside the three above, and
  `/review-data-pr` expects to find it carried over.

Skills that end in a hand-off (`/update-dataset`, `/review-data-pr`,
`/check-empty-entities`, `/check-hardcoded-years`, `/edit-faust-metadata`) each
name the danglers their own workflow tends to produce — those lists are the useful
part, and they're more specific than anything here.
