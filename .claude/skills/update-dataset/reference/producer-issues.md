# Producer issues: log, verify, find a contact, draft

Problems in the producer's data turn up all through an update. This file says how to collect them in one place and,
before the merge, turn the ones that survive a check of the producer's own documentation into one message to them.
The mechanics are in `scripts/producer_issues.py`; run it rather than redoing its checks by hand.

```bash
S=.claude/skills/update-dataset/scripts/producer_issues.py
.venv/bin/python $S template <short_name> <namespace>/<new_version>/<short_name>   # start the log
.venv/bin/python $S check <short_name>          # ready entries, and what holds back the rest
.venv/bin/python $S history <namespace>/<new_version>/<short_name>   # earlier reports in the repo and on GitHub
.venv/bin/python $S contacts <namespace>/<new_version>/<short_name>  # where to write
.venv/bin/python $S draft <short_name> [--ids 1,3]   # the message skeleton, ready entries only
```

## The log

`workbench/<short_name>/producer-issues.yml` (the workbench is gitignored, so contact details never reach the
public repo). Start it at the first problem; the template shows every field. Add an entry the moment a step surfaces
something that may be the producer's:

- the snapshot diff and the "Surface new indicators" check: values revised without a note, rows or entities gone,
  relabeled categories;
- sanity checks, the harmonization audit, carried-over `# NOTE:` workarounds that still apply;
- both fact-check passes (6c-bis and the Anomalist pass of step 7): every source-attributed 🔴 and 🟡, and every
  "producer-doc vs. shipped-file" 🟢;
- the data-diff report and the user's QA review.

One entry per problem, written in the producer's terms: their file or sheet, their indicator code or name, their
country name, their period. Record our value and the expected one with the evidence, and say what we did about it
(`corrections.yml` entry, kept as shipped, caveat in the metadata).

## Attribution first

An entry is the producer's only when the raw snapshot carries it (`in_raw_snapshot: true`; how to read it:
`fact-check-dataset/reference/source-errors.md`, "Attribution before routing"). A problem our processing introduced
is fixed in the step and never sent (`false`). Until it is established, the entry is held.

## The documentation check

Before any entry can go in a message, look for an explanation in **every** kind of documentation the producer
publishes, and record each search in `docs_checked`: the document, what you looked for, what you found.

| `kind` | Where to look |
|---|---|
| `methodology` | methodology, technical notes, sources and methods |
| `codebook` | codebook, data dictionary, indicator definitions or series metadata |
| `country_notes` | country notes, series-by-country notes, country metadata files |
| `release_notes` | release notes, what's new, revision notices, errata pages |
| `file_notes` | footnotes, flags, estimate markers and notes sheets inside the data file itself |

A kind the producer doesn't publish is recorded as `document: not published`, so "not searched" and "doesn't exist"
stay apart. Use the documentation the fact check saved (`ai/fact-check/<ns>/<version>/<short_name>/docs/`), and
fetch what it lacks the way `.claude/rules/producer-documentation.md` says, including the release you are ingesting
(an explanation published after the snapshot counts). Then set `explained`:

- `yes`: the documentation explains it. The entry is not sent. Check instead whether our metadata carries the
  caveat the producer gives, and add it if not.
- `partly`: something touches on it but doesn't settle it. It is sent as a question that quotes the passage
  (`explanation`).
- `no`: nothing explains it. It is ready.

`check` holds back every entry with a kind unsearched, a search without its result, or no attribution.

## Earlier reports

So that a known problem goes out as a follow-up rather than as news, look before drafting:

- **The repo and GitHub** (`history`): `reported:` dates and statuses in every version's `corrections.yml`; owid-issues
  issues, open and closed, on the dataset and the producer; earlier PRs of the dataset. Search owid-issues also for
  the topic page the dataset feeds (`gh issue list -R owid/owid-issues --state all --search "<topic>"`), and read
  the previous update PR's body for producer questions left open.
- **Slack**, with the Slack connector, read-only: the producer's name and the dataset's name, for a colleague
  already in touch with them or an earlier report of the same problem.
- **Gmail**, with the Gmail connector, read-only: only the contact addresses found below
  (`from:<address> OR to:<address>`), never the producer's name or domain, which return unrelated mail.

Record what you found in `previously_reported`. A connector that isn't available is a check not run: say so.

## Finding the contact

In this order, and say in the hand-off which route the contact came from:

1. **OWID's producer contacts table** in Notion, read with the Notion connector. Change it only as the last paragraph
   of this section says: a row added on the user's yes, nothing else. Its link is
   `NOTION_DATA_PRODUCERS_CONTACTS_TABLE_URL` in the repo's `.env`; when it is not set, ask the user for the link
   once and append that line to `.env`. Those contacts receive OWID's analytics reports, so ask the user whether the
   person is also the right one for a data question.
2. **A contact the producer publishes for the data** (`contacts` lists them, best first): a data or dataset
   mailbox, then a feedback or contact form, then other addresses on the producer's domain.
3. **The authors**, when the producer publishes no contact: the names in `citation_full` or on the dataset page,
   and an address each one publishes themselves (their staff page, the paper's corresponding-author line, ORCID) at
   their organization's domain. Never build an address from a name pattern, and record the page it came from.
4. Nothing found: say so, and hand over the draft without a recipient.

**A contact the table doesn't have yet:** when routes 2 or 3 find one, or the producer has no row at all, suggest
adding it to the table, in the same message as the draft. Give the producer, the person or mailbox, the address or
form, what it is for (data questions), and the page it was found on, laid out to fit the table's own columns. Add
the row through the Notion connector only when the user says yes; otherwise leave the table as it is.

## The draft

Offer it once, after the user's QA sign-off and before the pre-merge release check, when `check` lists ready
entries. An empty log, or one with no ready entry, completes the item: say so. The user chooses which entries go in;
`draft --ids` builds the skeleton from those only and refuses any entry that isn't ready.

Rewrite every `[FILL]` for the producer, who reads it cold:

- the ask first: what we would like them to confirm or correct;
- one numbered item per problem, with its exact location and values, the evidence, and what we checked in their
  documentation, so they don't point us back to it;
- questions as questions: we may be wrong, and a "partly" entry quotes the passage it rests on;
- a follow-up says when it was raised before;
- plain words, American English, no internal context (no colleagues' names, no Slack or Notion references), and no
  claim beyond what the evidence shows.

Save it to `workbench/<short_name>/producer-message.md` and show it in the reply. When the contact is an email
address, offer to create a Gmail draft with the Gmail connector; for a form, give the text and the form's link.
Claude never sends it.

## After it is sent

When the user says it went out: mark those entries `status: sent`, add the `reported:` date to the matching
`corrections.yml` entries, and list the open question in the PR's open items as "reported to the producer on
<date>" (the repo is public: no names or addresses). A reply moves the entry to `answered`, and its outcome goes
where it belongs: a corrections entry removed, a metadata caveat added, a `# NOTE:` for the next update.
