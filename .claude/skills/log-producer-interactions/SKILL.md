---
name: log-producer-interactions
description: >-
  Log the analytics reports sent to OWID's data producers, and the producers' replies, into the
  shared Notion reports log: one row per email, incrementally and without duplicates. Use after
  sending a round of producer analytics reports, or when the user says "log the report emails" or
  asks whether a producer ever replied to a report. Searches only the addresses recorded in the
  contacts table, only for report threads, and asks before writing anything. Runs against the
  caller's own mailbox only.
metadata:
  internal: true
---

# Log data producer report emails

Keep the shared Notion log of analytics-report exchanges up to date from the caller's Gmail. The log answers one question: which producer got which report, when, and what did they say back. A round of reports is eight to ten emails plus replies, so a run should cost the caller a minute.

**Scope, and why it is this narrow.** The log covers the analytics reports and their replies. Nothing else: not data issues, not release announcements, not introductions or scheduling. Two reasons, both learned the hard way:

- A Gmail-derived log can only ever hold the mailbox of whoever runs it. Calls, Slack, and anything a colleague handled are invisible to it, so a broader log looks like a complete history of a relationship while missing most of it, and colleagues then read "last contact January 2026" and conclude nobody has been in touch since.
- Producer correspondence is private correspondence. Copying it wholesale into a shared table exposes exchanges the sender would not expect a team to read.

**Data issues belong in Front**, the shared inbox everyone can act in, not here.

Background on the project: the "Data producer relations" Notion page (linked from the contacts table) explains why the reports exist and how they are produced.

## Prerequisites

- The **Gmail** and **Notion** claude.ai connectors, available in the session. Load the tools with ToolSearch before calling them: `mcp__claude_ai_Gmail__search_threads`, `mcp__claude_ai_Gmail__get_thread`, `mcp__claude_ai_Notion__notion-fetch`, `mcp__claude_ai_Notion__notion-query-data-sources`, `mcp__claude_ai_Notion__notion-create-pages`.
- Two entries in the repo's `.env` (ask Pablo for the values; see `.env.example`):
  - `NOTION_DATA_PRODUCERS_CONTACTS_TABLE_URL`: the contacts table (producers, contact people, report emails).
  - `NOTION_DATA_PRODUCER_INTERACTIONS_TABLE_URL`: the reports log.

Read both with `grep '^NOTION_DATA_P' .env`. If either is missing, stop and tell the user which one to add. Never hardcode the URLs, names, or email addresses in this skill or in any file committed to the repo: the repo is public.

## Procedure

### 1. Load the two tables

Call `notion-fetch` on each URL. From the results keep:

- the contacts data source URL (`collection://...`) and, for every row: page URL, `Name`, `Contacts`, `Emails for analytics reports`;
- the log's data source URL and its schema. Expected properties: `Summary` (title), `Date`, `Producer` (relation to contacts), `Direction`, `Type`, `Link`, `Notes`, `Status`, `Owner`. There is deliberately no channel property: every row comes from an email. If a property is missing, stop and report it rather than improvising.

Query the contacts rows with `notion-query-data-sources` in SQL mode (single data source). If the workspace's SQL quota is exhausted, view mode works and is not metered.

### 1b. Identify the caller's mailbox

Run `search_threads` with query `in:sent newer_than:90d`, `pageSize: 1`, and take the `sender` of the returned message as the caller's address. It is needed for the `authuser` part of every Link and to tell outgoing from incoming messages.

### 2. Collect the recorded addresses

For each producer, take every email address appearing in `Contacts` and `Emails for analytics reports`. Both columns are free text (`Contacts` typically reads `TO: <name> (<address>)`), so pull the addresses out with `[\w.+-]+@[\w.-]+\.\w+` rather than using the cell whole.

**These addresses are the only thing this skill ever searches for.** Do not search by the producer's domain, and do not search by the producer's name. A domain search returns everything the institution ever sent, most of which has nothing to do with us; a name search returns newsletters and third parties discussing the producer. Both were in an earlier version of this skill and both were wrong. If a contact writes from an unrecorded address, the reply simply will not be found, and the fix is to add that address to the contacts table, not to widen the search.

### 3. Find the watermark and the already-logged messages

Query the log once for `Producer`, `date:Date:start`, `Link`. Keep:

- the latest date per producer **among rows whose `Link` is set**, i.e. rows that came from an email (the watermark). Never take the maximum over all rows: a hand-added row (a call, a Slack thread) carries no Gmail link, and if it is dated in the future it would push the watermark forward and make the next run skip every email in between;
- the set of Gmail message IDs already logged: the part after `#all/` in every `Link` (older rows may use the `/mail/u/0/#all/<messageId>` form; treat both the same).

Search from **7 days before the watermark** (Gmail dates and thread grouping make a small overlap safer than an exact cutoff). For a producer with no rows yet, search the last 12 months.

### 4. Search Gmail, then keep only the report threads

One `search_threads` per producer, `pageSize: 50`, addresses only:

```
addr1 OR addr2 OR addr3 after:YYYY/MM/DD
```

Paginate with `pageToken` until exhausted. Then fetch each thread containing an unlogged message with `get_thread` and `messageFormat: "PLAIN_TEXT"`, and split it into messages. If the result is saved to a file, run:

```
.venv/bin/python .claude/skills/log-producer-interactions/scripts/strip_quotes.py <saved_file>
```

It prints one block per message (id, date, from, to, cc, subject) with quoted replies, gateway banners and signatures stripped. Apply the same stripping by hand to results returned inline.

**A thread is in scope only if it is about an analytics report.** In practice that means an outgoing message that shares one: the subject names the report, and the body carries the Drive link or the PDF is attached. Once a thread qualifies, every message in it is in scope, including replies that wander off topic.

Everything else the search returned is **out of scope**: data issues, release announcements, introductions, scheduling, advice, anything personal. Do not create rows for it and do not paste its body anywhere. Name it in one line in the report, so the user knows what the run saw and can act on it elsewhere.

Skip automated messages entirely: calendar and Calendly notices, Drive share requests, GitHub, Google Docs comments, Notion notifications, newsletters, auto-replies. A Drive share request from a producer contact is worth a mention in the Notes of the related "report sent" row, not a row of its own.

### 5. Propose the rows, and wait

Before writing anything, show the user a table of the rows you intend to create (producer, date, direction, type, summary) and the out-of-scope messages you are leaving out. **Wait for approval.** The log is shared, so a wrong row is seen by the whole team, and one confirmation per run costs far less than removing rows afterwards.

### 6. Create one row per message

Create pages with parent `{"type": "data_source_id", "data_source_id": "<log data source id>"}`, batched (up to ~40 per call), `allow_async: false`. Properties:

| Property | Value |
|---|---|
| `Summary` | One line, at most 110 characters, who did what. Never the subject line verbatim. Shape to aim for, with an invented producer: "Their data lead called the 2026 report useful and asked for a mid-year one". |
| `date:Date:start` | `YYYY-MM-DD` of the message; `date:Date:is_datetime`: the number `0` (Notion rejects the string `"0"`). |
| `Producer` | `["<contacts page URL>"]`. |
| `Direction` | `outgoing` if the sender is `@ourworldindata.org`, else `incoming`. For a colleague's message, add "sent by <first name>" to Notes. |
| `Type` | `report sent` for the message that shares a report; `feedback` for what the producer says back. Those are the only two options the property has, because nothing else is in scope. |
| `Link` | `https://mail.google.com/mail/?authuser=<caller email>#all/<messageId>` (message ID, not thread ID). The `authuser` parameter opens the right Google account whatever order the caller signed in. The link only resolves for the mailbox holding the message, so it is a convenience for its owner, not something a colleague can open. Do not use `git config user.email`; it is often a personal address. |
| `Notes` | Optional, at most 300 characters: people cc'd, whether the PDF was attached, commitments, whether the message got a reply. |
| `Status` | Who owes the next action: `waiting on OWID`, `waiting on producer`, or `closed` when the exchange needs nothing further. Every row carries one, so the table answers "what is still pending" by itself. |
| `Owner` | Set together with Status, to the OWID person who owes the action (user ID from `notion-fetch` of `self` when it is the caller, or leave empty and say so in the report). |

`content`: the stripped body of that message only, as plain markdown paragraphs. Keep the sign-off line; drop signatures, legal boilerplate and everything quoted from earlier messages. A producer's verbatim words about a report are the useful part of the log, because they are what the team can quote in a funding case.

Heuristics for Status: an outgoing report email with no reply yet is `waiting on producer`. An incoming reply that asks something still unanswered is `waiting on OWID`. A reply that needs nothing further is `closed`, and so is an outgoing report once it has been answered. Set `Owner` to whoever sent the report.

### 7. Report

Reply to the user with, in this order:

1. A table of the rows created: producer, date, direction, type, summary. Group by producer.
2. Rows not marked `closed`, with the reason, so the user can correct them.
3. Out-of-scope messages the search surfaced, one line each, and any reply that came from an address not in the contacts table, which is worth adding there.
4. Producers with no new report emails, in one line.

Do not paste email bodies into the reply.

## Guardrails

- Search only the addresses recorded in the contacts table. Never by domain, never by producer name.
- Log only analytics-report threads. Anything else gets a line in the report, never a row.
- Never write a row without showing it to the user first.
- Never edit or delete existing rows except to fill an empty `Link` or empty page body of a row that clearly matches a message (same producer, same date, same direction); say so in the report.
- Never add rows to the contacts table, and never edit it.
- Never write producer names, contact names or email addresses into files in this repo. Examples in this file use invented institutions on purpose.
- Nothing personal or sensitive belongs in a shared log, whatever address it came from.
- Each run covers the caller's mailbox only. Rows for messages the caller received in cc are fine; rows for messages the caller cannot see are not this skill's job.
