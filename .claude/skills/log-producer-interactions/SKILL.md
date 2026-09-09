---
name: log-producer-interactions
description: >-
  Sync the current user's Gmail exchanges with OWID's data producers into the shared Notion
  "Data Producer Interactions" log, one row per email, incrementally and without duplicates.
  Use when the user says "log my producer emails", "update the interactions log", "sync producer
  interactions", after sending analytics reports, or when someone asks what is pending with a
  data producer. Also flags emails from producers that are not yet in the contacts table, and
  emails that look like open questions or promises, so the team can track them with Status and
  Owner. Runs against the caller's own mailbox only.
metadata:
  internal: true
---

# Log data producer interactions

Keep the shared Notion log of exchanges with data producers up to date from the caller's Gmail. The log is the team's single place to answer "when did we last talk to producer X, about what, and is anything pending". It only works if every run costs the caller under a minute, so this skill is incremental, needs no manual input, and never asks questions it can answer from the data.

Background on the project: the "Data producer relations" Notion page (linked from the contacts table) explains why the log exists and how it relates to the analytics reports produced by `etl/scripts/create_report_for_data_producer.py`.

## Prerequisites

- The **Gmail** and **Notion** claude.ai connectors, available in the session. Load the tools with ToolSearch before calling them: `mcp__claude_ai_Gmail__search_threads`, `mcp__claude_ai_Gmail__get_thread`, `mcp__claude_ai_Notion__notion-fetch`, `mcp__claude_ai_Notion__notion-query-data-sources`, `mcp__claude_ai_Notion__notion-create-pages`, `mcp__claude_ai_Notion__notion-update-page`.
- Two entries in the repo's `.env` (ask Pablo for the values; see `.env.example`):
  - `NOTION_DATA_PRODUCERS_CONTACTS_TABLE_URL`: the contacts table (producers, contact people, report emails).
  - `NOTION_DATA_PRODUCER_INTERACTIONS_TABLE_URL`: the interactions log.

Read both with `grep '^NOTION_DATA_P' .env`. If either is missing, stop and tell the user which one to add. Never hardcode the URLs, names, or email addresses in this skill or in any file committed to the repo: the repo is public.

## Procedure

### 1. Load the two tables

Call `notion-fetch` on each URL. From the results keep:

- the contacts data source URL (`collection://...`) and, for every row: page URL, `Name`, `Contacts`, `Emails for analytics reports`;
- the interactions data source URL and its schema. Expected properties: `Summary` (title), `Date`, `Producer` (relation to contacts), `Direction`, `Channel`, `Type`, `Link`, `Notes`, `Status`, `Owner`. If a property is missing, stop and report it rather than improvising.

Query the contacts rows with `notion-query-data-sources` in SQL mode (single data source), or rows mode if SQL is unavailable on the workspace plan.

### 1b. Identify the caller's mailbox

Run `search_threads` with query `in:sent newer_than:90d`, `pageSize: 1`, and take the `sender` of the returned message as the caller's address. It is needed for the `authuser` part of every Link and to tell outgoing from incoming messages.

### 2. Build the search terms per producer

Two separate sets, used by the two passes below. Only the first one ever produces rows.

- **Addresses (pass 1).** Every email address in `Contacts` and `Emails for analytics reports`. Both columns are free text, so extract with `[\w.+-]+@[\w.-]+\.\w+` rather than reading the cell whole; `Contacts` typically looks like `TO: <name> (<address>)\nCC: <name> (<address>)`. These are the addresses the team has recorded for the relationship, and they are the only basis for logging.
- **Widening terms (pass 2 only).** The producer's name and its obvious short forms, plus the domain of each address above, excluding generic mail providers (`gmail.com`, `outlook.com`, `hotmail.com`, `yahoo.*`, `protonmail.com`, `icloud.com`). These exist because the person who writes is often a colleague of the recorded contact who is not in the table yet. They surface candidates for the report; they never create rows.

A producer with no address at all has no pass 1, only pass 2.

### 3. Find the watermark and the already-logged messages

Query the interactions table once (SQL, single source) for `Producer`, `date:Date:start`, `Link`. Keep:

- the latest date per producer **among rows that came from an email**, i.e. rows whose `Link` is set (the watermark). Never take the maximum over all rows: a `call` row is dated when the call happens, so a scheduled future call would push the watermark forward and make the next run skip every email in between;
- the set of Gmail message IDs already logged: the part after `#all/` in every `Link` (older rows may use the `/mail/u/0/#all/<messageId>` form; treat both the same).

Search from **7 days before the watermark** (Gmail dates and thread grouping make a small overlap safer than an exact cutoff). For a producer with no rows yet, search without a date filter.

### 4. Search Gmail

**Pass 1, the recorded addresses.** This is the only pass that produces rows. One `search_threads` per producer, `pageSize: 50`, query like:

```
addr1 OR addr2 OR addr3 after:YYYY/MM/DD
```

Addresses only, never `from:domain`. A whole institutional domain carries a lot of mail that has nothing to do with the data relationship, and logging by domain would copy it into a shared table. Paginate with `pageToken` until exhausted.

**Pass 2, widening.** One search per producer over the widening terms, same date filter, e.g.:

```
("Aurora Energy Institute" OR AEI OR from:aurora-energy.example OR to:aurora-energy.example) after:YYYY/MM/DD
```

Everything pass 2 finds that pass 1 did not is a **candidate**: do not log it, and do not paste its body anywhere. List it in the report with sender, date and subject, and let the user decide whether it belongs in the log and whether the sender should join the contacts table. A colleague of the recorded contact writing about the data relationship is the case this is for; anything else is noise the user can ignore in one glance.

If a search result is too large and gets saved to a file, filter it with `jq` on the saved file instead of re-running with a smaller page size; the tool result tells you the path.

### 5. Fetch threads and split into messages

For each thread with at least one message not yet logged, call `get_thread` with `messageFormat: "PLAIN_TEXT"`. If the result is saved to a file, run:

```
.venv/bin/python .claude/skills/log-producer-interactions/scripts/strip_quotes.py <saved_file>
```

It prints one block per message (id, date, from, to, cc, subject) with quoted replies and signatures stripped. Apply the same stripping by hand to results returned inline.

Skip automated messages: calendar and Calendly notices, Zoom or Meet invites, Drive share requests, GitHub, Google Docs comments, Notion notifications, newsletters, auto-replies, Slack digests. A Drive share request from a producer contact is worth a mention in the Notes of the related "report sent" row, not a row of its own.

Skip messages whose ID is already logged. Within a thread that mixes producers and third parties (for example a producer introduced to a reader), log only the messages where the producer wrote, or where OWID wrote to the producer.

### 6. Create one row per message

Create pages with parent `{"type": "data_source_id", "data_source_id": "<interactions data source id>"}`, batched (up to ~40 per call), `allow_async: false`. Properties:

| Property | Value |
|---|---|
| `Summary` | One line, at most 110 characters, who did what. Never the subject line verbatim. Shape to aim for, with an invented producer: "Their data lead announced the 2026 Aurora Energy Review and revised the 2025 figures". |
| `date:Date:start` | `YYYY-MM-DD` of the message; `date:Date:is_datetime`: the number `0` (Notion rejects the string `"0"`). |
| `Producer` | `["<contacts page URL>"]`. |
| `Direction` | `outgoing` if the sender is `@ourworldindata.org`, else `incoming`. For a colleague's message, add "sent by <first name>" to Notes. |
| `Channel` | `email`. Use `call` (no Link) only when a message schedules a call whose date is clear; one extra row for the call, with a Note asking to confirm it happened. |
| `Type` | `report sent` when OWID shares an analytics report or numbers; `release notice` when the producer announces or shares new or updated data; `data issue` for errors, inconsistencies, missing data or methodology questions in either side's data; `feedback` when the producer comments on OWID's work or on ideas OWID proposed; otherwise `other`. |
| `Link` | `https://mail.google.com/mail/?authuser=<caller email>#all/<messageId>` (message ID, not thread ID). The `authuser` parameter opens the right Google account whatever order the caller signed in; find the caller's address once per run with `search_threads` on `in:sent newer_than:90d` (`pageSize: 1`): the `sender` of that message is the mailbox owner. Do not use `git config user.email`; it is often a personal address. |
| `Notes` | Optional, at most 300 characters: people cc'd, attachments, commitments, whether the message got a reply. |
| `Status` | Set **only** when the message needs follow-up: `open` (OWID owes an answer or an action), `waiting on producer` (OWID asked, no reply yet). Leave empty otherwise. Never set `closed` when creating. |
| `Owner` | Set together with Status, to the OWID person who owes the action (user ID or URL from `notion-fetch` of the workspace users, or leave empty if unsure and say so in the report). |

`content`: the stripped body of that message only, as plain markdown paragraphs. Keep the sign-off line; drop signatures, legal boilerplate and everything quoted from earlier messages.

Heuristics for Status: an incoming message ending in a question, or asking for something (data, permission, a report, a signature), with no later outgoing message in the thread, is `open`. An outgoing message that asks the producer something, with no later incoming message, is `waiting on producer`. Set `Owner` to the OWID sender of the last outgoing message in that thread when there is one.

### 7. Report

Reply to the user with, in this order:

1. A table of the rows created: producer, date, direction, type, summary. Group by producer.
2. Rows given a Status, with the reason, so the user can correct them.
3. Candidates from pass 2 (people writing from addresses not in the contacts table) and any producer found only by name. Ask whether to add them to the contacts table; do not add them yourself.
4. Producers searched with zero new messages, in one line.

Do not paste email bodies into the reply.

## Guardrails

- Never edit or delete existing rows except to fill an empty `Link` or empty page body of a row that clearly matches a message (same producer, same date, same direction); say so in the report.
- Never add rows to the contacts table.
- Never write producer names, contact names or email addresses into files in this repo. Examples in this file use invented institutions on purpose.
- Log only what concerns the data relationship. A message that is personal, sensitive, or unrelated to the producer's data does not belong in a shared log, even when it comes from a recorded address: name it in the report and let the user decide, rather than creating a row. The same goes for anything a reasonable sender would not expect the whole team to read.
- Each run covers the caller's mailbox only. Rows for messages the caller received in cc are fine; rows for messages the caller cannot see are not this skill's job.
