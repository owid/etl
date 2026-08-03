---
tags:
  - Data Workflow
icon: lucide/signpost
---

# Redirecting explorers and charts to MDIMs

When an MDIM replaces an explorer or a set of charts, the old URLs must keep working. This
is the order of operations.

!!! warning "Redirects are created in production, and there is no bulk undo"
    Explorer redirects are removed one row at a time; the chart CLI unpublishes the source
    charts in the same transaction. The irreversible step comes last, on purpose.

## First: the MDIM has to be ready

Publish it, and **add the MDIM views you want as featured metrics by hand**. Nothing
propagates them — a topic page's *All charts* block is built from charts only, so it cannot
list an MDIM, and an explorer's featured position disappears with the explorer.

An unpublished MDIM is refused as a redirect target, and even if a row existed the baker
filters on publication, so the redirect would serve nothing.

## Explorers

Steps 1–3 are reversible; step 4 is not.

### 1. Replace the references in articles

Anything that **embeds** an explorer breaks the moment the redirect exists — the embed
renders by fetching the explorer page and parsing it, so a 302 leaves it blank. Prose links
survive on the 302 but should be updated anyway.

`/map-explorer-to-mdim` lists every reference with its replacement URL and the view it will
land on. Do the edits now.

### 2. Run `/map-explorer-to-mdim`

You write the routing rules; it emits **one `admin_bulk_payload.json` per explorer**. Post
that file, not `mapping.json` — the payload has empty-valued source dimensions stripped, and
keeping them routes those views to the catch-all instead of their targets.

### 3. Clear site redirects involving the explorer

A `/explorers/<slug>` that is already a site redirect's **source**, or its **target**, makes
the bulk endpoint reject the redirect — and it caches per-source checks, so one row fails
*every* entry for that explorer. Fix at `/admin/site-redirects`:

- **source** `/explorers/<slug>` → delete the row.
- **target** `/explorers/<slug>` → **repoint**, don't delete: many are real URLs and
  deleting one creates a 404. Delete then re-create with the MDIM as target. Only a vanity
  path nothing links to should just go.

Either action re-bakes automatically.

### 4. Apply

At `/admin/multi-dim-redirects`, use **Bulk-create redirects from JSON**, once per explorer.
Rehearse on staging if you like, but the real redirect is created **in production**. Verify:

```bash
curl -sI "https://ourworldindata.org/explorers/<slug>?<one view's params>" \
  | grep -i "^HTTP/\|^location:"   # expect 302 + a /grapher/... Location
```

Allow the bake plus a couple of minutes for the edge cache.

!!! note "The redirect darkens the explorer immediately"
    It is checked on *every* `/explorers/*` request, before the explorer page is served. So
    creating it is what retires the explorer — you cannot stage the two separately.

### 5. Retire the ETL step

**Never delete a step without archiving it** (see `CLAUDE.md`):

1. Delete the step's `.py` **and** its `.config.yml` — leaving an orphaned config has
   happened before.
2. Remove the `dag/*.yml` entry, `make check`, **commit**.
3. Run `etl archive-dag` and commit `dag/archive/*.yml` separately — it reads *committed*
   history. `git checkout` anything unrelated it sweeps in.
4. Archive anything now orphaned upstream (a garden step that only fed this explorer) in a
   second round.

!!! danger "Grep before deleting a shared step"
    Some explorer data steps are read **off-DAG**, by scripts fetching their published CSVs
    by URL. `etl archive-dag` cannot see those, so the step looks like a safe leaf when it
    is not. Search for its catalog URL first, and keep it until every consumer is retired.

## Charts

### 1. Run `/map-charts-to-mdim`

It matches charts to MDIM views by indicator ID and reports **every place each chart is
linked or embedded** with the replacement URL. Embedded references break at unpublish and no
redirect repairs them, so migrate those first.

### 2. Re-create any narrative charts

There is no way to repoint one, so each is re-created from the MDIM view and the article
pointed at the new name. The entity selection, other controls, and any overridden
title/subtitle/footnote do **not** carry over.

### 3. Hand the CSV to a Grapher developer

You get a `;`-delimited CSV plus a handoff note. **They run the migration, not us.** Ask
Martin first — he wrote the CLI; any Grapher developer can run it otherwise.

### 4. They run the CLI

`createMultiDimRedirectsFromCsv` creates the redirects, unpublishes the source charts, and
migrates their old slugs in one transaction.

!!! warning "This cannot be done from the admin UI"
    *Multi-dim redirects* refuses any source that already has an old slug pointing at it,
    and its bulk endpoint takes explorer sources only. Most charts have old slugs, so the
    CLI is the only route — and it is what repoints them, so hand-unpublishing a chart first
    turns its old URLs into 404s.

### 5. Log the cutover date

Ask for confirmation of when the run happened and record it. Once a chart stops being
published its view history resolves to a null chart id, retroactively, so a continuous
series needs the old slug before the cutover and the MDIM slug after.

## Why the two differ

| | Explorers | Charts |
|---|---|---|
| Applied with | admin **Bulk-create redirects from JSON**, one payload per explorer | `createMultiDimRedirectsFromCsv`, run by a Grapher developer |
| Redirect fires | every request — the source goes dark at once | only on a 404, so the source must be unpublished |
| Embeds break | when the redirect is created | when the chart is unpublished |
| Old slugs | n/a | migrated by the CLI; the admin API refuses them |
| Undo | one row at a time | none |
