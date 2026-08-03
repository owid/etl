---
tags:
  - Data Workflow
icon: lucide/signpost
---

# Redirecting explorers and charts to MDIMs

When an MDIM replaces an explorer or a set of charts, the old URLs have to keep working:
readers follow links from articles, from Google, and from other sites. This page is the
end-to-end order of operations for that cutover.

!!! warning "Redirects are created in production, and there is no bulk undo"
    Explorer redirects are removed one row at a time, and chart redirects unpublish the
    source charts in the same transaction. Everything below is arranged so that the
    irreversible step comes last, after the checks have passed.

## Before you start: the MDIM has to be ready

Publish the MDIM first, and **add the MDIM views you want as featured metrics by hand**.
Nothing propagates them for you — a retired chart's spot in a topic page's *All charts*
block cannot be inherited by an MDIM (that block is built from charts only), and an
explorer's featured position disappears with the explorer. Decide which views deserve to
be surfaced, and add them, before the old surface goes dark.

An unpublished MDIM is worse than a missing one here: the redirect is refused at creation,
and even if a row existed the baker filters on publication, so the redirect would silently
serve nothing.

## Explorers

Run these in order. Steps 1–3 are reversible; step 4 is not.

### 1. Replace the references in articles and other pages

Anything that **embeds** an explorer breaks the moment the redirect exists — not later,
when the explorer is retired. An embedded explorer is rendered by fetching the explorer
page and parsing it, so once that URL 302s to a grapher page the block renders nothing.
Prose links are fine (the 302 carries them), but they should be updated anyway so readers
don't take an extra hop.

`/map-explorer-to-mdim` produces this list for you, with the replacement URL for each
reference and the specific view it will land on. Do the edits before step 4.

### 2. Run `/map-explorer-to-mdim` to get one JSON per explorer

The skill reads the explorer's views and the target MDIM's views, you write the routing
rules, and it emits **one `admin_bulk_payload.json` per explorer** — that is the unit you
apply. It also reports which views it could not route, which land on the catch-all, and
which referencing URLs carry parameters the explorer no longer offers (those land on the
MDIM's default view rather than the intended one).

Post the payload, not `mapping.json`: the payload has empty-valued source dimensions
stripped out, and keeping them would route those views to the catch-all instead of their
intended targets.

### 3. Remove site redirects that involve the explorer

A `/explorers/<slug>` path that is already the **source** of a site redirect, or the
**target** of one, makes the bulk endpoint reject the redirect — and because it caches its
per-source checks, one such row fails *every* entry for that explorer, not one row.

Check and clear these at `/admin/site-redirects` before applying:

- **Source** `/explorers/<slug>` → delete the row.
- **Target** `/explorers/<slug>` → do **not** just delete it. Many of these are real URLs
  (an old chart slug, a renamed explorer) and deleting one turns a working URL into a 404.
  Repoint it: delete the row, then re-create it with the MDIM URL as the target. Only a
  vanity path that nothing links to should simply be deleted.

Deleting or adding a site redirect triggers a re-bake automatically.

### 4. Apply: paste each JSON into the admin

Go to `/admin/multi-dim-redirects` and use the **Bulk-create redirects from JSON** button,
once per explorer. You can rehearse the whole thing on a staging server, but the redirect
that readers follow has to be created **in production**.

Then verify, per explorer:

```bash
# expect a 302 and a /grapher/... Location
curl -sI "https://ourworldindata.org/explorers/<slug>?<one view's params>" \
  | grep -i "^HTTP/\|^location:"
```

Allow for the bake plus a couple of minutes — the redirect map is cached at the edge.

!!! note "The redirect takes effect immediately, even while the explorer is still published"
    An explorer redirect is checked on *every* request to `/explorers/*`, before the
    explorer page itself is served. So creating it is what darkens the explorer — you do
    not need to unpublish first, and you cannot stage the two separately.

### 5. Retire the explorer's ETL step

Once the redirect is verified, remove the explorer's footprint in this repo. **Never delete
a step without archiving it** (see the archiving rules in `CLAUDE.md`):

1. Delete the step files — the `.py` **and** its sibling `.config.yml`. Removing only the
   `.py` leaves an orphaned config behind, which has happened before.
2. Remove the step's `dag/*.yml` entry, run `make check`, and **commit**.
3. Run `etl archive-dag` and commit the regenerated `dag/archive/*.yml` separately. It reads
   *committed* history, so the removal has to land first. If it sweeps in unrelated steps
   somebody else left un-archived, `git checkout` those files to keep the PR scoped.
4. Check whether anything upstream is now orphaned — a garden step that existed only to feed
   the retired explorer has to be archived in a second round, the same way.

!!! danger "Grep before you delete a shared step"
    Some explorer data steps are consumed **off-DAG**, by scripts that read their published
    CSVs by URL. Those consumers are invisible to `etl archive-dag`, so the step looks like a
    safe leaf when it is not. Search the repo for the step's catalog URL before removing it,
    and keep it until every consumer is retired.

## Charts

### 1. Run `/map-charts-to-mdim`

It matches each chart to an equivalent MDIM view by indicator IDs, and — the part you act on
first — reports **every place a chart is linked or embedded**, with the replacement URL:
article chart blocks, prose links, data insights, static visualizations, and the
*All charts* blocks on topic pages. Embedded references break when the chart is
unpublished, and no redirect repairs them, so they are migrated before the cutover.

### 2. Re-create any narrative charts

If a chart being retired has narrative charts hanging off it, the skill tells you so and
hands you the steps. There is no way to repoint an existing one, so each is **re-created**
from the equivalent MDIM view and the referencing article is pointed at the new one. Two
things do not carry over and have to be set by hand: the entity selection and other
controls, and any title/subtitle/footnote the original overrode.

### 3. Hand the CSV to a Grapher developer

The skill produces a `;`-delimited CSV plus a handoff note. **Give both to a Grapher
developer and let them run the migration** — this step is not run from ETL.

### 4. They run the CLI

A Grapher developer runs `createMultiDimRedirectsFromCsv`, which creates the redirects,
unpublishes the source charts, and migrates the charts' old slugs, all in one transaction.

!!! warning "This cannot be done from the admin UI"
    The *Multi-dim redirects* admin page rejects exactly these cases: it refuses any source
    that already has an old slug pointing at it, and its bulk endpoint accepts explorer
    sources only. Charts with old slugs — which is most of them — can only be migrated by
    the CLI. The CLI is also what repoints the old slugs, so hand-unpublishing a chart first
    would turn its old URLs into 404s.

### 5. Log the cutover date

Ask the developer to confirm when the run happened, and record it. Analytics cannot
reconstruct it afterwards: once a chart stops being published its view history resolves to
a null chart id, retroactively, so a continuous series has to be stitched from the old slug
before the cutover and the MDIM slug after it.

## Why the two paths differ

| | Explorers | Charts |
|---|---|---|
| Applied with | admin **Bulk-create redirects from JSON**, one payload per explorer | `createMultiDimRedirectsFromCsv`, run by a Grapher developer |
| Redirect fires | on every request, so the source goes dark immediately | only when the URL 404s, so the source must be unpublished — which the CLI does |
| Embeds break | when the redirect is created | when the chart is unpublished |
| Old slugs | not applicable | migrated by the CLI; the admin API refuses them |
| Undo | one row at a time | none — unpublishing is part of the transaction |
