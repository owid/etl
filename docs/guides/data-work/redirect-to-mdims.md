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

Publish it, and **add the MDIM views you want as featured metrics by hand**. Nothing propagates charts and explorers here.

Note that a topic page's *All charts* won't show a redirected MDim, because the block is built from charts only. Have that in mind if you are redirecting charts touching multiple topic pages.

An unpublished MDIM is refused as a redirect target, and even if a row existed the baker filters on publication, so the redirect would serve nothing.

## Explorers

Steps 1–3 are reversible; step 4 is not.

### 1. Run `/map-explorer-to-mdim`

The skill reads the explorer's views and the target MDIM's views, then asks you to write the
routing rules: which explorer view corresponds to which MDIM view. That is the only manual
part, and it is per explorer.

**Review the matches before applying.** `/review-explorer-mdim-mapping` builds an HTML page
showing each explorer view beside the MDIM view it would redirect to, with approve/flag
controls; decisions persist in the browser and export to JSON. You can let Claude know about
these corrections in the matches.

It writes two JSON files per explorer, and only one of them is for posting:

- **`admin_bulk_payload.json`** — this is the file you paste in step 4.
- `mapping.json` — the record of the mapping, useful for reference and diffing.

Posting `mapping.json` by mistake is not obvious, so it is worth knowing why: it keeps the
blank dimension values that a view leaves unset, and a blank never matches a real URL. Those
views would quietly land on the MDIM's default view instead of the one you mapped them to. The
payload has them removed.

It also sweeps the site for everything pointing at the explorer, which is step 2.

### 2. Replace the references in articles

Anything that **embeds** an explorer breaks the moment the redirect exists — the embed
renders by fetching the explorer page and parsing it, so a redirect leaves it blank. Prose
links survive on the redirect but should be updated anyway.

Work from **`references.md`** (and `references.csv`, the same rows for sorting and filtering).
Each row gives you:

- the page holding the reference, and a link straight into its **Google Doc**;
- the **text to search for** in that doc, so you land on the right block;
- the **replacement URL**, and which MDIM view that link will actually resolve to;
- a per-explorer summary: how many references break, how many are just links, and whether
  anything blocks the redirect.

Start with the 🔴 sections — those are the embeds. It also flags links whose parameters no
longer match any view: those land on the MDIM's default view, so they need a deliberate
choice rather than a straight swap.

### 3. Clear site redirects involving the explorer

A `/explorers/<slug>` that is already a site redirect's **source**, or its **target**, makes
the bulk endpoint reject the redirect — and it caches per-source checks, so one row fails
*every* entry for that explorer. Fix at `/admin/site-redirects`:

- **source** `/explorers/<slug>` → delete the row. The explorer URL is being redirected to
  the MDIM instead, and a site redirect on the same path would win anyway.
- **target** `/explorers/<slug>` → **repoint it rather than deleting it.** Delete the row,
  then re-create it with the MDIM URL as the target.

  The reason to repoint: that row's *source* is usually a URL readers still follow — an old
  chart slug, or a renamed explorer. Deleting it without re-creating turns that URL into a
  404. `references.md` gives you the MDIM URL to use for each one.

  Deleting outright is fine only when nothing links to the source. The live example is
  `/poverty-explorer-launch`, a one-off announcement URL from when the explorer shipped:
  nothing points at it, so it can simply go.

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

### 5. Retire the explorer and its ETL step

**Unpublish or delete the explorer in the admin first — by hand.** Removing the ETL step does
not unpublish anything: the explorer row stays in the DB, so it keeps appearing in listings
and search. Flipping `isPublished` in the step's config and re-running is *not* the route.

Then remove the ETL footprint. **Never delete a step without archiving it** (see `CLAUDE.md`):

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

It matches charts to MDIM views by indicator ID, and writes **`references.md`** — the file you
work from before anything is applied (plus `references.csv` for sorting and filtering). Same
shape as the explorer one: the page holding each reference, a link into its Google Doc, the
text to search for, and the replacement URL.

Work the 🔴 sections first: those are embeds, they break when the chart is unpublished, and no
redirect repairs them. 🟡 rows keep working through the redirect but are worth updating. It
also lists the topic-page *All charts* entries, which need nothing — they drop out on their
own — and any narrative charts, which are step 2.

**Review the matches before applying.** The skill also builds an HTML page showing each
chart beside the MDIM view it would redirect to, with approve/flag controls; decisions
persist in the browser and export to JSON, which the preflight then reads so flagged charts
are excluded. To force or suppress a match, put the chart id in `overrides.csv` and re-run —
only matches it is confident about are proposed, and the rest are reported rather than
guessed.

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
