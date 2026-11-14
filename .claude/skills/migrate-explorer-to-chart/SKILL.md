---
name: migrate-explorer-to-chart
description: >-
  Move one ETL-authored explorer (a `viz://explorer` step) onto the regular chart pipeline as a
  `viz://chart` multidim shown as a data explorer, end to end and one explorer per PR: inventory
  and blast radius, branch and draft PR, port the step, staging comparison, view mapping and
  reference audit (via map-explorer-to-mdim), a Slack sign-off draft, the production cut-over
  (featured metrics, embeds, site redirects, bulk redirects) and retiring the explorer step.
  Trigger on "/migrate-explorer-to-chart SLUG", "migrate the SLUG explorer to a multidim",
  "port SLUG explorer to viz://chart", "which explorer should I migrate next", or "what's the
  blast radius of migrating SLUG". Tracking issue: owid/etl#6014. For an explorer that is not in
  ETL yet, use migrate-explorer-to-etl first.
metadata:
  internal: true
  status: temporary
  sunset: "retire when owid/etl#6014 closes and no viz://explorer step remains"
  owner: lucasrodes
---

# Migrate an ETL explorer to a multidim

An ETL explorer and a multidim share one engine (`Explorer` subclasses `Chart` in
`etl/viz/explorer/core.py`) and one YAML schema for `dimensions` / `views` /
`definitions.common_views`. So the port itself is small; the work is in proving nothing on the
site breaks. This skill owns the order of operations and the per-explorer paperwork. It does not
repeat what other skills already cover:

| Need | Skill |
|---|---|
| Multidim YAML, `common_views`, publishing | `/create-chart` |
| View mapping, reference audit, featured metrics, preflight, bulk redirects, applying them | `/map-explorer-to-mdim` |
| Every surface referencing the explorer | `/find-chart-references` |
| Screenshots on staging | `/check-chart-preview` |

Status lives in the tracking issue owid/etl#6014 (one checkbox per explorer, grouped in waves).
Tick the explorer there when its redirects are live, not when the PR merges.

## Never without the user's explicit approval

Ask first, every time, and wait for a yes that names the action:

- **Tagging or notifying anyone else**: requesting a PR review, assigning someone, or writing an
  `@handle` in a PR body, comment, commit message or issue. Write a suggested reviewer as a plain
  name until the user approves the tag.
- **Posting to Slack**, in any channel or DM, including drafts. Give the user the text, and they
  post it.
- **Anything on production**: redirects, featured metrics, gdoc edits, unpublishing the explorer.

Approval covers that one action. It does not carry over to the next explorer or the next step.

## 0. Pick the explorer and size it

```bash
ENV_FILE=<prod creds> DATA_API_ENV=production .venv/bin/python \
  .claude/skills/migrate-explorer-to-chart/scripts/inventory.py --out ai/explorer_migration [--slug <slug>]
```

Writes `inventory.csv` and `inventory.json`: views and dimensions, pageviews, published posts that
link to or embed it, site redirects that end on it, featured metrics, step size, and flags for
explorer-only settings. Check which env file holds prod credentials (`ls -la .env*`) and ask the
user if none does.

Start with explorers that have few views, no embeds, no featured metrics and no redirects
ending on them. Read the flags before committing to one:

- `checkbox` — becomes a two-choice `toggle-buttons` dimension. Easy.
- `picker` (`pickerColumnSlugs`) and `related` (`wpBlockId`) — no known multidim equivalent.
  Ask the user how to handle them before porting; don't drop them silently.
- `download` (`downloadDataLink`) — multidims build their own `download_package` on save; the
  grapher download UI may still be pending (owid/owid-grapher#6784).
- `py` (long Python step) — views are generated in code; the port touches the code too.
- A **twin**: some explorers already have an unpublished multidim mirror in
  `etl/steps/viz/chart/` (search `dag/` for the same namespace). Ask whether to build on it or
  replace it, so the end state has one multidim.

## 1. Branch and draft PR

```bash
.venv/bin/etl pr "📊🤖 Migrate <slug> explorer to a multidim" data
```

Suggested reviewer: the first `owners` entry of the garden datasets the explorer reads (follow
its `dag/*.yml` entry to the `data://grapher/...` deps, then the garden `.meta.yml`). Git history
of explorer steps is mostly moves and renames, so `git log` names the wrong people. Report the
name to the user; don't request the review or tag them yourself (see above).

## 2. Port the step

Create `etl/steps/viz/chart/<ns>/latest/<short>.config.yml` from the explorer step, and a
`viz://chart/<ns>/latest/<short>` DAG entry with the same direct dependencies. Add a `.py` only if
the explorer's script does more than load the config and save (a config-only chart step needs
none). Pin `grapher_schema: "012"` (quoted) at the top: the build fails without it. Leave the
`viz://explorer` step and its DAG entry in place: the explorer stays live until the redirects
exist.

Translation of the explorer's `config:` block:

| Explorer | Multidim |
|---|---|
| `explorerTitle` | `title.title` (+ `title.title_variant` if needed) |
| `explorerSubtitle` | `presentation.explorer_subtitle`, with `presentation.page_type: data-explorer` |
| `selection` | `default_selection` |
| explorer slug | `slug` (keep the explorer slug; check on production that no `chart_configs.slug`, `chart_slug_redirects.slug` or `multi_dim_data_pages.slug` has it) |
| `entityType`, `entityTypePlural`, `hasMapTab`, … | the same key in per-view `config` (shared values in `definitions.common_views`) |
| `hideAnnotationFieldsInTitle` | keep it only if the explorer's rendered titles hide the time span (some explorers apply it, some don't, and the served config can't tell): build without it, and add it back in `definitions.common_views` if the rendered headers gain a time span the explorer doesn't show |
| `isPublished`, `subNavId`, `subNavCurrentId`, `thumbnail`, `googleSheet`, `hideAlertBanner`, `yScaleToggle: false` | drop (not grapher keys; `yScaleToggle: true` is `yAxis.canChangeScaleType`) |
| `pickerColumnSlugs`, `wpBlockId`, `downloadDataLink` | see the flags in step 0 |

In view configs: `type: X` becomes `chartTypes: [X]`, `yAxisMin: N` becomes `yAxis: {min: N}`,
and `relatedQuestionUrl` + `relatedQuestionText` become `relatedQuestions: [{url, text}]`. Check any other view key against
the grapher schema (`https://files.ourworldindata.org/schemas/grapher-schema.012.json`).

Dimensions: `presentation.type: radio` and `checkbox` become `toggle-buttons` (drop
`choice_slug_true`); `dropdown` stays, or `dropdown-with-search` for long choice lists. Keep the
dimension and choice slugs unchanged — the view mapping in step 4 is then close to the identity.
A choice can carry a `description`: a dropdown shows it under the option, and toggle buttons show
all of them in an info tooltip on the dimension label. Use it for choices whose name is jargon.

In the Python step, `paths.create_explorer(...)` becomes `paths.create_chart(...)`, and
`c.save(tolerate_extra_indicators=True)` becomes `c.save()`. Add `topic_tags` from the
explorer's tag (`explorer_tags` in the DB). The build sorts views by choice order, and the
default view is the first view after sorting (or `default_dimensions`, if set). Make it match the
explorer's default, which is the first `graphers` row of `explorers.tsv` in the DB (the explorer
sorts too), not the first view in its YAML.

Build locally (no DB write) and read the output in `viz/chart/<ns>/latest/<short>/`:
view count, dimensions, and one view's config.

## 3. Publish on staging

```bash
git push
STAGING=1 .venv/bin/etlr viz://chart/<ns>/latest/<short> --grapher
```

A new multidim lands unpublished, and `/grapher/<slug>` on staging stays 404 until the site is
baked again (the next push rebuilds staging). Publish it on staging with `PATCH
<admin_api>/multi-dims/<id>` `{"published": true}` (assert the host is a staging one first), and
meanwhile review it at the admin preview URL that `etlr` prints.

To preview the gdoc swaps on staging, edit the staging DB copy of each doc: `GET
<admin_api>/gdocs/<id>` (no `contentSource`, so it reads the DB, not Google), replace the URLs in
the JSON, `PUT` it back. Saving a published doc re-bakes its page on staging. Never edit the
Google Doc for a staging preview: it is shared with production. To try a grapher branch there,
`ssh owid@<staging host>`, check the branch out in `~/owid-grapher`, and `pm2 restart admin
deploy-queue`; the server stays on that branch afterwards.

## 4. Map views

Run `/map-explorer-to-mdim` steps 1–3 (extraction, `mapping_rules.py`, build) with the explorer
slug and the new multidim's catalogPath, against the staging DB (the multidim exists only there).
Write the `*_MAP` dicts from the explorer config's choice names and slugs; the scaffold's
slugified guesses miss any slug that isn't the slugified name. Its review HTML is superseded by
the review page below.

## 5. Review page (the user signs off here)

Every migration gets one review page, served by a local app and updated at each later stage. Build
it as soon as the multidim is on staging:

```bash
S=.claude/skills/migrate-explorer-to-chart/scripts
R=ai/<slug>-review M=ai/<slug>-mdim-mapping P=<prod env file>
.venv/bin/python $S/compare_views.py --mapping-dir $M --staging <branch> --prod-env-file $P --out $R
.venv/bin/python $S/scan_references.py --mapping-dir $M --prod-env-file $P --out $R --grapher-repo <owid-grapher clone>
.venv/bin/python $S/build_review_page.py --review $R --mapping-dir $M --payload-env staging --stage review [--notes $R/notes.json]
```

- `compare_views.py` checks every view pair: indicators and display names (TSV vs multidim
  config), `dataChecksum` and `metadataChecksum` (production vs staging), the full grapher config
  (production `/explorers/<slug>.config.json` vs the staging view's full config, schema defaults
  dropped), and what readers see (header, footer, tabs, active tab, map legend labels and colors)
  on both pages rendered headlessly, on the default tab and the other main tab, with screenshots.
- **The rendered pages are the ground truth.** The explorer's served config (and its
  `explorer_views` configs) are lossy: they drop or invent settings the page doesn't show (color
  schemes, stray bins, `hideAnnotationFieldsInTitle`). A config difference whose rendered check
  passes can be explained in `notes.json` (`{"config:<field>": "why"}`); a rendered, indicator,
  data or metadata difference gets fixed in the multidim, never explained away. Re-run until every
  view is "Match".
  It stops early if the multidim page or a view config isn't served yet: wait for the bake, or
  (config 404 right after a fresh staging server came up) re-run the upload with `--force --only`.
  After the merge, `--staging production` re-checks the multidim on production.
  When the multidim already existed on production (a twin), staging serves each view's config
  from production's file store, so the rendered pages show the old twin: a rendered difference
  whose config is right in the staging DB waits for that post-merge re-check.
- `scan_references.py` is the exhaustive link list: every text/JSON column of the production DB
  (history and analytics tables skipped and listed), plus `git grep` of ETL and owid-grapher. It
  finds what the gdoc link index misses (unpublished drafts, chart notes, indicator metadata,
  site code). Each hit gets its new URL from the redirect rules and an edit link. It repairs old links
  that the live explorer already misreads (a value glued to the next param, a renamed dimension, a
  combination the explorer doesn't have, a dimension the link leaves out),
  so the new URL lands on the view they meant. Copy `references_full.json` to
  `references_baseline.json` before any swap: the page then marks each link swapped or still to do.
- Serve it with the local app, in the background, and give the user the link:

  ```bash
  .venv/bin/python $S/review_app.py   # http://localhost:8771/ lists every ai/*-review folder
  ```

  It embeds the production explorer and the staging multidim as live iframes (an artifact
  can't: it blocks iframes, and an https page can't frame the http staging server), with a switch
  to the screenshots. It rebuilds the page on every request, so re-running a script shows on
  reload. `build_review_page.py` also writes a static `index.html` with screenshots, for sharing
  with someone who can't reach staging. Rebuild with the new `--stage` at every later stage.

The page has: the checks summary, the explorer and multidim side by side with one set of
dimension controls, a matrix of every view, the links to replace, the dimension-by-dimension
mapping, and the redirect JSON. The user reviews it and merges the PR.

## 6. Announce and merge

If the user wants a Slack sign-off, draft it and hand it to the user, who posts it (never post it
yourself). It carries the attribution line from CLAUDE.md, what readers see change, where the
explorer shows up (from the review page), what goes away (flags from step 0), and the review page
link. Put a terse summary, without internal links, in the PR body (the repo is public). Request a
review only once the user says so.

Merging creates the multidim on production **unpublished**: the user publishes it in the admin.
`/grapher/<slug>` then takes a few minutes to go live; poll until it returns 200.

## 7. Cut over on production

In this order, because creating the redirects darkens the explorer at once:

1. **Links.** Rebuild the page with `--stage links`. The user (or the doc's author)
   swaps the links from its "Links to replace" section and republishes each doc; never edit a
   Google Doc yourself. Featured metrics go through `/map-explorer-to-mdim` step 5b with the
   topic owner. Then re-run `scan_references.py`: no reference that breaks on redirect may remain,
   and check the live pages (they re-bake a few minutes after the DB changes).
2. **Site redirects** that end on `/explorers/<slug>` (admin `/redirects`): repoint them to the
   multidim, so they don't chain.
3. **Redirect JSON.** Re-run `/map-explorer-to-mdim` steps 1–3 against production (the payload
   carries each view's config id, which differs from staging), then its `audit_references.py` and
   `preflight.py`. Rebuild the page with `--payload-env production --stage redirects`: only that
   version of the JSON is pasteable. Confirm `/grapher/<slug>` returns 200
   (preflight reads the DB, not the site). After the user pastes it, poll a few views with
   `curl -s -o /dev/null -w '%{http_code} %{redirect_url}'` until they return 302, including one
   with a `country=` param to check it is carried over.

## 8. Retire the explorer and its orphaned data

The user unpublishes the explorer in the admin (unpublish, don't delete: a deleted explorer row
breaks chart blocks that still name it). Then one PR (`etl pr "Retire <slug> explorer" data`)
archives the explorer and any data only it used, following CLAUDE.md's archive rule:

1. Remove the `viz://explorer` DAG entry. It often holds the full snapshot → grapher chain of its
   dataset; move that chain under the `viz://chart` entry so the dataset stays in the DAG. Remove
   the DAG entries of datasets nothing else uses any more. Commit.
2. `etl archive-dag`. It also sweeps in steps others left un-archived; `git checkout` those files
   and commit only this PR's archive entries.
3. Delete the explorer's `.py` and `.config.yml`, and the files of the archived datasets. Commit.
4. Delete orphaned backport mirrors in `snapshots/backport/latest/` of the grapher dataset the
   explorer used before it moved into ETL, if that dataset is archived in the DB and no chart uses
   its variables (`chart_dimensions`). Commit.

Then tick the explorer in owid/etl#6014, pointing the line at `/grapher/<slug>` and the PRs, and
rebuild the review page with `--stage retire`.

## Report

Close with what is still open: unchecked views, flags handled by dropping something, embeds or
featured metrics not yet swapped, the retirement PR, and anything you didn't verify.
