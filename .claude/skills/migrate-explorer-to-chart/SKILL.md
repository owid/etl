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
| View mapping, review HTML, reference audit, featured metrics, preflight, bulk redirects, applying them | `/map-explorer-to-mdim` |
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
none). Pin `grapher_schema: "012"` (quoted) at the top: the build fails without it. Leave the `viz://explorer` step and its DAG entry in place: the explorer stays live
until the redirects exist.

Translation of the explorer's `config:` block:

| Explorer | Multidim |
|---|---|
| `explorerTitle` | `title.title` (+ `title.title_variant` if needed) |
| `explorerSubtitle` | `presentation.explorer_subtitle`, with `presentation.page_type: data-explorer` |
| `selection` | `default_selection` |
| explorer slug | `slug` (keep the explorer slug, unless it collides with a chart slug) |
| `entityType`, `entityTypePlural`, `hasMapTab`, … | the same key in per-view `config` (shared values in `definitions.common_views`) |
| `hideAnnotationFieldsInTitle: true` | `hideAnnotationFieldsInTitle: {entity: true, time: true, changeInPrefix: true}` |
| `isPublished`, `subNavId`, `subNavCurrentId`, `thumbnail`, `googleSheet`, `hideAlertBanner`, `yScaleToggle: false` | drop (not grapher keys; `yScaleToggle: true` is `yAxis.canChangeScaleType`) |
| `pickerColumnSlugs`, `wpBlockId`, `downloadDataLink` | see the flags in step 0 |

In view configs: `type: X` becomes `chartTypes: [X]`, and `relatedQuestionUrl` +
`relatedQuestionText` become `relatedQuestions: [{url, text}]`. Check any other view key against
the grapher schema (`https://files.ourworldindata.org/schemas/grapher-schema.012.json`).

Dimensions: `presentation.type: radio` and `checkbox` become `toggle-buttons` (drop
`choice_slug_true`); `dropdown` stays, or `dropdown-with-search` for long choice lists. Keep the
dimension and choice slugs unchanged — the view mapping in step 4 is then close to the identity.

In the Python step, `paths.create_explorer(...)` becomes `paths.create_chart(...)`, and
`c.save(tolerate_extra_indicators=True)` becomes `c.save()`. Add `topic_tags` from the
explorer's tag (`explorer_tags` in the DB). The build sorts views by choice order, and the
default view is the first view after sorting (or `default_dimensions`, if set). Make it match the
explorer's default, which is the first `graphers` row of `explorers.tsv` in the DB (the explorer
sorts too), not the first view in its YAML.

Build locally (no DB write) and read the output in `viz/chart/<ns>/latest/<short>/`:
view count, dimensions, and one view's config.

## 3. Compare on staging

```bash
git push
STAGING=1 .venv/bin/etlr viz://chart/<ns>/latest/<short> --grapher
```

A new multidim lands unpublished, and `/grapher/<slug>` on staging stays 404 until the site is
baked again (the next push rebuilds staging). Publish it on staging with `PATCH
<admin_api>/multi-dims/<id>` `{"published": true}` (assert the host is a staging one first), and
meanwhile review it at the admin preview URL that `etlr` prints.

Check the default view and a sample of views (every dimension's first and last choice) against
`https://ourworldindata.org/explorers/<slug>`: same indicators, titles, subtitles, selected
entities, chart types, tabs. `/check-chart-preview` takes the screenshots.

## 4. Map views and audit references

Run `/map-explorer-to-mdim` with the explorer slug and the new multidim's catalogPath, using the
staging DB (the multidim exists only there). Follow it through `mapping_rules.py`, the build, the
review HTML and `audit_references.py`. Stop before its preflight and apply steps: those run in
step 6, on production.

## 5. Announce and get sign-off

Draft a Slack post asking for permission and hand it to the user, who posts it (never post it
yourself). It must carry the attribution line
from CLAUDE.md and cover:

- what readers see change: `/explorers/<slug>` → `/grapher/<slug>`, every view redirected to its
  equivalent, controls in the multidim design, still listed on `/explorers`;
- where the explorer shows up, from `inventory.json` and `references.md`: pages that embed it
  (swapped before the cut-over), pages that link to it (fixed by the redirect), featured metrics
  (repointed with the topic owner), site redirects ending on it (repointed to avoid chains),
  pageviews;
- anything that goes away (flags from step 0);
- links: staging preview, review HTML, PR;
- the planned cut-over date.

Put the same summary, without internal links, in the PR body (keep it terse; the repo is public).
Request the review only once the user says so. Merge once the reviewer signs off. Merging publishes the multidim on production.

## 6. Cut over on production

In this order, because creating the redirects darkens the explorer at once:

1. Repoint featured metrics (`/map-explorer-to-mdim` step 5b) and swap embedded explorers in
   gdocs for the multidim.
2. Repoint the site redirects that end on `/explorers/<slug>` (admin `/redirects`) to the
   multidim, so they don't chain.
3. Re-run `/map-explorer-to-mdim` steps 1–3 and 5–7 against production: extraction, build,
   reference audit, preflight, apply the bulk payload, `curl -sI` a few views.

## 7. Retire the explorer

In a follow-up PR, following CLAUDE.md's archive rule: unpublish the explorer in the admin, remove
its `viz://explorer` DAG entry → commit → `etl archive-dag` → commit → delete the step files. Then
tick the explorer in owid/etl#6014.

## Report

Close with what is still open: unchecked views, flags handled by dropping something, embeds or
featured metrics not yet swapped, the retirement PR, and anything you didn't verify.
