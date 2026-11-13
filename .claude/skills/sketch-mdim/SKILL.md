---
name: sketch-mdim
description: >-
  Open the MDim simulator: a page where someone sketches a small multidimensional chart (MDim) with the real
  Grapher before it exists in ETL. They set up dropdowns, link an indicator to each combination, edit the chart
  text, and copy the result as a viz://chart setup. Pre-fills the page from chart links, catalog paths or
  indicator IDs when the request names them. Use when someone wants to sketch, design or prototype an MDim,
  group a few charts or indicators into one chart with a dropdown, or says "sketch an MDim". When the setup is
  pasted back, it takes the person through to a live MDim: the step (via /create-chart), the PR, a check on
  staging, the merge, publishing, and redirecting old charts. Local Claude Code only, because the page opens in
  the person's browser and needs Tailscale.
metadata:
  internal: true
  owner: edomt
---

# Sketch an MDim

Builds the MDim simulator (`apps/mdim_simulator/`, see its README), a single HTML page that opens in the person's browser. They shape the MDim there with the real Grapher, copy the setup and paste it back; you then take it to a live MDim (step 5). Until then nothing changes in the repo: the page and its setup go to `ai/mdim_simulator/`, which git ignores, so no branch is needed yet.

## Talking to the person

They don't work in the ETL, and that holds from the first message to the last, also once `/create-chart` (written for engineers) takes over the mechanics.

- **Ask only what they can answer and can't change in the page themselves,** as multiple-choice questions (`AskUserQuestion`) in plain English, all at once: no file names, commands, catalog paths or YAML in the question or its options. Work out the rest yourself, and say what you guessed.
- **Talk only about the MDim and what they'll see,** including anything left open. Staging, live, PR, review, merge, publish and admin are words they know; branches, worktrees, commits, file paths, step names, commands and tool names are not. They see your notes between actions too: write one only when something they'd care about is done or ready, and don't narrate checks or next commands.
- **Do all the technical work yourself, and fix what fails.** Bring them in only when the choice is theirs; how the work is done (which commands, where they run, changes to this skill) never is. A known problem with a known fix is part of the work: apply it and don't report it.

## 1. Check the page can open

- **Cloud session** (`CLAUDE_CODE_REMOTE=true`): stop. The page has to open in their own browser, on Tailscale, so this needs Claude Code on their laptop (the desktop app or VS Code).
- **`.venv/bin/python apps/mdim_simulator/build.py --check` can't reach Grapher:** the page can't draw charts. Ask them to turn on Tailscale and tell you, then carry on.

## 2. Pre-fill the page, when the request names indicators

Start from any chart links, catalog paths or indicator IDs they gave; otherwise build an empty page. Indicators described only in words are not looked up: ask for a chart link. The page only knows indicators that are already live.

**Look for an MDim that already uses these indicators** before building the page: of the `viz://chart` steps in `dag/` that depend on each one's grapher dataset, check which have views on it; a step with a `.py` builds its views from table columns, so searching for `<table>#<indicator>` alone misses it. An MDim that was merged but never published shows up nowhere else, since the public Datasette and the live site only know published ones. If there is one, tell the person what it contains and who made it, and ask whether to build on it (change that step in a new PR, from step 5c on, since the page can't open it) or to make a separate MDim.

Write the draft to `ai/mdim_simulator/<name>.setup.yml` (`<name>` a short kebab-case name) in the format the page copies out: the multidim config of `/create-chart`, but with full catalog paths (`grapher/<namespace>/<version>/<dataset>/<table>#<indicator>`) and a `slug:`. `state_from_setup` in `build.py` shows what the page reads, and the build stops with a clear message on anything it can't use.

- **From a chart link:** the indicator is the `variableId` of the `y` dimension in `https://ourworldindata.org/grapher/<slug>.config.json`, and its `catalogPath` is in `https://api.ourworldindata.org/v1/indicators/<id>.metadata.json`. A chart with several `y` indicators has no single indicator, and neither has an MDim (its `.config.json` is a 404) or an explorer: ask which indicator they mean. Copy each chart's title, subtitle and note into its view and the first chart's selected countries into `default_selection`, so the views start out looking like the charts.
- **Without a chart link,** set `default_selection` to countries or regions every indicator has data for (`dimensions.entities` in the metadata JSON). Left out, the page ticks World and the six continents, which many indicators don't have.
- **Topic tags** come from the indicators' `presentation.topicTagsLinks`.

**Agree the web address before building the page.** Propose one from the title, checked free: absent from `chart_configs.slug`, `chart_slug_redirects.slug`, `multi_dim_data_pages.slug` and `redirects.source` (as `/grapher/<slug>`) in the public Datasette, and `https://ourworldindata.org/grapher/<slug>` returning 404, which also catches redirects to other MDims (`multi_dim_redirects`, not in the public Datasette). It goes in the draft as `slug:` and pre-fills the page's "Web address" field, where they can still change it.

## 3. Build and open the page

```bash
.venv/bin/python apps/mdim_simulator/build.py ai/mdim_simulator/<name>.html --setup ai/mdim_simulator/<name>.setup.yml
open ai/mdim_simulator/<name>.html        # xdg-open on Linux
```

Leave out `--setup` for an empty page. The page keeps its work in the browser, tied to the setup it was built from: to reopen a sketch, open the same file rather than rebuilding; a rebuild with a different setup starts fresh.

## 4. Tell them how to use it

Something like this, in plain words:

> The page is open in your browser. The panel at the bottom is where you set things up: the title and topic
> tags, the dropdowns and their options, and, in the table, which indicator goes with each combination (paste
> its catalog path or ID; chart links don't work there, but you can give them to me). Tick the view that should
> open first.
>
> On the chart itself, the countries you tick become the default for every view, and the tab you leave open is
> the one the MDim opens on. "✎ Edit FAUST and WYSK" lets you change each view's title, subtitle, footnote,
> "What you should know", number rounding and map colours.
>
> When it looks right, click "Copy MDim setup" and paste it here.

## 5. From the pasted setup to a live MDim

If the setup's MDim already has an open PR from an earlier session, carry on in that PR, on its branch, instead of opening another; its own web address then doesn't count as taken. The committed config is then the MDim: apply what they ask for to it from step 5d, and ask before dropping anything a pasted setup merely lacks.

### 5a. Check the setup, then ask everything in one go

First compare `default_selection` with each indicator's entities (`dimensions.entities` in its metadata JSON). The page lets them tick any country or region, but a chart drops the ones its indicator has no data for, without a message, and coverage differs between indicators of one dataset: some World Bank indicators have "Africa", others only "Sub-Saharan Africa (WB)". Then ask, together:

- **Does the MDim replace existing charts?** Name them if the draft came from chart links. If yes, step 5g handles them.
- **Its web address,** if the setup has no `slug:`, as in step 2. If it has one, check it's still free, since they may have changed it, and ask only if it isn't.
- **Which countries to show instead,** for any view that would lose some.
- **Whether to keep the old charts' map colours,** for views that came from a chart whose map has its own bins or flipped colours: the page can't show those, so the sketch's map looks different from the chart's. If yes, copy the chart's `map.colorScale` as it is: a last value in `customNumericValues` lower than the one before it is how Grapher leaves the last bin open-ended.
- **Anything the setup leaves open:** combinations with no indicator yet, and specific map bins if they asked for them (the page offers only automatic ones).

### 5b. Build the step

Turn the setup into the step with `/create-chart`, with these differences:

- **Keep the step a single `.config.yml`,** named in snake_case (`<short_name>`). The fast route for changes in 5d only works without a `.py`.
- **Shorten the full catalog paths** to `table#indicator`, as `/create-chart` says, so the version lives only in the DAG dependency.
- **Keep `slug:`.** ETL gives the MDim that address when it first writes it, and the step fails if the address is taken, so the staging build in 5c catches it: propose another address and push again.
- **The comments the page adds at the end of the setup:** the text changes against each indicator's original go in the PR description. Changes suggested for an indicator everywhere go through `/edit-faust-metadata`, which checks the other charts on it first.
- **Don't run the step yet,** on the laptop or against staging: a new staging server has to load the indicators before an MDim can point at them, so the staging build writes it the first time (5c).
- **To show the setup in the page again,** save it as the setup file and rebuild with `--setup`: it reopens exactly as copied.

### 5c. Open the PR and put it on staging

1. Open the PR with `etl pr "<MDim title>" feature`, then commit and push. The description says what the MDim shows, that it was sketched in the MDim simulator, and the text changes.
2. **Request one Codex review now** (a bare `@codex review`), so it runs alongside the staging build, and don't hold step 5d for it. Read its verdict as `babysit-pr` describes; the summary comment's "Running" is not one. When it comes in, fix what's valid, push, repeat items 3 and 4, and reply to and resolve each thread. No second round. Tell the person only if it changed something they'll see.
3. **Let the staging build write the MDim.** Each push starts `buildkite/etl-automated-staging-environment`, which writes the MDim in its "Build ETL catalog" step, well before the build reports a result. So poll the staging database (`query-grapher-db`) for the row in `multi_dim_data_pages` with `catalogPath` `<namespace>/latest/<short_name>#<short_name>`, or after a later push for the change in its `config`, instead of waiting for the build, and don't tell the person how long it will take. The build's status on the head commit (`gh api repos/owid/etl/commits/<sha>/status`) still says whether the step builds: if it fails, stop polling, read its log and fix the step.
4. **Check every view draws.** The preview loads each view's config from `http://<container>/grapher/by-uuid/<id>.config.json`, with `<container>` from `etl.config.get_container_name(<branch>)` and `<id>` the view's `fullConfigId` in `multi_dim_data_pages.config`. It is served from R2, which the build's own upload doesn't always reach. Every view must return 200; retry a 502 once, since it can show briefly. If one fails, take the MDim's config from the staging database, drop `fullConfigId` from every view, and send it back otherwise unchanged with `AdminAPI(OWIDEnv.from_staging(<branch>)).put_mdim_config("<namespace>/latest/<short_name>#<short_name>", config)`, then check the new IDs. Check again once the build has finished, since a later step can rewrite the MDim. Mention it only if views still fail after that.

### 5d. Let them check it on staging

As soon as every view returns 200, without waiting for Codex, send the admin preview link (the MDim isn't published on staging; the link needs Tailscale): `http://<container>/admin/grapher/<namespace>%2Flatest%2F<short_name>%23<short_name>/`. Ask them to try each option in each dropdown, look at the map, check the countries it opens with, read the titles and "What you should know", and tell you what to change. Say that once they tell you it looks right, you'll merge it.

**For changes, run the watcher in the background.** It writes each saved edit to staging in a few seconds, with no upstream data on the laptop (see `/create-chart`), where a push takes about half a minute:

```bash
STAGING=<branch> .venv/bin/etlr viz://chart/<namespace>/latest/<short_name> --grapher --only --watch
```

A change to the default countries gets the same check against the indicators' entities as in 5a. For each change, edit the config, wait for it in `multi_dim_data_pages.config`, check the views as in 5c item 4, and **show them right away, without being asked**: they can't tell from the page they have open when it has landed. The watcher can miss a save made while it is still writing the previous one: if a change doesn't arrive, run the same command once with `--force` and without `--watch`. If the session has Claude Desktop's built-in browser (`mcp__Claude_Browser__*`), open or reload the preview there; otherwise send the link again and tell them to reload. Commit and push when they have no more changes, or before leaving the work for later: every push rewrites the MDim from the committed file, so an uncommitted change is lost at the next one. Stop the watcher before merging. **Don't merge until they say it looks right.**

### 5e. Merge

Once they've said it looks right, Codex's threads are resolved and the PR's checks pass (fix failures from the MDim, retry unrelated flaky ones), mark the PR ready and squash-merge it. Tell them it takes about ten minutes to reach the live site's admin.

### 5f. Publish it

Wait for `buildkite/etl-deploy-master` to succeed on the merge commit (`gh api repos/owid/etl/commits/<merge sha>/status`): that run writes the MDim to production. If it fails, find out why, fix it if it's this MDim, and tell them in plain words.

> **Temporary step:** publishing an MDim is still done by hand in the admin. This is expected to go away.

Then send something like:

> It's ready to go live. Open https://admin.owid.io/admin/multi-dims, find "<MDim title>", and switch it to
> Published. It will then be at https://ourworldindata.org/grapher/<slug>.

If they say the Slug field is empty there, have them type `<slug>` in it, and find out from the run's log why it wasn't set.

### 5g. Replace the old charts, if they said yes

Only once the MDim is published (its address then returns 200): the admin won't redirect to an unpublished MDim.

1. **Find what still points at each old chart** with `/find-chart-references`. The admin refuses to delete a chart while a published article embeds it or links to it, or an explorer uses it. List those for the person, each with the MDim view link to put in its place.
2. **Send them the admin steps.** On the MDim's page in the admin (`https://admin.owid.io/admin/multi-dims`, then the MDim), with the matching view selected, add a redirect: Source `/grapher/<old slug>`, ticking "Redirect to specific view". If the old chart has old addresses of its own, the admin offers to redirect them too: say yes. Then delete the old chart, since the redirect only takes effect once it's gone.
3. **If the references can't all be changed,** write a short message they can send to a Grapher developer, asking for the redirect with `createMultiDimRedirectsFromCsv`, which unpublishes the chart instead of deleting it. `/map-charts-to-mdim` writes the CSV it needs.

## What the page can't do yet

Tell them when they run into these: one indicator per view (no views with several lines, no stacked or scatter charts); only automatic map bins; one set of default countries for all views; it can't open an MDim that already exists in ETL.
