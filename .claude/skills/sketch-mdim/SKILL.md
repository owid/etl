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

Builds the MDim simulator, a single HTML page, and opens it in the person's browser. They shape the MDim there,
seeing the real chart as they go, then copy the setup and paste it back to you. You then take it all the way to
a live MDim (step 5). The simulator lives in
`apps/mdim_simulator/` (see its README for how it works).

This skill is for people who don't work in the ETL. Talk to them plainly, and don't ask them to confirm things
they can change in the page themselves. Whenever something is unclear, ask them, as multiple-choice questions
(`AskUserQuestion`) in plain, non-technical English that needs no coding knowledge: no file names, commands,
catalog paths or YAML in the question or its options.

It changes nothing in the repo: the page and its setup go to `ai/mdim_simulator/`, which git ignores, so no
branch is needed.

## 1. Check you can open the page

- **Cloud session** (`CLAUDE_CODE_REMOTE=true`): stop. Explain that the page has to open in their own browser,
  on Tailscale, so this needs Claude Code on their laptop (the desktop app or VS Code).
- **Grapher reachable?** Run `.venv/bin/python apps/mdim_simulator/build.py --check`. If it fails, tell them:
  "The page needs Tailscale to draw charts. Turn it on, then tell me and I'll carry on." Wait for them.

## 2. Pre-fill the page, when the request names indicators

If they gave chart links, catalog paths or indicator IDs, start the page from them. Otherwise skip to step 3
and build an empty page. Indicators described only in words are not looked up: ask for a link, a catalog path
or an ID.

Find each indicator's full catalog path (`grapher/<namespace>/<version>/<dataset>/<table>#<indicator>`):

- **Chart link** (`ourworldindata.org/grapher/<slug>`): fetch `https://ourworldindata.org/grapher/<slug>.config.json`
  and take the `variableId` of its `y` dimension, then the `catalogPath` from
  `https://api.ourworldindata.org/v1/indicators/<id>.metadata.json`. A chart with several `y` indicators, an MDim
  or an explorer has no single indicator: say so, and ask which one they mean.
- **Indicator ID**: the `catalogPath` from the same metadata URL.
- **Catalog path**: use it as it is. The build stops with a clear message if it doesn't exist.

Write the draft to `ai/mdim_simulator/<name>.setup.yml`, in the same format the page copies out. `<name>` is a
short kebab-case name for the MDim, such as `internet-users`. A typical draft has one dropdown, with one option
per indicator:

```yaml
title:
  title: "Internet users"
topic_tags:
  - "Internet"
default_selection:   # from the first chart link, if there is one; leave out otherwise
  - "World"
dimensions:
  - slug: metric
    name: "Metric"
    choices:
      - slug: number_of_people
        name: "Number of people"
      - slug: share_of_population
        name: "Share of population"
views:
  - dimensions:
      metric: number_of_people
    indicators:
      y:
        - catalogPath: "grapher/<namespace>/<version>/<dataset>/<table>#<indicator>"
    config:          # only for indicators that came from a chart link: copy that chart's text
      title: "..."
      subtitle: "..."
      note: "..."
  - dimensions:
      metric: share_of_population
    indicators:
      y:
        - catalogPath: "grapher/<namespace>/<version>/<dataset>/<table>#<indicator>"
```

- **Option names**: short, taken from the indicators' or charts' titles.
- **Dropdown name**: "Metric" unless the options obviously differ in something else, such as "Sex" or "Age".
- **Title**: what the indicators have in common.
- **Topic tags**: from the indicators' `presentation.topicTagsLinks`.
- **From chart links**: copy each chart's title, subtitle and note into its view, so the view starts out looking
  like the chart. Use the first chart's selected countries as `default_selection`.

## 3. Build and open the page

```bash
.venv/bin/python apps/mdim_simulator/build.py ai/mdim_simulator/<name>.html --setup ai/mdim_simulator/<name>.setup.yml
open ai/mdim_simulator/<name>.html        # xdg-open on Linux
```

Leave out `--setup` for an empty page. Each page keeps its own work in the browser:

- **Reopening a sketch:** open the same file again. Don't rebuild it.
- **Rebuilding with a different setup:** the page starts fresh from the new setup and leaves the old work alone.

## 4. Tell them how to use it

Something like this, in plain words:

> The page is open in your browser. The panel at the bottom is where you set things up: the title and topic
> tags, the dropdowns and their options, and, in the table, which indicator goes with each combination (paste
> its catalog path or ID; chart links don't work there). Tick the view that should open first.
>
> On the chart itself, the countries you tick become the default for every view, and the tab you leave open is
> the one the MDim opens on. "✎ Edit FAUST and WYSK" lets you change each view's title, subtitle, footnote,
> "What you should know", number rounding and map colours.
>
> When it looks right, click "Copy MDim setup" and paste it here.

## 5. From the pasted setup to a live MDim

**The person is still the same non-technical person.** `/create-chart` is written for engineers. Use it for the
mechanics, not for the conversation:

- **Do all the technical work yourself:** files, DAG entry, running the step, the branch, the pull request,
  the review and the merge. Never ask them to run a command, or to understand the DAG, catalog paths, YAML or git.
- **Ask only what they can answer,** as plain multiple-choice questions, all at once. Work out everything else
  yourself, and if you had to guess, say what you guessed.
- **Talk only about the MDim and what they'll see,** in every message, including the last one and anything
  left open. Words they know are fine: staging, live, PR, review, merge, publish, admin. Leave out how you got
  there: no branches, worktrees, commits, pushes, file paths, step names, commands or tool names.
  This covers the short notes between your actions too, which they see: write one only when something they'd
  care about is done or ready, and don't narrate your checks, reviews or next commands.
- **If something technical fails, fix it.** Only bring them in when the choice is theirs, and explain it without
  jargon. How you do the work is never their choice: don't explain or ask about which commands you run, where
  they run, or changes to this skill. A known problem with a known fix is part of the work: apply the fix, and
  don't report it.

### 5a. Ask the questions up front, in one go

- **Does the MDim replace existing charts?** If the draft came from chart links, name those charts. If yes, step
  5g handles them.
- **Its web address.** Propose one from the title (`ourworldindata.org/grapher/<slug>`, lowercase words joined
  by hyphens). Before proposing it, check it's free, because the admin refuses an address that's taken. It must
  match none of these:
  - a chart (`chart_configs.slug`);
  - an old chart address (`chart_slug_redirects.slug`);
  - an MDim (`multi_dim_data_pages.slug`);
  - a site redirect (`redirects.source` = `/grapher/<slug>`).

  Check all four in the public Datasette. Then check that `https://ourworldindata.org/grapher/<slug>` returns
  404: anything else means the address is in use, including redirects to other MDims (`multi_dim_redirects`),
  which the public Datasette doesn't list. If the admin still refuses the address later, propose another one.
- **Anything the setup leaves open:** combinations with no indicator yet, and specific map bins if they asked
  for them (the page only offers automatic ones).

### 5b. Build the step

Turn the setup into the step with `/create-chart`. The setup is already in `viz://chart` form: `grapher_schema`,
`title`, `topic_tags`, `default_selection`, `default_dimensions`, shared settings under
`definitions.common_views`, `dimensions` and `views`.

- **Catalog paths and versions:** the setup uses full catalog paths, which include the dataset's version.
  Convert each one to the short `table#indicator` form, as every MDim does. The version then lives only in the
  step's DAG dependency, `data://grapher/<namespace>/<version>/<dataset>`, so a dataset update only has to change
  that line. Keep the full form only where two dependencies have a table of the same name.
- **The web address isn't part of the step:** the config has no slug field. Keep it for step 5f.
- **The comments at the end:** use the text changes against each indicator's original in the PR description.
  Changes suggested for an indicator everywhere go through `/edit-faust-metadata`, which checks the other charts
  using that indicator first.
- **Don't build it locally.** That pulls in the whole upstream chain, often with private datasets the laptop
  can't download. The staging build in step 5c checks the config against the real indicators instead.
- **To show the setup in the page again,** for example if they want to adjust it there, save it as
  `ai/mdim_simulator/<name>.setup.yml` and rebuild with `--setup`. The page reopens it exactly as it was copied.

### 5c. Open the PR and put it on staging

1. `.venv/bin/etl pr "<MDim title>" feature`. This opens a draft PR on a new branch from the latest master.
2. Commit the files by path, then `git push -u origin <branch>`. Check that `git ls-remote origin
   refs/heads/<branch>` matches `git rev-parse HEAD` before going on.
3. Write the PR description. Start with the attribution line from `CLAUDE.md`. Then say what the MDim shows,
   that it was sketched in the MDim simulator, and the text changes from the setup.
4. **Request one Codex review now,** with a bare `@codex review`, so it runs alongside the staging build. Its
   verdict is a review with findings, a "Didn't find any major issues" comment, a 👍 from the Codex bot, or its
   "Codex Review Summary" comment showing `✅ Completed` with no review or inline comments. That summary comment
   appears within seconds showing `🔄 Running`, which is not a verdict. Don't hold step 5d for it.
5. **Let the staging server write the MDim.** Don't run the step from the laptop, even with `STAGING=1`: chart
   steps read their upstream datasets from local disk, and private ones can't be downloaded. After each push,
   wait for `buildkite/etl-automated-staging-environment` to succeed on the head commit
   (`gh api repos/owid/etl/commits/<sha>/status`). That build writes the MDim to staging, and is the check that
   the step builds: if it fails, read its log and fix the step.
6. **Re-upload the views, then check they draw,** after every successful staging build. The preview loads each
   view's config from `http://<container>/grapher/by-uuid/<id>.config.json`, which is served from R2, not the
   database, and the build's own upload often doesn't reach it. So always take the MDim's config from
   `multi_dim_data_pages.config` in the staging database (`query-grapher-db` skill), drop `fullConfigId` from
   every view, and send it back unchanged with
   `AdminAPI(OWIDEnv.from_staging(<branch>)).put_mdim_config("<namespace>/latest/<name>#<name>", config)`. Then
   check that each view's new `fullConfigId` returns 200 at that URL; retry a 502 once, since it can show briefly
   before the real answer. Do this silently, and bring it up only if views still fail after the re-upload.

When Codex's verdict comes in, fix what's valid, push, repeat items 5 and 6, and reply to and resolve each
thread. Don't start a second round. Tell the person about the review only if it changed something they'll see.

### 5d. Let them check it on staging

Give them the preview link. The MDim isn't published on staging, so it's the admin preview address, which needs
Tailscale: `http://staging-site-<branch>/admin/grapher/<namespace>%2Flatest%2F<name>%23<name>/`. Use the
container name from `etl.config.get_container_name(<branch>)`. Then send something like:

> Your MDim is ready to check on staging: <link>. Please try each option in each dropdown, look at the
> map, check the countries it opens with, and read the titles and "What you should know". Tell me what to
> change, or say it looks right.

Send it as soon as every view returns 200, without waiting for Codex. If they ask for changes, make them, push,
repeat items 5 and 6 of step 5c, and send the link again. **Don't merge until they say it looks right.**

### 5e. Merge, once they say it looks right

Check that Codex's verdict is in and its threads are resolved, and that the PR's tests have passed
(`gh pr checks <number>`). Fix a failure that comes from the MDim. Retry one that doesn't, such as a test that
timed out on a network call. Then run `gh pr ready <number>` and `gh pr merge <number> --squash`. Tell them it's
merged, and that it takes about ten minutes to reach the live site's admin.

### 5f. Publish it

Watch the merge commit: `gh api repos/owid/etl/commits/<merge sha>/status`, until
`buildkite/etl-deploy-master` succeeds. That run writes the MDim to production. If it fails, find out why, fix it
if it's this MDim, and tell them in plain words.

> **Temporary step:** publishing an MDim is still done by hand in the admin. This is expected to go away.

Once the run has passed, send something like:

> It's ready to go live. Open https://admin.owid.io/admin/multi-dims, find "<MDim title>", type `<slug>` in
> its Slug field, and switch it to Published. It will then be at https://ourworldindata.org/grapher/<slug>.

### 5g. Replace the old charts, if they said yes

Do this only once the MDim is published: the admin won't redirect to an unpublished MDim.

1. **Switch the embeds first.** Run `/find-chart-references` for each old chart. Articles that embed one break
   once the chart is deleted. List them for the person, each with the MDim view link to use instead. Text links
   need nothing, because the redirect covers them.
2. **For each old chart, check whether it has old addresses** (rows in `chart_slug_redirects` pointing at it).
   - **None:** send them the admin steps. On the MDim's page in the admin
     (`https://admin.owid.io/admin/multi-dims`, then the MDim), add a redirect: Source `/grapher/<old slug>`,
     ticking "Redirect to specific view" for the matching view. Then delete the old chart, as the note on that
     form says. The redirect only takes effect once the chart is gone.
   - **Some:** the admin refuses these. Write a short message they can send to a Grapher developer, asking for
     the redirect with `createMultiDimRedirectsFromCsv`. `/map-charts-to-mdim` writes the CSV it needs.

## What the page can't do yet

Tell them when they run into these:

- One indicator per view: no views with several lines, and no stacked or scatter charts.
- Map bins: only the automatic strategies.
- One set of default countries for all views.
- It can't open an MDim that already exists in ETL.
