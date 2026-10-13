---
name: sketch-mdim
description: >-
  Open the MDim simulator: a page where someone sketches a small multidimensional chart (MDim) with the real
  Grapher before it exists in ETL. They set up dropdowns, link an indicator to each combination, edit the chart
  text, and copy the result as a viz://chart setup. Pre-fills the page from chart links, catalog paths or
  indicator IDs when the request names them. Use when someone wants to sketch, design or prototype an MDim,
  group a few charts or indicators into one chart with a dropdown, or says "sketch an MDim". Local Claude Code
  only, because the page opens in the person's browser and needs Tailscale. Turning the copied setup into the
  ETL step is done with /create-chart.
metadata:
  internal: true
  owner: edomt
---

# Sketch an MDim

Builds the MDim simulator, a single HTML page, and opens it in the person's browser. They shape the MDim there,
seeing the real chart as they go, then copy the setup and paste it back to you. The simulator lives in
`apps/mdim_simulator/` (see its README for how it works).

This skill is for people who don't work in the ETL. Talk to them plainly, and don't ask them to confirm things
they can change in the page themselves.

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

## 5. When the setup is pasted back

**The person is still the same non-technical person.** `/create-chart` is written for engineers. Use it for the
mechanics, not for the conversation:

- **Do all the technical work yourself:** files, DAG entry, running the step, the branch and the pull request.
  Never ask them to run a command, or to understand the DAG, catalog paths, YAML or git.
- **Only ask what they can answer,** in plain words and all at once. For example: "Should the MDim replace the
  two old charts?", or "What web address should it have?". Work out everything else yourself, and if you had to
  guess, say what you guessed.
- **Report progress in terms of what they'll see:** "I've turned your sketch into the chart files and opened a
  pull request. Here's where you can check it on the test site." Don't give them a log of commands.
- **If something technical fails, fix it.** Only bring them in when the choice is theirs, and explain it without
  jargon.

The setup is already in `viz://chart` form: `grapher_schema`, `title`, `topic_tags`, `default_selection`,
`default_dimensions`, shared settings under `definitions.common_views`, `dimensions` and `views`. Turn it into
the step with `/create-chart`:

- **The DAG entry:** its dependencies are the grapher datasets of the linked indicators. The setup uses full
  catalog paths; `/create-chart` prefers the short `table#indicator` form.
- **The comments at the end need reading:**
  - combinations with no indicator yet: ask what to do with them;
  - text changes against each indicator's original: useful for the PR description;
  - changes suggested for an indicator everywhere: these go through `/edit-faust-metadata`, which checks the
    other charts using that indicator first.
- **Two things are settled in the conversation, not in the page:** specific map bins, if they asked for them
  (the page only offers automatic ones), and the MDim's web address (slug), which is set in the admin when
  publishing.
- **To show the setup in the page again,** for example to adjust it, save it as
  `ai/mdim_simulator/<name>.setup.yml` and rebuild with `--setup`. The page reopens it exactly as it was copied.

## What the page can't do yet

Tell them when they run into these:

- One indicator per view: no views with several lines, and no stacked or scatter charts.
- Map bins: only the automatic strategies.
- One set of default countries for all views.
- It can't open an MDim that already exists in ETL.
