---
name: data-update-announcement
description: >-
  Draft the short public-facing "Data update" post published on ourworldindata.org/latest
  whenever a data scientist refreshes a dataset — two alternative drafts in OWID house style,
  then a styled Google Doc in the team's /Data updates folder. Use whenever someone asks for
  a "data update", a data update post or announcement, an OWID announcement, or pastes a
  #data-updates-comms Slack message and asks for copy. Use it for partial requests too — just
  a title, just the CTA — and even when the ask is casual, e.g. "can you write up the SIPRI
  update?" or "we refreshed the WASH charts, need a post." Also invoked by /update-dataset
  step 9b. This is NOT the internal Slack form itself — filling that in is /data-updates-comms.
metadata:
  internal: true
---

# OWID data update announcements

Short announcement posts published when a data scientist refreshes a dataset, written in that person's voice.

> **Ported skill — keep in sync.** This is a port of Charlie Giattino's claude.ai skill `owid-data-update-announcement` (exported 2026-08-17), adapted so `/update-dataset` can call it mid-pipeline. His is the version the comms team maintains, and `references/examples.md` here is his file verbatim. When you learn something general about how these posts read, tell Charlie so it lands in the source skill too — otherwise the two drift and the next re-export silently reverts it. Re-syncing means someone re-exporting the `.skill` bundle from claude.ai; it is not fetchable from a terminal session.
>
> Sibling skill: [`/data-updates-comms`](../data-updates-comms/SKILL.md) fills the internal Slack form that *feeds* this post. That one is written for Charlie; this one is written for readers.

**`references/examples.md` is the style guide.** Read at least three before drafting, picked to match the update in hand — solo refresh, multi-author, new chart, static viz, or politically contested. Everything about how these posts read is in there: how they open, how long they run, how they introduce a source, how they handle caveats, how they close. This file deliberately doesn't restate that, because rules abstracted from the examples get applied where they don't fit. Match the examples instead.

The same goes for subject matter. OWID has usually written about the topic already — a topic page, an article, a Data Insight — and that writing is the golden example for how to frame *this subject*. Step 3 covers it.

[`references/gdoc-format.md`](references/gdoc-format.md) holds the Google Doc mechanics: the CMS format, the styling, and the traps.

What follows is only what neither set of examples can tell you.

## Two entry modes

**Mode A — called by `/update-dataset` (step 9b).** The runner *is* the author, mid-pipeline. The facts are already gathered:

- `workbench/<short_name>/update-context.yml` — `dataset.title`, `dataset.producer`, `source.url_main`, `source.citation_full`, `coverage.*`, `charts.published_count`, `charts.selected_views`, and the `editorial_context.*` snippet lists.
- `workbench/<short_name>/slack-announcement.md` — the Slack draft from step 9. Its editorial framing is the closest cousin to this post; read it before drafting.
- Author is the git user (`git config user.name`), resolved to a canonical OWID name through `etl.owners.resolve_owner`. If that returns `None`, **ask who the post is by** rather than writing the raw git name — `authors:` is a public byline, and a checkout's commit identity can be an automation account or a spelling the site doesn't know. Nothing in the Mode A context records collaborators, so name the byline you resolved when you hand over the drafts and ask whether anyone else should be credited — updates are often joint work, and `authors:` takes a comma-separated list.

Skip step 1. Don't gate on the chart image (step 2) — a mid-pipeline dataset update shouldn't stall on a screenshot. Anything the post needs that isn't in the YAML, gather it (snapshot `.dvc`, garden `.meta.yml`, `url_main`) and **persist it back** so the next consumer doesn't redo the work.

**Mode B — standalone.** Someone asks for a post out of the blue, or pastes a Slack message. Run the full workflow below.

## Workflow

**1. Find the announcement (Mode B only).** Search Slack `#data-updates-comms` (channel ID `C0A8P7H0HC2`) with `include_bots: True`, sorted by `timestamp`. A workflow bot posts these, so without `include_bots` the channel looks empty.

Run `slack_read_thread` on the parent message even when search shows no replies — corrections and co-author additions live there.

The author is the **"Message from:"** field inside the bot message, not the Slack account that posted it. A thread commenter is not automatically a co-author; flag it as a question.

Link the message when reporting back, using the `Permalink` field from the search result: "Found [Tuna's 25 June message](permalink) on…".

Slack permalink timestamps convert as `p1234567890123456` → `1234567890.123456`.

**2. Get the chart.** The pick belongs to the data scientist, who has already attached candidates to the Slack message — don't suggest one. Post this block exactly as written:

````markdown
## Now let's choose a chart

1. Export a square version of the chart with no terminology definitions at the bottom
2. Rename it to `YYYY-MM-data-update-[short-topic].png` — e.g. `2026-08-data-update-whaling.png`.
3. Upload it to the [admin images page](https://admin.owid.io/admin/images), then use the button there to generate alt text.
4. Paste your chosen chart here in the chat so that I can use it to draft versions of the announcement
````

*Mode B*: **stop here** and don't draft the post until the image arrives. The gate is on the post, not on the skill — a partial ask the chart has no bearing on (the CTA `url:` and `text:`, a Doc title, a check on an existing draft) gets answered straight away; the description invites those. *Mode A*: post the block, fill the `filename:` slot from the convention, and keep going — the user can swap the chart later.

**3. Read the golden examples. There are two kinds.**

*How these posts are written* — at least three from `references/examples.md`, matched to this update.

*How OWID frames this topic* — whatever we've already published on it. This is where the tone for a specific subject comes from, and it's often quite different from what a general-purpose take would produce. Two to four pages is plenty; this doesn't need to be exhaustive:

- The **topic page introduction**. Grapher charts often name it in the footer — the child mortality chart reads `OurWorldinData.org/child-mortality`.
- Any **major article** on the topic, e.g. [Child mortality: the greatest problem, in brief](https://ourworldindata.org/child-mortality-big-problem-in-brief).
- **Data Insights**, especially recent ones. They're the closest thing to this format in length and register.

Find them by searching rather than guessing at URLs.

Read for framing and register, not for facts to lift: which aspect OWID leads with, how it words the concept, how much moral weight it carries, what it treats as the point. If the topic page frames the subject differently from the draft you had in mind, follow the topic page.

**4. Draft two versions** in the chat. Make them worth choosing between: a different angle or title pattern, not the same draft reworded. The point is to hand the author a real choice rather than anchor them on your first angle.

**5. Get sign-off, then save.** Iterate until the author picks one (or splices the two). In Mode A, save the chosen draft to `workbench/<short_name>/data-update.md`.

**6. Create the Google Doc** — only now, with the approved content already inside it, and share the link. See [`references/gdoc-format.md`](references/gdoc-format.md) for the title convention, the styled-HTML upload, the styling source, and the verification step. The ordering matters: the Drive MCP has no edit-content or delete tool, so a Doc created too early is an orphan the user has to clean up by hand.

**7. Post the admin reminder,** exactly as written, once, immediately after the Doc link:

````markdown
## Now that the GDoc is created, don't forget to:

1. Add the GDoc to the admin by clicking the blue "+ Add document" button in the top right [here](https://admin.owid.io/admin/gdocs) and following the instructions
2. Add relevant topic tags
````

## Template

Which fields the post carries, and in what order. This is the **drafting** shape — spaced out for reading in the chat, the way `references/examples.md` displays the published posts:

```
title:

excerpt:

type: announcement

authors: [Mode A: the resolved git user, plus anyone else credited; Mode B: the Slack "Message from:" line — comma-separated]

kicker: data-update

[+body]

[body paragraphs, blank line between them]

{.cta}

url: [see "The CTA link" below]

text: [see the examples]

{}

{.image}

filename: YYYY-MM-data-update-[short-topic].png

{}

[]
```

**The Google Doc is not spaced like this.** Drop every blank line except the ones between body paragraphs, between the body and `{.cta}`, and between `{.cta}` and `{.image}`; frontmatter fields and the insides of the `{.cta}` / `{.image}` blocks go on consecutive lines. [`references/gdoc-format.md`](references/gdoc-format.md) is the authority on the Doc layout — build the upload from it, not from the block above. A blank line that survives into the upload becomes an empty Google Docs paragraph, and the Drive MCP can't edit it back out.

`type` is always `announcement`; `kicker` is always `data-update`, lowercase and hyphenated. Content inside `:skip` / `:endskip` doesn't publish — use it for internal notes.

**Two mechanics in the static-viz example (#7, population growth) are not the house pattern — don't copy them.** It carries a `featured-image:` line between `excerpt:` and `type:`; we don't use that field, and the five frontmatter fields above are the complete set. Its image is `world_population_growth.png`; ours is always `YYYY-MM-data-update-<slug>.png`, static-viz refreshes included. Everything *else* about that post is a good model — the "We've refreshed" title, the plural voice, the paragraph on why the pipeline work matters — which is why it's in the examples file.

The comms person running this workflow (Charlie Giattino) never appears in `authors`.

## The CTA link

`text:` — see the examples. Between the eight of them they cover the range: dataset-wide, single chart, date depth, brand-new chart, topic page.

`url:` — **if the data scientist provided one, use theirs.** Don't second-guess it and don't invent a replacement. The table below is for when they didn't (which is the normal case in Mode A):

| Situation | URL |
|---|---|
| One chart is the point | `https://ourworldindata.org/grapher/<slug>` |
| Several charts (the default) | `https://ourworldindata.org/search?datasetProducts=<dataset title>` |
| The topic has an explorer | `https://ourworldindata.org/explorers/<name>` |
| A curated topic page fits better | the topic URL, e.g. `https://ourworldindata.org/population-growth` |

Never `/collection/custom?charts=…`.

The `datasetProducts` value is the **dataset title**, not the producer. Resolve it in this order: (a) `dataset.title` in the garden `.meta.yml` when it's set there as an override, otherwise (b) `meta.origin.title` in the snapshot `.dvc`. It often carries a parenthetical acronym — `World Bank Poverty and Inequality Platform (PIP)`, `Luxembourg Income Study (LIS)`. Spaces encode as `+`, parentheses as `%28` / `%29`, and several datasets join with `~` (see the democracy example).

## What the examples can't tell you, either kind

**Current conventions.** The examples span a convention change, so don't infer from majority vote:
- Kicker is `data-update`. Older posts show `Data update`; the file has been normalized, but don't be surprised by the old form elsewhere.
- Stat-as-title has been de-emphasized since mid-2026. Question titles are the default.
- Chart counts state public charts only. An update touching 41 charts of which 28 are public is "28 of our charts." Approximate counts are fine: if the data scientist gave a rough figure, "about 60 of our charts" works, and there's no need to chase an exact one. In Mode A, `charts.published_count` in `update-context.yml` is already filtered to published charts.

**Length.** Bodies in the examples run 117–161 words across four to seven paragraphs. Drafts should land in that range. Shorter is usually better on heavy topics.

**Voice.** Solo author refreshing data → first person singular. Multiple authors, a brand-new chart, or a static viz refresh → first person plural.

**Things the examples can't show you, because absences aren't visible:**
- Don't open with a statistic the chart doesn't support. A number in the first line reads as the headline finding and sends the reader into the image to find it.
- Don't omit the headline finding because the chart already shows it. Stating it is the copy's job.
- Don't invent URLs. Link only what was provided or confirmed.
- Don't paste drafts into the Doc before the author has picked one.

## Numbers must survive the click-through

Checking figures against the underlying data is a separate skill. Draft from what the data scientist provided and don't present numbers as verified.

That said, every figure and ranking in the body has to match what the **linked OWID charts** show, because that's where the reader lands. Producers routinely headline a different basis than our charts carry — current vs constant prices, grant equivalents vs net flows, gross vs net. A producer-basis claim our own charts contradict invites every careful reader to catch the mismatch.

So before echoing a producer's number or ranking, work out which measure × basis combination it holds on, and check that it's the one the CTA links to. If it holds on only one, either attribute it to the producer explicitly, round it until both bases agree ("around $170 billion", "roughly a quarter of a percent"), or — simplest — write about our charts only.

(ODA 2026-07: "Germany overtook the US as the largest donor" held only on grant equivalents at current prices, by 0.5%. Every OWID chart showed the US ahead.)

## Editorial sensitivity

Some datasets are politically contested: conflict and war, democracy indices, human rights, migration, military spending, contested territories. What makes them contested is that naming a country in a sentence implicates it, and reasonable people dispute the framing.

Grim is not contested. Child mortality, famine deaths, and disease burden are heavy subjects, but nobody disputes the numbers and no country is being accused. Those get normal treatment.

On genuinely contested topics:
- Keep the prose at the level of the dataset, the source, and long-run global patterns. Country-level figures, named events, and superlatives about particular conflicts or governments belong in the chart, where they arrive with sourcing and bounds attached.
- Treat the Slack "anything interesting to note" field as input, not copy. On these topics it fills with precise tolls, named atrocities, and attributions of cause. Use it to understand the update; don't lift it.
- Note that experts can reasonably disagree, present multiple sources without ranking them, don't adjudicate.
- Say in the chat that the post needs review with Esteban and Bastian before publishing.

## Artifacts and handoff (Mode A)

- The chosen draft lives at `workbench/<short_name>/data-update.md`.
- **The post never goes in the PR** — not embedded, not linked. Comms drafts are internal and stay out of the public data-update PR.
- Close with markdown links to both the Doc and the local draft, and say plainly that it's a first draft:

  ```markdown
  Data update post created at [<Doc title>](<doc viewUrl>) in /Data updates — this is a first
  draft for you and the team to review and edit. Please refine the copy as needed, add the chart
  screenshot, add the doc at https://admin.owid.io/admin/gdocs to bring it into the CMS, then
  preview, publish, and share.
  Draft source: [workbench/<short_name>/data-update.md](workbench/<short_name>/data-update.md)
  ```

  Render paths as markdown links, not inline code — the chat UI only makes them clickable that way.

## Working with the user

- Feedback on their draft is feedback, not a rewrite. Don't redraft unless asked.
- Tier it: error that must be fixed / judgment call / optional polish. Verdict first.
- Flag decision points instead of writing exhaustive commentary.
- Say what you noticed and chose to leave out. Naming a detail you saw and decided against is useful, not clutter.
- State reminders once.
- If they share the published version, compare it against the drafts and surface what changed.
