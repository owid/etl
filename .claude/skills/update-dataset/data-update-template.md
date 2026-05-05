# Data Update Post Template (for OWID /latest)

Use this template to draft the short reader-facing post that gets published on [https://ourworldindata.org/latest](https://ourworldindata.org/latest) when a dataset refresh ships. **The published format is a Google Doc** that gets ingested by OWID's CMS — the doc has structured frontmatter fields (`title`, `excerpt`, `type`, `authors`, `kicker`), a `\[+body\]` marker, body prose, and trailing `{.cta}` and `{.image}` blocks. The skill should produce output in **exactly that format** so the runner can paste it into a new Google Doc in the team's `/Data updates` Drive folder.

This is **separate from the Slack announcement** — that one is a 10-field form for the internal #data-updates-comms channel; this one is a mini-blog-post for OWID readers. They share editorial content but live in different formats.

Hand the draft to the user for review and publication. The skill does not presume where the post is published — the user creates the Google Doc and pastes the draft into it.

---

## Template (paste-ready for a new Google Doc)

```
title: [Punchy title — a finding/claim, a question, or an action/invitation. Examples: "Nearly one in ten people worldwide still live in extreme poverty", "How much do governments spend, and what do they spend it on?", "Track confirmed human cases of H5N1 'bird flu' since 1997". NOT just the dataset name.]
excerpt: [One short sentence summary that's distinct from the title. Common patterns: "Explore updated data from <producer>." / "Explore updated data on <topic> from <producer>." / "Track <topic descriptor>." / "We've updated <N> charts with the latest data from <producer>." / "We updated nearly N of our charts with the latest data."]
type: announcement
authors: [Author name(s). Comma-separated for co-authors, e.g. "Hannah Ritchie, Edouard Mathieu".]
kicker: Data update

\[+body\]

[Body — 100–200 words of flowing prose, first-person, conversational. Recipe (mirroring the published examples — ATUS, PIP, NVIDIA, OECD Government at a Glance, UNU-WIDER, robots, ozone, mobile money, fertilizers, democracy, WASH):

1. Hook — a question, a one-sentence framing of why this dataset matters, or a finding that names the chart.
2. Source/methodology framing — what the dataset is, how it works, why it's the right tool for the question.
3. Specific finding(s) with concrete numbers — at least one quantitative claim. (E.g. "In 1990, 2.3 billion people lived in extreme poverty. Since then the number has fallen by nearly two-thirds, to 826 million.")
4. Optional caveat ("Keep in mind that…", "It's important to note that…") if there's a real interpretation pitfall.
5. Optional cross-reference to a related OWID article ("Our colleague X wrote an article about Y", "We wrote a full article explaining…") if there's a natural one.
6. Closing source attribution + first-person update sentence: "This data comes from <producer>. I recently updated [our charts | this chart] with the latest [release | quarterly release]." Add a forward-looking note for recurring updates ("and will continue to do so each quarter").

Inline markdown links throughout: [link text](URL). Use them for the producer, related OWID articles, methodology pages.
Use *italics* with single asterisks for emphasis, sparingly.
Separate paragraphs with a blank line.]

{.cta}
url: [The exit link URL. One of:
- Single chart: https://ourworldindata.org/grapher/<slug>
- Multiple charts (default): https://ourworldindata.org/search?datasetProducts=<URL-encoded dataset title>
- Explorer for the topic exists: https://ourworldindata.org/explorers/<name>
- Curated topic page exists: https://ourworldindata.org/<topic>]
text: [Descriptive link text. One of:
- "Explore the updated data in our interactive charts" (default for dataset-wide)
- "Explore the updated data in our interactive chart" (single chart)
- "Explore all of the updated data in our interactive charts" (broad coverage)
- "Explore the interactive version of this chart" (single chart)
- "Explore this data going back to YYYY in our interactive chart" (single chart with date depth)
- "Explore the data in our new interactive chart" (new chart/MDIM)]
{}

{.image}
filename: [YYYY-MM-data-update-<slug>.png — a chart screenshot the user adds to the Doc separately. The slug is a short, lowercase-hyphenated topic tag, e.g. world-bank-pip, nvidia-revenue, h5n1-flu, govt-revenue, ozone-hole, robots-per-worker.]
{}

\[\]
```

The `\[+body\]` and `\[\]` markers use backslash-escaped brackets — that's how they appear in the published Google Docs (the escapes prevent Google Docs' auto-link rendering from collapsing the brackets). Keep the escapes when pasting into the Doc; the OWID CMS strips them on ingest.

**Spacing rules** the team prefers (different from the raw Google Docs export, which inserts paragraph breaks everywhere):

- **Frontmatter section** (`title:` through `kicker:`) — one field per line, **no blank lines** between fields.
- **Body** — keep blank lines between paragraphs.
- **`{.cta}` and `{.image}` blocks** — opening tag, fields, closing tag all on consecutive lines with **no blank lines inside**. Keep one blank line between the body and `{.cta}`, and between `{.cta}` and `{.image}`.

---

## Field-by-field guidance

**`title`** — A punchy claim, a question, or an action/invitation. The title is the part that pulls readers into the feed; "Luxembourg Income Study" is descriptive but unmemorable. Three observed patterns:

- **A finding/claim** (PIP, NVIDIA, mobile money, fertilizers): "Nearly one in ten people worldwide still live in extreme poverty", "NVIDIA's data center & AI revenue has grown nearly 15-fold since early 2023", "There are now nearly 800 million active mobile money accounts in the world", "Billions of people depend on synthetic fertilizers. Track how they're produced, traded, and used."
- **A question** the dataset helps answer (OECD Gov, US data centers, UNU-WIDER, democracy, robots): "How much do governments spend, and what do they spend it on?", "How is democracy changing around the world?", "Which countries are using the most industrial robots?".
- **An action/invitation** (H5N1, ozone, WASH): "Track confirmed human cases of H5N1 'bird flu' since 1997", "Track the recovery of the ozone layer with updated data", "Explore updated data on water, sanitation, and hygiene (WASH) around the world".

If the dataset doesn't lend itself to a single headline finding, the question form is the safer fallback.

**`excerpt`** — One short sentence, distinct from the title, that summarises what the post is about. This is what shows up as a teaser on the /latest feed. Patterns from the published examples:

- "Explore updated data from <producer>." — the most common default (PIP, NVIDIA-alt, H5N1, robots, government revenue).
- "Explore updated data on <topic> from <producer>." (ATUS: "Explore updated data on time use from the U.S. Bureau of Labor Statistics.")
- "Track <topic descriptor>." (NVIDIA: "Track the rapidly increasing demand for AI hardware.")
- "We've updated <N> charts with the latest data from <producer>." (WASH, democracy)
- "Explore our new interactive chart on <topic> with <coverage detail>." (fertilizers — for new chart launches)

**`type`** — Always `announcement` for /latest data updates.

**`authors`** — The person who did the work, by name. Comma-separated for co-authors. Pull from the user (or the slack-announcement.md draft if it lists one).

**`kicker`** — Always `Data update`. (One example used `Data Update` with capital U — both work; lowercase `update` matches the more recent posts.)

**`\[+body\]`** — Literal marker. Always sits between the frontmatter and the body. Keep the backslash escapes.

**Body voice** — First-person, conversational, author voice. The post reads like the person who did the work telling you about it. **Not** corporate ("OWID has updated…" is wrong); **yes** "I recently updated…", "I've just updated…", "I just updated…", or "We recently updated…" / "We've just updated…" for joint work.

**Body length** — 100–200 words. Sample: ATUS ~105, NVIDIA ~140, robots ~110, OECD Government at a Glance ~155, US data centers ~145, UNU-WIDER ~155, World Bank PIP ~190, ozone ~165, mobile money ~180, fertilizers ~170, H5N1 ~135. Shorter than ~80 words usually means the post lacks a concrete finding or methodology framing — go gather more from `url_main` or the garden metadata before publishing.

**Body shape** — 3–6 short paragraphs. Hook → framing → specific finding(s) with numbers → optional caveat → optional cross-reference → closing source + update statement. Keep paragraphs short (1–3 sentences); the format reads as a feed entry, not as an article.

**Inline links** — Use markdown link syntax `[text](URL)` throughout the body. Common targets:

- The producer's own page (linked from the producer name): `[Poverty and Inequality Platform](https://pip.worldbank.org/)`, `[International Federation of Robotics](https://ifr.org/...)`, `[American Time Use Survey](https://www.bls.gov/tus/)`.
- Related OWID articles: `[the future of progress](https://ourworldindata.org/end-progress-extreme-poverty)`, `[how these data sources differ](https://ourworldindata.org/democracies-measurement)`.
- Methodology / definition pages: `[International Poverty Line](https://ourworldindata.org/new-international-poverty-line-3-dollars-per-day)`.
- The OWID topic page or explorer when relevant.

**Italics** — Use `*word*` (single asterisks) sparingly, for emphasising a key term: *extreme* poverty, *excluded* (for caveats).

**Closing pattern** — One or two sentences:

1. Source attribution (often): "This data comes from <producer>." Or with coverage detail: "This data comes from the OECD's Government at a Glance dataset, which covers 47 countries." Sometimes folded into the prior paragraph instead.
2. Update statement: "I recently updated [our charts | this chart] with the latest [release | quarterly release | data release]." Variants: `I recently updated…`, `I've updated…`, `I just updated…`, `I've just updated…`, `We recently updated…`. For datasets that update on a recurring cadence, **add a forward-looking note**:
   - NVIDIA: "…and will continue to do so each quarter."
   - H5N1: "We update this data quarterly."
   - US data centers: "I do this quarterly, so our next update will be around June 2026."

For releases that meaningfully extend coverage (new countries, new years), the update statement sometimes folds the coverage detail in: "I recently updated our charts with the latest release, which now covers 198 countries and territories from 1980 to 2023." (UNU-WIDER).

**Optional caveat paragraph** — Add when there's a real interpretation pitfall a reader could trip over. Pattern: "Keep in mind that…" or "It's important to note that…". Skip if the data is straightforward to interpret. Don't manufacture caveats that aren't load-bearing.

**`{.cta}` block** — The exit link. URL choice:

- **One chart focus** ⇒ `https://ourworldindata.org/grapher/<slug>`
- **Multiple charts (default)** ⇒ `https://ourworldindata.org/search?datasetProducts=<URL-encoded dataset title>` — value is the **dataset title**, resolved in this order: (a) `dataset.title` from the garden `.meta.yml` when it's set as an override, otherwise (b) `meta.origin.title` from the snapshot `.dvc`. Often includes a parenthetical acronym like `Luxembourg Income Study (LIS)` or `World Bank Poverty and Inequality Platform (PIP)`. **Not** the bare `producer` field. URL-encode parentheses as `%28` and `%29` if the OWID CMS doesn't auto-encode them — recent published posts use both literal `(LIS)` and `%28PIP%29` forms.
- **Explorer for the topic exists** ⇒ `https://ourworldindata.org/explorers/<name>`
- **Curated topic page exists** ⇒ topic URL (e.g. `/sdgs`)

The `text:` is descriptive — see the patterns under "Field-by-field guidance" above.

**`{.image}` block** — Filename of the chart screenshot. Pattern: `YYYY-MM-data-update-<slug>.png`. The image itself isn't generated by the skill; it's added to the Google Doc separately by the user. The skill just fills in the expected filename so the slot is reserved.

**`\[\]`** — Literal end-of-post marker. Always sits at the very end of the published content.

**Optional `:skip` ... `:endskip` block (after `\[\]`)** — For paragraphs the author drafted but cut from the final version. Won't be published. The skill should NOT generate `:skip` content automatically; it's a human editing tool. Mention it only if the user asks how to keep deleted material around.

---

## Worked examples (verbatim from the team's `/Data updates` Drive folder)

These are real published examples. They are models for tone, length, and structure — **not** boilerplate to copy-paste.

### Question-titled, single chart with date depth (NVIDIA, ~140 words)

```
title: NVIDIA's data center & AI revenue has grown nearly 15-fold since early 2023
excerpt: Track the rapidly increasing demand for AI hardware.
type: announcement
authors: Veronika Samborska
kicker: Data update

\[+body\]

[Most of the chips](https://epoch.ai/data/ai-chip-sales?view=graph&tab=h100_equivalents&proportion=share&viewType=designer) used to train and run AI models come from NVIDIA. This makes NVIDIA's data center & AI revenue one of the clearest public figures available for tracking demand for AI hardware.

The chart here shows how the company's quarterly revenue has changed over the last eight years, split by market segment.

In early 2023, data center & AI revenue was around $4 billion per quarter. By late 2025, this had grown to $62 billion — a more than 15-fold increase in under three years.

This data comes from NVIDIA's financial reports and is not adjusted for inflation. I recently updated this chart with the latest quarterly release and will continue to do so each quarter.

{.cta}
url: https://ourworldindata.org/grapher/nvidia-quarterly-revenue-segment
text: Explore this data going back to 2014 in our interactive chart
{}

{.image}
filename: 2026-04-data-update-nvidia-revenue.png
{}

\[\]
```

### Action-titled, with caveat and quarterly cadence (H5N1, ~135 words)

```
title: Track confirmed human cases of H5N1 "bird flu" since 1997
excerpt: Explore updated data from the WHO Global Influenza Programme.
type: announcement
authors: Lucas Rodés-Guirao
kicker: Data update

\[+body\]

Avian influenza A (H5N1), often referred to as "bird flu", is a subtype of influenza virus that infects birds and mammals. In rare cases, humans can also be infected.

Public health experts consider H5N1 a potential pandemic threat and monitor it closely, especially through the [WHO Global Influenza Programme](https://www.cdc.gov/bird-flu/php/avian-flu-summary/chart-epi-curve-ah5n1.html) (GIP).

Since 2003, the WHO has recorded nearly 1,000 confirmed human infections with H5N1 across 25 countries, causing more than 450 deaths.

Keep in mind that the true burden of infection is not fully known, because only a small fraction of potential cases are tested by labs to confirm whether they have influenza and to identify their strain.

I've updated our chart with the latest data from the WHO GIP (obtained via the [US CDC](https://www.cdc.gov/bird-flu/php/surveillance/chart-epi-curve-ah5n1.html)), covering monthly reported cases since 1997. We update this data quarterly.

{.cta}
url: https://ourworldindata.org/grapher/h5n1-flu-reported-cases
text: Explore the updated data in our interactive chart
{}

{.image}
filename: 2026-04-data-update-h5n1-flu.png
{}

\[\]
```

### Finding-titled, with cross-reference (World Bank PIP, ~190 words)

```
title: Nearly one in ten people worldwide still live in extreme poverty
excerpt: Explore updated data from the World Bank Poverty and Inequality Platform.
type: announcement
authors: Pablo Arriagada
kicker: Data update

\[+body\]

How many people live in poverty around the world, and how has that changed over the last decades?

The World Bank's [Poverty and Inequality Platform](https://pip.worldbank.org/) (PIP) is one of the primary tools we have for answering these questions.

PIP achieves comprehensive global coverage by combining income and consumption surveys, and also includes non-monetary income. It's the official source used to track the [UN's goal](https://ourworldindata.org/sdgs/no-poverty) of ending poverty.

In recent decades, the world has made remarkable progress against *extreme* poverty, defined as living below the [International Poverty Line](https://ourworldindata.org/new-international-poverty-line-3-dollars-per-day) of $3 per day.

In 1990, 2.3 billion people lived in extreme poverty. Since then the number has fallen by nearly two-thirds, to 826 million. But progress has slowed recently, and nearly one in ten people worldwide still live in extreme poverty.

Our colleague Max Roser wrote an article about [the future of progress](https://ourworldindata.org/end-progress-extreme-poverty) against this worst kind of poverty.

I recently updated our charts with the latest PIP release from the World Bank.

{.cta}
url: https://ourworldindata.org/search?datasetProducts=World+Bank+Poverty+and+Inequality+Platform+%28PIP%29
text: Explore all of the updated data in our interactive charts
{}

{.image}
filename: 2026-04-data-update-world-bank-pip.png
{}

\[\]
```
