# Data Update Post Template (for OWID /latest)

Use this template to draft the short reader-facing post that gets published on [https://ourworldindata.org/latest](https://ourworldindata.org/latest) when a dataset refresh ships. This is **separate from the Slack announcement** — that one is a 10-field form for the internal #data-updates-comms channel; this one is a mini-blog-post for OWID readers, with a punchy title, 2–4 short paragraphs of substance, and a descriptive trailing link.

Hand the draft to the user for review and publication. The skill does not presume where the post is published — that's the human's call.

---

## Template

```
[Title — a punchy finding/claim or a question. Examples: "Nearly one in ten people worldwide still live in extreme poverty", "How much do governments spend, and what do they spend it on?", "NVIDIA's data center & AI revenue has grown nearly 15-fold since early 2023". NOT just the dataset name.]

[Body — 100–200 words of flowing prose, first-person, conversational. Recipe (mirroring 4 published examples — ATUS, PIP, NVIDIA, OECD Government at a Glance):

1. Hook — a question, or a one-sentence framing of why this dataset matters, or a finding that names the chart.
2. Source/methodology framing — what the dataset is, how it works, why it's the right tool for the question.
3. Specific finding(s) with concrete numbers — at least one quantitative claim a reader can latch onto. (E.g. "In 1990, 2.3 billion people lived in extreme poverty. Since then the number has fallen by nearly two-thirds, to 826 million." or "In early 2023, data center & AI revenue was around $4 billion per quarter. By late 2025, this had grown to $62 billion".)
4. Optional cross-reference to a related OWID article ("Our colleague X wrote an article about Y") if there's a natural one.
5. Brief closing source attribution + first-person update sentence: "This data comes from <producer>. I recently updated [our charts | this chart] with the latest [release | quarterly release]." Add a forward-looking note for recurring updates ("and will continue to do so each quarter").]

[Descriptive link text — NOT a bare URL. Render as a markdown link:
[Explore the updated data in our interactive charts](URL)
or, for single-chart updates with date depth:
[Explore this data going back to YYYY in our interactive chart](URL)
or, when the post mentions "all":
[Explore all of the updated data in our interactive charts](URL)
]
```

The URL inside the link is one of:

- One chart focus: `https://ourworldindata.org/grapher/<slug>`
- Multiple charts (default): `https://ourworldindata.org/search?datasetProducts=<URL-encoded dataset title>` — value is the **dataset title** (the `title` field in the snapshot `meta.origin` block, which often includes a parenthetical acronym like `Luxembourg Income Study (LIS)`), **not** the bare `producer` field.
- Explorer for the topic exists: `https://ourworldindata.org/explorers/<name>`
- Curated topic page exists: topic URL (e.g. `/sdgs`)

Do **not** use custom-collection URLs (`/collection/custom?charts=…`) — current OWID practice is to default to the search URL for multi-chart updates.

---

## Guidance per section

**Title** — A punchy claim, a question, or an action/invitation. The title is the part that pulls readers into the feed; "Luxembourg Income Study" is descriptive but unmemorable. Three observed patterns:

- **A finding/claim** (PIP, NVIDIA) — pull the most striking concrete number or trend from the data: "Nearly one in ten people worldwide still live in extreme poverty", "NVIDIA's data center & AI revenue has grown nearly 15-fold since early 2023".
- **A question** the dataset helps answer (OECD Government at a Glance, US data centers, UNU-WIDER Government Revenue) — "How much do governments spend, and what do they spend it on?", "How much revenue do governments collect, and where does it come from?".
- **An action/invitation** (H5N1) — "Track confirmed human cases of H5N1 'bird flu' since 1997". Less common, fits surveillance/monitoring datasets.

If the dataset doesn't lend itself to a single headline finding, the question form is the safer fallback.

**Body voice** — First-person, conversational, author voice. The post reads like the person who did the work telling you about it. **Not** corporate ("OWID has updated…" is wrong); **yes** "I recently updated…", "I've just updated…", "I just updated…".

**Body length** — 100–200 words. Sample: ATUS ~105, NVIDIA ~140, OECD Government at a Glance ~155, World Bank PIP ~190. Shorter than ~80 words usually means the post lacks a concrete finding or methodology framing — go gather more from `url_main` or the garden metadata before publishing.

**Body shape** — 2–4 short paragraphs. Hook → framing → specific finding(s) with numbers → optional cross-reference → closing source + update statement. Keep paragraphs short (1–3 sentences each); the format reads as a feed entry, not as an article.

**Closing pattern** — One or two sentences:

1. Source attribution (optional but common): "This data comes from <producer>." Or with a coverage detail: "This data comes from the OECD's Government at a Glance dataset, which covers 47 countries." Sometimes the source intro is folded into the prior paragraph instead (e.g. "The UNU-WIDER Government Revenue Dataset is one of the most comprehensive cross-country datasets…") and the update statement comes after.
2. Update statement: "I recently updated [our charts | this chart] with the latest [release | quarterly release | data release]." Variants seen: `I recently updated…`, `I've updated…`, `I just updated…`, `I've just updated…`. For datasets that update on a recurring cadence, **add a forward-looking note** in the same line or as a follow-up sentence:
   - NVIDIA: "…and will continue to do so each quarter."
   - H5N1: "We update this data quarterly."
   - US data centers: "I do this quarterly, so our next update will be around June 2026."

For releases that meaningfully extend coverage (new countries, new years), the update statement sometimes folds the coverage detail in: "I recently updated our charts with the latest release, which now covers 198 countries and territories from 1980 to 2023." (UNU-WIDER).

**Optional caveat paragraph** — Add when there's a real interpretation pitfall a reader could trip over. Pattern: "Keep in mind that…" or "It's important to note that…". Examples:

- H5N1: "Keep in mind that the true burden of infection is not fully known, because only a small fraction of potential cases are tested by labs to confirm whether they have influenza and to identify their strain."
- US data centers: "It's important to note that this only covers the cost of building the physical structures. Servers and other hardware inside are excluded, and they can make up a large share of the total cost of a data center."

Skip if the data is straightforward to interpret. Don't manufacture caveats that aren't load-bearing.

**Link text** — Always descriptive, always rendered as a markdown link, never a bare URL. Recurring patterns:

- `[Explore the updated data in our interactive charts](URL)` — the default for dataset-wide refreshes.
- `[Explore all of the updated data in our interactive charts](URL)` — when the post emphasises broad coverage (PIP).
- `[Explore this data going back to YYYY in our interactive chart](URL)` — for single-chart updates with date depth (NVIDIA).
- `[Explore the updated data in our interactive charts, with detailed information on each spending category](URL)` — when the link's destination has a useful additional dimension worth flagging.

**Don't restate the Slack form.** The Slack draft is form-shaped for stakeholders. The public post is a reader-facing mini-blog-post. They share editorial content (why this dataset matters, what's new) but live in different formats — don't paste form fields into the post.

---

## Worked examples (verbatim from /latest)

These are real published examples. They are models for tone, length, and structure — **not** boilerplate to copy-paste.

### Action-titled, with caveat and recurring-update note (H5N1, ~135 words)

```
Track confirmed human cases of H5N1 "bird flu" since 1997

Avian influenza A (H5N1), often referred to as "bird flu", is a subtype of influenza virus that infects birds and mammals. In rare cases, humans can also be infected.

Public health experts consider H5N1 a potential pandemic threat and monitor it closely, especially through the WHO Global Influenza Programme (GIP).

Since 2003, the WHO has recorded nearly 1,000 confirmed human infections with H5N1 across 25 countries, causing more than 450 deaths.

Keep in mind that the true burden of infection is not fully known, because only a small fraction of potential cases are tested by labs to confirm whether they have influenza and to identify their strain.

I've updated our chart with the latest data from the WHO GIP (obtained via the US CDC), covering monthly reported cases since 1997. We update this data quarterly.

[Explore the updated data in our interactive chart](https://ourworldindata.org/grapher/h5n1-flu-reported-cases)
```

### Single chart with date depth (NVIDIA, ~140 words)

```
NVIDIA's data center & AI revenue has grown nearly 15-fold since early 2023

Most of the chips used to train and run AI models come from NVIDIA. This makes NVIDIA's data center & AI revenue one of the clearest public figures available for tracking demand for AI hardware.

The chart here shows how the company's quarterly revenue has changed over the last eight years, split by market segment.

In early 2023, data center & AI revenue was around $4 billion per quarter. By late 2025, this had grown to $62 billion — a more than 15-fold increase in under three years.

This data comes from NVIDIA's financial reports and is not adjusted for inflation. I recently updated this chart with the latest quarterly release and will continue to do so each quarter.

[Explore this data going back to 2014 in our interactive chart](https://ourworldindata.org/grapher/nvidia-quarterly-revenue-segment)
```

### Question-led, dataset-wide (OECD Government at a Glance, ~155 words)

```
How much do governments spend, and what do they spend it on?

In the chart, we see total government spending broken down by purpose, such as health, education, and defense, relative to the size of the economy (as measured by GDP). This is shown for a selection of OECD countries.

How much governments spend varies quite a lot across OECD countries: in France it's 57% of GDP, while in Chile it's less than half that (28%).

Keep in mind that these are relative shares, not absolute amounts. GDP itself varies considerably across countries, so the same percentage can represent very different sums depending on the size of a country's economy.

This data comes from the OECD's Government at a Glance dataset, which covers 47 countries. I recently updated our charts with the latest release.

[Explore the updated data in our interactive charts, with detailed information on each spending category](URL)
```

### Finding-led with cross-reference (World Bank PIP, ~190 words)

```
Nearly one in ten people worldwide still live in extreme poverty

How many people live in poverty around the world, and how has that changed over the last decades?

The World Bank's Poverty and Inequality Platform (PIP) is one of the primary tools we have for answering these questions.

PIP achieves comprehensive global coverage by combining income and consumption surveys, and also includes non-monetary income. It's the official source used to track the UN's goal of ending poverty.

In recent decades, the world has made remarkable progress against extreme poverty, defined as living below the International Poverty Line of $3 per day.

In 1990, 2.3 billion people lived in extreme poverty. Since then the number has fallen by nearly two-thirds, to 826 million. But progress has slowed recently, and nearly one in ten people worldwide still live in extreme poverty.

Our colleague Max Roser wrote an article about the future of progress against this worst kind of poverty.

I recently updated our charts with the latest PIP release from the World Bank.

[Explore all of the updated data in our interactive charts](https://ourworldindata.org/search?datasetProducts=World+Bank+Poverty+and+Inequality+Platform+%28PIP%29)
```
