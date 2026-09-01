# Published reference announcements

Eight real published data updates. These are the authoritative style guide — where this skill's rules conflict with what these do, these win.

One normalization: kicker values are shown as `data-update`, the current convention. Some of these posts published with `Data update` before it settled.

Read at least three before drafting, chosen to match the update at hand: single-author routine refresh, multi-author, new chart, static viz, or politically sensitive.

## Contents

1. [Government spending](#1-government-spending) — question title, single author, multi-chart, inline caveat
2. [Ozone layer](#2-ozone-layer) — "Track…" title, narrative build
3. [NVIDIA revenue](#3-nvidia-revenue) — stat-as-title, single chart, quarterly cadence
4. [Democracy data](#4-democracy-data) — multi-author, plural voice, contested topic, hedging
5. [World Bank poverty](#5-world-bank-poverty-and-inequality) — term definition, hedged source claim, article-link close
6. [Fertilizers](#6-fertilizers) — brand-new chart, plural voice
7. [Population over the long run](#7-population-over-the-long-run) — static viz refresh
8. [Homicides](#8-homicides) — coverage caveat, modest chart count

---

## 1. Government spending

**Why this one:** The default shape. Question title, single-author singular voice, a comparison anchored to the chart, an inline caveat that heads off a predictable misreading, and the source introduced with coverage details in its own sentence.

```
title: How much do governments spend, and what do they spend it on?

excerpt: Explore updated data from the OECD on government spending.

type: announcement

authors: Pablo Arriagada

kicker: data-update

[+body]

In the chart, we see total government spending broken down by purpose, such as health, education, and defense, relative to the size of the economy (as measured by GDP). This is shown for a selection of OECD countries.

How much governments spend varies quite a lot across OECD countries: in France it's 57% of GDP, while in Chile it's less than half that (28%).

Keep in mind that these are relative shares, not absolute amounts. GDP itself varies considerably across countries, so the same percentage can represent very different sums depending on the size of a country's economy.

This data comes from the [OECD's Government at a Glance](https://www.oecd.org/en/publications/government-at-a-glance-2025_0efd0bcd-en.html) dataset, which covers 47 countries. I recently updated our charts with the latest release.

{.cta}

url: https://ourworldindata.org/search?datasetProducts=OECD+Government+at+a+Glance

text: Explore the updated data in our interactive charts, with detailed information on each spending category.

{}

{.image}

filename: 2026-03-data-update-govt-spending.png

{}

[]
```

---

## 2. Ozone layer

**Why this one:** "Track…" title. The body builds a short narrative — problem, response, result — in five one-to-two-sentence paragraphs, then lands the update in the final line. No forced "why it matters" beat; the story carried it.

```
title: Track the recovery of the ozone layer with updated data

excerpt: We've updated our charts on the Antarctic ozone hole with the latest data from NASA.

type: announcement

authors: Lucas Rodés-Guirao

kicker: data-update

[+body]

The [ozone layer](https://ourworldindata.org/ozone-layer) plays a vital role in making the planet habitable for us and other species by absorbing most of the sun's ultraviolet radiation.

But, during the 1970s–90s, humans were emitting large quantities of substances that depleted the ozone layer.

This led to the creation of ozone holes at the earth's poles, exposing life to higher levels of ultraviolet radiation and increasing the risks of skin cancer in humans.

During the 1980s, the world came together to form [an international agreement](https://ozone.unep.org/treaties/montreal-protocol) to reduce — and eventually eliminate — emissions of these depleting substances.

The political agreements were very effective. Since then, global emissions have [fallen by more than 99%](https://ourworldindata.org/grapher/ozone-depleting-substance-consumption).

The ozone holes have stopped growing and are now starting to close.

I recently updated our charts with the latest data from the NASA Goddard Space Flight Center's [Ozone Watch](https://ozonewatch.gsfc.nasa.gov/meteorology/annual_data.html), which tracks the size of the Antarctic ozone hole and the concentration of ozone in the stratosphere.

{.cta}

url: https://ourworldindata.org/search?datasetProducts=Ozone%20hole%20area%20and%20concentration

text: Explore the updated data in our interactive charts

{}

{.image}

filename: 2026-03-data-update-ozone-hole.png

{}

[]
```

---

## 3. NVIDIA revenue

**Why this one:** Stat-as-title (use sparingly since mid-2026, but this is what it looks like when justified). Single chart, quarterly cadence, and a one-clause inflation caveat folded into the source sentence rather than given its own paragraph.

```
title: NVIDIA's data center & AI revenue has grown nearly 15-fold since early 2023

excerpt: Track the rapidly increasing demand for AI hardware.

type: announcement

authors: Veronika Samborska

kicker: data-update

[+body]

[Most of the chips](https://epoch.ai/data/ai-chip-sales?view=graph&tab=h100_equivalents&proportion=share&viewType=designer) used to train and run AI models come from NVIDIA. This makes NVIDIA's data center & AI revenue one of the clearest public figures available for tracking demand for AI hardware.

The chart here shows how the company's quarterly revenue has changed since late 2018, split by market segment.

In early 2023, data center & AI revenue was around $4 billion per quarter. By late 2025, this had grown to $62 billion — a more than 15-fold increase in under three years.

This data comes from NVIDIA's financial reports and is not adjusted for inflation. I recently updated this chart with the latest quarterly release and will continue to do so each quarter.

{.cta}

url: https://ourworldindata.org/grapher/nvidia-quarterly-revenue-segment

text: Explore this data going back to 2014 in our interactive chart

{}

{.image}

filename: 2026-04-data-update-nvidia-revenue.png

{}

[]
```

---

## 4. Democracy data

**Why this one:** The model for a politically sensitive topic. Multi-author, so plural voice throughout. It states plainly that measurement is contested, presents sources as a list without ranking them, and links the explainer rather than adjudicating. Flag posts like this for Esteban and Bastian before publishing.

```
title: How can we measure the state of democracy around the world?

excerpt: We present data on democracy from several sources, and recently updated over 250 of our charts with the latest releases.

type: announcement

authors: Bastian Herre, Mojmír Vinkler, Lucas Rodés-Guirao

kicker: data-update

[+body]

Measuring democracy is challenging. It has many dimensions, and researchers assess them in different ways. No single source of data captures the full picture, so we present data from several of them.

These include [Varieties of Democracy](https://v-dem.net/data/the-v-dem-dataset/) (V-Dem; shown in the map here), the [Lexical Index](https://journals.sagepub.com/doi/10.1177/0010414015581050), [Freedom House](https://freedomhouse.org/report/freedom-world/2026/growing-shadow-autocracy), and the [Bertelsmann Transformation Index](https://bti-project.org/en/?&cb=00000).

Some of these data sources focus on broad characteristics of political systems to identify which countries are democracies. Others use expert surveys to assess smaller differences in the degree of democracy.

We wrote a full article explaining [how these data sources differ](https://ourworldindata.org/democracies-measurement) and when to use which one.

We recently updated more than 250 of our democracy charts with the latest releases from our sources, which now include data for 2025.

{.cta}

url: https://ourworldindata.org/search?datasetProducts=V-Dem+Country-Year+%28Full+%2B+Others%29%7ELexical+Index+of+Electoral+Democracy+%28LIED%29%7EFreedom+in+the+World%7EBertelsmann+Transformation+Index%2C+Scores%7EEpisodes+of+Regime+Transformation

text: Explore all of the updated data in our interactive charts

{}

{.image}

filename: 2026-03-data-update-v-dem.png

{}

[]
```

---

## 5. World Bank poverty and inequality

**Why this one:** Shows three patterns at once — a defined term surfaced properly ("extreme poverty, defined as living below the International Poverty Line of $3 per day"), the hedged source claim ("one of the primary tools", not "the main tool"), and a close that points to a relevant OWID article instead of the chart-count formula.

```
title: Nearly one in ten people worldwide still live in extreme poverty

excerpt: Explore updated data from the World Bank Poverty and Inequality Platform.

type: announcement

authors: Pablo Arriagada

kicker: data-update

[+body]

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

[]
```

---

## 6. Fertilizers

**Why this one:** A brand-new chart rather than a refresh, so plural voice and the "new interactive chart" CTA. Note the title breaks the question pattern with a statement plus an imperative — fine when the framing earns it.

```
title: Billions of people depend on synthetic fertilizers. Track how they're produced, traded, and used.

excerpt: Explore our new interactive chart on fertilizers with data for all countries since 1961.

type: announcement

authors: Hannah Ritchie, Edouard Mathieu

kicker: data-update

[+body]

Fertilizers have played an essential role in feeding a growing global population. It's estimated that just under half of the people alive today are dependent on synthetic fertilizers.

They have an environmental impact, too — both positive and negative.

They increase crop yields and thus reduce the amount of land we use for agriculture. But nitrogen fertilizers generate greenhouse gases and excess runoff into water systems, disrupting ecosystems.

Fertilizer use is about balance: using enough for productive farming, without overusing and damaging the environment.

We published a new interactive chart that helps you understand how much fertilizer is being used around the world, where it is produced, and how much different countries import and export.

The chart includes the latest data from the [Food and Agriculture Organization of the United Nations](https://www.fao.org/faostat/en/#data/RFN). It covers all countries since 1961, so you can see how fertilizer use has changed over time.

{.cta}

url: https://ourworldindata.org/grapher/fertilizer-use

text: Explore the data in our new interactive chart

{}

{.image}

filename: 2026-04-data-update-fertilizer-mdim.png

{}

[]
```

---

## 7. Population over the long run

**Why this one:** The static viz refresh variant. "We've refreshed" title, plural voice, a `featured-image` field, and a paragraph of context on why the pipeline work matters — appropriate here because the update is about the visualizations themselves, not new data.

```
title: We've refreshed key static visualizations on population growth over the long run

excerpt: How has the world's population changed over the last 12,000 years?

featured-image: default-featured-image.png

type: announcement

authors: Veronika Samborska, Marwa Boukarim

kicker: data-update

[+body]

How has the world's population changed over the last 12,000 years? How quickly did it grow in different periods, and what do projections tell us about the rest of this century?

We've refreshed four of our most popular static charts that show you answers to these questions, updating them with the latest estimates and projections from the [UN World Population Prospects (2024 revision)](https://population.un.org/wpp/).

These charts show up in multiple places across our work, including these two articles:

- [How has world population growth changed over time?](https://ourworldindata.org/population-growth-over-time)

- [Two centuries of rapid global population growth will come to an end](https://ourworldindata.org/world-population-growth-past-future)

This is part of a broader effort by our team of data scientists to build new pipelines for our static visualizations — making it easier to keep them current as new data becomes available, and more consistent visually.

You can learn more about how we combine multiple sources to [build our long-run population dataset](https://ourworldindata.org/population-sources), spanning from 10,000 BCE to 2100.

{.cta}

url: https://ourworldindata.org/population-growth

text: Explore all of our work on population on our dedicated topic page

{}

{.image}

filename: world_population_growth.png

{}

[]
```

---

## 8. Homicides

**Why this one:** Opens by defining the subject before using it. States a modest chart count (18) where it's informative, and closes on a coverage caveat that changes how a reader should interpret the map — exactly the kind of limitation worth surfacing rather than burying.

```
title: How do homicide rates vary around the world?

excerpt: Explore updated data on homicides from the UN Office on Drugs and Crime.

type: announcement

authors: Veronika Samborska

kicker: data-update

[+body]

Homicides — when people intentionally and illegally kill others for personal reasons — are the most serious crime.

To help us understand important aspects about homicide, such as how rates vary across countries and over time, the [United Nations Office on Drugs and Crime](https://www.unodc.org/) (UNODC) compiles an internationally comparable dataset going back to 1990.

The data also includes further breakdowns that tell us the sex of the victim, whether firearms were involved, or whether the perpetrator was an intimate partner.

I recently updated 18 of our charts with UNODC's latest release, from April this year.

Because not every country submits data every year, only a small minority of countries in the map here have data for 2024. Most run to 2022 or 2023, while some only have data from as far back as 2019.

{.cta}

url: https://ourworldindata.org/search?datasetProducts=United+Nations+Office+on+Drugs+and+Crime+-+Intentional+Homicide+Victims

text: Explore all of the updated data in our interactive charts

{}

{.image}

filename: 2026-06-data-update-homicides-unodc.png

{}

[]
```
