# Data Update Post Template (for OWID /latest)

Use this template to draft the short public-facing "Data update" blurb that gets published on [https://ourworldindata.org/latest](https://ourworldindata.org/latest) when a dataset refresh ships. This is **separate from the Slack announcement** — that one is a 10-field form for the internal #data-updates-comms channel; this one is short prose for OWID readers.

Hand the draft to the user for review and publication. The skill does not presume where the post is published — that's the human's call.

---

## Template

```
[Optional title — short noun phrase, usually the dataset name. Many /latest posts skip the title.]

[40–100 words of prose, recipe below. First-person, conversational, author voice.

Recipe:
- Optional lead: a "why" clause OR a one-sentence headline finding from the data.
- First-person update statement ("I recently updated", "I've just updated", etc.) + scope ("our charts", "more than 20 of our charts", "this chart").
- Source attribution ("with the latest [release | data] from <producer>").
- Optional one-detail clause about what's new in this release.]

[Single closing link → one of:
- One chart focus:        https://ourworldindata.org/grapher/<slug>
- Multiple charts (default): https://ourworldindata.org/search?datasetProducts=<URL-encoded-producer>
- Explorer for the topic exists: https://ourworldindata.org/explorers/<name>
- Curated topic page exists: https://ourworldindata.org/<topic>
]
```

---

## Guidance per section

**Title** — Optional. When used, it's a short noun phrase or topic question, e.g. "American Time Use Survey", "Forest Resource Assessment", "How are forest sizes changing around the world?". Many /latest posts have no explicit title — the body just leads.

**Tone** — First-person, conversational, author voice. The post reads like the person who did the work telling you about it. **Not** corporate ("OWID has updated…" is wrong); **yes** "I recently updated…", "I've just updated…", "I just updated…". "We've just updated…" appears occasionally and is fine when more than one person was involved.

**Length** — 40–100 words is the typical band on /latest. Aim shorter (~20–50 words) for single-chart refreshes and quarterly cadences; longer (up to ~150 words) when the dataset has 2–3 notable findings worth surfacing in the post itself. If the draft creeps past 150 words, cut — readers scan, they don't read.

**Lead** — Optional. Two patterns work:
- A "why" clause: "To help you track this…", "To help you understand the scale of tourism and some of its impacts…"
- A one-sentence headline finding from the data: "These losses have fallen dramatically since the millennium. Last year, 10,000 tonnes were spilled, less than one-thirtieth of the amount lost in a typical year in the 1970s."

Many short blurbs skip the lead entirely and dive straight in. Use a lead when there's something genuinely interesting to surface; skip it for routine quarterly refreshes.

**Update statement + scope** — One sentence. Pair the first-person verb with concrete scope: "I recently updated our charts" / "more than 20 of our interactive charts" / "this chart" / "over 400 of them" / "nearly 300 charts". Specificity is better than vague ("our charts").

**Source attribution** — Name the producer in the same sentence: "with the latest data from the UN Tourism Statistics Database", "with the 2026 European Electricity Review from Ember", "based on multiple sources compiled by Rupert Way at the University of Oxford". Match the producer wording from the dataset's `.meta.yml` for consistency.

**Optional one-detail clause** — One sentence on what's new in this release. "Now goes through 2024", "now include 2025 data for European countries, including Turkey", "We now have a better picture of how social spending changed during the COVID-19 pandemic." Skip if the only thing new is a routine year-rollover.

**Link choice** — Decide from step 8's chart-pick output:

- `1 published chart` was picked / one chart is the focus ⇒ **grapher URL** `https://ourworldindata.org/grapher/<slug>`.
- `>1 published charts` were picked / dataset-wide refresh ⇒ **search URL** `https://ourworldindata.org/search?datasetProducts=<URL-encoded-producer>`. This is the same URL the Slack announcement template builds — single source of truth.
- The producer/topic has an existing OWID **explorer** (e.g. minerals → `/explorers/minerals`, natural disasters → `/explorers/natural-disasters`, CO₂ → `/explorers/co2`) ⇒ prefer the explorer URL over the search URL.
- The producer/topic has a **curated topic page** (e.g. SDG Tracker → `/sdgs`) ⇒ prefer the topic URL over the search URL.

**Do not use** custom-collection URLs (`/collection/custom?charts=…`) even though some /latest posts do — current OWID practice is to default to the search URL for multi-chart updates.

**Don't restate the Slack form.** The Slack draft is form-shaped for stakeholders. The public post is short prose for readers. They share editorial content (why this dataset matters, what's new) but live in different formats — don't paste form fields into the post.

---

## Worked examples

These are real published examples from the /latest feed. They are models for tone and length, **not** boilerplate to copy-paste.

### Short single-chart refresh (~20 words, grapher URL)

```
California's driverless taxis now transport passengers for nearly five million miles per month.

I recently updated this chart based on the latest report, and will do so every quarter going forward.

https://ourworldindata.org/grapher/passenger-miles-traveled-self-driving-taxis
```

### Medium dataset-wide refresh (~40 words, search URL)

```
To help you understand the scale of tourism and some of its impacts, I recently updated more than 20 of our interactive charts with the latest data from the UN Tourism Statistics Database.

https://ourworldindata.org/search?datasetProducts=UN%20Tourism%20Statistics%20Database
```

### Explorer-backed refresh (~46 words, explorer URL)

```
I've just updated our charts with the latest data on natural disasters. This data helps us track where disasters are happening; what types of events they are; their human and economic impacts; and how these trends are changing over time.

https://ourworldindata.org/explorers/natural-disasters
```
