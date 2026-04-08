# Slack Dataset Update Announcement Template

Use this template when announcing a dataset update in Slack (#data-updates or similar channel).
Fill in each section using the guidance below. Omit optional sections if not applicable.

---

## Template

```
**What dataset(s) did you update?**
[Dataset name(s) and source(s). E.g.: "World Development Indicators – World Bank"]

**When was this data released? When is the next scheduled release / our plan for next update?**
[Release date of this version. Cadence or planned next update date. E.g.: "Released Feb 2026. Next update: Feb 2027."]

**Who is the data source(s)? Is there anything our users should know about them?**
[Institution name and any context on methodology, who compiles it, or notable quirks about the source.]

**What's the coverage of the data in terms of years and countries/regions?**
[Year range and geographic scope. Note any gaps or caveats in recent-year coverage.]

**How many charts did this update affect?**
[Number of charts. Add context: "small update (4 charts)" vs "large update (580 charts across multiple datasets)".]

**What does this dataset help our users understand about the world, and why is it important they know that?**
[Why do we publish this data? What question does it answer? What's unique about it vs similar datasets?]

**Any important caveats or pitfalls in interpretation that users should know about this data?** *(optional)*
[Methodology notes, comparability warnings, known limitations. Skip if none.]

**Anything interesting to note about this update, including what you had to do? Anything else you'd like to add?** *(optional)*
[Notable findings in the data. Any unusual processing steps. Collaboration with the data provider. Skip if routine.]

[Add 1–3 chart views representing the whole dataset — not overly specific to one country]

[Link to updated charts, e.g. https://ourworldindata.org/search?datasetProducts=World%20Development%20Indicators]
```

---

## Guidance per section

**Dataset name**
Use the full public-facing name, not the internal short_name. Include the source organisation.

**Release date / next update**
Be specific about when the source released this version. If the release cadence is irregular or unknown, say so.

**Data source**
Name the institution. Mention if data is compiled from multiple underlying sources (e.g. Pew combines Gallup, NES, ABC/WaPo). Flag any access restrictions or unusual data-sharing arrangements.

**Coverage**
Give the year range and number of countries. If recent years have lower coverage (common in annual releases), call that out explicitly — users may be confused by sparse recent data.

**Charts affected**
Give the raw number. A one-line qualifier helps: handful (1–10), moderate (10–50), large (50–200), massive (200+). If changes span multiple datasets, say so.

**Why we have this data**
Answer: what question does this dataset help answer that our users care about? Why this source over alternatives? What would be missing from OWID without it?

**Caveats** *(optional)*
Include if there's anything that could lead users to misinterpret the data — definitional quirks, aggregation choices, smoothing, known data quality issues. If you noted it in the metadata, say so.

**Interesting notes** *(optional)*
Highlight 2–3 notable findings or trends visible in the data. Mention any non-routine work: scraping, working with the provider, methodological changes, what we changed vs. previous version.

**Charts**
Pick 1–3 views that represent the breadth of the dataset. Avoid very country-specific or niche views. Prefer the most-viewed or most-linked charts if possible.

**Link**
Search result link preferred (e.g. `https://ourworldindata.org/search?datasetProducts=...`). If no good search link exists, use a chart collection link and put it in thread to keep the main post readable.
