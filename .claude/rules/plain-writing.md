---
paths:
  - "etl/steps/**/*.meta.yml"
  - "etl/steps/viz/**/*.yml"
  - "snapshots/**/*.dvc"
---

# Writing plainly for OWID readers

This rule covers text the public reads on ourworldindata.org: chart titles, subtitles and footnotes, `description_short`, `description_key`, `description_processing`, display names, the origin `description` in a snapshot `.dvc` (shown in the Sources tab), and public posts such as the /latest data update and data insights.

`description_from_producer` is exempt. It is the producer's own text, kept verbatim.

## Who you are writing for

Picture a curious friend who isn't an expert. They are smart and interested, but they have never studied this field. Write the way you would explain the data to them.

## Why it matters

- **Formal wording costs the reader.** Every fancy word or construction that only exists in writing makes the reader work harder. Many readers stop.
- **Plain words expose weak ideas.** If a sentence only sounds right in formal language, the idea under it is often vague. Saying it plainly forces you to say what you mean.
- **Simple isn't dumbed down.** Plain words can carry a complex, precise idea. Cut the wording, never the substance or the caveats.

## The test

Read each sentence as if you were saying it to that friend. If you would never say it that way, rewrite it. If you couldn't say it in one breath, split it.

## How

- **Use the everyday word when one exists.** When the precise term matters, because it names the indicator or it is what readers search for ("GDP per capita", "stunting", "life expectancy"), keep it and explain it in plain words the first time. Never rename an established term.
- **Accuracy wins over plainness.** Keep the exact meaning and the qualifiers ("estimated", "adults only", "private construction"). Find plain words for them, even if that takes a few more.
- **Use active voice and name who does what.** "We" is OWID; the producer goes by name. "We convert fiscal years to calendar years", not "Fiscal years were converted".
- **Plain is not casual.** State the fact; don't tell the reader what to do ("be careful when comparing"). No chatty phrasing ("a bit", "pretty much", "basically"). Contractions are fine.

## Editing existing text

Rewrite freely when a plainer version exists, within the rules above. When you work in a file, also list other stiff fields you notice nearby so the user can decide on them, rather than rewriting them silently.

## Examples

**A caveat in `description_key`**

> Before: It should be noted that cross-country comparability may be limited owing to heterogeneity in national definitions of homelessness.
>
> After: Comparisons between countries may be limited because national definitions of homelessness differ.

Why: nobody says "it should be noted" or "heterogeneity" out loud. The rewrite keeps the claim and its hedge, and changes only the words.

**A subtitle (`description_short`)**

> Before: The estimated aggregate quantity of anthropogenic greenhouse gas emissions attributable to the agricultural sector, expressed in CO₂-equivalents.
>
> After: Estimated greenhouse gas emissions from farming, measured in tonnes of CO₂-equivalents.

Why: "aggregate quantity", "anthropogenic" and "attributable to" make the reader work for nothing. "Estimated" stays because it is true.

**`description_processing`**

> Before: Fiscal-year data were converted to calendar years by attributing values to the year in which the fiscal year ends.
>
> After: The source reports data by fiscal year. We assign each fiscal year to the calendar year in which it ends.

Why: the passive voice hides who did it. Name OWID as "we".

**An origin `description` in a `.dvc`**

> Before: The Global Food Database constitutes a comprehensive repository of harmonized statistics pertaining to food systems worldwide.
>
> After: The Global Food Database provides statistics on food production and trade for about 200 countries, from 1961 onward.

Why: "comprehensive repository" sounds impressive but says nothing. Writing it plainly forces you to state what the data actually covers.

**A /latest data update post**

> Before: This update incorporates the most recent data release from the producer, extending temporal coverage through 2024.
>
> After: I've updated our charts with the latest data, which now goes up to 2024.

Why: you would never tell a friend you were "extending temporal coverage".

**Going too far**

> Too formal: Gross domestic product per capita, expressed in international-$ at 2021 purchasing power parity.
>
> Too simple, and wrong: How rich people are.
>
> Right: Gross domestic product per capita. This data is adjusted for inflation and for differences in living costs between countries.

Why: keep the term readers know and search for. Explain what it hides instead of renaming it.
