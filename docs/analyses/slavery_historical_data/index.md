# Tracking historical progress against slavery and forced labor: a long-run data view

!!! info ""
    :octicons-person-16: **[Bastian Herre](https://ourworldindata.org/team/bastian-herre), [Esteban Ortiz-Ospina](https://ourworldindata.org/team/esteban-ortiz-ospina), [Max Roser](https://ourworldindata.org/team/max-roser)** • :octicons-calendar-16: February 16, 2026 *(last edit)* • [**:octicons-mail-16: Feedback**](mailto:info@ourworldindata.org?subject=Feedback%20on%20technical%20publication%20-%20Slavery%20historical%20data)

Our team assembled a dataset and published the article [Tracking historical progress against slavery and forced labor: a long run data view](https://ourworldindata.org/) to show when each country ended large-scale forced labor (or whether a country has not abolished it).

"Large-scale" forced labor here means forced labor that was common and entrenched — tolerated, enabled, or imposed by authorities, rather than isolated abuse.

The data shows that almost all countries have ended large-scale forced labor, often surprisingly recently. This has been well documented in the many excellent books by historians and social scientists. What we add to this is a quantitative, bird's-eye perspective on the global history of slavery and forced labor.

Summarizing these massive changes is challenging. Forced labor can take many different forms; legal rules and real-world practices often don't match, and no country is completely free from forced labor. So, in the rest of this technical documentation, we detail how we constructed the dataset and show it in full.

You can read more about the significance of the decline in slavery and forced labor, and the more general measurement challenges, in [the article](https://ourworldindata.org/).


## What data we used

To measure forced labor, we used data from the [Varieties of Democracy project (V-Dem)](https://www.v-dem.net/), which relies on surveys of around 3,500 country experts, and is based at the University of Gothenburg in Sweden.

V-Dem's expert surveys include two questions on forced labor: one asks about how free men are from servitude and other forms of forced labor, and the other asks the same for women. Experts score each country on a scale from 0 to 4\. A score of 0 corresponds to forced labor being widespread and being accepted or organized by the state; a score of 4 corresponds to forced labor being virtually non-existent.

The full survey question for men \[women\] is:

> **Question:** "Are adult men \[women\] free from servitude and other kinds of forced labor?"
>
> **Clarification:** Involuntary servitude occurs when an adult is unable to quit a job s/he desires to leave — not by reason of economic necessity but rather by reason of employer's coercion. This includes labor camps but not work or service which forms part of normal civic obligations such as conscription or employment in command economies.

**Responses:**

| Score | Description |
|:-----:|-------------|
| **0** | Servitude or forced labor is widespread and accepted (perhaps even organized) by the state. |
| **1** | Servitude or forced labor is substantial. Although officially opposed by the public authorities, the state is unwilling or unable to effectively contain the practice. |
| **2** | Servitude or forced labor exists but is not widespread and usually actively opposed by public authorities, or only tolerated in some particular areas or among particular social groups. |
| **3** | Servitude or forced labor is infrequent and only found in the criminal underground. It is actively and sincerely opposed by the public authorities. |
| **4** | Servitude or forced labor is virtually non-existent. |

V-Dem takes the independent expert assessments and calculates an overall score. They do this by using a [statistical model](https://v-dem.net/media/publications/wp21_2025.pdf) that combines the individual expert scores while accounting for uncertainty and differences between the individual assessments.

This approach helps address key measurement challenges: using country experts helps measure what forced labor looks like in practice — not just what the law says. Asking the experts to rate forced labor on a scale recognizes that it exists everywhere, but at different levels. And the fact that each point on the scale comes with a clear description helps experts interpret the scores in a similar way, and makes different forms of forced labor more comparable across places and over time.

Despite these strengths, V-Dem also has limitations that matter for our purposes here. First, its forced-labor indicators are designed to measure severity, not to draw a clear line between countries with and without large-scale forced labor. V-Dem's main measure places countries on a continuous scale, and a secondary measure places them on the original 0-4 scale. But neither specifies where a meaningful cutoff for “large-scale” forced labor should lie.

Second, coverage is incomplete. Some of today's smaller countries are not included in V-Dem at all. Others are only covered from independence or from around 1900 onward[^vdem-coverage], and for a few country-years, the forced-labor assessment data are missing.

To address these limitations, we built on V-Dem's work in two ways. We set a cutoff for what counts as “large-scale” forced labor, and we extended coverage where possible by linking modern countries to earlier states that governed the same territory.

In a small number of cases where this was not possible, we consulted additional sources. Throughout, we took a conservative approach and never altered V-Dem's own assessments. We explain this process in detail in the next sections.

## How we built on V-Dem's data

To get from V-Dem's scores to when countries abolished large-scale forced labor, we first had to choose a cutoff on V-Dem's scale for what counts as “large-scale”.

We counted a country as having large-scale forced labor if V-Dem's aggregate score on the original 0–4 scale, for either men or women, was 0 (forced labor was widespread and accepted or organized by the state) or 1 (forced labor was substantial and the state either unable or unwilling to contain it).

We did not count a country as having large-scale forced labor if the overall scores for men and women were 2, 3, or 4\. This means cases where forced labor was at most tolerated in some areas or among some social groups (score of 2), infrequent and sincerely opposed by the state (score of 3), or virtually non-existent (score of 4).

We chose this cutoff because we think it strikes a balance between capturing the big differences between historical and current forms of slavery and forced labor, and acknowledging that forced labor was common in many countries until relatively recently, and is common in some countries to this day.

This cutoff also lines up reasonably well with how major historical changes are commonly understood. For example, it places the end of large-scale forced labor in the United States at the end of the Civil War, and in the Soviet Union at the disbanding of the Gulag system after Stalin's death. At the same time, it identifies prominent recent cases where forced labor remains widespread, such as Afghanistan and North Korea.

Wherever one draws the line, there will always be borderline cases. For example, the cutoff makes 1979 the year China eliminated large-scale forced labor, when its forced labor system was weakened after Mao's death. But it would also be reasonable to argue that large-scale forced labor still exists in China today, given reports that the government forces parts of the country's minority populations, especially the Uyghurs, to work. Still, changing the cutoff to include all these targeted cases of forced labor (score of 2\) does not change the overall trend of a large decline in forced labor.

## How we filled the gaps in V-Dem's historical coverage

V-Dem's data does not cover every country for all of the last two centuries. To build a continuous series starting from today's countries, we extended the data back in time where we could. We kept V-Dem's expert assessments unchanged and relied on V-Dem's own historical coverage wherever possible. Only when there was no way to do that did we conservatively draw on additional sources.

V-Dem's coverage is enough to identify an end year for large-scale forced labor in 112 countries using our cutoff. For the remaining 62 countries, V-Dem's coverage starts later — often because these countries were not independent states for the full period we cover. To include these countries in a consistent long-run global series, we extended coverage back in time.

Where a modern country's territory was previously governed by an identifiable predecessor state, we applied the predecessor's end year.[^predecessors] This allowed us to assign end years for another 39 countries. For example, Belarus is covered only from 1990, but was part of the Soviet Union, so we use the Soviet Union's end year of 1954\.

For the remaining 23 countries without clear historical predecessors, we consulted additional sources to manually fill the gaps ourselves. Where sources were unclear or disagreed, we adopted the conservative assumption that large-scale forced labor continued until the first year V-Dem covers the country. For example, the additional sources are unclear about the extent of forced labor in Nigeria in the early 20th century, which is why we use 1913 as the end year, the year before the V-Dem starts (and there's no large-scale forced labor based on the expert assessments and our cutoff).

And where data was missing for only a few years, we assumed there was no abrupt change. For instance, there is a two-year gap for Jordan in 1921 and 1922, when it wasn't a part of the Ottoman Empire anymore, but it isn't included by V-Dem on its own yet.

You can find a country-by-country table with the end year and short notes explaining how we decided on each of them below. The table also links to the sources for the chart's annotations, which we added ourselves (since V-Dem does not include country descriptions).

<div class="csv-table" data-src="slavery_historical_data.csv" data-detail-columns="Coding explanation,Chart annotation"></div>

[Download data](slavery_historical_data.csv){: download="slavery_historical_data.csv" }

[^vdem-coverage]: At most, V-Dem extends back to 1789, the year of the French Revolution, which it considers the beginning of modern history.
[^predecessors]: We identified predecessors using the historical maps of CShapes, the work of Andreas Wimmer and Brian Min, and V-Dem's own information on historical country names.
