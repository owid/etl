# Writing and style guide

> Source: OWID Notion page — [Writing and style guide](https://app.notion.com/p/owid/Writing-and-style-guide-d51a3739ff8542ca90297fa8de40437c).
> Last synced from Notion: 2026-09-11
> The check-metadata-style skill verifies this date on every run and refreshes this file from Notion when it is more than two months old. Commit refreshes via a PR.

This guide establishes a uniform style for all OWID content to ensure consistency, clarity, and accuracy across our publications. The rules in this guide apply to all writing we do on OWID, whether in articles, topic pages, or charts.

> ⚠️ If writing for OWID, you **must** have [Grammarly](https://www.grammarly.com/browser) installed on your computer and browser. This will take care of 95% of writing and style issues for you.
>
> Make sure to configure it with:
> - Check text with the browser extension
> - Check for writing suggestions on Google Docs
> - I write in 🇺🇸 American English
>
> Issues highlighted by Grammarly in red should always be addressed and fixed.

## Branding

### Write “Our World in Data” and “OWID”

Write “Our World in Data” with the first letter of each word capitalized, except for “in”. Use “OWID” for the acronym, not “OWiD”.

- ❌ The latest research from Our World In Data (OWiD) shows…
- ✅ The latest research from Our World in Data (OWID) shows…

### Write OurWorldinData.org for ease of reading

When spelling out URLs, capitalize each word except “in” so that it’s easier to read.

- ✅ OurWorldinData.org/child-mortality

### Write “Our World in Data” without italics

Write “Our World in Data” in normal font, without italics.

- ❌ One of my other most popular articles on *Our World in Data* looked at the role of food miles in the carbon footprint of our food.
- ✅ One of my other most popular articles on Our World in Data looked at the role of food miles in the carbon footprint of our food.

## Capitalization

### Don’t capitalize topic names in text

Do not capitalize the names of topics when they appear in the middle of a text or sentence.

- ❌ We published a new topic page on Animal Welfare.
- ✅ We published a new topic page on animal welfare.

One exception is when you directly refer to the page’s title.

- ❌ Our new energy page is live.
- ✅ Our new Energy page is live.

### Use sentence case almost everywhere

**Sentence case:**

- Title of articles
  - ❌ The Limits of our Personal Experience and the Value of Statistics
  - ✅ The limits of our personal experience and the value of statistics
- Title of charts
  - ❌ Life Expectancy at Birth
  - ✅ Life expectancy at birth
- Headers of linear topic pages
  - ❌ Understanding Labor Force Statistics: Participation, Unemployment, and Employment
  - ✅ Understanding labor force statistics: participation, unemployment, and employment
- Homepage components
  - ❌ Updates and Announcements
  - ✅ Updates and announcements

**Title case:**

- Title of topic pages
  - ❌ Artificial intelligence
  - ✅ Artificial Intelligence
- Headers of modular topic pages
  - ❌ Key charts on energy
  - ✅ Key Charts on Energy

## Grammar and syntax

### Use “who” for people

Use “who” when referring to people and “that” when referring to objects or animals.

- ❌ The children that suffer from malnutrition need help.
- ✅ The children who suffer from malnutrition need help.

### Express ratios and frequencies clearly

Avoid phrases like “every second” or “every third”, which give an inaccurate sense of regularity. Instead, prefer “1 in 2” or “half of”.

- ❌ Every second child in the study...
- ✅ Half of the children in the study... or 1 in 2 children in the study...

### “Data” is always a singular noun

We always use “data” as a singular noun, never as a plural one.

- ❌ The data published by OPHI are the most prominent multidimensional poverty data.
- ✅ The data published by OPHI is the most prominent multidimensional poverty data.

### Don’t start chart subtitles with “The” if the next word works just as well

Don’t add “The” at the start of a subtitle if it isn’t strictly required. (This rule also applies to `description_short` and `grapher_config.subtitle`, given ETL inheritance rules.)

- ❌ The estimated number of deaths from drowning among children under 5.
- ✅ Estimated number of deaths from drowning among children under 5.

### Always finish chart subtitles with punctuation (typically a period)

Chart subtitles should always end with punctuation, most likely a simple period. (This rule also applies to `description_short` and `grapher_config.subtitle`, given ETL inheritance rules.)

- ❌ Estimated number of deaths from drowning among children under 5
- ✅ Estimated number of deaths from drowning among children under 5.

## Short citations for charts

### Cite data products as the data provider writes them

In short citations on charts, cite the title of data products using the same capitalization provided by the data provider.

- ❌ World population prospects
- ✅ World Population Prospects

### Cite authors using their surnames

In short citations on charts, cite authors using only their surnames. For one author, use the surname only. For two authors, list both surnames. For three or more authors, use the first author’s surname followed by “et al.”.

- One author: Williams
- Two authors: Williams and Jones
- Three or more authors: Williams et al.

### Separate producer and data product with an en dash

In citations that name both the producer and the data product, separate them with a spaced en dash (–), not a hyphen (-) nor an em dash (—).

- ❌ BGS - World Mineral Statistics (2025)
- ✅ BGS – World Mineral Statistics (2025)

## Writing in GDoc

### Don’t add unclear links to consecutive words

Avoid adding hyperlinks to multiple consecutive words in a sentence. If you want to add multiple links, make them explicit.

- ❌ You can read more about artificial intelligence [in](https://ourworldindata.org/ai-impact) [our](https://ourworldindata.org/brief-history-of-ai) [articles](https://ourworldindata.org/ai-investments).
- ✅ You can read more about artificial intelligence in our articles on [how it’s transforming our world](https://ourworldindata.org/ai-impact), [its brief history](https://ourworldindata.org/brief-history-of-ai), and [the increasing investments in it](https://ourworldindata.org/ai-investments).

### Place links on informative words

Place links on informative words that briefly describe what’s at the link, rather than meaningless words like “here”.

- ✅ You can read more about this on [our Energy topic page](https://ourworldindata.org/energy).
- ❌ You can read more about this on our Energy topic page [here](https://ourworldindata.org/energy).

### Don’t add consecutive footnotes

Avoid placing several footnotes in a row. Instead, merge them into a single footnote.

- ❌ The theory was developed in the early 20th century.¹²³
- ✅ The theory was developed in the early 20th century.¹

### Limit footnotes to one per sentence

Use only one footnote per sentence. If multiple references are needed for a single sentence, group them into one footnote, always placed at the end of the sentence after the final punctuation mark, without a space before.

- ❌ The study revealed significant results¹ in various fields².
- ✅ The study revealed significant results in various fields.¹

### Use the “callout” tag

Utilize the “callout” tag in Google Docs for acknowledgments.

- ❌ Acknowledgments: X, Y, and Z provided valuable feedback on this article.
- ✅ `{.callout}title: Acknowledgments[.+text]X, Y, and Z provided valuable feedback on this article.[]{}`

## Descriptions

### Use girls/boys for people below 18, women/men for all ages

When referring to a group of people where everyone is under 18 years old, use “girls/boys”. Use “women/men” for all ages.

- ❌ The study included 17-year-old men.
- ✅ The study included 17-year-old boys.

### Do not use females/males as nouns

Do not use “females” or “males” as nouns. They can be used as adjectives.

- ❌ The survey was conducted among males and females.
- ✅ The survey was conducted among male and female participants.

### Use gender-neutral, people-first language

Use singular “they” for a person of unspecified gender. Write “humanity” or “people”, not “mankind”, and “human-made”, not “man-made”. Put the person before the condition: “people with diabetes”, not “diabetics”; “people living in poverty”, not “the poor”.

- ❌ Each patient must give his consent. Diabetics are at higher risk. Man-made emissions.
- ✅ Each patient must give their consent. People with diabetes are at higher risk. Human-made emissions.

### Don’t call a map a chart

When showing a map, call it a map, don’t call it a chart.

- ❌ As the two charts below show (…)
- ✅ As the bar chart and the world map below show (…)

### Don’t point to left/right content where it’s not always left/right

When writing about charts in articles or linear topic pages, avoid pointing readers to a “left” or “right” chart, given it all looks vertical on phones and tablets. You can talk about the first/second chart instead.

- ❌ The chart on the left (…), while the chart on the right (…)
- ✅ The first chart (…), while the second chart (…)

## Dates

### In text, write dates in American format

Write the month first, then the day, then the year. Use a comma before the year. Do not use ordinal suffixes or leading zeros on the day.

- ❌ 6 March 2025 / March 6th, 2025 / March 06, 2025 / March, 2025 / 03/06/2025 / 06-03-2025
- ✅ March 6, 2025 / March 6 / March 2025

### In data, write dates in ISO 8601 format

In data, filenames, and code, use ISO 8601 format.

- ❌ 06/03/2025, 06-03-2025
- ✅ 2025-03-06

### Use BCE/CE, not BC/AD

Use “BCE” (Before Common Era) and “CE” (Common Era) instead of “BC” (Before Christ) and “AD” (Anno Domini).

- ❌ The Roman Empire reached its peak in 117 AD.
- ✅ The Roman Empire reached its peak in 117 CE.

### Use en dashes for year ranges

Use en dashes (–) rather than hyphens (-) for year ranges.

- ❌ The study covers the period 1990-2020.
- ✅ The study covers the period 1990–2020.

### Write decades and centuries in digits

Write decades as “the 1990s”, with no apostrophe, never “the ’90s”. Write centuries as “the 20th century”, with digits and a lowercase c. Hyphenate when used before a noun: “20th-century medicine”, “mid-1990s”.

- ❌ The 1990’s, the ’90s, the twentieth century, 20th century medicine
- ✅ The 1990s, the 20th century, 20th-century medicine

## Punctuation

### Use the Oxford comma

Use the Oxford comma in lists to avoid ambiguity. This is the comma before the final “and” or “or” in a list.

- ❌ The key factors are economic growth, education and health.
- ✅ The key factors are economic growth, education, and health.

### Use commas and semi-colons appropriately

Use commas to separate elements in a sentence and semi-colons to link independent clauses or organize complex lists.

- ❌ The study shows a significant result, it indicates an increase in happiness.
- ✅ The study shows a significant result; it indicates an increase in happiness.

### Use em dashes for asides, with spaces on both sides

Always use em dashes (—) for asides, and add spaces on both sides of the em dash. Never use hyphens (-) or en dashes (–) for asides.

- ❌ Climate change - which affects global temperatures - is a pressing issue.
- ❌ Climate change – which affects global temperatures – is a pressing issue.
- ❌ Climate change—which affects global temperatures—is a pressing issue.
- ✅ Climate change — which affects global temperatures — is a pressing issue.

### Use double quotes, not single quotes

Always use double quotes for quotations and titles. Single quotes are used only for quotations within quotations. Punctuation goes outside the closing quote unless it is part of what is quoted.

- ❌ ‘Climate change is the defining issue of our time’, stated the researcher.
- ✅ “Climate change is the defining issue of our time”, stated the researcher.

### Use curly quotes and apostrophes

Use curly quotation marks (“ ”) and apostrophes (’) in all prose, chart text, and titles. Straight marks (" ') only inside code.

- ❌ "Data" is the world's most valuable resource.
- ✅ “Data” is the world’s most valuable resource.

### Hyphenate compound modifiers before a noun

Hyphenate a compound modifier when it comes before the noun, not after it, and never after an adverb ending in -ly. Common OWID compounds: low-income, middle-income, high-income, under-five, long-term, short-term. “Per capita” never takes a hyphen.

- ❌ Low income countries; the trend is long-term; a rapidly-growing city; per-capita emissions
- ✅ Low-income countries; the trend is long term; a rapidly growing city; per capita emissions

### Never capitalize after colons

Do not capitalize the first word after a colon unless it is a proper noun.

- ❌ The key findings were: Several factors influence this trend.
- ✅ The key findings were: several factors influence this trend.

## Spelling

### Write in American English

Write all text in American English.

- ❌ The programme was analysed and found to be effective.
- ✅ The program was analyzed and found to be effective.

### No period for “US” and “UK”

When writing about the United States and United Kingdom, don’t use periods when shortening their names.

- ❌ The U.S. and the U.K. are both high-income countries.
- ✅ The US and the UK are both high-income countries.

### Prefer “US” over “American”

When referring to the country, use “US” as the adjective rather than “American”.

- ❌ The American government wants to boost American exports.
- ✅ The US government wants to boost US exports.

### Use OWID’s standard country names

Write country names as they appear in OWID charts: Czechia, Democratic Republic of Congo. Prefer spelling out “United States” and “United Kingdom” the first time they come up, then “US” and “UK”; don’t write “United States (US)” as if readers needed to learn the acronym. Never “Britain” or “England” when you mean the UK.

- ❌ The Czech Republic, the United States (US), Britain
- ✅ Czechia, the United States, the UK

### Spell out numbers from one to ten…

Numbers from one to ten should be spelled out inside sentences. Larger numbers are written in digits. Always use digits for ages (“children under 5”), quantities with a unit (“3 km”, “7 tonnes”), percentages, and all chart text and tables.

- ❌ The MPI is made up of 10 indicators grouped into 3 dimensions. It measures how people experience poverty across more than one hundred countries.
- ✅ The MPI is made up of ten indicators grouped into three dimensions. It measures how people experience poverty across more than 100 countries.

### … but keep number formats harmonized

Do not combine spelled-out numbers and digit numbers in the same sentence if they refer to a similar thing.

- ❌ The death rate per 1,000 people has dropped from 18 per 1,000 to six per 1,000.
- ✅ The death rate per 1,000 people has dropped from 18 per 1,000 to 6 per 1,000.

### Write percentages with digits and %

Always write percentages as digits followed by “%”, with no space. Never “percent” or “per cent”. Write “percentage points” in full.

- ❌ Five percent of the population, a rise of 2 %
- ✅ 5% of the population, a rise of two percentage points

### Use commas in large numbers, and words from a million

Use a comma as the thousands separator. From a million upward, write digits followed by the word: “2.5 million”, “8 billion”. Never write these as long digit strings.

- ❌ 1000 people, 1 000 people, 2,500,000 people
- ✅ 1,000 people, 2.5 million people

### Write $ for US dollars and international-$ for PPP-adjusted figures

Write “$” alone for US dollars, before the number. For figures adjusted for purchasing power parity, write “international-$” and apply `#dod:int_dollar_abbreviation` on first use. Other currencies use their name or symbol (€, £).

- ❌ USD 1,200, US$1,200, 1,200 dollars, 1,200 PPP dollars
- ✅ $1,200, international-$1,200

### Spell out acronyms the first time

Spell out the full form of an acronym the first time it is used in a text, followed by the acronym in brackets. Exception for universally known acronyms such as “US” or “UN”.

- ❌ An RCT showed significant results in the first phase of the study. But RCTs are not always enough to get political buy-in.
- ✅ A randomized controlled trial (RCT) showed significant results in the first phase of the study. But RCTs are not always enough to get political buy-in.

### Spelling of specific words

- **tonnes**, not tons. We always use what’s called the “tonne” or “metric ton” across OWID, equivalent to 1,000 kg — but we always write it as “tonne”. We never use the American “short ton” or “long ton”.
- **acknowledgments**, not acknowledgements.
- **sub-Saharan Africa**, not Sub-Saharan Africa.
- **COVID-19**, not Covid-19 or COVID.
- **The Internet**, not the internet.
- **vs.**, not vs
- **dataset**, not data set.
- **healthcare**, not health care.
- **well-being**, not wellbeing.
