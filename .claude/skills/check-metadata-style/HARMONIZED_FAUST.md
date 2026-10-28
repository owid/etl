# Harmonizing FAUST across OWID

> Source: [Harmonizing FAUST across OWID](https://docs.google.com/document/d/1tcknTryJAxOR0yv8x38s3jkONq-TY3T4HNCEcZQ_c-s/edit?tab=t.0) (Google Doc)
> Last synced from Google Doc: 2026-10-28
> Committed snapshot used by `/check-metadata-style`. Refresh it with step 1 of SKILL.md; don't edit the rules here by hand.

Some metrics are used many times over on OWID, so their FAUST (title, subtitle, footnote, units) must stay consistent. When an indicator is one of the metrics below, use these elements. On charts with several metrics, a title proposed here can become one sentence of the subtitle, with another sentence explaining the other metric.

Placeholders: `<X>` is the population, cause or indicator; `[year]` is the price or index year; `xx` is a number to fill in.

## Measures in various currencies and prices

Applies to every indicator measured in money: US$, other national currencies, international-$, constant or current prices.

Principles:

- The subtitle answers two questions: is the data adjusted for inflation (or not), and is it adjusted for differences in living costs between countries (or not).
- When the data covers only one country, don't mention living-cost adjustment in the subtitle.
- When the data covers only one year, don't mention inflation adjustment in the subtitle: there is no change over time for it to affect. (NOTE: local addition, not in the Google Doc yet. Keep it on refresh.)
- Don't put the index year, "constant", "current" or "international-$" in the subtitle. Put them in the footnote.
- For national currencies, name the currency in simple terms in the subtitle ("US dollars", "British pounds"), and give the technical description in the footnote.
- When we adjusted the data for inflation, the footnote says which deflator we used (e.g. "Data is expressed in constant US$ in 2020 prices, deflated using the US consumer price index.").
- Don't mention any of this in the title.

### Current national currency

- Subtitle (several countries): This data is expressed in [currency, e.g. US dollars]. It is not adjusted for inflation or differences in living costs between countries.
- Subtitle (one country): This data is expressed in [currency, e.g. US dollars]. It is not adjusted for inflation.
- Subtitle (one year, several countries): This data is expressed in [currency, e.g. US dollars]. It is not adjusted for differences in living costs between countries. (NOTE: local addition.)
- Long unit: current [currency], e.g. `current US$`
- Short unit: the currency symbol, e.g. `$`

### Constant national currency

- Subtitle (several countries): This data is expressed in [currency, e.g. US dollars]. It is adjusted for inflation but does not account for differences in living costs between countries.
- Subtitle (one country): This data is expressed in [currency, e.g. US dollars], adjusted for inflation.
- Long unit: constant [year] [currency], e.g. `constant 2019 US$`
- Short unit: the currency symbol, e.g. `$`
- Footnote: This data is expressed in constant [year] [currency], e.g. "constant 2019 US$".

### Constant international-$

- Subtitle: This data is adjusted for inflation and differences in living costs between countries.
- Long unit: international-$ in [year] prices, e.g. `international-$ in 2017 prices`
- Short unit: `$`
- Footnote: This data is expressed in [international-$](#dod:int_dollar_abbreviation) at [year] prices.
- For PIP income or consumption data, add to the footnote: Depending on the country and year, the data relates to income measured after taxes and benefits, or to consumption, [per capita](#dod:per-capita).
- When the inflation indexing year differs from the ICP round (e.g. WID), use the inflation indexing year as [year]. Leave the details to the longer technical texts.

### Current international-$

- Subtitle: This data is adjusted for differences in living costs between countries but not for inflation.
- Long unit: `current international-$`
- Short unit: `$`
- Footnote: This data is expressed in current [international-$](#dod:int_dollar_abbreviation).

## Share of GDP

Most "% of GDP" indicators are not part of GDP: GDP is a separate variable used to scale for the size of the economy, as population is in "per capita". So always write the long unit as "% of GDP", never just "%", especially where a reader could assume part-to-whole (public spending).

- Subtitle: <indicator> divided by [gross domestic product](#dod:gdp), expressed as a percentage.
- Long unit: `% of GDP`
- Short unit: `%`

## GDP and GDP per capita (PPP-adjusted)

Follows the currency rules above.

- Title: Gross domestic product (GDP) / GDP per capita
- Subtitle: This data is adjusted for inflation and differences in living costs between countries.
- Footnote: This data is expressed in [international-$](#dod:int_dollar_abbreviation) at [year] prices.
- Footnote (Penn World Table): This data is expressed in [international-$](#dod:int_dollar_abbreviation) at 2017 prices, using multiple benchmark years to adjust for differences in the cost of living between countries over time.
- With another variable and little space (e.g. scatter plots): no subtitle; footnote: GDP per capita is measured in [international-$](#dod:int_dollar_abbreviation) at [year] prices to account for inflation and differences in living costs between countries.

## Education

### Average years of schooling

- Title: Average years of schooling for <X>
- Subtitle: Average number of years <X> participated in formal education.
- Footnote: Formal education is primary/ISCED 1 or higher. This does not include years spent repeating grades.

### Expected years of schooling

- Title: Expected years of schooling
- Subtitle: Number of years a child of school entrance age can expect to receive if the current age-specific enrollment rates persist throughout the child’s years of schooling.

## Energy and environment

### CO2 equivalents

- Title: don't mention it.
- Subtitle: Emissions are measured in carbon dioxide equivalents (CO2eq). This means non-CO2 gases are weighted by the amount of warming they cause over a 100-year timescale.
- Long unit: `tonnes CO2eq` (total) or `kilograms CO2eq` (per capita)
- Short unit: `t` (total) or `kg` (per capita)
- Footnote: Greenhouse gases are weighted by their global warming potential value (GWP100). GWP100 measures the relative warming impact of one molecule of a greenhouse gas, relative to carbon dioxide, over 100 years.

## Demography

### Fertility rate

- Title: Fertility rate: births per woman
- Subtitle: The [total fertility rate](#dod:fertility-rate) summarizes the total number of births a woman would have if she experienced the birth rates seen in women of each age group in one particular year across her childbearing years.
- Short subtitle (scatter plots): The [total fertility rate](#dod:fertility-rate) in a given year.

### Population density

- Subtitle: Number of people per km² of land area.

## Health

### Age-standardized

- Title: don't mention age standardization.
- Footnote: To allow for comparisons between countries and over time, this metric is [age-standardized](#dod:age_standardized).

### Basic handwashing facilities

- Title: Number/Share of <X> with/without access to basic handwashing facilities
- Footnote: Access to basic handwashing facilities refers to a device to facilitate handwashing with soap and water available on the premises. Handwashing facilities may be fixed or mobile, and include a sink with tap water, buckets with taps, tippy-taps, and jugs or basins designated for handwashing.

### DALYs

- Subtitle: [Disability-Adjusted Life Years (DALYs)](#dod:dalys) per 100,000 individuals from <X>. DALYs measure the total burden of disease – both from years of life lost due to premature death and years lived with a disability. One DALY equals one year of healthy life.

### Deaths

Risk-factor deaths are model estimates, so their subtitle must say "estimated".

- Title: <X> deaths (causes of death); Deaths attributed to <X> (risk factors)
- Subtitle (risk factors): Estimated annual number of deaths attributed to <X>.
- Subtitle (modeled causes, e.g. Global Burden of Disease, Global Health Estimates): Estimated annual number of deaths from <X>.
- Subtitle (causes that are accurately recorded): Annual number of deaths from <X>.

### Death rates

Put the cause first ("<X> death rate", not "Death rate from <X>"), so titles are easy to scan. "Death rate" is singular.

- Title: <X> death rate (causes of death); Rate of deaths attributed to <X> (risk factors)
- Subtitle (risk factors): Estimated annual number of deaths attributed to <X> per 100,000 people.
- Subtitle (modeled causes): Estimated annual number of deaths from <X> per 100,000 people.
- Subtitle (accurately recorded causes): Annual number of deaths from <X> per 100,000 people.

### Death rates in age groups

- Title: <X> death rate in children under five; <X> death rate in over-70s. Spell out numbers under ten, digits above ten.
- Subtitle (risk factors): Estimated annual number of deaths in <age group> attributed to <X> per 100,000 people in the same age group.
- Subtitle (modeled causes): Estimated annual number of deaths in <age group> from <X> per 100,000 people in the same age group.
- Subtitle (accurately recorded causes): Annual number of deaths in <age group> from <X> per 100,000 people in the same age group.

### Under-<X> deaths and mortality rate

- Write the age as a digit: "age of 5", not "age of five".
- When the title already contains the full definition (e.g. "Number of children who die before age 5"), no subtitle is needed.
- Mortality rates are always percentages, not rates per 1,000 or 100,000.
- Subtitle (rate): Share of newborns who die before age <5>.
- Long and short unit: `%`

### Improved sanitation facilities

- Title: Number/Share of <X> using/not using improved sanitation facilities
- Footnote: An 'improved sanitation facility' includes flush/pour flush (to piped sewer system, septic tank, pit latrine), ventilated improved pit latrine, pit latrine with slab, and composting toilet.

### Improved water source

- Title: Number/Share of <X> using/not using an improved water source
- Footnote: An 'improved water source' includes piped water on premises and other improved drinking water sources: public taps or standpipes, tube wells or boreholes, protected dug wells, protected springs, and rainwater collection.

### Life expectancy

- Title: Life expectancy
- Subtitle: The [period life expectancy](#dod:period-life-expectancy) is the average age to which a newborn can expect to live, if age-specific mortality rates in the current year were to stay the same throughout their life.

### Maternal mortality

- Subtitle: Number/share/rate of women who die from pregnancy-related causes (...)
- Footnote: A maternal death refers to the death of a woman from any cause related to, or aggravated by pregnancy (excluding accidental or incidental causes). It can occur during pregnancy, during childbirth, or within 42 days of termination of pregnancy.

### Stunting

- Title: Number/Share of <X> who are stunted
- Subtitle: Number/share of <X> who are stunted – significantly shorter than the average for their age. This is a common consequence of poor nutrition or repeated infection.
- Footnote: Stunting in children is defined as being less than two standard deviations below the median height for their age.

### Undernourishment

- Title: Number/Share of <X> who are undernourished
- Subtitle: Number/share of <X> who have a habitual energy intake that is lower than their requirements to live a normal and healthy life.
- Footnote: Food supply is estimated based on the average supply of calories per person, and levels of food inequality within a population. Requirements are calculated for a population taking the distribution of age, gender, height, weight and activity levels into account.

### Wasting

- Title: Number/Share of <X> who are wasted
- Subtitle: Number/share of <X> who are 'wasted' – whose weight is significantly lower than the average for their height, as a consequence of acute food shortage or disease.
- Footnote: Wasting in children is defined as having a weight less than two standard deviations below the median for their height.

## Poverty

The PIP income footnote, used below as "[PIP footnote]": Depending on the country and year, the data relates to income measured after taxes and benefits, or to consumption, [per capita](#dod:per-capita).

### Extreme poverty (World Bank)

- Title: Share of population living in extreme poverty / Number of people living in extreme poverty
- Subtitle: Extreme poverty is defined as living below the International Poverty Line of $2.15 per day. This data is adjusted for inflation and differences in the cost of living between countries.
- Footnote: This data is expressed in [international-$](#dod:int_dollar_abbreviation) at 2017 prices. [PIP footnote]
- With another variable (scatter plots): subtitle without the adjustment sentence; footnote: This data is measured in [international-$](#dod:int_dollar_abbreviation) at 2017 prices to account for inflation and differences in the cost of living between countries. It relates to income measured after taxes and benefits, or to consumption, [per capita](#dod:per-capita).

### Extreme poverty (historical regional data, Moatsos)

- Title: as for the World Bank.
- Subtitle: Extreme poverty is defined as living below the International Poverty Line of $1.90 per day. This data is adjusted for inflation and differences in the cost of living between countries.
- Footnote: Data after 1981 relates to household income or consumption surveys collated by the World Bank; before 1981 it is based on historical reconstructions of GDP per capita and inequality data. Data is measured in [international-$](#dod:int_dollar_abbreviation) at 2011 prices.
- Number version: the survey sentence moves to the subtitle, and the footnote reads: This data is expressed in [international-$](#dod:int_dollar_abbreviation) at 2011 prices to account for inflation and differences in the cost of living between countries.

### Extreme poverty (cost of basic needs)

- Title: as for the World Bank.
- Subtitle: This data is calculated based on a ‘cost of basic needs’ approach. It represents the share of the population (number of people) unable to meet basic needs (including minimal nutrition and adequately heated shelter) according to prices of locally-available goods and services at the time.

### Other poverty lines

- Title: Poverty: Share of population (Number of people) living on less than $xx a day
- Subtitle: This data is adjusted for inflation and differences in the cost of living between countries.
- Subtitle (international lines): The poverty line of $3.65/$6.85 per day is set by the World Bank to be representative of the definitions of poverty adopted in lower-middle-income/upper-middle-income countries. This data is adjusted for inflation and differences in the cost of living between countries.
- Footnote: This data is expressed in [international-$](#dod:int_dollar_abbreviation) at 2017 prices. Depending on the country and year, it relates to income measured after taxes and benefits, or to consumption, [per capita](#dod:per-capita).
- Historical regional data (Moatsos 2021): as above, with the Moatsos footnote and 2011 prices.

### Relative poverty

- Title: Relative poverty: Share (Number) of people below xx% of median income
- Subtitle: Relative poverty is measured in terms of a poverty line that rises and falls over time with average incomes – in this case set at xx% of median income.
- Footnote: [PIP footnote]

### Total shortfall

- Title: Total shortfall from extreme poverty / from a poverty line of $xx.xx a day
- Subtitle: This is the amount of money that would be theoretically needed to lift the incomes of all people in extreme poverty up to the International Poverty Line of $2.15 a day (up to $xx.xx a day). This data is adjusted for inflation and differences in the cost of living between countries.
- Footnote: This data is expressed in [international-$](#dod:int_dollar_abbreviation) at 2017 prices. The cost of closing the poverty gap does not take into account costs and inefficiencies from making the necessary transfers.

### Average shortfall ($)

- Title: Average shortfall from extreme poverty / from a poverty line of $xx.xx a day
- Subtitle: This is the amount of money that would be theoretically needed to lift the incomes of all people in extreme poverty up to the International Poverty Line of $2.15 a day, averaged across the population in poverty.
- Footnote: This data is measured in [international-$](#dod:int_dollar_abbreviation) at 2017 prices to account for inflation and differences in the cost of living between countries. It relates to income measured after taxes and benefits, or to consumption, [per capita](#dod:per-capita).

### Average shortfall (%)

- Title: Average shortfall from extreme poverty (as a share of the poverty line)
- Subtitle: This is the average shortfall expressed as a share of the poverty line, sometimes called the 'income gap ratio'. It captures the depth of poverty of those living on less than the International Poverty Line ($2.15 a day).
- Footnote: as for the average shortfall in $.

### Poverty gap index

- Title: Poverty gap index at $xx.xx a day
- Subtitle: The poverty gap index is a poverty measure that reflects both the prevalence and the depth of poverty. It is calculated as the share of population in poverty multiplied by the average shortfall from the poverty line (expressed as a % of the poverty line).
- Footnote: as for the average shortfall in $.

### Shared prosperity (World Bank)

- Title: Annual growth of the income or consumption of the poorest 40%
- Subtitle: The growth rate is calculated between two household surveys – the most recent survey available in [year] and a survey falling approximately five years earlier.
- Footnote: [PIP footnote]

### Multidimensional Poverty Index (MPI)

- Title: Multidimensional Poverty Index (MPI) / Share of population in multidimensional poverty / Intensity of multidimensional poverty
- Subtitle: Multidimensional poverty is defined as being deprived in a range of health, education and living standards indicators. Then, for the index: The MPI is a measure that reflects both the prevalence and the intensity of multidimensional poverty. For intensity: The intensity is the share of indicators in which the multidimensionally poor are deprived on average.
- Footnote ("current margin estimates"): Estimates based on the most recent household survey data (between [year] and [year]). A [separate dataset](link) is produced to monitor trends over time.
- Footnote ("harmonized over time"): This data aims to provide estimates that are comparable over time within individual countries. A [separate dataset](link) provides more complete data to be used for comparisons across countries at a point in time.

## Inequality and incomes across the distribution

Common rules:

- Say which income the title shows: nothing for PIP, "(before tax)" for income before tax, "(after tax)" for income after tax.
- Subtitle addition: nothing for PIP; "Inequality is measured here in terms of income before (after) taxes and benefits." or "Income here is measured before (after) taxes and benefits."
- Footnote for WID pre-tax national income: Income is measured before payment of taxes and non-pension benefits, but after the payment of public and private pensions.
- Footnote for PIP: [PIP footnote]
- Footnote for equivalized LIS income: Income has been [equivalized](#dod:equivalization).
- Money amounts follow the currency rules: subtitle "This data is adjusted for inflation and differences in the cost of living between countries."; footnote "This data is expressed (measured) in [international-$](#dod:int_dollar_abbreviation) at [year] prices (to account for inflation and differences in the cost of living between countries)."
- In explorers, add "per day", "per month" or "per year" to income titles as selected.

### Gini coefficient

Say which distribution it measures (income, wealth, life expectancy, education).

- Title: Income inequality: Gini coefficient ("Income inequality" can be dropped in explorers where the context gives it)
- Subtitle: The [Gini coefficient](#dod:gini) measures inequality on a scale from 0 to 1. Higher values indicate higher inequality.

### Palma ratio

- Title: Income inequality: Palma ratio
- Subtitle: The Palma ratio is a measure of inequality that divides the share received by the richest 10% by the share of the poorest 40%. Higher values indicate higher inequality.

### Shares of the richest and poorest

- Title: Income share of the richest 10% (1%, 0.1%); Wealth share of the richest 10% (WID wealth); Income share of the poorest 50%
- Subtitle: Share of income received by the richest 10% (poorest 50%) of the population. For wealth: The share of wealth owned by the richest 10% of the population. Wealth is defined as the total value of non-financial and financial assets (housing, land, deposits, bonds, equities, etc.) held by households, minus their debts.

### Relative poverty (inequality explorers, 50% of the median only)

- Title: Share of people in relative poverty
- Subtitle (PIP): The share of population with after tax income or consumption below 50% of the median. Relative poverty reflects the extent of inequality within the bottom of the distribution.
- Subtitle (before/after tax): The share of the population with income below 50% of the median. Relative poverty reflects the extent of inequality within the bottom of the distribution. Income here is measured before (after) taxes and benefits.

### Other inequality measures

- Mean log deviation. Title: Income inequality: Mean log deviation. Subtitle: The mean log deviation (MLD) is a measure of inequality. An MLD of zero indicates perfect equality and it takes on larger positive values as incomes become more unequal.
- Polarization index. Title: Income inequality: Polarization index. Subtitle: The polarization index, also known as the Wolfson polarization index, measures the extent to which the distribution of income or consumption is “spread out” and bi-modal. Like the Gini coefficient, the polarization index ranges from 0 (no polarization) to 1 (complete polarization).
- P90/P10 ratio. Title: Income inequality: P90/P10 ratio. Subtitle: P90 and P10 are the levels of income or consumption below which 90% and 10% of the population live, respectively. This variable gives the ratio of the two. It is a measure of inequality that indicates the gap between the richest and poorest tenth of the population.
- P90/P50 ratio. Title: Income inequality: P90/P50 ratio. Subtitle: The P90/P50 ratio measures the degree of inequality within the richest half of the population. A ratio of 2 means that someone just falling in the richest tenth of the population has twice the median income or consumption.
- P50/P10 ratio. Title: Income inequality: P50/P10 ratio. Subtitle: The P50/P10 ratio measures the degree of inequality within the poorest half of the population. A ratio of 2 means that the median income or consumption is two times higher than that of someone just falling in the poorest tenth of the population.

### Mean and median income

- Title: Mean (Median) income or consumption per day (PIP); Mean (Median) income (other sources)
- Subtitle: This data is adjusted for inflation and differences in the cost of living between countries.
- Footnote: This data is expressed in [international-$](#dod:int_dollar_abbreviation) at [year] prices.

### Mean income by decile

- Title: Mean income or consumption per day within the poorest/richest/xxth decile (PIP); Mean income within the poorest/richest/xxth decile (other sources)
- Subtitle (PIP): The mean after tax income or consumption per person per day within the poorest/richest/xxth decile (tenth of the population).
- Subtitle (other sources): The mean income per year within the poorest/richest/xxth decile (tenth of the population).
- Footnote: This data is measured in [international-$](#dod:int_dollar_abbreviation) at [year] prices to account for inflation and differences in the cost of living between countries.

### Decile thresholds

- Title: Threshold income or consumption per day marking the poorest/richest/xxth decile (PIP); Threshold income marking the poorest/richest/xxth decile (other sources). For the median: "… marking the 5th decile (median)". All together: Threshold income (or consumption per day) for each decile.
- Subtitle (PIP): The level of after tax income or consumption per person per day below which x0% (10%, 20%, 30%, etc.) of the population falls.
- Subtitle (other sources): The level of income per day below which x0% (10%, 20%, 30%, etc.) of the population falls.
- Footnote: as for mean income by decile.

### Income shares by decile

- Title: Income or consumption share of the poorest/richest/xxth decile (PIP); Income share of the poorest/richest/xxth decile (other sources)
- Subtitle (PIP): The share of after tax income or consumption received by the poorest/richest/xxth decile (tenth of the population).
- Subtitle (other sources): The share of income received by the poorest/richest/xxth decile (tenth of the population).

## Government spending

- Title: <X> spending as a share of total government spending
