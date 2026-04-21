---
status: new
---

# Visualizing global inequality

!!! info ""
    :octicons-person-16: **[Pablo Arriagada](https://ourworldindata.org/team/pablo-arriagada)** • :octicons-calendar-16: April 21, 2026 *(last edit)* • [**:octicons-mail-16: Feedback**](mailto:info@ourworldindata.org?subject=Feedback%20on%20technical%20publication%20-%20Visualizing%20global%20inequality)

## Introduction

In our [economic inequality page](https://ourworldindata.org/economic-inequality) and the article "Visualizing global inequality" ([TODO: article URL]), we include a bespoke interactive visualization that shows how income is distributed across countries and regions around the world. The tool lets readers compare income distributions side by side and explore how many people live below any chosen poverty line — from extreme poverty thresholds to the median income of a rich country.

This document is the technical companion to that visualization. It describes the data sources we rely on, the processing applied in our ETL pipeline, the methodological choices behind the smoothed distribution curves and the share-below-line computation, and the known limitations of the approach. The visualization itself is a bespoke component of the [`owid-grapher` repository](https://github.com/owid/owid-grapher); links to the relevant source code are gathered in the [References](#references) section at the end.

The inputs come from two different World Bank sources: the [Poverty and Inequality Platform (PIP)](https://pip.worldbank.org/), which publishes the binned global income distribution that is the backbone of the visualization, and the [World Development Indicators (WDI)](https://datatopics.worldbank.org/world-development-indicators/), from which we take supplementary Purchasing Power Parity factors and Consumer Price Index data for currency conversion.

## Data sources

### World Bank Poverty and Inequality Platform (PIP)

PIP provides the core inputs for this visualization: the income distribution data and the PPP factors used to express incomes in international dollars.

#### 1000-binned global distribution

The [1000-binned global distribution](https://datacatalog.worldbank.org/search/dataset/0064304/1000-binned-global-distribution) file contains, for each country and year, **1000 quantiles** (or "bins") of the average income or consumption per capita of the population. Each bin represents one thousandth of the country's population, ordered from the poorest 0.1% up to the richest 0.1%. Income is measured in **2021 [international dollars](https://ourworldindata.org/international-dollars)**, adjusted for differences in inflation and living costs between countries.

The data is available from **1990 until the year of the data release**, and is updated twice a year (typically around March and September), alongside the main PIP updates.

**Where the numbers come from.** PIP's distributions are derived from household surveys, where people are asked about their income or consumption. Because only a handful of countries run such surveys every year, PIP interpolates between survey years and extrapolates to the most recent year using estimates and projections of GDP per capita growth. See [PIP's methodology](https://datanalytics.worldbank.org/PIP-Methodology/lineupestimates.html) for the full procedure.

**Income vs. consumption.** PIP combines income and consumption surveys because not every country asks about the same welfare concept: most Western countries report income, while most countries in Africa and parts of Asia report consumption. A small number of countries report both. Comparing **inequality** estimates across the two concepts should be done with caution, since income is typically much more unequally distributed than consumption. Comparing **poverty** estimates is less problematic: most people in poverty are unable to save, so their income and consumption are close to identical. The 1000-binned file itself does not label each country-year series as income or consumption — that information is available in [PIP's country profiles](https://pip.worldbank.org/country-profiles) under "Data sources".

!!! warning "Top-income coverage"
    Survey data is not very good at capturing incomes at the top of the distribution. The richest are harder to reach, they are few, they are less likely to respond, and when they do respond, they tend to underreport — whether because of complex financial arrangements or fear of tax authorities. Other sources, such as the [World Inequality Database](https://wid.world/methodology/), attempt to correct for this, but PIP remains the only global dataset whose income concept matches what countries report in their own statistics and what people understand as "income".

**File structure.** The raw file is organized with columns identifying the country, the [World Bank region](https://datahelpdesk.worldbank.org/knowledgebase/articles/906519-world-bank-country-and-lending-groups), the quantile (from 1 to 1000), and the welfare indicator representing income or consumption per capita in that quantile. An additional column reports the population in each quantile — namely the country's total population divided by 1000, with population values coming from WDI (or, where WDI is unavailable, from the fallback sources listed in [PIP's methodology](https://datanalytics.worldbank.org/PIP-Methodology/lineupestimates.html#population)).

PIP's 1000-binned file covers **218 economies** across seven World Bank regions.

??? quote "Full list of 218 economies included (by region)"

    - **East Asia and Pacific**: American Samoa; Australia; Brunei Darussalam; Cambodia; China; Fiji; French Polynesia; Guam; Hong Kong SAR, China; Indonesia; Japan; Kiribati; Korea, Dem. Rep.; Korea, Rep.; Lao PDR; Macao SAR, China; Malaysia; Marshall Islands; Micronesia, Fed. Sts.; Mongolia; Myanmar; Nauru; New Caledonia; New Zealand; Northern Mariana Islands; Palau; Papua New Guinea; Philippines; Samoa; Singapore; Solomon Islands; Taiwan, China; Thailand; Timor-Leste; Tonga; Tuvalu; Vanuatu; Viet Nam.
    - **Europe and Central Asia**: Albania; Andorra; Armenia; Austria; Azerbaijan; Belarus; Belgium; Bosnia and Herzegovina; Bulgaria; Channel Islands; Croatia; Cyprus; Czechia; Denmark; Estonia; Faeroe Islands; Finland; France; Georgia; Germany; Gibraltar; Greece; Greenland; Hungary; Iceland; Ireland; Isle of Man; Italy; Kazakhstan; Kosovo; Kyrgyz Republic; Latvia; Liechtenstein; Lithuania; Luxembourg; Moldova; Monaco; Montenegro; Netherlands; North Macedonia; Norway; Poland; Portugal; Romania; Russian Federation; San Marino; Serbia; Slovak Republic; Slovenia; Spain; Sweden; Switzerland; Tajikistan; Turkmenistan; Türkiye; Ukraine; United Kingdom; Uzbekistan.
    - **Latin America and the Caribbean**: Antigua and Barbuda; Argentina; Aruba; Bahamas, The; Barbados; Belize; Bolivia; Brazil; British Virgin Islands; Cayman Islands; Chile; Colombia; Costa Rica; Cuba; Curaçao; Dominica; Dominican Republic; Ecuador; El Salvador; Grenada; Guatemala; Guyana; Haiti; Honduras; Jamaica; Mexico; Nicaragua; Panama; Paraguay; Peru; Puerto Rico (U.S.); Sint Maarten (Dutch part); St. Kitts and Nevis; St. Lucia; St. Martin (French part); St. Vincent and the Grenadines; Suriname; Trinidad and Tobago; Turks and Caicos Islands; Uruguay; Venezuela, RB; Virgin Islands (U.S.).
    - **Middle East, North Africa, Afghanistan and Pakistan**: Lebanon; Libya; Malta; Morocco; Oman; Pakistan; Qatar; Saudi Arabia; Syrian Arab Republic; Tunisia; United Arab Emirates; West Bank and Gaza; Yemen, Rep.
    - **North America**: Bermuda; Canada; United States.
    - **South Asia**: Bangladesh; Bhutan; India; Maldives; Nepal; Sri Lanka.
    - **Sub-Saharan Africa**: Angola; Benin; Botswana; Burkina Faso; Burundi; Cabo Verde; Cameroon; Central African Republic; Chad; Comoros; Congo, Dem. Rep.; Congo, Rep.; Côte d'Ivoire; Equatorial Guinea; Eritrea; Eswatini; Ethiopia; Gabon; Gambia, The; Ghana; Guinea; Guinea-Bissau; Kenya; Lesotho; Liberia; Madagascar; Malawi; Mali; Mauritania; Mauritius; Mozambique; Namibia; Niger; Nigeria; Rwanda; Senegal; Seychelles; Sierra Leone; Somalia; South Africa; South Sudan; Sudan; São Tomé and Príncipe; Tanzania; Togo; Uganda; Zambia; Zimbabwe.

#### PPP conversion factors used by PIP

PIP also publishes the **Purchasing Power Parity (PPP) conversion factors** used internally to express each country's income in international dollars. We use these as the primary source for the currency-conversion logic described later in this document, complemented by WDI's factors when the two disagree.

### World Development Indicators (WDI)

WDI contributes two additional series used to convert international dollar values into a user-selected local currency at current prices:

- [**PPP conversion factor, households and NPISHs Final consumption expenditure (LCU per international $)**](https://data.worldbank.org/indicator/PA.NUS.PRVT.PP) — indicator code `PA.NUS.PRVT.PP`. Used as a complement to PIP's PPP factors when the two sources disagree for a given country.
- [**Consumer price index (2010 = 100)**](https://data.worldbank.org/indicator/FP.CPI.TOTL) — indicator code `FP.CPI.TOTL`. Used to adjust values from 2021 prices to the latest year available in the series.

## Methodology

### 1000-binned distribution processing

Only minor processing is applied to the 1000-binned distribution in our ETL. The [garden step](https://github.com/owid/etl/blob/master/etl/steps/data/garden/wb/2026-03-25/thousand_bins_distribution.py) harmonizes country and region names against our internal reference, multiplies the per-bin population estimates by 1,000,000 (so the column lands in units of people rather than millions), and runs sanity checks to confirm that the per-quantile average income is **monotonically non-decreasing** within each country-year (each average must be greater than or equal to the average in the previous quantile). No monotonicity violations have been detected so far.

The raw snapshot lives at [`snapshots/wb/2026-03-25/thousand_bins_distribution.dta.dvc`](https://github.com/owid/etl/blob/master/snapshots/wb/2026-03-25/thousand_bins_distribution.dta.dvc).

### Kernel density plots

The visualization shows each country's income distribution as a **smoothed density curve** rather than a histogram. The curve is produced by feeding the 1000 per-bin averages, for a given country-year, through a kernel density estimator, with all inputs first converted to the log₂ scale.

!!! info "Why a log scale?"
    Income is strongly right-skewed — a handful of very high incomes pull the mean far above the median. On a linear axis, most of the distribution collapses into a spike near zero. A log scale turns multiplicative differences into equal visual distances, which matches how people experience income changes (a doubling of income matters similarly at $2/day and $200/day) and produces distribution curves that can be meaningfully compared across countries and years. Income also tends to follow a **log-normal distribution** — the logarithm of income is approximately normally distributed — so a log-axis curve is close to bell-shaped and easy to read.

We use a Gaussian [kernel density estimator](https://en.wikipedia.org/wiki/Kernel_density_estimation) via the [`fast-kde`](https://github.com/uwdata/fast-kde) JavaScript library with the following parameters:

| Parameter    | Value                       | Meaning                                                                                       |
| ------------ | --------------------------- | --------------------------------------------------------------------------------------------- |
| `bandwidth`  | `0.1` (on log₂ scale)       | Controls how smooth the resulting curve is. Smaller values follow the data more closely.      |
| `extent`     | `[log₂(0.25), log₂(1000)]`  | Range over which the density is evaluated, i.e., daily incomes from $0.25 to $1,000.          |
| `bins`       | `200`                       | Number of points at which the density is evaluated.                                           |

After the density is computed, we exponentiate the `x` coordinate labels (`2^x`) so the chart can plot them against income in international dollars on an intuitive axis.

### Share of population below a poverty line

From the 1000-binned distributions, we compute **the share of the population living below a chosen income line** for any combination of countries, regions, and the world aggregate.

Each country-year comes as a list of 1000 numbers describing the income distribution: the average income of the poorest 0.1% of the population, then the next 0.1%, and so on up to the richest 0.1%. Each of those slices — we'll call them **bins** — represents the same number of people: one thousandth of the country's population.

**For a single country**, the answer to "how many people earn less than $X a day?" is easy:

1. Count how many of the 1000 bins have an average income below $X. Call this count `k`.
2. Since each bin is one thousandth of the population, the share of people below the line is simply `k / 1000`, or equivalently `k / 10` as a percentage.

For example, if 250 of the 1000 bins in Brazil fall below the line, then roughly **25%** of Brazilians earn less than the line.

**For a region or the world**, we can't just average the country percentages — that would give India and Luxembourg equal weight. We need to weight by population: a country with more people contributes more to the aggregate. The rule we use is:

> share below line (region) = total people below the line across all countries in the region ÷ total population of the region

Concretely, for each country in the region, we compute the number of people below the line (`k × (country population ÷ 1000)`), add those up, and divide by the region's total population. The world aggregate works the same way, summing across all countries.

!!! note "Approximation"
    Because we only have 1000 bins rather than the full income distribution, we can't know exactly where the line falls *inside* a bin. In the worst case this miscounts about half a bin, i.e. 0.05 percentage points of the entity's population — well below the resolution shown in the chart (whole percentage points). Keeping the computation this simple also makes it fast enough to recompute every time the user drags the poverty-line slider.

### Currency

The chart lets the user switch **which currency** to display income in — international dollars (the default) or any of several local currencies like GBP, EUR, INR, etc.

#### What the underlying data is measured in

All the stored income values are in **international dollars per day**. International dollars are a synthetic currency, built from Purchasing Power Parity (PPP) adjustments, that make incomes comparable across countries by accounting for the fact that the same nominal dollar buys more in, say, Vietnam than in Switzerland. A person earning $5/day in international dollars has roughly the same material standard of living whether they live in Hanoi or Zurich — which they would not if we simply converted their local income using market exchange rates. You can find more information about international dollars in [our dedicated article](https://ourworldindata.org/international-dollars).

Using international dollars as the base unit also means a fixed poverty line (e.g. the World Bank's $3/day) has a consistent meaning across every country on the chart.

#### Converting to a local currency

When the user switches to a different currency, every income value is multiplied by a **conversion factor** specific to that currency. The factor is built upstream by an ETL step — [`int_dollar_conversions.py`](https://github.com/owid/etl/blob/master/etl/steps/data/external/owid_grapher/latest/int_dollar_conversions.py) — and fetched at runtime from the resulting JSON table, [`int_dollar_conversions.json`](https://owid-public.owid.io/marcel-bespoke-data-viz-02-2026/poverty-plots/int_dollar_conversions.json). The factor is the product of two components:

- **PPP factor** — converts international dollars (2021) to **local currency units (LCU) at 2021 prices**. Sourced from the World Bank, either from the [Poverty and Inequality Platform (PIP)](https://pip.worldbank.org/) or from [World Development Indicators (WDI)](https://databank.worldbank.org/source/world-development-indicators).
- **CPI factor** — inflates LCU values from 2021 prices to **the latest available year's prices** for that country, using CPI data from WDI (`fp_cpi_totl`). The factor is `CPI[latest] / CPI[2021]`, so it is country-specific both in magnitude and in which year "latest" refers to.

Multiplying an international dollar value by `PPP_factor × CPI_factor` therefore gives an amount in local currency at the latest available year's price level.

##### How the PPP factor is chosen

PIP and WDI publish PPP values derived from the same underlying data (the [International Comparison Program](https://www.worldbank.org/en/programs/icp)'s 2021 round), but their numbers don't always match exactly, because they apply different downstream methodologies. The ETL step reconciles the two as follows:

1. **If PIP and WDI agree within 3%**, use PIP — most of our poverty data comes from PIP, so aligning currency units with PIP keeps the whole pipeline internally consistent.
2. **If only one source has a value for a country**, use whichever has it. In practice, WDI covers a wider set of small territories (American Samoa, Cayman Islands, Greenland, Puerto Rico, etc.), while PIP is the only source for a handful of others (Taiwan, Venezuela, Yemen).
3. **If both have values but differ by more than 3%**, the country is manually adjudicated against a curated override list. Typical cases include:
    - **WDI preferred** when it reflects a recent currency redenomination that PIP hasn't caught up with — for example, the Belarusian ruble (2016, 1 BYN = 10,000 BYR), the Mauritanian ouguiya (2017, 1 MRU = 10 MRO), or Croatia's adoption of the Euro (2023).
    - **Country dropped** when the disagreement can't be resolved cleanly: large deviations in countries with complex currency histories (Zimbabwe, Liberia), ongoing currency transitions (Curaçao and Sint Maarten moving from the Antillean to the Caribbean Guilder in 2025), or unresolved multi-currency situations (Palestine).
    - **Country dropped** when the PIP entry represents only an urban or rural subpopulation rather than the whole country — e.g. the `(urban)` rows for Argentina, Bolivia, Colombia, Ecuador, Honduras, Suriname, and Uruguay, and the `(urban)`/`(rural)` rows for China, Ethiopia, and Rwanda. For Argentina and China, the country-level WDI value is used instead, so these countries remain on the chart; others without a WDI fallback are excluded entirely.

The list of manual overrides lives in the ETL step itself and is validated on every run: if a country newly falls outside the 3% agreement band and isn't in the list, the build fails. This forces us to make an explicit decision rather than silently absorbing new disagreements.

The UI uses IP-based country detection (via the [`detect-country.owid.io`](https://detect-country.owid.io/) service) to auto-suggest the user's local currency when the page loads, but the user can freely override it.

### Time interval

The chart also lets the user pick the time interval displayed. The raw data is per day. Switching to monthly or yearly multiplies every value by a fixed factor:

| Interval | Factor   |
| -------- | -------- |
| daily    | `1`      |
| monthly  | `365/12` |
| yearly   | `365`    |

Currency and time-interval factors are multiplicative, so they combine into a single `combinedFactor` that is applied once to each value before display.

### Colors

Each country, region, and the world aggregate has a color assigned to it. We use a local copy of Our World in Data's **Distinct Colors** palette, designed so that adjacent items in a chart remain visually separable.

#### Fixed colors

Two assignments are fixed:

- **World** is always shown in **Purple** (`#6d3e91`).
- The seven World Bank regional aggregates each map to a specific palette color. The mapping is stable so that, for example, Sub-Saharan Africa is always Mauve across the whole tool.

#### Country colors

For countries — where there are too many entities to pre-assign colors — we rotate through a **palette of seven colors** in the order the countries are received. The world aggregate, if present in the same selection, is always overridden back to Purple, so it stands out visually from any country that happens to land on the same slot.

## Limitations

- **Mixed income and consumption series.** As described in the [sources](#1000-binned-global-distribution) section, PIP's distributions combine series measured as either income or as consumption expenditure, depending on which concept each country reports in its household surveys. Because income is typically more unequally distributed than consumption, cross-country comparisons of inequality between income-based and consumption-based series should be read with care — part of the visible difference can reflect the welfare concept rather than a real inequality gap. Comparisons of poverty shares between the two types of series are less problematic, since people in poverty rarely save, and their income and consumption tend to coincide. The 1000-binned file does not label each country-year series by welfare concept; that information is available in [PIP's country profiles](https://pip.worldbank.org/country-profiles).
- **Top-income undercoverage.** Also described in the [sources](#1000-binned-global-distribution) section, the underlying data comes from household surveys, which do not accurately represent incomes at the top of the distribution due to underreporting, lower response rates, and the small number of very wealthy individuals.
- **Interpolated and extrapolated years.** For country-years with no survey available, PIP either interpolates between surveys or extrapolates using GDP per capita growth. In those years, the distribution and poverty estimates may not fully reflect the reality of that specific country-year; they should be read as levels of income compared against other countries in the region or globally, rather than as exact point estimates.
- **Smoothed curves.** The KDE curves are smoothed to show the shape of the distribution in general terms. The exact area under any portion of the curve does not necessarily correspond to the precise share of the population in that income range — the share-below-line figures (which come from the bin data directly, not from the smoothed curve) should be used for that.
- **CPI lags the current year.** The latest available year in WDI's CPI series is typically one or two years behind the present. Values shown in local currency are therefore in prices from one or two years ago, which is still a reasonable approximation of present-day monetary values in most economies.

## References

**Our World in Data source code**

- [Visualization component — `incomePlotUtils.ts`](https://github.com/owid/owid-grapher/blob/master/bespoke/projects/income-plots/src/utils/incomePlotUtils.ts) and the entry point at [`src/index.tsx`](https://github.com/owid/owid-grapher/blob/master/bespoke/projects/income-plots/src/index.tsx) and [`src/components/App.tsx`](https://github.com/owid/owid-grapher/blob/master/bespoke/projects/income-plots/src/components/App.tsx).
- [Currency conversions ETL step — `int_dollar_conversions.py`](https://github.com/owid/etl/blob/master/etl/steps/data/external/owid_grapher/latest/int_dollar_conversions.py).
- [1000-binned distribution garden step — `thousand_bins_distribution.py`](https://github.com/owid/etl/blob/master/etl/steps/data/garden/wb/2026-03-25/thousand_bins_distribution.py), and the raw [snapshot definition](https://github.com/owid/etl/blob/master/snapshots/wb/2026-03-25/thousand_bins_distribution.dta.dvc).

**External datasets and references**

- [World Bank Poverty and Inequality Platform (PIP)](https://pip.worldbank.org/)
- [PIP methodology](https://datanalytics.worldbank.org/PIP-Methodology/)
- [1000-binned global distribution dataset catalog](https://datacatalog.worldbank.org/search/dataset/0064304/1000-binned-global-distribution)
- [PPP conversion factor, households (WDI, `PA.NUS.PRVT.PP`)](https://data.worldbank.org/indicator/PA.NUS.PRVT.PP)
- [Consumer price index, 2010 = 100 (WDI, `FP.CPI.TOTL`)](https://data.worldbank.org/indicator/FP.CPI.TOTL)
- [International Comparison Program (ICP)](https://www.worldbank.org/en/programs/icp)
