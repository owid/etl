---
status: new
---

# Visualizing global inequality

!!! info ""
    :octicons-person-16: **[Pablo Arriagada](https://ourworldindata.org/team/pablo-arriagada), [Bertha Rohenkohl](https://ourworldindata.org/team/bertha-rohenkohl), [Marcel Gerber](https://ourworldindata.org/team), [Joe Hasell](https://ourworldindata.org/team/joe-hasell)** • :octicons-calendar-16: April 21, 2026 *(last edit)* • [**:octicons-mail-16: Feedback**](mailto:info@ourworldindata.org?subject=Feedback%20on%20technical%20publication%20-%20Visualizing%20global%20inequality)

## Introduction

This document describes how our team produced an interactive visualization of global income distributions, presented in our article "Visualizing global inequality" (TODO: add link).

Most available tools focus on inequality within individual countries. This tool brings together inequalities within countries and between countries into a single view, letting users compare countries' and regions' income distributions side by side and explore how many people live below any chosen income threshold.

Here, we cover the data sources, data processing, the key methodological choices, and the known limitations of the approach.

!!! note
    The interactive visualization is a bespoke component of the [`owid-grapher` repository](https://github.com/owid/owid-grapher). You can browse the code here (TODO: add link).

## Data source: Global incomes data from the World Bank

The data comes from the World Bank. More specifically, we rely on the following dataset:

* [1000 binned global distribution](https://datacatalog.worldbank.org/search/dataset/0064304/1000-binned-global-distribution) from the [Poverty and Inequality Platform (PIP)](https://pip.worldbank.org/) — contains data on the global income distribution from 1990 to the present, which is the backbone of the visualization.

The [1000 binned global distribution](https://datacatalog.worldbank.org/search/dataset/0064304/1000-binned-global-distribution) file contains, for each country and year, 1000 quantile groups (or "bins") of the average income (or consumption) per capita of the population. Each bin represents one thousandth of the country's population, ordered from bin number 1 (the poorest 0.1%) to bin number 1000 (the richest 0.1%).

Income is expressed in 2021 [international dollars](https://ourworldindata.org/international-dollars), and is adjusted for inflation and differences in living costs between countries.

The data is available from 1990 until the year of the latest data release.

**Where does the income data come from?**  The most common source of data on the incomes of people around the world is household surveys — studies that ask thousands of households in a country how much they earn or spend. These income and consumption expenditure surveys are conducted at the country level, and the World Bank collects and standardizes this data through the Poverty and Inequality Platform (PIP).

Some technical aspects of the data are worth highlighting:

* Because not all countries run household surveys every year, PIP fills the gaps: it interpolates between survey years and extrapolates to the present. These estimates are based on the assumption that incomes or consumption expenditures grow in line with the growth rates observed in national accounts data. For more details, see Chapter 5 of the Poverty and Inequality Platform [Methodology Handbook](https://datanalytics.worldbank.org/PIP-Methodology/lineupestimates.html).
* The dataset combines income and consumption expenditure surveys because not every country asks about the same welfare concept. In high-income countries, the surveys typically capture people's incomes, while in low- and middle-income countries, they measure consumption expenditure — what households spend on goods and services. A small number of countries report both. The two concepts are closely related but not the same: income equals consumption plus savings. Therefore, the global data is a mix of income and consumption expenditure data, making inequality estimates less comparable across countries that use different measures. The 1000 binned file itself does not label each country-year series as income or consumption, although that information is available in [PIP's country profiles](https://pip.worldbank.org/country-profiles) under "Data sources".
* In many poorer countries, a large share of people don't have any monetary income — they grow food for their own use or trade goods and services outside of markets. This data accounts for that by adding the estimated value of non-monetary income and home production.

!!! warning "Top-income coverage"
    Survey data is not very good at capturing incomes at the top of the distribution. The extremely rich are few in number, they are harder to reach, and less likely to participate in surveys even when contacted. And even when they participate, surveys tend to undercount their income — particularly income received from capital, business ownership, and complex financial arrangements. Other sources, such as the [World Inequality Database](https://wid.world/methodology/), attempt to adjust for this by combining surveys with data from tax records and national accounts. But PIP is the only global dataset covering this many countries with a consistent concept that most closely matches what countries report in their own statistics and what people would think of as "income".

### Data structure

The raw file is organized with columns identifying the country, the [World Bank region](https://datahelpdesk.worldbank.org/knowledgebase/articles/906519-world-bank-country-and-lending-groups), the quantile (from 1 to 1000), and the welfare indicator representing the average daily income or consumption per capita in that quantile. An additional column reports the population in each quantile (in millions of people) — namely, the country's total population divided by 1000. Population values come from the World Development Indicators (or, where it is unavailable, from the fallback sources listed in [PIP's methodology](https://datanalytics.worldbank.org/PIP-Methodology/lineupestimates.html#population)).

This data covers **218 economies** across seven World Bank regions.

??? note "Full list of 218 economies included (by World Bank region)"

    - **East Asia and Pacific**: American Samoa; Australia; Brunei Darussalam; Cambodia; China; Fiji; French Polynesia; Guam; Hong Kong SAR, China; Indonesia; Japan; Kiribati; Korea, Dem. Rep.; Korea, Rep.; Lao PDR; Macao SAR, China; Malaysia; Marshall Islands; Micronesia, Fed. Sts.; Mongolia; Myanmar; Nauru; New Caledonia; New Zealand; Northern Mariana Islands; Palau; Papua New Guinea; Philippines; Samoa; Singapore; Solomon Islands; Taiwan, China; Thailand; Timor-Leste; Tonga; Tuvalu; Vanuatu; Viet Nam.
    - **Europe and Central Asia**: Albania; Andorra; Armenia; Austria; Azerbaijan; Belarus; Belgium; Bosnia and Herzegovina; Bulgaria; Channel Islands; Croatia; Cyprus; Czechia; Denmark; Estonia; Faeroe Islands; Finland; France; Georgia; Germany; Gibraltar; Greece; Greenland; Hungary; Iceland; Ireland; Isle of Man; Italy; Kazakhstan; Kosovo; Kyrgyz Republic; Latvia; Liechtenstein; Lithuania; Luxembourg; Moldova; Monaco; Montenegro; Netherlands; North Macedonia; Norway; Poland; Portugal; Romania; Russian Federation; San Marino; Serbia; Slovak Republic; Slovenia; Spain; Sweden; Switzerland; Tajikistan; Turkmenistan; Türkiye; Ukraine; United Kingdom; Uzbekistan.
    - **Latin America and the Caribbean**: Antigua and Barbuda; Argentina; Aruba; Bahamas, The; Barbados; Belize; Bolivia; Brazil; British Virgin Islands; Cayman Islands; Chile; Colombia; Costa Rica; Cuba; Curaçao; Dominica; Dominican Republic; Ecuador; El Salvador; Grenada; Guatemala; Guyana; Haiti; Honduras; Jamaica; Mexico; Nicaragua; Panama; Paraguay; Peru; Puerto Rico (U.S.); Sint Maarten (Dutch part); St. Kitts and Nevis; St. Lucia; St. Martin (French part); St. Vincent and the Grenadines; Suriname; Trinidad and Tobago; Turks and Caicos Islands; Uruguay; Venezuela, RB; Virgin Islands (U.S.).
    - **Middle East, North Africa, Afghanistan and Pakistan**: Lebanon; Libya; Malta; Morocco; Oman; Pakistan; Qatar; Saudi Arabia; Syrian Arab Republic; Tunisia; United Arab Emirates; West Bank and Gaza; Yemen, Rep.
    - **North America**: Bermuda; Canada; United States.
    - **South Asia**: Bangladesh; Bhutan; India; Maldives; Nepal; Sri Lanka.
    - **Sub-Saharan Africa**: Angola; Benin; Botswana; Burkina Faso; Burundi; Cabo Verde; Cameroon; Central African Republic; Chad; Comoros; Congo, Dem. Rep.; Congo, Rep.; Côte d'Ivoire; Equatorial Guinea; Eritrea; Eswatini; Ethiopia; Gabon; Gambia, The; Ghana; Guinea; Guinea-Bissau; Kenya; Lesotho; Liberia; Madagascar; Malawi; Mali; Mauritania; Mauritius; Mozambique; Namibia; Niger; Nigeria; Rwanda; Senegal; Seychelles; Sierra Leone; Somalia; South Africa; South Sudan; Sudan; São Tomé and Príncipe; Tanzania; Togo; Uganda; Zambia; Zimbabwe.

### Data processing

We import the data and harmonize country and region names against our internal reference, convert population estimates from millions to persons, and run sanity checks to confirm that there are no negative values for the average income, and that it is monotonically non-decreasing for each country (that is, each average income must be greater than or equal to the average of the previous quantile). We haven't detected any violations in the current data.

```py
def sanity_checks(tb: Table) -> Table:
    """
    Check that there are no negative values for avg.
    Check that data is monotonically increasing in quantile.
    """
    # Check that there are no negative values for avg.

    mask = tb["avg"] < 0
    if not tb[mask].empty:
        paths.log.info(f"There are {len(tb[mask])} negative values for avg and will be transformed to zero.")
        tb["avg"] = tb["avg"].clip(lower=0)

    # Check that data is monotonically increasing in avg by country, year and quantile.
    tb = tb.sort_values(by=["country", "year", "quantile"]).reset_index(drop=True)

    mask = tb.groupby(["country", "year"])["avg"].diff() < 0

    if not tb[mask].empty:
        paths.log.info(f"There are {len(tb[mask])} values for avg that are not monotonically increasing.")
        paths.log.info("These values will be transformed to the previous value.")

        tb.loc[mask, "avg"] = tb.groupby(["country", "year"])["avg"].shift(1).loc[mask]

    return tb
```

## Plotting income distributions

### Estimating the shape of the income distributions

The data for each country is a list of 1000 average daily income values (for each bin). To turn these data points into the smooth income distribution curves shown in the visualization, we use a technique called [kernel density estimation (KDE)](https://en.wikipedia.org/wiki/Kernel_density_estimation).

The curve is produced by feeding the 1000 averages for a given country-year through a KDE function, with all incomes first converted to a logarithmic scale.

!!! info "Working in log scale"
    Income data spans several orders of magnitude — from less than a dollar a day to thousands. To handle this range, we work with the logarithm (base 2) of each income value. In log space, the distance between $1 and $2 is the same as between $50 and $100, which gives the lower end of the distribution the same visual resolution as the upper end.

Three parameters control the shape of this curve: the smoothness of the estimate (**bandwidth**), the income range it covers (**extent**), and the resolution of the output (**bins**). They are defined as follows:

```ts
// KDE_BANDWIDTH controls how smooth the resulting curve is. Smaller values follow the data more closely.
export const KDE_BANDWIDTH = 0.1
// KDE_EXTENT is the range over which the density is evaluated, i.e., daily incomes from $0.25 to $1,000.
export const KDE_EXTENT = [0.25, 1000].map(Math.log2) as [number, number]
// KDE_NUM_BINS is the number of points at which the density is evaluated.
export const KDE_NUM_BINS = 200
```

We then use the [`fast-kde`](https://github.com/uwdata/fast-kde) JavaScript library with the same parameters (for all countries in the data):

```ts
export function kdeLog(pointsLog2: number[]) {
    const k = fastKde.density1d(pointsLog2, {
        bandwidth: KDE_BANDWIDTH,
        extent: KDE_EXTENT,
        bins: KDE_NUM_BINS,
    })
    return [...k.points()].map((p) => ({
        ...p,
        x: Math.pow(2, p.x),
    })) as Array<{ x: number; y: number }>
}
```

After the density is computed in log space, the x-values are converted back to dollar amounts (by computing 2^x), so the curve can be plotted directly against income in international dollars.

#### How sensitive are the results to the choice of parameters? [in progress]

* Bandwidth: (TBC with Marcel)
* Extent:
    * No values are below int.-$ 0.25 a day
    * 30 countries have bins with average income values higher than int.-$ 1000 a day; with most in the richest 0.1%. These countries have a combined population of ~592M but only roughly 611k people (about 0.1% of the combined population) fall into bins whose average income exceeds this threshold. Visually, this trims the very tip of the right tail for those distributions.
* Bins: (TBC with Marcel)
    * We do not test the number of bins (200); this controls only the resolution of the curve, and values above ~100 produce visually identical results. - is this true?

### Calculating the share of the population below a given income threshold

We can now compute the share of the population living below a chosen poverty line (or any income threshold) for any combination of countries, regions, and the world.

For each country, we find the highest bin in the 1000-bin distribution whose average income falls (strictly) below the chosen line. That position tells us how many bins — and therefore how many people — are below it. Dividing by the country's total population gives the share of the population below that line. For example, if 250 of the 1000 bins in Brazil fall below a chosen line, then 25% of Brazilians are estimated to live below it.

For regions and the world aggregate, we weight by population. We sum the number of people below the line across all constituent countries and divide by the total population of the group.

!!! info "How accurate is this?"
    Because we have 1000 bins rather than the full income distribution, we can't know exactly where the line falls inside a bin — each bin is assigned entirely above or below the line based on its average income. A bin whose average is just below the line might contain some people who are above it, and vice versa. In the worst case, this is off by half a bin, i.e., 0.05 percentage points of the population. Keeping the computation this simple also makes it fast enough to recompute every time the user chooses a different line.

    We cross-checked the shares shown here against PIP's own extrapolated poverty series at 2021 international dollars for the most recent year available, for seven lines between $3 and $40 / day. At the country, regional, and global levels, every comparison agrees with PIP to within 0.1 percentage points — the full precision the 1000-bin data allows. Since the visualization shows poverty shares without decimal points, these minor differences are not visible on the chart.

## Currency conversions

The income data we use is originally expressed in constant 2021 international dollars per day, as we mentioned above. This means the data is adjusted for inflation and for differences in living costs between countries.

International dollars are a synthetic currency that adjusts for differences in living costs between countries, so that incomes are comparable globally. They are constructed using Purchasing Power Parity (PPP) conversion factors, which measure how many units of a country's local currency are needed to buy the same basket of goods that one US dollar would buy in the United States. You can read more about it in our article [What are international dollars?](https://ourworldindata.org/international-dollars)

To let users view values and explore the chart in their local currency (for example, British pounds, US dollars, or Euros), we compute a conversion factor for each country that converts 2021 international dollars into local currency units at recent prices.

!!! info "What changes when you switch currency, and what doesn't?"
    Switching currency multiplies every income value by the same conversion factor — it rescales the axis, but doesn't change the shape of any distribution or the relative position of countries. The underlying PPP adjustment remains unchanged. The data is already adjusted for living costs between countries, so switching the view to euros or pounds doesn't undo that — the chart just shows you values in a different currency.

#### Data sources

We need two datasets from the World Bank: a Purchasing Power Parity (PPP) conversion factor to go from international dollars to local currency, and a price index to adjust for inflation.

For the Purchasing Power Parity factors (PPPs):

* Our primary source is the [World Bank's Poverty and Inequality Platform](https://pip.worldbank.org/) (PIP) — the same source used for the income distributions above — which publishes the conversion factors it uses internally to translate local currency into 2021 international dollars.
* We complement this with [PPP conversion factors from the World Development Indicators](https://data.worldbank.org/indicator/PA.NUS.PRVT.PP) (WDI) (indicator PA.NUS.PRVT.PP) for countries where PIP's values are unavailable or where WDI better reflects the current currency situation.

For the inflation adjustment, we use the World Development Indicators' [Consumer price index (2010 = 100)](https://data.worldbank.org/indicator/FP.CPI.TOTL) (indicator code `FP.CPI.TOTL`). We use the CPI deflator because the underlying data measures household income and consumption, and CPI most closely tracks the prices that households face.

#### Data processing

PIP and WDI both derive their PPP factors from the same underlying data (the [International Comparison Program's 2021 round](https://www.worldbank.org/en/programs/icp)), but their numbers don't always agree, because they apply different methodologies.

When PPP conversion factors are available from PIP and WDI, and they agree within 3%, we use PIP. This is the case for roughly 80% of countries with available PPP data.

When only one source is available for a country, we use that. In practice, WDI has a broader coverage of small territories (American Samoa, Cayman Islands, Greenland, Puerto Rico, etc.), while PIP is the only source for a few others (Taiwan, Venezuela, Yemen).

When both PIP and WDI have values, but they disagree by more than 3%, we choose manually using some decision rules. Some common situations are:

* WDI is preferred when it accounts for a recent currency redenomination that PIP does not — for example, the Belarusian ruble in 2016, the Mauritanian ouguiya in 2017, or Croatia's adoption of the Euro in 2023.
* In a small number of borderline cases (e.g. France at 3.2%), the PIP value is retained.
* When the disagreement can't be resolved easily, we drop the currency from the selection menu. For example, there are large differences in countries with complex currency histories (Zimbabwe, Liberia), ongoing currency transitions (Curaçao and Sint Maarten), or unresolved multi-currency situations (Palestine).
* We also exclude the currency of countries where the PIP income distribution represents only an urban or rural subpopulation rather than the whole population — e.g. the (urban) rows for Argentina, Bolivia, Colombia, Ecuador, Honduras, Suriname, and Uruguay, and the (urban)/(rural) rows for China, Ethiopia, and Rwanda. For some of these cases, such as Argentina and China, the WDI value can be used instead, so these countries remain on the selector; others without a WDI fallback are excluded entirely.

The list of manual overrides is maintained in `int_dollar_conversions.py` and validated on every run.

#### Converting to a local currency

When the user switches to a different currency, every income value is multiplied by a **conversion factor** specific to that currency.

The conversion factor for each country is the product of two components.

1. The **PPP factor** converts from international dollars to local currencies at 2021 prices (effectively reversing the PPP adjustment that the World Bank applied to the original data).
2. The **CPI factor** adjusts for inflation between 2021 and the most recent year of CPI data available for that country. The adjustment is simply `CPI[latest year] / CPI[2021]`. The latest CPI year varies by country — for most it is close to the present, but for some it may be an earlier year.

The final conversion factor is the product of these two: `PPP_factor × CPI_factor`, rounded to 5 significant figures. Multiplying an international dollar value by it therefore gives an amount in local currency at the latest available year's price level.

#### Time intervals

The raw data is expressed as daily values. When the user switches to a monthly or yearly view, we multiply every value by a fixed factor: 365/12 for monthly, 365 for yearly. If a currency conversion is also active, both factors are applied together in a single step.

## Limitations

The big strength of the World Bank dataset we are using here is that it is one of the few sources that can give us this global perspective on income inequality. But the data comes with important caveats.

First, not all countries measure the same thing. As we discussed earlier, the PIP collects household survey data from national statistics offices and works to make that data as comparable across countries as possible. But countries don't all measure the same thing, with some surveys focusing on capturing income, and others on consumption expenditure. These two concepts are related (income equals consumption plus savings), but they are not the same. Income tends to be more unequally distributed than consumption, so when comparing distributions across countries, we should pay attention to this — part of the visible difference can reflect the welfare concept rather than a real gap.

Second, incomes at the top of the distribution are often missed or underreported in surveys. The very wealthy are few, hard to reach, and less likely to participate — and even when they do, surveys tend to miss income from capital, business ownership, and complex financial arrangements. The right tail of every distribution shown here is likely compressed relative to reality.

In addition to this, observations for many countries are estimated, not observed. The 1000 bins dataset is constructed to have one observation for every country and year since 1990. To do this, the World Bank team interpolates between survey years and also extrapolates from the last survey year to the present year. There are many assumptions involved in this, in particular, the extrapolations. In those years, the distribution and poverty estimates may not fully reflect the reality of the country. They are best read as approximate levels of income relative to other countries, rather than as precise point estimates. The PIP team recommends care when looking at specific filled values for individual countries. The poverty and inequality statistics published directly by PIP are estimated from the actual survey data, not from this filled dataset.

Beyond these data limitations, there are decisions we made that affect the visualization:

- The KDE curves are smoothed to show the general shape of the income distribution. They should not be used to read off the precise shares. The share of the population below a line comes from the bin data directly, not from the area under the curve. The curves are also evaluated over daily incomes from $0.25 to $1,000. No countries have bins below $0.25, but 30 countries have bins above $1,000 — typically only for the richest 0.1%. For those countries, the very tip of the right tail of the curve is visually trimmed.
- As we discussed before, by using bin averages rather than individual-level data, we are missing within-bin inequality.
- For the inflation adjustment, the CPI indicator lags the current year. The latest available year in WDI's CPI series is typically one or two years behind the present. Values shown in local currency are therefore in prices from one or two years ago, which is still a reasonable approximation of present-day monetary values in most economies, though less so in countries experiencing rapid inflation.

## References

**Our World in Data source code**

- [Visualization component — `incomePlotUtils.ts`](https://github.com/owid/owid-grapher/blob/master/bespoke/projects/income-plots/src/utils/incomePlotUtils.ts) and the entry point at [`src/index.tsx`](https://github.com/owid/owid-grapher/blob/master/bespoke/projects/income-plots/src/index.tsx) and [`src/components/App.tsx`](https://github.com/owid/owid-grapher/blob/master/bespoke/projects/income-plots/src/components/App.tsx).
- [Currency conversions ETL step — `int_dollar_conversions.py`](https://github.com/owid/etl/blob/master/etl/steps/data/external/owid_grapher/latest/int_dollar_conversions.py).
- [1000-binned distribution garden step — `thousand_bins_distribution.py`](https://github.com/owid/etl/blob/master/etl/steps/data/garden/wb/2026-03-25/thousand_bins_distribution.py), and the raw [snapshot definition](https://github.com/owid/etl/blob/master/snapshots/wb/2026-03-25/thousand_bins_distribution.dta.dvc).

**External datasets and references**

- World Bank. Poverty and Inequality Platform [Data set]. World Bank Group. [https://pip.worldbank.org](https://pip.worldbank.org/)
- Mahler, Daniel Gerszon; Yonzan, Nishant; Lakner, Christoph (2022). The Impact of COVID-19 on Global Inequality and Poverty. Policy Research Working Papers; 10198. © World Bank, Washington, DC. [https://openknowledge.worldbank.org/entities/publication/54fae299-8800-585f-9f18-a42514f8d83b](https://openknowledge.worldbank.org/entities/publication/54fae299-8800-585f-9f18-a42514f8d83b) updated with Poverty and Inequality Platform.
- [1000-binned global distribution dataset](https://datacatalog.worldbank.org/search/dataset/0064304/1000-binned-global-distribution), the updated link to the dataset above.
- [Poverty and Inequality Platform Methodology Handbook](https://datanalytics.worldbank.org/PIP-Methodology/)
- [PPP conversion factor, households (WDI, `PA.NUS.PRVT.PP`)](https://data.worldbank.org/indicator/PA.NUS.PRVT.PP)
- [Consumer price index, 2010 = 100 (WDI, `FP.CPI.TOTL`)](https://data.worldbank.org/indicator/FP.CPI.TOTL)
- [International Comparison Program (ICP)](https://www.worldbank.org/en/programs/icp)
