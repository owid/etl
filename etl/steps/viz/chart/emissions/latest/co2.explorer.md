### CO2 emissions: our data sources

Below we provide details of the underlying data sources used to build this dataset. When citing this work, please also cite the original sources.

### Territorial annual CO2 emissions

Our main metric – annual CO2 emissions – is sourced from the [Global Carbon Project](https://www.globalcarbonproject.org/carbonbudget/). This is the primary raw metric that we rely on to calculate the range of derived metrics presented below.

Andrew and Peters provide a [clear and detailed methodology](https://zenodo.org/doi/10.5281/zenodo.5569234) on how this data is constructed (this is recommended reading for users).

The Global Carbon Project updates its dataset annually with data on global and national CO2 emissions, extending back to the year 1750.

We present our data for three key metrics:

- ‘Fossil CO2 emissions’, which includes all emissions from the consumption of fossil fuels (from coal, oil, gas and flaring) plus direct industrial emissions from cement. It _does not_ include emissions from land use change.
- Land use change emissions.
- The combined total of land use change and fossil CO2 emissions.

The main [Global Carbon Project dataset](https://www.icos-cp.eu/science-and-impact/global-carbon-budget) provides annual emissions from 1959 onwards. However, [a long-term dataset](https://zenodo.org/doi/10.5281/zenodo.5569234) from 1750 onwards is also supplied by Robbie Andrew and Glen Peters. It is this long-term dataset that we rely on.

It also provides per capita estimates – we do not take these values, and instead calculate our own, as detailed in the next section. Any differences between our per capita emissions and those of the Global Carbon Project will result from differences in the population denominator that is applied.

The Global Carbon Project collate and assimilate a large number of data sources to produce their fossil CO2 emissions dataset, including official reports to the UNFCCC, national official statistical agencies, CDIAC-FF, Energy Institute, and many others.

**Full citation:** Andrew, R. M., & Peters, G. P. (2026). The Global Carbon Project's fossil CO2 emissions dataset (2026v11) [Data set]. Zenodo. [https://doi.org/10.5281/zenodo.23667912](https://doi.org/10.5281/zenodo.23667912)


### Consumption-based annual CO2 emissions

Our data on annual consumption-based CO2 emissions is also sourced from the [Global Carbon Project](https://www.globalcarbonproject.org/carbonbudget/). The data is available [here](https://www.icos-cp.eu/science-and-impact/global-carbon-budget).

It provides estimates for some – but not all – countries from 1990 onwards _(see our FAQ on this below)_. This is based on the methodology laid out by [Peters et al. (2012)](https://doi.org/10.5194/bg-9-3247-2012).

Consumption-based emissions data _does not_ include land use change. Only emissions from the consumption of fossil fuels (from coal, oil, gas and flaring) plus direct industrial emissions from cement production – are included.

The original data is presented in tonnes of carbon. To convert to tonnes of CO2, we have multiplied these values by 3.664. This is the conversion factor recommended by the Global Carbon Project. It comes from the fact that an average CO2 molecule has a mass 3.664 times that of a carbon atom.

### Total greenhouse gas, methane and nitrous oxide emissions

Our data on annual emissions of total greenhouse gas emissions, methane emissions and nitrous oxide emissions is sourced from [Jones et al., National contributions to climate change](https://zenodo.org/records/7636699/latest).

It provides estimates on total greenhouse gas emissions (carbon dioxide, methane and nitrous oxide, including land use change); methane emissions; and nitrous oxide emissions. These are measured in carbon dioxide equivalents (CO2e) based on 100-year global warming potential factors for non-CO₂ gases.

Note that these figures come with higher uncertainty than data on CO2 emissions from fossil fuels. Total greenhouse gas emissions can also appear to be lower than CO2 emissions from fossil fuels if a country has large _negative_ emissions from land use change _(see our FAQ on this below)_.

### Derived metrics from this dataset

At Our World in Data we use this long-term data on annual CO2 emissions to calculate several additional metrics. All of these metrics are presented in this data explorer.

Below we explain how these metrics are calculated, and any additional sources used.

### CO2 emissions per capita

CO2 emissions per capita measure the average annual emissions per person for a country or region.

It is calculated by dividing the total annual emissions of the country or region by its total population.

The source for the annual CO2 emissions data is the Global Carbon Project, as described above.

The population data used in this calculation is our long-run population series which combines three underlying data sources: the HYDE database (History Database of the Global Environment); Gapminder; and the UN World Population Prospects. We describe how this population dataset is constructed [here](https://ourworldindata.org/population-sources).

### Greenhouse gas emissions; methane emissions; and nitrous oxide emissions per capita

Emissions per capita measure the average annual emissions per person for a country or region.

It is calculated by dividing the total annual emissions of the country or region by its total population.

The source for the annual greenhouse gas, methane and nitrous oxide emissions data is Jones et al., as described above.

The population data used in this calculation is our long-run population series which combines three underlying data sources: the HYDE database (History Database of the Global Environment); Gapminder; and the UN World Population Prospects. We describe how this population dataset is constructed [here](https://ourworldindata.org/population-sources).

### Cumulative CO2 emissions

We calculate cumulative CO2 emissions as the sum of annual emissions from 1750 onwards.

For example, cumulative emissions in 1755 are the sum of emissions from 1750, 1751, 1752, 1753, 1754, and 1755.

We calculate this globally and for every country.

### Share of global CO2 emissions

We calculate this metric by dividing a country or region’s emissions by the global emissions in any given year.

For example, if Country A emitted 1 billion tonnes of CO2 in 2019, and global emissions in 2019 were 36 billion tonnes, country A was responsible for:

[1 / 36] * 100 = 2.8% of global emissions.

### CO2 emissions per unit of energy

CO2 emissions per unit of energy are used to measure how carbon-intensive a country’s energy mix is. A country that relies heavily on coal, for example, will emit large amounts of CO2 emissions per unit of energy. A country that has lots of nuclear and renewables will emit much less.

We calculate this metric by dividing the total annual emissions of a country or region by its [total energy supply](https://ourworldindata.org/grapher/energy-mix).

The source for the annual CO2 emissions data is the Global Carbon Project, as described above.

For annual energy supply, we combined three sources. Our primary data source is the [Energy Institute Statistical Review of World Energy](https://www.energyinst.org/statistical-review). However, it does not provide energy data for all countries. For countries absent from this dataset, we rely on the [international energy data from the U.S. Energy Information Administration (EIA)](https://www.eia.gov/international/data/world/total-energy/more-total-energy-data). For the world as a whole, we extend the series back to 1800 with historical estimates by Vaclav Smil.

### CO2 emissions per dollar of GDP (carbon intensity of economies)

CO2 emissions per dollar are used to measure how carbon-intensive a country’s economy is (hence why it is often called the ‘carbon intensity of economies’).

We calculate this by dividing a country or region’s annual CO2 emissions by its total annual gross domestic product (GDP).

The source for the annual CO2 emissions data is the Global Carbon Project, as described above.

The source for GDP data is the [Maddison Project Database](https://www.rug.nl/ggdc/historicaldevelopment/maddison/releases/maddison-project-database-2023). We calculate total GDP by multiplying the Maddison metric of GDP per capita, by the total population. You can find our chart with this data [here](https://ourworldindata.org/grapher/gdp-per-capita-maddison-project-database). GDP is measured in constant 2011 international-dollars. This means that it adjusts for price changes over time (inflation) and price differences between countries.

**Full citation:** Bolt, Jutta and Jan Luiten van Zanden (2024), “Maddison style estimates of the evolution of the world economy: A new 2023 update”, Journal of Economic Surveys, 1–41. DOI: [10.1111/joes.12618](https://doi.org/10.1111/joes.12618).

### Emissions embedded in trade

We calculate this as the difference between a country’s consumption-based emissions and territorial emissions. This means it is the _net_ trade of emissions.

It is equal to consumption-based emissions minus territorial emissions in any given year. This means net _importers_ of emissions have positive values. Net _exporters_ have negative values.

This metric is shown [here](https://ourworldindata.org/grapher/co-emissions-embedded-in-global-trade).

### Emissions embedded in trade as a share of domestic emissions

We calculate this by taking the metric described above – net emissions embedded in trade – and dividing it by a country’s territorial (domestic) emissions.

Again, positive values mean a country is a net importer of emissions. Negative values mean a country is a net exporter.

This metric is shown [here](https://ourworldindata.org/grapher/co2?country=CHN~USA~IND~GBR&gas=co2&accounting=consumption_based&fuel=na&count=share_embedded_in_trade&relative_to_world=total).

### Common FAQs about this emissions data

### How often is this data updated?

The Global Carbon Project (GCP) updates its data annually – typically in November or December each year. We aim to update very soon after each new release.

### How do researchers estimate and construct long-term CO2 emissions?

Unlike atmospheric CO2 _concentrations_ which we can monitor directly in the modern day, our _emissions_ of CO2 from energy production and industry are not measured directly.

Instead, researchers estimate them indirectly based on the amount of fuel that we burn, and the amount of industrial products we produce.

Calculating the amount of CO2 emitted from energy consumption is relatively simple. We need to know three things:

1. How much of a given fuel (e.g. coal) is burned
2. The carbon content of this fuel
3. How much of this carbon is oxidised in the combustion process

By multiplying these figures together we can estimate the CO2 produced from a given fuel:

> _CO2 (in terms of carbon) = Quantity of fuel burned * Carbon content of this fuel * Fraction of this carbon that is oxidised_

Points (2) and (3) are now well-established for the range of fuels that we use for energy – coal, oil, and gas (as well as the variation within these fuels – for example, lignite is a form of coal that has a low carbon content [compared to](https://www.eia.gov/energyexplained/coal/) bituminous coal).

For factor (1) researchers use detailed accounts of fossil fuel extraction, production, and trade to estimate how much of a given fuel is burned in each country. For a country, this would be calculated as:

> _Consumption = Production – Exports + Imports + Change in stocks_

For modern data this is much more straightforward: most countries and industries have and need detailed accounts of energy supply and demand (_see our [Energy Data Explorer](https://ourworldindata.org/grapher/energy-mix), for example_).

Researchers have invested large efforts into reconstructions of historical energy statistics, dating back to 1750 (see, for example, Andres et al. (1999); Etemad et al. (1998)). The further back in time we go, the less detailed our records are; and for some countries, these figures have greater uncertainty. Nonetheless, they provide solid estimates of long-term trends in energy consumption – and derived CO2 emissions.

### Are emissions from land use change included?

Until the 2022 update of the Global Carbon Budget, national emissions data was only available for CO2 emissions from fossil fuels and industrial processes (such as cement production). Emissions from land-use change were only reported as global totals.

The 2022 update – for the first time – includes land use estimates for countries, extending back to 1750.

The quality of these estimates is better for some countries than others.

Since the estimates of emissions for fossil fuels and industry are much more certain than for land use change, we still adopt the former as our main CO2 emissions metric. However, we now also include charts on land use change, and combined emissions from fossil fuels, industry and land use.

### Are emissions from aviation and shipping included?

Emissions from domestic aviation and shipping _are_ included in each country’s total. Emissions from international aviation and shipping _are not_ included in any country or region’s total. This is because there is no international agreement on how these emissions should be allocated: should they, for example, be allocated to the country of origin or destination? In our [related article](https://ourworldindata.org/carbon-footprint-flying) we look at a separate dataset on emissions from aviation.

They are, however, included in the global total. You also [find it here](https://ourworldindata.org/grapher/annual-co-emissions-by-region) as a separate category.

### Does this data account for emissions embedded in traded goods?

We report CO2 emissions in two ways:

- The standard reported metric is [territorial CO2 emissions](https://ourworldindata.org/grapher/co2?country=CHN~USA~IND~GBR~OWID_WRL&gas=co2&accounting=territorial&fuel=all_fossil&count=per_capita&relative_to_world=total). This is the metric typically reported by countries, and the method used to set targets and monitor progress towards them. It _does not_ adjust for emissions embedded in traded goods. It allocates emissions to each country on the basis of _where the CO2 was emitted_. For example, If China burns coal to produce goods that are then sold and shipped to the UK, these emissions are allocated to China.
- We also provide [consumption-based CO2 emissions](https://ourworldindata.org/grapher/co2?country=CHN~USA~IND~GBR~OWID_WRL&gas=co2&accounting=consumption_based&fuel=na&count=per_capita&relative_to_world=total) as a second metric. This _does_ adjust for emissions embedded in traded goods and services. Emissions are allocated to the country where the final goods are used. In the example above, the emissions would instead be allocated to the UK.

**Unless specified as consumption-based emissions in our charts or data, it is territorial CO2 emissions that are being used.**

### Why are consumption-based emissions only available from 1990? Why are they not available for all countries?

To calculate consumption-based emissions we need detailed trade data between countries and the emissions intensity (the amount of CO2 emitted per dollar spent) across many industries and sectors in each country. Prior to 1990, there is insufficient high-quality, high-resolution data to produce these calculations.

For this same reason – insufficient high-resolution trade data – it is not currently possible to calculate consumption-based emissions for all countries. It is mostly high-income and major economies that are included.

Consumption-based emissions also always lag territorial emissions by one year. For example, when territorial emissions for 2020 were released, the latest year for consumption-based emissions was 2019. This is because the required resolution of trade data was not yet available for 2020.

### Why are emissions from cement reported as an individual category?

You will notice in our charts which show the [breakdown of emissions by fuel type](https://ourworldindata.org/grapher/co2-by-source) (coal, oil, gas) that cement is also included as an individual category.

This category captures the _direct_ CO2 emissions from the industrial process used in cement production.

Cement has a binding agent called ‘clinker’. To produce this clinker, limestone (calcium carbonate: CaCO3) is heated to very high temperatures in a low-oxygen environment. This produces lime (CaO). But it also produces CO2 as a by-product. So, we get:

> CaCO3 + heat —> CaO + CO2

These are direct emissions from the cement production process. They account for around half of cement’s _total_ emissions (the direct emissions from this process, plus indirect emissions from energy use). Only the direct emissions for cement are allocated to this category in this dataset.

The _indirect_ emissions from producing energy to power this process are not included in the ‘cement’ category: they are allocated, instead, to the relevant fuels (coal, oil, gas) used in the energy process.

### Why are total greenhouse gas emissions _lower_ than CO2 emissions from fossil fuels for some countries?

For some countries, [total greenhouse gas emissions](https://ourworldindata.org/grapher/co2?tab=map&country=CHN~USA~IND~GBR~OWID_WRL&gas=all_ghg&accounting=territorial&fuel=na&count=per_capita&relative_to_world=total) can be _lower_ than emissions from fossil fuels, or in some cases they can even be negative.

This is because the metric of total greenhouse gas emissions includes emissions from land use change. In some years, land use emissions can be ‘negative’, if a country sequesters a lot of CO2 through reforestation or carbon sequestration. If land use change emissions are negative, total greenhouse gas emissions can appear to be lower than CO2 emissions from fossil fuels. If a country has very large negative emissions, total greenhouse gas emissions can also be negative.

Note that there is significant uncertainty in land use change emissions – significantly more than emissions from fossil fuels.

### Download the data

Download the complete dataset [from our data catalog](https://github.com/owid/co2-data).
