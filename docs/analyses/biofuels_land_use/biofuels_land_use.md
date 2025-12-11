# Land use for biofuels vs. solar

## Introduction and summary

This document is a technical companion to the article ["***"](***), that examines how much land is currently used to grow crops for biofuels, and the opportunity cost of that land compared with using it for solar photovoltaic (PV).
In this technical document, we explain how our estimates are derived and compare our assumptions with alternative ones in the literature.

In case you are short of time, the main conclusions of this document are as follows:

- The net land used to grow feedstocks for biofuels is around 32 million hectares, as [estimated by Cerulogy](https://www.cerulogy.com/diverted-harvest/).
This is similar to [the land area of Cote d'Ivoire](https://ourworldindata.org/grapher/land-area-hectares?country=DEU~CIV~POL), and roughly between Poland's and Germany's.
With that amount of cropland, biofuels produce 1.42 PWh of energy, according to [the Energy Institute's Statistical Review of World Energy](https://www.energyinst.org/statistical-review).

- If we could hypothetically cover the same area with solar PV panels, under reasonable assumptions on modern solar energy density and capacity factors (from IRENA, LBNL, and other sources), we could produce around 32 PWh of electricity.
This surpasses [the world's total electricity generation in 2024](https://ourworldindata.org/grapher/electricity-prod-source-stacked).
This also implies that, using the same amount of land, we could produce ~22 times more energy with solar panels than growing crops for biofuels.

- If we replaced vehicles currently powered by biofuels with electric vehicles (under reasonable assumptions on fuel-to-energy ratio for combustion and electric engines), we could power them using just ~2% of that land with solar farms.
Globally, around 26 PWh of fuel energy is consumed for road transport (e.g. cars and trucks), according to [the IEA's World Energy Outlook](https://www.iea.org/reports/world-energy-outlook-2025).
If we replaced them with electric vehicles, they'd need less than 10 PWh of electricity, which could be produced with solar panels in just a third of the land currently used for biofuel feedstock.

## How much area is globally used for biofuel production?

### Our estimate, based on Cerulogy

[This report by Cerulogy](https://www.transportenvironment.org/uploads/files/Cerulogy_Diverted-harvest_November_2024.pdf) estimates that there were 61.3 million hectares of feedstock in 2023.
They then take away a certain percentage of co-products, and estimate an area of 32.0 million hectares of net cropland attributed to biofuel feedstock.

As they show in their Table 1 (which is based on the Energy Institute's Statistical Review of World Energy), the countries they include cover 95% of the global production (from 9 regions; 5% is assigned to "Rest of the World").
However, 95% of production from those countries (USA, Brazil, EU+UK, Indonesia, China, India, Argentina, Canada, and Thailand) doesn't translate into 95% of land; in fact those countries may import feedstock from other countries.
They do point out this caveat: "Global trade in biofuel feedstocks (rather than finished fuel), which is not captured by this data, is also sizeable; but it is not trivial to determine from general trade data which feedstocks are used in the biofuels industry versus other industries (cf. Malins & Sandford, 2022)."

Note that second generation biofuels (e.g. from municipal solid waste or bagasse) is not included, but in terms of land use it's probably a small fraction compared to first generation.

Their values for feedstock in Table 2 ("Feedstock consumption (Mt) for first-generation biofuels in 2023 for the case study countries") comes from "USDA GAIN reports (Danielson, 2023; Das, 2024; Flach et al., 2023; Florence Mojica-Sevilla, 2024; Hayashi, 2024; Joseph, 2024; Prasertsri, 2024; Rahmanulloh, 2023); EIA data publication (U.S. Energy Information Administration, 2024a); UK RTFO data (UK Department for Transport, 2024c)".
Those sources are listed in their bibliography, but unfortunately, they are individual PDF reports for different countries, e.g. [India](https://www.fas.usda.gov/data/india-biofuels-annual-9), and [Canada](https://apps.fas.usda.gov/newgainapi/api/Report/DownloadReportByFileName?fileName=Biofuels%20Annual_Ottawa_Canada_CA2023-0030.pdf).
These reports seem to also rely on some conversion factors, but they are somewhat different for each country.

Replicating Cerulogy's estimate would be beyond the scope of this article.

Taking Cerulogy's estimate at face value, globally, first-generation biofuels use 32 million hectares of cropland.

### Alternative estimate by UFOP

[The UFOP Report on Global Market Supply 2022/2023](https://www.ufop.de/files/4716/7878/1413/UFOP_Global_Suppy_Report_2022-2023.pdf) says that:

> Crop plants were grown on more than 1.4 billion hectares worldwide in 2021. [...]
Only around 8 per cent of the area was used in biofuels production.

[FAOSTAT's Land Use dataset](https://www.fao.org/faostat/en/#data/RL) estimates that global cropland area in 2021 was [1.57 billion hectares](https://ourworldindata.org/grapher/cropland-area?tab=line&country=~OWID_WRL) (and hasn't changed much in the last decade).
A value as low as 1.4 billion hectares would take us back to the early 1980s.
Alternatively, UFOP could be referring to the global arable land (also from the Land Use dataset), which is 1.38 billion hectares.
But this would exclude palm oil and sugar cane area, so that would not be meaningful.
So, it's unclear what that area of 1.4 billion hectares refers to exactly.

They don't provide more specific numbers.
The chart on Page 41 (ironically called "Biofuels take up little space") shows the area of a selection of crops for 2021.
Adding up the numbers, we get to a total of 84 million hectares devoted to biofuel production, and 740 million hectares for other cropland.
This implies biofuel area (for those selected crops) accounts for 11% of the land.
However, the list is incomplete, and it's reasonable to expect that other feedstocks would have smaller shares, which could lead to the 8% they quote on that chart and elsewhere.

The source quoted in the chart is: OECD, USDA, Oil World.
But the chart says "©AMI 2022"; so I understand the chart was originally made by AMI based on those sources.
Going to the UFOP document's bibliography, [the AMI data](https://www.ami-informiert.de/ami-shop/shop/detail?ai%5Bd_name%5D=Markt_aktuell_%C3%96lsaaten_und_Bioenergie&ai%5Bd_prodid%5D=110&ai%5Bd_pos%5D=11&ai%5Bcontroller%5D=Catalog&ai%5Baction%5D=detail) seems to not be under a paywall.

Similar values between 7% and 8% have been quoted in different places (e.g. [Oils & Fat International](https://www.ofimagazine.com/news/only-8-of-global-crop-land-used-for-biofuels), [Food Unfolded](https://www.foodunfolded.com/article/biofuels-is-growing-food-for-energy-a-good-idea#ref1), [Biofuels international](https://biofuels-news.com/news/only-7-of-global-crops-is-used-for-biofuels-production-ufop-reveals/) or [Advanced Biofuels USA](https://advancedbiofuelsusa.info/raw-material-for-biofuel-production-is-available-on-7-of-the-cultivated-area)).
They all cite UFOP, even though the origin of those estimates are unclear.

Taking the UFOP values at face value, 8% of 1.4 billion hectares would correspond to 112 million hectares.
And applying that same percentage to the actual global cropland estimated by FAOSTAT, would lead to 126 million hectares.

But their estimates don't seem to adjust for the land use of co-products.

### Alternative estimate by Popp et al. (2018)

[Popp et al. (2018)](https://www.researchgate.net/publication/324131165_Biofuel_use_Peculiarities_and_implications) (similar to a previous paper [an earlier paper from the same lead author](https://www.mdpi.com/1420-3049/21/3/285)) estimated that 2% of global cropland is devoted to biofuels (30-35 million gross hectares).
After removing co-products, the area would be smaller.
They say:
> If we include co-products [...] then the land that is needed to grow feedstocks reduces [...] to about 1.5%.
This would imply around 23 million hectares.

It's unclear which year they are basing their estimates on, but they quote explicitly "At present, around 2% of the 1.515 billion ha which makes up the total global crop area (FAO, 2013)".
So, what they refer to as present may be based on 2013 FAO data (which could refer to 2011 or 2012).
Since then, biofuel production [has increased by ~86%](https://ourworldindata.org/grapher/biofuel-production), so it would be reasonable to expect a significantly larger net area today, closer to Cerulogy's estimate.

## How much electricity can be produced by solar PV in that area?

### Our estimate based on typical power densities and IRENA's latest capacity factors

If we were to produce solar energy on biofuel cropland now, we'd install solar panels with modern, utility-scale capacity factors.
We can find representative capacity factors in [the latest IRENA's Renewable Power Generation Costs report](https://www.irena.org/-/media/Files/IRENA/Agency/Publication/2025/Jul/IRENA_TEC_RPGC_in_2024_2025.pdf).
In Table 3.2 they show global weighted average capacity factors (AC/DC) for utility-scale solar PV systems by year of commissioning (page 99).
We can approximately convert the AC/DC factors to AC/AC by multiplying them by an inverter loading ratio (DC/AC) of 1.25.
The factors reported for 2024 (and their conversions) are as follows:

- 5th percentile: 11.5% (AC/DC) x 1.25 ~ 14% (DC/DC)
- Weighted average: 17.4% (AC/DC) x 1.25 ~ 22% (DC/DC)
- 95th percentile: 22.6% (AC/DC) x 1.25 ~ 28% (DC/DC)

After speaking with various people working in the field, it seems that a fiducial value for a typical installation is around 1.5 ha / MWp (in terms of DC power at peak).
In terms of AC power, this would correspond to ~0.53 MW(AC) / ha.

We can convert power density to an average energy density using the average DC conversion factor above:

( 1 MWp / 1.5 ha ) x ( 1 GWh / 1000 MWh ) x ( 17.4% of 8760 h / 1 year ) = 1.02 GWh / ha / year

We will therefore assume a fiducial value for energy density of 1 GWh / ha / year.

A conservative range of power densities would be somewhere between 0.3 MW(AC) / ha and 0.8 MW(AC) / ha, around a midpoint value of 0.53 MW(AC) / ha chosen before.

Assuming these fiducial ranges of capacity factors and power densities, we can calculate a reasonable lower limit, midpoint and upper limit for the amount of electricity produced in a year in 32 million hectares of land:

Lower:

32 · 10⁶ ha x ( 0.3 MW / ha ) x ( 1 PWh / 10⁹ MWh ) x ( 14% of 8760 h / 1 year ) = 12 PWh / year

Midpoint:

32 · 10⁶ ha x ( 1 GWh / ha / year ) x ( 1 PWh / 10⁶ GWh ) = 32 PWh / year

Upper:

32 · 10⁶ ha x ( 0.7 MW / ha ) x ( 1 PWh / 10⁹ MWh ) x ( 28% of 8760 h / 1 year ) = 55 PWh / year

So, if we filled the entire cropland of biofuel feedstocks with solar panels, we could produce ~32 PWh per year, which would be as much as the entire world's annual electricity generation.

### Alternative estimate based on Cerulogy

Cerulogy assumes an average area density of 0.08 kWp / m² for a solar farm, and a farm productivity of 3.9 kWh / kWp / day.
This leads to:

0.08 kWp x ( 3.9 kWh / kWp / day ) x ( 365 days / 1 year ) x ( 10⁴ m² / 1 ha) x ( 1 GWh / 10⁶ kWh ) =  1.14 GWh / year / ha

Therefore, Cerulogy's assumes a capacity factor (DC) of:

3.9 kWh / kWp / day x ( 1 day / 24 h ) = 16%

In terms of to AC, this corresponds to 20%, and a power density of:

0.08kWp / m² * ( 10⁴ m² / 1 ha ) * ( 1 MWp / 1000 kWp ) = 0.8 MWp / ha

Which corresponds to 0.64 MW(AC) / ha.

We can also estimate how much electricity could solar PV produce on the 32 million hectares used for biofuel production:

32 · 10⁶ ha x ( 0.64 MW / ha ) x ( 20% of 8760 h / year ) x ( 1 PWh / 10⁹ MWh ) = 36 PWh

### Alternative estimate based on UNECE's lifecycle assessment

An alternative estimate comes from [this UNECE report](https://unece.org/sites/default/files/2022-04/LCA_3_FINAL%20March%202022.pdf) (and [this corrigendum](https://unece.org/sites/default/files/2022-07/Corrigendum%20to%20UNECE%20LCA%20report%20-%20land%20use.pdf)), which is used in [this OWID article](https://ourworldindata.org/land-use-per-energy-source).

They do a lifecycle assessment of the amount of land required per year to produce 1 MWh of electricity.
This is estimated over the entire production process (so, including the extraction of materials, refining processes, etc.).

For Solar PV, I'll consider silicon technology only, which is the most commonly used.
The UNECE study considers poly-Si.
Their lifetime land use depends on whether they are installed on-ground, or on roofs.
Given that we want to consider the land currently used for biofuel production, we will consider only on-ground PV.
Therefore, the minimum, average, and maximum (lifecycle) land used is 12, 19, and 37 square meters-annum per MWh.

If we used 32 million hectares of land every year for electricity generation from solar PV, how much electricity could be produced?

Minimum:

32 · 10⁶ ha x ( 10⁴ m² / 1 ha ) x ( 1 MWh / 12 m² ) x ( 1 PWh / 10⁹ MWh ) = 27 PWh

Midpoint:

32 · 10⁶ ha x ( 10⁴ m² / 1 ha ) x ( 1 MWh / 19 m² ) x ( 1 PWh / 10⁹ MWh ) = 17 PWh

Maximum:

32 · 10⁶ ha x ( 10⁴ m² / 1 ha ) x ( 1 MWh / 37 m² ) x ( 1 PWh / 10⁹ MWh ) = 8.6 PWh

These lifecycle values are not directly comparable to Cerulogy's cropland estimate, since they do not represent the physical land footprint of solar farms.
But they can provide a rough lower bound on solar land intensity.

### Alternative estimate based on LBNL

[Bolinger & Bolinger (2022)](https://www.energy.gov/sites/default/files/2022-01/lbnl_ieee-land-requirements-for-utility-scale-pv.pdf) (Lawrence Berkeley National Laboratory, LBNL) estimate the following values for power and energy densities in the US:

- Power density:
  - Fixed-tilt: 0.87 MW(DC) / ha -> 0.69 MW(AC) / ha
  - Tracking plants: 0.59 MW(DC) / ha -> 0.45 MW(AC) / ha
- Energy density:
  - Fixed-tilt: 1.10 GWh / year / ha
  - Tracking plants: 0.97 GWh / year / ha

Assuming tracking plants, which are more commonly used:

32 · 10⁶ x ( 1 PWh / 10⁶ GWh ) x ( 0.97 GWh / ha / year ) =  31 PWh / year

This is in very good agreement with our estimate, given that their estimated energy density for tracking plants is very close to our fiducial value of 1 GWh / ha / year.

### Alternative estimate based on Victoria et al. (2021)

An alternative estimate comes from [Victoria et al. (2021)](https://www.sciencedirect.com/science/article/pii/S2542435121001008).
In their supplementary Note S3, they say:

"[...] Assuming 17% efficiency and that only 30% of the land is covered by solar panels in large-scale installations, the capacity density results in 51 W / m², which is in agreement with values reported in [Ong et al. (2013)](https://www.osti.gov/servlets/purl/1086349/).
For an average annual generation for solar PV of 1370 kWh / kW, 38 million ha would be needed.
The land area of the world is 13,003 million ha [OWID (2019)](https://ourworldindata.org/land-use).
Hence, our current electricity consumption could be supplied by solar PV covering 0.3% of the available land."

I understand these values refer to DC.
This translates into:

( 0.51 MWp / ha ) x ( 1370 MWh / MWp / yr ) x ( 1 GWh / 1000 MWh ) = 0.698 GWh / ha / yr

The assumed AC power density would be:

( 0.51 MWp / ha ) / 1.25 = 0.41 MW(AC) / ha

And the capacity factor, in AC (again assuming ILR of 1.25) would be:

( 0.51 MWp / ha ) x ( 1370 MWh / MWp / yr ) / ( ( 0.51 MWp / ha ) / 1.25 x ( 8760 h / yr ) ) = 20%

And this would lead to a total production of:

32 · 10⁶ x ( 1 PWh / 10⁶ GWh ) x ( 0.698 GWh / ha / yr ) = 22 PWh

The power density of 0.41 MW / ha is on the conservative side, and the capacity factor of 20% is just a bit below our previous estimates.
They say that their values are consistent with NREL's Ong et al. (2013).
That's a 2013 paper about the US, which may not be a representative estimate of today's global average capacity factor.

So the LBNL estimate (also based in the US, but from 2021) may be a more appropriate estimate.

### Other considerations

We have estimated that, if we replaced the cropland devoted for biofuels with modern solar panels, we could generate ~32 PWh of electricity per year.
This is just above [the world's total electricity production](https://ourworldindata.org/grapher/electricity-prod-source-stacked), which is 31 PWh in 2024, according to Ember.
Currently, ~18 PWh of that electricity comes from fossil fuels, and only ~2 PWh from solar.

Compared to [the ~1.42 PWh](https://ourworldindata.org/grapher/biofuel-production?tab=line&time=earliest..2024) of energy actually produced by biofuels, according to [the Energy Institute's Statistical Review of World Energy](https://www.energyinst.org/statistical-review), using solar farms we could produce ~22 times more energy (in electricity) than growing crops for biofuels.

Our fiducial value of 1 GWh / year / ha could be considered conservative, given that:
- A significant fraction of the biofuel cropland is in the US and Brazil, which have good conditions for solar generation.
- The hypothetical transition would take place in the coming years, so, it would be reasonable to expect that efficiencies would improve slightly with respect to the present.

Other previous estimates have come to similar conclusions.
For example, [Victoria et al. (2021)](https://www.cell.com/joule/fulltext/S2542-4351%2821%2900100-8) (in [their supplemental information](https://www.cell.com/cms/10.1016/j.joule.2021.03.005/attachment/c2b79392-88f9-4231-833d-faf1a0a2a125/mmc1.pdf)) have also concluded that we could power all electricity (27 PWh in 2019) with just 38 million hectares of solar PV.

We could improve our estimates and add values at the country level.
Some countries would be more efficient at converting biofuels into solar electricity.
Even within a country, there would be some hotspots where solar farms could be most effective (e.g. cropland close to transmission lines), as shown in [Sturchio et al. (2025)](https://www.pnas.org/doi/10.1073/pnas.2501605122) for the US.
Additionally, we could also look into the analogous result using wind instead of solar.
But those improvements fell out of the scope of this analysis.

#### TODO: Additional sources to acknowledge and compare with

Nøland et al. (2022) [estimate around](https://www.nature.com/articles/s41598-022-25341-9) 1 hectare per GWh per year.
This is from Table 1, which gives a value of 1 TWh per km².

Nøland, J. K., Auxepaules, J., Rousset, A., Perney, B., & Falletti, G. (2022).
Spatial energy density of large-scale electricity generation from power sources worldwide.
Scientific Reports, 12(1), 21280.

Franz et al. (2025) [assume](https://link.springer.com/article/10.1186/s13705-024-00504-w) 1.1 hectares per GWh per year.

Franz, M., & Dumke, H. (2025).
Evolution of patterns of specific land use by free-field photovoltaic power plants in Europe from 2006 to 2022.
Energy, Sustainability and Society, 15(1), 12.

## How much electricity would be needed to power all road transport?

### Top-down estimate

From [the IEA's World Energy Outlook 2025 (free dataset)](https://www.iea.org/data-and-statistics/data-product/world-energy-outlook-2025-free-dataset), we can extract the total road energy in 2024, which is 93.3 EJ (or 25.8 PWh).
We can safely neglect the fraction of vehicles that are currently powered with electricity.
Let's assume an efficiency of around 30% for combustion cars and 80% for electric cars.
Then, if all vehicles were replaced by electric ones, how much energy would they require?

93 EJ x ( 0.278 PWh / 1 EJ ) x ( 30% / 80% ) = 9.7 PWh

The amount of energy produced by biofuels in 2024 is, as we saw before, about 1.42 PWh.
Assuming, for simplicity, that all this energy goes to power combustion vehicles, we could replace those vehicles with electric ones, which would require only 0.53 PWh (assuming 30% and 80% efficiencies, as before).
To power all those vehicles with solar farms, assuming the energy density from LBNL, we'd need:

( 0.53 PWh / year ) x ( 10⁶ GWh / 1 PWh ) x ( 1 ha x year / 0.97 GWh ) =  0.546 million ha

In other words, if instead of growing crops to feed cars with biofuels, we produced solar electricity to power electric cars, we'd need 2% of the land for the same amount of transport.

Moreover, if all road transport was electric, they could be powered with just 10 million hectares of solar PV, since:

( 9.7 PWh / year ) x ( 10⁶ GWh / 1 PWh ) x ( 1 ha x year / 0.97 GWh ) = 10 million ha

So, to power all road transport, we'd need about 10 PWh of electricity per year.
This could roughly be achieved by 10 million hectares of solar PV, which is about a third of the area used for biofuel feedstock.

### Bottom-up estimate

I estimate around 7000 TWh per year, comprising 3500 TWh for cars and a similar amount for trucks.

Let's start with cars.
The International Energy Agency [estimates that](https://www.iea.org/data-and-statistics/data-product/world-energy-outlook-2025-free-dataset#tables-for-scenario-projections) globally, passenger cars covered 25800 billion passenger-kilometers in 2024.
First, we need to convert that to kilometers.
To do that, I've assumed that the average car occupancy is 1.5 (so, 1.5 people in a car, on average).
That gives 17200 billion kilometers.
We'll assume that the average electric car uses around 0.2 kilowatt-hours to drive one kilometer.
Covering 17200 billion kilometers would therefore use ( 17200 x 0.2 = 3445 billion kWh ).
That is equivalent to 3445 TWh of electricity to power the global car fleet, assuming all cars were electric.

The efficiency of electric cars varies based on model, age, size and other factors.
I assume a value of 0.2 kWh per kilometer based on several sources.
This analysis by [Weiss et al. (2024)](https://www.mdpi.com/2071-1050/16/17/7529) of the European car fleet found certified and average ratings of 19 kWh and 21 kWh per 100 kilometers.
That's around 0.2 kWh per kilometer.

Weiss, M., Winbush, T., Newman, A., & Helmers, E. (2024).
Energy consumption of electric vehicles in Europe.
Sustainability, 16(17), 7529.

You can find the efficiency of different models in [this EV Database](https://ev-database.org/cheatsheet/energy-consumption-electric-car).
The average across this huge number of models and brands was 0.19 kWh per kilometer.

What about trucks?

This is a bit more challenging to calculate, because their electric models have not reached market penetration in the way that electric cars have.
They are at an earlier stage of development, so numbers on energy efficiency are more difficult to find.

The International Energy Agency [estimates that](https://www.iea.org/data-and-statistics/data-product/world-energy-outlook-2025-free-dataset#tables-for-scenario-projections) heavy-duty trucks covered 35000 billion tonne-kilometers in 2024.
To convert to kilometers, I assume that the average truck load is around 12 tonnes.
That gives 35000 billion / 12 = 2,900 billion kilometers.
I have assumed that a medium heavy-duty truck uses around 1.2 kWh per kilometer driven (if electric).
Covering 2900 billion kilometers therefore uses 2900 billion / 1.2 = 3500 billion kWh.
That's 3500 TWh of electricity to power the global truck fleet, assuming they were all electric.

Again, the efficiency of electric trucks varies based on a range of factors including size, weight, and design.
I assume an efficiency of 1.2 kWh per km, based on published estimates from a number of sources.

The vehicle manufacturer, Scania, reports that its electric trucks have an efficiency of 1.1 kWh per km.
An independent group [tested the truck](https://www.scania.com/group/en/home/electrification/e-mobility-hub/sweden-to-turkey-top-insights-from-a-4500-km-bev-road-trip.html) and found a similar result of 1.15 kWh per km.

During [an 18-month trial](https://bett.cenex.co.uk/assets/reports/BETT---End-of-trial-report.pdf) of twenty 19-tonne electric trucks, the average efficiency ranged from 0.8 to 1.1 kWh per km, depending on urban or rural driving.

A [recent study](https://theicct.org/wp-content/uploads/2025/08/ID-359-%E2%80%93-EU-goods-transport_report_final.pdf) conducted by the International Council on Clean Transportation found a mean consumption in the range of 1.0 to 1.2 kW per km.

Larger trucks tend to require more electricity per kilometer, and some will have an energy consumption higher than 1.2 kWh per km.
However, this will tend to average out across an entire truck fleet.

# Bibliography

Popp, J., Kot, S., Lakner, Z., & Oláh, J. (2018).
Biofuel Use: Peculiarities and Implications.
Journal of Security & Sustainability Issues, 7(3).

Popp, J., Harangi-Rákos, M., Gabnai, Z., Balogh, P., Antal, G., & Bai, A. (2016).
Biofuels and their co-products as livestock feed: global economic and environmental implications.
Molecules, 21(3), 285.
