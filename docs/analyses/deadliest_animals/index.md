# What are the world's deadliest animals?

!!! info ""
    :octicons-person-16: **[Hannah Ritchie](https://ourworldindata.org/team/hannah-ritchie), [Fiona Spooner](https://ourworldindata.org/team/fiona-spooner)** • :octicons-calendar-16: February 16, 2026 *(last edit)* • [**:octicons-mail-16: Feedback**](mailto:info@ourworldindata.org?subject=Feedback%20on%20technical%20publication%20-%20World%20deadliest%20animals)

## Introduction and summary

In our article "[What are the world's deadliest animals?](http://ourworldindata.org/worlds-deadliest-animals)", we provide estimates for how many people are killed by various animals in a given year.

The purpose of this article is not to give *the* definitive numbers on deaths by animals. As we'll see below, the data quality for many animals is not good enough to do so. But from the research and data available, we can provide a better sense of the relative magnitude of each.

To not overstate the accuracy of these estimates, we have rounded them to only a few significant figures.

In this technical documentation, we provide a list of sources that we found and considered for each.

Many animals had multiple sources to draw on, and you'll find that our final figure is often not a perfect match for any of them. To get a final figure, we triangulated a reasonable estimate across multiple sources rather than opting for one over the others (without justification for doing so).

Here is a table of the final figures used in the main visualization in the article, and in the section below, you can find the sources we used for each animal.

| Animal | Number of humans killed per year |
| :---- | :---- |
| Mosquitoes | 760,000 |
| Humans | 600,000 |
| Snakes | 100,000 |
| Dogs | 40,000 |
| Freshwater snails (Schistosomiasis) | 14,000 |
| Kissing bugs (Chagas disease) | 8,000 |
| Sandflies (Leishmaniasis) | 5,000 |
| Roundworms (Ascariasis) | 5,000 |
| Scorpions | 3,000 |
| Tapeworms (Cysticercosis) | 2,000 |
| Tsetse flies (Trypanosomiasis) | 1,500 |
| Elephants | 1,000 |
| Hippopotamuses | \>500 |
| Bees, wasps and hornets | 500 |
| Big cats | 300 |
| Crocodiles | \>150 |
| Jellyfish | 100 |
| Spiders | 50 |
| Bears | 20 |
| Sharks | 6 |
| Gray wolves | 5 |

## Sources

### Mosquitoes

In the visualization, we assumed that mosquitoes kill approximately **760,000 people** per year. Here, mosquitoes included multiple species.

Mosquitoes kill humans indirectly by transmitting fatal infectious diseases. These disease are listed in the table below, sources, and estimates for how many people died from them in 2023.

The largest killer, by far, was malaria which is spread by infected female *Anopheles* mosquitoes. In the table below we list the latest estimate — around 670,000 — from the [Institute for Health Metrics and Evaluation](https://vizhub.healthdata.org/gbd-results/). The World Health Organization reports a slightly lower [estimate of 610,000 deaths](https://www.who.int/news-room/fact-sheets/detail/malaria) in the most recent year.

| Disease spread by mosquitoes | Number of deaths in 2023 | Source/comment |
| :---- | :---- | :---- |
| Malaria | 669,960 | [IHME, Global Burden of Disease (2025)](https://vizhub.healthdata.org/gbd-results/) |
| Dengue fever | 52,677 | [IHME, Global Burden of Disease (2025)](https://vizhub.healthdata.org/gbd-results/) |
| Japanese encephalitis | 25,000 | [World Health Organization (2024)](https://www.who.int/news-room/fact-sheets/detail/japanese-encephalitis) |
| Yellow fever | 4,419 | [IHME, Global Burden of Disease (2025)](https://vizhub.healthdata.org/gbd-results/) |
| Chikungunya | 3,700 | [dos Santos et al. (2025)](https://www.nature.com/articles/s41591-025-03703-w) |
| West Nile virus | 230 | [US CDC](https://www.cdc.gov/west-nile-virus/about/index.html) and [Beacon](https://beaconbio.org/en/report/?reportid=00ce28ab-7e94-4257-8742-876f152d27eb&eventid=97139f1c-4d2b-48b8-bcad-2c1d2e1386a9) |
| Zika | 1 | [IHME, Global Burden of Disease (2025)](https://vizhub.healthdata.org/gbd-results/) |
| Rift valley fever |  | Likely to be zero in recent years, but [historic outbreaks](https://en.wikipedia.org/wiki/List_of_Rift_Valley_fever_outbreaks) did result in hundreds of deaths per year |
| Lymphatic filariasis |  | Low or no mortality from this disease |
| O'nyong'nyong virus |  | Low or no mortality from this disease |
| **Total** | **755,987** | Rounded to 760,000 due to uncertainty |

### Humans

In the visualization, we assumed that **600,000 people** are killed by other humans each year. Here, we only include direct deaths from violence and conflict. We do not include categories such as suicide, or car crashes (where you could argue that under certain circumstances, humans are killed by others through negligence).

Our main source was the IHME's [Global Burden of Disease](https://vizhub.healthdata.org/gbd-results/). It includes three categories of violence: 435,675 deaths from interpersonal violence; 159,052 from conflict and terrorism, and 8,889 from police conflict and execution. That's **603,615** in total.

Other sources report similar figures.

The United Nations Office on Drugs and Crime [estimate that](https://ourworldindata.org/grapher/homicides-unodc?tab=line&country=~OWID_WRL) there were between 420,000 and 450,000 homicides per year over the last decade. This is closest in definition to the IHME's "interpersonal violence" (which totaled 436,000).

The Uppsala Conflict Data Program [estimated that](https://ourworldindata.org/grapher/deaths-in-armed-conflicts-by-region) there were around 160,000 deaths in wars and conflict in 2023; similar to the IHME estimate. Note that in 2022, this figure was much higher (at around 310,000) so the final numbers are sensitive to the year chosen.

We chose a figure of 600,000 deaths each year, but in particularly violent years, this can be much higher during large-conflict years.

### Snakes

In the visualization, we assumed that snakes kill **100,000 people** per year. Here, snakes included multiple species. This is one of our most uncertain estimates.

Most people are killed by snakes indirectly through their venom. In many cases, this can lead to paralysis and respiratory failure. Other species can cause organ failure, cardiac toxicity, or tissue damage and the development of sepsis.

The World Health Organization [says that](https://www.who.int/news-room/fact-sheets/detail/snakebite-envenoming) snakebite envenoming kills between 81,410 and 137,880 each year. That claim is repeated in [the scientific literature](https://www.thelancet.com/journals/lancet/article/PIIS0140-6736%2823%2901698-7/fulltext) (81,000 to 130,000).

The IHME's [Global Burden of Disease](https://vizhub.healthdata.org/gbd-results/) estimates that 90,000 people die from venomous animal contact (which we expect is mostly snakes).

Based on these sources, we think 100,000 is a reasonable estimate, but it does come with significant uncertainty. The WHO's range is already large, and this is also reflected in the IHME's figures: its lower and upper bound estimates between 1980 and 2023 vary from 45,000 to 153,000.

Getting accurate counts for how many die from snakebites [is challenging](https://www.scientificdiscovery.dev/p/14-how-many-people-die-from-snakebites) because most victims live in rural areas in low- to middle-income countries, where access to healthcare facilities and accurate death reporting is often limited.

It is plausible that deaths from snakebites are as much as 50% higher than our figure.

### Dogs

In the visualization, we assumed that dogs kill **40,000 people** per year. This is one of our most uncertain estimates.

Most people killed by dogs are killed indirectly through the contraction of rabies, rather than directly from attack wounds. The WHO [states that](https://www.who.int/news-room/fact-sheets/detail/rabies) dogs are responsible for almost all human rabies cases: "Dog bites and scratches cause 99% of the human rabies cases."

The IHME's [Global Burden of Disease](https://vizhub.healthdata.org/gbd-results/) estimated that 15,811 people died from rabies in 2023. However, its lower and upper bound estimates did range from 6,661 to 27,439.

Several other sources have much higher estimates.

The WHO states 59,000 deaths per year in its [Rabies Factsheet](https://www.who.int/news-room/fact-sheets/detail/rabies). We think this might be sourced from a [2015 paper by Hampson et al](https://journals.plos.org/plosntds/article?id=10.1371/journal.pntd.0003709). The WHO's [Global Health Estimates](https://www.who.int/data/global-health-estimates) for 2023 were 43,833.

The US Center for Disease Control and Prevention (CDC) gives [even higher figures](https://www.cdc.gov/rabies/around-world/index.html) of 70,000 deaths per year. However, it's not clear how often these estimates are updated.

Based on this range, we went with a figure of 40,000 deaths per year, but this comes with significant uncertainty.

### Freshwater snails (Schistosomiasis)

In the visualization, we assumed that freshwater snails kill **14,000 people** per year. This is through the spread of the disease *Schistosomiasis*.

The IHME and WHO estimates for this disease are similar.

The IHME's [Global Burden of Disease](https://vizhub.healthdata.org/gbd-results/) estimated that 13,467 people died from Schistosomasis in 2023.

The WHO's [Schistosomiasis Factsheet](https://www.who.int/news-room/fact-sheets/detail/schistosomiasis) gives a figure of 11,792, but the WHO also notes that these figures are "likely underestimated and need to be reassessed". Its Global Health Estimates had a figure of 14,133 for 2021.

### Kissing bugs (Chagas disease)

In the visualization, we assumed that kissing bugs kill **8,000 people** per year. This is through the spread of *Chagas disease*.

The IHME and WHO estimates for this disease are similar.

The IHME's [Global Burden of Disease](https://vizhub.healthdata.org/gbd-results/) estimated that 8,148 people died from Chagas disease in 2023 (ranging from 7,500 to 9,400).

The WHO's [Chagas Disease Factsheet](https://www.who.int/campaigns/world-chagas-disease-day/2025) stated "approximately 10,000 each year". Its Global Health Estimates had a figure of 6,449 for 2021.

Based on these figures, we think 8,000 is a reasonable estimate, but the true figure could be a few thousand higher or lower.

### Sandflies (Leishmaniasis)

In the visualization, we assumed that sandflies kill **5,000 people** per year.

Sandflies can transmit the disease *Leishmaniasis*, which has a case fatality rate of 95% if it's left untreated.

The IHME's [Global Burden of Disease](https://vizhub.healthdata.org/gbd-results/) estimated that 4,627 people died from Leishmaniasis in 2023. However, its lower and upper bound estimates ranged from 1,853 to 8,724.

The WHO's [Global Health Estimates](https://www.who.int/data/global-health-estimates) had a figure of 5,799 deaths in 2021.

Our 5,000 figure therefore falls between the two.

An older [WHO Factsheet from 2017](https://www3.paho.org/hq/dmdocuments/2017/2017-cha-leishmaniasis-factsheet-work.pdf) quoted a range between 20,000 to 30,000 deaths. However, its [current factsheet](https://www.who.int/news-room/fact-sheets/detail/leishmaniasis) does not give any death figures at all. We prefer to use the WHO's most recent global health estimates, rather than its previous 2017 figures.

### Ascaris roundworms (Ascariasis)

In the visualization, we assumed that Ascaris roundworms kill **4,000 people** per year. This is through the spread of the disease *Ascariasis*.

The number of deaths from *Ascariasis* is hard to estimate as its often misattributed to other factors such as intestinal obstruction and sepsis. Medical manuals — such as [this one](https://www.msdmanuals.com/home/infections/parasitic-infections-roundworms-nematodes/ascariasis) — often give a fairly large range of "\~2,000 to 10,000 deaths each year"

The IHME's [Global Burden of Disease](https://vizhub.healthdata.org/gbd-results/) estimated that 4,973 people died from Ascariasis in 2023.

The WHO's Global Health Estimates had a lower figure of 3,747 for 2021.

We think 4,000 is a reasonable estimate between the two.

### Scorpions

In the visualization, we assumed that scorpions kill **3,000 people** per year. This includes multiple species.

Global estimates on this were hard to find.

This paper by Vasconez-Gonzalez et al. (2025) [cites an estimate](https://www.sciencedirect.com/science/article/pii/S2590171025000050) of "at least 3000 deaths annually worldwide". This is based on earlier work by Jean-Philippe Chippaux.

In a [2012 paper](https://pmc.ncbi.nlm.nih.gov/articles/PMC3401053/), Jean-Philippe Chippaux gives a global death toll of around 2,600. [Earlier works](https://www.researchgate.net/profile/Jean-Philippe-Chippaux/publication/5277313_Epidemiology_of_scorpionism_A_global_appraisal/links/6319b436071ea12e3619ae8f/Epidemiology-of-scorpionism-A-global-appraisal.pdf) give even higher figures of more than 3250.

Without more recent estimates, we had to rely on these figures which are now around a decade old.

These figures could be an underestimate because scorpion bites [are not included](https://gh.bmj.com/content/10/11/e020682) in the WHO's Neglected Tropical Diseases, which means they get less attention than some other tropical diseases.

### Tsetse flies (Trypanosomiasis)

In the visualization, we assumed that Tsetse flies kill **1,500 people** per year. This is through the spread of the disease *Trypanosomiasis*.

The IHME's [Global Burden of Disease](https://vizhub.healthdata.org/gbd-results/) estimated that 1,417 people died from Trypanosomiasis in 2023. Its lower and upper bound estimates ranged from 672 to 2,491.

The WHO's [Global Health Estimates](https://www.who.int/data/global-health-estimates) reported 1,098 deaths in 2021. This was following a huge decline from around 25,000 deaths in 2000.

Older literature tends to have much higher numbers, which are still cited in the literature today ([this 2025 paper](https://journals.plos.org/globalpublichealth/article?id=10.1371%2Fjournal.pgph.0004634), for example, cites older estimates of 50,000 deaths per year).

While no death figures are given, [some literature](https://journals.plos.org/plosntds/article?id=10.1371/journal.pntd.0010047) reflects serious progress in reducing *Trypanosomiasis* cases in recent years. This could explain why recent estimates are far lower than previous ones.

### Tapeworms (Cysticercosis)

In the visualization, we assumed that tapeworms kill **2,000 people** per year. This is through the spread of the disease *Cysticercosis*. Death estimates for this disease are quite uncertain.

The IHME's [Global Burden of Disease](https://vizhub.healthdata.org/gbd-results/) estimated that 1,511 people died from Cysticercosis in 2023. Its lower and upper bound estimates ranged from 1,092 to 2,054.

The WHO's [Global Health Estimates](https://www.who.int/data/global-health-estimates) reported higher figures of 6,866 deaths in 2021.

Some literature cites far higher figures (up to as much as 50,000) as it includes deaths linked indirectly to neurocysticercosis complications (e.g., seizures, neurological sequelae), not just direct cause-of-death attribution.

### Elephants

In the visualization, we assumed that elephants kill **around 1,000 people** per year. This estimate includes multiple elephant species.

Commonly cited global figures for elephant-related deaths are often much lower. For example, the [National Geographic](https://www.nationalgeographic.com/animals/article/news-elephants-attack-humans-pressure) has previously cited a figure of around 500 deaths per year, but this estimate dates back to 2005 and appears to be outdated. [Encyclopaedia Britannica](https://www.britannica.com/list/9-of-the-worlds-deadliest-mammals) gives a wide range of "about 100 to more than 500 per year", which also seems too low given more recent national data.

More recent figures suggest substantially higher totals. In India alone, official government statistics [report between](https://www.indiatoday.in/india-today-insight/story/deepening-human-wildlife-crisis-killed-1783-indians-341-elephants-in-3-years-2682489-2025-02-19) 500 and 600 human deaths per year from elephant attacks. According to figures provided by India's Ministry of Environment, Forest and Climate Change, deaths increased from 549 in 2021–22 to 605 in 2022–23, and further to 629 in 2023–24. Sri Lanka also reported [around 176 deaths](https://www.bbc.co.uk/news/world-asia-68090450) from elephant encounters in 2024.

These two countries already account for well over 700 deaths per year. Given that elephant-related fatalities also occur across other parts of South and Southeast Asia, as well as parts of Africa, a global total closer to 1,000 deaths per year seems plausible, and may still be an underestimate.

This figure remains uncertain. We therefore use a rounded estimate of 1,000 deaths per year to reflect both the available evidence and the uncertainty.

### Bees, wasps and hornets

In the visualization, we assumed that bees, wasps and hornets kill **more than 500 people** per year. This includes multiple species.

Most of these deaths result from anaphylactic shock following stings, rather than from venom toxicity itself. Reliable global estimates are hard to get because deaths are often recorded under general external-cause categories rather than by specific animal species.

High-quality national and regional data suggest that deaths are not rare in countries with good vital registration systems. In the United States, the [Centers for Disease Control and Prevention](https://www.cdc.gov/mmwr/volumes/72/wr/mm7227a6.htm) report an average of around 72 deaths per year from hornet, wasp and bee stings. In Europe, the WHO Mortality Database [data reports](https://platform.who.int/mortality/themes/theme-details/topics/indicator-groups/indicator-group-details/MDB/other-unintentional-injuries) an average of around 74 deaths per year between 1994 and 2016 (using the ICD cause-of-death code X23, "contact with hornets, wasps and bees"). That's more than 140 deaths per year across the US and Europe alone.

The WHO Mortality Database includes similar cause-of-death data for a number of other countries with reliable vital statistics; these sum to a global total of just over 500 deaths per year.

This is likely to be an underestimate since some deaths will occur in countries missing from the database. That's why we give a figure of "more than 500" rather than giving a precise estimate.

### Big cats

In the visualization, we assumed that big cats kill **around 300 people** per year. This includes 100 deaths by tigers (a single species), 100 by lions (another single species), and another 100 from other large cats such as leopards.

#### Tigers

Global estimates for tiger deaths are scarce, but there are national figures for India, which is where many tiger-related fatalities occur.

In India, the Ministry of Environment, Forests and Climate Change [reported deaths](https://factly.in/data-number-of-humans-killed-in-tiger-attacks-increased-significantly-in-last-few-years/) ranging from 31 to 112 per year from 2018 to 2022. That means India alone can reach around 100 deaths in high-conflict years.

Nepal [also reports](https://www.abc.net.au/news/2025-02-06/nepal-tiger-attacks-spark-concern-about-growing-population/104891944) around 15 to 20 deaths per year.

Deaths are likely to also occur in other countries, particularly in South and Southeast Asia (although probably in far lower numbers than India).

We think that a global estimate of around 100 deaths seems reasonable in most years, but it could be slightly higher in some.

#### Lions

Global estimates were also challenging to find.

Sources typically cite something in the range of around 100 fatalities per year across Africa. For example, [this source](https://biologyinsights.com/how-many-people-have-been-killed-by-lions/) claims: "Estimates suggest 70 to 100 human fatalities annually in Africa, with some sources indicating up to 200 deaths per year."

[This paper](https://www.cambridge.org/core/journals/oryx/article/humanwildlife-conflict-in-mozambique-a-national-perspective-with-emphasis-on-wildlife-attacks-on-humans/434EEAAF88F3C10E9FA6B55F2C3ACE39) suggests that in Mozambique, there were around a dozen deaths from lions per year.

Extrapolated to other African countries with lion-human conflict, around 100 per year seems a reasonable estimate.

#### Other big cats

Beyond tigers and lions, other big cats (especially leopards) can cause fatal attacks in some regions, but we struggled to find a consistent, global dataset comparable to the sources above. Fatalities from other big cats (such as leopards) appear to be much lower in many countries, but may still add meaningfully in places where human–wildlife conflict is intense, and reporting is incomplete.

Based on the additional big cat deaths combined with the uncertainty of lion and tiger estimates, we think 300 deaths in total is a reasonable, rounded estimate for this category overall.

### Crocodiles

In the visualization, we assumed that crocodiles kill **more than 150 people** per year. We think 150 is a reasonable estimate, but the true figure could be several tens more.

One main source of data on crocodile attacks — which is used by [the IUCN](https://www.iucncsg.org/pages/Crocodilian-Attacks.html) — was [CrocBITE](https://en.wikipedia.org/wiki/CrocBITE) (which is no longer online). It registered 1,613 deaths in the decade from 2010 to 2019. That's around 160 deaths per year.

[CrocAttack](https://crocattack.org/2015-2024attackstats/) has data from 2015 to 2024 for some countries, totalling around 150 deaths per year. Some countries might be missing, so this could be an underestimate.

That's why we denote the death toll as "more than 150".

### Jellyfish

In the visualization, we assumed that jellyfish kill **around 100 people** per year. This includes multiple species, but most fatalities are thought to be caused by box jellyfish (multiple species) and, in some regions, Irukandji jellyfish.

We found it difficult to find a credible, recent global estimate so this comes with significant uncertainty.

A commonly cited source for global deaths is [Encyclopaedia Britannica](https://www.britannica.com/animal/box-jellyfish), which states: "Fatalities from box jellyfish stings range from 40 to more than 100 worldwide annually." It also notes that this estimate is likely too low because some countries with frequent lethal stings — such as the Philippines — do not have official reporting systems.

Country-level evidence supports the idea that the true global total could be higher than the low end of Britannica's range. For example, multiple sources cite that around [20 to 40 people die](https://pia.gov.ph/news/up-to-40-filipinos-die-annually-from-jellyfish-sting-prompting-research-to-stem-the-tide) each year from jellyfish stings in the Philippines alone.

Older estimates are sometimes much higher. In 1987, an Australian researcher (Peter Fenner) produced a count of around 500 deaths per year, which is still widely quoted. However, this is now several decades old, and we weren't able to find a consistent, modern global dataset that supports such a high figure.

We think that these fatal incidents could be quite highly concentrated in the Philippines, however, it seems unlikely that no other deaths occur elsewhere. Given that the Philippines count is up to 40 deaths per year, we think 100 globally is a reasonable estimate, but again, this is uncertain.

### Hippopotamuses

In the visualisation, we assumed that hippopotamuses kill **around 50 people** per year. This is for the single, common hippopotamus species (Hippopotamus amphibius).

A figure of around 500 deaths per year is widely repeated in media and online sources (here it is from [the BBC](https://www.bbc.co.uk/news/world-36320744) and [Africa Check](https://africacheck.org/fact-checks/meta-programme-fact-checks/yes-hippos-kill-around-500-people-year-africa)) but these claims often fail to present clear, concrete sources.

Where more systematic reporting does exist, the numbers can be much lower. A [2025 study](https://www.nature.com/articles/s41598-025-04934-0) using nationwide human–wildlife conflict records in Zimbabwe reports 20 hippo-related human fatalities from 2016 to 2022, which is around 3 deaths per year.

Zambia is home to around one-third of the global hippo population, [yet records](https://thewildsource.com/should-you-fear-hippos-on-safari-debunking-the-deadly-hippo-myth) only about 11 deaths per year. We also looked for a way to scale from a better-documented country to a rough global figure.

Extrapolating globally, that would suggest a global total of just 30 to 40 deaths per year.

Again, these figures are very uncertain, but an estimate of 50 deaths per year seems more plausible to us, based on the available national records, than one closer to 500 deaths per year.

Note that while the number of deaths is fairly low, hippo encounters have [the highest fatality rate](https://www.frontiersin.org/journals/conservation-science/articles/10.3389/fcosc.2022.954722/full) among typical safari animals. That means low deaths are explained by the fact that there are few human encounters.

### Spiders

In the visualization, we assumed that spiders kill **around 50 people** per year. This includes multiple species.

Although spider bites are often perceived as dangerous, fatal outcomes are rare in most high-income countries. It appears there have been [no confirmed deaths](https://en.wikipedia.org/wiki/List_of_medically_significant_spider_bites) from spider bites in Australia since 1979, despite the presence of potentially fatal species.

However, global mortality data suggest that deaths still occur elsewhere. The [WHO Mortality Database](https://www.who.int/data/data-collection-tools/who-mortality-database) reports approximately 50 deaths per year attributed to spider bites, based on countries with relatively complete cause-of-death registration. A large share of these deaths are reported from Mexico and a small number of other countries.

This figure likely underestimates the true number, since many countries with limited vital registration systems are not included in the database. Still, there is little evidence to suggest that global deaths are substantially higher than this, given the rarity of fatal envenomation and the widespread availability of antivenom for the most dangerous species. We therefore use 50 deaths per year as a reasonable, rounded estimate.

### Bears

In the visualization, we assumed that bears kill **around 20 people** per year. This includes multiple species: brown bears, sloth bears, polar bears, Asian black bears and American black bears.

Brown bears and sloth bears are responsible for the majority of fatal attacks globally.

The most comprehensive estimate for black bears comes from [Bombieri et al. (2019)](https://www.nature.com/articles/s41598-019-44341-w), who documented 95 fatal attacks by brown bears between 2000 and 2015, which is an average of around 6 deaths per year. Sloth bears are estimated to kill more than 10 people per year, based on reports summarized by [National Geographic](https://www.nationalgeographic.com/animals/article/sloth-bears-are-worlds-deadliest-india-human-conflict); this is mostly in India.

Other species kill far fewer people. [Polar bears](https://en.wikipedia.org/wiki/Bear_attack#Polar_bears) and [American black bears](https://en.wikipedia.org/wiki/Bear_attack#American_black_bears) kill fewer than 1 person per year based on historical records. [Asian black bears](https://en.wikipedia.org/wiki/Bear_attack#Asian_black_bears) are estimated to kill around one person per year in Japan.

Combined, this comes to around 20 deaths per year.

### Sharks

In the visualization, we assumed that sharks kill **6 people** per year. The most common fatal attacks are caused by these main species: the great white shark, tiger shark, bull shark, and the oceanic whitetip shark.

The Florida Museum of Natural History maintains a database called the [International Shark Attack File](https://www.floridamuseum.ufl.edu/shark-attacks/trends/fatalities/) (ISAF), where it logs both shark attacks and fatalities.

In the decade from 2010 to 2019, 54 fatal attacks were logged, which is around 6 per year.

Note that this can fluctuate from year to year; in 2023, there were 14 reported fatalities.

### Gray wolves

In the visualization, we assumed that gray wolves kill **5 people** per year. These are the species of wolf most likely to be in proximity to, and attack, humans.

The Norwegian Institute for Nature Research (NINA) [notes that](https://wolf.org/wp-content/uploads/2021/11/WolfAttacksUpdate.pdf) there were 26 recorded fatalities from 2002 to 2020. That's around 2 fatalities per year. Most died from rabies, rather than from direct attack wounds.

This data is more complete for Europe and North America, and there may be some attacks outside of these regions (although likely limited to Asia since others are outside of the wolf's range).

We therefore think that 2 per year is slightly too low, but the total wouldn't be dramatically higher. That's why we estimated 5 per year, but with uncertainty.

