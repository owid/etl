"""The country registry.

Every country carries two independent attributes:

* ``tier`` — what the national number is built from. This is the quality ladder, and it says
  nothing about whether we could recompute it.
* ``recalculated`` — whether we rebuilt the figure ourselves. True means it was rebuilt from counted
  births and women and checked against what the office publishes; False means the office's own rate
  was copied straight from the source. The labels say "Recalculated from births & women" and "Rate
  copied from source" — deliberately not "fully validated" and "not validated", which several reviewers
  read as claims about whether the number is right rather than about who computed it. Both now describe
  the operation and nothing more. This says how far we could take a figure apart, not how good it is:
  an incomplete registry we can decompose is still an incomplete registry.

``loader`` is None for countries with no national figure to plot.
"""

import os
import sys
import warnings

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
warnings.filterwarnings("ignore")

from argentina import argentina_tfr  # noqa: E402
from australia import australia_tfr  # noqa: E402
from austria import austria_tfr  # noqa: E402
from belgium import belgium_tfr  # noqa: E402
from canada import canada_tfr  # noqa: E402
from chile import chile_tfr  # noqa: E402
from china import china_tfr  # noqa: E402
from czechia import czechia_tfr  # noqa: E402
from ecuador import ecuador_tfr  # noqa: E402
from colombia import dane_female_pop, dane_registered_tfr  # noqa: E402
from france import france_tfr  # noqa: E402
from greece import greece_tfr  # noqa: E402
from guatemala import guatemala_tfr  # noqa: E402
from hungary import hungary_tfr  # noqa: E402
from india import india  # noqa: E402
from peru import peru_tfr  # noqa: E402
from sri_lanka import sri_lanka_tfr  # noqa: E402
from sweden import sweden_tfr  # noqa: E402
from switzerland import switzerland_tfr  # noqa: E402
from taiwan import taiwan_tfr  # noqa: E402
from uk import uk_tfr  # noqa: E402
from philippines import philippines  # noqa: E402
from poland import poland_tfr  # noqa: E402
from portugal import portugal_tfr  # noqa: E402
from kazakhstan import kazakhstan_tfr  # noqa: E402
from korea import korea_tfr  # noqa: E402
from malaysia import malaysia_tfr  # noqa: E402
from myanmar import myanmar_tfr  # noqa: E402
from netherlands import netherlands_tfr  # noqa: E402
from romania import romania_tfr  # noqa: E402
from published import (  # noqa: E402
    afghanistan,
    algeria,
    angola,
    azerbaijan,
    bangladesh,
    benin,
    bolivia,
    burkina_faso,
    burundi,
    cambodia,
    cameroon,
    chad,
    cote_divoire,
    cuba,
    dominican_republic,
    drc,
    ethiopia,
    ghana,
    guinea,
    haiti,
    honduras,
    indonesia,
    iran,
    iraq,
    israel,
    jordan,
    kenya,
    madagascar,
    malawi,
    mali,
    morocco,
    mozambique,
    nepal,
    niger,
    nigeria,
    north_korea,
    pakistan,
    papua_new_guinea,
    russia,
    rwanda,
    saudi_arabia,
    senegal,
    sierra_leone,
    somalia,
    south_sudan,
    sudan,
    syria,
    tajikistan,
    tanzania,
    tunisia,
    ukraine,
    uzbekistan,
    venezuela,
    vietnam,
    yemen,
    zambia,
    zimbabwe,
)
from sources import egypt, germany, japan, mexico, thailand, united_states  # noqa: E402
from south_africa import south_africa_tfr  # noqa: E402
from spain import spain_tfr  # noqa: E402
from turkey import turkey_tfr  # noqa: E402
from uganda import uganda_tfr  # noqa: E402

START = 2000

# tier key -> (label, color). Colors are ColorBrewer Dark2, which is qualitative — the ordering
# carries no meaning, it just keeps the six categories apart at small sizes and in both themes.
TIERS = {
    "complete": ("Complete registration", "#1B9E77"),
    "incomplete": ("Incomplete registration", "#D95F02"),
    "sample": ("Sample registration", "#7570B3"),
    "survey": ("Survey or census", "#E7298A"),
    "projection": ("Projection only", "#E6AB02"),
    "none": ("No official figure", "#666666"),
}


def colombia():
    pop = dane_female_pop()
    v = dane_registered_tfr(pop)[["year", "value"]].sort_values("year")
    span = range(int(v.year.min()), int(v.year.max()) + 1)
    return v.set_index("year").reindex(span).rename_axis("year").reset_index()


def brazil():
    import brazil as br

    return br.tfr()[["year", "value"]]


def italy():
    return pd.read_csv(os.path.join(HERE, "data", "italy_tfr.csv"))


def france():
    return france_tfr()


def C(name, src, loader, wpp_name, tier, recalculated, found, method, caveats, url=""):
    """found — what the office publishes. method — what we did with it. caveats — what to
    watch out for. Written plainly, because this is the documentation for later."""
    return dict(name=name, src=src, loader=loader, wpp_name=wpp_name, tier=tier,
                recalculated=recalculated, found=found, method=method, caveats=caveats, url=url)


COUNTRIES = [
    C("India", "Registrar General — Sample Registration System", india, "India", "sample", False,
      "The SRS Statistical Report 2024 publishes a total fertility rate for India and the bigger states, "
      "to one decimal place. Its table 15 gives it annually for 2019-24. There is also a chart of "
      "the 1985-2024 trend, but it is a picture with no numbers behind it.",
      "We read the India row of table 15 straight out of the report's PDF. No arithmetic of our "
      "own — the number is exactly what the Registrar General states.",
      "The SRS is a sample, not full civil registration: almost 9 million people are tracked "
      "continuously and the rates are scaled up to stand for the whole country. India's civil registration now records almost all "
      "births, but it does not tabulate the mother's age consistently across states, so there is no "
      "registration-based national figure to use instead. One decimal place also means the series looks "
      "flatter than it is.",
      "https://censusindia.gov.in/nada/index.php/catalog/47152"),
    C("Colombia", "DANE — Estadísticas Vitales", colombia, "Colombia", "complete", True,
      "DANE publishes registered births by five-year age group of mother every year, and national "
      "population by single year of age and sex.",
      "We divided registered births in each age group by the women in that group and added the results "
      "across the groups, for every year. Our rates match DANE's own published ones to within rounding — "
      "for 2025, 27.3 per thousand women aged 15-19 against its 27.3, and so on across the groups.",
      "DANE's population projections also carry a fertility assumption, and it has sat above what the "
      "registry shows. But DANE has been closing that gap itself: in July 2025 it revised the assumption "
      "down, saying the fall in births had been stronger than it first projected. So the office is moving "
      "towards its own registry rather than away from it. DANE also warns that its registry rates are raw, "
      "and will differ from figures produced by methods that adjust for missed births — which is its own "
      "account of why an estimate like the UN's would sit above this line. The 2025 figure is provisional "
      "and should rise a little, as 2024 did, because late records are folded in only at the final release. "
      "One thing worth keeping in view: the gap against the UN is recent. It was near zero as late as "
      "2015-18 and has opened to about 0.4 since. We divide by DANE's population projections, which are "
      "themselves modeled rather than counted.",
      "https://www.dane.gov.co/index.php/estadisticas-por-tema/salud/nacimientos-y-defunciones"),
    C("Mexico", "INEGI — registered births by the mother's age, over CONAPO's female population", mexico,
      "Mexico", "complete", True,
      "INEGI's OLAP cube gives registered births by year of occurrence and mother's age group, for every "
      "registration year separately. CONAPO publishes mid-year population by sex and single age.",
      "We summed births for each year of occurrence across all registration years, then divided by "
      "CONAPO's female population and summed across age groups.",
      "We stop at 2022. Later years of occurrence are still filling up with late registrations — only "
      "about 993,000 of 2024's births had been registered by the time of this extract, against roughly "
      "1.67 million expected — so plotting them would show a collapse that is not real. CONAPO's own "
      "model-based figure is well above the registry.",
      "https://www.inegi.org.mx/programas/natalidad/"),
    C("Ukraine", "Держстат — Population 1990-2021", ukraine, "Ukraine", "complete", False,
      "Ukraine's state statistics service, Держстат, publishes the fertility rate for every year from 1990 "
      "to 2021, with the rates by age group behind it, in one downloadable workbook.",
      "We read the total column. Summing the rates by age group and multiplying by five — the width of each "
      "band — reproduces the published total: 1.159 against 1.16 for 2021.",
      "The series stops at 2021 not because publication is running late but because Держстат cannot compute "
      "a population to divide by. It has been working on a replacement, including estimating population "
      "from mobile network records, and has set up an interagency group to find further sources, but nothing "
      "on births, fertility or population by age has come out of it for any year after 2021. Its monthly "
      "regional releases stop dead after January 2022. Birth counts for later years do circulate, from the "
      "justice ministry's registration records rather than Держстат, but they are not broken down by the "
      "mother's age and come with no population to divide by, so they cannot give a fertility rate. "
      "Territory matters too: the fertility figures exclude Crimea and Sevastopol from 2015, and from that "
      "year they drop the whole of Donetsk and Luhansk rather than only the occupied parts — a wider "
      "exclusion than the 2014 footnote, inside the same publication.",
      "https://stat.gov.ua/uk/datasets/narodzhuvanist"),
    C("Morocco", "HCP — censuses and demographic surveys", morocco, "Morocco", "survey", False,
      "Morocco's High Commission for Planning, HCP, publishes one long-run fertility series behind its "
      "indicator page, as a spreadsheet: 2.47 for 2004, 2.19 for 2010, 2.21 for 2014 and 2.00 for 2024. Its "
      "2024 census volumes give that last figure more precisely, as 1.97 — 1.77 in towns and cities, 2.37 in "
      "the countryside.",
      "We read the whole-country column of that series, and for 2024 we use the census's own 1.97 rather "
      "than the spreadsheet's rounded 2.00. Both are HCP's, one publication apart.",
      "The points do not all come from the same kind of count. 2004, 2014 and 2024 are censuses, which ask "
      "about births in the twelve months before enumeration; 2010 is a household survey; and the two "
      "earliest points, 1962 and 1975, fall in no census year — we have not identified which surveys they "
      "come from. Fertility is only asked on the census long form, given to about 30% of households, and the "
      "answers are then scaled up to the whole country. That scaling is a statistical estimate from a "
      "sample, not a correction for births that went unreported, and HCP does not publish both an "
      "unadjusted and an adjusted figure. It has also not published the rates by age group behind the 2024 "
      "figure, so nobody outside can check that figure by rebuilding it from its parts; so far the national "
      "number appears only inside the regional census volumes, with no national fertility volume yet. One "
      "other figure circulates for Morocco, 2.38: that comes from a health ministry survey covering roughly "
      "2015-17, not from a census, and is not part of the series here.",
      "https://www.hcp.ma/Naissances-et-fecondite_r554.html"),
    C("Uzbekistan", "Statistics Agency — registered births by the mother's age, over its own female "
      "population estimate", uzbekistan,
      "Uzbekistan", "complete", False,
      "Uzbekistan's statistics agency publishes a fertility rate annually from 2010, free to the public, "
      "with separate figures for towns and the countryside. It says it counts births from the justice "
      "ministry's birth registry, and divides them by its own estimate of how many women there are in each "
      "age group during the year.",
      "We use the nationwide figure as published. No arithmetic of our own.",
      "Uzbekistan's fertility rose rather than fell — from 2.42 in 2017 to 3.445 in 2023 — and the rise is in "
      "the agency's own registration-based figures, not only in outside estimates. It has since eased back "
      "to 3.20 in 2025, so on the agency's own numbers the peak may already have passed. The rise happened "
      "in both towns and the countryside, but faster in towns: rural fertility ran about 0.4 to 0.5 above "
      "urban until 2021, then the gap nearly closed — 0.02 in 2022 — before reopening to 0.41 by 2025. "
      "Recent years are open to revision. The agency's free online data has no births by age of mother, and "
      "its population brackets there are administrative ones that do not split 40-49 and have no clean "
      "15-19, so the rate cannot be rebuilt from what it puts online. Its printed demographic yearbook does "
      "carry a table of births by the mother's age, so a check against counts should be possible for anyone "
      "who can obtain it.",
      "https://stat.uz/en/official-statistics/demography"),
    C("Saudi Arabia", "GASTAT — Population Estimates", saudi_arabia, "Saudi Arabia", "projection", False,
      "GASTAT publishes a fertility rate annually from 2011, in three parallel series: Saudis, non-Saudis, "
      "and everyone resident in the kingdom. For 2024 those are 2.7, 0.8 and 2.0.",
      "We took the whole-population series, which is the one comparable with the UN's figures. No "
      "arithmetic of our own.",
      "Which of the three series you use matters more here than almost anywhere: non-Saudis are 44% of the "
      "population and their fertility is a third of the Saudi figure, because the non-Saudi resident "
      "population is overwhelmingly working-age and often without families in the country. GASTAT avoids "
      "any confusion by publishing all three separately. Its own methodology says these numbers are "
      "assumptions built into a population projection: starting from the 2022 census, it adds each year's "
      "births, deaths and migration, taken from administrative records, to estimate the next year. So the "
      "births behind the figure are registered ones, but the rate a reader sees comes out of a model rather "
      "than a head count — which is why this sits under projections rather than registration. GASTAT "
      "publishes no rates by age of mother and no births by age of mother, so there is nothing to rebuild "
      "the figure from. The nearest thing it publishes is the average number of children women of each age "
      "have ever had, which is a lifetime total rather than a count of births in the year, so adding those "
      "up would mean nothing. One translation trap: GASTAT's English text repeatedly says these rates are "
      "per 1,000 women, but the numbers are children per woman.",
      "https://www.stats.gov.sa/documents/20117/2435273/"
      "Population+Estimates+Statistics+2024+EN.pdf"),
    C("Ghana", "GSS — Demographic and Health Survey", ghana, "Ghana", "survey", False,
      "The Ghana Statistical Service runs the survey itself and publishes a fertility rate from each round: "
      "4.4 for 2003, 4.0 for 2008, 4.2 for 2014 and 3.9 for 2022. Its censuses give different figures "
      "again, and its 2021 census counts are published as raw tables through an open database.",
      "We use the rate each survey round published. Summing the 2022 report's own rates by age group "
      "reproduces its published 3.9 exactly.",
      "Ghana shows more clearly than anywhere else here how much the correction for missed births matters. "
      "For the 2010 census the office publishes both figures and names the method — 3.28 as reported and "
      "4.57 after a standard statistical correction for under-reported births, the relational Gompertz "
      "model — and attributes the gap to mothers misstating their age and under-reporting recent births. For "
      "2000 the pair is 3.99 and 5.66. For the 2021 census the reported figure is 3.1, which is what its own "
      "counts give, but we have found no corrected figure published alongside it. On that census's own "
      "pattern a correction would land about a child higher, so the 3.1 is not comparable with the survey "
      "rounds and we do not plot it. Its own survey puts birth registration at 75% of under-fives, and the "
      "office says outright that census and survey estimates are used because birth registration is not "
      "reliable enough.",
      "https://statsghana.gov.gh/"),
    C("Madagascar", "INSTAT — Enquête Démographique et de Santé", madagascar, "Madagascar", "survey",
      False,
      "Madagascar's national statistics office, INSTAT, publishes a fertility rate from each round of its "
      "Demographic and Health Survey — 5.2 for 2003-04, 4.8 for 2008-09 and 4.3 for 2021. Its 2018 census "
      "arrives at 4.3 as well, by a different route, and prints the births and women behind that figure as "
      "counts.",
      "We use the rate each survey round published. We also checked the census's own arithmetic: dividing "
      "its counts and summing gives 4.290 against the 4.3 it prints. The census publishes a second figure "
      "over a wider age range, 12 to 54, and recomputing that from the same counts gives 4.82 against its "
      "4.8 — which is a different measure from the 2008-09 survey's 4.8, despite the matching number.",
      "Madagascar is the one country here where the office worked out a correction and then declined to use "
      "it. Its census volume applies a standard demographic check for births women forget or misdate, which "
      "compares recent births against the totals women report over their lifetimes. It finds under-reporting "
      "in every age group and reports that correcting for it would raise fertility from 4.3 to 4.7 — then "
      "argues the gap looks more like a real recent decline than missed births, states that the data will "
      "not be adjusted, and publishes the lower figure. Declining to apply a correction you have already "
      "computed is unusual. Civil registration cannot be used here: the census is the only source for the "
      "population's age structure. Registration is not negligible — the survey puts 74% of births "
      "registered and 57% of children holding a certificate — but it is still not good enough to build a "
      "fertility rate from.",
      "https://dhsprogram.com/pubs/pdf/FR376/FR376.pdf"),
    C("Mali", "INSTAT — Enquête Démographique et de Santé", mali, "Mali", "survey", False,
      "Mali's national statistics office, INSTAT, publishes a fertility rate from each of seven survey "
      "rounds since 1987, the latest 6.0 for 2023-24. The two earliest fall before this chart begins, so "
      "five of them are drawn. Its 2022 census gives 6.1, and also publishes the underlying birth and women "
      "counts.",
      "We use the rate each survey reported. We also checked the census: dividing its own corrected birth "
      "counts by its women counts, age group by age group, reproduces every rate it published and gives "
      "6.09 against its printed 6.1.",
      "Mali's census applied one of the largest corrections we have seen anywhere. It found the raw count of "
      "births in the previous twelve months unusable — sex ratios running as high as 148 boys per 100 girls, "
      "and only about 70% as many declared births as there were children under one in the same count — and "
      "states plainly that the data are of poor quality and require adjustment. It names the method it used, "
      "an indirect technique for correcting under-reported births, and says which alternatives it tested and "
      "rejected. The effect is close to a doubling: 494,742 declared births become 930,503 corrected ones, "
      "about 88% more. Those two totals appear in different tables rather than side by side, so that "
      "comparison is ours. Civil registration is not used for fertility. The census found 83% of people hold "
      "a birth certificate, while the civil-registration directorate estimated in 2018 that between 40 and "
      "60% of births go unregistered — two different bodies measuring different things in different years.",
      "https://www.instat-mali.org/fr/publications/enquete-demographique-et-de-sante-eds"),
    C("Malaysia", "DOSM — its own published fertility rates by the mother's age", malaysia_tfr,
      "Malaysia", "complete", False,
      "Malaysia's national statistics office, DOSM, publishes fertility rates by age group and its own "
      "total, annually from 1958, along with population by five-year age group and sex. It also publishes "
      "the registered births by age of the mother, as counts, in its annual Vital Statistics report.",
      "We took DOSM's published total. The age-band comparison divides those registered births by the "
      "female population. Doing that for 2024 gives 1.55, not the 1.60 DOSM publishes; part of the "
      "difference is that DOSM printed that year's rates rounded to whole births per thousand women, but it "
      "does not fully account for the gap, and DOSM does not say what population its own rate divides by.",
      "Malaysia's rate counts all residents including non-citizens, both in the births counted and in the "
      "population they are divided by. That matters because non-citizen women are about a tenth of some "
      "childbearing age groups — 131,000 of the 1.3 million aged 25-29 in 2023. A birth is counted only if "
      "it is registered in Malaysia, so a foreign resident who registers a child in their home country "
      "instead drops out. DOSM marks its 2024 figures preliminary: they are real registered data, but late "
      "registrations will still be added. Population for 2015-19 is still on the 2010 census basis and DOSM "
      "says it will be revised. The seven age groups leave out a small number of births to mothers under 15 "
      "or over 49, and a few hundred where the age was not recorded.",
      "https://open.dosm.gov.my/data-catalogue/fertility"),
    C("Mozambique", "INE — Inquérito Demográfico e de Saúde", mozambique, "Mozambique", "survey", False,
      "Mozambique's national statistics office, INE, publishes a fertility rate from each round of its "
      "Demographic and Health Survey, and its 2022-23 report sets all four side by side: 5.2 in 1997, 5.5 in "
      "2003, 5.9 in 2011 and 4.9 in 2022-23. Its 2017 census separately publishes the raw material — births "
      "in the twelve months before the census by the mother's age, and women by age.",
      "We read the trend table in that report. Each round is plotted at the year most of its fieldwork fell "
      "in, so the last one sits at 2022 rather than 2023. Dividing the 2017 census's own counts gives 4.18, "
      "which sits below both the survey before it and the survey after, so we do not plot it.",
      "The 4.18 that the census counts imply is the usual sign that asking only about the last twelve months "
      "undercuts births. INE has published a study of fertility from that census, which works out both an "
      "uncorrected and a corrected figure and names the methods it uses; we have not yet incorporated it, so "
      "the census is absent from the line above rather than shown at its corrected level. Mozambican civil "
      "registration cannot be used at all, and is getting worse rather than better: the share of under-fives "
      "registered fell from 48% in 2011 to 31% in 2022-23.",
      "https://www.ine.gov.mz/"),
    C("Poland", "GUS — births and population by single year of age", poland_tfr, "Poland",
      "complete", True,
      "GUS publishes live births by single year of the mother's age, population by single year of age and "
      "sex twice a year, and its own fertility rate — all through one open API with no key.",
      "We divided the births at each single age by the women of that age at 30 June and summed. That is the "
      "population GUS says it divides by, and the result reproduces its own published rate to three "
      "decimals: 1.1576 against 1.158 for 2023, 1.0987 against 1.099 for 2024, 1.0675 against 1.068 for "
      "2025.",
      "Poland's rate is now among the lowest anywhere, and still falling. The series starts in 2013, which is "
      "where the mid-year population by single year of age begins in the database we read; births reach back "
      "to 2002, and older population tables exist elsewhere, so the line could be extended. GUS counts as "
      "resident anyone living in the country for three months or more. Whether that takes in the Ukrainians "
      "who arrived after 2022 under temporary protection is genuinely unclear: GUS describes the population "
      "it uses for its own rates as excluding people staying temporarily, but it also flags the treatment of "
      "recent arrivals as unsettled and does not quantify any effect. If they are in the population, part of "
      "the recent fall would be the denominator growing, and it would affect GUS's own figure exactly as much "
      "as ours. GUS has not yet published a final figure for 2025, so ours should be read as provisional even "
      "though nothing in the database is marked as such. Eurostat's figures for Poland run about 0.03 to 0.04 "
      "higher throughout, because it uses a different definition of who counts as resident.",
      "https://bdl.stat.gov.pl/"),
    C("Peru", "INEI — registered births by the mother's age, over its female population estimates",
      peru_tfr, "Peru", "complete",
      True,
      "Peru's national statistics institute, INEI, publishes registered births broken down by the mother's "
      "age group, and female population by age group. The birth table covers only births registered online, "
      "which is 97 to 99% of them depending on the year. INEI also publishes two fertility rates of its own, "
      "neither from the registry: 1.8 for 2023 from its continuous household survey, and 2.2 as the "
      "assumption inside its population projection for 2020-25.",
      "We divided the births by the female population in each age group and summed, giving 1.84 for 2022, "
      "1.69 for 2023 and 1.51 for 2024.",
      "The registry shows a much steeper fall than either of INEI's own figures. Our 1.69 for 2023 sits 0.5 "
      "children per woman below the projection assumption, which dates from 2019 and so predates both the "
      "pandemic and the decline since. INEI's own text expects a real fall, attributing it to women "
      "postponing or forgoing motherhood rather than to a data problem. But some of the gap is registration "
      "lag rather than fertility: births are counted by the year they were registered rather than the year "
      "they happened, and although Peru's deadline is 60 to 90 days, enough records arrive later to lift a "
      "year's total by several percent once the following year's report is published. That correction has not "
      "happened yet for 2024, so it is the least complete year here and should be expected to rise. Late "
      "registration was under 3% in 2023 but rose to over 4% in 2024, which makes the point. The 2020 "
      "collapse in registrations was lockdown closing registry offices, which INEI says outright, so it is "
      "not a fertility signal; our series starts after it.",
      "https://www.gob.pe/institucion/inei/informes-publicaciones"),
    C("Philippines", "Philippine Statistics Authority — registered live births", philippines,
      "Philippines", "incomplete", True,
      "The Philippine Statistics Authority publishes registered live births by age group of mother in its "
      "online database, one table per year, alongside a projected population by five-year age group and sex.",
      "We divided registered births by the projected women in each age group and added the results. The "
      "population table is published in thousands, which we scaled up.",
      "This understates real fertility, and by an amount that grows towards the present. The authority "
      "counts each birth in the year it happened, but closes the books a few months after the year ends, "
      "and late filings keep arriving for years afterwards — so the newest year has had the least time to "
      "fill up. That is the likeliest reason our figure falls from 1.61 in 2023 to 1.49 in 2024, a drop far "
      "steeper than the authority's own reported change in total births, and it is why the 2024 point "
      "should be expected to rise. Not every birth is registered either, and the authority states plainly "
      "that it makes no adjustment for the ones that are missed; it publishes how many are filed within "
      "thirty days, but not how complete the register eventually becomes. Its own survey, the 2022 National "
      "Demographic and Health Survey, measured about 1.9 — though from women's birth histories over "
      "roughly the three years before it, so that figure describes an earlier period than ours. There are "
      "only two years here because the database publishes a separate table for each one.",
      "https://openstat.psa.gov.ph/"),
    C("Egypt", "CAPMAS — Annual Bulletin of Births and Deaths", egypt, "Egypt", "complete", False,
      "Each annual bulletin has a table 13 giving age-specific fertility rates, the female population of "
      "each age group, and CAPMAS's own total fertility rate. Table 12 separately gives registered live "
      "births by age of mother, as raw numbers.",
      "We summed the age-specific rates and multiplied by five, the width of each age group, which reproduces "
      "CAPMAS's own published total exactly. The age-band comparison uses the registered births from table "
      "12 rather than the rates, because those are the only figures the registry itself produces.",
      "CAPMAS's published rates are not its registered births divided by its own population. The bulletin "
      "says so plainly: they are estimated with the fertility module of the Population Analysis "
      "Spreadsheets, a US Census Bureau tool that takes the number of births per 1,000 people and the "
      "female population and fits a standard pattern across the age groups. Dividing the registry's own "
      "counts instead gives 2.38 for 2024 against the published 2.41 — close in total, but spread very "
      "differently across ages: 143 births per thousand women aged 25-29 where CAPMAS publishes 164. That "
      "makes Egypt's line closer in kind to a UN estimate than to a pure count. The fertility table only "
      "appears from the 2019 edition onward and the counts by age of mother only from 2021, so the series "
      "is short. One age group is labeled 40-45 where it means 40-44.",
      "https://censusinfo.capmas.gov.eg/metadata-en-v4.2/index.php/catalog"),
    C("Brazil", "IBGE — Estatísticas do Registro Civil", brazil, "Brazil", "complete", True,
      "IBGE's statistics portal, SIDRA, gives registered births by the mother's age group and population "
      "projections by sex and single year of age. Both can be downloaded directly, without an account.",
      "We divided births by the women in each age group and added the results across the groups. Births "
      "come from SIDRA table 197 for 2000-02 and table 2612 from 2003 on. The older table has since been "
      "retired and no longer appears in the portal's own list, so the first three years cannot be checked "
      "from the link above.",
      "The 2000-02 points come from the older table and sit too low, because birth registration coverage "
      "was still improving — the step up in 2003 is coverage, not fertility. The 2024 figure is "
      "provisional. IBGE publishes two other fertility rates that are not this one: an assumption inside its "
      "population projections, and a figure of 1.55 for 2022 from the census, which it called the lowest "
      "ever recorded. Our registry figure for 2022 is 1.52 — close, but built a different way, from birth "
      "records rather than from asking women how many children they have had.",
      "https://sidra.ibge.gov.br/pesquisa/registro-civil/tabelas"),
    C("United Kingdom", "Three registration offices — births by the mother's age, over ONS's UK female "
      "population", uk_tfr,
      "United Kingdom", "complete", True,
      "No office publishes a fertility rate for the United Kingdom. Its three registration offices each "
      "publish one for their own part of it, and each publishes the births behind it by the mother's age: "
      "ONS for England and Wales, back to 1938; National Records of Scotland from 2000; and NISRA for "
      "Northern Ireland, by single year of the mother's age, back to 1974. ONS separately publishes "
      "mid-year population by single year of age and sex for the United Kingdom as a whole, 2011 to 2024.",
      "We built the UK rate ourselves, because nobody publishes it. We added the three offices' births in "
      "each age group, divided by the women of that age across the whole United Kingdom, and summed. Run "
      "on one country at a time the same arithmetic reproduces each office's own published rate: for 2024, "
      "1.415 against ONS's 1.415, 1.247 against Scotland's 1.250, and 1.602 against Northern Ireland's "
      "1.603.",
      "This is the one country here whose figure we assembled rather than read, so it is worth being clear "
      "about why. The UN publishes fertility for the United Kingdom and for nothing smaller, so a series "
      "for England and Wales — about 89% of UK births — was never a like-for-like comparison, and on a map "
      "it colored Scotland and Northern Ireland with a rate that excluded both. There is no England or "
      "England-and-Wales on the UN's side to compare against instead. The four nations differ a lot: in "
      "2024 Northern Ireland was at about 1.60 and Scotland at 1.25, with England and Wales between them "
      "at 1.42. The series runs from 2011 because that is where ONS's UK population estimates begin, and "
      "stops at 2024 because that is the last year all three offices have published; England and Wales "
      "alone reaches 2025. The youngest and oldest age groups are wider on the births side than on the "
      "women side, which is how all three offices build their own rates. The youngest counts births to "
      "every mother under 20, including those below 15, and divides them by the women aged 15 to 19; the "
      "oldest counts births at 40 and over and divides them by the women aged 40 to 44. Both therefore "
      "come out a little above a strict 15-to-19 or 40-to-44 rate. Following the same conventions, each "
      "group's rate here reproduces the one ONS prints, to the decimal ONS prints it to. One year does not "
      "line up as well as the rest — for 2023 our England "
      "and Wales figure comes out 0.016 below ONS's own, because the population estimates now published "
      "for that year are not the ones ONS used when it calculated the rate.",
      "https://www.ons.gov.uk/peoplepopulationandcommunity/birthsdeathsandmarriages/livebirths"),
    C("Germany", "Destatis — birth statistics by the mother's age, over the average resident female "
      "population", germany, "Germany",
      "complete", False,
      "Destatis publishes live births per 1,000 women for every single year of age 15 to 49, from 1972 to "
      "2025 (table 12612-0008), the births themselves by age of mother and birth order from 2009 (table "
      "12612-0005), and the average population over the year by single year of age, in its annual "
      "population report (table 12411-10).",
      "We added the rates across the ages. They are already per single year of age, so there is no "
      "age-group width to multiply by. The age-band comparison divides the births by the population from "
      "the third table, which is the population Destatis itself uses — the mean of the counts at the start "
      "and the end of the year.",
      "Two mistakes we checked for and avoided. The population table has one column for all residents and "
      "another for German nationals only; the nationals column is far smaller, and using it would push the "
      "rate much too high, so the all-residents column is the one that matches. And the births tables work "
      "out a mother's age by subtracting birth years, while the population tables use her exact age on a "
      "given date — mixing the two would shift the rate by about 1%, which is why the line stays Destatis's "
      "own summed rates rather than our arithmetic. Destatis also publishes the rate separately by the "
      "mother's citizenship, and the two are far apart, so the single national figure plotted here averages "
      "over a wide gap. Recalculated from 2012 onward using population totals updated by the 2022 census, "
      "so the numbers differ slightly from releases published before that update — our 2023 comes out at "
      "1.385 where the original release said 1.35. The 2025 figure is provisional. The age-band comparison "
      "only starts in 2009, because that is where the births table begins, and Destatis publishes the "
      "population report one year at a time.",
      "https://www-genesis.destatis.de/genesis/online"),
    C("Thailand", "National Statistical Office — Statistical Yearbook", thailand, "Thailand", "complete",
      True,
      "Thailand's Statistical Yearbook publishes registered births by the mother's age group (table 1.10) "
      "and registered population by age group and sex (table 1.4). It does not publish a total fertility "
      "rate anywhere, so there is no official figure to check ours against.",
      "For each five-year age group we divided the births by the women, then added the groups together and "
      "multiplied by five, the width of each group. The births shown for 45-49 include the yearbook's "
      "separate row for mothers of 50 and over — 1,086 plus 39 in 2023 — so that figure will not match any "
      "single line of the source.",
      "Because we build the rate ourselves rather than copying one, it is worth knowing that other Thai "
      "sources circulate a higher figure: about 1.16 for 2022 and 2023, against our 1.07 and 1.11. We "
      "cannot trace where that comes from. One likely part of the answer is the denominator — the "
      "yearbook's population table is a registration snapshot rather than the mid-year population usually "
      "used for this, and births to non-citizen residents may be registered less completely than the "
      "population count includes them. Both would push our figure low. Each yearbook edition prints only a "
      "rolling three-year window, and the two tables cover different windows, so the series is limited to "
      "the years where they overlap; older editions would extend it back to about 2017 but have not been "
      "read.",
      "https://www.nso.go.th/nsoweb/nso/statistics_and_indicators"),
    C("France", "INSEE — civil register births by the mother's age, over the mean resident female "
      "population", france, "France",
      "complete", False,
      "France's national statistics institute, INSEE, publishes fertility rates by single year of age of the "
      "mother and, separately, births by single year of age, both taken from the civil register, the "
      "government's record of every birth. Each table comes in two versions: one for the whole republic, "
      "including its five overseas departments, and one for mainland France and Corsica alone. INSEE's "
      "population pyramid gives women by single year of age at 1 January — for the whole republic from 1991, "
      "and for the mainland from 1901.",
      "We read the mainland tables, because that is the territory the UN's figures for France cover too: it "
      "counts Mayotte, Reunion, Guadeloupe, Martinique and French Guiana as places of their own. Across the "
      "thirty years the two sources overlap, our mainland figure and the UN's are 0.004 apart on average; the "
      "whole-republic figure is five times further off. We summed the rates across ages. The age-band "
      "comparison divides the births by the mean population over the year — the average of the 1 January "
      "figures for that year and the next — because that is how INSEE defines a fertility rate: births to "
      "women of a given age divided by the mean population of women of the same age over the year.",
      "INSEE marks 2023, 2024 and 2025 provisional and will revise them. Population estimates for earlier "
      "years also get revised in later releases — about 1% for one cohort we checked — so the rates and the "
      "population have to come from the same release. The mainland series has no territorial break: Mayotte "
      "entered the whole-republic figures in 2014, but was never part of the mainland ones.",
      "https://www.insee.fr/fr/statistiques/8999017"),
    C("Japan", "Ministry of Health, Labour and Welfare — vital statistics", japan, "Japan", "complete",
      False,
      "Japan's Ministry of Health, Labour and Welfare publishes the total fertility rate and each age "
      "group's share of it, and the births by the same age groups, through e-Stat, the government's "
      "statistics portal (tables 0003411608 and 0003411607). The Statistics Bureau separately publishes "
      "population by single year of age and sex every October.",
      "We took the ministry's published total fertility rate directly. For the age-band comparison we "
      "divided its births by the Statistics Bureau's female population, which is where the ministry says "
      "its own denominator comes from. That gives 1.145 for 2024 against the published 1.15.",
      "The rate is five-yearly before 2000 and annual after, so the earlier points are less precise. "
      "e-Stat gives each age group as its share of the total rather than as a rate per 1,000 women, so the "
      "seven shares add to the published total directly. The ministry's rate counts Japanese women only, "
      "not all residents, and the population file offers both columns — picking the wrong one adds the "
      "foreign resident population to the denominator and pulls the rate down. Even with the right column "
      "the match is not exact: a child born to a foreign mother and a Japanese father counts among the "
      "births but the mother never appears in a Japanese-only population, so a small mismatch survives. "
      "Births are published in five-year age groups for most of the series, which is why we do not rebuild "
      "the rate ourselves; a single-year table exists from 2015 but covers only births within marriage.",
      "https://www.e-stat.go.jp/dbview?sid=0003411608"),
    C("Italy", "ISTAT — national population register and civil status records", italy, "Italy",
      "complete", False,
      "ISTAT, Italy's national statistics office, publishes a birth rate for every single year of a "
      "mother's age, from 2000 onward, drawn from the national population register.",
      "We added those rates across the ages. They are already per single year of age, so there is no "
      "age-group width to multiply by.",
      "This figure covers all residents of Italy. ISTAT also publishes it separately for Italian citizens "
      "and for foreign citizens, and those two differ; the all-residents figure is the one plotted, and it "
      "is the one ISTAT headlines. Italy's 1.18 for 2024 is its lowest recorded, but only just: it had "
      "already reached 1.19 in 1995 before recovering to about 1.44 around 2010 and falling back. This "
      "chart starts in 2000, as every country here does, so that earlier trough is off the left edge. We "
      "do not have Italy's births or its female population broken down by age, so there is no age-band "
      "comparison for it.",
      "https://esploradati.istat.it/"),
    C("United States", "CDC / NCHS — natality via data.cdc.gov", united_states, "United States",
      "complete", False,
      "NCHS publishes age-specific birth rates per 1,000 women by maternal age group in two datasets: a "
      "historical one running 1940 to 2018, and a current one covering 2016 to 2024. It also publishes its "
      "own total fertility rate in the annual Births report.",
      "We summed the rates and multiplied by five, the width of each age group, which is exactly how NCHS builds its own "
      "total. The two datasets are stitched at 2016, where they overlap. Our figures reproduce NCHS's "
      "published totals to the decimal — 1.621 for 2023, 2.056 for 2000.",
      "The current dataset splits 15-19 into 15-17 and 18-19 as well as the whole band, which would "
      "double-count if not filtered, and labels the top band 45-54 where the rate is per woman aged 45-49. "
      "Births as the number of births by the mother's age do exist, in the annual Births report and in the record-level "
      "natality files, so a recalculation against Census population is possible — but which Census vintage "
      "you divide by moves the answer by about 1.4%, so it would no longer match NCHS. One thing to know "
      "before comparing with older reports: NCHS recalculated its rates for the 1990s and for 2000 and "
      "2001 once the 2000 census showed its population estimates had been too low. The figures here are "
      "the revised ones, so a reader who finds the report published at the time will see a higher number "
      "for 2000 — 2.130 rather than 2.056 — and that is the superseded one. The CDC website "
      "itself refuses automated requests; these two datasets come from a separate host that does not.",
      "https://data.cdc.gov/"),
    C("Russia", "Rosstat — Demographic Yearbook of Russia", russia, "Russia", "complete", False,
      "The yearbook is eight chapter spreadsheets behind an HTML index. Chapter 2 gives the total fertility "
      "rate for the whole country back to the early 1960s; chapter 4 gives the number of live births by the "
      "mother's age. A separate release gives female population by single year of age at 1 January.",
      "We read the national column of the fertility-rate sheet. Dividing the births by the population gives "
      "1.39 for 2022 against Rosstat's published 1.416. We used those counts for the age-band comparison, "
      "but the line on the chart is Rosstat's own figure.",
      "That 1.8% gap comes down to which population the births are divided by. Rosstat divides by the "
      "average across the year, but the only population count updated to match the 2020 census is a "
      "snapshot taken on 1 January, and that snapshot is slightly larger. There is a second mismatch of the "
      "same kind: Rosstat has not updated its published rates for 2011 to 2021 to reflect the 2020 census, "
      "while the population files it publishes all have been — so 2022 is the only year that is consistent "
      "with its own population count. The 2022 figure excludes the four annexed Ukrainian regions; Crimea "
      "has been included since 2014. The series stops at 2022 because that is the latest yearbook "
      "edition. If Rosstat publishes its average-population file for 2022, the gap should close "
      "and this figure could become one we have checked ourselves.",
      "https://rosstat.gov.ru/folder/12781"),
    C("Zambia", "ZamStats — Demographic and Health Survey", zambia, "Zambia", "survey", False,
      "ZamStats publishes a fertility rate from each survey round, the latest 4.0 for 2024. Its 2022 census "
      "reports 4.6, and publishes the raw births and women behind it in a separate tables volume.",
      "We use the rate each survey round published, as published. We also computed the census's uncorrected "
      "figure from its own counts: 3.35, against the 4.6 it prints.",
      "That is a 37% upward correction, and ZamStats explains why it made one. It compared how many children "
      "women said they had ever had against what a standard formula predicts, found that recent births had "
      "been under-reported, and corrected for it using a widely used method that scales the year's birth "
      "count up to match the more complete lifetime count. It says such a correction is necessary because "
      "Zambia's birth registration is too incomplete to measure fertility directly — only 31% of people "
      "under 50 have a registered birth. One oddity is worth recording: the report's own worked example of "
      "that comparison does not reproduce from the numbers it quotes. Its formula raises a ratio to the "
      "fourth power, which on its own inputs gives 5.66, but it prints 5.921 — the figure you get by "
      "multiplying by four instead. The corrected rates themselves are published, in an annex table, and "
      "adding them up does return the 4.6. The census and the survey also disagree in an informative way: "
      "4.6 for 2022 against 4.0 for 2024, a 0.6 fall in two years where the survey series had been falling "
      "about 0.12 a year.",
      "https://www.zamstats.gov.zm/"),
    C("Vietnam", "National Statistics Office — population change and family planning survey", vietnam,
      "Vietnam", "survey", False,
      "The office publishes a total fertility rate annually from 2001. Its online database stops at 2023, but "
      "the 2025 survey report prints the whole 2001-2025 series in one table. The report and the 2019 census "
      "volume also publish births by age of mother and population by age group, though only inside long PDFs.",
      "We read the whole-country column of that table. No arithmetic of our own.",
      "This is not a count of registered births. The office estimates the rate from a household sample "
      "survey and then raises it, using a standard demographic correction that assumes women under-report "
      "the births they had in the previous twelve months. Its own reports say they do this, but never print "
      "how large the correction is, and never print the figure before it is applied — so the published rate "
      "is closer in kind to a model estimate than to a count, and how far from a count cannot be worked out "
      "from anything published. We tried the same check on the 2025 survey, but its table counts the mothers "
      "who gave birth rather than the number of babies, so the comparison is not like for like and we have "
      "left it out. Some years come from a different instrument: 2024 is from the mid-term population and "
      "housing survey, not the annual one. Vietnam does have civil registration, but it is not what the "
      "published rate is built on.",
      "https://www.nso.gov.vn/"),
    C("Australia", "ABS — births by age of mother divided by its population estimates", australia_tfr,
      "Australia", "complete", True,
      "The Australian Bureau of Statistics publishes registered births by single year of the mother's age, "
      "female population by single year of age at 30 June, and its own fertility rate.",
      "We divided the births at each single age by the women of that age and summed. Our figures come out "
      "within a few thousandths of the bureau's own published rate in every year — 1.482 against its 1.481 "
      "for 2024, 1.497 against 1.499 for 2023 — because population estimates are revised after the bureau "
      "calculates a year's rate, so our denominator is never quite the one it used.",
      "Australia's rate is at a record low. The one caveat that matters is registration timing: the bureau counts "
      "births by the year they were registered, and two states have had backlogs. Victoria registered nearly "
      "a quarter of its 2024 births as having occurred in an earlier year, and clearing a 2023 processing "
      "delay pushed its 2024 registrations up 12.9% against a national rise of 1.9% — the bureau says outright that "
      "much of that is administrative rather than real, and warns against comparing years. Western Australia "
      "cleared a backlog in early 2025 that also lands in 2024. The bureau does publish a series counted by "
      "the year each birth happened rather than the year it was registered, but it is far too incomplete to "
      "use for recent years: it has only 248,159 births for 2024 against 292,318 on the registration basis.",
      "https://www.abs.gov.au/statistics/people/population/births-australia/latest-release"),
    C("Bangladesh", "BBS — Sample Vital Registration System", bangladesh, "Bangladesh", "sample", False,
      "The 2023 SVRS report carries a table of every year from 1982 to 2023, giving the total fertility "
      "rate alongside four other fertility measures. Each annual edition also states its own year's figure "
      "in the text, with a range showing how far out it could be.",
      "We read the whole 42-year table out of the report. We also summed the age-specific rates for 2023 "
      "and got 2.18 against the printed 2.175 — the difference is rounding in the published rates.",
      "The SVRS is a sample of about 2,000 areas where resident registrars record births monthly — a "
      "continuous sample system, not full civil registration. The health survey run by the government's "
      "population institute puts the rate higher — by about 0.1 in 2022 and more in earlier rounds — because "
      "it asks women to recall past births "
      "and averages three years, so the two are not measuring quite the same thing. BBS publishes no "
      "birth or population counts by age, only rates, so there is nothing to recalculate from. Everything "
      "is PDF only, and the files sit on cloud storage rather than on the office's own site.",
      "https://bbs.gov.bd/"),
    C("Indonesia", "BPS — censuses and surveys taken between them", indonesia, "Indonesia", "survey", False,
      "BPS, Indonesia's statistics agency, publishes a total fertility rate for censuses and the surveys "
      "it runs between them, and for nothing else. Its 2025 survey "
      "release charts all four rounds together: 2.41 for the 2010 census, 2.28 for the 2015 survey, 2.18 "
      "for the long form of the 2020 census, and 2.13 for the 2025 survey.",
      "We read all four values out of that chart. An earlier version of this page had 2.42 for the 2020 "
      "long form, which is wrong — BPS's own figure is 2.18, and 2.41 belongs to the 2010 census.",
      "None of these figures come from birth records: Indonesia's system for officially recording births is "
      "still incomplete, so BPS instead asks each woman how many children she has had. Each round is "
      "plotted at the year BPS names it for, which is not always when it happened — the 2020 census's "
      "detailed questionnaire was actually carried out in 2022. BPS's main website refuses automated "
      "requests, so this release had to be read from a saved copy on the Internet Archive. A separate "
      "BPS census website does load, but its fertility tables give the children a woman has had over her "
      "whole life rather than in one year, so there is nothing there to check this number against.",
      "https://www.bps.go.id/id/pressrelease/2026/05/05/2645/"),
    C("Pakistan", "PBS — Pakistan Demographic Survey", pakistan, "Pakistan", "survey", False,
      "The Bureau of Statistics ran the Demographic Survey most years until 2007 — it is designed to run "
      "every other year between censuses — and then again in 2020. Every "
      "edition still online prints a total fertility rate: 4.1 in 2001 falling to 3.7 in 2007, and 3.7 again "
      "in 2020. The 2007 round also publishes live births and female population by age group as raw numbers.",
      "We read each edition's own figure out of its report. For 2007 we also did the arithmetic from the "
      "counts: births divided by women in each age group, summed and multiplied by five, the width of each age group, gives "
      "3.69 — the Bureau's published 3.7.",
      "Pakistan has no usable civil registration for this. The thirteen-year gap from 2008 to 2019 is one "
      "the Bureau explains itself: the survey was skipped while the 2017 census kept being postponed, "
      "and when that census went ahead without covering fertility, it revived the survey to fill the "
      "hole. The 2020 figure covers "
      "2018-20 rather than one year. The 2020 report has a trap: a table titled live births by age of "
      "mother holds children ever born, not one year's births, and totals over 100 million. The health "
      "survey run by the National Institute of Population Studies gives 3.6 for 2017-18.",
      "https://www.pbs.gov.pk/pds/"),
    C("China", "NBS — census yearbooks, table on fertility by age and birth order", china_tfr, "China",
      "survey", True,
      "For each of the last three censuses the statistics bureau publishes a downloadable table giving, for "
      "every single year of age from 15 to 49, the number of women of childbearing age, the number of "
      "births they had in the year before the count, and the resulting rate. The bureau's director "
      "separately stated a fertility rate of 1.3 for 2020 at the census press conference.",
      "We divided births by women at each age from 15 to 49 and added the results. This gives 1.221 for "
      "2000, 1.188 for 2010 and 1.296 for 2020. Grouping the same table's ages into five-year groups "
      "instead gives 1.301 for 2020, which is what the press conference rounded to 1.3.",
      "This series covers census years only, once a decade. Between censuses the bureau runs a smaller "
      "annual survey covering about one person in a thousand, and its age breakdown appears only in a "
      "yearbook that is not free — so the gap years cannot be filled, and the line jumps ten years at a "
      "time. The figures come from the households given the detailed census questionnaire, not from the "
      "full population count, and they cover the twelve months to 31 October of the census year rather "
      "than the calendar year. Some Chinese demographers argued the 2010 census figure of 1.18 was "
      "implausibly low because births went unreported; others concluded under-reporting was not the "
      "explanation.",
      "https://www.stats.gov.cn/sj/pcsj/rkpc/7rp/zk/html/B0603.xls"),
    C("Nepal", "National Statistics Office — 2021 census", nepal, "Nepal", "survey", False,
      "The National Statistics Office publishes a fertility rate of 1.94 for 2021, and a series back to 1971. "
      "It also publishes online the counts behind the census: births in the twelve months before enumeration "
      "by the mother's age group, and women by age group.",
      "We show the office's published figure. Dividing its own counts gives 1.556 instead — about a fifth "
      "lower — and the office says why. A check it ran after the census found children under five had been "
      "undercounted, which makes the count of births in the previous year too low. So rather than tabulate "
      "those births directly, it estimated fertility from the number of children women report ever having "
      "had, comparing the 2011 and 2021 censuses, using a standard indirect method named after its author, "
      "Arriaga. That is where 1.94 comes from.",
      "The two figures are the raw count and the corrected estimate, not a contradiction, and the correction "
      "is the one to trust here: the office's own comparison points out that its household survey put "
      "fertility at 2.1 and another survey at 2.0, both close to the corrected 1.94 and far above the raw "
      "1.556. Nepal has estimated fertility this way at every census since 1971 rather than counting births "
      "directly, so the gap between the two is a permanent feature of these numbers rather than a problem "
      "with 2021. The age-band comparison below shows the raw counts, which is why adding them up does not "
      "give the plotted figure. Nepal's birth records are not a usable substitute either: the census found "
      "26% of children under six had never had their birth registered.",
      "https://censusresults.nsonepal.gov.np/fertility"),
    C("Venezuela", "INE — projection-based fertility series", venezuela, "Venezuela", "projection", False,
      "Venezuela's national statistics institute, INE, publishes a fertility series in a statistics summary "
      "dated August 2024: 2.9 for 2000 falling to 2.3 for 2015, then 2.2 and 2.1 drawn as forward "
      "projections. The health ministry separately publishes registered births by age of mother, but only up "
      "to 2014.",
      "We take the series up to 2015 and leave out the years INE draws as projections. Dividing the "
      "ministry's 2014 births by INE's estimate of women aged 15 to 49 gives about 2.25, close to INE's 2.3 "
      "for 2015.",
      "Every point here comes out of one projection exercise, built on the 2011 census and calculated in "
      "2013 — including the years before 2015, which INE draws as a solid line but which are model output "
      "just the same. Nothing has been re-estimated from current registration. So the cutoff we use is not a "
      "line between measurement and projection; it is simply where INE itself stops drawing the series as "
      "elapsed time. The 2015 figure was not published until 2024, nine years after the year it describes. "
      "The general statistical yearbook stops at 2003, and INE has been digitizing volumes from 1909 to 1944 "
      "rather than adding recent ones. The health ministry's births-by-age tables stop at 2014, published in "
      "2018. A university-run household survey has reported fertility figures for later years, which we have "
      "not used because we could not establish that they are measured comparably.",
      "https://ine.gob.ve/wp-content/uploads/2024/08/Resumen_de_estadisticas_1999-2023.pdf"),
    C("Niger", "INS — national fertility and health surveys", niger, "Niger", "survey", False,
      "Niger's national statistics office, INS, publishes a fertility rate from each survey round: 7.1 for "
      "2006, 7.6 for 2012 and 6.2 for its 2021 fertility survey. Its 2012 census gives 7.5, and also "
      "publishes the underlying birth and women counts.",
      "We read the survey rounds. We also checked the census: dividing its own counts gives 7.476 against "
      "the published 7.5.",
      "Niger has the highest fertility in the world, and its 2012 census is one of several cases here where "
      "an office worked out a correction and then chose not to use it. A standard adjustment for "
      "under-reported births, the relational Gompertz model, would have raised the figure from 7.5 to 7.8. "
      "The report rejected it on a technical test — one of the model's own fit parameters fell outside the "
      "range its authors recommend — and kept the figures collected in the field, so the published number is "
      "the lower one. The census also examined whether births in its twelve-month window had been "
      "under-declared, and concluded they probably had not: comparing its 7.5 against the surveys' 7.6 for "
      "2012 and 7.1 for 2006 argued against under-declaration rather than for it. Civil registration is not "
      "used nationally; INS does compute a rate from registrations for Niamey alone, where completeness is "
      "69%, and it comes out above the survey figure for the same city — 4.8 against 4.2, though the two "
      "cover different periods. The fifth census had reached its post-pilot stage by mid-2026, more than "
      "thirteen years after the fourth. One INS chart gives 7.0 and 7.2 for 1992 and 1998 where its own "
      "primary tables give 7.4 and 7.5; the tables are right.",
      "https://stat-niger.org/"),
    C("North Korea", "Central Bureau of Statistics — 2008 census and 2014 survey", north_korea,
      "North Korea", "survey", False,
      "The bureau's 2008 census publishes the underlying birth and women counts as tables without stating a "
      "rate; the rate of 2.01 appears in its later work. Its 2014 socio-economic and health survey reports "
      "1.89. A 2017 household survey gives 1.9.",
      "We read both plotted figures. We also checked the census: dividing its own counts gives 2.008 against "
      "the 2.01 the bureau states elsewhere.",
      "These figures are the bureau's own work, even though none of the reports survives on a North Korean "
      "server — the census foreword is signed by its director-general and credits the UN population fund only "
      "for material and technical support. There is no North Korean statistics website; the reports are kept "
      "by UN bodies and in web archives instead. We use them because of who produced them, not where they "
      "are now kept, which is also why a UN estimate would not count here. The 2017 survey is the one figure "
      "we have not plotted: it was run by the bureau but published by UNICEF, and by the same test we apply "
      "to the census it arguably belongs on the line — we have not added it pending a look at the report "
      "itself. Nothing newer than 2017 has appeared. Two loose ends: the bureau's own publications give 1993 "
      "as both 2.1 and 2.20, and the only analysis of that census available anywhere is the US Census "
      "Bureau's, which is not a North Korean source and is not used here.",
      "https://dprkorea.un.org/en"),
    C("Nigeria", "National Population Commission — Nigeria Demographic and Health Survey", nigeria,
      "Nigeria", "survey", False,
      "The 2024 survey report sets every round since 2003 side by side in one table: 5.7 in 2003 and 2008, "
      "then 5.5, 5.3 and 4.8. The National Bureau of Statistics separately publishes a row it calls "
      "Calculated TFR — a filled-in estimate for the years between surveys rather than a new "
      "measurement — and its own 2021 household survey found 4.6.",
      "We read the trend table out of the survey report. Summing its age-specific rates and multiplying by "
      "five, the width of each age group, gives 4.79, the published 4.8.",
      "These are measured rates from women's birth histories, not registration — birth registration in "
      "Nigeria is far too incomplete to use. Each round's figure covers the three years before its "
      "fieldwork, so each point sits later than the fertility it describes. We do not use the Calculated TFR "
      "row, because it is drawn as a straight line between survey rounds — which is why it falls by almost "
      "the same amount every year — rather than measured. Counts of women by age exist only as "
      "projections from the 2006 census, and they do not match the number of women the survey itself "
      "counted, about 11% apart, so there is nothing solid to check the rate against.",
      "https://nationalpopulation.gov.ng/publications"),
    C("Iran", "Statistical Center of Iran — trend of fertility, 1396 to 1400", iran, "Iran",
      "incomplete", False,
      "The Statistical Center of Iran published a fertility report in 2022 covering the Iranian years 1396 "
      "to 1400, which are 2017-18 to 2021-22. Its table 3 gives a total fertility rate for each year on "
      "four different bases; we plot the one computed for the whole population, Iranian and non-Iranian "
      "residents together, which is the population the UN's figure also covers.",
      "We read that column: 2.07, 1.97, 1.77, 1.71 and 1.74. The office computed it itself, by adding up "
      "birth rates for each age group, so the figure is its own rather than ours.",
      "The office builds this from civil registration but says plainly that registration coverage is "
      "incomplete, and that births to non-Iranian residents were badly under-recorded in the earlier years "
      "— which is why it patches in the health ministry's birth records for them, and why its "
      "Iranian-nationals-only series runs lower from 1397 on: 2.09, 1.95, 1.74, 1.65, 1.65. Nothing more "
      "recent than 1400 was found. Getting even this much took a web archive, because none of the three "
      "government hosts can be reached from outside the country, and each fails differently. The statistics "
      "center at amar.org.ir accepts a connection and then drops it without answering, the same way "
      "whatever browser settings are used — which looks like something at the edge of the network refusing "
      "foreign traffic rather than a problem with the site. Its other address, sci.org.ir, answers but "
      "serves a maintenance notice, so it is worth retrying later. The civil registration organization at "
      "sabteahval.ir answers with a security check that asks for phone-motion data a script cannot "
      "provide, so a person browsing normally might get through where we could not. Do not use nocr.ir: it "
      "is no longer that organization's address and now points at a domain reseller.",
      "https://www.amar.org.ir/"),
    C("Turkey", "TurkStat — birth statistics by the mother's age, over the women in the address-based "
      "population register", turkey_tfr,
      "Turkey", "complete", False,
      "TurkStat's annual birth statistics bulletin publishes a birth rate for each age group of mother, and "
      "the births themselves, both from 2001. An older population portal gives female population by "
      "five-year age group back to 1935 — from censuses until 2006, and from the address-based population "
      "register from 2007, when that register began.",
      "We added the rate for each age group and multiplied by five, the width of each group, giving 1.484 "
      "for 2024 against the 1.48 TurkStat states. For that one year we also checked it from the counts: "
      "dividing the registered births by the register's female population gives 1.4832, which rounds to the "
      "same 1.48. The rest of the line is TurkStat's own figure, not ours, which is why this counts as "
      "copied rather than recalculated. The age-band comparison uses those counts.",
      "TurkStat splits the teens into 15-17 and 18-19, which we combine into one 15-19 group. It also "
      "revises birth figures for up to five years after "
      "first publishing them, marking each revised row with an 'r', so different editions of the same "
      "bulletin disagree with each other. And because the population figures switch from censuses to the "
      "register in 2007, the same table mixes two kinds of data — the age-band comparison therefore cannot "
      "start earlier than 2007.",
      "https://data.tuik.gov.tr/Bulten/Index?p=Dogum-Istatistikleri-2024-54196"),
    C("Ethiopia", "Ethiopian Statistical Service — Demographic and Health Survey", ethiopia, "Ethiopia",
      "survey", False,
      "The Ethiopian Statistical Service publishes a total fertility rate for each survey round. The "
      "2024-25 report's trend figure prints the national value for all five rounds since 2000, and its "
      "table 3 breaks the latest one down by the mother's age.",
      "We read the five round values off the trend figure. Adding up the rate for each age group and "
      "multiplying by five, the width of each group, gives 4.05 — the published 4.0.",
      "These are household surveys asking women about past pregnancies, not registration. Each point "
      "covers the three years before the survey, so it sits later than the fertility it describes: the "
      "point plotted at 2024 is really about 2021 to 2024. Ethiopia's last "
      "completed census was 2007 and the next has been postponed repeatedly, so there is no recent "
      "count of women by age to divide by — the 2007 census is the only one, and it is nineteen years "
      "old. The old statsethiopia.gov.et domain is dead; the service is now at ess.gov.et.",
      "https://ess.gov.et/wp-content/uploads/2026/01/edhs-2024-25-kir-01172026.pdf"),
    C("Cote d'Ivoire", "ANSTAT — household survey rounds", cote_divoire, "Cote d'Ivoire", "survey",
      False,
      "Côte d'Ivoire's national statistics agency publishes a fertility rate from each round of its "
      "Demographic and Health Survey: 5.3 in 1994, 5.2 in 1998-99, 5.0 in 2011-12 and 4.3 in 2021, each "
      "with the rates by age group behind it. A separate survey program it also runs, the Multiple "
      "Indicator Cluster Survey, gives 4.6 for 2016.",
      "We use the rate each round published. Summing each round's own rates reproduces its total — 4.95 "
      "against the published 5.0, and 4.595 against 4.6.",
      "The 2021 census publishes no fertility rate at all, and not even births by age of mother. Its results "
      "report lists fertility as one of the thematic volumes to come later, and that volume has not "
      "appeared. So the survey rounds are the only source, and because the 2016 figure comes from a "
      "different survey program than the rest, it is not strictly comparable with the points on either side "
      "of it. Civil registration is not used for fertility, and the agency explains why by publishing "
      "coverage instead: 55% of children under five registered in 2006 and 65% in 2011-12. A 2016 figure of "
      "72% is often quoted alongside those, but it appears to count children holding a birth certificate, "
      "which is a narrower thing than being registered. The agency itself was reorganized in 2024, when INS "
      "became ANSTAT.",
      "https://dhsprogram.com/pubs/pdf/SR280/SR280.pdf"),
    C("Democratic Republic of Congo", "National Institute of Statistics — Demographic and Health Survey",
      drc, "Democratic Republic of Congo", "survey", False,
      "The National Institute of Statistics publishes a fertility rate for each survey round. The "
      "2023-24 report gives 5.5 and states the previous round measured 6.6.",
      "We took both figures as stated. We checked the 2023-24 figure by adding up the report's birth rate "
      "for each five-year age group, then multiplying by five, the number of years in each group. That "
      "gives 5.47, close to the published 5.5. The report's own appendix, which recomputes the rate "
      "allowing for the way the survey sample was drawn, gets 5.47 as well, with a range of 5.23 to 5.71.",
      "There has been no census since 1984 and no civil registration good enough to use, so every "
      "national fertility figure comes from one of the three surveys. Each figure describes roughly the "
      "three years before the survey was in the field rather than the year we plot it at: the institute "
      "attributes the 6.6 to 2011-2013, and the 5.5 covers about 2021-2023. The age breakdown comes from "
      "the same survey, not a separate count of the population, so we have no independent numbers to check "
      "the rate against.",
      "https://ins.gouv.cd/publication/RDC-EDS-III.pdf"),
    C("Syria", "Central Bureau of Statistics — family health surveys", syria, "Syria", "survey", False,
      "The bureau's statistical abstract carries one fertility table, giving rates by age group for its 2001 "
      "and 2009 family health surveys: 3.8 and 3.5.",
      "We read both, from the original spreadsheet inside the abstract's own chapter download. The rates sum "
      "to the printed totals.",
      "Each of these two figures is an average over the three years before its survey, as the source's own "
      "table title says, rather than a single year's rate like the UN line beside it. Nothing has been added "
      "since 2009. Every edition we could find — 2016, 2017, 2019 and 2020 — reprints this same table "
      "unchanged. The registered-births tables next to it were being updated as late as 2019, but they break "
      "the data down by sex and province only, never by the mother's age, so a newer rate cannot be built "
      "from them. The bureau's website is no longer online, so this comes from an archived copy; it kept "
      "publishing for months after the change of government in December 2024, with fresh pages as late as "
      "July 2025. The health ministry's site is live and modern but publishes only disease surveillance.",
      "https://web.archive.org/web/20241209121919/http://cbssyr.sy/yearbook.htm"),
    C("Tanzania", "National Bureau of Statistics — 2022 Demographic and Health Survey", tanzania,
      "Tanzania", "survey", False,
      "For 2022 the statistics offices of the mainland and Zanzibar published two different figures: 4.6 "
      "from the census, and 4.8 from the Demographic and Health Survey they also run. Their own census "
      "report picks between them, recommending that the survey's 4.8 be used as the official rate.",
      "We plot the 4.8 the offices name as official. We also checked the census's arithmetic, because it is "
      "the more interesting number: its report prints the rates for each age group twice, as women reported "
      "them and after correction, and they sum to 3.195 and 4.63 — matching the 3.2 and 4.6 it prints.",
      "That gap between 3.2 and 4.6 is entirely the correction. The census asked women about births in the "
      "previous twelve months, then raised the answers using a standard demographic technique, on the "
      "grounds that women under-report and misdate recent births — and the report says plainly that the "
      "country's system for recording births directly is too incomplete to compute a rate from. So the "
      "census figure could be rebuilt from its own counts, but doing that would reproduce the uncorrected "
      "3.2 rather than either published figure. We show the survey instead because that is what the "
      "offices asked for, and because it measures fertility from women's full birth histories rather than "
      "from a corrected twelve-month recall.",
      "https://www.nbs.go.tz/uploads/statistics/documents/en-1752866506-Fertility%20and%20Nuptiality.pdf"),
    C("Spain", "INE — birth statistics by the mother's age, over the women in the continuous population "
      "count", spain_tfr, "Spain",
      "complete", True,
      "Spain's national statistics office, INE, publishes registered births by single year of age of the "
      "mother from 2009 onward, and female population by single year of age — twice a year from 1971, and "
      "every quarter since 2021. Both are free to download from its website.",
      "We divided births at each single age by the women of that age on 1 July and summed. Our figures "
      "land within 0.01 of INE's own published fertility indicator every year — 1.107 against 1.10 for "
      "2024, 1.122 against 1.12 for 2023.",
      "The population series is INE's continuously updated population count, which replaced the older "
      "municipal register figures. Births start in 2009 on this table, so the line is shorter than the "
      "population series behind it. Recent years can still move a little, because INE keeps revising that "
      "population as registrations arrive.",
      "https://www.ine.es/dyngs/INEbase/es/operacion.htm?c=Estadistica_C&cid=1254736177007"
      "&idp=1254735573002"),
    C("South Korea", "Ministry of Data and Statistics — annual birth statistics release", korea_tfr,
      "South Korea", "complete", False,
      "South Korea's statistics agency — Statistics Korea until October 2025, and the Ministry of Data and "
      "Statistics since — publishes its birth statistics in English every year as a PDF. Each edition carries "
      "a table with an eleven-year run of the total fertility rate, and some editions also print live births "
      "by the mother's age group as raw counts. The interior ministry separately publishes female population "
      "by five-year age group from the resident register, the national record of where people live, as a "
      "spreadsheet download.",
      "We chained six editions of the release to build 2000-2024, taking the newer edition wherever two "
      "overlap because the office revises. For 2025 we used the agency's own preliminary release, which comes "
      "out each February for the year just ended. As a check we recalculated 2015 from the counts — births by "
      "age group from the release, women by age group from the resident register — and got 1.245 against the "
      "published 1.24.",
      "The 2025 figure is preliminary. The agency publishes its final figure for a year the following August, "
      "so 2025 will be revised. Recent editions dropped the full births-by-age table, so a recalculation for "
      "the latest year is not possible from them. The years before 2005 are printed to three decimals and the "
      "rest to two, because that is how the editions they come from print them.",
      "https://mods.go.kr/board.es?mid=a20108010000&bid=11773"),
    C("Cameroon", "INS — Demographic and Health Survey", cameroon, "Cameroon", "survey", False,
      "Cameroon's national statistics office, INS, publishes a fertility rate from each survey round: 5.1 "
      "for 2011 and 4.8 for 2018. The 2005 census reports 5.2, and prints the uncorrected figure of 4.1 "
      "alongside it.",
      "We read the two survey rounds. We also checked the census arithmetic: summing its corrected rates by "
      "age group gives 4.16 for urban Cameroon and 6.21 for rural, matching its own printed 4.1607 and "
      "6.2130.",
      "Cameroon's newest published fertility figure is the 2018 survey's, and it has had no census result "
      "since 2005. That is about to change. The fourth census was launched in 2016 and then delayed for "
      "years, partly over how to enumerate the North-West and South-West regions, which have been affected "
      "by conflict; it finally went into the field in 2026, and the government extended the count by "
      "decree to 31 July 2026. Nothing from it has been published yet. A new household survey was in the "
      "field through 2026 as well, so two more recent figures should follow. The "
      "2005 census corrects its raw figure upward, from 4.1 to 5.2, using a standard demographic method for "
      "births women forget or misdate; both the raw and the corrected figures appear in the volume, and the "
      "results chapter's trend table uses the corrected ones. Civil registration is not used: INS's own "
      "vital statistics report says the system's coverage problems mean it uses the survey instead, and puts "
      "birth registration completeness at 54%, ranging from 92% in one region to 35% in another.",
      "https://ins-cameroun.cm/"),
    C("Burkina Faso", "INSD — Enquête Démographique et de Santé", burkina_faso, "Burkina Faso", "survey",
      False,
      "Burkina Faso's national statistics office, INSD, publishes a fertility rate from each survey round — "
      "5.9 for 2003, 6.0 for 2010 and 4.4 for 2021 — and separately from its 2019 census, which gives 5.4 "
      "and prints the underlying birth and women counts.",
      "We use the rate each survey reported. Summing the census's own published rates reproduces its 5.4. "
      "The 2003 round originally headlined 6.2, measured over the five years before it; the 5.9 we plot is "
      "the office's own later recalculation of that round over three years, which is the window the other "
      "rounds use, so all three points sit on the same basis.",
      "The census and the survey are a full child apart two years apart: 5.4 for 2019 against 4.4 for 2021. "
      "We found nothing that reconciles them, though the census volumes were published before the survey's "
      "report, so they could not have. We use the survey series because it is the more recent and the more "
      "internally consistent run. But part of that fall may be coverage rather than fertility: insecurity "
      "kept the 2021 survey out of 86 of its 600 sampled areas, concentrated in the two highest-fertility "
      "regions of the country — two thirds of the sampled areas in the Sahel region and a third in the East "
      "were never visited, and five provinces were dropped altogether. The report warns against relying on "
      "those regions' own figures but says nothing about what it does to the national one. Unusually, the "
      "census's correction lowered its figure rather than raising it: INSD found an implausible spike in "
      "fertility at ages 45-49 in its raw data, tested one standard correction method which gave 5.8, and "
      "adopted a different one instead, which moved the national figure from 5.5 to 5.4 and cut the urban "
      "figure from 4.5 to 4.1. Civil registration is not used for fertility; the 2021 survey found 85% of "
      "children under five registered and 73% holding a certificate.",
      "https://www.insd.bf/"),
    C("Canada", "Statistics Canada — births by age of mother divided by its population estimates", canada_tfr,
      "Canada", "complete", True,
      "Statistics Canada publishes registered live births by age group of mother, female population by "
      "age group at 1 July, and its own fertility rate — all three as open, downloadable files, annually "
      "from 1991.",
      "We divided the births by the female population in each age group and summed. Our figures land within "
      "0.01 of Statistics Canada's own published rate in every year of the series, and the largest gap "
      "anywhere is 0.009. That is close but not identical: in about a third of the years the two fall on "
      "opposite sides of a rounding boundary, so our 2024 figure of 1.255 shows as 1.26 where Statistics "
      "Canada publishes 1.25.",
      "Births are dated by the year they occurred, not the year they were registered, and late "
      "registrations are added back to the year of the birth when the table is revised each year — about a "
      "thousand cases five years on — so recent years are not too low for that reason. The 2024 figure is "
      "described as preliminary in the table's notes and may be revised. Nova Scotia under-recorded births "
      "in 2021. For confidentiality, Statistics Canada does not show births to mothers of 50 and over "
      "separately; it puts them in with the births whose mother's age was not recorded, and we spread that "
      "whole group across the seven age bands in proportion to their size. Canada's population count "
      "includes non-permanent residents as well as citizens, and the number of women aged 20-24 rose 9% "
      "between 2019 and 2024, partly because of student migration — so some of the fall in the rate is "
      "there being more women of that age rather than fewer babies per woman.",
      "https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1310041601"),
    C("Yemen", "Health ministry and CSO — National Health and Demographic Survey 2013", yemen, "Yemen",
      "survey", False,
      "Only one figure falls in the years covered here: 4.4 children per woman for the three years to 2013, "
      "from the survey the Ministry of Public Health and Population ran with the Central Statistical "
      "Organisation. The 2004 census is also covered, but two different figures are published for it — 4.93 "
      "and 6.1.",
      "We read the national column of table 2. Adding up its birth rates for each five-year age group and "
      "multiplying by five gives 4.43, the published 4.4.",
      "We use the 2013 survey rather than the census because the census does not give one number. The 4.93 "
      "comes from counting the births women reported. The 6.1 comes from an indirect estimate, worked out "
      "from the number of children women said they had ever had — and the rates by age group printed "
      "alongside it appear to have been scaled to match that estimate, so the two are not independent "
      "calculations that happen to agree. Picking between 4.93 and 6.1 means weighing the methods rather "
      "than reading a number off a table, so the survey — one figure, from the standard approach of asking "
      "women about the children they have had — is the safer choice. Nothing has been published since 2013: "
      "the war began in 2015, and the office's own websites have not survived it.",
      "https://web.archive.org/web/20220608201754/https://cso.gov.ye/about_cso"),
    C("Angola", "INE — Multiple Indicator and Health Survey", angola, "Angola", "survey", False,
      "Angola's national statistics office, INE, publishes a fertility rate for each round of its Multiple "
      "Indicator and Health Survey: 6.2 from the 2015-16 round and 4.8 from the 2023-24 one, with the rates "
      "by age group behind each. The 2014 census separately gave 5.7.",
      "We use the national figure each round published; adding up its printed rates by age group gives the "
      "same totals back, 6.215 and 4.78.",
      "We use the two surveys rather than the census, because they measure the same way: from the birth "
      "histories women give, with no correction applied. INE's own population projection re-derived the 2014 "
      "census figure as 5.5 rather than 5.7, using a standard statistical correction, because it found women "
      "had under-reported the children they had ever had and over-reported births in the last twelve "
      "months. The census and the survey also disagree by more than the time between them explains: 5.7 for "
      "2014 against 6.2 from a survey whose own three-year window reaches back to about 2013. Angolan civil "
      "registration cannot be used at all — the 2023-24 survey found only 38% of children under five "
      "registered, and only 36% holding a birth certificate. We could not check these rates against actual "
      "birth counts, because we found no Angolan source that publishes births by the mother's age rather "
      "than rates. The 2024 census has published definitive results with no fertility figure at all; a later "
      "volume from it may add one.",
      "https://www.ine.gov.ao/publicacoes/Todas?titulo2=IIMS"),
    C("Argentina", "Health ministry births by the mother's age, over INDEC's female population",
      argentina_tfr, "Argentina",
      "complete", True,
      "The health ministry publishes registered live births by age group of mother as open CSV, annually "
      "from 2005. INDEC publishes female population by age group in two projection series, one based on the "
      "2010 census and one on the 2022 census. What INDEC does not publish is an annual fertility rate: its "
      "only figures are four projected years, starting at 1.27 for 2025.",
      "We divided the births by the female population in each age group and summed. Because there is no "
      "official annual rate to take, this is the only way to get a year-by-year figure for Argentina. Our "
      "2014 value of 2.35 lines up with the 2.36 the national identity registry publishes for that year.",
      "Argentina's fall is the steepest in this whole dataset — from 2.38 in 2010 to 1.19 in 2024, halving in "
      "fourteen years. Argentina's population figures switch source partway along: the years before 2022 rest "
      "on estimates projected from the 2010 census, and 2022 onward on the 2022 census. We have not adjusted "
      "for the switch, so there may be a small artificial step right at 2022. On registration, the ministry "
      "says more than 95% of births are registered within three months — but it also reports that comparing "
      "registered births against the 2010 census left a shortfall of about 6%, still 3.8% after four years of "
      "late records arriving. So the counts are close to complete but never quite get there. For 2023 the "
      "ministry warns separately that provinces had delays and difficulties sending in their figures, which "
      "matters because the recent years are the ones driving the fall. Mothers whose age was not stated — "
      "under 1% in recent years and falling — are spread across the bands. The top group is open-ended at 45 "
      "and over, treated here as 45-49. The series starts in 2010 because that is where INDEC's population by "
      "age begins; births go back to 2005.",
      "https://datos.salud.gob.ar/dataset/nacidos-vivos-registrados-por-jurisdiccion-de-residencia-de-la-madre-republica-argentina-ano"),
    C("Afghanistan", "CSO and Ministry of Public Health — Demographic and Health Survey 2015", afghanistan,
      "Afghanistan", "survey", False,
      "One figure: 5.3 children per woman for the three years to 2015, from the survey the Central "
      "Statistics Organization ran with the health ministry. The same table gives the age-specific rates "
      "behind it, and the report breaks the figure down by all 34 provinces.",
      "We read the national column of table 5.1. Adding up the birth rates for each five-year age group of "
      "women and multiplying by five, the number of years in each group, gives 5.29 — the published 5.3.",
      "Afghanistan has no birth registration reliable enough to use, so the only figure comes from a survey, "
      "and it is now over a decade old. The office that ran it has since been renamed the National Statistics "
      "and Information Authority, which is still publishing, but we found nothing newer on fertility. A 2022 "
      "household survey is reported to have measured 5.4; we could not reach the report to check it, so it is "
      "not plotted. The survey's figures come from interviewing a sample of women and scaling the results up "
      "to the whole country rather than counting everyone, and we did not find an Afghan count of women by "
      "age group to divide the births by independently.",
      "https://web.archive.org/web/20170511114715/http://cso.gov.af/Content/files/"
      "Afghanistan%20DHS%202015%20KIR/AFDHS_Final%20Report.pdf"),
    C("Algeria", "ONS — Démographie Algérienne", algeria, "Algeria", "incomplete", False,
      "The annual bulletin's main indicators table gives a fertility index for most years from 2002, "
      "alongside births and age-specific rates. Female population by five-year age group is in the same "
      "bulletin.",
      "We read the index row out of the 2019 edition, which carries the whole span in one table. Adding up "
      "the seven age-group rates and multiplying by five — the width of each group — reproduces the published "
      "3.0 for 2019 exactly, and multiplying those rates by the female population implies 1,032,000 births "
      "against the 1,034,000 the same bulletin reports.",
      "ONS stopped publishing the fertility index after 2019. The 2020-2023 edition dropped the whole "
      "fertility section — the term survives only in the glossary, with no number attached — so the series "
      "ends there. 2019 is also the last year ONS adjusted registered births upward to allow for births that "
      "are never registered, using correction factors it has not updated since 2002. The birth total is a "
      "registration count, but the split by the mother's age is not: ONS says it recalculated the 2010-2019 "
      "rates using the mix of mothers' ages found in its household labor force surveys, and earlier editions "
      "say one year's age pattern, from 2008, was reused for several later years. The population figures "
      "behind the rates are not recounted each year either — they are carried forward from the 2008 census "
      "using births minus deaths alone, as though nobody moved in or out. Some years were never published at "
      "all, so the line has gaps. Some editions misprint 2017 as \"7102\" in the column headers.",
      "https://www.ons.dz/spip.php?rubrique182"),
    C("Iraq", "COSIT — census and household survey rounds", iraq, "Iraq", "survey", False,
      "Iraq's statistics office, COSIT, prints a table of the fertility rate for each year it was measured: "
      "5.7 for the 1997 census, 4.0 in 2004 from the living conditions survey, 4.3 in 2006 and 4.5 in 2011 "
      "from the two household cluster surveys, and 4.2 in 2007 from the socio-economic survey. The 2024 "
      "census adds 3.1, in a separate table by governorate in the 2024 edition of the abstract. COSIT also "
      "publishes a projection series running 4.08 in 2015 down to 3.82 in 2020.",
      "We read the measured rounds and the census figures from COSIT's own tables. We do not use the "
      "projection series: it is model output rather than measurement.",
      "Iraq has no usable vital statistics for this. COSIT says so itself — the interior ministry holds a "
      "civil registry of more than 46 million people, but it is not yet organized for statistical use, and "
      "COSIT is only starting to turn it into statistics. So there are no births by age of mother to check "
      "these figures against. The 2024 census was Iraq's first full count since 1987, and it covers the "
      "Kurdistan region too, reporting 2.6 there against 3.1 nationally. The older survey rounds are less "
      "even in that respect: an adolescent fertility table on a COSIT platform reports zero for the three "
      "Kurdish governorates, which cannot be right. There is also a gap in the middle of the line: a 2018 "
      "survey round is reported to have measured 3.6, but we could not get hold of the report, so it is not "
      "plotted.",
      "https://cosit.gov.iq/"),
    C("Uganda", "UBOS — 2024 National Population and Housing Census", uganda_tfr, "Uganda", "survey", False,
      "Table 7.2 of the 2024 census report gives, for every five-year age group, the number of women, the "
      "births they reported in the previous twelve months, the same births after correction, and both the "
      "reported and corrected rates. The 2014 census gave 5.8. The health survey UBOS runs separately gives a "
      "direct series: 6.9 in 2000, 6.7 in 2006, 6.2 in 2011, 5.4 in 2016 and 5.2 in 2022.",
      "We rebuilt the 2024 figure from the corrected rates, getting 4.47 where the report prints 4.5. Summing "
      "the reported rates instead gives 4.15, and dividing the reported births by the women gives the same — "
      "so the published figure sits about 8% above what women actually reported. The age-band comparison uses "
      "the reported counts, because those are what the census counted.",
      "The correction raises the reported birth counts to allow for births women forget or misdate; it is a "
      "standard demographic method, the Brass P/F adjustment. UBOS applies it because Uganda's civil "
      "registration cannot be used at all: the census found only 10% of children under five had a registered "
      "birth. That also means the census and the health survey measure fertility in different ways — the "
      "survey's 5.2 for 2022 against the census's 4.5 for 2024 is partly a real fall and partly the two "
      "asking about births differently, and we have not tried to separate the two.",
      "https://www.ubos.org/nphc-2024-census-page/"),
    C("South Africa", "Stats SA — mid-year population estimates", south_africa_tfr, "South Africa",
      "projection", False,
      "The fertility rate Stats SA publishes is an input to its population projection, not a rate computed "
      "from births. Its own report says the series was \"derived following a detailed review of TFR estimates "
      "(1985-2024), (both published and unpublished), from various authors, methods and data sources\", "
      "informed by registered births and health-system records. Separately, the recorded live births report "
      "publishes the number of registered births by the mother's age, and the mid-year estimates publish the "
      "female population by age group.",
      "We read the modeled series from the current edition's own spreadsheet. The age-band comparison uses "
      "the registered births instead, because those are the only counts the registry produces.",
      "The two disagree sharply. Dividing the registered births by the female population gives 1.53 for "
      "2024, against the 2.15 Stats SA now publishes for that year. Some of the difference is timing — it "
      "says about 10% of births are registered a year or more late, and a year keeps filling up for years "
      "afterward — but not all of it: it also estimates that even once late registrations are in, "
      "registration captures only about 90% of births. So South Africa's headline figure sits well above "
      "what its own registry shows, and is a modeled estimate like the UN's rather than a count. It is also "
      "revised often, and by a lot: the 2024 edition of this series put 2024 at 2.41 and the 2026 edition "
      "puts it at 2.15, with every year from 2016 on revised down, which Stats SA attributes to bringing the "
      "rate into line with what its administrative records show. Fertility results from the 2022 census have "
      "still not been released, which Stats SA notes as a reason the census could not feed the estimate. No "
      "mid-year estimates edition was published for 2023.",
      "https://www.statssa.gov.za/publications/P0302/P03022026.pdf"),
    C("Sudan", "Central Bureau of Statistics — Multiple Indicator Cluster Survey 2014", sudan, "Sudan",
      "survey", False,
      "One figure: 5.2 children per woman for women aged 15-49, from the Bureau's own 2014 survey, printed "
      "in a short fertility brief. The same brief's longer table stops at a 1990 health survey.",
      "We read the figure from the brief. No arithmetic of our own.",
      "The Bureau's website no longer exists — its own nameservers stopped answering, so the domain does not "
      "even resolve, while the rest of the Sudanese government's web presence is fine. That looks like "
      "abandonment rather than a block, and is unsurprising given the war since 2023. This brief was "
      "therefore recovered from a web archive; it is genuinely the Bureau's own document but carries no "
      "publication date, and one of its other tables has stray percent signs on figures that are children "
      "per woman. There is no age detail to recalculate from, and nothing more recent: the 2008 census files "
      "that survive give population by area only, with no age breakdown. The health ministry's site is alive "
      "but stuck in a redirect loop, so nothing can be read from it either.",
      "http://cbs.gov.sd/"),
    C("Kenya", "KNBS — 2019 Population and Housing Census, volume VI", kenya, "Kenya", "survey", False,
      "The census fertility volume gives a national total fertility rate of 3.4 for 2019 and 4.8 for 2009, "
      "plus age-specific rates for every county. The 2022 household health survey, also run by KNBS, "
      "separately found 3.4.",
      "We read both census values out of the trends table. We also added up the national rate for each age "
      "group and got 3.24, a little below the published 3.4: a few counties' rates were statistically "
      "corrected before being folded into the national total, which is why our own sum does not land exactly "
      "on it.",
      "The census measures fertility by asking women about the births they had in the twelve months before "
      "the count, not from birth records. Rates for the North-Eastern counties were corrected with a "
      "statistical model because reporting there was inconsistent. This report does not break births down by "
      "the mother's age at all, and its pages carry no readable text layer, so the figures had to be read "
      "off the images. KNBS does publish registered births by the mother's age in its annual vital "
      "statistics reports, and even computes a rate from them — but that rate falls as registration "
      "completeness falls, from 77% in 2023 to 70% in 2024, so it would read as a collapse in fertility "
      "that is really a collapse in recording.",
      "https://www.knbs.or.ke/wp-content/uploads/2024/05/2019-Kenya-population-and-Housing-Census-"
      "Analytical-Report-on-Fertility-and-Nuptiality-Vol.VI_.pdf"),
    C("Myanmar", "Department of Population — 2024 census and 2019 Inter-censal Survey", myanmar_tfr,
      "Myanmar", "survey", True,
      "For both its 2024 census and its 2019 inter-censal survey, the Department of Population publishes an "
      "appendix table giving, for every five-year age group of women in ordinary households, the number of "
      "women counted, the live births they reported in the twelve months before enumeration, and the "
      "department's own rate for that group. The 2024 census also publishes a second, higher figure — 1.8 "
      "against the 1.4 the reported births give — worked out by a different method.",
      "We divided each age group's births by its women, added the seven results, and multiplied by five, the "
      "number of years in each group. That reproduces every rate the department prints, and gives 2.007 "
      "against its published 2.0 for 2019 and 1.401 against its published 1.40 for 2024. Both plotted points "
      "are the reported-births figure, so they are on the same method.",
      "The department's own preferred figure for 2024 is the higher one, 1.8. It is built from the children "
      "older women report ever having had, which corrects for births a twelve-month recall question misses, "
      "and the department calls it the more robust of the two. We plot 1.4 because the 2019 round publishes "
      "nothing equivalent, and putting a corrected point beside an uncorrected one would show a fall that is "
      "partly a change of method. So read 1.4 as the floor and 1.8 as the department's best estimate. The "
      "2024 census could not reach the whole country: of 330 townships, 152 were fully enumerated, 120 only "
      "partly, and 58 not at all, and the population of those was estimated from satellite imagery. The 2019 "
      "round was a sample survey rather than a count, scaled up from the households it did enumerate. On the "
      "reported-births method the 2014 census gives 2.3, which is the figure to compare 2019's 2.0 against.",
      "https://www.dop.gov.mm/sites/dop.gov.mm/files/publication_docs/2024mphc_appendixtables.pdf"),
    C("Sri Lanka", "DCS — registered births by the mother's age, over the mid-year female population",
      sri_lanka_tfr,
      "Sri Lanka", "complete", True,
      "Sri Lanka's Department of Census and Statistics publishes no annual fertility rate built from its "
      "birth registrations, but it does publish both ingredients every year: registered live births by age "
      "of mother, and mid-year population by age group and sex. Its own fertility figures come instead from "
      "surveys and censuses at intervals — 2.3 from a 2006-07 survey, 2.4 from the 2012 census, 2.2 from a "
      "2016 survey and 1.3 from the 2024 census.",
      "We divided the births in each age band by the women in it and summed — the two tables the department "
      "publishes for exactly this purpose. This is our own arithmetic on its counts, not a figure it has "
      "endorsed, and it is not directly comparable with its survey and census figures.",
      "Two things to know. First, the series stops at 2021 — not because registration stopped, but because "
      "the detailed births-by-age tables did. Headline birth totals kept being published through 2025, and "
      "they fall steeply: 319,108 in 2019 to 214,570 in 2025. We found no age breakdown for any year after "
      "2021, in the department's tables, the registrar general's, or the statistical abstract. Second, the "
      "2024 census reports 1.3, far below the 1.64 we get for 2021, but it is a different measure: it asks "
      "ever-married women how many children they have had and when the last one was born, rather than "
      "counting a year's registrations. Neither figure replaces the other. The population we divide by is "
      "published rounded to thousands, and its age structure is rolled forward from the 2012 census rather "
      "than the 2024 one.",
      "https://www.statistics.gov.lk/Population/StaticalInformation/VitalStatistics/Fertility"),
    C("Taiwan", "Ministry of the Interior — births by age of mother divided by the household register",
      taiwan_tfr, "Taiwan", "complete", True,
      "The interior ministry publishes births by the mother's age group, population by single year of age "
      "and sex, and its own fertility rate — all from the household register, all back to at least 2000.",
      "We divided the births in each age band by the women in it and summed. Our figures come out close to "
      "the ministry's own: 0.888 against its 0.885 for 2024, and 0.868 against its 0.865 for 2023.",
      "Taiwan has one of the lowest fertility rates in the world — 0.89 in 2024, against 1.66 in 2000 — and "
      "the ministry's own figure for 2025 is lower again, 0.695. The Chinese zodiac moves the rate visibly, "
      "because some years are traditionally seen as more auspicious than others for having a child: it fell "
      "to 0.88 in 2022, a Tiger year, and rose to 0.89 in 2024, a Dragon year, the only rise in a decade. "
      "Our line stops at 2024 even though the ministry has published a 2025 rate. Births here are counted by "
      "the date they happened rather than the date they were registered, so the most recent year keeps "
      "filling in: for 2025 we could account for 105,676 births against the ministry's final 107,812, about "
      "2% short, and the rate that gives is nearly 2% above the ministry's own where every finished year "
      "comes out within 0.34% of it. Rather than plot a figure we know to be built on an unfinished count, "
      "we leave the year out. The population we divide by is the year-end household register — not a "
      "mid-year estimate, and not the de facto resident population that Taiwan's statistics agency publishes "
      "separately.",
      "https://www.moi.gov.tw/cl.aspx?n=4404"),
    C("Senegal", "ANSD — continuous Demographic and Health Survey", senegal, "Senegal", "survey", False,
      "Senegal's national statistics agency, ANSD, has run its health and demographic survey in most years "
      "since 2012, and its 2023 census report prints the whole series from 1978 in one trend table. The "
      "census publishes its own figure separately, along with the births and women behind it.",
      "We use the rate each survey round published. We also checked the census: adding up the rate for each "
      "of its seven age groups gives 3.69, which is the uncorrected figure ANSD itself prints.",
      "The census shows unusually clearly how an office decides how much to correct. ANSD ran three "
      "estimating methods on the same data and published all three: one that compares the children women "
      "report having had against their recent birth rates gave 4.54, a curve fitted to the age pattern of "
      "fertility gave 4.35, and a third that allows fertility to have changed between two censuses gave "
      "4.41. It chose the third, saying so, and noted that 4.4 sits close to the survey's 4.0 for the same "
      "year. So the published census figure is 4.4 against an uncorrected 3.7. Its own 2013 census ran a "
      "narrower version of the check and found almost no correction was needed. One inconsistency to know "
      "about: ANSD's projections document and its most recent annual population report still carry an "
      "earlier, provisional estimate of 4.2 for 2023, which was never updated to the census report's "
      "revised 4.4. The survey rounds are not annual throughout either — none ran between 2019 and 2023 — "
      "and each figure covers the three years before its fieldwork, so consecutive points overlap. Civil "
      "registration is not used for any of this; the 2023 survey found 80% of children under five holding a "
      "birth certificate.",
      "https://www.ansd.sn/"),
    C("Malawi", "NSO — census and Demographic and Health Survey rounds", malawi, "Malawi", "survey",
      False,
      "Malawi's National Statistical Office gave its 2018 census a dedicated fertility report, and its 2024 "
      "health survey charts every round since 1992. The census publishes the women and births by age group "
      "behind its own figure, which few censuses here do.",
      "We read the published rates. Adding up the census's own rates by age group gives 4.17, matching the "
      "4.167 it prints as its uncorrected figure.",
      "Malawi's census report is unusually open about how it decided how much to correct that figure. It "
      "tried three statistical techniques for estimating fertility from imperfect census answers and "
      "published all three: the P/F ratio method gave 4.7 to 5.1 children per woman, a Gompertz curve fit "
      "4.9 to 5.4, and the Arriaga method 4.23. It chose Arriaga and said why — that the other two assume "
      "fertility is not changing, and Malawi's is falling. So the correction it settled on was small, under "
      "2%, which is far less than some other countries here have applied to their own censuses. The 2008 "
      "point needs a note: that census's own report gives 5.2 uncorrected, while the 6.0 we plot comes from "
      "the 2018 report's trend series — the version comparable with the corrected 4.2 we plot for 2018. "
      "Fertility is measured from census and survey questions, not registration, though registration itself "
      "has risen from 67% of children under five in 2015 to 78% in 2024.",
      "https://www.nsomalawi.mw/"),
    C("Somalia", "SNBS — Somali Health and Demographic Survey 2020", somalia, "Somalia", "survey", False,
      "Somalia's statistics bureau publishes one national fertility figure of its own, from its 2020 health "
      "and demographic survey: 6.9, with the rate for each age group behind it.",
      "We read the published figure, and checked that its own rates by age group sum to it: 6.885 against "
      "the printed 6.9.",
      "The survey is titled 2020, but its own report dates the fieldwork to 2018 and 2019, and the rate "
      "covers the three years before each woman was interviewed — so it describes fertility somewhat earlier "
      "than the year we plot it at. Three other surveys report fertility for Somalia, and none of them "
      "replaces it. A 2026 survey by the same bureau reports 5.7, but covers five districts and describes "
      "itself as a step toward national coverage. A 2006 survey gives a national 6.7, but was published by "
      "a UN agency with three planning ministries rather than by the bureau, and the 2020 report notes that "
      "its fertility table leaves out nomadic households. And the 2013 population survey, the one that gave "
      "Somalia its population count, was published by a UN agency and contains no fertility rate at all. No "
      "census has published results since 1975; another was attempted in the mid-1980s but its findings were "
      "never released. Birth registration is between 3.5% and 5.9%, depending on which part of the same "
      "report you read.",
      "https://nbs.gov.so/"),
    C("Chad", "INSEED — survey and census rounds", chad, "Chad", "survey", False,
      "Chad's national statistics office, INSEED, publishes four figures, none of them annual: 7.1 from the "
      "2009 census, 6.9 from a 2010 survey, and 6.4 from each of two later surveys, in 2014-15 and 2019. "
      "The survey reports give the rate for each age group, but not the counts of births and women behind "
      "them.",
      "We read the published figures. Each survey's own rates by age group add up to the total it prints, "
      "which is as far as checking can go without the counts.",
      "None of these figures is measured the same way as a year of registered births: each survey rate "
      "covers the three to five years before its fieldwork, so the points describe fertility somewhat "
      "earlier than the years they sit at. The two 6.4s are genuinely separate measurements from separate "
      "surveys rather than one figure counted twice. A 2019 attribution of the earlier one also circulates, "
      "but a health ministry document was quoting 6.4 in March 2016, before the later survey existed. The "
      "2009 census's dedicated fertility volume does not appear to exist in digital form anywhere, so the "
      "full derivation of the 7.1 cannot be traced, though the census's own results report does publish it "
      "alongside its other headline rates. Birth registration was 16% in 2010, up from 9% in 2004, and only "
      "a quarter of those could show a certificate. The 2009 census was Chad's most recent for seventeen "
      "years; fieldwork for the next one finished in August 2026, with results promised progressively.",
      "https://inseed.ssn-tchad.td/"),
    C("Chile", "INE — registered births by the mother's age, over its female population estimates",
      chile_tfr, "Chile",
      "complete", True,
      "One spreadsheet from Chile's national statistics institute, INE, carries the whole series from 1992: "
      "births by five-year age group of the mother, the women in each group, and INE's own rate.",
      "We divided births by the women in each age group and added up the results. Our 1.034 for 2024 rounds "
      "to INE's published 1.03, and our 1.159 to its 1.16 for 2023.",
      "Chile's registered births imply much lower fertility than its own population projections assumed. The "
      "projection in force until January 2026 put fertility far above what the registry was counting — a gap "
      "of about half a child per woman. INE then rebuilt its population estimates on the 2024 census that "
      "month, and the new projection assumes 1.06 for 2024, close to what the registry gives. But that has "
      "not yet changed the published rate: INE says it is still calculating the national and regional rates "
      "on the older, 2017-census population base, deliberately, until estimates from the new census exist at "
      "regional level. So the two figures have converged in the projections without the rate itself having "
      "been rebuilt. The counting is sound: births are dated to the year they happened, and almost all of a "
      "year's registrations turn out to belong to that year. The 2024 figure is still preliminary; the 2023 "
      "one has since been finalized, unchanged at 1.16. The Metropolitan region around Santiago was already "
      "at 1.03 in 2023, a year before the country as a whole.",
      "https://www.ine.gob.cl/estadisticas-por-tema/demografia-y-poblacion/estadisticas-vitales"),
    C("Netherlands", "CBS — births by age of mother divided by its mean population", netherlands_tfr,
      "Netherlands", "complete", True,
      "Statistics Netherlands, CBS, publishes births by single year of the mother's age from 1950, "
      "population by single year of age and sex, and its own rate. The births and the population sit in "
      "separate tables from the summary rate.",
      "We divided births at each single age by the women of that age and added up the results. Our total "
      "for 2023, 1.4299, rounds to CBS's published 1.430; our 1.4262 for 2024 rounds to its 1.426. Every "
      "year since 1995 matches its published figure.",
      "CBS states exactly what its rate is built from — births in an age group divided by the average "
      "number of women in it, the average being half the population on 1 January and half on 31 December — "
      "so nothing has to be guessed at. Births are dated to when they happened rather than when they were "
      "reported, and CBS marks every year final rather than provisional, though it does fold late "
      "notifications back into the year of the birth, so even a final year can edge up. The register covers "
      "everyone registered as resident in a municipality whatever their nationality. Our line starts in "
      "1995 because that is where CBS's own average-population figures begin; the births go back to 1950, "
      "and the January population needed to work the average out ourselves exists back to 1988, so the "
      "line could be extended at least that far.",
      "https://www.cbs.nl/nl-nl/cijfers/detail/85722ned"),
    C("Zimbabwe", "ZIMSTAT — census and Demographic and Health Survey rounds", zimbabwe, "Zimbabwe",
      "survey", False,
      "Zimbabwe's statistics agency, ZIMSTAT, gave its 2022 census a dedicated fertility report. The counts "
      "behind its figure — women counted and births in the previous twelve months, by age group — are in the "
      "main census report rather than that one. The health survey series runs alongside both.",
      "We read the published rates. We also checked the census: dividing each age group's births by its "
      "women, adding the seven results and multiplying by five gives 3.72, which rounds to the 3.7 it "
      "publishes. Those counts come to 438,776 births among 3,814,701 women — dividing those two totals "
      "gives the general fertility rate of 115 per thousand that ZIMSTAT also reports, not the fertility "
      "rate itself.",
      "ZIMSTAT ran two standard techniques for adjusting undercounted recent births as a check on its "
      "census figure — one gave 3.8 and the other 3.7 against the counted 3.7 — and concluded that because "
      "the three agree, the direct estimate is sound and the unadjusted figure is the one it would use "
      "throughout. Registration is not used for any of this and could not be: ZIMSTAT's own vital-statistics "
      "report puts birth-registration completeness at 30.9% for 2023 and 26.4% for 2024, falling rather "
      "than rising, because mobile registration drives in 2022 and 2023 pulled registrations forward. That "
      "report does publish registered births by age of mother, but computes no rate from them.",
      "https://www.zimstat.co.zw/wp-content/uploads/Census/Fertility_Report.pdf"),
    C("Ecuador", "INEC — registered births by the mother's age, over its female population estimates",
      ecuador_tfr, "Ecuador",
      "incomplete", True,
      "One sheet of INEC's birth records carries both sides from 2010: births in each five-year age group "
      "of the mother, and the projected women in that group. INEC publishes the rate for each age group "
      "from exactly those two columns; we did not find it publishing their sum.",
      "We divided births by the women in each age group and added up the results. Our figures reproduce "
      "its printed rates for each age group exactly. INEC's own bulletin headlines births per head of "
      "population and teenage birth rates rather than a fertility rate, so we found no published total to "
      "check ours against.",
      "Ecuador's registered births imply lower fertility than the projection INEC publishes, and INEC says "
      "in writing why. Its projection methodology lists four estimates for 2022, ranging from 1.76 to 2.12 "
      "by different methods, and adopts 1.86 on the stated grounds that the figure should sit above the "
      "registered births because those always carry some under-registration. That is the office's own "
      "account of its registration being incomplete, which is why this sits under incomplete registration "
      "rather than complete. So the gap is deliberate rather than an oversight. Its projection assumes 1.82 "
      "for 2023 against a counted 1.61, and the 2018 health survey gave 2.19. Recent years are incomplete "
      "by design: a year is provisional until the following March and semi-definitive for three more. Our "
      "line therefore stops at 2023 — registered births fell almost 10% in 2024, against about 4% the year "
      "before, and INEC's own schedule says that figure will rise as late registrations arrive.",
      "https://www.ecuadorencifras.gob.ec/nacidos-vivos-y-defunciones-fetales/"),
    C("Kazakhstan", "Bureau of National Statistics — births by age of mother divided by its population",
      kazakhstan_tfr, "Kazakhstan", "complete", True,
      "Kazakhstan's Bureau of National Statistics publishes births by age of mother, average yearly "
      "population by sex and age, and its own rate, all as open data.",
      "We divided births by the women in each five-year age group and added up the results. Our figures "
      "match the bureau's published rate to within 0.01 in every year both exist: 2.957 against 2.96 for "
      "2023, 2.798 against 2.80 for 2024.",
      "Kazakhstan's fertility rose sharply and is now falling back, and the rise shows up in the number of "
      "births actually counted rather than being a quirk of the data. The rate went from 2.84 in 2018 to "
      "3.32 in 2021, then down to 2.57 by 2025 — a fall of 0.75 in four years. Two things limit what we can "
      "say. First, a birth is dated to the year it was registered rather than the year it happened: the "
      "bureau states outright that a birth registered this year counts in this year's figures even if the "
      "child was born earlier. Second, the way ages are grouped changed in 2025, and the population series "
      "we use runs back only to 2018 under the current grouping. Earlier years do exist — the bureau's own "
      "demographic yearbooks carry births by mother's age and population by age back to at least 2009 — so "
      "this line could be extended.",
      "https://stat.gov.kz/ru/industries/social-statistics/demography/"),
    C("Benin", "INStaD — Demographic and Health Survey rounds", benin, "Benin", "survey", False,
      "Benin's National Institute of Statistics and Demography publishes a fertility rate from each round "
      "of its Demographic and Health Survey, and the 2017-18 report's trend table carries them back to "
      "1996. The 2013 census gives 4.8, broken down by region and by education, but does not publish the "
      "birth and women counts needed to check it.",
      "We use the rate each survey round published; adding up the rates by age group in the 2017-18 report "
      "returns each round's own total. The census figure cannot be checked the same way, because the counts "
      "behind it are not published.",
      "Benin's statistics office says it corrected the census figure, but not how. Its own volume states "
      "that the fertility, marriage and mortality indicators were produced by indirect estimation — working "
      "the rate out from indirect clues, such as how many children women say they have had in total, rather "
      "than counting births — by a team of demographers with an international expert. So 4.8 is a model "
      "output rather than a count. We found no name for the method and no uncorrected figure printed beside "
      "it, so there is nothing to compare it against. The census and the surveys also disagree about "
      "direction: the censuses fall steadily, 6.1 in 1992 to 5.5 in 2002 to 4.8 in 2013, while the surveys "
      "are flat or rising, 4.9 in the 2011-12 round against 5.7 in 2017-18. A separate 2014 survey, run "
      "under a different program, also reported 5.7. Birth registration is high, at 86% of children under "
      "five, but is not used for any fertility figure.",
      "https://instad.bj/"),
    C("Cambodia", "NIS — Demographic and Health Survey rounds", cambodia, "Cambodia", "survey", False,
      "Cambodia's National Institute of Statistics publishes a fertility rate from each round of its "
      "Demographic and Health Survey, and the 2021-22 report charts every round since 2000. The 2008 and "
      "2019 censuses, and the surveys taken in between and after them in 2013 and 2024, each publish a "
      "figure with the calculation behind it.",
      "We use the rate each survey round published. We also added up the 2019 census's own rates by age "
      "group and got 2.512 against its printed 2.51 — which shows the table adds up, not that the number "
      "behind it is right, because those rates are the corrected ones and the counts they came from are not "
      "published.",
      "The office has corrected its census fertility figure on every count since 2008, and printed both "
      "numbers each time: births reported in the twelve months before the count give 1.6 in 2008 against an "
      "adopted 3.1, 2.05 in 2013 against 2.8, 1.67 in 2019 against 2.51, and 1.4 in 2024 against about 2.3. "
      "The uncorrected figure runs between about half and three quarters of the published one every time. It "
      "names the statistical techniques it uses to correct for under-reported births — Brass, Arriaga, Rele "
      "and a relational Gompertz curve — and in the two most recent rounds says the Brass-Arriaga "
      "combination suits Cambodia best; in the earlier two it averaged across a range instead. It also "
      "explains the under-reporting it is correcting for: children who were born and then died go "
      "undeclared, dates get misplaced, and someone other than the mother often answers. So this is settled "
      "practice rather than a one-off. It also means the census figures and the survey figures are not "
      "measuring the same way and should not be joined into one line: the surveys are flat at 2.7 from 2014 "
      "to 2022 while the corrected census figures fall. One thing to watch when comparing documents: the "
      "2019 census report looks back at 2008 and gives 2.7, which is one method's value from the 2008 table "
      "rather than the 3.1 that report actually adopted. Registration is not used at all, though 92% of "
      "children under five are registered. The 2024 report's own two tables also disagree about its answer.",
      "https://nis.gov.kh/"),
    C("Guinea", "INS — Demographic and Health Survey rounds", guinea, "Guinea", "survey", False,
      "Guinea's national statistics institute, INS, publishes a fertility rate from each round of its "
      "Demographic and Health Survey, and the 2018 report's table carries the rounds back to 1999. The 2014 "
      "census publishes the counts behind its own figure in an annex: women by age group, and births in the "
      "twelve months before the count.",
      "We use the rate each survey round published. We also rebuilt the census figure from its counts — "
      "dividing each age group's births by its women, summing and multiplying by five — and got 5.19 against "
      "the 5.3 it publishes, then compared it age group by age group.",
      "That comparison shows something the census report does not. The office raised the census's birth "
      "counts because a standard check, comparing the children women report having had over their lifetimes "
      "against their recent births, showed the recent ones were too low. The effect on the total is small, "
      "about 2%. But the fix was not spread evenly across ages: for every age group from 20 to 49 the "
      "published rate is within 1% of the counted one, and the whole correction lands on 15-19, where 105 "
      "births per thousand women becomes 130, a 24% increase. The report states its overall conclusion but "
      "never points this out; it appears only when the rates are recalculated from the counts it publishes. "
      "Birth registration cannot substitute as a source here: 62% of children under five are registered and "
      "51% hold a certificate. The documents used here are archived copies from 2018, because INS's own site "
      "is currently unreachable. Guinea ran a new census in 2025, and preliminary population results were "
      "published in early 2026, but they carry no fertility figure.",
      "https://www.dhsprogram.com/pubs/pdf/FR353/FR353.pdf"),
    C("Romania", "INS — births by age of mother divided by the resident population", romania_tfr, "Romania",
      "complete", True,
      "Romania's National Institute of Statistics publishes births by the mother's age group on a "
      "usual-residence basis from 2012, and resident population by age in the middle of each year from "
      "2002. It does not publish an annual total fertility rate built from them, though it has published "
      "one in the past, in a report covering 1960 to 2010.",
      "We divided births by the women in each five-year age group and added up the results. For 2024 that "
      "gives 1.3757.",
      "This series should be treated with more caution than most here. It rises from 1.36 in 2012 to 1.86 "
      "by 2019 and falls back to 1.38 by 2024 — but both Eurostat and the UN put Romania's rise smaller and "
      "later, peaking around 2021 rather than 2019, and their figures sit up to 0.15 away from ours at both "
      "ends of the line and up to 0.09 the other way in the middle years. The middle of the range, 2014 to "
      "2017 and 2022, agrees closely. We have not established what accounts for the rest, and the shape of "
      "the disagreement — one way at the ends, the other way in between — looks more like the population we "
      "divide by shifting under the series than like a real pattern in births. Romania keeps two population "
      "counts: the resident population, meaning everyone whose usual residence is in the country, and the "
      "population by domicile, meaning citizens registered as living there whether or not they still do. We "
      "use the resident one, which the institute says is the right choice for international comparison, and "
      "in a country with emigration on Romania's scale the two differ a lot. Births are dated to when they "
      "happened, but a year is not final until late registrations from the following three years are folded "
      "in, so the most recent years will still move.",
      "http://statistici.insse.ro:8077/tempo-online/"),
    C("Rwanda", "NISR — Demographic and Health Survey rounds", rwanda, "Rwanda", "survey", False,
      "Rwanda's national statistics office, NISR, runs a health survey every five years, and its 2025 "
      "report charts every round since 2000. It also publishes census fertility, and an annual fertility "
      "rate built from civil registration, which few countries here have.",
      "We use the rate each survey round published. Each covers the three years before its fieldwork rather "
      "than the year we plot it at. We also recomputed the 2022 census figure from its own women counts and "
      "rates by age group and got 3.635 against the 3.63 it publishes.",
      "The office changed its mind between census rounds, using the same test both times. In 2012 the raw "
      "census figure was 3.8 against the survey's 4.6 two years earlier; NISR said a 20% fall in three "
      "years was unlikely, concluded births had been under-reported, tried three statistical techniques for "
      "correcting that, rejected two on stated grounds and adopted the third, publishing 4.02. In 2022 it "
      "ran the equivalent check, found no evidence of under-reporting, and published the raw 3.6 "
      "unadjusted. Same office, same diagnostic, opposite decisions.\n\nThere is a third channel worth "
      "knowing about. NISR publishes a registration-based rate every year, scaling registered births up by "
      "its own measured completeness — 3.2 becomes 3.5 for 2025 — and prints it beside the census and "
      "survey figures in one table. Registration coverage reached about 93% in 2025, high for the region, "
      "though it dipped in the early 2020s before recovering. That annual series would give a denser line "
      "than the five-yearly survey we plot, and is worth switching to. The census and the survey have "
      "essentially converged — 3.6 from the 2022 census against 3.7 from the 2025 survey — having been 14% "
      "apart at the previous pair of readings.",
      "https://statistics.gov.rw/"),
    C("Tunisia", "INS — registered births by the mother's age, over the mid-year female population",
      tunisia, "Tunisia", "complete",
      False,
      "Tunisia's national statistics office, INS, publishes a fertility rate every year, built from civil "
      "registration, and the statistical yearbook prints the births by the mother's age group and the "
      "mid-year population behind it.",
      "We read the published series. We also rebuilt 2023 from those counts. Dividing them directly gives "
      "1.489, but 7,709 of the 135,148 births have no age recorded, and spreading those across the age "
      "groups in proportion gives 1.579 — INS's published 1.58, and its printed rates for each age group "
      "almost exactly. That is how the office handles them, and dropping them instead would understate the "
      "rate by 6%.",
      "Tunisia's fall is steep and recent: 2.4 in 2015 to 1.58 in 2023. Only the most recent year of each "
      "yearbook carries the age breakdown, so the other years cannot be checked the same way. We could not "
      "establish whether a birth is dated to the year it happened or the year it was registered, or whether "
      "any late-registration correction is applied to live births — the yearbooks carry no methodology "
      "section at all. The office does say that more than half of stillbirths go unregistered, and it labels "
      "its deaths total corrected while never labeling the births total that way. The population is all "
      "residents rather than citizens, which in Tunisia hardly matters: the 2024 census puts foreign "
      "nationals at 0.55% of the population.",
      "https://www.ins.tn/statistiques/112"),
    C("Burundi", "ISTEEBU — Demographic and Health Survey rounds", burundi, "Burundi", "survey", False,
      "Burundi's statistics office publishes a fertility rate from two survey rounds, 2010 and 2016-17. "
      "Its 2008 census publishes the fertility tables as spreadsheets — women and births by age group, and "
      "separately the children women have had over their lifetimes.",
      "We use the rate each survey round published. We also rebuilt the census figure from its counts, "
      "dividing each age group's births by its women, summing and multiplying by five, and got 5.954. The "
      "census itself reports 6.0, rounded to one decimal.",
      "No correction was applied to that census figure, and the census says why: it checked its own birth "
      "reporting and concluded the births had been well declared, so the fertility figures could be used "
      "without adjustment. We can see the same thing in its numbers. Because it publishes both the births "
      "in the previous twelve months and the children women have had over their lifetimes, the standard "
      "check for missed births can be run on it, and across the middle age groups — the ones that check is "
      "usually trusted on — the two line up almost exactly. Civil registration produces quarterly counts "
      "but no rate, and the office says why: too many births go undeclared for a rate to be meaningful. The "
      "office was renamed in 2022. The 2024 census has so far published only preliminary population totals, "
      "with nothing yet on fertility.",
      "https://www.insbu.bi/"),
    C("Haiti", "Ministry of Health — EMMUS survey rounds", haiti, "Haiti", "survey", False,
      "The surveys go back to the 1990s; the three rounds plotted here are the ones with rates measured "
      "the same way, the newest from 2016-17. The 2003 census publishes its fertility tables as scanned "
      "images of printed pages.",
      "We read the survey figures. For 2006 we plot 3.9 rather than the 4.0 on the front page of that "
      "round's own report, because that round measured fertility over the five years before the survey "
      "and the two later ones measured it over three: both later reports print 3.9 for 2006 whenever "
      "they state the trend, and 3.9 is the figure measured the same way as the rest of the line. We "
      "also divided the census's own counts and got 3.53. The institute publishes only \"4 children\" "
      "for the census, with no decimal, so we cannot tell whether its 4 comes from the same division or "
      "from an adjusted calculation. Our 3.53 is lower than the surveys either side of it imply — 4.7 "
      "for 1994-95 and 3.9 for 2005-06 — which is the usual result when a census asks women how many "
      "children they had in the previous twelve months, because recent births get missed.",
      "Haiti has had no fertility figure since 2016-17, and no census since 2003. The statistics "
      "institute says why in its own 2024 population report: the census could not be held, in what it "
      "calls the country's many-sided crisis, and it is therefore taking its population figures from "
      "CELADE, the UN's regional demographic center for Latin America, rather than producing estimates "
      "of its own. Those CELADE figures were published in 2008, so Haiti's official population now rests "
      "on projections made before the earthquake. The institute's page for birth and death registration "
      "is live and empty, with every section marked as pending. The surveys everyone cites as Haiti's "
      "fertility rate are run by a child-health institute for the health ministry, with an American firm "
      "credited as co-author; the statistics institute is named only as a collaborator. They are still "
      "Haitian government figures, which is why they are plotted, but the institute's own last fertility "
      "figure is the 2003 census.",
      "https://dhsprogram.com/publications/publication-fr326-dhs-final-reports.cfm"),
    C("South Sudan", "NBS — household and multiple-indicator surveys", south_sudan, "South Sudan",
      "survey", False,
      "Three figures. The bureau's own estimate from the 2008 census, 6.92, which it published in a 2013 "
      "report on fertility and mortality in that census. Then 7.5 from the 2010 household health survey, "
      "run by the health ministry with the bureau. Then 6.4 from the bureau's own 2025 survey, published "
      "as a preliminary report in 2026. The census tables also publish women and births by age group.",
      "We plot all three. We also divided the 2008 census's raw counts ourselves and got 3.9, a little "
      "over half what the bureau itself gets from the same census.",
      "That distance between 3.9 and 6.92 is the useful thing on this page, because both numbers come "
      "out of the same census. The census asked women how many children they had borne in the previous "
      "twelve months. Divide those answers directly and you get 3.9. The bureau did not stop there: it "
      "combined them with the number of children each woman had ever borne, using standard methods for "
      "filling in the births a census of this kind misses, and published 6.92. A survey asking women for "
      "their full birth histories two years later gave 7.5. Nobody thinks fertility nearly doubled "
      "between 2008 and 2010, so what the 3.9 measures is the size of the undercount, in one country's "
      "own numbers. It is worth knowing that a raw census figure can be wrong by that much. The UN fits "
      "a smooth decline below all three of South Sudan's figures, from 6.0 in 2005 to 3.7 now, about 2.7 "
      "children below the latest national one. The 2025 survey is a preliminary report published in 2026, "
      "after the UN's last revision, so it could not have been taken into account. This is a country "
      "where the honest answer is that nobody knows the level, not one where a single source is wrong. "
      "South Sudan has had no census since 2008, none since independence, and no announced date for one. "
      "Birth registration stands at 36%.",
      "https://nbs.gov.ss/"),
    C("Bolivia", "INE — Demographic and Health Survey rounds", bolivia, "Bolivia", "survey", False,
      "Bolivia's statistics office, INE, runs a health survey every few years, and its 2023 fertility "
      "report charts all five rounds since 1998, each with the rate for every age group behind it. The 2024 "
      "census asks women how many children they have ever borne, and INE publishes those counts. Its "
      "population projection carries a fertility assumption of its own.",
      "We use the rate each survey round published. Adding up each round's own rates by age group returns "
      "its published total, for all five. We could not go further and rebuild the rate from counts, because "
      "the reports give the rates rather than the births and women behind them.",
      "Bolivia's statistics office publishes a headline fertility figure lower than what its own survey "
      "found, which is the reverse of the usual direction — offices more often publish a figure above what "
      "their registered births imply. Its projection assumes 1.69 for 2024 against a survey figure of 2.1 "
      "for 2023, and it explains its reasoning: it treats the survey as its most reliable source, built a "
      "smoothed curve close to it, then checked that curve against three administrative series — birth "
      "registrations, school enrollment and health-ministry birth records — found all three implied less "
      "fertility, and lowered the adopted figure accordingly. It also declined to apply to the 2024 census "
      "the standard correction for under-reported births it had used on the 2001 and 2012 ones, saying it "
      "was incompatible with how fast fertility had fallen. Our own rough check agrees with the direction: "
      "registered births against projected women imply something near 1.4 for 2024. One thing to watch for: "
      "the electoral court's statistical bulletin looks like a vital-statistics release, but its births "
      "table counts birth-certificate printouts — 2.7 million for 2021, about thirteen times the number of "
      "actual births.",
      "https://www.ine.gob.bo/"),
    C("Tajikistan", "Agency on Statistics — registered births by the mother's age, over the resident "
      "female population",
      tajikistan, "Tajikistan", "complete", False,
      "The demographic yearbook prints the fertility rate for every year since 1989, the age-specific "
      "rates behind it, and the female population by single year of age for benchmark years.",
      "We read the published series. We also multiplied the 2023 rates for each age group by the women in "
      "that group and got 3.032 against the published 3.016, and implied births of 250,616 against the "
      "250,285 the agency separately reports as registered. That 0.13% agreement between two "
      "independently tabulated numbers is a good sign for both.",
      "Tajikistan's officially recorded fertility rate has risen, from 2.64 in 2021 to 3.02 in 2023, as "
      "Uzbekistan's and Kazakhstan's have. Raw birth counts by age of mother are not published anywhere, "
      "only the rates, so the series is copied rather than rebuilt. The agency's own publications flag two "
      "things. It marks 2002 to 2017 as preliminary or estimated, and 2007's value of 2.35 is a one-year "
      "drop of nearly a full child that it never explains — probably a problem with the series rather than "
      "a real collapse in births. And the population it divides by is the registered resident population, "
      "which by the agency's own definition includes people temporarily absent. Tajikistan has one of the "
      "highest rates of labor emigration in the world, so a large number of working-age men abroad are "
      "still counted at home; that mostly affects the male side, and our reconciliation suggests it has "
      "little effect on the count of women of childbearing age. One more thing to weigh against the "
      "\"complete registration\" label: the share of births actually registered was well short of complete "
      "at the start of this series. Surveys the agency itself cites put it at 74.6% in 2000 and 88.3% in "
      "2005, reaching 95.3% by 2010, which is the most recent figure published. And the 2017 health survey, "
      "which asked women directly rather than counting registrations, reported 3.8 for the three years "
      "before it — about 30% above what registration gives for the same years.",
      "https://www.stat.tj/ru/elektronnye-versii-publikaczij-arhiv/"),
    C("Sweden", "Statistics Sweden — births by the mother's age, over the mean female population",
      sweden_tfr, "Sweden", "complete", True,
      "Statistics Sweden's database serves births by single year of the mother's age from 1968, mean "
      "population by single year of age, and its own rate. All of it downloads freely, with no "
      "registration and no access key.",
      "We divided births at each single age by the women of that age and summed. That gives 1.4245 for "
      "2024 against the office's published 1.43, and 1.4466 against 1.45 for 2023 — the extra digits are "
      "there only to make the small gap visible.",
      "Sweden's rate is at a record low and still falling: 1.85 in 2016 to 1.43 in 2024 on the office's "
      "own figures, and it has since published 1.42 for 2025. That last year is not plotted here, because "
      "the two tables this series is rebuilt from — births and mean population by single year of age — "
      "still stop at 2024, and copying one year's published rate into a series recalculated from counts "
      "would mix two methods in one line. The gap of about four tenths of a percent between our figures "
      "and the office's is a definition rather than an error, and the office documents it: the public "
      "births table records the age the mother had reached by the end of the year, while its own rate uses "
      "her age at the birth itself. Summing its five-year rates instead reproduces its published figure "
      "exactly. It also states outright that it divides by the mean of the population at the start and the "
      "end of the year, and that this covers everyone registered as resident whatever their citizenship. "
      "One trap in the tables: alongside the individual marital statuses, the population table carries an "
      "\"all marital statuses\" row, so counting that row as well as the individual ones counts everyone "
      "twice.",
      "https://www.scb.se/be0101"),
    C("Jordan", "DOS — Population and Family Health Survey", jordan, "Jordan", "survey", False,
      "The survey rate, every five or six years. The Department of Statistics also publishes registered "
      "births every year and female population by age group, but breaks the births down only by "
      "governorate and the child's sex, never by the mother's age. A second government body does publish "
      "them by the mother's age: the Civil Status and Passports Department, which keeps the birth "
      "register. It goes further and publishes its own fertility rate from them, for every year from 2015 "
      "to 2022.",
      "We plot the survey figures. We also read the register's own rate and checked it: it falls from 3.12 "
      "in 2015 to 2.53 in 2022, and the rates it publishes for each age group sum to those totals exactly. "
      "That 2.53 sits almost on top of the 2.5 the 2023 survey gives for Jordanian women, which is the "
      "comparison to make, because the register covers Jordanians and the survey covers everyone living in "
      "the country. We plot the survey because everyone living in the country is what the UN's figure "
      "describes.",
      "The number to be careful with is which population it describes. About a third of Jordan's residents "
      "are not Jordanian, roughly half of those Syrian, and the 2023 survey publishes the rate separately "
      "for each group: 2.5 for Jordanians, 4.1 for Syrians — 4.9 for those in camps and 3.9 outside — and "
      "2.1 for other nationalities, against 2.6 for everyone. That whole spread, from 2.1 to 4.9, appears "
      "in a single survey table rather than as a series anyone can follow from year to year. Two more "
      "things to know about the Department of Statistics's own figures. Its \"Jordan in Figures\" booklets "
      "carry old survey figures under the current year's column with only a footnote — the 2.7 printed for "
      "2022 is 2017-18 survey data. And registered births have fallen 16% since 2019, from 197,000 to "
      "166,000.",
      "https://dosweb.dos.gov.jo/"),
    C("Honduras", "INE — ENDESA survey rounds", honduras, "Honduras", "survey", False,
      "Three rounds of the national demographic and health survey, ENDESA, which Honduras's statistics "
      "office INE runs: 2005-06, 2011-12 and 2019. INE also publishes registered births by the mother's "
      "age group, in bulletins covering two or three years at a time rather than one a year, with nothing "
      "published for 2017 through 2020. The 2013 census publishes both the women and the births in the "
      "previous twelve months.",
      "We plot the three survey rates. We also worked out the census's own rate for 2013: for each "
      "five-year age group we divided its births in the previous twelve months by its women, then added "
      "the seven rates and multiplied by five. That gives 2.13. INE adopted 2.74 for the same year, 29% "
      "higher than what its own census counted.",
      "Unusually, INE published a registration-based fertility rate for a few years and then stopped. Its "
      "vital-statistics releases for 2013-14 and 2015-16 print the full working — age, births, women, each "
      "rate, the total — giving 2.45 for 2013, 2.67 for 2014, 2.85 for 2015 and 2.45 for 2016, close to "
      "the survey and projection figures for those years. From the 2021-22 release onward it keeps the "
      "birth counts by age and drops the rate, while the text still says fertility is falling without "
      "giving a number. INE does set out how it gets from the census to the higher figure it adopts: "
      "volume 10 of the census series gives the method step by step, taking the census's pattern of "
      "fertility by age and rescaling it to the survey's level, then checking the result against its own "
      "count of children under one. It gives internal consistency as the reason, not any criticism of the "
      "census. Registered births have fallen 27% since 2018, from 181,000 to 132,000. Two cautions. There "
      "has been no census since 2013, so the population figures used to work out any recent rate are "
      "projections from a census now thirteen years old. And the census's own question about births in the "
      "previous twelve months found fewer births than were officially registered for the same period — "
      "usually it is registration that misses births, not a census. One earlier survey exists and is not "
      "plotted: a 2001 round run by a family-planning association with the health ministry rather than by "
      "INE, whose rate covers 1998 to 2000, before this chart starts.",
      "https://ine.gob.hn/"),
    C("Guatemala", "INE — registered births by the mother's age, over its female population projections",
      guatemala_tfr,
      "Guatemala", "complete", True,
      "Guatemala's national statistics institute, INE, publishes no annual fertility rate built from its "
      "birth registry — its bulletin headlines a crude birth rate and rates for teenagers. But it does "
      "publish the raw records, one row per birth, with the mother's age and the year the birth happened, "
      "and population projections by single year of age.",
      "We counted the births by age of mother out of 2.2 million individual records and divided by the "
      "projected women. Two of INE's own figures line up with the result: its 2018 census estimate of 2.7 "
      "for 2018-19 against our 2.6, and the roughly 2.2 for 2022 from its national maternal and child "
      "health survey against our 2.23.",
      "Guatemala's registry shows fertility falling faster than its own population projections assume. The "
      "projection puts it at 2.44 for 2022-23 and 2.33 for 2024-25; the registry gives 2.19 for 2023. Our "
      "line stops there. Births are dated to the year they occurred, and INE waits six months to catch late "
      "registrations before publishing, but the 2024 file holds 12.7% fewer births than 2023 after four "
      "years that moved by about a percent either way, and reporting on the 2025 figures shows part of that "
      "drop reversing. A break that size followed by a rebound is what an under-registered year looks like "
      "rather than a fall in fertility, so we leave 2024 out. Late registrations run at roughly 12% of a "
      "year's records, "
      "and the share moves around: it rose during the pandemic and has risen since 2018 for indigenous "
      "families. The population we divide by is a projection built on the 2018 census, and it does assume "
      "people keep leaving the country, so it is not simply extrapolating a closed population.",
      "https://datos.ine.gob.gt/dataset/estadisticas-vitales-nacimientos"),
    C("Azerbaijan", "State Statistical Committee — registered births by the mother's age, over the average "
      "annual female population",
      azerbaijan, "Azerbaijan", "complete", False,
      "The committee publishes its own rate for every year from 1970, and for 1959, with nothing for the "
      "1960s. It also publishes the rate for each age group, and births by the mother's age back to 1970 "
      "— all as plain spreadsheets at fixed addresses, with no login and no database to query.",
      "We plot the published rate. The committee's own rates by age group reproduce that rate to within "
      "rounding from 2004 onward, but not before: for 2000 through 2003 they come out 0.21 to 0.31 lower, "
      "1.70 against a published 2.0 for 2000 and 1.59 against 1.9 for 2003. What we could not do is "
      "rebuild the rate independently, and the reason is worth setting out.",
      "There is no annual female population by age group anywhere on the site — only census years and one "
      "current snapshot. Dividing the published births by the women the census actually counted gives 1.75 "
      "against a published 1.8 for 2019, 1.83 against 2.3 for 2009, and 1.72 against 2.0 for 1999. The "
      "recomputed figure is lower every time, and much the most so in 2009. That is what happens when a "
      "rate is worked out each year from a population estimate rolled forward from the last census, and "
      "the next census finds the estimate was too low. Two separate checks therefore point the same way: "
      "the published levels for the early 2000s look somewhat overstated — not by our arithmetic, but by "
      "the committee's own later census and by its own table of rates by age. Recent years do get revised. "
      "Every age group's rate for 2019 and 2020 changed between editions of that table, though years more "
      "than a few old appear to freeze: the rows for 1970 to 2011 are identical in editions a decade "
      "apart. Its methodology states that births are dated to when they happened, and that the population "
      "it divides by is the average of the figures at the start and end of the year. The 2019 census "
      "excluded Nagorno-Karabakh and the territories around it, then under Armenian occupation, and the "
      "population series since shows no jump when they were reincorporated. One figure to treat carefully: "
      "the online table gives 1.84 for 2002, the only two-decimal entry in a column of one-decimal ones, "
      "and the committee's own printed 2025 yearbook gives 1.8 for that year.",
      "https://www.stat.gov.az/source/demoqraphy/az/002_3.xls"),
    C("United Arab Emirates", "FCSC — Emirati women only", None, "United Arab Emirates", "none", False,
      "The Federal Competitiveness and Statistics Centre publishes a fertility rate for Emirati women — 3.1 "
      "for 2022, down from 3.7 in 2016 — and a crude birth rate for everybody. It publishes no rate for "
      "non-Emirati residents and no combined rate at all.",
      "Nothing. No published rate covers non-Emirati residents or the population as a whole, so there is "
      "nothing here that can be set against the UN's figure.",
      "Emiratis are about 12% of the people living in the country, so a rate for them alone is not "
      "comparable with the UN's figure for the whole population. The pieces of a comparable rate are "
      "split apart: the federal office publishes Emirati births and an Emirati rate, but its own current "
      "population series carries an explicit note that the split by nationality is unavailable. So we know "
      "how many Emirati babies are born each year and not how many Emirati women there are to compare that "
      "count against. Individual emirates have published more: Dubai reported 1.2 for non-nationals in "
      "2014 against 3.4 for nationals. There has been no federal census since 2005 — the 2010 one was "
      "cancelled and replaced by an identity register rather than a survey — though several emirates have "
      "counted their own populations since, Abu Dhabi in 2010, Sharjah in 2015 and Ajman in 2017, none "
      "publishing much beyond a total.",
      "https://uaestat.fcsc.gov.ae/"),
    C("Czechia", "ČSÚ — registered births by the mother's age, over the mid-year female population",
      czechia_tfr, "Czechia", "complete", False,
      "One spreadsheet has the office's own total fertility rate back to 1950, together with the "
      "fertility rate for each single year of a woman's age. The yearly demographic yearbooks also "
      "publish births by single year of the mother's age as counts, one archive per year.",
      "We read the published rate and the office's own breakdown of it by age. Adding up the rate for "
      "each single year of age gives back the total the office prints — once the last row is read for "
      "what it is. That row is labelled 45-49 and is the rate for the whole group rather than for the "
      "single age 45, so it counts five times over; add every row once instead and the total comes out "
      "short by up to 0.0033. Dividing the yearbook's own counts by its mid-year population gives 1.3675 "
      "against a published 1.3679 for 2024 — the same 0.0004 gap in every year, which is rounding in the "
      "printed population.",
      "Czechia's rate rose and fell unusually sharply: 1.71 in 2018, a peak of 1.83 in 2021, then 1.37 by "
      "2024 and a preliminary 1.28 for 2025, the steepest fall in its modern series. The population "
      "figure the rate is divided by is measured at midnight between 30 June and 1 July, and counts "
      "everyone who usually lives in the country; from 2022 that explicitly includes people granted "
      "temporary protection, which is how Ukrainian refugees enter it. That moves both the births counted "
      "and the women counted — the number of women of childbearing age jumped by about 77,000 in 2022. "
      "The office does not flag this on the fertility series itself, but publishes the tables that show "
      "it.",
      "https://csu.gov.cz/produkty/demograficka-prirucka-2024"),
    C("Hungary", "KSH — registered births by the mother's age, divided by the women of that age in the "
      "middle of the year", hungary_tfr, "Hungary",
      "complete", False,
      "Hungary's statistical office, KSH, publishes its rate for every year since 1900, the rates by "
      "five-year age group since 1980, and population by single year of age — all as small files at fixed "
      "addresses. It does not put the underlying counts of births by the mother's age on those pages, but "
      "they are public: Eurostat republishes the counts Hungary reports to it, by the mother's single year "
      "of age.",
      "We plot the office's own rate. Multiplying its rates by the mid-year women gives 88,393 births for "
      "2022 against the 88,491 it reports, and a rate of 1.549 against its 1.55.",
      "Hungary's rate rose and then gave the rise back. It went from 1.23 in 2011 to a peak of 1.61 in "
      "2021, under government policies designed to raise the birth rate, then fell every year after: 1.55 "
      "in 2022, 1.51 in 2023, 1.39 in 2024 and 1.31 in 2025, below where it stood in 2013. Births are down "
      "from 93,039 in 2021 to 72,017 in 2025. Two things to note. The 1.31 for 2025 is the office's "
      "preliminary year-end estimate, published in January 2026, and has not yet been through the revision "
      "that settles a year's figures. And the population series was revised back to 2013 on the basis of "
      "the 2022 census, which found nearly 80,000 fewer people than had been estimated. We could not find "
      "a before-and-after comparison for the fertility series itself, so the size of the effect on these "
      "rates is hard to see — though it is visible indirectly, because Eurostat still carries Hungary's "
      "pre-revision figures and they run up to 0.07 higher than the office's own from 2013 on.",
      "https://www.ksh.hu/stadat_files/nep/hu/nep0006.csv"),
    C("Cuba", "ONEI — registered births by the mother's age, over the mean female population", cuba,
      "Cuba", "complete", False,
      "The demographic yearbook publishes the rate, the age-specific rates, births by the mother's age "
      "group and the mean female population by age group — as spreadsheets, one edition per year.",
      "We read the published series. We also divided the 2024 counts ourselves and reproduced ONEI's own "
      "rate to five decimal places, once its convention of folding births under 15 into the 15-19 group "
      "and births at 50 and over into 45-49 is followed.",
      "Unusually, ONEI deliberately cut the population it divides by to account for emigration, and said "
      "so. From 2021 it stopped counting the resident population and started "
      "counting the \"effective\" population — everyone actually present for at least 180 of the last "
      "365 days — because, in its words, it wanted to count the population as realistically as possible "
      "given the migratory context. Its published population fell from 11.18 million in 2020 to 9.43 "
      "million in 2025, and it publishes the net migration balance that explains it: about 1.26 million "
      "people over 2021 to 2024. So the sharp fall in the rate to 1.29 in 2024 is happening despite a "
      "population figure that has been shrunk, not because of one left stale. The years before 2021 have "
      "never been restated on the new definition, so that change is a real break in the series. Recent "
      "years, though, do move: comparing successive editions of the yearbook, 2021's rate went from 1.45 "
      "to 1.47 and 2022's from 1.41 to 1.52, in both cases because ONEI restated the population as more "
      "migration data came in — the birth counts never changed. The newest point, 1.29 for 2024, is a "
      "first-edition figure of exactly that kind, so treat it as unconfirmed rather than settled. One trap "
      "in the tables: watch for TGF, not TFG. They are the same three letters reordered, and the TFG row a "
      "little above is a different and much smaller number — births per thousand women of all ages rather "
      "than children per woman.",
      "http://www.onei.gob.cu/sites/default/files/publicaciones/2025-07/00-anuario-demografico-2024.pdf"),
    C("Papua New Guinea", "NSO — Demographic and Health Survey and Socio-Demographic and Economic Survey",
      papua_new_guinea, "Papua New Guinea", "survey", False,
      "The three figures plotted here, all from surveys: 4.4 from 2006, 4.2 from 2016-18, and 3.72 from "
      "the 2022 Socio-Demographic and Economic Survey. Earlier ones exist — a 1996 health survey put the "
      "rate at 4.8 — and the 2022 report publishes the women, the children ever born and the births in "
      "the previous twelve months behind its figure.",
      "We read the published figures. We also summed the 2022 survey's own reported rates and got 3.273, "
      "matching the raw figure it prints before adjustment, and the 2016-18 rates give 4.195 against its "
      "published 4.2.",
      "Papua New Guinea's recent censuses have published no fertility rate. The 2011 census asked the "
      "questions and never published the answers; the 2024 census asked six questions and fertility was "
      "not among them. Older ones did feed one: the statistics office put the rate at 4.6 for 2000 in a "
      "monograph on fertility and mortality, alongside a birth rate taken from that year's census. The "
      "2022 survey is a fully worked adjustment case, and a good illustration of why these numbers get "
      "corrected. Women reported fewer births in the past year than their lifetime totals implied, so the "
      "office raised the raw rate of 3.27 by about 14% to get the 3.72 it adopted. A second standard "
      "method would have given 3.93, and the office says why it chose the first: it keeps the pattern of "
      "fertility by age that women actually reported and corrects only the overall level. The census "
      "history is its own story. The census due in 2021 was deferred twice, and in the meantime the "
      "country's population was put at 11.8 million, against the 10.2 million the census eventually "
      "counted in 2024. Nothing published reconciles the two. Birth registration was 13% at the last "
      "measurement.",
      "https://www.nso.gov.pg/"),
    C("Dominican Republic", "ONE — ENHOGAR-MICS household survey", dominican_republic,
      "Dominican Republic", "survey", False,
      "The Dominican statistics office measures fertility through its household survey, most recently in "
      "2019. Its population projection carries a fertility assumption of its own, and its vital-statistics "
      "yearbooks publish registered births by the mother's age.",
      "We use the survey's own figure. Two other numbers can be worked out from the office's counts, "
      "neither of which we plot: dividing the 2010 census's counts age group by age group gives 2.51, and "
      "doing the same with 2022's registered births and projected women gives 1.86.",
      "That 1.86 is far too low to read as a fertility rate, because a recent year's registered births are "
      "not all of its births. The office publishes a table of when births were registered against when they "
      "happened, and it shows registration continuing for two decades: the 2020 birth cohort stood at "
      "141,548 when first counted and 159,466 two years later. So a rate calculated from a recent year will "
      "be too low, and by less each year as the count fills in. The office gives two reasons registration "
      "lags. Nearly a fifth of 2022 registrations were late. And registering a birth in practice requires "
      "the mother to have an identity document — the law allows other routes, but they are harder — which is "
      "why the office notes that the fall in registered births to mothers under 15 may reflect undocumented "
      "mothers rather than fewer births. About 15% of registered births in 2022 were to Haitian mothers. "
      "Two newer figures exist that we have not been able to read at source: a demographer citing the 2022 "
      "census puts fertility at 2.3, and the 2025 round of the same household survey is reported at about "
      "1.97. Both would be worth plotting once confirmed.",
      "https://www.one.gob.do/"),
    C("Belgium", "Statbel — registered births by the mother's age, over the mean female population",
      belgium_tfr, "Belgium",
      "complete", False,
      "Statbel publishes a births-and-fertility workbook for each year since 2011. It gives the rate for "
      "the country and each region, split by whether the mother is Belgian or foreign, and — in the same "
      "workbook — births by single year of the mother's age.",
      "We read the published series. As a check, dividing the 2024 births by the mean of the 1 January "
      "populations either side gives 1.4361 against the published 1.44, and reproduces Statbel's own rate "
      "for every single year of the mother's age exactly; using the 1 January count alone fits measurably "
      "worse, which confirms the mean is what it uses.",
      "The nationality split is wide and moves the picture. In 2024 the rate was 1.33 for Belgian mothers "
      "and 1.89 for foreign ones, with the headline 1.44 between them; in Flanders the same split is wider "
      "still, 1.35 for Belgian mothers against 2.13 for foreign ones. Both the births counted and the "
      "population they are divided by come from the same legal-residence register. Since 2010 the births "
      "are drawn from that register rather than from certificates filed with the civil registry, which "
      "makes 2010 a break in any longer series, and births abroad to women legally resident in Belgium are "
      "counted. Foreign nationals went from 11.2% of residents in 2015 to 13.4% in 2023 — Statbel puts the "
      "2021 rebound down to catching up after the pandemic, and the 2022-23 surge down to the war in "
      "Ukraine. 2024 and 2025 are provisional; definitive figures come about a year later.",
      "https://statbel.fgov.be/fr/themes/population/natalite-et-fecondite"),
    C("Greece", "ELSTAT — its own published fertility rate", greece_tfr, "Greece",
      "complete", False,
      "ELSTAT publishes the rate itself, for every year from 1950, in the time series of its demographic "
      "indicators release. It also publishes births by the mother's age group for every year since 1980 "
      "and population by age group at 1 January, though not the rates for each age group.",
      "We plot ELSTAT's own rate. We also rebuilt it from its births and its population, dividing births "
      "by the average of the 1 January figures either side, which is what its methodology says the "
      "average population means. That comes out about 2% lower — 1.234 against its 1.2557 for 2024, and "
      "the same gap in every year since 2011 — even though both inputs match its published tables "
      "exactly. Something inside its own calculation therefore differs from the reconstruction, which is "
      "why its figure is the one plotted.",
      "Greek fertility has fallen steeply: 1.45 in 2021 to 1.26 in 2024. Two things are worth knowing. "
      "The 2021 census found that Greece's population had been overstated by about 140,000 people. "
      "ELSTAT corrected this by revising its population estimates for 2012 through 2021, with larger "
      "corrections in the later years, and its fertility file carries a note that every rate from 2011 "
      "on rests on the revised figures. And its population table has a Greek-citizens-only column "
      "alongside the all-residents total; the age-by-sex table used for the comparison below is the "
      "total. Births are counted in the year they happened, even if they were registered later.",
      "https://www.statistics.gr/en/statistics/-/publication/DKT75/2024"),
    C("Sierra Leone", "Stats SL — Demographic and Health Survey rounds", sierra_leone, "Sierra Leone",
      "survey", False,
      "Four survey rounds, falling steadily: 5.1 in 2008, 4.9 in 2013, 4.1 in the office's 2017 household "
      "survey, and 4.2 in 2019. The 2015 census publishes its own figure, the rates behind it, and the "
      "women by age group.",
      "We plot the surveys' own figures. We also reconstructed the census's arithmetic from its own "
      "tables. Applying the rate it publishes for each age group to the women it counted in that group "
      "gives 87,472 births among 1,835,328 women. Adding those seven rates and multiplying by five gives "
      "1.567, which matches the 1.6 the census prints directly. Its own text states slightly different "
      "totals, 87,302 births among 1,831,953 women, and both land on the same rounded rate.",
      "Sierra Leone's 2015 census carries an unusually large correction. Its own reported births give a "
      "fertility rate of 1.6. Statistics Sierra Leone published that number, said it could not be right, "
      "and applied indirect methods to reach 5.6 — scaling the births from 87,302 to 328,433, a factor of "
      "nearly four. Its fertility volume is candid about why: respondents may not have used the "
      "definition of a live birth, may have misjudged the twelve-month window, may have omitted newborn "
      "deaths or reported children who were not their own — and, tellingly, a significant number of women "
      "reported more than one birth in twelve months. The office also says it chose the model whose "
      "answer sat closest to the survey trend. Its two official census volumes, published the same month, "
      "then disagree with each other about the answer: the national report adopts 5.6 and the fertility "
      "volume adopts 5.7, from a different method. The 2021 mid-term census published population counts "
      "only, with no fertility results at all.",
      "https://dhsprogram.com/pubs/pdf/FR365/FR365.pdf"),
    C("Austria", "Statistik Austria — registered births by the mother's age, over the mean female "
      "population", austria_tfr,
      "Austria", "complete", True,
      "The office publishes births by five-year age group of the mother from 2006, mean population by "
      "single year of age from 2004, and its own rate at full precision — 1.29649 for 2025. Its table of "
      "births by single year of the mother's age covers only the latest year.",
      "We divided births by women in each age group and summed. That gives 1.301 for 2025 against the "
      "office's 1.2965, and 1.317 against its 1.3115 for 2024: using five-year age groups instead of each "
      "single year of age changes the answer by about a third of a percent. For 2025, the one year where "
      "births by single year of age are published, doing it at that detail instead gives 1.2958 — within a "
      "thousandth of the office's own figure.",
      "Austria's rate has fallen every year since 2021 except one, from 1.48 to 1.30. Nationality matters "
      "here, and the births file gives it directly: of the 76,067 children born in 2025, 18,759 were to "
      "mothers who are not Austrian citizens, just under a quarter. The population the rate is divided by "
      "is stated plainly as the mean over the year, which is also why the office withholds its final rate "
      "until the "
      "mid-year population is settled around July. Births are counted by when they happened, from the "
      "central civil-status register, and each year is cleaned once in the following spring: the "
      "provisional 2025 figure of 75,718 births became 76,067.",
      "https://www.statistik.at/statistiken/bevoelkerung-und-soziales/bevoelkerung/geburten"),
    C("Switzerland", "BFS — registered births by the mother's age, over the female population",
      switzerland_tfr,
      "Switzerland", "complete", False,
      "The office publishes its rate back to 1803, and separately for Swiss and foreign mothers since "
      "1971. One spreadsheet, its table T 01.04.01.01, carries births by the mother's age group, the rate "
      "for each age group, and the total, for every year from 1960. All of it downloads freely, with no "
      "registration and no access key.",
      "We plot the published rate. Adding up the office's own rates for each age group and multiplying by "
      "five gives 1.2949 for 2024 against its published 1.2879, and the same half-percent excess in every "
      "year back to 1960. That is expected rather than wrong: the figures it prints for each age group are "
      "averages across the five single ages inside it, while its own total is built from each single age, "
      "so the check confirms the shape of the calculation without reproducing it exactly.",
      "The nationality gap is about 0.3 and stable: 1.20 for Swiss mothers against 1.50 for foreign ones "
      "in 2024, with 1.29 overall. Two things to be careful about. There is a second office series, the "
      "one that feeds its population scenarios, whose rates by age sum to 1.40 for 2024 against the 1.29 "
      "it publishes as the actual figure — an 8.7% difference, because the scenario schedule is smoothed "
      "across years. It is the wrong series to use and an easy one to reach for. And the latest year "
      "moves: 2024 was first published as 1.28 in April 2025, described as provisional, and settled at "
      "1.29 in the definitive figures that September. The office states in its statistical yearbook that "
      "rates like these are per thousand women of the mean resident population, so the population it "
      "divides by is an average over the year rather than a count on one date. What it does not say is "
      "whether births are dated to when they happened or when they were registered.",
      "https://www.bfs.admin.ch/asset/de/je-d-01.04.01.01"),
    C("Portugal", "INE — registered births by the mother's age, over the resident female population",
      portugal_tfr, "Portugal",
      "complete", False,
      "Statistics Portugal, INE, publishes its own fertility rate, births by the mother's age, and "
      "population by age group, all free to query with no sign-up. Its annual demographic statistics "
      "report also prints the rate for each age group.",
      "We plot INE's own rate. We also rebuilt it from the counts and got about 3% less: 1.284 for 2023 "
      "against a published 1.32. Averaging the population estimates either side of the year closes part "
      "of the gap but not all of it, so INE must be working from a slightly different population count "
      "than the one it publishes by age group. Its own rates by age group add up to its published total "
      "exactly, so the difference sits between its figures and ours rather than inside its own.",
      "Portugal fell to 1.21 in 2013, recovered to 1.43 by 2019, and has fallen back since. The thing to "
      "know about the recent years is that INE cut them sharply in June 2026, when it revised its "
      "estimates of how many people live in the country. The rate for 2023 went from 1.44 to the 1.32 "
      "plotted here, a fall of about 8% with no change at all in the number of births. That revision is "
      "also most of why the distance from the UN's figure widens from 2021 on: INE now counts about 10% "
      "more women aged 15 to 49 than the UN does, while the two count almost the same number of births. "
      "Recent years may move again the next time those estimates are revised. INE also reissues each "
      "indicator under a new number whenever it redraws Portugal's statistical regions, and each edition "
      "carries only the last few years, so a long series has to be pieced together from two or three of "
      "them; they agree exactly where they overlap. INE publishes births by the mother's nationality too, "
      "which matters given how much immigration there has been since 2018, but we have not used it.",
      "https://www.ine.pt/"),
    C("Israel", "CBS — registered births by the mother's age, over the mean female population", israel,
      "Israel", "complete",
      False,
      "Israel's Central Bureau of Statistics publishes fertility rates by age and religion in its "
      "statistical abstract, along with births by single year of the mother's age. The table gives annual "
      "figures only for its most recent years and five-year averages before that, which is why the series "
      "here is short: three points, for 2020, 2022 and 2023.",
      "We read the published figures and checked that the bureau's own rates for each age group in 2023 "
      "add up to 2.847, against the 2.85 it prints. We also checked one age group against the underlying "
      "counts of births and women, and it matched to within 0.3%.",
      "Israel's rate is high for a rich country, and it is high because of a wide spread between groups: "
      "3.00 for Jewish women in 2023, 2.81 for Muslim women, 1.75 for Druze and 1.64 for Christian, "
      "against 2.85 for everyone. The population figure the rate is divided by is the average over the "
      "year, and both the births and the population include East Jerusalem and the Golan Heights. "
      "Residents who are foreign nationals, about 2% of the population, are left out of it.",
      "https://www.cbs.gov.il/en/publications/Pages/2024/Population-Statistical-Abstract-of-Israel-2024-No.75.aspx"),
]

PANELS = [(c["name"], c["src"], c["loader"], c["wpp_name"]) for c in COUNTRIES if c["loader"]]
DOCS = {c["name"]: (c["found"], c["method"], c["caveats"], c["url"]) for c in COUNTRIES}
