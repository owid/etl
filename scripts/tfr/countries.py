"""The country registry.

Every country carries two independent attributes:

* ``tier`` — what the national number is built from. This is the quality ladder, and it says
  nothing about whether we could recompute it.
* ``recalculated`` — whether the figure shown is one we computed from counted births and
  women, rather than a rate or total the office published itself. A transparency badge, not
  a quality claim: an incomplete registry we can decompose is still an incomplete registry.

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
from canada import canada_tfr  # noqa: E402
from china import china_tfr  # noqa: E402
from colombia import dane_female_pop, dane_registered_tfr  # noqa: E402
from france import france_tfr  # noqa: E402
from india import india  # noqa: E402
from philippines import philippines  # noqa: E402
from korea import korea_tfr  # noqa: E402
from myanmar import myanmar_tfr  # noqa: E402
from published import (  # noqa: E402
    afghanistan,
    algeria,
    bangladesh,
    drc,
    ethiopia,
    indonesia,
    iraq,
    kenya,
    nigeria,
    pakistan,
    russia,
    sudan,
    tanzania,
    vietnam,
    yemen,
)
from sources import egypt, england_wales, germany, japan, mexico, thailand, united_states  # noqa: E402
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
      "to one decimal place. Annexure table 15 gives it annually for 2019-24. There is also a chart of "
      "the 1985-2024 trend, but it is a picture with no numbers behind it.",
      "We read the India row of annexure table 15 straight out of the report's PDF. No arithmetic of our "
      "own — the number is exactly what the Registrar General states.",
      "The SRS is a sample, not full civil registration: roughly 8 million people are enumerated "
      "continuously and the rates are scaled up. India's civil registration now records almost all "
      "births, but it does not tabulate the mother's age consistently across states, so there is no "
      "registration-based national figure to use instead. One decimal place also means the series looks "
      "flatter than it is.",
      "https://censusindia.gov.in/nada/index.php/catalog/47152"),
    C("Colombia", "DANE — Estadísticas Vitales", colombia, "Colombia", "complete", True,
      "DANE publishes registered births by five-year age group of mother every year, and national "
      "population by single year of age and sex.",
      "We divided registered births in each age group by the female population of that group and summed "
      "across groups. Our figures match DANE's own published rates to within rounding.",
      "DANE also publishes a projection-based fertility rate of about 1.68 for 2024 — far above what its "
      "own registry shows. The 2025 figure is provisional and will revise up slightly, as 2024 did. "
      "The denominators are DANE's population projections, which are themselves modeled.",
      "https://www.dane.gov.co/index.php/estadisticas-por-tema/salud/nacimientos-y-defunciones"),
    C("Mexico", "INEGI — registered births, over CONAPO population", mexico, "Mexico", "complete", True,
      "INEGI's OLAP cube gives registered births by year of occurrence and mother's age group, for every "
      "registration year separately. CONAPO publishes mid-year population by sex and single age.",
      "We summed births for each year of occurrence across all registration years, then divided by "
      "CONAPO's female population and summed across age groups.",
      "We stop at 2022. Later years of occurrence are still filling up with late registrations — only "
      "about 993,000 of 2024's births had been registered by the time of this extract, against roughly "
      "1.67 million expected — so plotting them would show a collapse that is not real. CONAPO's own "
      "model-based figure is well above the registry.",
      "https://www.inegi.org.mx/programas/natalidad/"),
    C("Philippines", "PSA — OpenSTAT registered live births", philippines, "Philippines", "incomplete", True,
      "PSA's OpenSTAT database gives registered live births by age group of mother, one table per year, "
      "and a projected population by five-year age group and sex.",
      "We divided registered births by the projected female population in each age group and summed. "
      "The population table is published in thousands, which we scaled up.",
      "Registration covers roughly 90% of births and is tabulated by year of registration, so this "
      "understates the real rate — our 1.49 for 2024 against PSA's own survey-based figure of about "
      "1.9. Only two years, because OpenSTAT publishes a separate table per year.",
      "https://openstat.psa.gov.ph/"),
    C("Egypt", "CAPMAS — Annual Bulletin of Births and Deaths", egypt, "Egypt", "complete", False,
      "Each annual bulletin has a table 13 giving age-specific fertility rates, the female population of "
      "each age group, and CAPMAS's own total fertility rate. Table 12 separately gives registered live "
      "births by age of mother, as counts.",
      "We summed the age-specific rates and multiplied by the five-year band width, which reproduces "
      "CAPMAS's own published total exactly. The age-band comparison uses the registered births from table "
      "12 rather than the rates, because those are the only figures the registry itself produces.",
      "CAPMAS's published rates are not its registered births divided by its own population. The bulletin "
      "says so plainly: they are estimated with the fertility module of the Population Analysis "
      "Spreadsheets, a US Census Bureau tool that takes the crude birth rate and the female population and "
      "imposes a model age pattern. Dividing the registry's own counts instead gives 2.38 for 2024 against "
      "the published 2.41 — close in total, but with a very different shape, 143 births per thousand women "
      "aged 25-29 where CAPMAS publishes 164. That means Egypt's line is closer in kind to a UN estimate "
      "than to a pure count. The fertility table only appears from the 2019 edition onward and the counts "
      "by age of mother only from 2021, so the series is short; CAPMAS also revised its 2015 figure from "
      "3.7 down to 3.3 between editions without explanation. One band is labeled 40-45 where it means "
      "40-44. CAPMAS's site is a JavaScript application, but the catalog behind it answers plain requests "
      "and every publication file downloads directly.",
      "https://www.capmas.gov.eg/"),
    C("Brazil", "IBGE — Estatísticas do Registro Civil", brazil, "Brazil", "complete", True,
      "IBGE's SIDRA database gives registered births by mother's age group, and population projections "
      "by sex and single age. Both have a public API.",
      "We divided births by the female population in each age group and summed. Births come from SIDRA "
      "table 197 for 2000-02 and table 2612 for 2003 onward.",
      "The 2000-02 points come from the older table and sit too low, because birth registration coverage "
      "was still improving — the step up in 2003 is coverage, not fertility. The 2024 figure is "
      "provisional. IBGE's own published fertility rate comes from projections, not the registry.",
      "https://sidra.ibge.gov.br/pesquisa/registro-civil/tabelas"),
    C("England and Wales", "ONS — birth registrations over the mid-year population", england_wales,
      "United Kingdom", "complete", False,
      "ONS table 10 gives live births and age-specific fertility rates by age group of mother, back to 1938. "
      "ONS also publishes mid-year population by single year of age and sex for the United Kingdom and each "
      "of its nations, 2011 to 2024, in one spreadsheet.",
      "We summed the age-specific rates and multiplied by the band width. The age-band comparison divides "
      "the births by ONS's own mid-year female population, which is what ONS itself divides by.",
      "This is England and Wales, about 89% of UK births, while the UN figures are UK-wide. We measured how "
      "much that matters: ONS's own reference table gives both, and England and Wales sits a steady 0.02 "
      "above the UK — 1.55 against 1.53 in 2021, 1.58 against 1.56 in 2020, 1.65 against 1.63 in 2019. That "
      "is far smaller than the gaps this page is about, so the shorter, more current England and Wales "
      "series is used rather than the UK one, which ONS stopped publishing after 2021. Building a UK figure "
      "ourselves would mean stitching in Scotland and Northern Ireland; Scotland's office publishes what is "
      "needed, but Northern Ireland's refuses scripted downloads outright, so it would not be reproducible. "
      "The 2025 figure is provisional and ONS calculates it against population projections rather than "
      "estimates, so the age-band comparison for that year falls back to the population its own rate "
      "implies. Table 10 publishes rates on both a 15-44 and a 15-49 base; the 15-49 column is empty for "
      "most recent years, so the 15-44 one is used throughout.",
      "https://www.ons.gov.uk/peoplepopulationandcommunity/birthsdeathsandmarriages/livebirths"),
    C("Germany", "Destatis — Geburtenstatistik over the average resident population", germany, "Germany",
      "complete", False,
      "Destatis table 12612-0008 gives live births per 1,000 women for every single year of age 15-49, from "
      "1972 to 2025. Table 12612-0005 gives the births themselves by age of mother and birth order, from "
      "2009. Table 12411-10, in the annual population report, gives the average population over the year by "
      "single year of age.",
      "We summed the rates across ages. Because they are single-year rates there is no band width to "
      "multiply by. The age-band comparison divides the births by the population from 12411-10, which is "
      "the concept Destatis itself uses — the mean of the stocks at the start and end of the year.",
      "Two traps we checked and got right. The population table has an all-residents column and a "
      "German-nationals column; the nationals column is 20-30% smaller and using it would push the rate far "
      "too high, so the all-residents column is the one that fits. And the births tables date a mother's age "
      "by subtracting birth years rather than by exact age on a reference date, while the population tables "
      "use exact age — mixing the two biases a recalculation by about 1%, which is why the line stays "
      "Destatis's own summed rates. Rebased on the 2022 census from 2012 onward, so numbers differ slightly "
      "from releases published before the rebasing — our 2023 comes out at 1.385 where the original release "
      "said 1.35. The 2025 figure is provisional. The age-band comparison only starts in 2009, because that "
      "is where the births table begins, and the average-population table ships one year per report edition. "
      "Neither births file could be pulled through the GENESIS API — the access token we have authenticates "
      "as the guest account, which cannot download tables — so both were exported by hand from the web "
      "interface; the population report, by contrast, downloads as a plain spreadsheet with no token at all.",
      "https://www-genesis.destatis.de/genesis/online"),
    C("Thailand", "NSO — Statistical Yearbook tables 1.10 and 1.4", thailand, "Thailand", "complete", True,
      "The Statistical Yearbook has table 1.10, registered live births by age group of mother, and table "
      "1.4, registered population by age group and sex.",
      "We divided births by the female population in each age group and summed. Both tables were read "
      "out of the yearbook PDFs.",
      "Each yearbook edition prints only a rolling three-year window, and the two tables cover different "
      "windows, so only the years where they overlap can be computed. Earlier editions would extend the "
      "series back to about 2017.",
      "https://www.nso.go.th/"),
    C("France", "INSEE — état civil over the mean resident population", france, "France", "complete", False,
      "INSEE publishes fertility rates by single year of age of mother and, separately, births by single year "
      "of age, both from the civil register. Its population pyramid gives women by single year of age at 1 "
      "January, for the whole country from 1991 and for mainland France from 1901.",
      "We summed the rates across ages. The age-band comparison divides the births by the mean population "
      "over the year — the average of the 1 January figures for that year and the next — because that is how "
      "INSEE defines a fertility rate: births to women of an age over \"la population moyenne de l'année des "
      "femmes de même âge\".",
      "Excludes Mayotte before 2014 and includes it after, a small discontinuity we have not corrected. INSEE "
      "does publish a mainland-only series with no such break, going back to 1901, but mainland France is not "
      "the territory the UN figures cover, so switching would trade one mismatch for another. Population "
      "estimates for a given year get revised in later releases — about 1% for one cohort we checked — so "
      "rates and population have to come from the same vintage. The 2025 figure is provisional. The single "
      "year of age population is not in INSEE's documented API, which only carries five-year bands; it comes "
      "from the data file behind INSEE\'s own interactive population pyramid, which answers plain requests but "
      "is not a published interface, so the path could change.",
      "https://www.insee.fr/fr/statistiques/8999017"),
    C("Japan", "MHLW — Vital Statistics via e-Stat", japan, "Japan", "complete", False,
      "e-Stat table 0003411608 gives the total fertility rate and each age group's contribution to it; "
      "table 0003411607 gives births by the same age groups. The Statistics Bureau separately publishes "
      "population by single year of age and sex every October, as a spreadsheet download.",
      "We took MHLW's published total fertility rate directly. For the age-band comparison we divided "
      "MHLW's births by the Statistics Bureau's female population, which is where MHLW says its own "
      "denominator comes from. That gives 1.145 for 2024 against the published 1.15.",
      "Five-yearly before 2000 and annual after, so the early part of the line is coarse. e-Stat "
      "publishes each band as its contribution to the total rather than as a rate per 1,000 women — the "
      "seven bands sum exactly to the published total, so do not multiply those by five. MHLW's rate "
      "counts Japanese women only, not all residents, and the population file gives both columns; using "
      "the wrong one inflates the denominator by the foreign resident population. Births are published "
      "only in five-year bands, so a full recalculation would still approximate. The e-Stat API needs a "
      "key, but registration is email-only and the spreadsheets download without one.",
      "https://www.e-stat.go.jp/"),
    C("Italy", "ISTAT — ANPR / stato civile", italy, "Italy", "complete", False,
      "ISTAT's SDMX service publishes age-specific fertility rates by single year of age of mother, from "
      "2000 onward.",
      "We summed the rates across single ages.",
      "We could not get births or female population by age: ISTAT's population dataflow covers every "
      "municipality and times out when requested unfiltered, so there is no age-band comparison for "
      "Italy yet. Sourced from the national resident register (ANPR).",
      "https://esploradati.istat.it/"),
    C("United States", "CDC / NCHS — natality via data.cdc.gov", united_states, "United States",
      "complete", False,
      "NCHS publishes age-specific birth rates per 1,000 women by maternal age group in two datasets: a "
      "historical one running 1940 to 2018, and a current one covering 2016 to 2024. It also publishes its "
      "own total fertility rate in the annual Births report.",
      "We summed the rates and multiplied by the band width, which is exactly how NCHS builds its own "
      "total. The two datasets are stitched at 2016, where they overlap. Our figures reproduce NCHS's "
      "published totals to the decimal — 1.621 for 2023, 2.056 for 2000.",
      "The current dataset splits 15-19 into 15-17 and 18-19 as well as the whole band, which would "
      "double-count if not filtered, and labels the top band 45-54 where the rate is per woman aged 45-49. "
      "Births by age of mother as counts do exist, in the annual Births report and in the record-level "
      "natality files, so a recalculation against Census population is possible — but which Census vintage "
      "you divide by moves the answer by about 1.4%, so it would no longer match NCHS. The CDC website "
      "itself refuses automated requests; these two datasets come from a separate host that does not.",
      "https://data.cdc.gov/"),
    C("Russia", "Rosstat — Demographic Yearbook of Russia", russia, "Russia", "complete", False,
      "The yearbook is eight chapter spreadsheets behind an HTML index. Chapter 2 gives the total fertility "
      "rate for the whole country back to the early 1960s; chapter 4 gives live births by age of mother as "
      "counts. A separate bulletin gives female population by single year of age at 1 January.",
      "We read the national column of the fertility-rate sheet. Dividing the births by the population gives "
      "1.39 for 2022 against Rosstat's published 1.416, so the age-band comparison uses those counts while "
      "the line stays Rosstat's own figure.",
      "The 1.8% gap is the denominator: Rosstat divides by the average population over the year, and the "
      "only population file on the same census basis is the one for 1 January, which is slightly larger. "
      "There is also a rebasing trap — Rosstat did not rebase its published rates for 2011-21 on the 2020 "
      "census, but the population files are all rebased, so only 2022 is internally consistent. The 2022 "
      "figure excludes the four annexed Ukrainian regions; Crimea has been included since 2014. Rosstat's "
      "site serves a broken certificate chain, so downloads need certificate checking relaxed, and its "
      "unified statistics database refuses requests from outside the country altogether. The series stops "
      "at 2022 because that is the latest yearbook edition.",
      "https://rosstat.gov.ru/folder/12781"),
    C("Vietnam", "National Statistics Office — population change and family planning survey", vietnam,
      "Vietnam", "survey", False,
      "The office publishes a total fertility rate annually from 2001. Its PxWeb database stops at 2023, but "
      "the 2025 survey report prints the whole 2001-2025 series in one table. The report and the 2019 census "
      "volume also publish births by age of mother and population by age group, though only inside long PDFs.",
      "We read the whole-country column of that table. No arithmetic of our own.",
      "This is not a count of registered births. The office estimates the rate from a household sample survey "
      "and then adjusts it upward with the Trussell P/F technique, because women under-report births in the "
      "previous twelve months. Its own report says so but never prints the adjustment. Dividing the 2019 "
      "census's own counts gives 1.85 against the published 2.09, so the correction that year was about 13% — "
      "which makes Vietnam's figure closer in kind to a model estimate than to a count. Doing the same on the "
      "2025 survey tables implies a far larger correction, but the survey's birth table counts mothers rather "
      "than births and may not be scaled the same way as its population table, so we do not trust that "
      "comparison and have not used it. Some years come from a different instrument: 2024 is from the "
      "mid-term population and housing survey, not the annual one. Vietnam does have civil registration, but "
      "it is not what the published rate is built on, and neither PxWeb nor the statistical yearbook carries "
      "any table of births or population by age at all.",
      "https://www.nso.gov.vn/"),
    C("Bangladesh", "BBS — Sample Vital Registration System", bangladesh, "Bangladesh", "sample", False,
      "The 2023 SVRS report carries a table of every year from 1982 to 2023, giving the total fertility "
      "rate alongside four other fertility measures. Each annual edition also states its own year's figure "
      "in the text, with a confidence interval.",
      "We read the whole 42-year table out of the report. We also summed the age-specific rates for 2023 "
      "and got 2.18 against the printed 2.175, which is what the rounding of those rates allows.",
      "The SVRS is a sample of about 2,000 areas where resident registrars record births monthly — a "
      "continuous sample system, not full civil registration. The health survey run by the government's "
      "population institute puts the rate about 0.2 higher, because it asks women to recall past births "
      "and averages three years, so the two are not measuring quite the same thing. BBS publishes no "
      "birth or population counts by age, only rates, so there is nothing to recalculate from. Everything "
      "is PDF only, served from a cloud bucket behind the BBS site.",
      "https://bbs.gov.bd/"),
    C("Indonesia", "BPS — censuses and inter-censal surveys", indonesia, "Indonesia", "survey", False,
      "BPS publishes a total fertility rate for census and inter-censal survey rounds only. Its 2025 survey "
      "release charts all four rounds together: 2.41 for the 2010 census, 2.28 for the 2015 survey, 2.18 "
      "for the long form of the 2020 census, and 2.13 for the 2025 survey.",
      "We read all four values out of that chart. An earlier version of this page had 2.42 for the 2020 "
      "long form, which is wrong — BPS's own figure is 2.18, and 2.41 belongs to the 2010 census.",
      "BPS says plainly that Indonesian civil registration coverage is still incomplete and cannot be used "
      "for this, so the rate comes from retrospective questions about children ever born. Each round is "
      "placed at the year BPS names it for; the 2020 long form was actually fielded in 2022. BPS's main "
      "website refuses automated requests, so this release had to be read through a web archive; the "
      "separate census microsite does answer, but its fertility tables hold children ever born over a "
      "lifetime, not one year's births, so there is nothing to recalculate from.",
      "https://www.bps.go.id/id/pressrelease/2026/05/05/2645/"),
    C("Pakistan", "PBS — Pakistan Demographic Survey", pakistan, "Pakistan", "survey", False,
      "The Bureau of Statistics ran the Demographic Survey annually until 2007, then again in 2020. Every "
      "edition still online prints a total fertility rate: 4.1 in 2001 falling to 3.7 in 2007, and 3.7 again "
      "in 2020. The 2007 round also publishes live births and female population by age group as counts.",
      "We read each edition's own figure out of its report. For 2007 we also did the arithmetic from the "
      "counts: births divided by women in each age group, summed and multiplied by the band width, gives "
      "3.69 — the Bureau's published 3.7.",
      "Pakistan has no usable civil registration for this. The Bureau's own report explains it revived the "
      "survey after a thirteen-year gap because the national database authority told them vital-event "
      "records were not good enough — hence the empty stretch from 2008 to 2019. The 2020 figure covers "
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
      "We divided births by women at each single age and summed. This gives 1.221 for 2000, 1.188 for 2010 "
      "and 1.296 for 2020. The bureau's own companion table computes 1.301 for 2020 from five-year bands, "
      "and the press conference rounded that to 1.3.",
      "Only census years. Between censuses the bureau relies on an annual sample survey of about one person "
      "in a thousand, whose age detail appears in a yearbook it does not publish free of charge — so there "
      "is no way to fill the gaps, and the line jumps ten years at a time. The counts are the census long "
      "form's sample, not the whole population, and the reference period is the twelve months to 31 October "
      "of the census year rather than the calendar year. Chinese demographers argued the 2010 census figure "
      "of 1.18 was implausibly low because births went unreported. The bureau's queryable data portal "
      "refuses requests from outside the country, but these static files download without trouble.",
      "https://www.stats.gov.cn/sj/pcsj/rkpc/7rp/zk/html/B0603.xls"),
    C("Nigeria", "National Population Commission — Nigeria Demographic and Health Survey", nigeria,
      "Nigeria", "survey", False,
      "The 2024 survey report sets every round since 2003 side by side in one table: 5.7 in 2003 and 2008, "
      "then 5.5, 5.3 and 4.8. The Bureau of Statistics separately publishes a row called Calculated TFR, "
      "and its own 2021 household survey found 4.6.",
      "We read the trend table out of the survey report. Summing its age-specific rates and multiplying by "
      "the band width gives 4.79, the published 4.8.",
      "These are measured rates from women's birth histories, not registration — birth registration in "
      "Nigeria is far too incomplete to use. Each round's figure covers the three years before its "
      "fieldwork, so the years are approximate. We do not use the Bureau's Calculated TFR row: the "
      "Population Commission produces it by drawing a straight line between survey rounds, which is why it "
      "falls by almost the same amount every year, and it had Nigeria at 5.14 in 2022 when the next actual "
      "measurement came in at 4.8. Female population by age exists only as projections from the 2006 "
      "census, and they do not reconcile with the survey's own denominators — about 11% apart — so there is "
      "nothing solid to recalculate from.",
      "https://nationalpopulation.gov.ng/publications"),
    C("Iran", "Statistical Center of Iran and the civil registration organization — nothing we could reach",
      None, "Iran", "none", False,
      "Nothing we could get to. We found one real table — registered births by age of mother for the whole "
      "country in 1386, which is 2007-08 — but only through a web archive, and no female population by age "
      "to divide it by. No total fertility rate was found anywhere, live or archived.",
      "Nothing. Iran is on this list rather than filled in from an international compilation, which is the "
      "point of the exercise.",
      "The three hosts fail in three different ways, and the difference matters for anyone trying again. "
      "The statistics center at amar.org.ir accepts a connection and then drops it before sending anything "
      "back, identically across every TLS version, cipher setting and browser header we tried — that is a "
      "perimeter device refusing foreign traffic, not a certificate problem. Its other domain, sci.org.ir, "
      "answers normally but serves a maintenance notice, so it is worth retrying later. The civil "
      "registration organization at sabteahval.ir answers too, but with a bot challenge that wants device "
      "motion data; a real browser might well get through where a script cannot. Do not use nocr.ir — it "
      "is no longer the registration organization's domain and now redirects to a domain reseller.",
      "https://www.amar.org.ir/"),
    C("Turkey", "TurkStat — birth statistics over the address-based population register", turkey_tfr,
      "Turkey", "complete", False,
      "TurkStat's annual birth statistics bulletin publishes age-specific fertility rates and births by "
      "mother's age group, both from 2001. An older population portal gives female population by five-year "
      "age group from the address-based population register, back to 1935.",
      "We summed the age-specific rates and multiplied by the band width, giving 1.484 for 2024 against the "
      "1.48 TurkStat states. We also checked it from the counts: dividing the registered births by the "
      "register's female population gives 1.4832, which agrees with TurkStat's own arithmetic to four "
      "decimal places. The age-band comparison uses those counts.",
      "The two bulletin tables cannot be fetched by a script. TurkStat's current portal is a JavaScript "
      "application whose download links are single-use tokens, so they were downloaded by hand; the "
      "population portal, by contrast, answers a form-encoded POST, though a JSON body gets a 404. TurkStat "
      "splits the teens into 15-17 and 18-19, which we fold together. It also revises birth figures for up "
      "to five years after first release — rows carry an (r) marker — so editions disagree with each other, "
      "and the population figures before 2007 come from censuses rather than the register, a break inside "
      "one table. The population series is complete from 2007, so the age-band comparison cannot go earlier.",
      "https://data.tuik.gov.tr/Bulten/Index?p=Dogum-Istatistikleri-2024-54196"),
    C("Ethiopia", "Statistical Service — Ethiopia Demographic and Health Survey", ethiopia, "Ethiopia",
      "survey", False,
      "The Statistical Service publishes a total fertility rate for each survey round. The 2024-25 "
      "report's trend figure prints the national value for all five rounds since 2000, and its table 3 "
      "gives the age-specific rates behind the latest one.",
      "We read the five round values off the trend figure. Summing the age-specific rates in table 3 and "
      "multiplying by the band width gives 4.05, which is the published 4.0.",
      "These are household surveys asking women about past pregnancies, not registration. Each point is "
      "the average of the three years before the survey, so the years are approximate. Ethiopia's last "
      "completed census was 2007 and the next has been postponed repeatedly, so there is no recent "
      "female population by age to divide by — the 2007 census is the only one, and it is nineteen years "
      "old. The old statsethiopia.gov.et domain is dead; the service is now at ess.gov.et.",
      "https://ess.gov.et/wp-content/uploads/2026/01/edhs-2024-25-kir-01172026.pdf"),
    C("Democratic Republic of Congo", "INS — Enquête Démographique et de Santé", drc,
      "Democratic Republic of Congo", "survey", False,
      "The Institut National de la Statistique publishes a fertility index for each survey round. The "
      "2023-24 report gives 5.5 and states the previous round measured 6.6.",
      "We took both figures as stated. Summing the age-specific rates in table 5.1 and multiplying by the "
      "band width gives 5.47, which is the published 5.5.",
      "There has been no census since 1984 and no civil registration good enough to use, so every "
      "national fertility figure comes from one of the three surveys. The report's age structure is the "
      "surveyed sample, not a population count, so there is nothing to recalculate from. The institute's "
      "ins-rdc.org domain returns a block; the working site is ins.gouv.cd.",
      "https://ins.gouv.cd/publication/RDC-EDS-III.pdf"),
    C("Tanzania", "NBS — 2022 Population and Housing Census", tanzania, "Tanzania", "survey", False,
      "The census fertility monograph prints age-specific rates twice: as women reported them, and after "
      "adjustment. The reported rates sum to a total fertility rate of 3.2, the adjusted ones to 4.6. NBS "
      "presents 4.6 as the country's figure. The 2022 household health survey separately found 4.8.",
      "We read the adjusted total out of table 3.2. We also checked the arithmetic: the reported rates sum "
      "to 3.195 and the adjusted ones to 4.63, matching both printed totals.",
      "The gap between 3.2 and 4.6 is entirely the adjustment. NBS applies the Arriaga method because "
      "women under-report and misdate births, and says plainly that Tanzania's vital registration is too "
      "incomplete to compute the rate directly. The census does publish births in the last twelve months "
      "and women by age group, so a direct recalculation is possible, but it would reproduce the "
      "unadjusted 3.2 rather than the figure NBS stands behind.",
      "https://www.nbs.go.tz/statistics/topic/demographic-and-socio-economic-statistics"),
    C("Spain", "INE — birth statistics over the continuous population count", spain_tfr, "Spain",
      "complete", True,
      "INE publishes registered births by single year of age of mother, 2009 onward, and female "
      "population by single year of age every quarter back to 1971. Both come out of its open service as "
      "whole-table CSV with no key and no registration.",
      "We divided births at each single age by the women of that age on 1 July and summed. Our figures "
      "land within 0.01 of INE's own published fertility indicator every year — 1.107 against 1.10 for "
      "2024, 1.122 against 1.12 for 2023.",
      "The population series is the Estadística Continua de Población, which replaced the older municipal "
      "register figures and revises as registrations arrive. Births start in 2009 on this table, so the "
      "line is shorter than the population series behind it. The 2024 figure is the first release and will "
      "revise slightly.",
      "https://www.ine.es/dyngs/INEbase/es/operacion.htm?c=Estadistica_C&cid=1254736177007"),
    C("South Korea", "Statistics Korea — annual birth statistics release", korea_tfr, "South Korea",
      "complete", False,
      "Statistics Korea publishes its birth statistics in English every year as a PDF, and each edition's "
      "first table gives an eleven-year run of the total fertility rate. Some editions also print live "
      "births by mother's age group as counts. The interior ministry separately publishes female population "
      "by five-year age group from the resident register, as a spreadsheet download.",
      "We chained five editions of the release to build 2003-2024, taking the newer edition wherever two "
      "overlap because the office revises. As a check we recalculated 2015 from the counts — births by age "
      "group from the release, women by age group from the resident register — and got 1.245 against the "
      "published 1.24.",
      "The obvious source would be the KOSIS database, but its interface refuses scripted requests and its "
      "API rejects everything without a key; getting a key means a Statistics Korea account, and that needs "
      "either a Korean mobile phone registered in your own name or a Korean identity number. There is no "
      "route for a foreigner and no English signup at all, so this uses the published PDFs instead. Editions "
      "before 2013 print only the current and previous year, which is why the series starts in 2003. Recent "
      "editions dropped the full births-by-age table, so a recalculation for the latest year is not "
      "possible from them.",
      "https://mods.go.kr/board.es?mid=a20108010000&bid=11773"),
    C("Canada", "Statistics Canada — births by age of mother over its population estimates", canada_tfr,
      "Canada", "complete", True,
      "Statistics Canada publishes registered live births by age group of mother, female population by "
      "age group at 1 July, and its own fertility rate — all three as plain CSV from one open service "
      "with no key, annually from 1991.",
      "We divided the births by the female population in each age group and summed. Our figures match "
      "Statistics Canada's own published rate to the second decimal in every year we checked: 1.269 "
      "against 1.27 for 2023, 1.255 against 1.25 for 2024, 1.510 against 1.51 for 2018.",
      "Births are dated by the year they occurred, not the year they were registered, and late "
      "registrations are folded back into the year of the birth by annual revision — about a thousand cases "
      "five years on — so recent years do not understate. The 2024 figure is flagged provisional in the "
      "table notes but not in the data itself, so anything automated has to treat the latest year as "
      "provisional by hand. Nova Scotia under-recorded births in 2021. Births to mothers of 50 and over are "
      "folded into the 45-49 row for confidentiality, and mothers whose age was not stated are spread "
      "across the bands. There is no citizens-only denominator to pick wrongly here, but the population does "
      "include non-permanent residents, and the number of women aged 20-24 rose 9% between 2019 and 2024 "
      "partly on student migration — so some of the fall in the rate is the denominator growing rather than "
      "births falling.",
      "https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1310041601"),
    C("Yemen", "CSO and health ministry — National Health and Demographic Survey 2013", yemen, "Yemen",
      "survey", False,
      "One figure inside our window: 4.4 children per woman for the three years to 2013, from the survey "
      "the Central Statistical Organisation ran with the health ministry. The 2004 census is also covered, "
      "but the organisation publishes two different figures for it — 4.93 by one method and 6.1 by another.",
      "We read the national column of table 2. Summing its age-specific rates and multiplying by the band "
      "width gives 4.43, the published 4.4.",
      "We use the 2013 survey rather than the census because the census figures are not one number: the "
      "organisation's own yearbook prints 4.93 from directly observed births and 6.1 from cumulating "
      "age-specific rates, and a separate study of its own reaches 6.1 by indirect estimation. Choosing "
      "between those is a methodological judgement, not a lookup, so the survey — a single figure from "
      "standard birth-history estimation — is the safer one. Nothing has been published since 2013; the war "
      "began in 2015. Three of the organisation's four domains are unusable: cso-yemen.com sits behind a "
      "firewall that rejects anything that is not a browser, cso.gov.ye has its secure port blackholed, and "
      "cso-yemen.org and the planning ministry's domain have both lapsed to squatters — the former now "
      "serves a spam blog, so anything found live there should not be trusted. This report came from a web "
      "archive.",
      "https://web.archive.org/web/20220608201754/https://cso.gov.ye/about_cso"),
    C("Argentina", "Health ministry births over INDEC population", argentina_tfr, "Argentina",
      "complete", True,
      "The health ministry publishes registered live births by age group of mother as open CSV, annually "
      "from 2005. INDEC publishes female population by age group in two projection series, one based on the "
      "2010 census and one on the 2022 census. What INDEC does not publish is an annual fertility rate: its "
      "only figures are four projected years, starting at 1.27 for 2025.",
      "We divided the births by the female population in each age group and summed. Because there is no "
      "official annual rate to take, this is the only way to get a year-by-year figure for Argentina. Our "
      "2014 value of 2.35 lines up with the roughly 2.3 usually quoted for that year.",
      "Argentina's fall is the steepest in this whole dataset — from 2.38 in 2010 to 1.19 in 2024, halving in "
      "fourteen years. The population switches vintage at 2022, from the 2010 census basis to the 2022 one, "
      "and we have not smoothed that seam. Births are tabulated by year of registration, but the ministry "
      "defines the series to absorb only one year of lag and its own tables show 95% of a year's "
      "registrations occurred that year and almost all the rest the year before, so unlike Mexico there is no "
      "need to stop the series early. Mothers whose age was not stated — under 1% in recent years and "
      "falling — are spread across the bands. The top group is open-ended at 45 and over, treated here as "
      "45-49. The series starts in 2010 because that is where INDEC\'s population by age begins; births go "
      "back to 2005. INDEC\'s file paths are not linked from its own menus and a stale path returns an HTML "
      "error page with a 200 status, which is easy to mistake for data.",
      "https://datos.salud.gob.ar/dataset/nacidos-vivos-registrados-por-jurisdiccion-de-residencia-de-la-madre-republica-argentina"),
    C("Afghanistan", "CSO and Ministry of Public Health — Demographic and Health Survey 2015", afghanistan,
      "Afghanistan", "survey", False,
      "One figure: 5.3 children per woman for the three years to 2015, from the survey the Central "
      "Statistics Organization ran with the health ministry. The same table gives the age-specific rates "
      "behind it, and the report breaks the figure down by all 34 provinces.",
      "We read the national column of table 5.1. Summing its age-specific rates and multiplying by the band "
      "width gives 5.29, the published 5.3.",
      "Afghanistan has no vital registration usable for this, so the only figure is survey-based, and it is "
      "now over a decade old. The office's old domain, cso.gov.af, no longer resolves at all — its "
      "nameservers were deprovisioned but the delegation was never updated, so this report had to come from "
      "a web archive. Its successor, nsia.gov.af, is alive and actively maintained, with a certificate valid "
      "into late 2026, but serves a JavaScript application that a script cannot read, so we could not check "
      "whether anything newer has been published. The survey's own age structure is a weighted sample, not a "
      "population count, and no Afghan source gives female population by age group outside it, so there is "
      "nothing to recalculate from.",
      "https://nsia.gov.af/"),
    C("Algeria", "ONS — Démographie Algérienne", algeria, "Algeria", "complete", False,
      "The annual bulletin's main indicators table gives a fertility index for most years from 2002, "
      "alongside births and age-specific rates. Female population by five-year age group is in the same "
      "bulletin.",
      "We read the index row out of the 2019 edition, which carries the whole span in one table. Summing its "
      "age-specific rates and multiplying by the band width reproduces the published 3.0 for 2019 exactly, "
      "and multiplying those rates by the female population implies 1,032,000 births against the 1,034,000 "
      "the same bulletin reports.",
      "ONS stopped publishing the fertility index after 2019. The 2020-2023 edition dropped the whole "
      "fertility section — the term survives only in the glossary, with no number attached — so the series "
      "ends there, which is also the year ONS stopped inflating registered births for under-registration "
      "using coverage factors last estimated in 2002. The total is a registration count, but the age split "
      "is not: ONS says the rates for 2010-2019 were readjusted onto the age structure of births taken from "
      "its labour force surveys, and earlier editions say a single 2008 age curve was reused for several "
      "years. The population denominator is rolled forward from the 2008 census by natural increase alone, "
      "assuming no net migration. Some years were never published at all, so the line has gaps. The site "
      "serves its certificate without the intermediate, so downloads need certificate checking relaxed, and "
      "the year headers in several editions carry a digit-transposition typo for 2017.",
      "https://www.ons.dz/spip.php?rubrique182"),
    C("Iraq", "COSIT — household survey rounds", iraq, "Iraq", "survey", False,
      "The Annual Statistical Abstract has a table of the fertility rate for the years it was measured: 4.0 "
      "in 2004 from the living conditions survey, 4.3 in 2006 and 4.5 in 2011 from the two household cluster "
      "surveys, and 4.2 in 2007 from the socio-economic survey. Separately, COSIT publishes a projection "
      "series running 4.08 in 2015 down to 3.82 in 2020.",
      "We read the measured rounds from the abstract's table. We do not use the projection series, on the "
      "same reasoning as Nigeria: it is model output, not measurement.",
      "Iraq has no usable vital statistics for this. COSIT says so itself — the interior ministry holds a "
      "civil registry of more than 46 million people, but it is not yet organised for statistical use, and "
      "COSIT is only now building that pipeline. So there are no births by age of mother to recalculate from, "
      "and the series stops in 2011 because that is the last measured round published. Iraq ran its first "
      "full census since 1987 in November 2024, but only headline counts are out; the fertility results are "
      "not published yet, so this will be worth revisiting. The Kurdistan region's own statistics office "
      "blocks automated requests entirely, and the federal figures do not cover it — an adolescent fertility "
      "table on a COSIT platform reports zero for the three Kurdish governorates.",
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
      "The correction is a Brass P/F adjustment for births women forget or misdate, which UBOS applies because "
      "Uganda's civil registration cannot be used at all: the census found only 10% of children under five had "
      "a registered birth. That also means the census and the health survey are measuring by different methods "
      "— the survey's 5.2 for 2022 against the census's 4.5 for 2024 is partly a real fall and partly a change "
      "of instrument, and we have not tried to separate the two. UBOS's site serves a broken certificate "
      "chain, so downloads need certificate checking relaxed.",
      "https://www.ubos.org/nphc-2024-census-page/"),
    C("South Africa", "Stats SA — mid-year population estimates", south_africa_tfr, "South Africa",
      "projection", False,
      "The fertility rate Stats SA publishes is an input to its population projection, not a rate computed "
      "from births. Its own report says the series was \"derived following a detailed review of TFR estimates "
      "(1985-2024), (both published and unpublished), from various authors, methods and data sources\", "
      "informed by registered births and health-system records. Separately, the recorded live births report "
      "publishes registered births by age of mother as counts, and the mid-year estimates publish female "
      "population by age group.",
      "We read the modelled series from table 2 of the mid-year estimates. The age-band comparison uses the "
      "registered births instead, because those are the only counts the registry produces.",
      "The two disagree enormously. Dividing the registered births by the female population gives 1.53 for "
      "2024 against the published 2.41, and 1.88 for 2016 against 2.33. Some of that is timing — Stats SA "
      "says about 10% of births are registered a year or more late, and a year keeps filling up for years "
      "afterwards — but not all of it: it also estimates that even once late registrations are in, "
      "registration captures only about 90% of births. So South Africa's headline figure is deliberately set "
      "well above what its own registry shows, and is closer in kind to a UN estimate than to a count. "
      "Fertility results from the 2022 census have still not been released, which Stats SA notes as a reason "
      "the census could not feed the estimate. No mid-year estimates edition was published for 2023.",
      "https://www.statssa.gov.za/publications/P0302/P03022024.pdf"),
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
      "We read both census values out of the trends table. We also summed the national age-specific rates "
      "and got 3.24, a little below the published 3.4 — the rates are printed to three decimals and the "
      "national total absorbs county-level adjustments, so the two do not have to agree exactly.",
      "The census measures fertility by asking women about births in the three years before the count, not "
      "from registration. Rates for the North-Eastern counties are adjusted with a Gompertz model because "
      "reporting there was inconsistent. The whole report is scanned page images with no text layer, so "
      "everything had to be read by optical character recognition. Births by age of mother as counts are "
      "not in this volume.",
      "https://www.knbs.or.ke/wp-content/uploads/2024/05/2019-Kenya-population-and-Housing-Census-"
      "Analytical-Report-on-Fertility-and-Nuptiality-Vol.VI_.pdf"),
    C("Myanmar", "Department of Population — 2019 Inter-censal Survey", myanmar_tfr, "Myanmar",
      "survey", True,
      "Appendix table D-1 of the Union Report gives, for every five-year age group, the number of women "
      "enumerated and the number of live births in the twelve months before the survey, plus the "
      "department's own age-specific rate.",
      "We divided births by women in each age group and summed, times the band width. Every rate we get "
      "matches the printed one to the second decimal, and our total of 2.007 is the published 2.0.",
      "This is a sample survey, not registration: the counts are the survey's own enumerated households "
      "scaled up, and a twelve-month recall question misses births. The 2014 census gave 2.5 by the same "
      "kind of table. Nothing more recent is published — a 2024 census has been run but its fertility "
      "results are not out.",
      "https://www.dop.gov.mm/sites/dop.gov.mm/files/publication_docs/ics_appendixtables_en.pdf"),
]

PANELS = [(c["name"], c["src"], c["loader"], c["wpp_name"]) for c in COUNTRIES if c["loader"]]
DOCS = {c["name"]: (c["found"], c["method"], c["caveats"], c["url"]) for c in COUNTRIES}
