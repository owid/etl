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

from china import china_tfr  # noqa: E402
from colombia import dane_female_pop, dane_registered_tfr  # noqa: E402
from france import france_tfr  # noqa: E402
from india import india  # noqa: E402
from philippines import philippines  # noqa: E402
from korea import korea_tfr  # noqa: E402
from myanmar import myanmar_tfr  # noqa: E402
from published import (  # noqa: E402
    bangladesh,
    drc,
    ethiopia,
    indonesia,
    kenya,
    nigeria,
    pakistan,
    russia,
    tanzania,
    turkey,
    vietnam,
)
from sources import egypt, england_wales, germany, japan, mexico, thailand, united_states  # noqa: E402
from spain import spain_tfr  # noqa: E402

START = 2000

# tier key -> (label, color)
TIERS = {
    "complete": ("Complete registration", "#1d7a4c"),
    "incomplete": ("Incomplete registration", "#a8690a"),
    "sample": ("Sample registration", "#7b5ea7"),
    "survey": ("Survey or census", "#b0632c"),
    "projection": ("Projection only", "#5a7a8c"),
    "none": ("No official figure", "#8a8a8a"),
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
    C("England and Wales", "ONS — birth registrations, table 10", england_wales, "United Kingdom",
      "complete", False,
      "ONS table 10 gives live births and age-specific fertility rates by age group of mother, back to "
      "1938.",
      "We summed the age-specific rates and multiplied by the band width. Because the table also gives "
      "births, we can divide one by the other to recover the female population ONS used, which is what "
      "the age-band comparison uses.",
      "This is England and Wales only, about 89% of UK births, but the UN figures are UK-wide, so the "
      "two are not exactly like for like. Scotland and Northern Ireland publish separately and would "
      "need stitching in. The 2025 figure is provisional.",
      "https://www.ons.gov.uk/peoplepopulationandcommunity/birthsdeathsandmarriages/livebirths"),
    C("Germany", "Destatis — Geburtenstatistik (GENESIS 12612-0008)", germany, "Germany", "complete", False,
      "Destatis table 12612-0008 gives live births per 1,000 women for every single year of age 15-49, "
      "from 1972 to 2025. Table 12612-0005 gives the births themselves by age of mother and birth "
      "order, from 2009.",
      "We summed the rates across ages. Because they are single-year rates there is no band width to "
      "multiply by. Dividing births by the rate recovers the female population Destatis used, which "
      "is what the age-band comparison shows.",
      "Rebased on the 2022 census from 2012 onward, so numbers differ slightly from Destatis releases "
      "published before the rebasing — our 2023 comes out at 1.385 where the original release said 1.35. "
      "The 2025 figure is provisional. The age-band comparison only starts in 2009, because that is where "
      "the births table begins. Neither file could be pulled through the GENESIS API — the access token we "
      "have authenticates as the guest account, which cannot download tables — so both were exported by "
      "hand from the web interface.",
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
    C("France", "INSEE — état civil", france, "France", "complete", False,
      "INSEE publishes fertility rates by single year of age of mother, and separately births by single "
      "year of age, both from the civil register.",
      "We summed the rates across ages. Both files use age reached during the year, so dividing births "
      "by the rate recovers INSEE's own female population, which is what the age-band comparison uses.",
      "Excludes Mayotte before 2014 and includes it after, a small discontinuity we have not corrected. "
      "The 2025 figure is provisional.",
      "https://www.insee.fr/fr/statistiques/9000195"),
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
    C("Vietnam", "National Statistics Office — PxWeb table V02.15", vietnam, "Vietnam", "survey", False,
      "The statistics office publishes a total fertility rate annually from 2001, through a PxWeb database "
      "with a working JSON API.",
      "We pulled the whole-country column straight from the API. No arithmetic of our own.",
      "This is not a count of registered births. The office estimates the rate from an annual household "
      "sample survey and then adjusts it upward with the Trussell P/F technique, because women under-report "
      "births in the previous twelve months. Some years come from a different, larger survey instead of the "
      "annual one — 2024 is one of those — so the series is not from a single instrument. Vietnam does have "
      "civil registration, but it is not what the published rate is built on.",
      "https://pxweb.nso.gov.vn/"),
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
    C("Turkey", "TurkStat — Population Statistics Portal", turkey, "Turkey", "complete", False,
      "TurkStat publishes a total fertility rate annually from 2009, and female population by five-year "
      "age group from the address-based population register.",
      "We read the national row of TurkStat's own fertility-rate export. Our 2024 value of 1.48 matches "
      "the figure in its press bulletin.",
      "TurkStat's current data portal is a JavaScript application whose download links are single-use "
      "tokens, so it cannot be read by a script. An older server-rendered portal still answers plain "
      "requests, and that is what this uses — asking it for the national series rather than the province "
      "breakdown takes an undocumented empty parameter. Age-specific rates exist in the bulletins but "
      "only inside the JavaScript portal, so there is no age-band comparison for Turkey. The series starts "
      "in 2009 on this portal; TurkStat's own bulletins go back to 2001.",
      "https://nip.tuik.gov.tr/"),
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
