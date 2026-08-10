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

from colombia import dane_female_pop, dane_registered_tfr  # noqa: E402
from france import france_tfr  # noqa: E402
from india import india  # noqa: E402
from philippines import philippines  # noqa: E402
from myanmar import myanmar_tfr  # noqa: E402
from published import (  # noqa: E402
    bangladesh,
    china,
    drc,
    ethiopia,
    indonesia,
    nigeria,
    pakistan,
    russia,
    tanzania,
    turkey,
    vietnam,
)
from sources import egypt, england_wales, germany, japan, mexico, thailand, united_states  # noqa: E402

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
      "each age group, and CAPMAS's own total fertility rate.",
      "We summed the age-specific rates and multiplied by the five-year band width. This reproduces "
      "CAPMAS's own published total exactly, so we treat the figure as theirs rather than ours.",
      "One bulletin per year, so the series only covers the editions we have (2021-24). CAPMAS labels "
      "one band 40-45 where it means 40-44; we treat it as a normal five-year band, which is what "
      "reproduces their published total.",
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
      "table 0003411607 gives births by the same age groups.",
      "We took MHLW's published total fertility rate directly. For the age-band comparison we divided "
      "births by each band's rate to recover MHLW's own female population.",
      "Five-yearly before 2000 and annual after, so the early part of the line is coarse. e-Stat "
      "publishes each band as its contribution to the total rather than as a rate per 1,000 women — the "
      "seven bands sum exactly to the published total.",
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
      "The NCHS Data Query System publishes age-specific birth rates per 1,000 women by maternal age "
      "group.",
      "We summed the rates and multiplied by the band width. Our figures match NCHS's published totals "
      "to two decimals — 1.621 against 1.617 for 2023.",
      "Only 2016 onward, because that is all this dataset covers. A longer series would mean CDC WONDER, "
      "whose API needs an XML request. The dataset splits 15-19 into 15-17 and 18-19 as well as the "
      "whole band, which would double-count if not filtered.",
      "https://data.cdc.gov/"),
    C("Russia", "Rosstat — Demographic Yearbook of Russia", russia, "Russia", "complete", False,
      "Rosstat publishes a total fertility rate, age-specific rates, births by age of mother and female "
      "population by age, all in the Demographic Yearbook, as a PDF with an Excel appendix.",
      "We read the national row of the Excel appendix's fertility-rate sheet. The number is Rosstat's own.",
      "The Excel appendix carries only the two most recent years, so this series is short; the yearbook PDF "
      "has more. Russia's site serves a broken certificate chain, so downloads need certificate checking "
      "relaxed — an earlier attempt wrongly concluded the site was unreachable. The 2022 figure excludes "
      "the four annexed Ukrainian regions but includes Crimea, and 2022 was rebased on the 2020 census "
      "while earlier years were not, so there is a break in the series.",
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
      "The annual SVRS report publishes a total fertility rate with confidence intervals, plus age-specific "
      "rates and the sample's age structure.",
      "We took the two years the 2023 report states in its own text: 2.17 in 2023, down from 2.20 in 2022.",
      "The SVRS is a sample of about 2,000 areas where resident registrars record births monthly — a "
      "continuous sample system, not full civil registration. A different BBS survey gives 2.10 for the same "
      "year, so BBS itself publishes two disagreeing figures. Everything is PDF only, and the report's "
      "longer trend is a chart with no numbers behind it, so only two years could be read.",
      "https://bbs.gov.bd/"),
    C("Indonesia", "BPS — Long Form of the 2020 census", indonesia, "Indonesia", "survey", False,
      "BPS publishes a total fertility rate for census and inter-censal survey rounds only. The Long Form of "
      "the 2020 census, fielded in 2022, gives 2.42.",
      "We took the figure from BPS's own press release. No arithmetic of our own.",
      "BPS says plainly that Indonesian civil registration coverage is still incomplete and cannot be used "
      "for this, so the rate comes from retrospective questions about children ever born. BPS's main website "
      "blocks automated access, so a newer figure from the 2025 survey — reported as 2.13 — could not be "
      "confirmed and is left out.",
      "https://sensus.bps.go.id/"),
    C("Pakistan", "PBS — Pakistan Demographic Survey 2020", pakistan, "Pakistan", "survey", False,
      "The Bureau of Statistics publishes a total fertility rate of 3.7 from its Demographic Survey 2020, "
      "with age-specific rates alongside. The health survey run by the National Institute of Population "
      "Studies separately gives 3.6 for 2017-18.",
      "We took the Bureau's own 3.7 and placed it at 2020, the survey year. Its reference period is 2018-2020.",
      "Pakistan has no usable civil registration for this. The Bureau's own report explains it revived the "
      "survey after a thirteen-year gap because the national database authority told them vital-event records "
      "were not good enough. So this is one survey estimate, not a series.",
      "https://www.pbs.gov.pk/pds/"),
    C("China", "NBS — Seventh National Population Census", china, "China", "survey", False,
      "The statistics bureau states a total fertility rate only around census years. Its director gave 1.3 "
      "for 2020 at the census press conference. The annual communique reports births and a crude birth rate "
      "but no fertility rate.",
      "We took the 1.3 figure as stated. No arithmetic of our own.",
      "China has no birth-registration-based vital statistics. Figures come from the ten-yearly census and, "
      "between censuses, a household sample survey covering about one person in a thousand. Detailed "
      "age-specific tables exist but are published as scanned images rather than text or spreadsheets. Lower "
      "figures circulating for recent years are computed by outside analysts, not published by the bureau.",
      "https://www.stats.gov.cn/"),
    C("Nigeria", "NBS — Demographic Statistics Bulletin, Calculated TFR", nigeria, "Nigeria",
      "projection", False,
      "The Bureau of Statistics publishes a row called Calculated TFR running from 5.50 in 2013 to 5.14 in "
      "2022, sourced to the National Population Commission.",
      "We read that row out of the 2022 bulletin. No arithmetic of our own.",
      "This is not a measured rate. The Population Commission produces it by drawing a straight line between "
      "the 2008, 2013 and 2018 household survey rounds, which is why it falls by almost exactly the same "
      "amount every year. The most recent actual measurement is the 2023-24 household survey, which found "
      "4.8 — well below the projected path. Birth registration is far too incomplete to use, and the female "
      "population denominators are projections from the 2006 census.",
      "https://www.nigerianstat.gov.ng/elibrary/read/1241422"),
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
