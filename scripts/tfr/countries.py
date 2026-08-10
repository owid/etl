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
]

PANELS = [(c["name"], c["src"], c["loader"], c["wpp_name"]) for c in COUNTRIES if c["loader"]]
DOCS = {c["name"]: (c["found"], c["method"], c["caveats"], c["url"]) for c in COUNTRIES}
