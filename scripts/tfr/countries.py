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


def C(name, src, loader, wpp_name, tier, recalculated, note):
    return dict(name=name, src=src, loader=loader, wpp_name=wpp_name, tier=tier,
                recalculated=recalculated, note=note)


COUNTRIES = [
    C("Colombia", "DANE — Estadísticas Vitales", colombia, "Colombia", "complete", True,
      "Registered births by age of mother over DANE's own national population projections. DANE also "
      "publishes a projection-based figure of about 1.68 for 2024, far above what its registry shows. "
      "The 2025 figure is provisional."),
    C("Mexico", "INEGI — registered births, over CONAPO population", mexico, "Mexico", "complete", True,
      "Births by year of occurrence, summed across every registration year, over CONAPO mid-year female "
      "population. CONAPO's own model-based figure is well above this. Stops at 2022 because later "
      "years of occurrence are still filling up with late registrations."),
    C("Philippines", "PSA — OpenSTAT registered live births", philippines, "Philippines", "incomplete", True,
      "Registered births by age group of mother over PSA's 2020-census-based female population "
      "projection. Coverage is around 90% and births are tabulated by year of registration, so this "
      "understates the true rate — PSA's own published figure comes from the NDHS survey. OpenSTAT "
      "publishes one table per year, hence the short series."),
    C("Egypt", "CAPMAS — Annual Bulletin of Births and Deaths, table 13", egypt, "Egypt", "complete", False,
      "CAPMAS's own age-specific fertility rates, summed. One bulletin per year, so the series only "
      "covers the editions to hand."),
    C("Brazil", "IBGE — Estatísticas do Registro Civil", brazil, "Brazil", "complete", True,
      "SIDRA tables 197 (2000–02) and 2612 (2003 onward), over IBGE population projections. The "
      "2000–02 points come from the older table and understate the level, because birth registration "
      "coverage was still improving — the step up in 2003 is coverage, not fertility. The 2024 figure "
      "is provisional."),
    C("England and Wales", "ONS — birth registrations, table 10", england_wales, "United Kingdom",
      "complete", False,
      "England and Wales only, which is about 89% of UK births — but the UN WPP lines are UK-wide, so "
      "the two are not exactly like for like. The 2025 figure is provisional."),
    C("Germany", "Destatis — Geburtenstatistik (GENESIS 12612-0008)", germany, "Germany", "complete", False,
      "Live births per 1,000 women by single year of age, summed across ages 15–49. Rebased on the 2022 "
      "census from 2012 onward, so figures differ slightly from earlier Destatis releases. The 2025 "
      "figure is provisional."),
    C("Thailand", "NSO — Statistical Yearbook tables 1.10 and 1.4", thailand, "Thailand", "complete", True,
      "Registered births by age group of mother over the registered female population. The yearbook "
      "prints a rolling three-year window per table, so only the years where both tables overlap can be "
      "computed — earlier editions would extend this back."),
    C("France", "INSEE — état civil", france, "France", "complete", False,
      "INSEE's own fertility rates by detailed age of mother, summed across ages. Excludes Mayotte "
      "before 2014 and includes it after. The 2025 figure is provisional."),
    C("Japan", "MHLW — Vital Statistics via e-Stat", japan, "Japan", "complete", False,
      "MHLW's own published total fertility rate. Five-yearly before 2000, annual after."),
    C("Italy", "ISTAT — ANPR / stato civile", italy, "Italy", "complete", False,
      "ISTAT's own age-specific fertility rates, summed across single years of age."),
    C("United States", "CDC / NCHS — natality via data.cdc.gov", united_states, "United States",
      "complete", False,
      "Age-specific birth rates from the NCHS Data Query System, which only publishes 2016 onward. A "
      "longer series would need CDC WONDER."),
]

PANELS = [(c["name"], c["src"], c["loader"], c["wpp_name"]) for c in COUNTRIES if c["loader"]]
NOTES = {c["name"]: c["note"] for c in COUNTRIES}
