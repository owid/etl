"""The country registry: how each national series is loaded, described and caveated."""

import os
import sys
import warnings

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
warnings.filterwarnings("ignore")

from colombia import dane_female_pop, dane_registered_tfr  # noqa: E402
from sources import egypt, england_wales, germany, japan, mexico, thailand, united_states  # noqa: E402

START = 2000


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
    return pd.read_csv(os.path.join(HERE, "data", "france_tfr.csv"))


# display name, short source label, loader, the name UN WPP uses, and the caveat shown
# under that country's charts
COUNTRIES = [
    (
        "Colombia",
        "DANE — Estadísticas Vitales",
        colombia,
        "Colombia",
        "Registered births by age of mother over DANE's own national population projections. "
        "The 2025 figure is provisional.",
    ),
    (
        "Brazil",
        "IBGE — Estatísticas do Registro Civil",
        brazil,
        "Brazil",
        "SIDRA tables 197 (2000–02) and 2612 (2003 onward), over IBGE population projections. "
        "The 2000–02 points come from the older table and understate the level, because birth "
        "registration coverage was still improving — the step up in 2003 is coverage, not fertility. "
        "The 2024 figure is provisional.",
    ),
    (
        "France",
        "INSEE — état civil",
        france,
        "France",
        "INSEE's own fertility rates by detailed age of mother, summed across ages. Excludes "
        "Mayotte before 2014 and includes it after. The 2025 figure is provisional.",
    ),
    (
        "Italy",
        "ISTAT — ANPR / stato civile",
        italy,
        "Italy",
        "ISTAT's own age-specific fertility rates, summed across single years of age.",
    ),
    (
        "England and Wales",
        "ONS — birth registrations, table 10",
        england_wales,
        "United Kingdom",
        "England and Wales only, which is about 89% of UK births — but the UN WPP lines are UK-wide, "
        "so the two are not exactly like for like. The 2025 figure is provisional.",
    ),
    (
        "United States",
        "CDC / NCHS — natality via data.cdc.gov",
        united_states,
        "United States",
        "Age-specific birth rates from the NCHS Data Query System, which only publishes 2016 onward. "
        "A longer series would need CDC WONDER.",
    ),
    (
        "Japan",
        "MHLW — Vital Statistics via e-Stat",
        japan,
        "Japan",
        "MHLW's own published total fertility rate. Five-yearly before 2000, annual after.",
    ),
    (
        "Germany",
        "Destatis — Geburtenstatistik (GENESIS 12612-0008)",
        germany,
        "Germany",
        "Live births per 1,000 women by single year of age, summed across ages 15–49. Rebased on the "
        "2022 census from 2012 onward, so figures differ slightly from earlier Destatis releases. "
        "The 2025 figure is provisional.",
    ),
    (
        "Thailand",
        "NSO — Statistical Yearbook tables 1.10 and 1.4",
        thailand,
        "Thailand",
        "Registered births by age group of mother over the registered female population. The yearbook "
        "prints a rolling three-year window per table, so only the years where both tables overlap "
        "can be computed — earlier editions would extend this back.",
    ),
    (
        "Egypt",
        "CAPMAS — Annual Bulletin of Births and Deaths, table 13",
        egypt,
        "Egypt",
        "CAPMAS's own age-specific fertility rates. One bulletin per year, so the series only covers "
        "the editions to hand.",
    ),
    (
        "Mexico",
        "INEGI — registered births, over CONAPO population",
        mexico,
        "Mexico",
        "Births by year of occurrence, summed across every registration year, over CONAPO mid-year "
        "female population. Stops at 2022 because later years of occurrence are still filling up with "
        "late registrations.",
    ),
]

# kept for callers that only need the first four fields
PANELS = [c[:4] for c in COUNTRIES]
NOTES = {c[0]: c[4] for c in COUNTRIES}
