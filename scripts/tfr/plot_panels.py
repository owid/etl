"""2x2 panel: national vital statistics vs UN WPP, 2000 onward."""

import os
import sys
import warnings

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.ticker import MultipleLocator

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
warnings.filterwarnings("ignore")

from build_colombia_tfr import dane_female_pop  # noqa: E402
from plot_colombia import dane_registered_tfr  # noqa: E402

WPP = "/Users/edouard/dev/owid/etl/data/garden/un/2024-07-12/un_wpp"
START = 2000
NSO_C, WPP_C, PROJ_C = "#c94a3b", "#3b82c4", "#9ec4e6"


def wpp(country, variant="estimates"):
    from owid.catalog import Dataset

    tb = Dataset(WPP)["fertility_rate"].reset_index()
    s = tb[(tb.country == country) & (tb.age == "all") & (tb.sex == "all") & (tb.variant == variant)]
    return pd.DataFrame({"year": s.year.astype(int), "value": s.fertility_rate.astype(float)}).sort_values("year")


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


from sources import egypt, england_wales, germany, japan, mexico, thailand, united_states  # noqa: E402

# (display name, national source, loader, name used by the modelling groups)
PANELS = [
    ("Colombia", "DANE — Estadísticas Vitales", colombia, "Colombia"),
    ("Brazil", "IBGE — Registro Civil (SIDRA)", brazil, "Brazil"),
    ("France", "INSEE — état civil", france, "France"),
    ("Italy", "ISTAT — ANPR / stato civile", italy, "Italy"),
    (
        "England and Wales",
        "ONS — birth registrations, Table 10 (modelled lines are UK-wide)",
        england_wales,
        "United Kingdom",
    ),
    ("United States", "CDC / NCHS — natality via data.cdc.gov", united_states, "United States"),
    ("Japan", "MHLW — Vital Statistics via e-Stat", japan, "Japan"),
    ("Germany", "Destatis — Geburtenstatistik (GENESIS 12612-0008)", germany, "Germany"),
    ("Thailand", "NSO — Statistical Yearbook tables 1.10 and 1.4", thailand, "Thailand"),
    ("Egypt", "CAPMAS — Annual Bulletin of Births and Deaths, table 13", egypt, "Egypt"),
    ("Mexico", "INEGI — registered births by year of occurrence, over CONAPO population", mexico, "Mexico"),
]

if __name__ == "__main__":
    plt.rcParams.update({"font.family": "sans-serif", "font.sans-serif": ["Helvetica Neue", "Arial", "DejaVu Sans"]})
    fig, axes = plt.subplots(2, 2, figsize=(13, 8.6), dpi=200)

    rows = []
    for ax, (country, src, fn) in zip(axes.flat, PANELS):
        nso = fn()
        nso = nso[nso.year >= START]
        est = wpp(country)
        est = est[est.year >= START]
        end = int(nso.dropna().year.max())

        ax.plot(est.year, est.value, color=WPP_C, lw=2.3, marker="o", ms=3.0, zorder=3)
        anchor = est.iloc[-1]
        for var in ("high", "medium", "low"):
            p = wpp(country, var)
            p = p[p.year <= end]
            if not len(p):
                continue
            ax.plot(
                [anchor.year] + p.year.tolist(),
                [anchor.value] + p.value.tolist(),
                color=PROJ_C,
                lw=1.7,
                ls=(0, (4, 2)),
                marker="o",
                ms=2.6,
                zorder=2,
            )
        ax.plot(nso.year, nso.value, color=NSO_C, lw=2.6, marker="o", ms=3.0, zorder=5)

        last = nso.dropna().iloc[-1]
        gap = last.value - float(est[est.year == est.year.max()].value.iloc[0])
        rows.append((country, int(last.year), last.value, float(est.iloc[-1].value), gap))

        ax.text(0, 1.155, country, transform=ax.transAxes, fontsize=13.5, fontweight="bold", color="#1d1d1b")
        ax.text(0, 1.055, src, transform=ax.transAxes, fontsize=8.8, color="#8a8a8a")
        ax.text(
            0.985,
            0.93,
            f"{last.value:.2f}\nin {int(last.year)}",
            transform=ax.transAxes,
            fontsize=10.5,
            fontweight="bold",
            color=NSO_C,
            ha="right",
            va="top",
            linespacing=1.25,
        )

        ax.set_xlim(START - 0.5, 2027)
        ax.set_ylim(0, 2.6)
        ax.yaxis.set_major_locator(MultipleLocator(0.5))
        ax.grid(axis="y", color="#e9e9e9", lw=0.9)
        ax.set_axisbelow(True)
        for s in ("top", "right", "left"):
            ax.spines[s].set_visible(False)
        ax.spines["bottom"].set_color("#cfcfcf")
        ax.tick_params(length=0, labelsize=9.5, colors="#666")

    fig.text(
        0.038,
        0.965,
        "National vital statistics vs UN WPP fertility estimates",
        fontsize=17,
        fontweight="bold",
        color="#1d1d1b",
        va="top",
    )
    fig.text(
        0.038,
        0.928,
        "Total fertility rate, children per woman. Red: computed from each statistical office's own registered "
        "births by age of mother.\nBlue: UN World Population Prospects 2024 — solid where estimated, dashed for "
        "the high / medium / low projections.",
        fontsize=9.6,
        color="#666",
        va="top",
    )

    fig.text(
        0.038,
        0.017,
        "Sources: DANE Estadísticas Vitales; IBGE Estatísticas do Registro Civil (SIDRA tables 197 and 2612) with "
        "IBGE population projections; INSEE fertility rates by detailed age of mother; ISTAT age-specific "
        "fertility rates.\nUN World Population Prospects 2024. Colombia and Brazil computed as the sum of "
        "age-specific rates using each country's own female population; France and Italy use the rates their "
        "statistical office publishes.",
        fontsize=7.6,
        color="#999",
        va="bottom",
    )

    fig.subplots_adjust(left=0.05, right=0.985, top=0.83, bottom=0.115, hspace=0.44, wspace=0.13)
    fig.savefig("tfr_panels.png", facecolor="white")

    print(f"{'country':<10}{'yr':>6}{'NSO':>8}{'WPP':>8}{'gap':>8}")
    for c, y, n, w, g in rows:
        print(f"{c:<10}{y:>6}{n:>8.2f}{w:>8.2f}{g:>+8.2f}")
    print("\nwrote tfr_panels.png")
