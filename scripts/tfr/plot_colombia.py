"""Colombia TFR: national vital statistics vs UN WPP vs UN Demographic Yearbook."""

import glob
import os
import re
import warnings

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.ticker import MultipleLocator

from build_colombia_tfr import dane_female_pop, dane_tgf, dyb_tfr, wpp_tfr, wpp_variant

warnings.filterwarnings("ignore")

DATA = os.path.join(os.path.dirname(__file__), "data")

AGE_ROW = re.compile(r"De\s+(\d+)\s*-\s*(\d+)", re.I)


def dane_births_by_age():
    """{year: {(lo, hi): births}} from DANE's Cuadro 1/9, with unknown maternal age
    redistributed proportionally across the known groups."""
    out = {}
    for path in sorted(glob.glob(os.path.join(DATA, "dane_nac/nac_*.xls*"))):
        year = int(re.search(r"nac_(\d{4})", path).group(1))
        try:
            xl = pd.ExcelFile(path)
        except Exception:
            continue  # encrypted / not a workbook
        sheet = next((s for s in xl.sheet_names if "cuadro" in s.lower() and "1" in s), xl.sheet_names[0])
        df = pd.read_excel(path, sheet_name=sheet, header=None)

        groups, unknown = {}, 0.0
        # layout A (2008+): age groups down the first column, national total in column 1
        for _, r in df.iterrows():
            label = str(r.iloc[0]).strip()
            m = AGE_ROW.match(label)
            val = pd.to_numeric(r.iloc[1], errors="coerce")
            if m and pd.notna(val):
                groups[(int(m.group(1)), int(m.group(2)))] = float(val)
            elif label.lower().startswith("sin informaci") and pd.notna(val):
                unknown = float(val)

        # layout B (pre-2008): age groups across columns, departments down rows
        if not groups:
            hdr = next((i for i, r in df.iterrows() if sum(bool(AGE_ROW.match(str(v).strip())) for v in r) >= 5), None)
            if hdr is None:
                continue
            cols = {}
            for j, v in enumerate(df.iloc[hdr]):
                m = AGE_ROW.match(str(v).strip())
                if m:
                    cols[j] = (int(m.group(1)), int(m.group(2)))
                elif str(v).strip().lower().startswith("sin informaci"):
                    cols[j] = "unknown"
            trow = next(
                (i for i in range(hdr, len(df)) if str(df.iloc[i, 0]).strip().lower() in {"total", "total nacional"}),
                None,
            )
            if trow is None:
                continue
            for j, key in cols.items():
                val = pd.to_numeric(df.iloc[trow, j], errors="coerce")
                if pd.isna(val):
                    continue
                if key == "unknown":
                    unknown = float(val)
                else:
                    groups[key] = float(val)
        if not groups:
            continue

        counted = sum(groups.values())
        scale = (counted + unknown) / counted if unknown else 1.0
        out[year] = {band: b * scale for band, b in groups.items()}
    return out


def dane_registered_tfr(pop):
    """TFR from DANE's own Cuadro 1 (births by age group of mother), per year."""
    rows = []
    for year, groups in sorted(dane_births_by_age().items()):
        tfr = 0.0
        for (lo, hi), births in groups.items():
            ages = [a for a in range(lo, hi + 1) if a in pop.columns]
            denom = pop.loc[year, ages].sum()
            if denom > 0:
                tfr += births / denom * len(ages)
        rows.append({"year": year, "value": tfr, "births": sum(groups.values())})
    return pd.DataFrame(rows).sort_values("year").reset_index(drop=True)


if __name__ == "__main__":
    pop = dane_female_pop()
    dane_proj = dane_tgf()
    dane_proj = dane_proj[dane_proj.year <= 2025]
    wpp = wpp_tfr()
    dyb = dyb_tfr(pop)[["year", "value"]]
    reg = dane_registered_tfr(pop)

    # one national vital-statistics series: DANE's own tables, DYB filling the years DANE encrypts
    vital = pd.concat([reg[["year", "value"]], dyb[~dyb.year.isin(reg.year)]], ignore_index=True)
    vital = vital.sort_values("year").reset_index(drop=True)
    # reindex to a continuous span so missing years break the line instead of interpolating
    span = range(int(vital.year.min()), int(vital.year.max()) + 1)
    vital = vital.set_index("year").reindex(span).rename_axis("year").reset_index()

    print("DANE registered-births TFR:")
    print(reg.to_string(index=False))
    print("\nCombined vital-statistics series:")
    print(vital.to_string(index=False))

    pd.concat(
        [
            wpp.assign(source="UN WPP 2024"),
            vital.assign(source="DANE vital statistics (registered births)"),
        ]
    ).to_csv("colombia_tfr.csv", index=False)

    # ---------------------------------------------------------------- plot
    plt.rcParams.update({"font.family": "sans-serif", "font.sans-serif": ["Helvetica Neue", "Arial", "DejaVu Sans"]})
    fig, ax = plt.subplots(figsize=(11, 6.6), dpi=200)

    ax.plot(
        wpp.year,
        wpp.value,
        color="#3b82c4",
        lw=2.6,
        marker="o",
        ms=3.4,
        label="UN WPP (2024 revision) — estimates",
        zorder=3,
    )

    # projection variants, dashed, carried to the last DANE year
    anchor = wpp.iloc[-1]
    end = int(vital.dropna().year.max())
    for variant, shade in [("high", "#8fb8dd"), ("medium", "#3b82c4"), ("low", "#8fb8dd")]:
        p = wpp_variant(variant)
        p = p[p.year <= end]
        yr = [anchor.year] + p.year.tolist()
        vl = [anchor.value] + p.value.tolist()
        ax.plot(yr, vl, color=shade, lw=2.0, ls=(0, (4, 2)), marker="o", ms=3.4, zorder=2)
        ax.text(yr[-1] + 0.25, vl[-1], f" {variant}", color=shade, fontsize=8.5, va="center")
    ax.plot(
        vital.year,
        vital.value,
        color="#c94a3b",
        lw=2.8,
        label="DANE — vital statistics (registered births)",
        marker="o",
        ms=3.4,
        zorder=5,
    )

    fig.text(
        0.055,
        0.945,
        "Colombia's fertility rate: what the country records vs what the UN estimates",
        fontsize=15.5,
        fontweight="bold",
        color="#1d1d1b",
        va="top",
    )

    ax.set_xlim(1997, 2028)
    ax.set_ylim(0, 3.4)
    ax.yaxis.set_major_locator(MultipleLocator(0.5))
    ax.grid(axis="y", color="#e6e6e6", lw=0.9)
    ax.set_axisbelow(True)
    for s in ("top", "right", "left"):
        ax.spines[s].set_visible(False)
    ax.spines["bottom"].set_color("#c9c9c9")
    ax.tick_params(length=0, labelsize=10, colors="#555")
    ax.legend(frameon=False, fontsize=9.8, loc="upper right", handlelength=2.4, labelspacing=0.55)

    fig.text(
        0.008,
        0.015,
        "Sources: DANE Estadísticas Vitales (Cuadro 1, births by age of mother); UN World Population "
        "Prospects 2024.\nFertility rates computed as the sum of "
        "five-year age-specific rates, using DANE female population as the denominator.",
        fontsize=7.6,
        color="#8a8a8a",
        va="bottom",
    )

    fig.subplots_adjust(left=0.06, right=0.98, top=0.855, bottom=0.155)
    fig.savefig("colombia_tfr.png", facecolor="white")
    print("\nwrote colombia_tfr.png")
