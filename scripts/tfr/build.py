"""Assemble the comparison page.

Parsing the national sources means reading ~110MB of spreadsheets, so every computed series
is cached to cache/ as CSV. Presentation-only changes then rebuild in well under a second.
Pass --fresh to re-parse the sources.
"""

import datetime
import json
import os
import sys
import warnings

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
warnings.filterwarnings("ignore")

from countries import COUNTRIES, DOCS, START, TIERS  # noqa: E402
from detail import compare  # noqa: E402
from render import dumbbell, line_chart  # noqa: E402
from sources import COLORS, un_wpp  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
CACHE = os.path.join(HERE, "cache")
OUT = os.path.join(HERE, "tfr_charts.html")
X1 = 2032


def cached(name, build):
    """Read cache/<name>.csv, or build it and write it. --fresh skips the read."""
    os.makedirs(CACHE, exist_ok=True)
    path = os.path.join(CACHE, f"{name}.csv")
    if "--fresh" not in sys.argv and os.path.exists(path):
        return pd.read_csv(path)
    df = build()
    df.to_csv(path, index=False)
    return df


def national(country, fn):
    """The country's own series, reindexed so missing years break the line."""
    df = cached(f"nso_{country.replace(' ', '_')}", fn)
    df = df[df.year >= START]
    span = range(int(df.year.min()), int(df.year.max()) + 1)
    return df.set_index("year").reindex(span).rename_axis("year").reset_index()


def wpp_series(model_country):
    def build():
        out = []
        for label, df, is_proj in un_wpp(model_country):
            out.append(df.assign(label=label, proj=is_proj))
        return pd.concat(out, ignore_index=True)

    return cached(f"wpp_{model_country.replace(' ', '_')}", build)


def at(wpp, year, label):
    """Value at `year` from one WPP variant, interpolating a coarse grid."""
    d = wpp[wpp.label == label].dropna(subset=["value"]).sort_values("year")
    if d.empty:
        return None
    if year in set(d.year):
        return float(d[d.year == year].value.iloc[0])
    if d.year.min() <= year <= d.year.max():
        return float(np.interp(year, d.year, d.value))
    return None


def main():
    rows, unplotted = [], []
    for c in COUNTRIES:
        if not c["loader"]:
            unplotted.append(dict(country=c["name"], tier=c["tier"], note=c["src"]))
            continue
        nso = national(c["name"], c["loader"])
        wpp = wpp_series(c["wpp_name"])
        d = nso.dropna(subset=["value"])
        year, nso_v = int(d.year.max()), float(d.iloc[-1].value)
        wpp_v = at(wpp, year, "medium") or at(wpp, year, "estimates")
        rows.append(dict(country=c["name"], src=c["src"], nso=nso, wpp=wpp, model_country=c["wpp_name"],
                         year=year, nso_v=nso_v, wpp_v=wpp_v, tier=c["tier"], recalc=c["recalculated"],
                         gap=(wpp_v - nso_v) if wpp_v else 0.0))

    rows.sort(key=lambda r: -r["gap"])

    sections, options = [], []
    for r in sorted(rows, key=lambda x: x["country"]):
        options.append(f'<option value="{r["country"]}">{r["country"]}</option>')
    for r in rows:
        tier_label, tier_color = TIERS[r["tier"]]
        badges = f'<span class="badge" style="background:{tier_color};color:#fff">{tier_label}</span>'
        if r["recalc"]:
            badges += '<span class="badge rc">Recalculated by us</span>'
        sections.append(
            f'<section data-country="{r["country"]}"><h2>{r["country"]}{badges}</h2>'
            f'<p class="src">{r["src"]}</p>{country_chart(r)}'
            f"{docs_block(r['country'])}{detail_block(r)}</section>"
        )

    legend = (f'<span class="k"><i style="color:{COLORS["nso"]}"></i>National statistical office</span>'
              f'<span class="k"><i style="color:{COLORS["UN WPP"]}"></i>UN WPP</span>')

    ov_rows = [dict(country=r["country"], year=r["year"], nso=round(r["nso_v"], 4),
                    wpp=round(r["wpp_v"], 4), tier=r["tier"], recalc=r["recalc"])
               for r in rows if r["wpp_v"]]

    html = open(os.path.join(HERE, "template.html")).read()
    for token, value in [
        ("{{START}}", str(START)),
        ("{{LEGEND}}", legend),
        ("{{OPTIONS}}", "".join(options)),
        ("{{ROWS}}", json.dumps(ov_rows)),
        ("{{TIERSJSON}}", json.dumps(TIERS)),
        ("{{UNPLOTTED}}", json.dumps(unplotted)),
        ("{{SECTIONS}}", "".join(sections)),
        ("{{STAMP}}", datetime.datetime.now().strftime("%Y-%m-%d %H:%M")),
    ]:
        html = html.replace(token, value)
    open(OUT, "w").write(html)

    print("  ranked:", ", ".join(f"{r['country']} {r['gap']:+.3f}" for r in rows))
    print(f"  unplotted: {len(unplotted)}")
    print(f"wrote {OUT}")


def docs_block(country):
    """Plain-language record of what the office publishes, what we did, and what to watch."""
    found, method, caveats, url = DOCS.get(country, ("", "", "", ""))
    link = f' <a href="{url}" target="_blank" rel="noopener">Source</a>' if url else ""
    return (f'<dl class="docs">'
            f"<dt>What the office publishes</dt><dd>{found}{link}</dd>"
            f"<dt>What we did</dt><dd>{method}</dd>"
            f"<dt>Watch out for</dt><dd>{caveats}</dd></dl>")


def _pairs(df):
    return [(int(y), None if pd.isna(v) else float(v)) for y, v in zip(df.year, df.value)]


def country_chart(r):
    series = []
    for label in ("high", "medium", "low"):
        d = r["wpp"][r["wpp"].label == label]
        d = d[(d.year >= START) & (d.year <= X1)]
        if not d.empty:
            series.append(("ln proj", COLORS["UN WPP"], _pairs(d)))
    est = r["wpp"][r["wpp"].label == "estimates"]
    est = est[(est.year >= START) & (est.year <= X1)]
    if not est.empty:
        series.append(("ln", COLORS["UN WPP"], _pairs(est)))
    series.append(("ln", "var(--nso)", _pairs(r["nso"][r["nso"].year <= X1])))
    return line_chart(series, START, X1, f'{r["country"]} fertility rate')


def detail_block(r):
    key = f"detail_{r['country'].replace(' ', '_')}_{r['year']}"
    path = os.path.join(CACHE, f"{key}.csv")
    if "--fresh" not in sys.argv and os.path.exists(path):
        df = pd.read_csv(path)
        bands = list(df.itertuples(index=False, name=None)) if not df.empty else None
    else:
        bands = compare(r["country"], r["model_country"], r["year"])
        os.makedirs(CACHE, exist_ok=True)
        pd.DataFrame(bands or [], columns=["band", "nso_births", "wpp_births", "nso_women", "wpp_women"]).to_csv(
            path, index=False
        )
    if not bands:
        return ('<div class="detail"><p class="na">This office publishes fertility rates only, not the births '
                "and female population behind them, so the two sources cannot be compared age band by age "
                "band.</p></div>")
    births = [(b, nb, wb) for b, nb, wb, _, _ in bands]
    women = [(b, nw, ww) for b, _, _, nw, ww in bands]
    return (f'<div class="detail">'
            f'<h3>Births by age of mother, {r["year"]}</h3>{dumbbell(births, "births")}'
            f'<h3>Women by age group, {r["year"]}</h3>{dumbbell(women, "women")}'
            f"</div>")


if __name__ == "__main__":
    main()
