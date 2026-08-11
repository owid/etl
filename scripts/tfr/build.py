"""Assemble the comparison page.

Parsing the national sources means reading ~110MB of spreadsheets, so every computed series
is cached to cache/ as CSV. Presentation-only changes then rebuild in well under a second.
Pass --fresh to re-parse the sources.
"""

import datetime
import hashlib
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
from track import refresh  # noqa: E402
from worldmap import paths as world_paths  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
CACHE = os.path.join(HERE, "cache")
OUT = os.path.join(HERE, "tfr_charts.html")
X1 = 2032


FINGERPRINTS = os.path.join(CACHE, "fingerprints.json")


def _fingerprints():
    if os.path.exists(FINGERPRINTS):
        return json.load(open(FINGERPRINTS))
    return {}


def _fingerprint_of(*fns):
    """A hash of every module the given functions live in, or None if none of them has a file.

    Modules rather than functions, because a loader's behaviour depends on the constants around it.
    Taiwan's series was cut to 2024 by setting LAST_COMPLETE, which does not appear in the text of
    taiwan_tfr() at all — a function-level hash would have missed it, exactly as no hash at all did.
    Countries whose loaders share published.py all rebuild together when any of them changes, which
    costs nothing: those loaders are hand-entered figures, not parsing.
    """
    h, seen = hashlib.sha256(), set()
    for fn in fns:
        mod = sys.modules.get(getattr(fn, "__module__", None))
        path = getattr(mod, "__file__", None)
        if not path or path in seen or not os.path.exists(path):
            continue
        seen.add(path)
        h.update(open(path, "rb").read())
    return h.hexdigest()[:16] if seen else None


def cached(name, build, fingerprint=None):
    """Read cache/<name>.csv, or build it and write it.

    Rebuilds when the code that produced the cache has changed since it was written. Without that,
    editing a loader left the old series on the page and said nothing: Taiwan's page described a line
    stopping at 2024 while the chart, the map and the detail panels all still carried the 2025 point
    the edit had removed. --fresh forces every series to rebuild regardless.
    """
    os.makedirs(CACHE, exist_ok=True)
    path = os.path.join(CACHE, f"{name}.csv")
    stored = _fingerprints().get(name)
    fresh_code = fingerprint is not None and stored != fingerprint
    if "--fresh" not in sys.argv and os.path.exists(path) and not fresh_code:
        return pd.read_csv(path)
    if fresh_code and os.path.exists(path):
        print(f"  rebuilding {name}: its code changed since the cache was written")
    df = build()
    df.to_csv(path, index=False)
    if fingerprint is not None:
        marks = _fingerprints()
        marks[name] = fingerprint
        json.dump(marks, open(FINGERPRINTS, "w"), indent=0, sort_keys=True)
    return df


def national(country, fn):
    """The country's own series, reindexed so missing years break the line."""
    df = cached(f"nso_{country.replace(' ', '_')}", fn, _fingerprint_of(fn))
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


# shorter labels for the overview table and the map tooltip, where the full name crowds the row.
# The country pages and the picker keep the full name.
SHORT = {
    "Democratic Republic of Congo": "DR Congo",
}


def iso_codes():
    """{our country name: ISO alpha-3}, so the map can be keyed the same way as the geometry."""
    from owid.catalog import Dataset

    reg = Dataset("/Users/edouard/dev/owid/etl/data/garden/regions/2023-01-01/regions")["regions"]
    reg = reg.reset_index()
    by_name = {n: c for n, c in zip(reg.name, reg.iso_alpha3) if isinstance(c, str)}
    out = {}
    for c in COUNTRIES:
        if c["name"] not in out:
            code = by_name.get(c["name"]) or by_name.get(c["wpp_name"])
            if code:
                out[c["name"]] = code
    return out


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
                         loader=c["loader"], gap=(wpp_v - nso_v) if wpp_v else 0.0))

    rows.sort(key=lambda r: -r["gap"])

    sections, options = [], []
    for r in sorted(rows, key=lambda x: x["country"]):
        options.append(f'<option value="{r["country"]}">{r["country"]}</option>')
    for r in rows:
        tier_label, tier_color = TIERS[r["tier"]]
        badges = f'<span class="badge" style="background:{tier_color};color:#fff">{tier_label}</span>'
        if r["recalc"]:
            badges += ('<span class="badge rc">Recalculated from births &amp; women</span>'
                       if r["recalc"] else '')
        sections.append(
            f'<section data-country="{r["country"]}"><h2>{r["country"]}{badges}</h2>'
            f'<p class="src">{r["src"]}</p>{country_chart(r)}'
            f"{docs_block(r['country'])}{detail_block(r)}</section>"
        )

    legend = (f'<span class="k"><i style="color:{COLORS["nso"]}"></i>National statistical office</span>'
              f'<span class="k"><i style="color:{COLORS["UN WPP"]}"></i>UN WPP</span>')

    iso = iso_codes()
    ov_rows = [dict(country=r["country"], year=r["year"], nso=round(r["nso_v"], 4),
                    wpp=round(r["wpp_v"], 4), tier=r["tier"], recalc=r["recalc"],
                    iso=iso.get(r["country"], ""), short=SHORT.get(r["country"], r["country"]))
               for r in rows if r["wpp_v"]]
    shapes, map_height = world_paths()

    html = open(os.path.join(HERE, "template.html")).read()
    for token, value in [
        ("{{START}}", str(START)),
        ("{{LEGEND}}", legend),
        ("{{OPTIONS}}", "".join(options)),
        ("{{ROWS}}", json.dumps(ov_rows)),
        ("{{TIERSJSON}}", json.dumps(TIERS)),
        ("{{UNPLOTTED}}", json.dumps(unplotted)),
        ("{{SECTIONS}}", "".join(sections)),
        ("{{SHAPES}}", json.dumps(shapes)),
        ("{{MAPBOX}}", f"0 0 1000 {map_height}"),
        ("{{STAMP}}", datetime.datetime.now().strftime("%Y-%m-%d %H:%M")),
    ]:
        html = html.replace(token, value)
    open(OUT, "w").write(html)

    print("  ranked:", ", ".join(f"{r['country']} {r['gap']:+.3f}" for r in rows))
    print(f"  unplotted: {len(unplotted)}")
    print("  " + refresh())
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
    # only the national series carries dots: the UN's is annual, so every year of it is a real value
    series.append(("ln dots", "var(--nso)", _pairs(r["nso"][r["nso"].year <= X1])))
    return line_chart(series, START, X1, f'{r["country"]} fertility rate')


COLUMNS = ["band", "nso_births", "wpp_births", "nso_women", "wpp_women"]


def detail_block(r):
    key = f"detail_{r['country'].replace(' ', '_')}_{r['year']}"

    def build():
        rows = compare(r["country"], r["model_country"], r["year"])
        return pd.DataFrame(rows or [], columns=COLUMNS)

    # the same code-changed rebuild as the series, over the dispatcher and the country's own module,
    # since the bands come from both. Czechia's 45-49 births were wrong by a factor of five and the fix
    # was in czechia.py, so hashing only the dispatcher would have left the wrong dot on the page.
    df = cached(key, build, _fingerprint_of(compare, r["loader"]))
    bands = list(df.itertuples(index=False, name=None)) if not df.empty else None
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
