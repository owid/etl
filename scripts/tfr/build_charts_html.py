"""National vital statistics vs the four modelling groups — one chart per country, stacked, inline SVG."""

import datetime
import math
import os
import sys
import warnings

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
warnings.filterwarnings("ignore")

from plot_panels import PANELS, START  # noqa: E402
from detail import compare  # noqa: E402
from sources import COLORS, SOURCES, un_wpp  # noqa: E402
import numpy as np  # noqa: E402

OUT = "tfr_charts.html"

JS = """<script>
(function () {
  const sel = document.getElementById('pick');
  const secs = Array.from(document.querySelectorAll('section[data-country]'));
  function apply() {
    const v = sel.value;
    secs.forEach((s) => {
      const on = !v || s.dataset.country === v;
      s.hidden = !on;
      s.classList.toggle('solo', Boolean(v) && on);
    });
    if (v) window.scrollTo({ top: 0 });
  }
  sel.addEventListener('change', apply);
  apply();
})();
</script>"""
W, H = 940, 340
PAD_L, PAD_R, PAD_T, PAD_B = 44, 20, 14, 30
X0, X1 = START, 2032
Y0 = 0.0


def sx(y):
    return PAD_L + (y - X0) / (X1 - X0) * (W - PAD_L - PAD_R)


def sy(v, y1):
    return PAD_T + (y1 - v) / (y1 - Y0) * (H - PAD_T - PAD_B)


# painting order, back to front — SVG draws later elements on top
Z = ["un-wpp", "nso"]


def slug(name):
    return name.lower().replace(" ", "-")


def clip(df):
    return df[(df.year >= X0) & (df.year <= X1)]


def path(df, y1):
    """SVG path, broken wherever a year is missing."""
    d, pen = [], False
    for _, r in df.iterrows():
        if pd.isna(r.value):
            pen = False
            continue
        d.append(f"{'M' if not pen else 'L'}{sx(r.year):.1f},{sy(r.value, y1):.1f}")
        pen = True
    return " ".join(d)


def _at(series, year, prefer):
    """Value at `year` from the named series, interpolating a coarse grid."""
    for lab, df, _ in series:
        if lab != prefer:
            continue
        d = df.dropna().sort_values("year")
        if year in set(d.year):
            return float(d[d.year == year].value.iloc[0])
        if len(d) and d.year.min() <= year <= d.year.max():
            return float(np.interp(year, d.year, d.value))
    return None


def spread(model_country, nso):
    """|UN WPP medium - national| at the NSO's latest year. Before the projections start
    (2024) the medium variant does not exist, so the WPP estimate stands in for it."""
    d = nso.dropna()
    yr, val = int(d.year.max()), float(d.iloc[-1].value)
    w = un_wpp(model_country)
    ref = _at(w, yr, "medium")
    if ref is None:
        ref = _at(w, yr, "estimates")
    if ref is None:
        return 0.0, yr, 0
    return abs(val - ref), yr, 2


def chart(country, nso):
    """Zero-based y-axis; the top is set by the tallest line actually drawn."""
    series = []
    for name, fn in SOURCES:
        for _, df, is_proj in fn(country):
            df = clip(df)
            if not df.empty and df.value.notna().any():
                series.append((COLORS[name], slug(name), df, is_proj))
    series.append(("var(--nso)", "nso", clip(nso), False))

    top = max(d.value.max() for _, _, d, _ in series)
    step = 0.5 if top > 1.6 else 0.25
    y1 = math.ceil(top / step) * step + step * 0.15  # a little headroom above the tallest line

    g = []
    v = step
    while v < y1:
        g.append(f'<line class="grid" x1="{PAD_L}" y1="{sy(v, y1):.1f}" x2="{W - PAD_R}" y2="{sy(v, y1):.1f}"/>')
        g.append(
            f'<text class="ylab" x="{PAD_L - 8}" y="{sy(v, y1) + 3.5:.1f}">{v:.2f}'.rstrip("0").rstrip(".") + "</text>"
        )
        v += step
    g.append(f'<line class="axis" x1="{PAD_L}" y1="{sy(0, y1):.1f}" x2="{W - PAD_R}" y2="{sy(0, y1):.1f}"/>')
    g.append(f'<text class="ylab" x="{PAD_L - 8}" y="{sy(0, y1) + 3.5:.1f}">0</text>')
    for yr in range(START, X1 + 1, 5):
        g.append(f'<text class="xlab" x="{sx(yr):.1f}" y="{H - 10}">{yr}</text>')

    series.sort(key=lambda t: Z.index(t[1]) if t[1] in Z else -1)
    for col, src, df, is_proj in series:
        cls = "ln proj" if is_proj else "ln"
        pts = df.dropna(subset=["value"])
        if len(pts) == 1:
            r = pts.iloc[0]
            g.append(
                f'<circle class="dot" data-src="{src}" style="fill:{col}" '
                f'cx="{sx(r.year):.1f}" cy="{sy(r.value, y1):.1f}" r="3.4"/>'
            )
        else:
            g.append(f'<path class="{cls}" data-src="{src}" style="stroke:{col}" d="{path(df, y1)}"/>')

    return f'<svg viewBox="0 0 {W} {H}" role="img" aria-label="{country} fertility rate">{"".join(g)}</svg>'


# ---------------------------------------------------------------- dumbbells
DB_L, DB_R, DB_T, DB_B, ROW = 84, 24, 16, 32, 30


def _nice(top):
    """Round the axis top up to 1, 2 or 5 times a power of ten."""
    if top <= 0:
        return 1.0
    mag = 10 ** math.floor(math.log10(top))
    for m in (1, 2, 5, 10):
        if top <= m * mag:
            return m * mag
    return 10 * mag


def _fmt(v):
    if v >= 1e6:
        return f"{v / 1e6:g}M"
    if v >= 1e3:
        return f"{v / 1e3:g}k"
    return f"{v:g}"


def dumbbell(rows, title):
    """rows: [(label, national, wpp)] — one row per age band, the two values joined."""
    h = DB_T + len(rows) * ROW + DB_B
    top = _nice(max(max(a, b) for _, a, b in rows))
    span = W - DB_L - DB_R

    def dx(v):
        return DB_L + v / top * span

    g = []
    for i in range(5):
        v = top * i / 4
        g.append(
            f'<line class="grid" x1="{dx(v):.1f}" y1="{DB_T - 4}" x2="{dx(v):.1f}" y2="{DB_T + len(rows) * ROW}"/>'
        )
        g.append(f'<text class="xlab" x="{dx(v):.1f}" y="{h - 12}">{_fmt(v)}</text>')

    for i, (label, a, b) in enumerate(rows):
        y = DB_T + i * ROW + ROW / 2
        g.append(f'<text class="blab" x="{DB_L - 10}" y="{y + 4:.1f}">{label}</text>')
        g.append(f'<line class="bar" x1="{dx(min(a, b)):.1f}" y1="{y:.1f}" x2="{dx(max(a, b)):.1f}" y2="{y:.1f}"/>')
        g.append(f'<circle class="pw" cx="{dx(b):.1f}" cy="{y:.1f}" r="5"/>')
        g.append(f'<circle class="pn" cx="{dx(a):.1f}" cy="{y:.1f}" r="5"/>')

    return f'<svg viewBox="0 0 {W} {h}" role="img" aria-label="{title}">{"".join(g)}</svg>'


def detail_block(country, model_country, yr):
    rows = compare(country, model_country, yr)
    if not rows:
        return (
            '<div class="detail"><p class="na">This office publishes fertility rates only, not the '
            "births and female population behind them, so the two sources cannot be compared "
            "age band by age band.</p></div>"
        )
    births = [(lab, nb, wb) for lab, nb, wb, _, _ in rows]
    women = [(lab, nw, ww) for lab, _, _, nw, ww in rows]
    return (
        f'<div class="detail">'
        f"<h3>Births by age of mother, {yr}</h3>{dumbbell(births, f'{country} births {yr}')}"
        f"<h3>Women by age group, {yr}</h3>{dumbbell(women, f'{country} women {yr}')}"
        f"</div>"
    )


def main():
    built = []
    for country, src, fn, model_country in PANELS:
        nso = fn()
        nso = nso[nso.year >= START].copy()
        span = range(int(nso.year.min()), int(nso.year.max()) + 1)
        nso = nso.set_index("year").reindex(span).rename_axis("year").reset_index()
        sd, yr, n = spread(model_country, nso)
        built.append((sd, yr, n, country, src, nso, model_country))

    # biggest gap to the UN WPP medium projection first
    built.sort(key=lambda r: -r[0])
    sections, options = [], ['<option value="">All countries</option>']
    for sd, yr, n, country, src, nso, model_country in built:
        options.append(f'<option value="{country}">{country}</option>')
        sections.append(
            f'<section data-country="{country}"><h2>{country}</h2>'
            f'<p class="src">{src}</p>{chart(model_country, nso)}'
            f"{detail_block(country, model_country, yr)}</section>"
        )
    print("  ranked:", ", ".join(f"{c} {sd:.3f}" for sd, _, _, c, *_ in built))

    keys = "".join(
        f'<span class="k"><i style="border-color:{c}"></i>{n}</span>'
        for n, c in [("National statistical office", COLORS["nso"])] + [(n, COLORS[n]) for n, _ in SOURCES]
    )
    stamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

    open(OUT, "w").write(f"""<title>National vital statistics vs modelled fertility estimates</title>
<style>
 :root {{ --bg:#fff; --fg:#1d1d1b; --mut:#6b6b6b; --line:#e9e9e9; --nso:#c94a3b; }}
 @media (prefers-color-scheme: dark) {{
   :root {{ --bg:#14161a; --fg:#e9e9e7; --mut:#9a9a97; --line:#2b2e34; --nso:#e8705f; }} }}
 :root[data-theme="dark"] {{ --bg:#14161a; --fg:#e9e9e7; --mut:#9a9a97; --line:#2b2e34; --nso:#e8705f; }}
 :root[data-theme="light"] {{ --bg:#fff; --fg:#1d1d1b; --mut:#6b6b6b; --line:#e9e9e9; --nso:#c94a3b; }}
 body {{ background:var(--bg); color:var(--fg); margin:0; padding:34px 24px 76px;
        font:15px/1.6 -apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif; }}
 .wrap {{ max-width:1000px; margin:0 auto; }}
 h1 {{ font-size:27px; margin:0 0 8px; letter-spacing:-.01em; }}
 .lede {{ color:var(--mut); margin:0 0 20px; max-width:76ch; }}
 .key {{ display:flex; gap:18px; flex-wrap:wrap; font-size:13px; color:var(--mut);
        position:sticky; top:0; z-index:10; background:var(--bg); margin:0 -24px 30px;
        padding:12px 24px; border-bottom:1px solid var(--line); }}
 .key i {{ display:inline-block; width:22px; height:0; border-top:3px solid; vertical-align:middle; margin-right:7px; }}
 section {{ margin:0 0 36px; }}
 h2 {{ font-size:19px; margin:0 0 2px; }}
 .src {{ color:var(--mut); font-size:13px; margin:0 0 6px; }}
 svg {{ width:100%; height:auto; display:block; overflow:visible; }}
 .grid {{ stroke:var(--line); stroke-width:1; }}
 .axis {{ stroke:var(--line); stroke-width:1.4; }}
 .ylab {{ fill:var(--mut); font-size:11px; text-anchor:end; }}
 .xlab {{ fill:var(--mut); font-size:11px; text-anchor:middle; }}
 .ln {{ fill:none; stroke-width:1.8; stroke-linejoin:round; stroke-linecap:round; }}
 .proj {{ stroke-dasharray:5 3; }}
 .blab {{ fill:var(--mut); font-size:11.5px; text-anchor:end; }}
 .bar {{ stroke:var(--line); stroke-width:4; stroke-linecap:round; }}
 .pn {{ fill:var(--nso); }}
 .pw {{ fill:{COLORS["UN WPP"]}; }}
 select {{ font:inherit; font-size:13px; color:var(--fg); background:var(--bg); border:1px solid var(--line);
          border-radius:6px; padding:4px 8px; margin-left:auto; }}
 .detail {{ display:none; margin-top:24px; padding-top:6px; border-top:1px solid var(--line); }}
 section.solo .detail {{ display:block; }}
 section[hidden] {{ display:none; }}
 h3 {{ font-size:14.5px; font-weight:600; margin:20px 0 4px; }}
 .na {{ color:var(--mut); font-size:13.5px; max-width:70ch; }}
 .k {{ display:inline-flex; align-items:center; font-size:13px; color:var(--mut); }}
 footer {{ color:var(--mut); font-size:12.5px; border-top:1px solid var(--line); padding-top:16px; max-width:82ch; }}
</style>
<div class="wrap">
<h1>National vital statistics vs modelled fertility estimates</h1>
<p class="lede">Total fertility rate, children per woman, {START} onward. The red line is computed from each
country's own statistical office — registered births by age of mother over its own female population. The blue lines
is the UN World Population Prospects 2024: solid where it estimates the past, and one line per projection variant
after that.</p>

<div class="key">{keys}<select id="pick" aria-label="Pick a country">{"".join(options)}</select></div>

{"".join(sections)}

<footer><strong>National sources.</strong> Colombia: DANE Estadísticas Vitales, births by age of mother, over DANE
population projections. Brazil: IBGE Estatísticas do Registro Civil, SIDRA tables 197 and 2612, over IBGE population
projections. France: INSEE fertility rates by detailed age of mother. Italy: ISTAT age-specific fertility rates.<br><br>
<strong>Comparison source.</strong> UN World Population Prospects 2024 — estimates to 2023, then the
high, medium and low variants.<br><br>
Brazil's 2000–02 points come from a different IBGE table than 2003 onward and understate the level, because birth
registration coverage was still improving — the step up in 2003 is coverage, not fertility. Latest years are
provisional in Colombia, Brazil and France. Built {stamp}.</footer>
{JS}
</div>
""")
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
