"""Inline-SVG renderers. No data access here — callers pass plain numbers."""

import math

W, H = 940, 340
PAD_L, PAD_R, PAD_T, PAD_B = 44, 20, 14, 30
DB_L, DB_R, DB_T, DB_B, ROW = 84, 24, 16, 32, 30


# ---------------------------------------------------------------- line chart
def line_chart(series, x0, x1, label=""):
    """series: [(css_class, color, [(year, value_or_None)])]. Zero-based y-axis whose top
    is set by the tallest point actually drawn; a None breaks the line."""
    pts = [v for _, _, s in series for _, v in s if v is not None]
    top = max(pts)
    step = 0.5 if top > 1.6 else 0.25
    y1 = math.ceil(top / step) * step + step * 0.15

    def sx(year):
        return PAD_L + (year - x0) / (x1 - x0) * (W - PAD_L - PAD_R)

    def sy(v):
        return PAD_T + (y1 - v) / y1 * (H - PAD_T - PAD_B)

    g = []
    v = step
    while v < y1:
        g.append(f'<line class="grid" x1="{PAD_L}" y1="{sy(v):.1f}" x2="{W - PAD_R}" y2="{sy(v):.1f}"/>')
        g.append(f'<text class="ylab" x="{PAD_L - 8}" y="{sy(v) + 3.5:.1f}">{v:g}</text>')
        v += step
    g.append(f'<line class="axis" x1="{PAD_L}" y1="{sy(0):.1f}" x2="{W - PAD_R}" y2="{sy(0):.1f}"/>')
    g.append(f'<text class="ylab" x="{PAD_L - 8}" y="{sy(0) + 3.5:.1f}">0</text>')
    for year in range(x0, x1 + 1, 5):
        g.append(f'<text class="xlab" x="{sx(year):.1f}" y="{H - 10}">{year}</text>')

    for cls, color, s in series:
        drawn = [(y, v) for y, v in s if v is not None]
        if len(drawn) == 1:
            year, val = drawn[0]
            g.append(f'<circle class="pn" style="fill:{color}" cx="{sx(year):.1f}" cy="{sy(val):.1f}" r="3.4"/>')
            continue
        d, pen = [], False
        for year, val in s:
            if val is None:
                pen = False
                continue
            d.append(f"{'M' if not pen else 'L'}{sx(year):.1f},{sy(val):.1f}")
            pen = True
        g.append(f'<path class="{cls}" style="stroke:{color}" d="{" ".join(d)}"/>')

    return f'<svg viewBox="0 0 {W} {H}" role="img" aria-label="{label}">{"".join(g)}</svg>'


# ---------------------------------------------------------------- dumbbell
def _axis_top(top):
    """Round up: to the next half below 5, else to 1/2/5 times a power of ten."""
    if top <= 0:
        return 1.0
    if top < 5:
        return math.ceil(top * 2) / 2
    mag = 10 ** math.floor(math.log10(top))
    for m in (1, 2, 5, 10):
        if top <= m * mag:
            return m * mag
    return 10 * mag


def compact(v):
    if v >= 1e6:
        return f"{v / 1e6:g}M"
    if v >= 1e3:
        return f"{v / 1e3:g}k"
    return f"{v:g}"


def dumbbell(rows, label="", label_w=DB_L, values=False, fmt=compact):
    """rows: [(row_label, national, wpp)]. With values=True each dot is labeled — the lower
    one to its left, the higher to its right, so the pair never collides."""
    h = DB_T + len(rows) * ROW + DB_B
    pad = 46 if values else 0                      # room for the right-hand value label
    top = _axis_top(max(max(a, b) for _, a, b in rows))
    span = W - label_w - DB_R - pad

    def dx(v):
        return label_w + v / top * span

    g = []
    for i in range(5):
        v = top * i / 4
        g.append(f'<line class="grid" x1="{dx(v):.1f}" y1="{DB_T - 4}" '
                 f'x2="{dx(v):.1f}" y2="{DB_T + len(rows) * ROW}"/>')
        g.append(f'<text class="xlab" x="{dx(v):.1f}" y="{h - 12}">{compact(v)}</text>')

    for i, (row_label, a, b) in enumerate(rows):
        y = DB_T + i * ROW + ROW / 2
        g.append(f'<text class="blab" x="{label_w - 10}" y="{y + 4:.1f}">{row_label}</text>')
        g.append(f'<line class="bar" x1="{dx(min(a, b)):.1f}" y1="{y:.1f}" '
                 f'x2="{dx(max(a, b)):.1f}" y2="{y:.1f}"/>')
        g.append(f'<circle class="pw" cx="{dx(b):.1f}" cy="{y:.1f}" r="5"/>')
        g.append(f'<circle class="pn" cx="{dx(a):.1f}" cy="{y:.1f}" r="5"/>')
        if values:
            for v, cls in ((a, "vn"), (b, "vw")):
                low = v == min(a, b)
                g.append(f'<text class="{cls}" x="{dx(v) + (-10 if low else 10):.1f}" y="{y + 4:.1f}" '
                         f'text-anchor="{"end" if low else "start"}">{fmt(v)}</text>')

    return f'<svg viewBox="0 0 {W} {h}" role="img" aria-label="{label}">{"".join(g)}</svg>'
