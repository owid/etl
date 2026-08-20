#!/usr/bin/env python3
"""Solve a grapher embed export so the imported chart fills a template band on the first try.

Requesting the aspect you *want* is the mistake: grapher insets the drawing inside the SVG it hands
back, so the group Figma imports is smaller than the declared size — and it is the group that has to
fill the band. Ask for the aspect that *yields* the one you want instead.

The model, from `reference/FITTING.md` and Step 3:

    inset       ~= 1.4 * imFontSize on each axis
    canvas       : W * H ~= 510000 px^2   (the server renormalizes to this)
    content      : (W - 1.4F) x (H - 1.4F)
    label size   ~= 0.75 * F, then scaled by (usable band height / content height)

The label scale is the HEIGHT-first factor, because that is the only rescale Step 7 spends. From
`reference/FITTING.md`: "Fit to the band's height, then map x to fill the width", and the x-map that
follows "takes the plot out to the full content width without touching a single font size" (L243,
L450, L456). The width-first `placed width / content width` agrees with it exactly whenever the
solved aspect IS the band's usable aspect, so it reads as equivalent on a first pass -- and stops
being equivalent the moment --content-aspect carries the reflected aspect of a second pass, which is
deliberately off the band's own. There it asked for a font up to 4px away from the right one.

Substituting W = 510000/H into (W - 1.4F)/(H - 1.4F) = A gives a quadratic in H:

    A*H^2 + 1.4*F*(1 - A)*H - 510000 = 0

which this solves in closed form. F is then found by bisection, because the content width that sets
the final label size itself depends on F.

The band is not the target. Step 7 fits the chart to `band - 2*gap`, not to the band itself, so the
aspect to solve for is `bandW / (bandH - 2*gap)`. Solving for the band's own aspect lands the chart
edge to edge with a zero-pixel gap, which `measure_fit.js` then flags and you re-export. The gap is
14 by default with 12-16 the comfortable range on the 540-wide frames, but it is a flag because the
Instagram portrait runs at 30 (reference/FITTING.md).

Accurate to a few px, which is what the docs claim ("expect at most one correction"). To spend that
correction, take the `nextPass` command `measure_fit.js` prints — do NOT pass the aspect you measured
off the import back in as --content-aspect. Solving for the aspect you already got aims at the miss
instead of at the target and roughly doubles it; the correction has to be the reflection of the
measured aspect about the target, `2*target - measured`, which is what `nextPass` carries.

Usage — from the repo root, through the venv (this file is committed non-executable, like every
other script in this directory):

    S=.claude/skills/create-figma-chart/scripts/solve_export.py
    .venv/bin/python $S --band 508x371 --slug life-expectancy
    .venv/bin/python $S --band 508x371 --gap 30 --target-label 15 --params "country=USA~CHN"
    .venv/bin/python $S --band 302x220 --thumbnail --slug life-expectancy

Self-test (validates the model against the worked examples in the docs, round-trips the band
arithmetic — the height-fitted content must leave exactly --gap at each end and no width for the
x-map to close — and checks that a reflected second pass still lands its --target-label):
    .venv/bin/python $S --self-test
"""

from __future__ import annotations

import argparse
import math
import sys

CANVAS_AREA = 510_000.0  # px^2 the server renormalizes an uncaptioned/default export to
INSET_PER_FONT = 1.4  # inset on each axis, as a multiple of imFontSize
LABEL_RATIO = 0.75  # segment values / entity names, as a multiple of the base font
MIN_LABEL = 12.0  # the floor the guidelines set for a full-size chart
DEFAULT_GAP = 14.0  # px at each end of the band; 12-16 is the house range on 540-wide frames
DEFAULT_TARGET_LABEL = 13.5  # final label px on a 540-wide frame; the portrait ladder uses 15

# 302-wide thumbnail route (SMALL-CHARTS.md). imType=thumbnail returns early from extractOptions, so
# imWidth/imHeight are the canvas outright — but grapher still insets the ink inside that canvas, so
# the canvas has to be solved from the *content* width, not the frame width.
THUMB_MARGIN = 12.0  # side margin of the 302-wide templates: content box is 12 ... 290
THUMB_INK_INSET = 7.2  # measured ink padding per side at imFontSize=16
THUMB_TARGET_LABEL = 12.0  # top of the format's range; imFontSize=16 lands here on both types
THUMB_MIN_LABEL = 11.0  # the format's floor — the templates' own labels are 11px by design


def solve_canvas(content_aspect: float, font: float) -> tuple[float, float]:
    """Return the (W, H) to request so the *content* comes back at content_aspect."""
    a = content_aspect
    b = INSET_PER_FONT * font * (1.0 - a)
    disc = b * b + 4.0 * a * CANVAS_AREA
    h = (-b + math.sqrt(disc)) / (2.0 * a)
    return CANVAS_AREA / h, h


def content_box(w: float, h: float, font: float) -> tuple[float, float]:
    return w - INSET_PER_FONT * font, h - INSET_PER_FONT * font


def label_at(font: float, content_h: float, fitted_h: float) -> float:
    """Final on-page label size once the export is HEIGHT-fitted to fitted_h.

    Height-first because that is the only rescale Step 7 spends: the x-map that follows closes the
    width without touching a font size (reference/FITTING.md L243/L450/L456). Identical to the
    width-first factor when the solved aspect is the band's usable aspect; not identical, and not
    interchangeable, once --content-aspect carries a second pass's reflected aspect.
    """
    return LABEL_RATIO * font * (fitted_h / content_h)


def solve_font(content_aspect: float, usable_h: float, target_label: float) -> float:
    """Bisect for the imFontSize whose labels land on target_label after the height fit."""
    lo, hi = 8.0, 200.0
    for _ in range(200):
        mid = (lo + hi) / 2.0
        w, h = solve_canvas(content_aspect, mid)
        _, ch = content_box(w, h, mid)
        if label_at(mid, ch, usable_h) < target_label:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2.0


def self_test() -> int:
    """The two exports recorded in the docs, reproduced from the model."""
    cases = [
        # font, declared WxH, content WxH  (reference/FITTING.md / Step 3)
        (32.0, 901.0, 566.0, 857.0, 520.0),
        (30.0, 862.0, 591.0, 818.9, 550.0),
    ]
    worst = 0.0
    print(f"{'F':>5} {'aspect':>8} {'W pred':>8} {'W obs':>7} {'H pred':>8} {'H obs':>7} {'err px':>7}")
    for font, dw, dh, cw, ch in cases:
        w, h = solve_canvas(cw / ch, font)
        err = max(abs(w - dw), abs(h - dh))
        worst = max(worst, err)
        print(f"{font:5.0f} {cw / ch:8.4f} {w:8.1f} {dw:7.0f} {h:8.1f} {dh:7.0f} {err:7.1f}")
        # and the inset model itself
        pcw, pch = content_box(dw, dh, font)
        print(f"      inset check: content {pcw:.1f}x{pch:.1f} vs observed {cw:.1f}x{ch:.1f}")
    print(f"\nworst canvas error: {worst:.1f}px — docs state the model is approximate (+-3px)")
    ok = worst <= 3.0

    # The band arithmetic, round-tripped through the HEIGHT-first fit Step 7 actually performs:
    # solve for band - 2*gap, rescale by usable_h / content_h, and the result must leave exactly gap
    # at each end, land the full band width (nothing for the x-map to close) and hit target_label.
    # A zero-gap solve here is one regression this covers — it is what a chart crowding the header
    # and footer looks like.
    bands = [(508.0, 371.0, 14.0, 13.5), (508.0, 371.0, 12.0, 13.5), (508.0, 552.0, 30.0, 15.0)]
    print(f"\n{'band':>12} {'gap':>5} {'F':>4} {'content':>15} {'gap/end':>8} {'x-map':>7} {'label':>7}")
    worst_gap = 0.0
    worst_xmap = 0.0
    worst_label = 0.0
    for bw, bh, gap, target in bands:
        usable = bh - 2.0 * gap
        font = solve_font(bw / usable, usable, target)
        w, h = solve_canvas(bw / usable, font)
        cw, ch = content_box(w, h, font)
        scale = usable / ch
        got_gap = (bh - ch * scale) / 2.0
        xmap = bw - cw * scale
        label = label_at(font, ch, usable)
        worst_gap = max(worst_gap, abs(got_gap - gap))
        worst_xmap = max(worst_xmap, abs(xmap))
        worst_label = max(worst_label, abs(label - target))
        print(
            f"{f'{bw:g}x{bh:g}':>12} {gap:5.0f} {font:4.0f} {f'{cw:.1f}x{ch:.1f}':>15} "
            f"{got_gap:8.2f} {xmap:7.3f} {label:7.2f}"
        )
    print(
        f"\nworst gap error {worst_gap:.3f}px, worst x-map leftover {worst_xmap:.3f}px, "
        f"worst label error {worst_label:.3f}px — all exact arithmetic (<0.01px)"
    )
    ok = ok and worst_gap < 0.01 and worst_xmap < 0.01 and worst_label < 0.01

    # A reflected SECOND pass must still land its --target-label. The reflection is deliberately off
    # the band's own aspect, which is exactly where a width-first label scale stops agreeing with the
    # height-first fit: on the docs' 508x371 case measured at 1.4342 it asked for imFontSize 29 and
    # promised 13.5px labels the height fit renders at 13.93px, and across the miss range it picked a
    # font up to 4px off. Reflections come from measure_fit.js's `nextPass`.
    print(f"\n{'band':>12} {'measured':>9} {'reflected':>10} {'F':>4} {'label':>7} {'want':>6} {'err':>6}")
    worst_refl = 0.0
    for (bw, bh, gap, target), measured_frac in zip(bands, [-0.0316, 0.05, -0.10]):
        usable = bh - 2.0 * gap
        band_aspect = bw / usable
        measured = band_aspect * (1.0 + measured_frac)
        reflected = 2.0 * band_aspect - measured
        font = solve_font(reflected, usable, target)
        w, h = solve_canvas(reflected, font)
        _, ch = content_box(w, h, font)
        label = label_at(font, ch, usable)
        err = abs(label - target)
        worst_refl = max(worst_refl, err)
        print(
            f"{f'{bw:g}x{bh:g}':>12} {measured:9.4f} {reflected:10.4f} {font:4.0f} "
            f"{label:7.2f} {target:6.1f} {err:6.3f}"
        )
    print(f"\nworst reflected-pass label error: {worst_refl:.3f}px — the second pass keeps --target-label")
    ok = ok and worst_refl < 0.01

    print("PASS" if ok else "FAIL")
    return 0 if ok else 1


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--band", help="the band to fill, in template px, e.g. 508x371")
    ap.add_argument(
        "--content-aspect",
        type=float,
        help="the content aspect to solve for, bypassing the band/gap derivation. This is the "
        "TARGET, not the measurement: pass the reflected value from measure_fit.js's `nextPass` "
        "(2*target - measured), never the aspect you just measured off the import, which aims the "
        "solve at the miss and doubles it.",
    )
    ap.add_argument(
        "--gap",
        type=float,
        default=DEFAULT_GAP,
        help=f"px to leave at each end of the band (default {DEFAULT_GAP:g}; 12-16 on the 540-wide "
        "frames, 30 on the Instagram portrait). The solve targets band height - 2*gap.",
    )
    ap.add_argument(
        "--target-label",
        type=float,
        help=f"desired final label px (default {DEFAULT_TARGET_LABEL:g}, or {THUMB_TARGET_LABEL:g} on --thumbnail)",
    )
    ap.add_argument(
        "--placed-width",
        type=float,
        help="width the height-fitted group is placed at (default: band width). Sets the aspect to "
        "solve for and the width the x-map has to reach — NOT the label scale, which follows the "
        "height fit.",
    )
    ap.add_argument("--slug", help="grapher slug, to emit a ready curl")
    ap.add_argument("--params", default="", help="extra grapher query params, e.g. 'country=USA~CHN'")
    ap.add_argument(
        "--thumbnail",
        action="store_true",
        help="302-wide route: imType=thumbnail takes the size outright (staticBounds = imWidth/4 x "
        "imHeight/4), so no aspect solve and no rescale. Solves the canvas from the content width "
        "and the ink inset, so the group lands on the template's content box. See SMALL-CHARTS.md.",
    )
    ap.add_argument(
        "--margin",
        type=float,
        default=THUMB_MARGIN,
        help=f"--thumbnail only: side margin of the template (default {THUMB_MARGIN:g}, giving a "
        "278px content box on a 302-wide frame)",
    )
    ap.add_argument(
        "--ink-inset",
        type=float,
        default=THUMB_INK_INSET,
        help=f"--thumbnail only: ink padding per side inside the canvas (default {THUMB_INK_INSET:g}, "
        "measured at imFontSize=16 — measure the import and expect one correction)",
    )
    ap.add_argument("--self-test", action="store_true", help="validate the model against the docs")
    args = ap.parse_args()

    if args.self_test:
        return self_test()
    if not args.band:
        ap.error("--band is required (or use --self-test)")

    try:
        bw, bh = (float(x) for x in args.band.lower().split("x"))
    except ValueError:
        ap.error("--band must look like 508x371")
    if bw <= 0 or bh <= 0:
        ap.error(f"--band must be positive on both axes, got {bw:g}x{bh:g}")
    if args.content_aspect is not None and args.content_aspect <= 0:
        ap.error(f"--content-aspect must be positive, got {args.content_aspect:g}")

    placed_w = args.placed_width or bw
    default_label = THUMB_TARGET_LABEL if args.thumbnail else DEFAULT_TARGET_LABEL
    target_label = args.target_label if args.target_label is not None else default_label

    if args.thumbnail:
        # Target the ink, not the frame: a 302-wide canvas puts its ink at 7.2 ... 294.2, which
        # overflows the template's 12 ... 290 content box at both ends (SMALL-CHARTS.md).
        content_w = bw - 2.0 * args.margin
        if content_w <= 0:
            ap.error(f"--margin {args.margin:g} leaves no content width in a {bw:g}px frame")
        canvas_w = content_w + 2.0 * args.ink_inset
        im_w, im_h = int(round(canvas_w * 4)), int(round(bh * 4))
        font = target_label / LABEL_RATIO
        print("thumbnail route — the canvas is taken outright, so no aspect solve and no rescale:")
        print(f"  frame           {bw:g} x {bh:g}")
        print(f"  content box     {content_w:g} wide  (margin {args.margin:g} per side)")
        print(f"  canvas          {canvas_w:.1f} wide  (+{args.ink_inset:g} ink inset per side) -> ink ~{content_w:g}")
        print(f"  imWidth/Height  {im_w}/{im_h}   (staticBounds = imWidth/4 x imHeight/4)")
        print(f"  imFontSize      {font:.0f}   ({target_label:g}px labels at {LABEL_RATIO}x base)")
        if target_label < THUMB_MIN_LABEL:
            print(f"  *** {target_label:g}px is under this format's {THUMB_MIN_LABEL:g}px floor")
        if args.slug:
            p = f"{args.params}&" if args.params else ""
            print(
                f'\ncurl -sL "https://ourworldindata.org/grapher/{args.slug}.svg'
                f"?{p}imType=thumbnail&imWidth={im_w}&imHeight={im_h}"
                f'&imFontSize={font:.0f}&nocache" -o thumb.svg'
            )
        print(f"\nThe group is its own ink, so set chart.x = {args.margin:g} after import — no rescale.")
        print("The ink inset was measured at one font size: measure the import and expect one correction.")
        return 0

    usable_h = bh - 2.0 * args.gap
    if usable_h <= 0:
        ap.error(f"--gap {args.gap:g} leaves no height in a {bh:g}px band (2*gap >= band height)")

    aspect = args.content_aspect if args.content_aspect is not None else placed_w / usable_h
    font = solve_font(aspect, usable_h, target_label)
    w, h = solve_canvas(aspect, font)
    cw, ch = content_box(w, h, font)
    label = label_at(font, ch, usable_h)
    im_w = int(round(w / h * 1000))

    # Height-first, exactly as Step 7 fits: `chart.rescale(TARGET_H / chart.height)`. The gap is then
    # --gap at each end by construction, so it is no longer the diagnostic — the leftover width the
    # x-map has to close is, and it IS the aspect miss expressed in px (measure_fit.js reports the
    # same quantity as `xMapShortfall`).
    scale = usable_h / ch
    fitted_w = cw * scale
    shortfall = placed_w - fitted_w
    if abs(shortfall) < 0.05:
        shortfall = 0.0  # so an exact band solve prints 0.0 rather than float noise's -0.0

    src = "explicit target aspect" if args.content_aspect is not None else "band minus gaps"
    print(f"band            {bw:g} x {bh:g}   (aspect {bw / bh:.4f})")
    print(f"usable          {bw:g} x {usable_h:g}   (aspect {bw / usable_h:.4f}, {args.gap:g}px gap per end)")
    print(f"solving for     {aspect:.4f}  [{src}]")
    print(f"fitting to      {placed_w:g}px wide, height-first")
    print()
    print(f"imFontSize      {font:.0f}")
    print(f"imWidth/Height  {im_w}/1000        (aspect ratio only — the server renormalizes)")
    print(f"declared        {w:.0f} x {h:.0f}   ({w * h:,.0f} px^2)")
    print(f"content         {cw:.1f} x {ch:.1f}   (aspect {cw / ch:.4f})")
    print(f"scale into band {scale:.4f}   (height-first: usable height / content height)")
    print(f"gap per end     {args.gap:g}px   (exact — the height fit sets it by construction)")
    print(f"x-map closes    {shortfall:.1f}px of the {placed_w:g}px width", end="")
    if args.content_aspect is not None:
        print("   (a reflected second pass over-corrects on purpose, so expect this to be non-zero)")
    elif abs(shortfall) > 1.0:
        print("   *** should be ~0 on a band solve — re-check --band and --gap")
    else:
        print()
    print(f"final labels    {label:.1f}px", end="")
    if label < MIN_LABEL:
        print(f"   *** under the {MIN_LABEL:g}px floor — raise --target-label or cut entities")
    else:
        print()

    if args.slug:
        p = f"{args.params}&" if args.params else ""
        print(
            f'\ncurl -sL "https://ourworldindata.org/grapher/{args.slug}.svg'
            f"?{p}imType=uncaptioned&imWidth={im_w}&imHeight=1000&imFontSize={font:.0f}"
            f'&nocache" -o embed.svg'
        )
        print("\nthen check what came back, and correct once if the group's aspect moved:")
        print("  head -c 300 embed.svg")
        print("  grep -oE 'font-size=\"[0-9.]+\"' embed.svg | sort | uniq -c | sort -rn | head -3")
    print(
        "\nFit the height, then x-map the width — never a second rescale (reference/FITTING.md)."
        "\nHide connectors and year markers BEFORE measuring the group — they extend past the plot,"
        "\nso hiding them narrows it and makes it relatively taller. Then measure with"
        "\nscripts/measure_fit.js and run the `nextPass` command it prints: it reflects the measured"
        "\naspect about the target for you. Passing the measured aspect straight back to"
        "\n--content-aspect solves for the miss and doubles it."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
