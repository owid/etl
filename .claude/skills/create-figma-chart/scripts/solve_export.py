#!/usr/bin/env python3
"""Solve a grapher embed export so the imported chart fills a template band.

Requesting the aspect you *want* is the mistake: grapher insets the drawing inside the SVG it hands
back, so the group Figma imports is smaller than the declared size — and it is the group that has to
fill the band. Ask for the aspect that *yields* the one you want instead.

    inset        = declared - ink, PER AXIS (see below — the axes are not equal)
    canvas       : W * H ~= 510000 px^2   (the server renormalizes to this)
    ink          : (W - insetX) x (H - insetY)
    label size  ~= 0.75 * imFontSize, then scaled by the HEIGHT-first fit factor

The label scale is the HEIGHT-first factor, because that is the only rescale Step 7 spends. From
`reference/FITTING.md`: "Fit to the band's height, then map x to fill the width", and the x-map that
follows "takes the plot out to the full content width without touching a single font size". The
width-first `placed width / ink width` agrees with it exactly when the achieved aspect IS the target
aspect — which pass 2 guarantees and pass 1 does not, and on a pass-1 miss the width-first factor
asked for an imFontSize up to 4px away from the right one.

Substituting W = 510000/H into (W - insetX)/(H - insetY) = A gives a quadratic in H:

    A*H^2 + (insetX - A*insetY)*H - 510000 = 0

solved here in closed form.

TWO PASSES, AND THE SECOND IS EXACT. Do not try to land it in one.

  1. `--band WxH` alone uses the `1.4 * imFontSize` model for the inset and bisects imFontSize for
     your target label size — then ROUNDS it, because `imFontSize` travels in the URL as an integer,
     and recomputes every reported number from that integer, so the prediction is what the emitted
     `curl` actually produces. Treat the result as a probe: export it, import it, hide the furniture,
     measure the ink (scripts/measure_fit.js does all of it in one call).
  2. Re-run with `--declared` and `--ink` (or `--inset-x/--inset-y`) from that measurement, plus
     `--im-font-size` from the probe. The inset is stable to ~2px across an aspect change at the same
     imFontSize, so this pass lands.

Why the model is only a probe: the `1.4 * imFontSize` figure is symmetric, and the real inset is not.
Measured on an `imType=uncaptioned` line chart that reserves a right margin for a direct entity
label:

    imFontSize 30 -> insetX 64.1, insetY 29.0   (model predicts 42.0 / 42.0)   democracy index
    imFontSize 21 -> insetX 70.6, insetY 40.8   (model predicts 29.4 / 29.4)   democracy index
    imFontSize 29 -> insetX 69.8, insetY 33.7   (model predicts 40.6 / 40.6)   CO2 per capita

The model is right for the charts it was measured on — the recorded examples come out ~44/46 at
imFontSize 32 — and wrong by 2x on the horizontal axis for this class. Note also that insetY got
*larger* as the font got *smaller*, and that the third row above sits at a *higher* insetX than the
first despite a lower font: the inset is a property of the CHART (how much right margin its direct
labels reserve, how wide its axis ticks are), not a function of imFontSize. Don't try to fit one from
a few runs. Measure it.

What IS stable — and is the whole reason pass 2 works — is the inset at a FIXED imFontSize across the
aspect change pass 2 makes. Measured live on the CO2 chart: pass 1 declared 849x601 and pass 2
declared 868x587, and the inset read 69.75/33.72 on both, i.e. **0.00px of drift**. The x-map leftover
went from 24.47px on the probe to 0.15px on the measured pass — sub-pixel, no correction needed. And the measured inset pins the font — pass 2 keeps the
imFontSize the inset was measured at rather than re-bisecting for a label target, because changing
the font invalidates the measurement.

And don't try to read the ink out of the SVG to skip the first import: text ink depends on font
metrics that are not in the file. Parsing every coordinate in one came out 13-33px wide of what
Figma measured.

The band is not the target. Step 7 fits the chart to `band - 2*gap`, not to the band itself, so the
aspect to solve for is `bandW / (bandH - 2*gap)`. Solving for the band's own aspect lands the chart
edge to edge with a zero-pixel gap. The gap is 14 by default with 12-16 the comfortable range on the
540-wide frames, but it is a flag because the Instagram portrait runs at 30 (reference/FITTING.md).

Usage — from the repo root, through the venv (this file is committed non-executable, like every
other script in this directory):

    S=.claude/skills/create-figma-chart/scripts/solve_export.py

    # pass 1 — the probe
    .venv/bin/python $S --band 508x409 --target-label 15 --slug liberal-democracy

    # pass 2 — after measuring the import (declared from the SVG, ink from measure_fit.js, which
    # prints this command for you as `nextPass`)
    .venv/bin/python $S --band 508x409 --declared 791x645 --ink 726.92x615.96 --im-font-size 30 \\
        --slug liberal-democracy --params "tab=chart&country=~CHL"

    .venv/bin/python $S --band 302x220 --thumbnail --slug life-expectancy

Self-test (validates the model against the worked examples in the docs, round-trips the band
arithmetic — the height-fitted ink must leave exactly --gap at each end and no width for the x-map
to close — and reproduces the two measured-inset second passes recorded from a real run):
    .venv/bin/python $S --self-test
"""

from __future__ import annotations

import argparse
import math
import sys

CANVAS_AREA = 510_000.0  # px^2 the server renormalizes an uncaptioned/default export to
INSET_PER_FONT = 1.4  # the SYMMETRIC model inset — pass 1's probe only, see the module docstring
LABEL_RATIO = 0.75  # segment values / entity names, as a multiple of the base font
MIN_LABEL = 12.0  # the floor the guidelines set for a full-size chart
DEFAULT_GAP = 14.0  # px at each end of the band; 12-16 is the house range on 540-wide frames
DEFAULT_TARGET_LABEL = 13.5  # final label px on a 540-wide frame; the portrait ladder uses 15
# How far an INTEGER imFontSize can leave the final label from --target-label: half a font step,
# 0.5 * target / F, which is ~0.25px at the 13.5px/F=29 and 15px/F=27 the templates run.
ROUNDING_TOLERANCE = 0.25

# 302-wide thumbnail route (SMALL-CHARTS.md). imType=thumbnail returns early from extractOptions, so
# imWidth/imHeight are the canvas outright — but grapher still insets the ink inside that canvas, so
# the canvas has to be solved from the *content* width, not the frame width.
THUMB_MARGIN = 12.0  # side margin of the 302-wide templates: content box is 12 ... 290
THUMB_INK_INSET = 7.2  # measured ink padding per side at imFontSize=16
THUMB_TARGET_LABEL = 12.0  # top of the format's range; imFontSize=16 lands here on both types
THUMB_MIN_LABEL = 11.0  # the format's floor — the templates' own labels are 11px by design


def solve_canvas(ink_aspect: float, inset_x: float, inset_y: float) -> tuple[float, float]:
    """Return the (W, H) to request so the *ink* comes back at ink_aspect."""
    a = ink_aspect
    b = inset_x - a * inset_y
    h = (-b + math.sqrt(b * b + 4.0 * a * CANVAS_AREA)) / (2.0 * a)
    return CANVAS_AREA / h, h


def model_inset(font: float) -> tuple[float, float]:
    return INSET_PER_FONT * font, INSET_PER_FONT * font


def label_at(font: float, ink_h: float, fitted_h: float) -> float:
    """Final on-page label size once the export is HEIGHT-fitted to fitted_h.

    Height-first because that is the only rescale Step 7 spends: the x-map that follows closes the
    width without touching a font size (reference/FITTING.md). Identical to the width-first factor
    when the achieved aspect is the target aspect — which pass 2 guarantees — and not interchangeable
    on a pass-1 miss.
    """
    return LABEL_RATIO * font * (fitted_h / ink_h)


def target_aspect(band_w: float, band_h: float, gap: float) -> float:
    """The ink aspect that leaves `gap` at each end of the band once height-fitted.

    Solving for the band's own aspect instead — which is what a gap of 0 means — asks the chart to
    fill the band edge to edge and leaves nothing for the 12-16px the guidelines want.
    """
    usable = band_h - 2.0 * gap
    if usable <= 0:
        raise ValueError(f"gap {gap:g} leaves no height in a {band_h:g}px band (2*gap >= band height)")
    return band_w / usable


def solve_font(ink_aspect: float, usable_h: float, target_label: float) -> float:
    """Bisect for the imFontSize whose labels land on target_label after the height fit.

    Pass 1 only: it runs under the MODEL inset, which is what makes its result a probe.
    """
    lo, hi = 8.0, 200.0
    for _ in range(200):
        mid = (lo + hi) / 2.0
        ix, iy = model_inset(mid)
        w, h = solve_canvas(ink_aspect, ix, iy)
        if label_at(mid, h - iy, usable_h) < target_label:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2.0


def self_test() -> int:
    """The docs' recorded exports, the band round-trip, and a real run's measured-inset passes."""
    ok = True

    # --- the model inset, against the two exports the docs record.
    print("model inset, against the two exports the docs record:")
    print(f"{'F':>4} {'aspect':>8} {'W pred':>8} {'W obs':>7} {'H pred':>8} {'H obs':>7} {'err':>6}")
    for font, dw, dh, cw, ch in [(32.0, 901.0, 566.0, 857.0, 520.0), (30.0, 862.0, 591.0, 818.9, 550.0)]:
        ix, iy = model_inset(font)
        w, h = solve_canvas(cw / ch, ix, iy)
        err = max(abs(w - dw), abs(h - dh))
        ok &= err <= 3.0
        print(f"{font:4.0f} {cw / ch:8.4f} {w:8.1f} {dw:7.0f} {h:8.1f} {dh:7.0f} {err:6.1f}")
    print("(the docs state the model is approximate, +-3px — that is why pass 1 is a probe)")

    # --- the band arithmetic, round-tripped through the HEIGHT-first fit Step 7 actually performs:
    # solve for band - 2*gap, rescale by usable_h / ink_h, and the result must leave exactly gap at
    # each end, land the full band width (nothing for the x-map to close) and hit target_label. A
    # zero-gap solve here is one regression this covers — it is what a chart crowding the header and
    # footer looks like. `F` is the ideal bisection result and `emit` the integer the URL carries;
    # `label` is measured against `emit`, because that is the export you actually get.
    bands = [(508.0, 371.0, 14.0, 13.5), (508.0, 371.0, 12.0, 13.5), (508.0, 552.0, 30.0, 15.0)]
    print(
        f"\n{'band':>12} {'gap':>5} {'F':>7} {'emit':>5} {'ink':>15} "
        f"{'gap/end':>8} {'x-map':>7} {'label':>7} {'err':>6}"
    )
    worst_gap = worst_xmap = worst_ideal = worst_label = 0.0
    for bw, bh, gap, target in bands:
        usable = bh - 2.0 * gap
        aspect = bw / usable
        font_exact = solve_font(aspect, usable, target)
        font = float(round(font_exact))
        ix, iy = model_inset(font)
        w, h = solve_canvas(aspect, ix, iy)
        iw, ih = w - ix, h - iy
        scale = usable / ih
        got_gap = (bh - ih * scale) / 2.0
        xmap = bw - iw * scale
        label = label_at(font, ih, usable)
        ixe, iye = model_inset(font_exact)
        _, he = solve_canvas(aspect, ixe, iye)
        worst_gap = max(worst_gap, abs(got_gap - gap))
        worst_xmap = max(worst_xmap, abs(xmap))
        worst_ideal = max(worst_ideal, abs(label_at(font_exact, he - iye, usable) - target))
        worst_label = max(worst_label, abs(label - target))
        print(
            f"{f'{bw:g}x{bh:g}':>12} {gap:5.0f} {font_exact:7.3f} {font:5.0f} "
            f"{f'{iw:.1f}x{ih:.1f}':>15} {got_gap:8.2f} {xmap:7.3f} {label:7.2f} {label - target:+6.3f}"
        )
    print(
        f"\nworst gap error {worst_gap:.3f}px and x-map leftover {worst_xmap:.3f}px — exact (<0.01px), "
        f"rounding does not touch the aspect.\nideal-font label error {worst_ideal:.3f}px (<0.01px); "
        f"emitted-font label error {worst_label:.3f}px (<{ROUNDING_TOLERANCE:g}px, the integer bound)"
    )
    ok = ok and worst_gap < 0.01 and worst_xmap < 0.01
    ok = ok and worst_ideal < 0.01 and worst_label < ROUNDING_TOLERANCE

    # --- pass 2, against the two measured-inset exports recorded from a real run. These validate
    # the per-axis quadratic against reality: the solve, fed the inset measured off the probe, must
    # predict the declared size the server actually returned.
    print("\nmeasured inset, against a real run's second passes:")
    print(f"{'case':10s} {'W pred':>8} {'W obs':>7} {'H pred':>8} {'H obs':>7} {'err':>6}")
    for label_, bw, bh, ix, iy, dw, dh in [
        ("square", 508.0, 409.0, 64.08, 29.04, 837.0, 609.0),
        ("desktop", 818.0, 521.0, 70.60, 40.80, 921.0, 554.0),
        # Measured end to end on a live run: probe at imFontSize 29 declared 849x601 and inked
        # 779.25x567.28 (inset 69.75/33.72); the pass-2 solve below predicted 869x587 and grapher
        # returned 868x587, landing the x-map leftover at 0.15px.
        ("co2 DI", 508.0, 380.0, 69.75, 33.72, 868.0, 587.0),
    ]:
        a = target_aspect(bw, bh, DEFAULT_GAP)
        w, h = solve_canvas(a, ix, iy)
        err = max(abs(w - dw), abs(h - dh))
        ok &= err <= 3.0
        print(f"{label_:10s} {w:8.1f} {dw:7.0f} {h:8.1f} {dh:7.0f} {err:6.1f}")
        # and the round-trip: the ink the solve implies must sit at the target aspect exactly, so
        # the height fit leaves exactly DEFAULT_GAP per end and nothing for the x-map to close.
        iw2, ih2 = w - ix, h - iy
        scale = (bh - 2.0 * DEFAULT_GAP) / ih2
        ok &= abs(iw2 / ih2 - a) < 1e-9
        ok &= abs((bh - ih2 * scale) / 2.0 - DEFAULT_GAP) < 0.01
        ok &= abs(bw - iw2 * scale) < 0.01
    print("(pass-2 ink aspect, gap and x-map leftover round-trip exactly — checked to <0.01px)")

    print("\nPASS" if ok else "\nFAIL")
    return 0 if ok else 1


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--band", help="the band to fill, in template px, e.g. 508x409")
    ap.add_argument(
        "--gap",
        type=float,
        default=DEFAULT_GAP,
        help=f"px to leave at each end of the band (default {DEFAULT_GAP:g}; 12-16 on the 540-wide "
        "frames, 30 on the Instagram portrait). The solve targets band height - 2*gap.",
    )
    ap.add_argument("--declared", help="pass 2: the probe's declared SVG size, e.g. 791x645")
    ap.add_argument("--ink", help="pass 2: the ink measured in Figma after hiding furniture, e.g. 726.92x615.96")
    ap.add_argument("--inset-x", type=float, help="pass 2: measured horizontal inset, if you have it directly")
    ap.add_argument("--inset-y", type=float, help="pass 2: measured vertical inset")
    ap.add_argument(
        "--im-font-size",
        type=float,
        help="pass 2: the imFontSize the inset was measured at (required — the inset is only valid "
        "at that font, so pass 2 keeps it rather than re-solving for --target-label)",
    )
    ap.add_argument(
        "--target-label",
        type=float,
        help=f"pass 1 only: desired final label px (default {DEFAULT_TARGET_LABEL:g}, or "
        f"{THUMB_TARGET_LABEL:g} on --thumbnail). Ignored on pass 2, where the measured inset pins "
        "the font.",
    )
    ap.add_argument(
        "--placed-width",
        type=float,
        help="width the height-fitted group is placed at (default: band width). Sets the aspect to "
        "solve for and the width the x-map has to reach — NOT the label scale, which follows the "
        "height fit.",
    )
    ap.add_argument(
        "--slug",
        help="slug to emit a ready curl for. A bare slug exports from /grapher/<slug>.svg; an "
        "Explorer view exports from a DIFFERENT route, /explorers/<slug>.svg (SKILL.md, Step 1), "
        "so pass the site-relative path when the input is not a grapher chart: "
        "--slug explorers/natural-disasters.",
    )
    ap.add_argument("--params", default="", help="extra grapher query params, e.g. 'tab=chart&country=~CHL'")
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

    def pair(s: str, flag: str) -> tuple[float, float]:
        try:
            a, b = (float(x) for x in s.lower().split("x"))
        except ValueError:
            ap.error(f"{flag} must look like 508x409")
            raise
        if a <= 0 or b <= 0:
            ap.error(f"{flag} must be positive on both axes, got {a:g}x{b:g}")
        return a, b

    bw, bh = pair(args.band, "--band")
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
        # Rounded for the same reason as the main route: imFontSize is an integer in the URL, so the
        # label size to report is the one that integer yields, not the one the target asked for.
        font = float(round(target_label / LABEL_RATIO))
        label = LABEL_RATIO * font
        print("thumbnail route — the canvas is taken outright, so no aspect solve and no rescale:")
        print(f"  frame           {bw:g} x {bh:g}")
        print(f"  content box     {content_w:g} wide  (margin {args.margin:g} per side)")
        print(f"  canvas          {canvas_w:.1f} wide  (+{args.ink_inset:g} ink inset per side) -> ink ~{content_w:g}")
        print(f"  imWidth/Height  {im_w}/{im_h}   (staticBounds = imWidth/4 x imHeight/4)")
        print(f"  imFontSize      {font:.0f}   ({label:g}px labels at {LABEL_RATIO}x base)", end="")
        if abs(label - target_label) > 0.05:
            print(f"   (asked for {target_label:g}; imFontSize is an integer)")
        else:
            print()
        if label < THUMB_MIN_LABEL:
            print(f"  *** {label:g}px is under this format's {THUMB_MIN_LABEL:g}px floor")
        if args.slug:
            p = f"{args.params}&" if args.params else ""
            path = args.slug if "/" in args.slug else f"grapher/{args.slug}"
            print(
                f'\ncurl -sL "https://ourworldindata.org/{path}.svg'
                f"?{p}imType=thumbnail&imWidth={im_w}&imHeight={im_h}"
                f'&imFontSize={font:.0f}&nocache" -o thumb.svg'
            )
        print(f"\nThe group is its own ink, so set chart.x = {args.margin:g} after import — no rescale.")
        print("The ink inset was measured at one font size: measure the import and expect one correction.")
        return 0

    usable_h = bh - 2.0 * args.gap
    if usable_h <= 0:
        ap.error(f"--gap {args.gap:g} leaves no height in a {bh:g}px band (2*gap >= band height)")
    aspect = placed_w / usable_h

    # --- which pass?
    inset_x = inset_y = None
    if args.declared and args.ink:
        dw, dh = pair(args.declared, "--declared")
        iw, ih = pair(args.ink, "--ink")
        inset_x, inset_y = dw - iw, dh - ih
        if inset_x <= 0 or inset_y <= 0:
            ap.error(
                f"--ink {iw:g}x{ih:g} is not inside --declared {dw:g}x{dh:g} — the inset came out "
                f"{inset_x:g}/{inset_y:g}. The ink must be measured on the freshly-imported probe, "
                "before any fit, and belong to the same export as --declared."
            )
    elif args.inset_x is not None and args.inset_y is not None:
        if args.inset_x <= 0 or args.inset_y <= 0:
            ap.error(f"insets must be positive, got {args.inset_x:g}/{args.inset_y:g}")
        inset_x, inset_y = args.inset_x, args.inset_y
    elif args.declared or args.ink or args.inset_x is not None or args.inset_y is not None:
        ap.error("pass 2 needs BOTH --declared and --ink, or BOTH --inset-x and --inset-y")

    # Solve, then round: imFontSize goes into the URL as an integer, so the integer is what the
    # export will actually use. Recompute the canvas, the ink box and the label from it — reporting
    # the ideal font's label beside an `imFontSize` one step away promises a size the request cannot
    # deliver. The aspect survives rounding untouched, because solve_canvas enforces it at whatever
    # inset it is given, so the gap and the x-map leftover below stay exact.
    if inset_x is None:
        # PASS 1 — the probe. Model inset, font bisected for the target label size.
        font_exact = solve_font(aspect, usable_h, target_label)
        font = float(round(font_exact))
        inset_x, inset_y = model_inset(font)
        pass_label = "1 (PROBE — model inset, expect to re-run)"
    else:
        # PASS 2 — measured inset. The font is what the inset was measured at, not re-bisected:
        # changing it invalidates the measurement.
        if args.im_font_size is None:
            ap.error("--im-font-size is required with a measured inset: the inset is only valid at that font")
        font_exact = args.im_font_size
        font = float(round(font_exact))
        pass_label = "2 (measured inset — this one lands)"

    w, h = solve_canvas(aspect, inset_x, inset_y)
    ink_w, ink_h = w - inset_x, h - inset_y
    im_w = int(round(w / h * 1000))

    # Height-first, exactly as Step 7 fits: `chart.rescale(TARGET_H / chart.height)`. The gap is then
    # --gap at each end by construction, so it is no longer the diagnostic — the leftover width the
    # x-map has to close is, and it IS the aspect miss expressed in px (measure_fit.js reports the
    # same quantity as `xMapShortfall`).
    scale = usable_h / ink_h
    label = label_at(font, ink_h, usable_h)
    shortfall = placed_w - ink_w * scale
    if abs(shortfall) < 0.05:
        shortfall = 0.0  # so an exact solve prints 0.0 rather than float noise's -0.0

    print(f"pass            {pass_label}")
    print(f"band            {bw:g} x {bh:g}   (aspect {bw / bh:.4f})")
    print(f"usable          {placed_w:g} x {usable_h:g}   (target ink aspect {aspect:.4f}, {args.gap:g}px gap per end)")
    print(f"inset           x {inset_x:.2f}   y {inset_y:.2f}")
    print()
    print(f"imFontSize      {font:.0f}", end="")
    if abs(font - font_exact) > 0.05:
        print(f"   (ideal {font_exact:.1f}, rounded — the URL carries an integer)")
    else:
        print()
    print(f"imWidth/Height  {im_w}/1000        (aspect ratio only — the server renormalizes)")
    print(f"declared        {w:.0f} x {h:.0f}   ({w * h:,.0f} px^2)")
    print(f"ink             {ink_w:.1f} x {ink_h:.1f}   (aspect {ink_w / ink_h:.4f})")
    print(f"scale into band {scale:.4f}   (height-first: usable height / ink height)")
    print(f"gap per end     {args.gap:g}px   (exact — the height fit sets it by construction)")
    print(f"x-map closes    {shortfall:.1f}px of the {placed_w:g}px width", end="")
    if pass_label.startswith("1") and abs(shortfall) > 1.0:
        print("   *** should be ~0 — re-check --band and --gap")
    else:
        print()
    print(f"final labels    {label:.1f}px", end="")
    if label < MIN_LABEL:
        print(f"   *** under the {MIN_LABEL:g}px floor — raise --target-label or cut entities")
    elif pass_label.startswith("1") and abs(label - target_label) > 0.05:
        print(f"   (asked for {target_label:g}; imFontSize {font:.0f} is the closest integer)")
    elif pass_label.startswith("2") and args.target_label is not None:
        print("   (--target-label is ignored on pass 2 — the measured inset pins the font)")
    else:
        print()

    if args.slug:
        p = f"{args.params}&" if args.params else ""
        path = args.slug if "/" in args.slug else f"grapher/{args.slug}"
        print(
            f'\ncurl -sL "https://ourworldindata.org/{path}.svg'
            f"?{p}imType=uncaptioned&imWidth={im_w}&imHeight=1000&imFontSize={font:.0f}"
            f'&nocache" -o embed.svg'
        )
        if pass_label.startswith("1"):
            print('  grep -oE \'width="[0-9]+" height="[0-9]+"\' embed.svg | head -1   # the declared size, for pass 2')

    if pass_label.startswith("1"):
        print(
            "\nThis is a PROBE. Import it, hide the furniture, measure the ink, then run the pass-2"
            "\ncommand: scripts/measure_fit.js does all three in one call (set CONFIG.declared and"
            "\nCONFIG.imFontSize from the export above) and prints the command as `nextPass`."
        )
    print(
        "\nFit the height, then x-map the width — never a second rescale (reference/FITTING.md)."
        "\nHide connectors and year markers BEFORE measuring the ink: they extend past the plot, so"
        "\nhiding them narrows the group and makes it relatively taller."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
