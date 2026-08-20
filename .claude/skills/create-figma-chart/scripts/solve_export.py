#!/usr/bin/env python3
"""Solve a grapher embed export so the imported chart fills a template band on the first try.

Requesting the aspect you *want* is the mistake: grapher insets the drawing inside the SVG it hands
back, so the group Figma imports is smaller than the declared size — and it is the group that has to
fill the band. Ask for the aspect that *yields* the one you want instead.

The model, from `reference/FITTING.md` and Step 3:

    inset       ~= 1.4 * imFontSize on each axis
    canvas       : W * H ~= 510000 px^2   (the server renormalizes to this)
    content      : (W - 1.4F) x (H - 1.4F)
    label size   ~= 0.75 * F, then scaled by (placed width / content width)

Substituting W = 510000/H into (W - 1.4F)/(H - 1.4F) = A gives a quadratic in H:

    A*H^2 + 1.4*F*(1 - A)*H - 510000 = 0

which this solves in closed form. F is then found by bisection, because the content width that sets
the final label size itself depends on F.

Accurate to a few px, which is what the docs claim ("expect at most one correction"). Passing a
--content-aspect measured off a real import removes even that.

Usage:
    solve_export.py --band 508x371 --slug life-expectancy
    solve_export.py --band 508x371 --target-label 15 --params "country=USA~CHN&tab=chart"
    solve_export.py --band 302x220 --thumbnail --slug life-expectancy

Self-test (validates the model against the two worked examples in the docs):
    solve_export.py --self-test
"""

from __future__ import annotations

import argparse
import math
import sys

CANVAS_AREA = 510_000.0  # px^2 the server renormalizes an uncaptioned/default export to
INSET_PER_FONT = 1.4  # inset on each axis, as a multiple of imFontSize
LABEL_RATIO = 0.75  # segment values / entity names, as a multiple of the base font
MIN_LABEL = 12.0  # the floor the guidelines set for a full-size chart


def solve_canvas(content_aspect: float, font: float) -> tuple[float, float]:
    """Return the (W, H) to request so the *content* comes back at content_aspect."""
    a = content_aspect
    b = INSET_PER_FONT * font * (1.0 - a)
    disc = b * b + 4.0 * a * CANVAS_AREA
    h = (-b + math.sqrt(disc)) / (2.0 * a)
    return CANVAS_AREA / h, h


def content_box(w: float, h: float, font: float) -> tuple[float, float]:
    return w - INSET_PER_FONT * font, h - INSET_PER_FONT * font


def label_at(font: float, content_w: float, placed_w: float) -> float:
    """Final on-page label size once the export is scaled to placed_w."""
    return LABEL_RATIO * font * (placed_w / content_w)


def solve_font(content_aspect: float, placed_w: float, target_label: float) -> float:
    """Bisect for the imFontSize whose labels land on target_label at placed_w."""
    lo, hi = 8.0, 200.0
    for _ in range(200):
        mid = (lo + hi) / 2.0
        w, h = solve_canvas(content_aspect, mid)
        cw, _ = content_box(w, h, mid)
        if label_at(mid, cw, placed_w) < target_label:
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
    print("PASS" if ok else "FAIL")
    return 0 if ok else 1


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--band", help="the band to fill, in template px, e.g. 508x371")
    ap.add_argument(
        "--content-aspect",
        type=float,
        help="measured content aspect of a real import — overrides the band's own aspect. "
        "Use this for the one correction: hide connectors and year markers first, then re-read it.",
    )
    ap.add_argument("--target-label", type=float, default=13.5, help="desired final label px (default 13.5)")
    ap.add_argument("--placed-width", type=float, help="width the export is placed at (default: band width)")
    ap.add_argument("--slug", help="grapher slug, to emit a ready curl")
    ap.add_argument("--params", default="", help="extra grapher query params, e.g. 'country=USA~CHN'")
    ap.add_argument(
        "--thumbnail",
        action="store_true",
        help="302-wide route: imType=thumbnail takes the size outright (staticBounds = imWidth/4 x "
        "imHeight/4), so no aspect solve and no rescale. See SMALL-CHARTS.md.",
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

    placed_w = args.placed_width or bw

    if args.thumbnail:
        print("thumbnail route — request the pixel size directly, no solve needed:")
        print(f"  imWidth={int(round(bw * 4))}&imHeight={int(round(bh * 4))}   (staticBounds = imWidth/4 x imHeight/4)")
        print(f"  imFontSize: aim for {args.target_label:.0f} at 0.75x base -> ~{args.target_label / LABEL_RATIO:.0f}")
        if args.slug:
            p = f"{args.params}&" if args.params else ""
            print(
                f'\ncurl -sL "https://ourworldindata.org/grapher/{args.slug}.svg'
                f"?{p}imType=thumbnail&imWidth={int(round(bw * 4))}&imHeight={int(round(bh * 4))}"
                f'&imFontSize={args.target_label / LABEL_RATIO:.0f}&nocache" -o thumb.svg'
            )
        return 0

    aspect = args.content_aspect if args.content_aspect else bw / bh
    font = solve_font(aspect, placed_w, args.target_label)
    w, h = solve_canvas(aspect, font)
    cw, ch = content_box(w, h, font)
    label = label_at(font, cw, placed_w)
    im_w = int(round(w / h * 1000))

    src = "measured content aspect" if args.content_aspect else "band aspect"
    print(f"band            {bw:g} x {bh:g}   (aspect {bw / bh:.4f})")
    print(f"solving for     {aspect:.4f}  [{src}]")
    print(f"placed at       {placed_w:g}px wide")
    print()
    print(f"imFontSize      {font:.0f}")
    print(f"imWidth/Height  {im_w}/1000        (aspect ratio only — the server renormalizes)")
    print(f"declared        {w:.0f} x {h:.0f}   ({w * h:,.0f} px^2)")
    print(f"content         {cw:.1f} x {ch:.1f}   (aspect {cw / ch:.4f})")
    print(f"scale into band {placed_w / cw:.4f}")
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
        "\nHide connectors and year markers BEFORE measuring the group — they extend past the plot,"
        "\nso hiding them narrows it and makes it relatively taller. Re-run with --content-aspect"
        "\nfrom the group you are actually going to fit."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
