"""Audit a chart's colors for color-vision deficiency and label contrast.

Usage:
    python3 color_audit.py '#bc8e5a,#883039,#6d3e91,#d73c50,#4c6a9c,#585c64'
    python3 color_audit.py '#bc8e5a,#883039' --names Poultry,Beef
    python3 color_audit.py '#bc8e5a,...' --suggest        # search the OWID palette for a safer set

Colors are given in stack/legend order, so adjacent pairs are the ones that touch.
Run it on every chart before proposing it — eyeballing does not catch a dE of 9.
"""

import argparse
import itertools
import math

# The design team's Chart Colors library, read off the cheat-sheet swatches in the DI Guidelines
# file (8gxqkVmZ9x3MK3ky5oigrJ) and verified to match OwidDistinctColors in owid-grapher's
# CustomSchemes.ts. The Figma library is the source of truth; re-read it if the two ever diverge.
PALETTE = {
    "Purple": "#6d3e91", "DarkOrange": "#c05917", "LightTeal": "#58ac8c", "Blue": "#286bbb",
    "Maroon": "#883039", "Camel": "#bc8e5a", "MidnightBlue": "#00295b", "DustyCoral": "#c15065",
    "DarkOliveGreen": "#18470f", "DarkCopper": "#9a5129", "Peach": "#e56e5a", "Mauve": "#a2559c",
    "Turquoise": "#38aaba", "OliveGreen": "#578145", "Cherry": "#970046", "Teal": "#00847e",
    "RustyOrange": "#b13507", "Denim": "#4c6a9c", "Fuchsia": "#cf0a66", "TealishGreen": "#00875e",
    "Copper": "#b16214", "DarkMauve": "#8c4569", "Lime": "#3b8e1d", "Coral": "#d73c50",
    "Gray": "#6e7581",
}

# The library's "Categorical Maps" group — muted fills for choropleths, NOT interchangeable with
# the Default Palette. See the add-provider-regions skill for how map colors get agreed.
CATEGORICAL_MAPS = {
    "Sand": "#c3a27c", "LightSand": "#d8c0a2", "Taupe": "#b9b2a6", "Olive": "#5b6d35",
    "LeafGreen": "#6fa54f", "Mustard": "#d9bc54", "Tomato": "#d94c3f", "Lavendar": "#8e97c7",
    "SoftPurple": "#77538f", "MutedTeal": "#238a84", "LightTeal": "#4fb2ac",
    "MutedCherry": "#b04e74", "LightCherry": "#cb7fa0", "MutedDenim": "#526f9b",
}

# The library's "Line and Slope Charts" group: darkened variants for thin marks and text on
# white. Six colors differ from the fill palette; the rest are shared.
LINE_VARIANTS = {
    "Camel": "#996d39", "LightTeal": "#2c8465", "Turquoise": "#008291",
    "Lime": "#338711", "Peach": "#c4523e", "DarkOrange": "#be5915",
}

# Deuteranopia and protanopia are the common ones (~8% of men between them); tritanopia is
# vanishingly rare, so it is reported but never allowed to drive a recommendation.
COMMON = ["normal", "deuteranopia", "protanopia"]
ALL_KINDS = COMMON + ["tritanopia"]

TOO_CLOSE, TIGHT = 20.0, 30.0


def srgb(h):
    h = h.lstrip("#")
    return tuple(int(h[i : i + 2], 16) / 255 for i in (0, 2, 4))


def _lin(c):
    return c / 12.92 if c <= 0.04045 else ((c + 0.055) / 1.055) ** 2.4


def _delin(c):
    c = max(0.0, min(1.0, c))
    return 12.92 * c if c <= 0.0031308 else 1.055 * c ** (1 / 2.4) - 0.055


def _to_lms(rgb):
    r, g, b = (_lin(c) for c in rgb)
    return (
        17.8824 * r + 43.5161 * g + 4.11935 * b,
        3.45565 * r + 27.1554 * g + 3.86714 * b,
        0.0299566 * r + 0.184309 * g + 1.46709 * b,
    )


def _from_lms(long, med, short):
    return tuple(
        _delin(x)
        for x in (
            0.0809444479 * long - 0.130504409 * med + 0.116721066 * short,
            -0.0102485335 * long + 0.0540193266 * med - 0.113614708 * short,
            -0.000365296938 * long - 0.00412161469 * med + 0.693511405 * short,
        )
    )


_SIM = {
    "deuteranopia": lambda long, med, short: (long, 0.494207 * long + 1.24827 * short, short),
    "protanopia": lambda long, med, short: (2.02344 * med - 2.52581 * short, med, short),
    "tritanopia": lambda long, med, short: (long, med, -0.395913 * long + 0.801109 * med),
}


def simulate(hexcol, kind):
    """The color as seen with the given deficiency (Viénot-style LMS projection)."""
    if kind == "normal":
        return srgb(hexcol)
    return _from_lms(*_SIM[kind](*_to_lms(srgb(hexcol))))


def to_lab(rgb):
    r, g, b = (_lin(c) for c in rgb)
    x = (0.4124 * r + 0.3576 * g + 0.1805 * b) / 0.95047
    y = 0.2126 * r + 0.7152 * g + 0.0722 * b
    z = (0.0193 * r + 0.1192 * g + 0.9505 * b) / 1.08883
    def f(t):
        return t ** (1 / 3) if t > 0.008856 else 7.787 * t + 16 / 116

    fx, fy, fz = f(x), f(y), f(z)
    return (116 * fy - 16, 500 * (fx - fy), 200 * (fy - fz))


def delta_e(a, b):
    return math.sqrt(sum((x - y) ** 2 for x, y in zip(to_lab(a), to_lab(b))))


def luminance(rgb):
    r, g, b = (_lin(c) for c in rgb)
    return 0.2126 * r + 0.7152 * g + 0.0722 * b


def contrast(h1, h2):
    a, b = luminance(srgb(h1)), luminance(srgb(h2))
    a, b = max(a, b), min(a, b)
    return (a + 0.05) / (b + 0.05)


def min_delta_e(hexes, kinds=COMMON):
    worst = math.inf
    for kind in kinds:
        sims = [simulate(h, kind) for h in hexes]
        for i, j in itertools.combinations(range(len(hexes)), 2):
            worst = min(worst, delta_e(sims[i], sims[j]))
    return worst


def audit(hexes, names):
    print("=== label contrast on each fill (WCAG: 4.5 normal text, 3.0 large/bold) ===")
    for name, h in zip(names, hexes):
        cw, cb = contrast("#ffffff", h), contrast("#000000", h)
        print(f"  {name:<18} {h}  white {cw:4.2f}  black {cb:4.2f}  -> use {'white' if cw >= cb else 'black'}")

    print("\n=== pairs, worst first (dE < 20 fails, 20-30 is tight) ===")
    failures = []
    for kind in ALL_KINDS:
        sims = [simulate(h, kind) for h in hexes]
        pairs = sorted(
            ((delta_e(sims[i], sims[j]), names[i], names[j], abs(i - j) == 1)
             for i, j in itertools.combinations(range(len(hexes)), 2)),
        )
        rare = "  (rare - do not repaint for this alone)" if kind == "tritanopia" else ""
        print(f"  {kind}:{rare}")
        for d, a, b, adjacent in pairs[:3]:
            flag = " FAILS" if d < TOO_CLOSE else (" tight" if d < TIGHT else "")
            touch = " [adjacent in the stack]" if adjacent else ""
            print(f"    {a:<18} vs {b:<18} dE {d:5.1f}{flag}{touch}")
            if d < TOO_CLOSE and kind in COMMON:
                failures.append((d, a, b, kind))

    score = min_delta_e(hexes)
    print(f"\n  overall: min dE {score:.1f} across normal/deuteranopia/protanopia")
    return failures, score


def suggest(hexes, names, fixed_idx=()):
    """Search the palette for a safer set, keeping the colors at fixed_idx."""
    fixed = {i: hexes[i] for i in fixed_idx}
    free = [i for i in range(len(hexes)) if i not in fixed]
    print(f"\n=== searching the OWID palette for {len(free)} replacement(s) "
          f"(keeping {', '.join(names[i] for i in fixed_idx) or 'nothing'}) ===")
    results = []
    for combo in itertools.permutations(PALETTE, len(free)):
        trial = list(hexes)
        for slot, cname in zip(free, combo):
            trial[slot] = PALETTE[cname]
        if len(set(trial)) != len(trial):
            continue
        results.append((min_delta_e(trial), combo))
    results.sort(reverse=True)
    seen, shown = set(), 0
    for score, combo in results:
        key = frozenset(combo)
        if key in seen:
            continue
        seen.add(key)
        print(f"  min dE {score:5.1f}")
        for slot, cname in zip(free, combo):
            h = PALETTE[cname]
            label = "white" if contrast("#ffffff", h) >= contrast("#000000", h) else "black"
            print(f"     {names[slot]:<18} -> {cname:<15} {h}   label: {label}")
        shown += 1
        if shown >= 4:
            break
    print("\n  Colors live in the chart, not the image — hand these to whoever owns it.")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("colors", help="comma-separated hex colors, in stack/legend order")
    ap.add_argument("--names", help="comma-separated category names, same order")
    ap.add_argument("--suggest", action="store_true", help="search the palette for a safer set")
    ap.add_argument("--maps", action="store_true",
                    help="search the Categorical Maps group instead (choropleth fills)")
    ap.add_argument("--line", action="store_true",
                    help="use the Line and Slope Charts variants (thin marks and text on white)")
    ap.add_argument("--keep", default="", help="indices (0-based) to hold fixed when suggesting")
    args = ap.parse_args()

    hexes = [c.strip() for c in args.colors.split(",") if c.strip()]
    names = ([n.strip() for n in args.names.split(",")] if args.names
             else [f"series {i + 1}" for i in range(len(hexes))])
    if len(names) != len(hexes):
        ap.error("--names must have the same number of entries as colors")

    if args.maps:
        PALETTE.clear()
        PALETTE.update(CATEGORICAL_MAPS)
    if args.line:
        PALETTE.update(LINE_VARIANTS)

    failures, score = audit(hexes, names)
    if args.suggest:
        keep = tuple(int(i) for i in args.keep.split(",") if i.strip() != "")
        suggest(hexes, names, keep)
    elif failures:
        print("\n  Failing pairs found — rerun with --suggest (and --keep for the colors that")
        print("  carry meaning) to search for a safer set. Note that swapping one color often")
        print("  does not help: failures are usually independent, so the floor barely moves.")


if __name__ == "__main__":
    main()
