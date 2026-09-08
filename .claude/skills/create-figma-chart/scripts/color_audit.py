"""Audit a chart's colors for color-vision deficiency and label contrast.

Usage (run from the repo root, with the repo interpreter):
    .venv/bin/python color_audit.py '#bc8e5a,#883039,#6d3e91,#d73c50,#4c6a9c,#585c64'
    .venv/bin/python color_audit.py '#bc8e5a,#883039' --names Poultry,Beef
    .venv/bin/python color_audit.py '#bc8e5a,...' --suggest   # search the OWID palette for a safer set
    .venv/bin/python color_audit.py '#bc8e5a,...' --separated # bars/lines/maps: no fills touch

Colors are given in stack/legend order. Whether consecutive entries *touch* depends on the chart,
and the grayscale seam check only applies where they do:

    stacked / segmented fills      consecutive entries share an edge   -> seam gates (the default)
    separate bars, lines, maps     nothing shares an edge              -> pass --separated

Only a stacked or segmented chart has seams. A plain or grouped bar chart draws each fill against
the background, so legend order says nothing about adjacency and gating on it would reject good
palettes for an arbitrary reason: pass --separated there. --line and --maps imply it.
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

# Two fills that touch must also survive being printed in black and white. Below this ratio the
# seam between them disappears: Denim beside Gray measures 1.18:1.
GRAYSCALE_MIN = 1.6

# How many candidate palettes the search prints.
SHOW_BEST = 4


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


_LUM_CACHE = {}


def _lum(hexcol):
    """Relative luminance of a hex color, memoized — the seam check calls this in a hot loop."""
    if hexcol not in _LUM_CACHE:
        _LUM_CACHE[hexcol] = luminance(srgb(hexcol))
    return _LUM_CACHE[hexcol]


def contrast(h1, h2):
    a, b = _lum(h1), _lum(h2)
    a, b = max(a, b), min(a, b)
    return (a + 0.05) / (b + 0.05)


def min_seam(hexes):
    """The weakest grayscale seam between any two *touching* fills.

    Unlike the dE floor this depends on the order the colors are laid out in, so the search has to
    evaluate an arrangement, not just a set. inf when there is nothing to touch.
    """
    if len(hexes) < 2:
        return math.inf
    return min(contrast(hexes[i], hexes[i + 1]) for i in range(len(hexes) - 1))


def hue_family(hexcol):
    """Which 60-degree hue sector a color sits in; None if it is too gray to have a hue."""
    _, a, b = to_lab(srgb(hexcol))
    if math.hypot(a, b) < 12:
        return None
    return int(((math.degrees(math.atan2(b, a)) + 360) % 360) // 60)


_LAB_CACHE = {}


def sim_lab(hexcol, kind):
    """Lab coordinates of a color as seen with the given deficiency, memoized.

    The search scores hundreds of thousands of candidate palettes drawn from the same 25 colors,
    so converting each (color, kind) pair once instead of per candidate is most of the speedup.
    """
    key = (hexcol, kind)
    if key not in _LAB_CACHE:
        _LAB_CACHE[key] = to_lab(simulate(hexcol, kind))
    return _LAB_CACHE[key]


def min_delta_e(hexes, kinds=COMMON):
    """The closest any two colors come, across the given deficiencies.

    Depends only on the *set* of colors, not their order — every pair is compared. That is what
    lets the search enumerate combinations rather than permutations.
    """
    worst = math.inf
    for kind in kinds:
        labs = [sim_lab(h, kind) for h in hexes]
        for a, b in itertools.combinations(labs, 2):
            d = math.dist(a, b)
            if d < worst:
                worst = d
    return worst


def audit(hexes, names, adjacent_fills=True):
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
            touch = " [adjacent in the stack]" if adjacent and adjacent_fills else ""
            print(f"    {a:<18} vs {b:<18} dE {d:5.1f}{flag}{touch}")
            if d < TOO_CLOSE and kind in COMMON:
                failures.append((d, a, b, kind))

    gray_failures = []
    if adjacent_fills:
        print(f"\n=== grayscale seam between touching fills (needs {GRAYSCALE_MIN}:1) ===")
        for i in range(len(hexes) - 1):
            ratio = contrast(hexes[i], hexes[i + 1])
            flag = " MERGES IN PRINT" if ratio < GRAYSCALE_MIN else ""
            print(f"  {names[i]:<18} | {names[i + 1]:<18} {ratio:4.2f}:1{flag}")
            if ratio < GRAYSCALE_MIN:
                gray_failures.append((ratio, names[i], names[i + 1]))
    else:
        # Lines cross and map neighbors are geographic, so legend order says nothing about which
        # fills meet. Report the closest pairs in print and leave the judgement to the reader
        # instead of failing arbitrary consecutive pairs.
        print("\n=== grayscale separation, closest pairs (informational — no seam gate) ===")
        pairs = sorted(
            (contrast(hexes[i], hexes[j]), names[i], names[j])
            for i, j in itertools.combinations(range(len(hexes)), 2)
        )
        for ratio, a, b in pairs[:3]:
            flag = " close in print" if ratio < GRAYSCALE_MIN else ""
            print(f"  {a:<18} | {b:<18} {ratio:4.2f}:1{flag}")
        print(f"  which marks meet is not knowable from legend order here, so the {GRAYSCALE_MIN}:1 "
              f"seam gate is not applied — for lines lean on direct labeling, for maps check the "
              f"neighbors that actually share a border.")

    score = min_delta_e(hexes)
    print(f"\n  overall: min dE {score:.1f} across normal/deuteranopia/protanopia")
    if gray_failures:
        worst = min(gray_failures)
        print(f"  grayscale: {worst[1]} and {worst[2]} touch at {worst[0]:.2f}:1 — "
              f"reorder the stack or move one color")
    return failures, score


def _assign(combo, hexes, free, seams):
    """Best arrangement of one candidate set, or None if no arrangement is usable.

    The dE floor and the hue-family count are the same whatever order the set is laid out in, but
    the grayscale seam is not — it only compares touching fills. So an arrangement, not just a set,
    is what has to clear GRAYSCALE_MIN, and a set with no clearing arrangement is rejected outright
    rather than recommended into a palette that fails the check the audit then demands.

    `seams` lists only the seams the search can actually influence (at least one end is being
    replaced). A seam between two kept colors is not this function's to fix — judging candidates on
    it would reject every one of them and report nothing useful.

    Among the arrangements that clear it, pick the one closest to the colors already in use: a
    designer reads a small shift as a fix and a wholesale repaint as a different chart.
    """
    best = None
    for perm in itertools.permutations(combo):
        trial = list(hexes)
        for slot, cname in zip(free, perm):
            trial[slot] = PALETTE[cname]
        seam = min((contrast(trial[i], trial[i + 1]) for i in seams), default=math.inf)
        if seam < GRAYSCALE_MIN:
            continue
        drift = sum(
            math.dist(sim_lab(hexes[slot], "normal"), sim_lab(PALETTE[cname], "normal"))
            for slot, cname in zip(free, perm)
        )
        if best is None or drift < best[1]:
            best = (perm, drift, seam)
    return best


def suggest(hexes, names, fixed_idx=(), adjacent_fills=True):
    """Search the palette for a safer set, keeping the colors at fixed_idx."""
    fixed = {i: hexes[i] for i in fixed_idx}
    free = [i for i in range(len(hexes)) if i not in fixed]
    print(f"\n=== searching the OWID palette for {len(free)} replacement(s) "
          f"(keeping {', '.join(names[i] for i in fixed_idx) or 'nothing'}) ===")
    # A seam between two kept colors is outside the search's reach: no choice of replacement can
    # separate them. Say so rather than rejecting every candidate over something it cannot fix.
    # With no adjacency to speak of (--line, --maps) there is no seam to constrain at all: gating on
    # legend order there rejected every candidate and returned nothing usable.
    seams = []
    if adjacent_fills:
        seams = [i for i in range(len(hexes) - 1) if i not in fixed or (i + 1) not in fixed]
        for i in range(len(hexes) - 1):
            if i in seams:
                continue
            ratio = contrast(hexes[i], hexes[i + 1])
            if ratio < GRAYSCALE_MIN:
                print(f"  note: {names[i]} and {names[i + 1]} are both kept and touch at "
                      f"{ratio:.2f}:1 — the search cannot fix that seam; free one of them or "
                      f"reorder the stack.")
    # Combinations, not permutations. min_delta_e and the hue-family count both depend only on the
    # *set* of colors, so the len(free)! arrangements of one set all score identically. Enumerating
    # permutations meant 25P6 = 127.5M candidates for a six-category chart (hours, and every
    # passing one held in memory); the 25C6 = 177k sets are the real search space.
    candidates = []
    for combo in itertools.combinations(PALETTE, len(free)):
        trial = list(hexes)
        for slot, cname in zip(free, combo):
            trial[slot] = PALETTE[cname]
        if len(set(trial)) != len(trial):
            continue
        score = min_delta_e(trial)
        if score < TOO_CLOSE:
            continue                      # not worth showing a set that still fails
        families = len({hue_family(h) for h in trial} - {None})
        # Rank by hue variety FIRST: ranking on safety alone returns sets that are all blues and
        # greens (safe, but a reader can no longer tell six categories apart at a glance).
        candidates.append((-families, -score, combo))
    if not candidates:
        print("  nothing in the palette clears dE 20 for this many categories — "
              "consider merging categories instead.")
        return
    candidates.sort()

    # Drift only ever separates candidates that tie on hue variety AND safety, so work through the
    # tied groups in ranked order and arrange every member of a group before looking at the next.
    # Pruning to a fixed number of candidates *before* this point would decide the drift ordering by
    # whatever came first, which is how a set with drift 31.3 lost to one with 49.6.
    finalists, rejected, examined = [], 0, 0
    for _, group in itertools.groupby(candidates, key=lambda c: (c[0], c[1])):
        for neg_families, neg_score, combo in group:
            examined += 1
            best = _assign(combo, hexes, free, seams)
            if best is None:
                rejected += 1                # no arrangement keeps every seam above GRAYSCALE_MIN
                continue
            perm, drift, seam = best
            finalists.append((neg_families, neg_score, drift, perm, seam))
        # Groups descend in (hue variety, safety), so once this group is finished and we have enough
        # finalists, nothing later can displace them.
        if len(finalists) >= SHOW_BEST:
            break
    # Only the top-ranked groups get arranged, so scope the rejection count to what was examined
    # rather than implying it is a tally over all the candidates that cleared dE.
    note = (f"; {rejected} of the {examined} best-ranked rejected for a grayscale seam under "
            f"{GRAYSCALE_MIN}:1" if rejected else "")
    if not finalists:
        print(f"  {len(candidates)} palette(s) clear dE {TOO_CLOSE:.0f}, but none survives the "
              f"grayscale seam check{note} — reorder the stack, or merge categories.")
        return
    finalists.sort()
    print(f"  {len(candidates)} palette(s) clear dE {TOO_CLOSE:.0f}{note}; showing the best "
          f"{SHOW_BEST}, closest to the current colors first")
    for neg_families, neg_score, drift, perm, seam in finalists[:SHOW_BEST]:
        shown_seam = f"{seam:4.2f}:1" if math.isfinite(seam) else "    n/a"
        print(f"  hue families {-neg_families}/6   min dE {-neg_score:5.1f}   "
              f"seam {shown_seam}   drift {drift:5.1f}")
        for slot, cname in zip(free, perm):
            h = PALETTE[cname]
            label = "white" if contrast("#ffffff", h) >= contrast("#000000", h) else "black"
            print(f"     {names[slot]:<18} -> {cname:<15} {h}   label: {label}")
    print("\n  Colors live in the chart, not the image — hand these to whoever owns it.")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("colors", help="comma-separated hex colors, in stack/legend order")
    ap.add_argument("--names", help="comma-separated category names, same order")
    ap.add_argument("--suggest", action="store_true", help="search the palette for a safer set")
    ap.add_argument("--maps", action="store_true",
                    help="search the Categorical Maps group instead (categorical choropleth "
                         "fills only — not a sequential Viridis/ColorBrewer ramp, which is "
                         "ordered and set in grapher)")
    ap.add_argument("--line", action="store_true",
                    help="use the Line and Slope Charts variants (thin marks and text on white)")
    ap.add_argument("--separated", action="store_true",
                    help="no fills touch (plain or grouped bars, lines, maps): report the "
                         "grayscale seam but never gate on it")
    ap.add_argument("--keep", default="",
                    help="indices (0-based, within range, no repeats) to hold fixed when suggesting")
    args = ap.parse_args()

    hexes = [c.strip() for c in args.colors.split(",") if c.strip()]
    names = ([n.strip() for n in args.names.split(",")] if args.names
             else [f"series {i + 1}" for i in range(len(hexes))])
    if len(names) != len(hexes):
        ap.error("--names must have the same number of entries as colors")

    # Parse and validate --keep before any work: a negative index is legal Python but nonsense here —
    # `free` only holds nonnegative positions, so -1 would report the last color as kept while
    # replacing it in every recommendation, and an out-of-range one raises IndexError inside the search.
    try:
        keep = tuple(int(i) for i in args.keep.split(",") if i.strip() != "")
    except ValueError:
        ap.error("--keep takes comma-separated integers, e.g. --keep 4,5")
    if bad := [i for i in keep if not 0 <= i < len(hexes)]:
        ap.error(f"--keep index out of range: {bad} (valid: 0..{len(hexes) - 1})")
    if len(set(keep)) != len(keep):
        ap.error(f"--keep has repeated indices: {args.keep}")

    if args.maps:
        PALETTE.clear()
        PALETTE.update(CATEGORICAL_MAPS)
    if args.line:
        PALETTE.update(LINE_VARIANTS)

    # Only a stacked or segmented chart lays its fills out edge to edge in the order given, so only
    # there does "adjacent in the list" mean "these two touch". Everything else — plain and grouped
    # bars as much as lines and maps — draws each fill against the background.
    adjacent_fills = not (args.separated or args.line or args.maps)
    print(f"Assuming fills {'touch in the order given' if adjacent_fills else 'do not touch'}"
          f" — {'stacked/segmented' if adjacent_fills else 'separated'}."
          + ("  Pass --separated for a plain or grouped bar chart, a line chart or a map."
             if adjacent_fills else ""))

    failures, score = audit(hexes, names, adjacent_fills)
    if args.suggest:
        suggest(hexes, names, keep, adjacent_fills)
    elif failures:
        print("\n  Failing pairs found — rerun with --suggest (and --keep for the colors that")
        print("  carry meaning) to search for a safer set. Note that swapping one color often")
        print("  does not help: failures are usually independent, so the floor barely moves.")


if __name__ == "__main__":
    main()
