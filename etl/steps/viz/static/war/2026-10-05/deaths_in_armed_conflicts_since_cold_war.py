"""Deaths in armed conflicts since the end of the Cold War, by world region and conflict type.

Static chart for the article "Millions have died in conflicts since the Cold War; most of them in Africa
and intrastate conflicts" (ourworldindata.org/conflict-deaths-breakdown). It shows the total number of
conflict deaths between 1989 and the latest year, as three bars that each sum to the same total:

1. all armed conflicts,
2. split by the world region where the deaths occurred (UCDP's regions),
3. split by conflict type (intrastate, one-sided violence, non-state, interstate).

Refresh of the 1989-2024 version (Charts (2025) Figma file `l8AusQOZfgaxJ0wVcMrTry`, page
"20250724 Conflict deaths since the Cold War (Bastian)", frame `13530:9`, 1258 x 1119). The scope is a
data update only, so the layout reproduces that frame: its size, its slot positions and its segment
colors are copied from it (positions read with `get_metadata`, colors sampled from the published PNG).
Positions below are in that frame's pixels, y measured from the top.

Data: the cumulative deaths come from `garden/war/2026-06-10/ucdp_cumulative`, the dataset behind the
grapher charts `cumulative-deaths-in-armed-conflicts*`, so the static and interactive charts show the same
numbers. It sums UCDP's yearly best estimates of deaths in ongoing conflicts from 1989 to the latest year.

Rounding follows the 2025 version: one decimal in millions at or above a million ("2.1m"), otherwise two
significant figures in thousands ("750k").
"""

import logging
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties, findfont
from matplotlib.patches import Rectangle
from matplotlib.textpath import TextPath

from etl.helpers import PathFinder
from etl.viz.static import apply_svg_rcparams, export_frame

paths = PathFinder(__file__)
apply_svg_rcparams()

# Fonts: emit Lato (what the template uses) and measure/draw with what is installed. See
# `.claude/skills/create-static-viz/reference/WRITING-THE-STEP.md`.
EMITTED_FONT_STACK = ["Lato", "Arial", "Helvetica", "Liberation Sans", "sans-serif"]
MEASURED_FONT_STACK = ["Arial", "Helvetica", "Liberation Sans", "DejaVu Sans", "sans-serif"]
matplotlib.rcParams["font.family"] = "sans-serif"
matplotlib.rcParams["font.sans-serif"] = EMITTED_FONT_STACK
_OPTIONAL_FACES = tuple({*EMITTED_FONT_STACK, *MEASURED_FONT_STACK})
logging.getLogger("matplotlib.font_manager").addFilter(
    lambda record: (
        "Falling back" in record.getMessage()
        or not any(f"Font family '{face}' not found" in record.getMessage() for face in _OPTIONAL_FACES)
    )
)
_DRAWN_FACE = findfont(FontProperties(family=EMITTED_FONT_STACK))
_MEASURED_FACE = findfont(FontProperties(family=MEASURED_FONT_STACK))
assert _DRAWN_FACE == _MEASURED_FACE, f"draws {Path(_DRAWN_FACE).name}, measures {Path(_MEASURED_FACE).name}"

# First year of UCDP's Georeferenced Event Dataset, and the start of the period the chart covers.
FIRST_YEAR = 1989

# Frame of the 2025 version.
FRAME_W, FRAME_H = 1258, 1119
MARGIN = 24
PX_PER_INCH = 100
PT_PER_PX = 0.72

# Bars: left edge, full width (each bar sums to the same total), and height.
BAR_X0, BAR_W, BAR_H = 54, 1104, 127
BAR_TOPS = {"total": 226, "region": 512, "type": 832}
# Thin vertical rule left of each bar, overhanging it by this much at both ends.
RULE_X, RULE_OVERHANG = 53, 15
# Value labels: padding inside a segment; the first label of a bar carries the unit.
VALUE_PAD_FIRST, VALUE_PAD = 24, 18

# Text sizes in frame px, read off the 2025 frame's text nodes with `use_figma` (font sizes, not node heights).
SIZE = {
    "title": 40,
    "subtitle": 22,
    "section": 26,
    "section_sub": 22,
    "legend": 22,
    "value": 26,
    "footer": 14,
}

TITLE_COLOR = "#2d2e2d"
TEXT_COLOR = "#5b5b5b"
FOOTER_COLOR = "#858585"
RULE_COLOR = "#2d2e2d"
VALUE_COLOR = "#ffffff"

TITLE = "Deaths in armed conflicts since the end of the Cold War"
TAGLINE = "OurWorldinData.org — Research and data to make progress against the world's largest problems."
AUTHORS = ["Bastian Herre", "Klara Auerbach"]
NOTE = (
    "An armed conflict is defined here as a disagreement between organized groups, or between one organized "
    "group and civilians, that causes at least 25 deaths during a year."
)

# Segments, largest first (the step warns when the data no longer ranks them this way, see `run`).
# (key in the data, label, color sampled from the 2025 PNG, legend x in frame px)
REGIONS = [
    ("Africa (UCDP)", "Africa", "#ad7bad", 53),
    ("Middle East (UCDP)", "Middle East", "#c4a681", 176),
    ("Asia and Oceania (UCDP)", "Asia & Oceania", "#619b98", 356),
    ("Europe (UCDP)", "Europe", "#7588ad", 569),
    ("Americas (UCDP)", "Americas", "#de9080", 703),
]
# (column in the data, bold term, definition, color, legend x, legend text width)
TYPES = [
    (
        "intrastate_deaths",
        "Intrastate conflicts:",
        "conflicts between a state and a non-state armed group.",
        "#985e62",
        55,
        215,
    ),
    (
        "onesided_deaths",
        "One-sided violence:",
        "use of force by a state or non-state armed group against civilians.",
        "#b88454",
        322,
        235,
    ),
    # Interstate before non-state since the 1989-2025 update, when interstate deaths overtook non-state deaths.
    ("interstate_deaths", "Interstate conflicts:", "conflicts between states.", "#7588ad", 605, 223),
    ("nonstate_deaths", "Non-state conflicts:", "conflicts between non-state armed groups.", "#b6654a", 876, 223),
]
TOTAL_COLOR = "#d26d76"

# Section headings (top y of the text node) and legends.
SECTIONS = {
    "total": (169, "Total deaths from all armed conflicts", None),
    "region": (
        382,
        "Deaths in armed conflicts by world region",
        "Deaths are based on where they occurred, not what the person’s nationality was.",
    ),
    "type": (666, "Deaths in armed conflicts by conflict type", None),
}
REGION_LEGEND_Y = 454
TYPE_LEGEND_Y = 714
CHIP = 25
CHIP_GAP = 11
LEGEND_LINE_PX = 26.4  # Lato's auto line height at 22px, as in the 2025 frame

# Header and footer.
TITLE_Y = 24
SUBTITLE_Y = 83
SUBTITLE_LINE_PX = 26
NOTE_Y = 1007
SOURCE_Y = 1033
FOOTER_LINE_PX = 18
TAGLINE_Y = 1077


def run() -> None:
    tb = paths.load_dataset("ucdp_cumulative").read("ucdp_cumulative")
    assert tb["year"].nunique() == 1, "expected one row per entity, stamped with the latest year"
    last_year = int(tb["year"].max())
    tb = tb.set_index("country")

    world = int(tb.loc["World", "all_deaths"])
    regions = [int(tb.loc[key, "all_deaths"]) for key, *_ in REGIONS]
    types = [int(tb.loc["World", column]) for column, *_ in TYPES]
    paths.log.info(f"Cumulative deaths {FIRST_YEAR}-{last_year}: world {world}, regions {regions}, types {types}")

    # Each bar must sum to the total.
    assert sum(regions) == world, f"regions sum to {sum(regions)}, world total is {world}"
    assert sum(types) == world, f"conflict types sum to {sum(types)}, world total is {world}"

    # Segments are ordered by size, largest first. The order is fixed here (legend positions depend on it),
    # so say when the data no longer ranks them this way, to revisit it at the next update.
    for name, values in [("region", regions), ("type", types)]:
        if values != sorted(values, reverse=True):
            paths.log.warning(f"The {name} segments are no longer in descending order: {values}")

    citation = source_citation(tb, last_year)
    fig = create_visualization(world, regions, types, last_year, citation)
    export_frame(paths, fig, paths.short_name)
    plt.close(fig)


# Source line in the format of the 2025 version: full author names and page ranges, which the origins do not carry.
# The yearly UCDP article ("Organized violence 1989-<year>") changes with every release; `source_citation` checks it.
SOURCE = (
    "Sundberg, Ralph, and Erik Melander, 2013, Introducing the UCDP Georeferenced Event Dataset. Journal of Peace "
    "Research 50(4): 523-532; Davies, Shawn, Therése Pettersson, and Magnus Öberg. 2026. Organized violence "
    "1989-2025, and violent political protests. Journal of Peace Research 63(4): 705-723."
)


def source_citation(tb, last_year: int) -> str:
    """Return SOURCE, after checking it cites the UCDP article for the data's own release."""
    citations = " ".join(
        origin.citation_full
        for origin in tb["all_deaths"].metadata.origins
        if origin.producer == "Uppsala Conflict Data Program"
    )
    assert citations, "no UCDP origin found on the plotted indicator"
    for text, name in [(citations, "the origin's citation"), (SOURCE, "SOURCE")]:
        assert f"{FIRST_YEAR}–{last_year}" in text.replace("-", "–"), (
            f"{name} does not cite the UCDP article for {FIRST_YEAR}-{last_year}: update SOURCE to the new release"
        )
    return SOURCE


def format_deaths(value: int) -> str:
    """'2.1m' at or above a million, otherwise two significant figures in thousands ('750k')."""
    if value >= 1_000_000:
        return f"{value / 1_000_000:.1f}m"
    digits = len(str(value)) - 2
    return f"{round(value, -digits) // 1000:.0f}k"


# ---------------------------------------------------------------------------
# Drawing
# ---------------------------------------------------------------------------


def create_visualization(world: int, regions: list[int], types: list[int], last_year: int, citation: str):
    fig = plt.figure(figsize=(FRAME_W / PX_PER_INCH, FRAME_H / PX_PER_INCH))
    fig.patch.set_facecolor("white")  # the PNG stays legible; the SVG is saved transparent

    # Header.
    text(fig, MARGIN, TITLE_Y, 48, TITLE, "title", color=TITLE_COLOR, gid="title")
    subtitle = [
        "Number of combatants and civilians who died due to fighting in armed conflicts between "
        f"{FIRST_YEAR} and {last_year}.",
        "This excludes deaths due to disease and starvation, which can make the death tolls much larger.",
    ]
    for i, line in enumerate(subtitle):
        top = SUBTITLE_Y + i * SUBTITLE_LINE_PX
        text(fig, MARGIN, top, SUBTITLE_LINE_PX, line, "subtitle", color=TEXT_COLOR, gid=f"subtitle-{i}")

    # Section headings.
    for key, (top, heading, sub) in SECTIONS.items():
        text(fig, 53, top, 30, heading, "section", color=TITLE_COLOR, bold=True, gid=f"section__{key}")
        if sub:
            text(fig, 53, top + 30, 30, sub, "section_sub", color=TITLE_COLOR, gid=f"section__{key}-sub")

    # Legends.
    for key, label, color, x in [(k, lbl, c, x) for k, lbl, c, x in REGIONS]:
        draw_chip(fig, x, REGION_LEGEND_Y, color, gid=f"legend__{slug(label)}-chip")
        text(
            fig,
            x + CHIP + CHIP_GAP,
            REGION_LEGEND_Y - 1,
            27,
            label,
            "legend",
            color=TITLE_COLOR,
            gid=f"legend__{slug(label)}",
        )
    for key, term, definition, color, x, width in TYPES:
        draw_chip(fig, x, TYPE_LEGEND_Y, color, gid=f"legend__{slug(key.removesuffix('_deaths'))}-chip")
        lines = [term] + wrap(definition, SIZE["legend"], width)
        for i, line in enumerate(lines):
            text(
                fig,
                x + CHIP + CHIP_GAP,
                TYPE_LEGEND_Y + i * LEGEND_LINE_PX,
                LEGEND_LINE_PX,
                line,
                "legend",
                color=TITLE_COLOR,
                gid=f"legend__{slug(key.removesuffix('_deaths'))}-{i}",
            )
        # As in the 2025 frame, the legend may reach the top of the rule, but not the bar itself.
        assert TYPE_LEGEND_Y + len(lines) * LEGEND_LINE_PX < BAR_TOPS["type"], f"{term} legend runs into the bar"

    # Bars.
    draw_bar(fig, "total", [("all", "World", TOTAL_COLOR, world)], world, unit_at="end")
    draw_bar(fig, "region", [(slug(lbl), lbl, c, v) for (_, lbl, c, _), v in zip(REGIONS, regions)], world)
    draw_bar(
        fig, "type", [(slug(k.removesuffix("_deaths")), k, c, v) for (k, _, _, c, _, _), v in zip(TYPES, types)], world
    )

    draw_footer(fig, citation)
    return fig


def draw_bar(fig, name: str, segments: list[tuple[str, str, str, int]], total: int, unit_at: str = "start") -> None:
    """One 100% bar: segments left to right, a value label in each, a rule on its left edge."""
    top = BAR_TOPS[name]
    rule = fig.add_artist(
        plt.Line2D(
            [fx(RULE_X)] * 2,
            [fy(top - RULE_OVERHANG), fy(top + BAR_H + RULE_OVERHANG)],
            color=RULE_COLOR,
            linewidth=1 * PT_PER_PX,
        )
    )
    rule.set_gid(f"{name}__rule")

    x = float(BAR_X0)
    for i, (key, _, color, value) in enumerate(segments):
        width = BAR_W * value / total
        fig.add_artist(
            Rectangle(
                (fx(x), fy(top + BAR_H)),
                width / FRAME_W,
                BAR_H / FRAME_H,
                facecolor=color,
                edgecolor="none",
                gid=f"bar__{name}-{key}",
            )
        )

        label = format_deaths(value) + (" deaths" if i == 0 else "")
        label_w = text_width(label, SIZE["value"])
        baseline = top + BAR_H / 2 + cap_height(SIZE["value"]) / 2
        if unit_at == "end":
            # The single-segment total bar carries its label at the right end, as in the 2025 version.
            label_x, ha, label_color = x + width - 22, "right", VALUE_COLOR
        elif label_w + 2 * VALUE_PAD <= width:  # padding on both sides
            label_x, ha, label_color = x + (VALUE_PAD_FIRST if i == 0 else VALUE_PAD), "left", VALUE_COLOR
        else:
            # Too narrow for its label: only the last segment can carry it outside, in its own color.
            assert i == len(segments) - 1, f"{name}: label {label!r} does not fit its {width:.0f}px segment"
            label_x, ha, label_color = x + width + 6, "left", color
        fig.text(
            fx(label_x),
            fy(baseline),
            label,
            fontsize=SIZE["value"] * PT_PER_PX,
            color=label_color,
            ha=ha,
            va="baseline",
            gid=f"label__{name}-{key}",
        )
        x += width

    assert abs(x - (BAR_X0 + BAR_W)) < 1e-6, f"{name} bar does not fill its width"


def draw_footer(fig, citation: str) -> None:
    """Note, source (wrapped to the content width), then the tagline and license on one row."""
    content_w = FRAME_W - 2 * MARGIN
    runs_row(fig, MARGIN, NOTE_Y, [("Note: ", True), (NOTE, False)], gid="footer__note")
    assert run_width("Note: ", True) + text_width(NOTE, SIZE["footer"]) <= content_w, "Note no longer fits one line"

    lead = "Source: "
    lines = wrap(citation, SIZE["footer"], content_w, first_indent=run_width(lead, True))
    for i, line in enumerate(lines):
        runs = [(lead, True), (line, False)] if i == 0 else [(line, False)]
        runs_row(fig, MARGIN, SOURCE_Y + i * FOOTER_LINE_PX, runs, gid=f"footer__source-{i}")
    assert SOURCE_Y + len(lines) * FOOTER_LINE_PX <= TAGLINE_Y, "Source runs into the tagline row"

    runs_row(
        fig,
        MARGIN,
        TAGLINE_Y,
        [("OurWorldinData.org ", True), (TAGLINE.split(" ", 1)[1], False)],
        gid="footer__tagline",
    )
    license_text = f"Licensed under CC-BY by the authors {' and '.join(AUTHORS)}"
    fig.text(
        fx(FRAME_W - MARGIN),
        fy(TAGLINE_Y + cap_height(SIZE["footer"])),
        license_text,
        fontsize=SIZE["footer"] * PT_PER_PX,
        color=FOOTER_COLOR,
        ha="right",
        va="baseline",
        gid="footer__license",
    )
    tagline_w = run_width("OurWorldinData.org ", True) + text_width(TAGLINE.split(" ", 1)[1], SIZE["footer"])
    assert tagline_w + 20 + text_width(license_text, SIZE["footer"]) <= content_w, "tagline and license overlap"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def fx(x_px: float) -> float:
    return x_px / FRAME_W


def fy(y_px: float) -> float:
    return 1 - y_px / FRAME_H


def prop(size_px: float, bold: bool = False) -> FontProperties:
    return FontProperties(family=MEASURED_FONT_STACK, size=size_px * PT_PER_PX, weight="bold" if bold else "normal")


def text_width(s: str, size_px: float, bold: bool = False) -> float:
    """Ink width of `s` in frame px."""
    return TextPath((0, 0), s, prop=prop(size_px, bold)).get_extents().width / PT_PER_PX


def run_width(s: str, bold: bool = False) -> float:
    """Advance of a footer run, trailing space included (a sentinel glyph keeps it from being trimmed)."""
    return text_width(s + "|", SIZE["footer"], bold) - text_width("|", SIZE["footer"], bold)


def cap_height(size_px: float) -> float:
    return TextPath((0, 0), "0", prop=prop(size_px)).get_extents().ymax / PT_PER_PX


def text(fig, x: float, top: float, line_h: float, s: str, role: str, color: str, gid: str, bold: bool = False) -> None:
    """One line of text in a slot whose top is `top`, on a baseline centred in its line box."""
    size = SIZE[role]
    baseline = top + line_h / 2 + cap_height(size) / 2
    fig.text(
        fx(x),
        fy(baseline),
        s,
        fontsize=size * PT_PER_PX,
        color=color,
        fontweight="bold" if bold else "normal",
        ha="left",
        va="baseline",
        gid=gid,
    )


def runs_row(fig, x: float, top: float, runs: list[tuple[str, bool]], gid: str) -> None:
    """A footer row of bold/regular runs, laid out by advance. No run may start with a space."""
    baseline = top + cap_height(SIZE["footer"])
    for i, (s, bold) in enumerate(runs):
        assert not s.startswith(" "), f"run {s!r} starts with a space"
        fig.text(
            fx(x),
            fy(baseline),
            s.rstrip(),
            fontsize=SIZE["footer"] * PT_PER_PX,
            color=FOOTER_COLOR,
            fontweight="bold" if bold else "normal",
            ha="left",
            va="baseline",
            gid=f"{gid}-{i}",
        )
        x += run_width(s, bold)


def wrap(s: str, size_px: float, width_px: float, first_indent: float = 0) -> list[str]:
    """Greedy wrap by measured width; the first line may be shortened by `first_indent`."""
    lines: list[str] = []
    line = ""
    for word in s.split():
        candidate = f"{line} {word}".strip()
        available = width_px - (first_indent if not lines else 0)
        if line and text_width(candidate, size_px) > available:
            lines.append(line)
            line = word
        else:
            line = candidate
    lines.append(line)
    return lines


def draw_chip(fig, x: float, top: float, color: str, gid: str) -> None:
    fig.add_artist(
        Rectangle((fx(x), fy(top + CHIP)), CHIP / FRAME_W, CHIP / FRAME_H, facecolor=color, edgecolor="none", gid=gid)
    )


def slug(s: str) -> str:
    return s.lower().replace(" & ", "-").replace(" ", "-")
