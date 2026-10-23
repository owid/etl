"""Pension entitlements as a share of GDP, by country.

A static chart of the pensions people have already earned (accrued-to-date pension entitlements in social
insurance), valued today and measured as a share of GDP, for the European countries Eurostat reports.

One stacked horizontal bar per country, ranked by its total. The total sits at the end of each bar, with the unit
spelled out on the longest one only. A one-row legend with square chips names the parts; their names take their
part's color. Three frames, each a `LAYOUTS` entry:
- `pension_entitlements_by_country`: every country on the tall template, split into the two parts Eurostat
  publishes (`PARTS_BY_FUNDING`): unfunded government schemes (social security and some schemes for government
  employees, paid from future contributions and taxes) and private and funded schemes;
- `pension_entitlements_by_country_selection`: the 14 `SELECTION` countries on the landscape template, split the
  same way;
- `pension_entitlements_by_country_scheme_type`: the same 14 countries split by scheme type
  (`PARTS_BY_SCHEME_TYPE`): state pensions (social security), employer promises (defined benefit schemes, of
  private employers and for government employees) and pension pots (defined contribution schemes). It has its own
  title and subtitle, and a shorter Note.

Data: table `pension_entitlements` of `garden/eurostat/2026-10-23/pension_entitlements` (Eurostat,
`nasa_10_pens1`), and its flags table `pension_entitlements_flags`. Derived from the data, so a new release
redraws them without hand edits:
- the year: the latest one in which at least `MIN_COUNTRIES_PER_YEAR` countries report a total. Countries
  report every three years, so a year with only a few voluntary reports is skipped (2024 has only Czechia
  in this release);
- the countries left out of the full frame because they publish their two parts but no total (Romania in
  2021), named in the Note;
- the Note's flag sentences, from Eurostat's flags on the published shares of the countries shown ("p"
  provisional, "d" different definition, "e" estimated, "b" break in series), and the discount rate after
  inflation that Eurostat recommends for government schemes in that year (`real_discount_rate`, from Eurostat's
  metadata).

Render:  .venv/bin/etlr viz://static/eurostat/2026-10-23/pension_entitlements_by_country
Verify:  .venv/bin/python .claude/skills/create-static-viz/scripts/verify_static_viz.py \
             etl/steps/viz/static/eurostat/2026-10-23/pension_entitlements_by_country \
             --template vertical --require-named-marks --expect-gid spain__unfunded --expect-gid spain__funded
         .venv/bin/python .claude/skills/create-static-viz/scripts/verify_static_viz.py \
             etl/steps/viz/static/eurostat/2026-10-23/pension_entitlements_by_country_selection \
             --template horizontal --require-named-marks --expect-gid spain__unfunded --expect-gid slovakia__funded
         .venv/bin/python .claude/skills/create-static-viz/scripts/verify_static_viz.py \
             etl/steps/viz/static/eurostat/2026-10-23/pension_entitlements_by_country_scheme_type \
             --template horizontal --require-named-marks --expect-gid spain__state --expect-gid spain__employer \
             --expect-gid slovakia__pots
Then read the three PNGs.

Figma handoff
-------------
Record names, not just ids: the frame NAME is the durable join; ids are a convenience.
- File: Charts (2026), key `s6Sv60bakebRRW2TxsMQbF`
- Page: "20260824 Wealth charts (Bertha)" (`26458:5`), an existing page shared with other wealth charts,
  so it carries no ` [sketch]` marker; the frames do.
- Frames, variants of one desktop format (the person picks at finalize). None has a PNG reference copy.
  Frames a and b show this step's current render:
  - `pension-entitlements-by-country--sketch-a` -> `29779:23` (850 x 1095), every country, from frame
    stem `pension_entitlements_by_country`, cloned from `Static Chart Template_Vertical` `5332:93`, at
    x=8430, y=0. Chart cropped to 16..832.71 x 124..1001.81; footer re-pinned to its 1078.81 bottom.
  - `pension-entitlements-by-country--sketch-b` -> `29782:26` (850 x 638), the `SELECTION` countries,
    from frame stem `pension_entitlements_by_country_selection`, cloned from
    `Static Chart Template_Horizontal` `5332:75`, at x=8430, y=1195. Chart cropped to 16..832.71 x 124..545;
    footer re-pinned to its 622 bottom.
  - `pension-entitlements-by-country--sketch-c` (was `29793:1132`), the `SELECTION` countries by scheme
    type, from frame stem `pension_entitlements_by_country_scheme_type`, cloned from frame b, at x=8430, y=1933.
    It is no longer on the page, so its render here is newer than any Figma copy. To bring it back, clone frame b
    and import this stem with `inheritStyles: false` and the `families` below. Chart cropped to 16..832.71 x
    105..559 under its two-line subtitle; footer re-pinned to its 622 bottom.
- Deep links: https://www.figma.com/design/s6Sv60bakebRRW2TxsMQbF/Charts--2026-?node-id=29779-23 (a),
  https://www.figma.com/design/s6Sv60bakebRRW2TxsMQbF/Charts--2026-?node-id=29782-26 (b),
  https://www.figma.com/design/s6Sv60bakebRRW2TxsMQbF/Charts--2026-?node-id=29793-1132 (c)
- Import: `upload_assets` of the SVG, then `restyle_static_import.js` with one job per frame (`canvasWidth`
  816, `frameWidth` 850, no `reference`, `reflowLegend` false). Frames a and b re-import with
  `inheritStyles: true`. Frame c is styled from scratch (`inheritStyles: false`) with `families` Default
  Palette/Denim (`#4c6a9c`, key `e1538d9330d7b22168f0c19fa562897aa8975f90`) for `state` and `unfunded`,
  Default Palette/Camel (`#bc8e5a`, key `45161823f7f1a90184f8efaeaa46bea6a5b71061`) for `employer`, and
  Default Palette/Rusty Orange (`#b13507`, key `65bab597d085689b1ea82a69f4d785cb9212c234`) for `pots` and
  `funded`, and `bodyTextParent: /__(label|total)$/` so row labels and totals take Text/Gray 80. The restyle
  paints legend names in their family's base, so after it rebind `category__employer` to Line and Slope
  Charts/Camel (`#996d39`, key `c17ca762a19289d7f2ec4fd00ba83a053f4d39f9`), the darker variant for text.
- Text slots: title and subtitle set as one string each, with this file's line breaks, and kept on their
  template styles; the subtitle box is 731 wide beside the logo (one-line title). The Note keeps the template's
  runs (`Note:` Bold, space Medium, rest Regular) and this file's line break; the licence row its runs
  (Medium, `CC-BY` Bold, Medium, author Bold). After writing the Note, re-pin the footer to its bottom (1078.81
  on frame a, 622 on b and c). Frame c's subtitle is two lines, broken where `SUBTITLE_BY_SCHEME_TYPE` breaks.

Node names (`gid`s) this step emits, for the restyle pass: `<country>__<part>` for the bar segments
(`unfunded` / `funded` on frames a and b, `state` / `employer` / `pots` on frame c), `<country>__label` for the
row labels, `<country>__total` for the totals, `legend-chip__<part>` for the chips and `category__<part>` for
the legend names.
"""

import logging
import re
from functools import lru_cache
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties, findfont
from matplotlib.patches import Rectangle
from matplotlib.textpath import TextPath
from owid.catalog import Table

from etl.helpers import PathFinder
from etl.viz.static import PIXELS_PER_INCH, TEMPLATES, apply_svg_rcparams, export_frame, source_citation

# Figma-editable text, deterministic ids. Must run before any figure is created.
apply_svg_rcparams()

paths = PathFinder(__file__)

TITLE_BY_FUNDING = "Pension entitlements as a share of GDP"
TITLE_BY_SCHEME_TYPE = "Pension wealth as a share of GDP"
SUBTITLE_BY_FUNDING = (
    "The value today of the pensions people have already earned in state, workplace, and private schemes, "
    "as a share of GDP. Unfunded government schemes will pay them from future contributions and taxes; "
    "private and funded schemes mostly from savings set aside. Data for {year}."
)
# A subtitle with line breaks keeps them, and its lines are drawn as given rather than wrapped: they were fitted
# to the template's subtitle box in its own face, Lato, which this step can't measure (it sets this first line
# 2.4% wider than Lato does: 743px against 725px in Figma, in a 731px box).
# NOTE: After editing this subtitle, re-measure each line in Figma (TEMPLATES.md -> Predicting the template's line
# breaks); a line wider than the box wraps there.
SUBTITLE_BY_SCHEME_TYPE = (
    "The present value today of pension entitlements, expressed as a share of gross domestic product (GDP).\n"
    "Data for {year}, for selected countries."
)
# The shorter frames' countries: the top and the bottom of the ranking, the largest economies, the
# countries that rely most on funded schemes and the ones that rely on none. Left out: Denmark, whose figures
# leave out its state pension (Eurostat counts it as social assistance), and Ireland and Norway, whose GDP is a
# poor measure of their economies (inflated by multinationals' profits in Ireland and by oil and gas in Norway), so
# a share of it understates their pensions. Portugal and Slovakia, with every published value unflagged, fill in.
SELECTION = [
    "Spain",
    "Italy",
    "Netherlands",
    "France",
    "Portugal",
    "Switzerland",
    "Germany",
    "Hungary",
    "Sweden",
    "Poland",
    "Iceland",
    "Czechia",
    "Slovakia",
    "Bulgaria",
]
# The year shown is the latest one in which at least this many countries report a total.
# NOTE: Countries report every three years (2015, 2018, 2021; 2024 is due in early 2027). Check that the
# threshold still picks the latest full round.
MIN_COUNTRIES_PER_YEAR = 25
# Discount rate after inflation that Eurostat recommends for government schemes: 3% up to 2015 and 2% afterward
# (https://ec.europa.eu/eurostat/cache/metadata/en/nasa_10_pens_esms.htm). Countries' rates before inflation differ
# only by their inflation targets (4% in most, 4.5% in Poland and Romania, 5% in Hungary), so the Note gives the
# rate after inflation, which they share.
# NOTE: Re-check both rates against the metadata at each update.
REAL_DISCOUNT_RATE_UP_TO_2015 = 3
REAL_DISCOUNT_RATE_AFTER_2015 = 2
# How the Note says each Eurostat flag, for one value ("total") or several ("figures").
FLAG_PHRASES = {
    "p": ("is provisional", "are provisional"),
    "e": ("is estimated", "are estimated"),
    "d": ("uses a different definition", "use a different definition"),
    "b": ("follows a break in the series", "follow a break in the series"),
}
AUTHOR = "Bertha Rohenkohl"

# Exact template strings (TEMPLATES.md -> Exact strings the templates use).
TAGLINE = "OurWorldinData.org — Research and data to make progress against the world’s largest problems."
LICENSE = f"Licensed under CC-BY by the author {AUTHOR}"

# The parts of each bar, in stack order, with their legend names and the garden columns each one adds up. The
# Figma pass binds each to a library style by these keys (`<country>__<key>`), so the hexes here only make the PNG
# a preview: Default Palette/Denim on the largest part (the quieter colour on the bigger area).
# By funding: the two parts Eurostat publishes as a share of GDP.
PARTS_BY_FUNDING = [
    {"key": "unfunded", "columns": ["unfunded_share_gdp"], "name": "Unfunded government schemes", "color": "#4c6a9c"},
    {"key": "funded", "columns": ["private_and_funded_share_gdp"], "name": "Private and funded schemes", "color": "#b13507"},
]
# By scheme type: social security; defined benefit schemes, of private employers and for government employees;
# and defined contribution schemes, private and government-run. Together they are all of Eurostat's schemes
# (columns A, B and D to H of its table 29), so they add up to its total. Camel goes between Denim and Rusty Orange.
PARTS_BY_SCHEME_TYPE = [
    {"key": "state", "columns": ["social_security_share_gdp"], "name": "State pensions", "color": "#4c6a9c"},
    {
        "key": "employer",
        "columns": [
            "private_defined_benefit_share_gdp",
            "government_employee_defined_benefit_financial_corporations_share_gdp",
            "government_employee_defined_benefit_in_core_share_gdp",
            "government_employee_defined_benefit_outside_core_share_gdp",
        ],
        "name": "Employer promises",
        "color": "#bc8e5a",
        # Camel is too pale for text on the canvas, so its name takes the library's darker variant for text.
        "text_color": "#996d39",
    },
    {
        "key": "pots",
        "columns": ["private_defined_contribution_share_gdp", "government_defined_contribution_share_gdp"],
        "name": "Pension pots",
        "color": "#b13507",
    },
]
TOTAL_COLUMN = "total_share_gdp"
# Eurostat rounds each value to a whole percent, so the parts can miss the published total by one.
ROUNDING_TOLERANCE = 1.0

# Two font stacks, because two readers want different answers (WRITING-THE-STEP.md). This one lands in
# the SVG's `font-family` verbatim, so naming Lato first asks Figma to render the import in the
# template's own face on arrival.
EMITTED_FONT_STACK = ["Lato", "Arial", "Helvetica", "Liberation Sans", "sans-serif"]
# What this step measures and draws with. Deliberately does NOT name Lato, which is not installed on
# our machines: a face the step can ask for, not one it can measure in.
# Liberation Sans is the metric-compatible Arial substitute on Linux, so the Arial slacks below hold.
MEASURED_FONT_STACK = ["Arial", "Helvetica", "Liberation Sans", "DejaVu Sans", "sans-serif"]

matplotlib.rcParams["font.family"] = "sans-serif"
matplotlib.rcParams["font.sans-serif"] = EMITTED_FONT_STACK

# Drop the per-face misses for faces deliberately listed as alternatives, and nothing else. A blanket
# silence would also take "Falling back to DejaVu Sans", which says a whole stack failed.
_OPTIONAL_FACES = tuple({*EMITTED_FONT_STACK, *MEASURED_FONT_STACK})
logging.getLogger("matplotlib.font_manager").addFilter(
    lambda record: (
        "Falling back" in record.getMessage()
        or not any(f"Font family '{face}' not found" in record.getMessage() for face in _OPTIONAL_FACES)
    )
)

# The invariant every measured width rests on: the face drawn IS the face measured.
_DRAWN_FACE, _MEASURED_FACE = (
    findfont(FontProperties(family=EMITTED_FONT_STACK)),
    findfont(FontProperties(family=MEASURED_FONT_STACK)),
)
assert _DRAWN_FACE == _MEASURED_FACE, f"draws {Path(_DRAWN_FACE).name}, measures {Path(_MEASURED_FACE).name}"

# Grapher's design language, read from its source (WRITING-THE-STEP.md -> Borrow grapher's design
# language). The step sets no palette beyond the PNG preview: Figma binds the colors.
TEXT_COLOR = "#5b5b5b"
TITLE_COLOR = "#2d2e2d"
FOOTER_COLOR = "#858585"

# A template pixel in points: 100 template px per inch, 72 pt per inch.
POINTS_PER_PIXEL = 72 / PIXELS_PER_INCH

# Template type sizes in px (TEMPLATES.md -> Slot positions); the in-plot body rank is 14.
SLOT_PX = {"title": 25, "subtitle": 16, "note": 12, "source": 12, "footer": 11, "body": 14}
# Line pitches in the template (TEMPLATES.md -> the rhythm): title 29, subtitle 19, Note 14.
TITLE_LINE_PX = 29
SUBTITLE_LINE_PX = 19
NOTE_LINE_PX = 14
HEADER_GAP_PX = 6

# How much wider or narrower this step's Arial sets than the template's faces (TEMPLATES.md ->
# Predicting the template's line breaks): a Lato slot holds a little more, a Playfair one a little less.
LATO_SLOT_SLACK = 1.008
PLAYFAIR_SLOT_SLACK = 0.968

# The band is the room between the header's bottom and the footer's top, inset 14 px at each end.
BAND_INSET = 14
# The one-row legend: 12px square chips, 5px to their name, 16px between items, 16px above the bars.
LEGEND_LINE_PX = 16
LEGEND_CHIP_PX = 12
LEGEND_CHIP_GAP = 5
LEGEND_ITEM_GAP = 16
LEGEND_BAR_GAP = 16
# Row labels end 8px left of the bars; totals start 5px right of them.
LABEL_PAD = 8
VALUE_PAD = 5
# Bars at about 70% of their pitch read as separate rows (WRITING-THE-STEP.md).
BAR_FRACTION = 0.7
# A 1px gap of canvas between the two segments carries the boundary.
SEGMENT_GAP_PX = 1

# One entry per frame this step emits, keyed by the output filename. Positions are template px from the top
# edge (TEMPLATES.md -> Vertical and Horizontal): the header grows down from `origin_y`, and the footer's rows
# keep their y while the Note grows up from its ink bottom. The first frame shows every country on the tall
# template; the second the `SELECTION` on the landscape one, where 14 rows land on the same bar pitch as the
# tall frame's 29; the third the same countries split by scheme type. Each layout sets its own title and subtitle,
# and whether its Note counts the countries shown and says what a lower discount rate would do. The verifier needs
# `--template horizontal` for the second and third stems.
LAYOUTS = {
    "pension_entitlements_by_country": {
        "template": "vertical",
        "countries": None,
        "parts": PARTS_BY_FUNDING,
        "title": TITLE_BY_FUNDING,
        "subtitle": SUBTITLE_BY_FUNDING,
        "note_counts_countries": True,
        "note_explains_rate": True,
        "margin": 16,
        "origin_y": 16,
        "title_slot_px": 737.84,
        "logo_x": 771,
        "note_ink_bottom": 1043.81,
        "source_y": 1047.81,
        "footer_y": 1065.81,
    },
    "pension_entitlements_by_country_selection": {
        "template": "horizontal",
        "countries": SELECTION,
        "parts": PARTS_BY_FUNDING,
        "title": TITLE_BY_FUNDING,
        "subtitle": SUBTITLE_BY_FUNDING,
        "note_counts_countries": True,
        "note_explains_rate": True,
        "margin": 16,
        "origin_y": 16,
        "title_slot_px": 737.84,
        "logo_x": 771,
        "note_ink_bottom": 587,
        "source_y": 590.62,
        "footer_y": 608.62,
    },
    "pension_entitlements_by_country_scheme_type": {
        "template": "horizontal",
        "countries": SELECTION,
        "parts": PARTS_BY_SCHEME_TYPE,
        "title": TITLE_BY_SCHEME_TYPE,
        "subtitle": SUBTITLE_BY_SCHEME_TYPE,
        # The subtitle already says the countries are a selection.
        "note_counts_countries": False,
        "note_explains_rate": False,
        "margin": 16,
        "origin_y": 16,
        "title_slot_px": 737.84,
        "logo_x": 771,
        "note_ink_bottom": 587,
        "source_y": 590.62,
        "footer_y": 608.62,
    },
}


def run() -> None:
    #
    # Load inputs.
    #
    # Load the pension entitlements and Eurostat's flags on them.
    ds = paths.load_dataset("pension_entitlements")
    tb = ds.read("pension_entitlements")
    tb_flags = ds.read("pension_entitlements_flags")
    sanity_check_inputs(tb)
    source = source_citation(tb[TOTAL_COLUMN])

    #
    # Process data.
    #
    # Step 1: Keep the latest year most countries report.
    year = latest_reporting_year(tb)
    tb = tb[tb["year"] == year].reset_index(drop=True)
    tb_flags = tb_flags[tb_flags["year"] == year].reset_index(drop=True)

    # Step 2: Leave out the countries with no total, and rank the rest by their total.
    without_total = sorted(tb.loc[tb[TOTAL_COLUMN].isna(), "country"])
    tb = tb[tb[TOTAL_COLUMN].notna()].sort_values(TOTAL_COLUMN, ascending=False, kind="stable").reset_index(drop=True)
    sanity_check_year(tb)
    paths.log.info(f"{year}: {len(tb)} countries with a total; left out for having none: {without_total}")

    #
    # Save outputs.
    #
    # Step 3: Draw one frame per layout.
    for name, layout in LAYOUTS.items():
        shown = add_parts(select_countries(tb, layout["countries"]), layout["parts"])
        sanity_check_parts(shown, layout["parts"])
        not_shown = without_total if layout["countries"] is None else []
        note = build_note(
            shown,
            len(tb) if layout["note_counts_countries"] else len(shown),
            not_shown,
            tb_flags,
            year,
            explain_rate=layout["note_explains_rate"],
        )
        fig = build(shown, layout, source, note)
        export_frame(paths, fig, name, template=layout["template"])
        plt.close(fig)


def sanity_check_inputs(tb: Table) -> None:
    """Assert what the chart assumes about the garden table."""
    part_columns = [column for part in PARTS_BY_FUNDING for column in part["columns"]]
    assert not tb.duplicated(subset=["country", "year"]).any(), "A country-year appears twice."
    assert (tb[part_columns + [TOTAL_COLUMN]].min() >= 0).all(), "A negative share of GDP."
    # The parts must add up to Eurostat's own total wherever all three are published.
    rows = tb[part_columns + [TOTAL_COLUMN]].dropna()
    gap = (rows[part_columns].sum(axis=1) - rows[TOTAL_COLUMN]).abs()
    assert (gap <= ROUNDING_TOLERANCE).all(), f"Parts do not add up in rows {rows.index[gap > ROUNDING_TOLERANCE].tolist()}."


def latest_reporting_year(tb: Table) -> int:
    """The latest year in which at least `MIN_COUNTRIES_PER_YEAR` countries report a total."""
    counts = tb[tb[TOTAL_COLUMN].notna()].groupby("year")["country"].nunique()
    years = counts[counts >= MIN_COUNTRIES_PER_YEAR].index
    assert len(years), f"No year has a total for at least {MIN_COUNTRIES_PER_YEAR} countries."
    return int(years.max())


def sanity_check_year(tb: Table) -> None:
    """Assert what the chart assumes about the year it shows."""
    part_columns = [column for part in PARTS_BY_FUNDING for column in part["columns"]]
    assert tb[part_columns].notna().all().all(), "A country with a total is missing one of its two parts."
    assert len(tb) >= MIN_COUNTRIES_PER_YEAR, f"Only {len(tb)} countries to show."


def select_countries(tb: Table, countries: list[str] | None) -> Table:
    """The layout's countries, in ranking order; all of them when it names none."""
    if countries is None:
        return tb
    missing = set(countries) - set(tb["country"])
    assert not missing, f"Selected countries not in the data: {sorted(missing)}"
    return tb[tb["country"].isin(countries)].reset_index(drop=True)


def add_parts(tb: Table, parts: list[dict]) -> Table:
    """One column per part of the bar, named by its key: the sum of the part's schemes.

    Eurostat leaves out (flag "m") the schemes a country doesn't have or doesn't report, so a scheme with no value
    counts as zero. `sanity_check_parts` then asserts that the parts still add up to Eurostat's total.
    """
    tb = tb.copy()
    for part in parts:
        tb[part["key"]] = tb[part["columns"]].sum(axis=1)
    return tb


def sanity_check_parts(tb: Table, parts: list[dict]) -> None:
    """Assert that each country's parts add up to its total, so no scheme is missing from its bar."""
    keys = [part["key"] for part in parts]
    gap = (tb[keys].sum(axis=1) - tb[TOTAL_COLUMN]).abs()
    off = tb.loc[gap > ROUNDING_TOLERANCE, "country"].tolist()
    assert not off, f"Parts {keys} do not add up to the total for {off}."


def real_discount_rate(year: int) -> float:
    """The discount rate after inflation Eurostat recommends for government schemes in a given year."""
    return REAL_DISCOUNT_RATE_UP_TO_2015 if year <= 2015 else REAL_DISCOUNT_RATE_AFTER_2015


def build_note(
    shown: Table, n_with_total: int, not_shown: list[str], tb_flags: Table, year: int, explain_rate: bool = True
) -> str:
    """The Note: what the frame leaves out, the discount rate behind the values, and Eurostat's flags."""
    countries = list(shown["country"])
    sentences = []
    # What the frame leaves out.
    if len(shown) < n_with_total:
        sentences.append(f"This chart shows {len(shown)} of the {n_with_total} countries with data")
    # The discount rate behind the values of government schemes, which matters most to the estimates.
    rate = f"Eurostat values future pensions in government schemes with a discount rate of {real_discount_rate(year):g}% after inflation"
    sentences.append(f"{rate}; a lower rate would give higher values" if explain_rate else rate)
    # Eurostat's flags on the values shown, in ranking order.
    flagged = []
    for country in countries:
        country_flags = tb_flags[tb_flags["country"] == country]
        for flag in sorted(set(country_flags["flag"])):
            assert flag in FLAG_PHRASES, f"No Note wording for Eurostat flag {flag!r} ({country})."
            indicators = set(country_flags.loc[country_flags["flag"] == flag, "indicator"])
            singular = indicators == {TOTAL_COLUMN}
            phrase = FLAG_PHRASES[flag][0 if singular else 1]
            flagged.append(f"{country}’s {'total' if singular else 'figures'} {phrase}")
    if flagged:
        sentences.append(", and ".join(flagged) if len(flagged) == 2 else join_words(flagged))
    # Countries left out for having no total.
    if not_shown:
        verb = "is" if len(not_shown) == 1 else "are"
        reason = "it reports" if len(not_shown) == 1 else "they report"
        sentences.append(f"{join_words(not_shown)} {verb} not shown, as {reason} no total")
    return ". ".join(sentences) + "."


def join_words(items: list[str]) -> str:
    """`a`, `a and b`, `a, b, and c` (with the Oxford comma the style guide asks for)."""
    if len(items) <= 2:
        return " and ".join(items)
    return ", ".join(items[:-1]) + ", and " + items[-1]


def px_to_pt(px: float) -> float:
    return px * POINTS_PER_PIXEL


def build(tb: Table, layout: dict, source: str, note_text: str) -> plt.Figure:
    """Draw one frame: header, legend, ranked stacked bars with their totals, and the footer."""
    template = TEMPLATES[layout["template"]]
    width_px, height_px = template.width_px, template.height_px
    fig = plt.figure(figsize=template.figsize)
    fig.patch.set_facecolor("white")  # legible when the PNG is reviewed; the SVG is saved transparent

    def fx(px: float) -> float:
        return px / width_px

    def fy(px: float) -> float:
        return 1 - px / height_px  # matplotlib's y runs upward

    margin = layout["margin"]
    content_w = width_px - 2 * margin
    year = int(tb["year"].iloc[0])

    # Step 1: Header. Under a one-line title the subtitle starts beside the logo, so its box ends 24px
    # short of it (TEMPLATES.md); wrap there, into lines of about equal length.
    title = wrap_to_width(layout["title"], layout["title_slot_px"] * PLAYFAIR_SLOT_SLACK, px_to_pt(SLOT_PX["title"]))
    title_lines = lines_in(title)
    subtitle_slot = layout["logo_x"] - 24 - margin if title_lines == 1 else content_w
    subtitle = layout["subtitle"].format(year=year)
    if "\n" in subtitle:
        for line in subtitle.split("\n"):
            width = text_width_px(line, px_to_pt(SLOT_PX["subtitle"]))
            assert width <= content_w, f"Subtitle line {line!r} is wider than the content ({width:.0f}px)."
    else:
        subtitle = wrap_balanced(subtitle, subtitle_slot * LATO_SLOT_SLACK, px_to_pt(SLOT_PX["subtitle"]))
    subtitle_y = layout["origin_y"] + title_lines * TITLE_LINE_PX + HEADER_GAP_PX
    draw_lines(fig, fx, fy, margin, layout["origin_y"], title, SLOT_PX["title"], TITLE_LINE_PX, TITLE_COLOR, "title")
    draw_lines(fig, fx, fy, margin, subtitle_y, subtitle, SLOT_PX["subtitle"], SUBTITLE_LINE_PX, TEXT_COLOR, "subtitle")
    band_top = subtitle_y + lines_in(subtitle) * SUBTITLE_LINE_PX

    # Step 2: Footer, in the template's slots. The Note grows up from its ink bottom, and the band ends
    # at its top.
    note = wrap_balanced(f"Note: {note_text}", content_w * LATO_SLOT_SLACK, px_to_pt(SLOT_PX["note"]))
    note_top = layout["note_ink_bottom"] - lines_in(note) * NOTE_LINE_PX
    draw_note(fig, fx, fy, margin, note_top, note)
    footer_text(fig, fx(margin), fy(layout["source_y"]), f"Data source: {source}", SLOT_PX["source"], "data-source")
    footer_text(fig, fx(margin), fy(layout["footer_y"]), TAGLINE, SLOT_PX["footer"], "tagline")
    footer_text(fig, fx(width_px - margin), fy(layout["footer_y"]), LICENSE, SLOT_PX["footer"], "license", ha="right")
    band_bottom = note_top

    # Step 3: The legend, one row under the subtitle.
    legend_top = band_top + BAND_INSET
    draw_legend(fig, fx, fy, margin, legend_top, layout["parts"])

    # Step 4: The plot's extent. The label column is as wide as the widest name, and the longest bar's
    # total, unit included, ends on the content edge.
    body_pt = px_to_pt(SLOT_PX["body"])
    labels = tb["country"].tolist()
    label_col_px = max(text_advance_px(label, body_pt) for label in labels)
    plot_left = margin + label_col_px + LABEL_PAD
    totals = tb[TOTAL_COLUMN].tolist()
    value_texts = [format_total(value, unit=index == 0) for index, value in enumerate(totals)]
    px_per_point = (width_px - margin - text_advance_px(value_texts[0], body_pt) - VALUE_PAD - plot_left) / totals[0]
    # Every other bar's total must also end inside the content edge.
    for value, text in zip(totals, value_texts):
        end = plot_left + value * px_per_point + VALUE_PAD + text_advance_px(text, body_pt)
        assert end <= width_px - margin + 0.01, f"the total {text!r} overruns the content edge by {end - width_px + margin:.1f}px"

    # Step 5: Rows. The first bar's ink starts under the legend, the last one's ends one inset above
    # the Note: with bars at BAR_FRACTION of the pitch, the pitch follows from those two edges.
    bars_top = legend_top + LEGEND_LINE_PX + LEGEND_BAR_GAP
    ink_bottom = band_bottom - BAND_INSET
    n_rows = len(tb)
    pitch = (ink_bottom - bars_top) / (n_rows - 1 + BAR_FRACTION)
    bar_h = BAR_FRACTION * pitch
    paths.log.info(f"{n_rows} bars of {bar_h:.1f}px on a {pitch:.1f}px pitch; {px_per_point:.3f}px per % of GDP")

    cap_px = cap_height_px(body_pt)
    for index, row in tb.iterrows():
        slug = slugify(row["country"])
        top = bars_top + index * pitch
        centre = top + bar_h / 2
        baseline = centre + cap_px / 2  # an explicit baseline, so digits and names sit on one line

        fig.text(
            fx(plot_left - LABEL_PAD),
            fy(baseline),
            row["country"],
            ha="right",
            va="baseline",
            fontsize=body_pt,
            color=TEXT_COLOR,
            gid=f"{slug}__label",
        )
        parts = layout["parts"]
        left = plot_left
        for position, part in enumerate(parts):
            width = row[part["key"]] * px_per_point
            last = position == len(parts) - 1
            # Every segment but the last stops short by the gap, so each still starts at its value. A
            # segment too narrow to survive the gap is skipped rather than drawn backwards.
            drawn = width if last else width - SEGMENT_GAP_PX
            if drawn > 0:
                fig.add_artist(
                    Rectangle(
                        (fx(left), fy(top + bar_h)),
                        drawn / width_px,
                        bar_h / height_px,
                        transform=fig.transFigure,
                        facecolor=part["color"],
                        linewidth=0,
                        gid=f"{slug}__{part['key']}",
                    )
                )
            left += width
        fig.text(
            fx(left + VALUE_PAD),
            fy(baseline),
            value_texts[index],
            ha="left",
            va="baseline",
            fontsize=body_pt,
            color=TEXT_COLOR,
            gid=f"{slug}__total",
        )

    return fig


def draw_legend(fig, fx, fy, margin: float, top: float, parts: list[dict]) -> None:
    """One row of square chips, each followed by its part's name in the part's own colour."""
    body_pt = px_to_pt(SLOT_PX["body"])
    centre = top + LEGEND_LINE_PX / 2
    x = margin
    for part in parts:
        fig.add_artist(
            Rectangle(
                (fx(x), fy(centre + LEGEND_CHIP_PX / 2)),
                fx(LEGEND_CHIP_PX),
                LEGEND_CHIP_PX / (fig.get_figheight() * PIXELS_PER_INCH),
                transform=fig.transFigure,
                facecolor=part["color"],
                linewidth=0,
                gid=f"legend-chip__{part['key']}",
            )
        )
        x += LEGEND_CHIP_PX + LEGEND_CHIP_GAP
        fig.text(
            fx(x),
            fy(centre + cap_height_px(body_pt) / 2),
            part["name"],
            ha="left",
            va="baseline",
            fontsize=body_pt,
            color=part.get("text_color", part["color"]),
            gid=f"category__{part['key']}",
        )
        x += text_advance_px(part["name"], body_pt) + LEGEND_ITEM_GAP


def draw_lines(fig, fx, fy, x_px: float, y_px: float, text: str, size_px: float, pitch_px: float, color, gid: str):
    """A wrapped slot, one call per line at the template's pitch; lines after the first are `<gid>-line<n>`."""
    for index, line in enumerate(text.split("\n")):
        fig.text(
            fx(x_px),
            fy(y_px + index * pitch_px),
            line,
            ha="left",
            va="top",
            fontsize=px_to_pt(size_px),
            color=color,
            gid=gid if index == 0 else f"{gid}-line{index + 1}",
        )


def draw_note(fig, fx, fy, x_px: float, y_px: float, note: str) -> None:
    """The Note with its `Note:` lead-in in bold: matplotlib has no rich text, so the lead is its own run."""
    fontsize = px_to_pt(SLOT_PX["note"])
    lines = note.split("\n")
    lead = "Note:"
    assert lines[0].startswith(lead + " ")
    fig.text(fx(x_px), fy(y_px), lead, ha="left", va="top", fontsize=fontsize, fontweight="bold", color=FOOTER_COLOR, gid="note")
    rest_x = x_px + text_advance_px(lead + " ", fontsize, bold=True)
    fig.text(fx(rest_x), fy(y_px), lines[0][len(lead) + 1 :], ha="left", va="top", fontsize=fontsize, color=FOOTER_COLOR, gid="note-rest")
    for index, line in enumerate(lines[1:], start=2):
        fig.text(
            fx(x_px),
            fy(y_px + (index - 1) * NOTE_LINE_PX),
            line,
            ha="left",
            va="top",
            fontsize=fontsize,
            color=FOOTER_COLOR,
            gid=f"note-line{index}",
        )


def footer_text(fig, x, y, text: str, size_px: float, gid: str, ha: str = "left") -> None:
    fig.text(x, y, text, ha=ha, va="top", fontsize=px_to_pt(size_px), color=FOOTER_COLOR, gid=gid)


def format_total(value: float, unit: bool) -> str:
    """A total as a whole percent; the longest bar spells the unit out, the rest carry the bare number."""
    return f"{value:.0f}% of GDP" if unit else f"{value:.0f}%"


def slugify(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


@lru_cache(maxsize=None)
def text_width_px(text: str, fontsize: float, bold: bool = False) -> float:
    """Measured ink width of text, in template pixels, in the face this step draws."""
    if not text.strip():
        return 0.0
    prop = FontProperties(family=MEASURED_FONT_STACK, size=fontsize, weight="bold" if bold else "normal")
    return TextPath((0, 0), text, prop=prop).get_extents().width / POINTS_PER_PIXEL


def text_advance_px(text: str, fontsize: float, bold: bool = False) -> float:
    """Width of text including any trailing space: measured with a sentinel glyph, then subtracted."""
    sentinel = "|"
    return text_width_px(text + sentinel, fontsize, bold) - text_width_px(sentinel, fontsize, bold)


def cap_height_px(fontsize: float) -> float:
    """Height of a digit's ink above the baseline, in template pixels."""
    prop = FontProperties(family=MEASURED_FONT_STACK, size=fontsize)
    return TextPath((0, 0), "0", prop=prop).get_extents().ymax / POINTS_PER_PIXEL


def wrap_to_width(text: str, max_px: float, fontsize: float) -> str:
    """Wrap text greedily against measured widths."""
    lines: list[str] = []
    current = ""
    for word in text.split():
        candidate = f"{current} {word}".strip()
        if current and text_width_px(candidate, fontsize) > max_px:
            lines.append(current)
            current = word
        else:
            current = candidate
    if current:
        lines.append(current)
    return "\n".join(lines)


def wrap_balanced(text: str, max_px: float, fontsize: float) -> str:
    """Wrap to the fewest lines the slot allows, then narrow the width while that count holds."""
    wrapped = wrap_to_width(text, max_px, fontsize)
    target = lines_in(wrapped)
    width = max_px
    while width > max_px / 2:
        candidate = wrap_to_width(text, width - 2, fontsize)
        if lines_in(candidate) > target:
            break
        wrapped, width = candidate, width - 2
    return wrapped


def lines_in(text: str) -> int:
    return text.count("\n") + 1

