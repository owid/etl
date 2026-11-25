"""Tests for etl.viz.static — the shared contract every static viz has to satisfy.

The point of these is the negative cases. Four of the five shipped static_viz steps violated the
Figma handoff contract in a way that is invisible in the rendered PNG, so a test that only checks
the happy path would have passed on all of them.
"""

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pytest
from matplotlib.text import Text

from etl import paths as etl_paths
from etl.helpers import PathFinder
from etl.viz.static import (
    PIXELS_PER_INCH,
    REPRODUCIBLE_METADATA,
    SVG_HASHSALT,
    TEMPLATES,
    SketchPaths,
    _sub_yearly_grid,
    apply_svg_rcparams,
    export_frame,
    name_furniture,
    nice_year_ticks,
    save_fig,
    source_citation,
    style_time_axis,
    style_year_axis,
    time_axis_ticks,
    unclip,
    year_axis_ticks,
)


class _Origin:
    def __init__(self, producer, date_published, attribution_short=None, title=None):
        self.producer = producer
        self.date_published = date_published
        self.attribution_short = attribution_short
        self.title = title


class _Meta:
    def __init__(self, origins):
        self.origins = origins


class _Col:
    def __init__(self, origins):
        self.metadata = _Meta(origins)


class _Paths:
    """Stands in for PathFinder, recording what export_fig was asked to write."""

    def __init__(self, tmp_path):
        self.directory = tmp_path
        self.calls = []

        class _Log:
            def info(self, *_a, **_k):
                pass

        self.log = _Log()

    def export_fig(self, fig, filename, extensions, **kwargs):
        self.calls.append((filename, tuple(extensions), kwargs))


def test_sketch_paths_mirrors_the_pathfinder_surface(tmp_path):
    # A sketch is written in the exact shape of a step, so promotion is a one-line swap of this object
    # for PathFinder(__file__). The dir is the slug: the file is always sketch.py.
    p = SketchPaths(tmp_path / "my_slug" / "sketch.py")
    assert p.directory == tmp_path / "my_slug"
    assert p.short_name == "my_slug"
    assert callable(p.log.info)


def test_sketch_paths_export_frame_writes_both_files_reproducibly(tmp_path):
    """The round trip a sketch relies on, without a subprocess: real files, real stamps stripped."""
    apply_svg_rcparams()
    sketch_dir = tmp_path / "demo"
    sketch_dir.mkdir()
    p = SketchPaths(sketch_dir / "sketch.py")
    fig = plt.figure(figsize=TEMPLATES["horizontal"].figsize)
    fig.text(0.5, 0.5, "hello", gid="title")
    export_frame(p, fig, "chart", template="horizontal")
    plt.close(fig)

    svg = (sketch_dir / "chart.svg").read_text()
    assert (sketch_dir / "chart.png").exists()
    assert "<text" in svg, "svg.fonttype must be 'none' so the copy stays editable"
    assert "<dc:date>" not in svg and "<dc:creator>" not in svg, "the version stamp must be stripped"

    from PIL import Image

    assert "Software" not in Image.open(sketch_dir / "chart.png").info


def test_save_fig_merges_metadata_over_the_defaults(tmp_path):
    fig = plt.figure(figsize=(2, 1))
    written = save_fig(tmp_path, fig, "x", ["svg"], metadata={"Title": "hello"})
    plt.close(fig)
    assert written == [tmp_path / "x.svg"]
    svg = written[0].read_text()
    assert "<dc:title>hello</dc:title>" in svg, "a caller's metadata must survive the merge"
    assert "<dc:date>" not in svg, "…without re-introducing the stamp the defaults strip"
    assert REPRODUCIBLE_METADATA["svg"]["Date"] is None


def test_pathfinder_export_fig_delegates_to_save_fig(monkeypatch):
    """The mutation guard for the refactor: a local loop reintroduced in helpers.py fails this."""
    pf = PathFinder(str(etl_paths.STEP_DIR / "viz/static/who/2026-08-07/height_for_age.py"))
    recorded = []
    monkeypatch.setattr("etl.viz.static.save_fig", lambda *a, **k: recorded.append((a, k)))
    fig = plt.figure(figsize=(2, 1))
    pf.export_fig(fig, "x", ["svg"], transparent=True)
    plt.close(fig)
    assert recorded == [((pf.directory, fig, "x", ["svg"]), {"log": pf.log, "transparent": True})]


def test_apply_svg_rcparams_sets_both():
    matplotlib.rcParams["svg.fonttype"] = "path"
    apply_svg_rcparams()
    # fonttype is the one that matters: the default outlines every glyph, so the copy arrives in
    # Figma as vector paths. Two shipped steps omitted it.
    assert matplotlib.rcParams["svg.fonttype"] == "none"
    assert matplotlib.rcParams["svg.hashsalt"] == SVG_HASHSALT


def test_templates_figsize_matches_pixel_size():
    for key, t in TEMPLATES.items():
        assert t.figsize == (t.width_px / PIXELS_PER_INCH, t.height_px / PIXELS_PER_INCH), key
        assert t.ratio == pytest.approx(t.width_px / t.height_px), key


def test_templates_agree_with_the_verifier_ratios():
    # verify_static_viz.py keys its frame-ratio check off the same names. If these drift, a step
    # laid out against one is checked against the other.
    expected = {
        "horizontal": (850, 638),
        "vertical": (850, 1095),
        "mobile": (540, 824),
        "mobile-square": (540, 540),
    }
    assert {k: (v.width_px, v.height_px) for k, v in TEMPLATES.items()} == expected
    # The verifier judges an SVG's scale at this same 100px per inch.
    assert PIXELS_PER_INCH == 100


def test_unclip_turns_clipping_off_everywhere():
    fig, ax = plt.subplots()
    (line,) = ax.plot([0, 1], [0, 1])
    assert line.get_clip_on() is True
    unclip(fig)
    assert line.get_clip_on() is False
    assert all(not a.get_clip_on() for a in fig.findobj())
    plt.close(fig)


def test_export_frame_writes_opaque_png_and_transparent_svg(tmp_path):
    paths = _Paths(tmp_path)
    t = TEMPLATES["horizontal"]
    fig = plt.figure(figsize=t.figsize)
    export_frame(paths, fig, "chart", template="horizontal")
    plt.close(fig)

    assert [c[0] for c in paths.calls] == ["chart", "chart"]
    png, svg = paths.calls
    assert png[1] == ("png",) and png[2]["dpi"] == 300
    assert "transparent" not in png[2], "the PNG must keep its opaque canvas — it is the reference copy"
    assert svg[1] == ("svg",) and svg[2]["transparent"] is True


def test_export_frame_sweeps_clipping_before_saving(tmp_path):
    paths = _Paths(tmp_path)
    t = TEMPLATES["mobile"]
    fig = plt.figure(figsize=t.figsize)
    ax = fig.add_subplot()
    (line,) = ax.plot([0, 1], [0, 1])
    export_frame(paths, fig, "chart", template="mobile")
    assert line.get_clip_on() is False
    plt.close(fig)


def test_name_furniture_names_ticks_grids_and_spines_by_value():
    fig, ax = plt.subplots()
    ax.plot([2000, 2020], [0, 100], gid="series__demo")
    ax.set_xticks([2000, 2010, 2020])
    ax.grid(True)
    assert name_furniture(fig) > 0
    gids = {a.get_gid() for a in fig.findobj() if a.get_gid()}
    assert {"axes__spine-left", "axes__x-grid-2010", "axes__x-tick-2010", "axes__x-tick-label-2010"} <= gids
    assert "series__demo" in gids, "a gid the step set itself is kept"
    plt.close(fig)


def test_name_furniture_never_overwrites_a_step_gid():
    fig, ax = plt.subplots()
    ax.spines["left"].set_gid("frame__baseline")
    name_furniture(fig)
    assert ax.spines["left"].get_gid() == "frame__baseline"
    plt.close(fig)


def test_name_furniture_keeps_nearby_large_ticks_distinct():
    # The default "g" format rounds to six digits, so 1,000,000..1,000,005 would all read 1e+06.
    fig, ax = plt.subplots()
    ax.set_xlim(1_000_000, 1_000_005)
    ax.set_xticks(range(1_000_000, 1_000_006))
    name_furniture(fig)
    labels = [t.label1.get_gid() for t in ax.xaxis.get_major_ticks()]
    assert labels == [f"axes__x-tick-label-{v}" for v in range(1_000_000, 1_000_006)]
    plt.close(fig)


def test_name_furniture_reaches_inset_and_secondary_axes():
    # Both live in ax.child_axes, which fig.axes does not list.
    fig, ax = plt.subplots()
    inset = ax.inset_axes([0.6, 0.6, 0.3, 0.3])
    inset.set_xticks([0, 1])
    secondary = ax.secondary_xaxis("top")
    name_furniture(fig)
    assert inset.spines["left"].get_gid() == "axes-child1__spine-left"
    assert inset.xaxis.get_major_ticks()[1].label1.get_gid() == "axes-child1__x-tick-label-1"
    assert secondary.spines["top"].get_gid() == "axes-child2__spine-top"
    plt.close(fig)


def test_name_furniture_keeps_the_first_axes_name_when_another_is_added():
    # A later render that adds a twin axis must not rename the original axes' furniture.
    fig, ax = plt.subplots()
    twin = ax.twinx()
    name_furniture(fig)
    assert ax.spines["left"].get_gid() == "axes__spine-left"
    assert twin.spines["right"].get_gid() == "axes2__spine-right"
    plt.close(fig)


def test_name_furniture_names_left_and_right_titles():
    fig, ax = plt.subplots()
    ax.set_title("Left", loc="left")
    ax.set_title("Right", loc="right")
    name_furniture(fig)
    gids = {a.get_text(): a.get_gid() for a in fig.findobj(match=Text) if isinstance(a, Text)}
    assert gids["Left"] == "axes__title-left" and gids["Right"] == "axes__title-right"
    plt.close(fig)


def test_name_furniture_never_repeats_an_id():
    fig, ax = plt.subplots()
    ax.set_yticks([0.1, 0.1 + 1e-17, 0.2])
    ax.text(0, 0, "x", gid="axes__y-grid-0.2")
    name_furniture(fig)
    gids = [a.get_gid() for a in fig.findobj() if a.get_gid()]
    assert len(gids) == len(set(gids)), "every mark needs its own id for the re-import to tell them apart"
    assert "axes__y-grid-0.2-2" in gids, "a name already taken gets a suffix, not a duplicate"
    plt.close(fig)


def test_export_frame_svg_has_no_unnamed_axes_furniture(tmp_path):
    """End to end: the saved SVG carries the names, which is what the Figma re-import reads."""
    paths = SketchPaths(tmp_path / "sketch.py")
    t = TEMPLATES["horizontal"]
    fig = plt.figure(figsize=t.figsize)
    ax = fig.add_subplot()
    ax.plot([0, 1], [0, 1], gid="series__demo")
    ax.set_xticks([0, 1])
    ax.set_yticks([0, 0.5, 1])
    ax.grid(True)
    export_frame(paths, fig, "chart", template="horizontal")
    plt.close(fig)
    svg = (tmp_path / "chart.svg").read_text()
    assert 'id="axes__y-grid-0.5"' in svg and 'id="axes__x-tick-label-1"' in svg
    assert "<clipPath" not in svg, "the sweep must reach ticks the naming draw created"


def test_export_frame_rejects_a_figsize_that_is_not_the_template(tmp_path):
    """The negative case: a figsize typo, or a bbox_inches="tight" that cropped to the ink."""
    paths = _Paths(tmp_path)
    fig = plt.figure(figsize=(8.5, 8.5))  # square, but "horizontal" is 850x638
    with pytest.raises(AssertionError, match="does not match template"):
        export_frame(paths, fig, "chart", template="horizontal")
    plt.close(fig)
    assert paths.calls == [], "nothing may be written once the aspect check fails"


def test_export_frame_rejects_the_right_ratio_at_the_wrong_scale(tmp_path):
    """The subtle one: the aspect is exact, only the scale is off.

    A ratio-only check passes this, and the SVG then needs a uniform rescale into the template
    frame — which divides every point-denominated font size by that same factor, so the type
    hierarchy lands off the template's with nothing in the rendered image to show it.
    """
    paths = _Paths(tmp_path)
    t = TEMPLATES["horizontal"]
    fig = plt.figure(figsize=(t.figsize[0] * 2, t.figsize[1] * 2))
    assert fig.get_figwidth() / fig.get_figheight() == pytest.approx(t.ratio), "aspect must be exact"
    with pytest.raises(AssertionError, match="does not match template"):
        export_frame(paths, fig, "chart", template="horizontal")
    plt.close(fig)
    assert paths.calls == [], "nothing may be written once the size check fails"


def test_export_frame_accepts_the_exact_template_figsize(tmp_path):
    for key, t in TEMPLATES.items():
        paths = _Paths(tmp_path)
        fig = plt.figure(figsize=t.figsize)
        export_frame(paths, fig, "chart", template=key)
        plt.close(fig)
        assert len(paths.calls) == 2, key


def test_export_frame_holds_a_declared_height_at_the_template_width_and_scale(tmp_path):
    t = TEMPLATES["horizontal"]
    fig = plt.figure(figsize=(t.figsize[0], 6.15))
    paths = _Paths(tmp_path)
    export_frame(paths, fig, "chart", template="horizontal", height_px=615)
    plt.close(fig)
    assert len(paths.calls) == 2
    # The same aspect at twice the scale, and the template's own height, both fail.
    for figsize, height_px in (((t.figsize[0] * 2, 12.3), 615), ((t.figsize[0], 6.15), t.height_px)):
        paths = _Paths(tmp_path)
        fig = plt.figure(figsize=figsize)
        with pytest.raises(AssertionError, match="does not match template"):
            export_frame(paths, fig, "chart", template="horizontal", height_px=height_px)
        plt.close(fig)
        assert paths.calls == []


def test_export_frame_needs_a_template_for_a_declared_height(tmp_path):
    fig = plt.figure(figsize=(8.5, 6.15))
    with pytest.raises(ValueError, match="needs template"):
        export_frame(_Paths(tmp_path), fig, "chart", height_px=615)
    plt.close(fig)


def test_export_frame_rejects_an_unknown_template(tmp_path):
    paths = _Paths(tmp_path)
    fig = plt.figure(figsize=(8.5, 6.38))
    with pytest.raises(ValueError, match="unknown template"):
        export_frame(paths, fig, "chart", template="landscape")
    plt.close(fig)


def test_export_frame_without_a_template_skips_the_check(tmp_path):
    # Pre-template steps exist and are allowed to save; they just get no assertion.
    paths = _Paths(tmp_path)
    fig = plt.figure(figsize=(13.65, 9.5))
    export_frame(paths, fig, "chart")
    plt.close(fig)
    assert len(paths.calls) == 2


def test_source_citation_groups_years_under_one_producer():
    # Two products from one producer cite as one entry carrying both years, which is grapher's own
    # footer convention — not as two separate sources.
    col = _Col([_Origin("WHO", "2006-04-27"), _Origin("WHO", "2007-09-01")])
    assert source_citation(col) == "WHO (2006; 2007)"


def test_source_citation_separates_distinct_producers():
    col = _Col([_Origin("UN WPP", "2024-07-11"), _Origin("HYDE", "2023-01-01")])
    assert source_citation(col) == "HYDE (2023); UN WPP (2024)"


def test_source_citation_spans_several_columns_and_dedupes():
    a = _Col([_Origin("UN WPP", "2024-07-11")])
    b = _Col([_Origin("UN WPP", "2024-07-11"), _Origin("Gapminder", "2022-03-04")])
    assert source_citation(a, b) == "Gapminder (2022); UN WPP (2024)"


def test_source_citation_can_key_on_attribution_short():
    col = _Col([_Origin("World Health Organization", "2006-04-27", attribution_short="WHO")])
    assert source_citation(col, key="attribution_short") == "WHO (2006)"


def test_source_citation_falls_back_to_producer_without_a_short_attribution():
    """`attribution_short` is optional on Origin, so a column can carry a mix.

    The origin that lacks it must still reach the footer under its producer — dropping it
    under-attributes the chart, and nothing in the rendered image shows a source went missing.
    """
    col = _Col(
        [
            _Origin("World Health Organization", "2006-04-27", attribution_short="WHO"),
            _Origin("Institute for Health Metrics and Evaluation", "2024-05-16"),
        ]
    )
    assert source_citation(col, key="attribution_short") == (
        "Institute for Health Metrics and Evaluation (2024); WHO (2006)"
    )


def test_source_citation_takes_a_prefix():
    col = _Col([_Origin("WHO", "2006-04-27")])
    assert source_citation(col, prefix="Data sources: ") == "Data sources: WHO (2006)"


def test_source_citation_survives_a_missing_date_and_missing_key():
    col = _Col([_Origin("WHO", None), _Origin(None, "2020-01-01")])
    assert source_citation(col) == "WHO"


def test_source_citation_is_empty_when_there_are_no_origins():
    assert source_citation(_Col([])) == ""


@pytest.mark.parametrize(
    "lo,hi",
    [(1700, 2100), (1900, 2020), (1990, 2024), (-10000, 2100), (2000, 2005)],
)
def test_nice_year_ticks_span_the_range_and_are_evenly_spaced(lo, hi):
    ticks = nice_year_ticks(lo, hi)
    assert len(ticks) >= 3
    assert all(lo <= t <= hi for t in ticks)
    steps = {b - a for a, b in zip(ticks, ticks[1:])}
    assert len(steps) == 1, f"uneven spacing for {lo}..{hi}: {ticks}"


def test_nice_year_ticks_respects_max_ticks_when_both_endpoints_align():
    """1700..2100 is the trap: a 50-year step cuts the span into exactly 8 intervals — 9 ticks.

    Testing the interval count instead of the tick count let a `max_ticks=8` request return nine,
    and put a 400-year span on half-centuries rather than the centuries the docstring promises.
    """
    ticks = nice_year_ticks(1700, 2100)
    assert ticks == [1700, 1800, 1900, 2000, 2100]
    assert len(ticks) <= 8


@pytest.mark.parametrize(
    "lo,hi,max_ticks",
    [(1700, 2100, 8), (1900, 2020, 5), (1990, 2024, 4), (-10000, 2100, 6), (2000, 2005, 8)],
)
def test_nice_year_ticks_never_exceeds_the_requested_limit(lo, hi, max_ticks):
    ticks = nice_year_ticks(lo, hi, max_ticks=max_ticks)
    assert len(ticks) <= max_ticks, f"{lo}..{hi} with max_ticks={max_ticks} returned {ticks}"


def test_nice_year_ticks_degenerate_range():
    assert nice_year_ticks(2000, 2000) == [2000]
    assert nice_year_ticks(2000, 1990) == [2000]


@pytest.mark.parametrize(
    "lo,hi,max_ticks",
    [(1700, 2100, 8), (1950, 2023, 8), (1990, 2024, 8), (1990, 2024, 4), (-10000, 2100, 6), (2000, 2005, 8)],
)
def test_year_axis_ticks_open_and_close_on_the_data(lo, hi, max_ticks):
    """Grapher's time axis shows the first and last year, whatever the round years between them."""
    ticks = year_axis_ticks(lo, hi, max_ticks=max_ticks)
    assert ticks[0] == lo and ticks[-1] == hi, ticks
    assert ticks == sorted(set(ticks)), ticks
    assert len(ticks) <= max_ticks, ticks


def test_year_axis_ticks_keep_the_round_step_and_add_the_last_year():
    """Round years stay on their step, never re-spaced from first to last (2000, 2006, 2012, 2018, 2024)."""
    assert year_axis_ticks(2000, 2024) == [2000, 2005, 2010, 2015, 2020, 2024]
    assert year_axis_ticks(1700, 2100) == [1700, 1800, 1900, 2000, 2100]
    assert year_axis_ticks(2000, 2000) == [2000]


def test_year_axis_ticks_drop_a_round_year_only_where_its_label_collides():
    """2020 next to 2023 collides on an 850-wide plot over 73 years; next to 2022 over 22 years it does not."""
    assert year_axis_ticks(1950, 2023, plot_width_px=770) == [1950, 1960, 1970, 1980, 1990, 2000, 2010, 2023]
    assert year_axis_ticks(2000, 2022, plot_width_px=770) == [2000, 2005, 2010, 2015, 2020, 2022]
    assert year_axis_ticks(2000, 2022, plot_width_px=400) == [2000, 2005, 2010, 2015, 2022]


@pytest.mark.parametrize(
    "lo,hi,plot_width_px",
    [(2000, 2014, 200), (1950, 2023, 200), (2000, 2024, 470), (1950, 2023, 770), (1990, 2024, 300)],
)
def test_year_axis_ticks_keep_the_minimum_gap_between_every_pair_of_ticks(lo, hi, plot_width_px):
    """On a narrow plot or a facet the round step coarsens, so no two neighbouring labels overlap.

    Checking the gap only against the two ends let 2000..2014 at 200 px keep a 2-year step, with
    interior labels about 29 px apart.
    """
    min_gap_px = 56
    ticks = year_axis_ticks(lo, hi, plot_width_px=plot_width_px, min_gap_px=min_gap_px)
    assert ticks[0] == lo and ticks[-1] == hi, ticks
    px_per_year = plot_width_px / (hi - lo)
    gaps = [(b - a) * px_per_year for a, b in zip(ticks, ticks[1:])]
    assert min(gaps) >= min_gap_px, f"{lo}..{hi} at {plot_width_px}px: {ticks} has gaps {gaps}"


def test_year_axis_ticks_coarsen_the_round_step_on_a_narrow_plot():
    assert year_axis_ticks(2000, 2014, plot_width_px=200) == [2000, 2005, 2010, 2014]
    assert year_axis_ticks(1950, 2023, plot_width_px=200) == [1950, 2000, 2023]


def _year_axes(ticks, xs=None):
    fig, ax = plt.subplots(figsize=(8.5, 6.38))
    xs = xs if xs is not None else [ticks[0], ticks[-1]]
    ax.plot(xs, [0, 1], gid="line__value")
    ax.set_xticks(ticks, [str(getattr(t, "year", t)) for t in ticks])
    return fig, ax


def test_style_year_axis_closes_the_axis_as_grapher_draws_it():
    """Range pinned to the ends, a 1px butt-capped spine, 5x1px marks, end labels inset to their ticks."""
    fig, ax = _year_axes([2000, 2005, 2010, 2015, 2020, 2024])
    style_year_axis(ax, 2000, 2024)
    assert ax.get_xlim() == (2000, 2024)
    spine = ax.spines["bottom"]
    assert spine.get_capstyle() == "butt" and spine.get_linewidth() == pytest.approx(0.72)
    # Drawing must keep the alignment: matplotlib reuses the ticks it zipped the locations with.
    name_furniture(fig)
    ticks = ax.xaxis.get_major_ticks()
    assert [t.label1.get_horizontalalignment() for t in ticks] == [
        "left",
        "center",
        "center",
        "center",
        "center",
        "right",
    ]
    assert ticks[0].tick1line.get_markersize() == pytest.approx(3.6)
    assert ticks[0].tick1line.get_markeredgewidth() == pytest.approx(0.72)
    plt.close(fig)


def test_style_year_axis_writes_end_labels_on_their_ticks_and_a_butt_spine(tmp_path):
    """In the SVG, the end labels anchor start / end at their tick's x and the spine stops at the end ticks."""
    import re

    apply_svg_rcparams()
    fig, ax = _year_axes([2000, 2010, 2020, 2024])
    style_year_axis(ax, 2000, 2024)
    ax.tick_params(labelsize=10)  # a later tick_params must not reset the alignment
    name_furniture(fig)
    unclip(fig)
    path = tmp_path / "axis.svg"
    fig.savefig(path, format="svg")
    plt.close(fig)
    svg = path.read_text()

    def label(value):
        match = re.search(
            rf'id="axes__x-tick-label-{value}">\s*<text style="[^"]*text-anchor: (\w+)[^"]*" x="([0-9.]+)"', svg
        )
        assert match, f"no label for {value}"
        return match.group(1), float(match.group(2))

    def mark(value):
        match = re.search(rf'id="axes__x-tick-{value}">.*?<use [^>]*x="([0-9.]+)"', svg, re.S)
        assert match, f"no tick mark for {value}"
        return float(match.group(1))

    assert label(2000) == ("start", mark(2000))
    assert label(2024) == ("end", mark(2024))
    assert label(2010)[0] == "middle"
    spine = re.search(
        r'id="axes__spine-bottom">\s*<path d="M ([0-9.]+) [0-9.]+\s+L ([0-9.]+) [0-9.]+\s*" style="([^"]*)"', svg
    )
    assert spine, "no bottom spine in the SVG"
    assert (float(spine.group(1)), float(spine.group(2))) == (mark(2000), mark(2024))
    assert "stroke-linecap: square" not in spine.group(3) and "stroke-width: 0.72" in spine.group(3)


def test_style_year_axis_aligns_the_visible_end_ticks_on_a_date_axis():
    """Dates are converted to the axis' units; a tick outside the range is not the one aligned."""
    import datetime as dt

    ticks = [dt.date(year, 1, 1) for year in (1995, 2000, 2010, 2020, 2030)]
    fig, ax = _year_axes(ticks, xs=[dt.date(2000, 1, 1), dt.date(2020, 1, 1)])
    style_year_axis(ax, dt.date(2000, 1, 1), dt.date(2020, 1, 1))
    name_furniture(fig)
    alignment = {t.get_loc(): t.label1.get_horizontalalignment() for t in ax.xaxis.get_major_ticks()}
    visible = [alignment[loc] for loc in sorted(alignment) if ax.get_xlim()[0] <= loc <= ax.get_xlim()[1]]
    assert visible == ["left", "center", "right"]
    plt.close(fig)


def test_style_year_axis_centers_a_single_tick():
    fig, ax = _year_axes([2020], xs=[2020, 2020])
    style_year_axis(ax, 2019, 2021)
    assert [t.label1.get_horizontalalignment() for t in ax.xaxis.get_major_ticks()] == ["center"]
    plt.close(fig)


def test_style_time_axis_colors_the_furniture_as_grapher_does():
    """The axis line and marks are #999 (SOLID_TICK_COLOR), the labels #5b5b5b unless the step asks otherwise."""
    from matplotlib.colors import to_hex

    fig, ax = _year_axes([2000, 2010, 2024])
    style_time_axis(ax, 2000, 2024)
    name_furniture(fig)
    assert to_hex(ax.spines["bottom"].get_edgecolor()) == "#999999"
    tick = ax.xaxis.get_major_ticks()[0]
    assert to_hex(tick.tick1line.get_markeredgecolor()) == "#999999"
    assert to_hex(tick.label1.get_color()) == "#5b5b5b"
    style_time_axis(ax, 2000, 2024, label_color="#767676")
    assert to_hex(ax.xaxis.get_major_ticks()[0].label1.get_color()) == "#767676"
    assert style_year_axis is style_time_axis
    plt.close(fig)


def _grid(start, end, interval, target):
    import datetime as dt

    days = _sub_yearly_grid(dt.date.fromisoformat(start), dt.date.fromisoformat(end), interval, target)
    return [(d.isoformat(), label) for d, label in days]


@pytest.mark.parametrize(
    "start,end,interval,target,expected",
    [
        # Mirrors grapher/src/axis/timeAxisTicks.test.ts.
        ("2020-01-01", "2020-09-01", "month", 6, ["Jan 2020", "Mar", "May", "Jul", "Sep"]),
        ("2018-03-01", "2021-11-01", "month", 4, ["2019", "2020", "2021"]),
        ("2007-06-01", "2022-06-01", "month", 6, ["2010", "2015", "2020"]),
        ("2020-01-09", "2026-06-28", "month", 6, ["2021", "2022", "2023", "2024", "2025", "2026"]),
        ("2020-01-05", "2020-08-20", "month", 6, ["Mar 2020", "May", "Jul"]),
        ("2020-01-01", "2021-06-01", "month", 6, ["Jan 2020", "Apr", "Jul", "Oct", "Jan 2021", "Apr"]),
        ("2020-02-01", "2020-11-01", "month", 4, ["Apr 2020", "Jul", "Oct"]),
        ("2020-09-15", "2021-05-01", "month", 3, ["Oct 2020", "Jan 2021", "Apr 2021"]),
        ("2020-02-29", "2020-03-09", "day", 6, ["Mar 1, 2020", "Mar 3", "Mar 5", "Mar 7", "Mar 9"]),
        ("2020-03-01", "2020-04-05", "day", 6, ["Mar 2, 2020", "Mar 9", "Mar 16", "Mar 23", "Mar 30"]),
        ("2020-03-04", "2020-04-02", "day", 2, ["Mar 16, 2020", "Mar 30, 2020"]),
        ("2015-06-15", "2022-03-01", "day", 6, ["2016", "2018", "2020", "2022"]),
        ("2020-12-20", "2021-01-20", "day", 6, ["Dec 21, 2020", "Dec 28", "Jan 4, 2021", "Jan 11", "Jan 18"]),
    ],
)
def test_sub_yearly_grid_matches_grapher(start, end, interval, target, expected):
    assert [label for _, label in _grid(start, end, interval, target)] == expected


def test_quarter_grid_never_steps_below_a_quarter():
    assert _grid("2020-01-01", "2020-12-31", "quarter", 6) == [
        ("2020-01-01", "Jan 2020"),
        ("2020-04-01", "Apr"),
        ("2020-07-01", "Jul"),
        ("2020-10-01", "Oct"),
    ]


def test_time_axis_ticks_keep_to_the_calendar_grid_by_default():
    """A daily axis from Feb 24, 2023 to Nov 19, 2026 shows 2024, 2025, 2026, as grapher does: no dated ends."""
    import datetime as dt

    dates, labels = time_axis_ticks("2023-02-24", "2026-11-19", "day", plot_width_px=815)
    assert labels == ["2024", "2025", "2026"]
    assert dates == [dt.date(2024, 1, 1), dt.date(2025, 1, 1), dt.date(2026, 1, 1)]


def test_time_axis_ticks_label_the_ends_in_grapher_formats_and_drop_colliding_grid_labels():
    dates, labels = time_axis_ticks("2023-02-24", "2026-11-19", "day", plot_width_px=815, label_ends=True)
    assert labels == ["Feb 24, 2023", "2024", "2025", "2026", "Nov 19, 2026"]
    # A quarter reads from its first month to its last; 2019 sits on the start and is dropped.
    _, labels = time_axis_ticks("2019-01-01", "2026-07-01", "quarter", plot_width_px=770, label_ends=True)
    assert labels[0] == "Jan 2019" and labels[-1] == "Sep 2026" and "2019" not in labels
    # A week reads from its Monday to its Sunday.
    _, labels = time_axis_ticks("2024-01-03", "2024-03-27", "week", plot_width_px=770, label_ends=True)
    assert labels[0] == "Jan 1, 2024" and labels[-1] == "Mar 31, 2024"


def test_time_axis_ticks_thin_out_on_a_narrow_plot():
    _, wide = time_axis_ticks("2020-01-01", "2021-06-01", "month", plot_width_px=770)
    _, narrow = time_axis_ticks("2020-01-01", "2021-06-01", "month", plot_width_px=200)
    assert len(narrow) < len(wide) and len(narrow) >= 2


def test_time_axis_ticks_reject_a_year_interval_and_a_backwards_range():
    with pytest.raises(ValueError, match="year_axis_ticks"):
        time_axis_ticks("2020-01-01", "2024-01-01", "year")
    with pytest.raises(ValueError, match="forwards"):
        time_axis_ticks("2024-01-01", "2020-01-01", "month")


def test_style_time_axis_centers_grid_labels_and_insets_only_an_overhanging_one():
    """On a calendar grid every label is centered on its tick; one that would hang past an end is aligned inwards."""
    import datetime as dt

    fig, ax = plt.subplots(figsize=(8.5, 6.38))
    start, end = dt.date(2023, 2, 24), dt.date(2026, 12, 20)
    ax.plot([start, end], [0, 1])
    dates, labels = time_axis_ticks(start, end, "day", plot_width_px=815)
    ax.set_xticks(dates, labels)
    style_time_axis(ax, start, end, inset_ends=False)
    name_furniture(fig)
    assert [t.label1.get_horizontalalignment() for t in ax.xaxis.get_major_ticks()] == ["center"] * len(labels)
    # Marked as a calendar grid, so the verifier does not ask it for the data's first and last date.
    assert ax.xaxis.get_gid() == "axes__x-axis-calendar"
    # A label on a tick right at the end would overhang, so it turns inwards.
    ax.set_xticks([dt.date(2024, 1, 1), end], ["2024", "Dec 20, 2026"])
    style_time_axis(ax, start, end, inset_ends=False)
    assert [t.label1.get_horizontalalignment() for t in ax.xaxis.get_major_ticks()] == ["center", "right"]
    plt.close(fig)
