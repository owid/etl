"""Tests for etl.static_viz — the shared contract every static viz has to satisfy.

The point of these is the negative cases. Four of the five shipped static_viz steps violated the
Figma handoff contract in a way that is invisible in the rendered PNG, so a test that only checks
the happy path would have passed on all of them.
"""

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pytest

from etl.static_viz import (
    PIXELS_PER_INCH,
    SVG_HASHSALT,
    TEMPLATES,
    apply_svg_rcparams,
    export_frame,
    nice_year_ticks,
    source_citation,
    unclip,
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


def test_nice_year_ticks_degenerate_range():
    assert nice_year_ticks(2000, 2000) == [2000]
    assert nice_year_ticks(2000, 1990) == [2000]
