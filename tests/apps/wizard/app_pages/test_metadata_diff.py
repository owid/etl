"""Tests for the Metadata Diff blast-radius logic (pure, no DB).

These cover the key distinction the tool makes: a changed field that comes from the shared
indicator metadata (propagates to charts / other MDIMs) vs. one that comes from an MDIM-level
override (contained to the MDIM).
"""

from apps.wizard.app_pages.metadata_diff.core import (
    ViewDiff,
    build_view_bundle,
    diff_views,
    override_snippet,
)
from apps.wizard.app_pages.metadata_diff.usage import _indicator_ids_in_mdim_config


def _view(dims, indicators=None, metadata=None):
    view = {"dimensions": dims}
    if indicators is not None:
        view["indicators"] = indicators
    if metadata is not None:
        view["metadata"] = metadata
    return view


def _var(id, description_short=None, description_key=None, name="Var"):
    return {
        "id": id,
        "name": name,
        "titlePublic": None,
        "descriptionShort": description_short,
        "descriptionKey": description_key,
        "descriptionProcessing": None,
        "descriptionFromProducer": None,
    }


def test_indicator_change_flags_affects_indicator():
    """When the indicator's own text changes between envs, the field is flagged as shared."""
    dims = {"metric": "mean"}
    src = build_view_bundle(_view(dims), None, _var(10, description_short="New text"), None)
    tgt = build_view_bundle(_view(dims), None, _var(7, description_short="Old text"), None)

    [diff] = diff_views([src], [tgt])

    assert diff.changed
    assert diff.affects_indicator
    assert "descriptionShort" in diff.indicator_changed_fields
    assert diff.indicator_id == 10  # the staging (source) id, used for the blast-radius lookup


def test_mdim_override_change_is_not_shared():
    """A change coming only from an MDIM view override must NOT be flagged as affecting charts."""
    dims = {"metric": "mean"}
    var = _var(10, description_short="Indicator text")  # identical indicator in both envs
    # View-level overrides are stored camelCased in the DB config (descriptionShort, not description_short).
    src = build_view_bundle(_view(dims, metadata={"descriptionShort": "Override NEW"}), None, var, None)
    tgt = build_view_bundle(_view(dims, metadata={"descriptionShort": "Override OLD"}), None, var, None)

    [diff] = diff_views([src], [tgt])

    assert diff.changed  # the merged text differs...
    assert "descriptionShort" in diff.fields
    assert not diff.affects_indicator  # ...but the indicator itself didn't change
    assert diff.indicator_changed_fields == set()


def test_chart_field_change_is_never_shared():
    """Chart title/subtitle/note are MDIM-local and never count as an indicator change."""
    dims = {"metric": "mean"}
    var = _var(10, description_short="same")
    src = build_view_bundle(_view(dims), None, var, {"title": "New chart title"})
    tgt = build_view_bundle(_view(dims), None, var, {"title": "Old chart title"})

    [diff] = diff_views([src], [tgt])

    assert "chart.title" in diff.fields
    assert not diff.affects_indicator


def test_new_view_does_not_flag_indicator_change():
    """A brand-new MDIM view does not, by itself, change any indicator's metadata."""
    dims = {"metric": "mean"}
    src = build_view_bundle(_view(dims), None, _var(10, description_short="text"), None)

    [diff] = diff_views([src], [])  # no target: view is new

    assert diff.is_new
    assert not diff.affects_indicator


def test_chart_shaped_bundle_diff():
    """A standalone chart = a single bundle with empty dims: indicator text (shared) + chart FAUST (local)."""
    src = build_view_bundle(
        {"dimensions": {}},
        None,
        _var(10, description_key=["BER a", "b"]),
        {"title": "T", "subtitle": "New", "note": "N"},
    )
    tgt = build_view_bundle(
        {"dimensions": {}}, None, _var(7, description_key=["a", "b"]), {"title": "T", "subtitle": "Old", "note": "N"}
    )

    [d] = diff_views([src], [tgt])

    assert d.changed
    assert "descriptionKey" in d.fields  # indicator-level change (shared → affects other charts/MDims)
    assert "chart.subtitle" in d.fields  # chart-config change (local to this chart)
    assert d.affects_indicator and "descriptionKey" in d.indicator_changed_fields
    assert "subtitle" not in d.indicator_changed_fields  # chart FAUST is never an indicator change
    assert d.indicator_id == 10


def test_override_snippet_pins_view_to_baseline():
    """The generator emits a real MDim .py override idiom, routing each field to the right container."""
    v = ViewDiff(dimensions={"decile": "p50", "welfare": "income"}, fields={})

    # descriptionKey (list) -> view.metadata, snake_case key, one bullet per line
    dk = override_snippet(v, "descriptionKey", ["Bullet one.", "Bullet two."])
    assert 'if view.matches(decile="p50", welfare="income"):' in dk
    assert "view.metadata = view.metadata or {}" in dk
    assert 'view.metadata["description_key"] = [' in dk and '"Bullet one.",' in dk

    # titlePublic -> nested under presentation
    assert 'setdefault("presentation", {})["title_public"] = "T"' in override_snippet(v, "titlePublic", "T")

    # chart field -> view.config
    cs = override_snippet(v, "chart.subtitle", "S")
    assert "view.config = view.config or {}" in cs and 'view.config["subtitle"] = "S"' in cs


def test_indicator_ids_in_mdim_config_scans_all_axes():
    config = {
        "views": [
            {"indicators": {"y": [{"id": 1}, {"id": 2}], "x": [3], "color": [{"id": 4}]}},
            {"indicators": {"y": [2, {"id": 5}]}},
            {"indicators": {}},
            {},
        ]
    }
    assert _indicator_ids_in_mdim_config(config) == {1, 2, 3, 4, 5}
