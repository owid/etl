"""Tests for the Metadata Diff blast-radius logic (pure, no DB).

These cover the key distinction the tool makes: a changed field that comes from the shared
indicator metadata (propagates to charts / other MDIMs) vs. one that comes from an MDIM-level
override (contained to the MDIM).
"""

from apps.wizard.app_pages.metadata_diff.core import (
    ViewDiff,
    build_view_bundle,
    change_group_identity,
    diff_views,
    distinct_indicator_short_names,
    group_changes,
    override_snippet,
    parse_catalog_path,
    yaml_field_snippet,
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


def test_override_snippet_routes_each_field():
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


def test_parse_catalog_path_resolves_garden_file_and_anchor():
    """The PR brief resolves an indicator catalogPath to (garden dir, table, short_name)."""
    assert parse_catalog_path("grapher/worldbank_wdi/2026-07-27/wdi/wdi#fp_cpi_totl_zg") == (
        "etl/steps/data/garden/worldbank_wdi/2026-07-27/wdi",
        "wdi",
        "fp_cpi_totl_zg",
    )
    # No explicit table segment -> table defaults to the dataset name.
    assert parse_catalog_path("grapher/ns/2020-01-01/ds#col") == ("etl/steps/data/garden/ns/2020-01-01/ds", "ds", "col")
    # Unusable inputs return None (brief falls back to a generic hint).
    assert parse_catalog_path(None) is None
    assert parse_catalog_path("grapher/ns/2020/ds") is None


def test_distinct_indicator_short_names_fingerprints_shared_definition():
    """A change hitting several indicators is the fingerprint of a shared definition/anchor.

    Different flatten suffixes on the same base name collapse to one; genuinely different base names
    (gini vs share_top_1) stay distinct, so >1 signals a shared `definitions.*` edit to the PR brief."""
    # Same indicator, two dimension-flattened columns -> one base short_name.
    assert distinct_indicator_short_names(
        {
            "grapher/wid/2026-06-18/wid/inequality#gini__welfare_type_wealth",
            "grapher/wid/2026-06-18/wid/inequality#gini",
        }
    ) == ["gini"]
    # Two different indicators sharing the identical text -> the shared-definition fingerprint.
    assert distinct_indicator_short_names(
        {
            "grapher/wid/2026-06-18/wid/inequality#gini__welfare_type_wealth",
            "grapher/wid/2026-06-18/wid/inequality#share_top_1__welfare_type_wealth",
        }
    ) == ["gini", "share_top_1"]
    assert distinct_indicator_short_names(set()) == []


def test_group_changes_collects_catalog_paths_across_indicators():
    """A shared text change accumulates every indicator's catalogPath, so the brief can detect a
    shared definition (>1 distinct base short_name) rather than guessing one variable."""
    shared = {"descriptionKey": {"old": ["a"], "new": ["a", "NEW"]}}
    v1 = ViewDiff(
        dimensions={"metric": "gini", "welfare_type": "wealth"},
        fields=shared,
        indicator_id=1,
        catalog_path="grapher/wid/2026-06-18/wid/inequality#gini__welfare_type_wealth",
        indicator_changed_fields={"descriptionKey"},
    )
    v2 = ViewDiff(
        dimensions={"metric": "share_top_1", "welfare_type": "wealth"},
        fields=shared,
        indicator_id=2,
        catalog_path="grapher/wid/2026-06-18/wid/inequality#share_top_1__welfare_type_wealth",
        indicator_changed_fields={"descriptionKey"},
    )
    (group,) = group_changes([v1, v2])
    assert distinct_indicator_short_names(group.catalog_paths) == ["gini", "share_top_1"]
    # Both indicators are collected so the brief can union their blast radius (apply-to-all reaches all).
    assert group.indicator_ids == {1, 2}


def test_yaml_field_snippet_is_pastable():
    """The snippet uses the snake_case metadata key and renders lists as YAML bullets."""
    assert yaml_field_snippet("descriptionShort", "Annual inflation.") == "description_short: Annual inflation."
    dk = yaml_field_snippet("descriptionKey", ["One.", "Two."])
    assert dk.splitlines()[0] == "description_key:"
    assert "- One." in dk and "- Two." in dk


def test_group_changes_collapses_shared_text_and_ranks_by_reach():
    """The review unit: identical changes across views collapse to one group, ranked by reach."""
    shared = {"descriptionKey": {"old": ["a"], "new": ["a", "NEW"]}}
    v1 = ViewDiff(
        dimensions={"m": "mean", "w": "income"},
        fields=shared,
        indicator_id=10,
        indicator_changed_fields={"descriptionKey"},
    )
    v2 = ViewDiff(
        dimensions={"m": "mean", "w": "consumption"},
        fields=shared,
        indicator_id=10,
        indicator_changed_fields={"descriptionKey"},
    )
    v3 = ViewDiff(
        dimensions={"m": "median", "w": "income"}, fields={"descriptionShort": {"old": "x", "new": "y"}}
    )  # override

    groups = group_changes([v1, v2, v3])

    assert len(groups) == 2  # the two identical shared changes collapse into one
    top = groups[0]  # ranked by reach: the 2-view group first
    assert top.field == "descriptionKey" and len(top.view_dims) == 2
    assert top.affects_indicator and top.indicator_id == 10
    assert groups[1].field == "descriptionShort" and not groups[1].affects_indicator


def test_change_group_identity_is_content_bound():
    """Lock-in: same slot keeps its change_key, but any text edit changes content_hash (→ stale)."""
    base = dict(dimensions={"m": "mean"}, indicator_id=10, indicator_changed_fields={"descriptionKey"})
    [g1] = group_changes([ViewDiff(fields={"descriptionKey": {"old": ["a"], "new": ["a", "NEW"]}}, **base)])
    [g2] = group_changes([ViewDiff(fields={"descriptionKey": {"old": ["a"], "new": ["a", "NEW"]}}, **base)])
    [g3] = group_changes([ViewDiff(fields={"descriptionKey": {"old": ["a"], "new": ["a", "EDITED"]}}, **base)])

    k1, h1 = change_group_identity("grapher/x/mdim", g1)
    k2, h2 = change_group_identity("grapher/x/mdim", g2)
    k3, h3 = change_group_identity("grapher/x/mdim", g3)

    assert (k1, h1) == (k2, h2)  # identical change → identical identity (approval persists)
    assert k3 == k1 and h3 != h1  # edited text → same slot key, new hash → stored approval goes stale


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


def test_metadata_signature_detects_text_change_and_ignores_nulls():
    """The MDim list's ✏️ marker compares this signature across environments.

    A text edit usually lands in indicator metadata without touching the MDim config, so comparing
    configs alone would leave the edited MDim unmarked. NULL/NaN/"" must normalize to the same thing,
    or every indicator with an empty optional field would look changed.
    """
    from apps.wizard.app_pages.metadata_diff.data import _metadata_signature

    base = {"name": "V", "titlePublic": None, "descriptionShort": "s", "descriptionKey": '["a"]'}
    assert _metadata_signature(base) == _metadata_signature(dict(base))
    # A changed WYSK must change the signature.
    assert _metadata_signature(base) != _metadata_signature({**base, "descriptionKey": '["b"]'})
    # None, NaN, "" and surrounding whitespace are all the same absence of text.
    assert _metadata_signature(base) == _metadata_signature({**base, "titlePublic": float("nan")})
    assert _metadata_signature(base) == _metadata_signature({**base, "titlePublic": "  "})
    # A missing indicator (absent from the baseline) is a change, not a match.
    assert _metadata_signature(None) != _metadata_signature(base)
