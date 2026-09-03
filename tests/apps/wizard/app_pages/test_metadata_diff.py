"""Tests for the Metadata Diff blast-radius logic (pure, no DB).

These cover the key distinction the tool makes: a changed field that comes from the shared
indicator metadata (propagates to charts / other MDIMs) vs. one that comes from an MDIM-level
override (contained to the MDIM).
"""

import re

import pandas as pd

from apps.owidbot.metadata_diff import format_metadata_diff, status_icon
from apps.wizard.app_pages.metadata_diff.brief import changed_text_lines, decision, garden_location_lines, ship_section
from apps.wizard.app_pages.metadata_diff.core import (
    ChangeGroup,
    ViewDiff,
    build_view_bundle,
    change_group_identity,
    diff_views,
    distinct_garden_datasets,
    distinct_indicator_short_names,
    group_changes,
    override_snippet,
    parse_catalog_path,
    where_note,
    yaml_field_snippet,
)
from apps.wizard.app_pages.metadata_diff.datapage import ordered_slots
from apps.wizard.app_pages.metadata_diff.discovery import (
    EXPLORER_EXPORT_KIND,
    MDIM_EXPORT_KIND,
    BranchScope,
    ExplorerChanges,
    Summary,
    _config_file_collection_name,
    _count_fields,
    _dataset_of,
    _emitted_collection_names,
    _export_kind,
    _export_namespace,
    charts_reached,
    compare_explorer_views,
    compare_indicator_texts,
    mdim_in_branch,
    mdim_namespace,
    mdim_short_name,
    narrow_to_branch,
    split_mdim_groups,
)
from apps.wizard.app_pages.metadata_diff.render import render_text_html
from apps.wizard.app_pages.metadata_diff.review_state import surface_key
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


def test_view_repointed_to_another_indicator_is_not_a_shared_edit():
    """Replacing a view's indicator is not an edit to the replacement's metadata.

    The two `base` bundles then describe two unrelated indicators, and every field they disagree on
    would be reported as a shared indicator edit — sending the reviewer to change garden metadata on an
    indicator nobody touched, and counting every other chart using it as blast radius. The view's own
    text difference is still reported; only the shared-edit claim is withheld.
    """
    dims = {"metric": "mean"}
    src_var = _var(10, description_short="Deaths from drowning") | {
        "catalogPath": "grapher/un/2026-08-19/wpp/tbl#deaths"
    }
    tgt_var = _var(7, description_short="Population") | {"catalogPath": "grapher/un/2026-08-19/wpp/tbl#population"}

    [diff] = diff_views(
        [build_view_bundle(_view(dims), None, src_var, None)], [build_view_bundle(_view(dims), None, tgt_var, None)]
    )

    assert diff.changed  # the view's text really did change...
    assert "descriptionShort" in diff.fields
    assert not diff.affects_indicator  # ...but nobody edited `deaths`
    assert diff.indicator_changed_fields == set()


def test_version_bump_is_still_the_same_indicator():
    """A version bump moves the catalogPath without changing which indicator it is.

    The counterweight to the test above: demanding equal paths would report no indicator-layer change at
    all for a re-versioned dataset — the update this tool exists to review.
    """
    dims = {"metric": "mean"}
    src_var = _var(10, description_short="New text") | {"catalogPath": "grapher/un/2026-08-19/wpp/tbl#population"}
    tgt_var = _var(7, description_short="Old text") | {"catalogPath": "grapher/un/2026-05-01/wpp/tbl#population"}

    [diff] = diff_views(
        [build_view_bundle(_view(dims), None, src_var, None)], [build_view_bundle(_view(dims), None, tgt_var, None)]
    )

    assert diff.affects_indicator
    assert "descriptionShort" in diff.indicator_changed_fields


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


# --- Discovery: what did this branch change? -----------------------------------------------------


def _row(path="grapher/ns/2026-01-01/ds/tb#v", id=1, short="s", key=None, name="V"):
    """A `variables` row as the DB hands it back."""
    return {
        "id": id,
        "name": name,
        "catalogPath": path,
        "titlePublic": None,
        "descriptionShort": short,
        "descriptionKey": key,
        "descriptionProcessing": None,
        "descriptionFromProducer": None,
    }


def test_indicator_comparison_is_keyed_by_catalog_path():
    """Matching indicators by id would report a whole version-bumped dataset as changed.

    A grapher step bumped to a new version mints fresh variable ids on staging, so the ids simply do not
    exist in the baseline. catalogPath is the identifier that survives, and a differing id behind the
    same path is not a change to any text a reader sees.
    """
    path = "grapher/ns/2026-01-01/ds/tb#v"
    source = {path: _row(id=9999, short="Same text.")}
    target = {path: _row(id=1, short="Same text.")}
    assert compare_indicator_texts(source, target).diffs == {}
    # The staging id is still recorded — it is what the blast-radius lookup needs.
    assert compare_indicator_texts(source, target).ids == {path: 9999}

    changed = compare_indicator_texts({path: _row(short="New text.")}, {path: _row(short="Old text.")})
    assert list(changed.diffs) == [path]
    assert changed.diffs[path].fields["descriptionShort"] == {"old": "Old text.", "new": "New text."}
    assert changed.diffs[path].catalog_path == path


def test_indicator_comparison_ignores_empty_variants_and_flags_new():
    """NULL / NaN / "" / whitespace are the same absence of text; a path absent from the baseline is new."""
    path = "grapher/ns/2026-01-01/ds/tb#v"
    source = {path: _row(short=None)}
    assert compare_indicator_texts(source, {path: _row(short=float("nan"))}).diffs == {}
    assert compare_indicator_texts(source, {path: _row(short="   ")}).diffs == {}

    result = compare_indicator_texts(source, {})
    assert result.new_paths == {path}
    # A new indicator has no old text to diff, so it is not reported as a text change.
    assert result.diffs == {}


def test_narrow_to_branch_keeps_only_this_branch_datasets():
    """Without narrowing, a branch lagging master reports master's edits as its own."""
    paths = [
        "grapher/mine/2026-01-01/ds/tb#a",
        "grapher/theirs/2026-01-01/ds/tb#b",
    ]
    narrowed, applied = narrow_to_branch(paths, ["grapher/mine/2026-01-01/ds"])
    assert narrowed == ["grapher/mine/2026-01-01/ds/tb#a"]
    assert applied is True

    # git unavailable: keep everything, but say so, so the UI can warn instead of quietly over-reporting.
    unnarrowed, applied = narrow_to_branch(paths, None)
    assert unnarrowed == paths
    assert applied is False


def _explorer_row(dims, title="T", subtitle="S", note=None):
    return {"dimensions": dims, "title": title, "subtitle": subtitle, "note": note}


def test_explorer_views_compared_per_view_id():
    """Explorer views are matched on (slug, viewId) — the key that is stable across environments."""
    source = {
        ("co2", "v1"): _explorer_row({"Metric": "Total"}, title="New title"),
        ("co2", "v2"): _explorer_row({"Metric": "Per capita"}),
    }
    target = {
        ("co2", "v1"): _explorer_row({"Metric": "Total"}, title="Old title"),
        ("co2", "v2"): _explorer_row({"Metric": "Per capita"}),
    }
    changed = compare_explorer_views(source, target)
    # Only the view whose text moved is reported; the identical one is not.
    assert list(changed) == ["co2"]
    assert len(changed["co2"]) == 1
    assert changed["co2"][0].fields["chart.title"] == {"old": "Old title", "new": "New title"}
    assert changed["co2"][0].dimensions == {"Metric": "Total"}


def test_explorer_view_missing_in_baseline_is_new():
    source = {("co2", "v1"): _explorer_row({"Metric": "Total"})}
    changed = compare_explorer_views(source, {})
    assert changed["co2"][0].is_new is True


def test_summary_field_counts_group_identical_changes():
    """The owidbot line counts *distinct* changes, so one reworded bullet on 50 indicators counts once."""
    diffs = [
        ViewDiff(dimensions={"d": "a"}, fields={"descriptionKey": {"old": ["x"], "new": ["y"]}}),
        ViewDiff(dimensions={"d": "b"}, fields={"descriptionKey": {"old": ["x"], "new": ["y"]}}),
        ViewDiff(dimensions={"d": "c"}, fields={"chart.subtitle": {"old": "p", "new": "q"}}),
    ]
    counts: dict[str, int] = {}
    _count_fields(counts, diffs)
    assert counts == {"WYSK": 1, "Chart subtitle": 1}


# --- Data-page layout (Ed's review-view request) --------------------------------------------------


def test_ordered_slots_follow_the_data_page():
    """Fields are shown where the reader meets them, not in field-name order."""
    slots = ordered_slots({"descriptionKey", "titlePublic"})
    labels = [s.label for s in slots]
    assert labels.index("Title") < labels.index("What you should know about this data")
    # The footnote sits under the chart; everything above it is above the chart.
    assert [s.region for s in slots if s.label == "Footnote"] == ["under"]
    changed = {s.label for s in slots if s.changed}
    assert changed == {"Title", "What you should know about this data"}

    # Only-changed mode drops the placeholders for untouched slots.
    assert all(s.changed for s in ordered_slots({"titlePublic"}, include_unchanged=False))


def test_ordered_slots_keep_an_unplaced_field_visible():
    """A field with no slot yet must still be rendered — dropping a change is the one unacceptable bug."""
    slots = ordered_slots({"someNewField"})
    assert [s.label for s in slots if s.changed] == ["someNewField"]


# --- Reviewed/unreviewed bookkeeping --------------------------------------------------------------


def test_list_review_surfaces_cannot_collide_with_sign_off():
    """A list tick and an Approve/Flag sign-off must never write the same row.

    The Review page keys sign-off on the bare catalogPath (MDims) or `chart:<slug>`; the list toggles are
    namespaced under `list:`, so the two stay independent even for the same change.
    """
    assert surface_key("mdim", "grapher/ns/latest/ds") == "list:mdim:grapher/ns/latest/ds"
    assert surface_key("chart", "daily-mean-income") != "chart:daily-mean-income"
    assert surface_key("explorer", "co2").startswith("list:")


def test_review_mark_is_bound_to_the_text():
    """Editing the text again in the same PR must reopen the change — a tick is never carried over."""
    group = ChangeGroup(field="descriptionKey", old=["a"], new=["b"], view_dims=[{"d": "x"}])
    surface = surface_key("mdim", "grapher/ns/latest/ds")
    change_key, content_hash = change_group_identity(surface, group)

    edited = ChangeGroup(field="descriptionKey", old=["a"], new=["c"], view_dims=[{"d": "x"}])
    edited_key, edited_hash = change_group_identity(surface, edited)
    # Same slot (so the stored row is found), different content (so it reads as stale).
    assert edited_key == change_key
    assert edited_hash != content_hash


# --- PR brief routing ------------------------------------------------------------------------------


def test_brief_decision_routes_from_the_card_tick():
    """The brief reads the one review state there is, so it cannot disagree with the page.

    It used to route on an Approve/Flag sign-off stored apart from these ticks — two records that could
    contradict each other indefinitely, and that nothing read at merge time.
    """
    assert decision({"stale": False, "reviewed": True}) == "reviewed"
    assert decision({"stale": False, "reviewed": False}) == "pending"
    # A change ticked and then edited is not reviewed text, and the brief says so.
    assert decision({"stale": True, "reviewed": False}) == "stale"
    # Rows built before the tick existed carry neither key.
    assert decision({}) == "pending"


# --- Attribution: this branch's change, or the baseline moving on? --------------------------------


def test_branch_scope_separates_data_steps_from_export_recipes():
    """`export://` URIs identify a changed MDim/explorer recipe; everything else is a dataset path."""
    scope = BranchScope(
        dataset_paths={"garden/wid/2026-06-18/world_inequality_database"},
        export_products={(MDIM_EXPORT_KIND, "incomes_wid")},
    )
    assert scope.covers_indicator("grapher/wid/2026-06-18/world_inequality_database/tb#share_top_1")
    assert not scope.covers_indicator("grapher/wb/2026-06-26/world_bank_pip/tb#gini")
    assert scope.covers_export(MDIM_EXPORT_KIND, "incomes_wid")
    assert not scope.covers_export(MDIM_EXPORT_KIND, "poverty_pip")


def test_export_scope_is_per_kind_not_per_name():
    """A recipe name is only unique within its export kind, and `migration_flows` exists in both.

    `export/multidim/migration/latest/migration_flows.py` and
    `export/explorers/migration/latest/migration_flows.py` both exist, so a name-only scope would let an
    edit to the MDim recipe vouch for every differing view of the unrelated explorer.
    """
    assert _export_kind("export://multidim/migration/latest/migration_flows") == MDIM_EXPORT_KIND
    assert _export_kind("export://explorers/migration/latest/migration_flows") == EXPLORER_EXPORT_KIND

    scope = BranchScope(export_products={(MDIM_EXPORT_KIND, "migration_flows")})
    assert scope.covers_export(MDIM_EXPORT_KIND, "migration_flows")
    assert not scope.covers_export(EXPLORER_EXPORT_KIND, "migration_flows")


def test_mdim_short_name_from_catalog_path():
    assert mdim_short_name("grapher/wid/latest/incomes_wid#incomes_wid") == "incomes_wid"
    assert mdim_short_name("wid/latest/incomes_wid") == "incomes_wid"


def test_split_mdim_groups_keeps_config_changes_only_for_our_own_recipe():
    """An MDim we touched is not a licence to attribute *everything* in it to this PR.

    Its view configs are rewritten whenever master rebuilds it, so on an older staging server they differ
    wholesale. Unless the branch changed the MDim's recipe, only indicator-layer changes are ours.
    """
    indicator_change = ViewDiff(
        dimensions={"d": "a"},
        fields={"descriptionKey": {"old": ["x"], "new": ["y"]}},
        indicator_changed_fields={"descriptionKey"},
    )
    config_change = ViewDiff(dimensions={"d": "b"}, fields={"chart.title": {"old": "p", "new": "q"}})

    scope = BranchScope(dataset_paths=set(), export_products=set())
    ours, other = split_mdim_groups("grapher/ns/latest/mine#mine", [indicator_change, config_change], scope)
    assert [g.field for g in ours] == ["descriptionKey"]
    assert [g.field for g in other] == ["chart.title"]

    # When the branch edits the MDim's own recipe, its config-level edits are exactly the point.
    scope_with_recipe = BranchScope(dataset_paths=set(), export_products={(MDIM_EXPORT_KIND, "mine")})
    ours, other = split_mdim_groups("grapher/ns/latest/mine#mine", [indicator_change, config_change], scope_with_recipe)
    assert {g.field for g in ours} == {"descriptionKey", "chart.title"}
    assert other == []


def test_split_mdim_groups_reports_everything_when_git_is_unavailable():
    """No narrowing signal means no attribution — show it all rather than guess it away."""
    config_change = ViewDiff(dimensions={"d": "b"}, fields={"chart.title": {"old": "p", "new": "q"}})
    ours, other = split_mdim_groups("grapher/ns/latest/mine#mine", [config_change], BranchScope(available=False))
    assert len(ours) == 1
    assert other == []


def test_explorer_changes_split_branch_from_lag():
    """The section shows this branch's explorers; the rest stay visible but separate."""
    changes = ExplorerChanges(
        views={"mine": [ViewDiff(dimensions={})], "theirs": [ViewDiff(dimensions={})]},
        in_branch={"mine"},
    )
    assert list(changes.branch_views()) == ["mine"]
    assert list(changes.other_views()) == ["theirs"]

    # Without narrowing, nothing is claimed as lag.
    unnarrowed = ExplorerChanges(views={"a": [ViewDiff(dimensions={})]}, in_branch=set(), narrowed=False)
    assert list(unnarrowed.branch_views()) == ["a"]
    assert unnarrowed.other_views() == {}


def test_export_recipe_scope_reads_the_name_it_publishes():
    """A recipe's file name is not always the collection it publishes, and the slug is what we match on.

    `explorers/emissions/latest/ipcc_scenarios.py` publishes `ipcc-scenarios`; `multidim/un/latest/
    un_wpp.py` publishes `population-and-demography`. Matching the file name alone would file a recipe
    edit of either as baseline lag and drop it from the review.
    """
    assert _emitted_collection_names(
        'explorer = paths.create_collection(config=config, short_name="ipcc-scenarios", explorer=True)'
    ) == {"ipcc-scenarios"}
    assert _emitted_collection_names('    collection_name="population-and-demography",') == {
        "population-and-demography"
    }
    # A recipe that names nothing publishes under its own file name, which is matched anyway.
    assert _emitted_collection_names("c = paths.create_collection(config=config)") == set()

    # The resolved names are what `covers_export` is asked about — either spelling has to answer yes.
    scope = BranchScope(
        dataset_paths=set(),
        export_products={
            (EXPLORER_EXPORT_KIND, "ipcc_scenarios"),
            (EXPLORER_EXPORT_KIND, "ipcc-scenarios"),
        },
    )
    assert scope.covers_export(EXPLORER_EXPORT_KIND, "ipcc-scenarios")
    assert scope.covers_export(EXPLORER_EXPORT_KIND, "ipcc_scenarios")


def test_multi_collection_recipe_names_come_from_its_config_files():
    """A recipe publishing several collections derives their names, so no literal is there to read.

    `multidim/covid/latest/covid.py` builds one collection per `covid.<key>.yml` companion file, naming
    each after the file. Without that, an edit to one of those configs would look like baseline lag.
    """
    assert _config_file_collection_name("covid.cases.yml") == "covid_cases"
    assert _config_file_collection_name("covid.xm_models.yml") == "covid_xm_models"
    # `.config.yml` companions carry the same meaning without the marker segment.
    assert _config_file_collection_name("democracy.eiu.config.yml") == "democracy_eiu"


def test_reach_counts_every_chart_that_can_reach_the_text():
    """A multi-indicator chart has no data page, but its readers can still reach the text.

    "Learn more about this data" opens the sources drawer, where each indicator's own description, WYSK and
    notes are listed. An earlier version of this counted those charts out of every reach number, which
    understated the audience of a WYSK edit on precisely the charts whose readers have to go looking.
    """
    usage = {7: [{"chartId": 1, "has_data_page": True}, {"chartId": 2, "has_data_page": False}]}
    wysk = ChangeGroup(field="descriptionKey", old=["a"], new=["b"], indicator_ids={7})
    assert charts_reached([wysk], usage) == {1, 2}

    title = ChangeGroup(field="titlePublic", old="Old", new="New", indicator_ids={7})
    assert charts_reached([title], usage) == {1, 2}


def test_one_change_reports_one_reach_everywhere():
    """The Charts section, the MDim card, the scope label and the PR brief read the same usage.

    They have to agree, or the identical change reports a different audience depending on which page you
    opened — which is what happened while some of them filtered by "has a data page" and others did not.
    """
    from apps.wizard.app_pages.metadata_diff.core import affected_charts

    usage = {
        7: {
            "charts": [
                {"chartId": 1, "slug": "a", "has_data_page": True},
                {"chartId": 2, "slug": "b", "has_data_page": False},
            ],
            "mdims": [],
        }
    }
    wysk = ChangeGroup(field="descriptionKey", old=["a"], new=["b"], affects_indicator=True, indicator_ids={7})
    assert [c["chartId"] for c in affected_charts(wysk, usage)] == [1, 2]


def test_an_empty_diff_is_not_all_clear_when_indicators_are_new():
    """A version bump replaces every catalog path, so the comparison is empty and nothing is reviewed.

    Green there would wave through a whole dataset's worth of reader-facing text — the same trap
    `Summary.has_changes` closes for the page as a whole.
    """
    from apps.wizard.app_pages.metadata_diff.charts_section import _empty_diff_notice
    from apps.wizard.app_pages.metadata_diff.discovery import IndicatorChanges

    all_clear, message = _empty_diff_notice(IndicatorChanges())
    assert all_clear and "No indicator text changes" in message

    all_clear, message = _empty_diff_notice(IndicatorChanges(new_paths={"grapher/ns/v/d/t#a", "grapher/ns/v/d/t#b"}))
    assert not all_clear
    assert "2 indicators unreviewed" in message


def test_only_the_json_backed_field_is_json_decoded():
    """Every field but WYSK is plain text; decoding one that happens to be valid JSON rewrites it."""
    row = {
        "id": 1,
        "name": "Internal name",
        "titlePublic": "false",
        "descriptionShort": "null",
        "descriptionKey": '["a", "b"]',
        "descriptionProcessing": None,
        "descriptionFromProducer": None,
    }
    bundle = build_view_bundle({"dimensions": {}}, None, row, None)

    # Decoded, these became the boolean False (so the internal name replaced it) and None.
    assert bundle.metadata["titlePublic"] == "false"
    assert bundle.metadata["descriptionShort"] == "null"
    # The one JSON column is still decoded, or every WYSK diff would compare raw JSON strings.
    assert bundle.metadata["descriptionKey"] == ["a", "b"]


def test_group_spanning_two_datasets_names_both_files_and_rebuilds():
    """One group can span two garden datasets: it is keyed on the text, and identical edits collapse.

    Naming only the first dataset would send the author to fix half the change, and leave the second
    dataset unbuilt on staging.
    """
    paths = {
        "grapher/ns_a/2026-01-01/ds_a/ds_a#gdp",
        "grapher/ns_b/2026-01-01/ds_b/ds_b#gdp",
    }
    assert distinct_garden_datasets(paths) == [
        "etl/steps/data/garden/ns_a/2026-01-01/ds_a",
        "etl/steps/data/garden/ns_b/2026-01-01/ds_b",
    ]

    g = ChangeGroup(
        field="descriptionShort",
        old="a",
        new="b",
        affects_indicator=True,
        catalog_path=sorted(paths)[0],
        catalog_paths=paths,
    )
    where = "\n".join(garden_location_lines(g, "2 charts"))
    assert "ds_a.meta.yml" in where and "ds_b.meta.yml" in where
    # Two files cannot share a `definitions.*` block, so that claim must not be made here.
    assert "shared definition" not in where

    ship = "\n".join(ship_section([g], "production"))
    assert "garden/ns_a/2026-01-01/ds_a" in ship and "garden/ns_b/2026-01-01/ds_b" in ship

    # The Charts section's "where to edit" caption says the same thing.
    where_caption = where_note(g.field, g.catalog_paths)
    assert "2 separate garden datasets" in where_caption
    assert "ds_a.meta.yml" in where_caption and "ds_b.meta.yml" in where_caption


def test_reordered_wysk_bullets_are_shown_not_hidden():
    """A reorder edits no bullet, so a membership filter finds nothing while the lists genuinely differ.

    Rendering "(no changes here)" for a change the tool itself detected lets a reviewer sign off without
    ever seeing what moved, so a reorder falls through to the full, positional list.
    """
    old, new = ["alpha", "beta", "gamma"], ["gamma", "alpha", "beta"]

    def _bullets(html: str) -> list[str]:
        return [re.sub(r"<[^>]+>", "", li.split("</li>")[0]) for li in html.split("<li>")[1:]]

    # Each side shows its own full order, so what moved is visible on both.
    assert _bullets(render_text_html(new, old, side="new", changed_only=True)) == new
    assert _bullets(render_text_html(old, new, side="old", changed_only=True)) == old

    # A genuine no-op still says so, rather than printing the whole list for nothing.
    assert "(no changes here)" in render_text_html(["a", "b"], ["a", "b"], side="new", changed_only=True)

    # The brief has to hand the executor an instruction too, not an empty diff block.
    g = ChangeGroup(field="descriptionKey", old=old, new=new)
    lines = "\n".join(changed_text_lines(g))
    assert "reordered" in lines
    assert "- alpha\n- beta\n- gamma" in lines and "+ gamma\n+ alpha\n+ beta" in lines


def test_new_mdim_needs_a_branch_signal_too():
    """Absent from the baseline is not by itself this branch's work.

    A staging server materializes master's rebuilds as well, so an MDim master added after the baseline
    was published shows up here untouched by this PR. It needs the same recipe signal a config change
    needs — and either way it stays in `has_changes`, so nothing is dropped from the page.
    """
    df = pd.DataFrame(
        {
            "catalogPath": ["grapher/ns/latest/mine#mine", "grapher/ns/latest/theirs#theirs"],
            "is_new": [True, True],
            "config_changed": [False, False],
            "indicator_changed": [False, False],
        }
    )
    scope = BranchScope(export_products={(MDIM_EXPORT_KIND, "mine")})
    own_recipe = df["catalogPath"].map(lambda cp: scope.covers_export(MDIM_EXPORT_KIND, mdim_short_name(str(cp))))
    assert list(mdim_in_branch(df, own_recipe)) == [True, False]

    # An indicator-layer change is already branch-narrowed, so it stands on its own.
    df["indicator_changed"] = [False, True]
    assert list(mdim_in_branch(df, own_recipe)) == [True, True]


def test_explorer_attribution_is_per_view_not_per_slug():
    """One qualifying view must not vouch for the rest of a lagging explorer's views.

    A master rebuild moves every view of an explorer. If a single view of it renders an indicator this
    branch edited, only that view is this branch's — the others are lag, and are reported as such.
    """
    ours, lag = ViewDiff(dimensions={"v": "1"}), ViewDiff(dimensions={"v": "2"})
    changes = ExplorerChanges(
        views={"mixed": [ours, lag]},
        in_branch={"mixed"},
        branch_view_diffs={"mixed": [ours]},
        other_view_diffs={"mixed": [lag]},
    )
    assert changes.branch_views() == {"mixed": [ours]}
    assert changes.other_views() == {"mixed": [lag]}


def test_summary_counts_new_indicators_as_something_to_review():
    """A version bump gives every indicator a fresh catalog path, so nothing has a baseline to diff."""
    assert not Summary().has_changes
    assert Summary(n_new_indicators=3).has_changes
    assert status_icon(Summary(n_new_indicators=3)) == "✏️"
    # ... and the report says so, instead of returning "No metadata text changes." before that line.
    assert "New indicators: 3" in format_metadata_diff(Summary(n_new_indicators=3))


def test_dataset_of_indicator_path_matches_datasets_table():
    """`datasets.catalogPath` has no channel prefix and stops at the dataset."""
    assert _dataset_of("grapher/wid/2026-06-18/world_inequality_database/tb#share_top_1") == (
        "wid/2026-06-18/world_inequality_database"
    )
    assert _dataset_of("garden/wb/2026-06-26/world_bank_pip/poverty#headcount") == "wb/2026-06-26/world_bank_pip"


def test_candidate_selection_needs_both_git_scope_and_a_rebuild_here():
    """Attribution needs both signals; either alone credits other people's work to this branch.

    A changed file expands into its whole downstream subgraph, so the git scope is far wider than what the
    branch touched — measured on a one-line metadata edit: 118 datasets in scope, 9 actually rebuilt on
    the server. Attributing on scope alone put 526 differences from an unrelated data page in this
    branch's list. "Rebuilt here" alone is not enough either: an automatic job can refresh a dataset on
    this server without the branch asking for it.
    """
    from apps.wizard.app_pages.metadata_diff.discovery import BranchScope, select_candidates

    ours = "grapher/wb/2026-06-26/world_bank_pip/poverty#headcount"
    downstream = "grapher/un/2026-01-01/un_wpp/tb#population"  # in scope via the DAG, never rebuilt here
    automatic = "grapher/covid/latest/cases/tb#cases"  # rebuilt here by a job, not in this branch's scope

    scope = BranchScope(
        dataset_paths={"garden/wb/2026-06-26/world_bank_pip", "garden/un/2026-01-01/un_wpp"},
        export_products=set(),
    )
    built = {"wb/2026-06-26/world_bank_pip", "covid/latest/cases"}

    selected, narrowed = select_candidates([ours, downstream, automatic], scope, built)
    assert selected == [ours]
    assert narrowed is True


def test_candidate_selection_without_git_still_requires_a_rebuild_here():
    """With no git signal the scope filter is skipped, but "rebuilt here" still applies."""
    from apps.wizard.app_pages.metadata_diff.discovery import BranchScope, select_candidates

    ours = "grapher/wb/2026-06-26/world_bank_pip/poverty#headcount"
    theirs = "grapher/un/2026-01-01/un_wpp/tb#population"
    selected, narrowed = select_candidates(
        [ours, theirs], BranchScope(available=False), {"wb/2026-06-26/world_bank_pip"}
    )
    assert selected == [ours]
    # False so the UI can say the list is not narrowed to the branch.
    assert narrowed is False


# --- Where a difference came from -----------------------------------------------------------------


def test_origins_call_a_change_ours_only_when_master_does_not_have_it():
    """The verdict is per text, so a real change is no longer hedged just because master touched the dataset.

    The previous rule was dataset-level: "this server rebuilt it AND master edited it" meant every change
    in that dataset got a warning. On a normal branch that is most of them — it fired on all ten changes
    of a PR whose ten changes were all its own. Comparing against master's own server answers it directly.
    """
    from apps.wizard.app_pages.metadata_diff.discovery import MASTER, OURS, classify_origins

    mine = "grapher/wb/2026-06-26/world_bank_pip/poverty#headcount"
    theirs = "grapher/wb/2026-06-26/world_bank_pip/poverty#headcount_ratio"

    origins = classify_origins([mine, theirs], identical_to_master={theirs}, stale={}, master_checked=True)
    assert origins[mine] == OURS
    assert origins[theirs] == MASTER


def test_origins_are_unknown_when_master_cannot_be_reached():
    """No master server is a reason to say so, not to guess — and never to claim the change is ours."""
    from apps.wizard.app_pages.metadata_diff.discovery import UNKNOWN, classify_origins

    path = "grapher/wb/2026-06-26/world_bank_pip/poverty#headcount"
    assert classify_origins([path], set(), {}, master_checked=False) == {path: UNKNOWN}


def test_a_stale_dataset_outranks_every_other_verdict():
    """A stale build inverts the diff, so nothing else about that change is worth acting on first."""
    from apps.wizard.app_pages.metadata_diff.discovery import STALE, classify_origins

    path = "grapher/wid/2026-06-18/world_inequality_database/tb#share_top_1"
    stale = {"wid/2026-06-18/world_inequality_database": ("2026-08-05", "2026-08-17")}
    # Identical to master AND stale: still STALE, because the rebuild has to happen before the rest means anything.
    origins = classify_origins([path], identical_to_master={path}, stale=stale, master_checked=True)
    assert origins[path] == STALE


def test_stale_datasets_are_those_this_server_built_earlier_than_the_baseline(monkeypatch):
    """Only "we are behind" counts — being ahead is the normal state of a branch that changed something.

    And only for a dataset this server actually rebuilt. One it never rebuilt is older here for the
    ordinary reason (the baseline moved on after the fork), which no rebuild of ours fixes — counting it
    would flag most long-lived servers as broken.
    """
    from datetime import datetime

    from apps.wizard.app_pages.metadata_diff import discovery

    created = datetime(2026, 8, 5, 9, 0)
    behind = datetime(2026, 8, 5, 13, 54)
    ahead = datetime(2026, 8, 19, 9, 29)
    baseline = datetime(2026, 8, 17, 18, 2)
    never_built_here = datetime(2026, 7, 1, 10, 0)  # predates the fork: cloned from the baseline, untouched

    times = {
        "here": {
            "ns/v/stale": behind,
            "ns/v/ours": ahead,
            "ns/v/same": baseline,
            "ns/v/only_here": ahead,
            "ns/v/untouched": never_built_here,
        },
        "there": {
            "ns/v/stale": baseline,
            "ns/v/ours": baseline,
            "ns/v/same": baseline,
            "ns/v/untouched": baseline,  # the baseline rebuilt it after the fork — not our staleness
        },
    }
    monkeypatch.setattr(discovery, "dataset_edit_times", lambda engine: times[engine])
    monkeypatch.setattr(discovery, "_staging_creation_time", lambda engine: created)

    stale = discovery.stale_datasets("here", "there")
    assert set(stale) == {"ns/v/stale"}
    assert stale["ns/v/stale"] == (behind, baseline)


def test_owidbot_leads_with_a_stale_server_and_flags_it_in_the_icon():
    """A stale server makes every count below it untrustworthy, so it cannot be a footnote."""
    from apps.owidbot.metadata_diff import format_metadata_diff, status_icon
    from apps.wizard.app_pages.metadata_diff.discovery import Summary

    clean = Summary(n_charts=3, n_indicators=2, fields={"WYSK": 1})
    assert status_icon(clean) == "✏️"
    assert "behind on" not in format_metadata_diff(clean)

    stale = Summary(
        n_charts=3,
        n_indicators=2,
        fields={"WYSK": 1},
        stale={"wid/2026-06-18/world_inequality_database": ("2026-08-05", "2026-08-17")},
    )
    assert status_icon(stale) == "🚧"
    body = format_metadata_diff(stale)
    assert "behind on 1 dataset" in body
    assert body.index("behind on") < body.index("Charts:")  # it leads

    # And it survives the no-changes path, where a stale build may be the reason there are none.
    assert "behind on 1 dataset" in format_metadata_diff(Summary(stale=stale.stale))


def test_prominence_is_labelled_rather_than_deducted():
    """The data-page distinction survives as *where* the text appears, not whether it appears.

    Only a data-page-only field can be behind the drawer: a title or short description feeds the chart
    itself, so it is on the canvas of every chart regardless.
    """
    from apps.wizard.app_pages.metadata_diff.core import behind_sources_drawer, charts_behind_drawer

    on_page = {"chartId": 1, "has_data_page": True}
    drawer_only = {"chartId": 2, "has_data_page": False}

    assert behind_sources_drawer({"descriptionKey"}, drawer_only)
    assert not behind_sources_drawer({"descriptionKey"}, on_page)
    # A field the chart renders itself is never behind the drawer, whatever the chart is.
    assert not behind_sources_drawer({"titlePublic"}, drawer_only)
    # Nor is a mixed set: something in it is on the canvas.
    assert not behind_sources_drawer({"descriptionKey", "titlePublic"}, drawer_only)

    assert [c["chartId"] for c in charts_behind_drawer({"descriptionKey"}, [on_page, drawer_only])] == [2]


def test_export_products_only_covers_recipes_the_branch_edited():
    """A data-step edit expands into every export downstream of it, none of whose configs we wrote.

    `covers_export` is what lets config-level MDim and explorer text count as this branch's work. Fed
    the whole downstream subgraph, one garden metadata edit would vouch for every MDim built on that
    dataset, and `split_mdim_groups` would hand the reviewer a wholesale config diff nobody authored —
    the exact failure that split exists to prevent.
    """
    from etl.io import get_directly_changed_export_uris

    data_step = "etl/steps/data/garden/wb/2026-06-26/world_bank_pip.meta.yml"
    recipe = "etl/steps/export/multidim/wb/latest/poverty_pip.py"

    assert get_directly_changed_export_uris({data_step: "M", recipe: "M"}) == [
        "export://multidim/wb/latest/poverty_pip"
    ]
    # A branch touching only a data step claims no export recipe at all.
    assert get_directly_changed_export_uris({data_step: "M"}) == []


def test_a_change_with_no_visible_chart_reach_still_counts_as_a_change():
    """`n_charts` is reach, not existence — and a real change can legitimately reach nobody.

    A WYSK edit on an indicator that only feeds multi-indicator charts has a blast radius of zero
    readers, but the Charts section still renders it as a change to review. Keying the verdict off the
    filtered reach put "No metadata text changes" and a green all-clear over exactly that.
    """
    assert Summary(n_charts=0, n_indicators=1, n_chart_changes=1).has_changes
    # Nothing anywhere is still nothing.
    assert not Summary().has_changes
    # And the pre-existing reasons to speak up are unaffected.
    assert Summary(n_charts=3).has_changes
    assert Summary(n_new_indicators=2).has_changes
    assert Summary(n_mdims=1).has_changes
    assert Summary(n_explorers=1).has_changes


def test_shared_metadata_file_credits_its_sibling_steps():
    """`shared.meta.yml` is merged into every sibling step, but is itself no step at all.

    It resolves to a `.../shared` path that is in no DAG, so the scope came back empty — and an empty
    scope narrows away every rebuilt indicator, reporting "no metadata text changes" for an edit that
    rewrote text across every dataset in the folder.
    """
    from apps.wizard.app_pages.metadata_diff.discovery import _shared_step_file_datasets

    reached = _shared_step_file_datasets({"etl/steps/data/garden/ihme_gbd/2026-02-07/shared.meta.yml": "M"})
    assert len(reached) > 1
    assert "garden/ihme_gbd/2026-02-07/gbd_cause_deaths" in reached
    assert all(p.startswith("garden/ihme_gbd/2026-02-07/") for p in reached)
    # `shared` is not a step, so it must not be credited as one.
    assert "garden/ihme_gbd/2026-02-07/shared" not in reached

    # A file that *is* a step needs no expansion — the subgraph walk already covers it.
    assert (
        _shared_step_file_datasets({"etl/steps/data/garden/ihme_gbd/2026-02-07/gbd_cause_deaths.meta.yml": "M"})
        == set()
    )
    # Files outside a step folder are not ours to expand.
    assert _shared_step_file_datasets({"apps/wizard/app_pages/metadata_diff/core.py": "M"}) == set()


def test_a_draft_mdim_is_marked_rather_than_filtered_away(monkeypatch):
    """Grapher serves an MDim only when `published = 1`, so a draft's text reaches no reader.

    38 of the 78 MDims on this branch's staging server are drafts, and one of the two the branch changes is
    among them — so counting drafts reported twice the reader-facing work that exists. But filtering them
    out of the query removed them from the list and the "other differences" section as well, and a branch
    whose only change is a draft MDim then read as "no metadata text changes". They are kept and marked, so
    the count stays reader-facing while the review still sees them.
    """
    import pandas as pd

    from apps.wizard.app_pages.metadata_diff import discovery

    def fake_mdim_list(engine):
        return pd.DataFrame(
            {
                "catalogPath": ["ns/latest/live#live", "ns/latest/draft#draft"],
                "configMd5": ["here" if engine == "source" else "there"] * 2,
                "published": [1, 0],
                "slug": ["live", "draft"],
            }
        )

    def no_db(engine):
        raise RuntimeError("no database in this test")

    monkeypatch.setattr(discovery, "mdim_list", fake_mdim_list)
    monkeypatch.setattr(discovery, "_load_configs", no_db)
    monkeypatch.setattr(discovery, "branch_scope", lambda: discovery.BranchScope(available=False))

    df = discovery.mdim_changes_df("source", "target")
    # Both are present — nothing is dropped at the query — and only the unpublished one is marked.
    assert set(df.index) == {"ns/latest/live#live", "ns/latest/draft#draft"}
    assert not bool(df.loc["ns/latest/live#live", "is_draft"])
    assert bool(df.loc["ns/latest/draft#draft", "is_draft"])
    # Both still register as changed against the baseline, which is what puts them in front of a reviewer.
    assert bool(df.loc["ns/latest/draft#draft", "config_changed"])


def test_package_step_files_credit_the_step_they_live_in():
    """A step implemented as a package keeps its metadata *inside* the step folder, not beside it.

    `garden/democracy/2026-03-17/vdem` is one of six active steps that are packages, and its 200 kB of
    reader-facing text sits in `.../vdem/vdem.meta.yml` — one level below the sibling layout. That strips
    to `.../vdem/vdem`, a path in no DAG, so the scope came back empty; and an empty scope narrows away
    every rebuilt indicator, reporting "no metadata text changes" for an edit that rewrote a whole
    dataset's text.
    """
    from apps.wizard.app_pages.metadata_diff.discovery import _shared_step_file_datasets

    step = "garden/democracy/2026-03-17/vdem"
    assert _shared_step_file_datasets({f"etl/steps/data/{step}/vdem.meta.yml": "M"}) == {step}
    # The package's own module names no step of its own either, so it needs the same resolution.
    assert _shared_step_file_datasets({f"etl/steps/data/{step}/__init__.py": "M"}) == {step}

    # The *nearest* ancestor wins: a package file credits its own step, not every step in the version
    # folder. `who/latest/monkeypox` shares its folder with three unrelated steps that it must not claim.
    assert _shared_step_file_datasets({"etl/steps/data/garden/who/latest/monkeypox/__init__.py": "M"}) == {
        "garden/who/latest/monkeypox"
    }

    # A file nested in a folder that names *no* step falls back to the folder-wide credit, exactly as a
    # flat `shared.py` does — a helper sub-package serves the same siblings.
    reached = _shared_step_file_datasets({"etl/steps/data/garden/owid/latest/key_indicators/table_population.py": "M"})
    assert reached
    assert all(p.startswith("garden/owid/latest/") for p in reached)


def test_review_widget_state_is_bound_to_the_text_it_marked():
    """A mark must not inherit through the reviewer's open session when the text is edited.

    `change_key` identifies the slot and deliberately survives an edit, so widget state keyed on it alone
    kept the previous mark in session across the edit — and the next save wrote it back against the new
    content hash, so text nobody had reviewed read as reviewed. The Approve/Flag keys this used to check
    are gone with that workflow; the Reviewed toggle is the control that carries the risk now.
    """
    from apps.wizard.app_pages.metadata_diff.review_state import ReviewMark, reviewed_toggle_key

    group = ChangeGroup(field="titlePublic", old="Old", new="New", view_dims=[{"sex": "female"}])
    edited = ChangeGroup(field="titlePublic", old="Old", new="New, revised", view_dims=[{"sex": "female"}])

    key, content_hash = change_group_identity("grapher/ns/latest/mine#mine", group)
    key_after, hash_after = change_group_identity("grapher/ns/latest/mine#mine", edited)

    # The slot is the same; only the content moved. That is the whole trap.
    assert key_after == key
    assert hash_after != content_hash

    surface = surface_key("mdim", "grapher/ns/latest/mine#mine")
    before = ReviewMark(group=group, change_key=key, content_hash=content_hash, reviewed=True, stale=False)
    after = ReviewMark(group=edited, change_key=key_after, content_hash=hash_after, reviewed=False, stale=True)
    assert reviewed_toggle_key(surface, before) != reviewed_toggle_key(surface, after)


def test_export_scope_is_per_namespace_not_just_per_name():
    """A recipe file name is not unique within one export kind either.

    `multidim/emissions/latest/air_pollution.py` and `multidim/ihme_gbd/latest/air_pollution.py` both
    publish an MDim whose catalogPath ends in `air_pollution`, so matching on kind+name alone let an edit
    to one vouch for the other. On a lagging staging server that presents a whole MDim's worth of
    config-level text nobody in the PR wrote as this branch's work.
    """
    assert _export_namespace("export://multidim/ihme_gbd/latest/air_pollution") == "ihme_gbd"
    assert mdim_namespace("grapher/ihme_gbd/latest/air_pollution#air_pollution") == "ihme_gbd"

    scope = BranchScope(
        export_products={(MDIM_EXPORT_KIND, "air_pollution")},
        export_namespaces={(MDIM_EXPORT_KIND, "air_pollution"): {"emissions"}},
    )
    assert scope.covers_mdim("grapher/emissions/latest/air_pollution#air_pollution")
    assert not scope.covers_mdim("grapher/ihme_gbd/latest/air_pollution#air_pollution")
    # A different name is no match either way.
    assert not scope.covers_mdim("grapher/emissions/latest/ceds#ceds")

    # An explorer slug carries no namespace, so it keeps matching on kind and name alone.
    assert BranchScope(export_products={(EXPLORER_EXPORT_KIND, "ipcc-scenarios")}).covers_export(
        EXPLORER_EXPORT_KIND, "ipcc-scenarios"
    )


def test_shared_export_helper_credits_its_sibling_recipes():
    """A recipe's helpers are not recipes — the export mirror of the shared-metadata blind spot.

    `explorers/un/latest/un_wpp.py` imports its siblings `utils.py` and `view_edits.py` and reads
    `map_brackets.yml`. None is a step, so the changed-export scan derives `.../utils`, a recipe that
    exists in no DAG, and the explorer's own text differences get filed as baseline lag instead.
    """
    from apps.wizard.app_pages.metadata_diff.discovery import _export_scope_names, _shared_export_recipe_uris

    for helper in ("utils.py", "view_edits.py", "map_brackets.yml"):
        reached = _shared_export_recipe_uris({f"etl/steps/export/explorers/un/latest/{helper}": "M"})
        assert reached == {"export://explorers/un/latest/un_wpp"}, helper

    # And the recipe it credits answers to the slug the explorer actually publishes under.
    assert "population-and-demography" in _export_scope_names("export://explorers/un/latest/un_wpp")

    # A file that *is* a recipe, and a config companion resolving to one, need no expansion.
    assert _shared_export_recipe_uris({"etl/steps/export/explorers/un/latest/un_wpp.py": "M"}) == set()
    assert (
        _shared_export_recipe_uris({"etl/steps/export/explorers/un/latest/un_wpp.sex_ratio.config.yml": "M"}) == set()
    )
    # Files outside an export folder are not ours to expand.
    assert _shared_export_recipe_uris({"apps/wizard/app_pages/metadata_diff/core.py": "M"}) == set()


def test_shared_export_helper_credits_only_the_recipes_that_use_it():
    """A folder is not a consumer list: an untouched sibling must not be credited.

    `multidim/un/latest` holds three recipes, and only `un_wpp.py` reaches the helpers — by import
    (`utils`, `view_edits`) and by name (`map_brackets.yml`). Crediting `child_labor` and
    `hazardous_work` too would flip `covers_mdim` for them, and that alone makes `split_mdim_groups`
    hand over every config difference in those MDims as this branch's work. Unlike the data-step mirror
    there is no second gate to catch it: no rebuilt-here check, no master cross-check.
    """
    from apps.wizard.app_pages.metadata_diff.discovery import _recipes_using, _shared_export_recipe_uris

    for helper in ("utils.py", "view_edits.py", "map_brackets.yml"):
        reached = _shared_export_recipe_uris({f"etl/steps/export/multidim/un/latest/{helper}": "M"})
        assert reached == {"export://multidim/un/latest/un_wpp"}, helper

    # "No recipe names it" means "cannot tell" — a helper reached only via another helper falls back to
    # the whole folder, keeping the reviewer's own edit visible rather than dropping it.
    siblings = {"export://multidim/un/latest/child_labor", "export://multidim/un/latest/hazardous_work"}
    assert _recipes_using("utils.py", siblings) == set()


def test_export_scope_without_recorded_namespaces_still_matches_on_name():
    """Narrowing must not get *stricter* by accident: unknown namespace means fall back to the name."""
    scope = BranchScope(export_products={(MDIM_EXPORT_KIND, "mine")})
    assert scope.covers_mdim("grapher/ns/latest/mine#mine")
    # A path too short to carry a namespace resolves to None, which also falls back to the name.
    assert mdim_namespace("mine") is None
    assert scope.covers_export(MDIM_EXPORT_KIND, "mine", mdim_namespace("mine"))


def test_reviewed_toggle_key_is_bound_to_the_text_it_ticked():
    """The list toggle is the same stale-state trap the sign-off widgets had.

    `change_key` survives an edit, so a key without the content hash kept the tick reading "Reviewed" in
    an open session while the stored mark and the stale caption both said unreviewed.
    """
    from apps.wizard.app_pages.metadata_diff.review_state import ReviewMark, reviewed_toggle_key

    group = ChangeGroup(field="titlePublic", old="Old", new="New", view_dims=[{"sex": "female"}])
    edited = ChangeGroup(field="titlePublic", old="Old", new="New, revised", view_dims=[{"sex": "female"}])
    key, content_hash = change_group_identity("grapher/ns/latest/mine#mine", group)
    key_after, hash_after = change_group_identity("grapher/ns/latest/mine#mine", edited)

    # Same slot, moved content — exactly the case the key has to tell apart.
    assert key_after == key
    before = ReviewMark(group=group, change_key=key, content_hash=content_hash, reviewed=True, stale=False)
    after = ReviewMark(group=edited, change_key=key_after, content_hash=hash_after, reviewed=False, stale=True)
    assert reviewed_toggle_key("surface", before) != reviewed_toggle_key("surface", after)


def test_indicator_identity_ignores_only_the_version():
    """A version bump is the same indicator; another dataset's same-named indicator is not.

    Short names are unique only within a dataset, and the common ones (`gini`, `population`, `share`) are
    common exactly where repointing an MDim view between sources is plausible — so comparing the
    `#short_name` tail alone would call a replacement an edit and report garden metadata nobody touched.
    """
    from apps.wizard.app_pages.metadata_diff.core import _same_indicator

    bumped_old = "grapher/un/2026-05-01/wpp/tbl#population"
    bumped_new = "grapher/un/2026-08-19/wpp/tbl#population"
    assert _same_indicator(bumped_old, bumped_new)

    # Same short name, different dataset: a replacement, not an edit.
    assert not _same_indicator(
        "grapher/wb/2026-06-26/world_bank_pip/inequality#gini",
        "grapher/wid/2026-06-18/world_inequality_database/wid#gini",
    )
    # Same dataset and version, different indicator.
    assert not _same_indicator(
        "grapher/wb/2026-06-26/world_bank_pip/poverty#headcount",
        "grapher/wb/2026-06-26/world_bank_pip/poverty#headcount_ratio",
    )
    # A channel prefix must not affect identity, and an unknown path stays comparable.
    assert _same_indicator("garden/un/2026-05-01/wpp/tbl#population", bumped_new)
    assert _same_indicator(None, bumped_new)


def test_a_draft_mdim_is_reported_but_not_counted_as_reader_facing():
    """An unpublished MDim shows readers nothing — but the branch still changed its text.

    Filtering drafts out of the query removed them from the list, the counts and the "other differences"
    section at once, so a PR whose only change was a draft MDim read as "No metadata text changes". They
    are now counted separately and labelled, the same way every other not-reader-facing case is.
    """
    from apps.owidbot.metadata_diff import format_metadata_diff, status_icon
    from apps.wizard.app_pages.metadata_diff.discovery import Summary

    draft_only = Summary(n_draft_mdims=1)
    assert draft_only.has_changes
    assert status_icon(draft_only) == "✏️"
    body = format_metadata_diff(draft_only)
    assert "Unpublished MDims changed: 1" in body
    assert "No metadata text changes" not in body

    # A draft never inflates the reader-facing MDim count.
    both = Summary(n_mdims=2, n_draft_mdims=3)
    assert "MDims: 2" in format_metadata_diff(both)


def test_a_branch_owned_draft_is_not_also_reported_as_baseline_lag():
    """A draft this branch changed belongs to the branch, so it is not a difference someone else caused.

    `n_other_mdims` was every changed MDim minus the *published* in-branch ones, which left the branch's own
    drafts in the baseline-lag bucket: the PR comment then reported the same MDim twice, once as
    "Unpublished MDims changed" and once as "a further difference ... not this branch". The MDims list had
    always used the right set (`has_changes & ~in_branch`); only the counter disagreed with it.
    """
    import pandas as pd

    from apps.wizard.app_pages.metadata_diff import discovery

    df = pd.DataFrame(
        {
            "has_changes": [True, True, True],
            "in_branch": [True, True, False],
            "is_draft": [False, True, False],
        },
        index=["ours/published", "ours/draft", "master/rebuilt"],
    )
    reader_facing = df["in_branch"] & ~df["is_draft"]
    flagged = list(df.index[reader_facing])
    # Only the MDim nobody in the branch touched is baseline lag — not the branch's own draft.
    assert int((df["has_changes"] & ~df["in_branch"]).sum()) == 1
    # The old arithmetic counted the draft as well, which is what double-reported it.
    assert int(df["has_changes"].sum()) - len(flagged) == 2
    assert list(df.index[df["has_changes"] & ~df["in_branch"]]) == ["master/rebuilt"]
    assert discovery.Summary(n_other_mdims=1).n_other == 1


def test_too_many_draft_mdims_reports_a_ceiling_instead_of_a_truncated_count():
    """Resolving drafts is capped, and a capped count must say it is a ceiling.

    The published path already reports `len(flagged)` and sets `mdims_resolved = False` when it cannot diff
    view by view. The draft path instead sliced to the first `MAX_MDIMS_RESOLVED` and reported whatever it
    found there as exact — an undercount with nothing to signal it. The flag is separate from the published
    one, so overflowing drafts never mislabel a reader-facing count that resolved fine.
    """
    from apps.owidbot.metadata_diff import format_metadata_diff
    from apps.wizard.app_pages.metadata_diff.discovery import MAX_MDIMS_RESOLVED, Summary

    capped = Summary(n_mdims=2, n_draft_mdims=MAX_MDIMS_RESOLVED + 9, draft_mdims_resolved=False)
    body = format_metadata_diff(capped)
    assert f"Unpublished MDims changed: {MAX_MDIMS_RESOLVED + 9} (flagged; too many to resolve view by view)" in body
    # The reader-facing count resolved cleanly, so it carries no ceiling qualifier of its own.
    assert "MDims: 2</li>" in body

    # And the usual case stays unqualified.
    assert "Unpublished MDims changed: 3 — no reader sees them yet" in format_metadata_diff(Summary(n_draft_mdims=3))


def test_a_retired_export_recipe_does_not_claim_the_live_product():
    """A recipe file left in the tree after its DAG entry moved on publishes nothing — and must vouch for nothing.

    `etl/steps/export/explorers/wash/2024-02-15/water_and_sanitation.py` is in no DAG; only
    `export://explorers/wash/latest/water_and_sanitation` is. The derived URI still reads like the live
    product, because the scope names are taken from the recipe's own source, so editing the retired file put
    `water-and-sanitation` in scope and filed the live explorer's baseline lag as this branch's work — the
    one bucket a reviewer will not search for their own edit in.

    Erring narrow is right on the export side: `covers_export` has no second gate, and a dropped URI is
    still reported under "other differences" rather than lost.
    """
    from apps.wizard.app_pages.metadata_diff.discovery import _active_export_uris, _export_scope_names

    active = _active_export_uris()
    assert "export://explorers/wash/latest/water_and_sanitation" in active
    retired = "export://explorers/wash/2024-02-15/water_and_sanitation"
    assert retired not in active
    # The retired recipe really does still answer to the live explorer's name — hence the filter.
    assert "water-and-sanitation" in _export_scope_names(retired)


def test_filtering_to_active_export_recipes_drops_no_real_recipe():
    """The filter must not narrow away a recipe the branch legitimately edited.

    Every export step file that names an active DAG step has to survive it, or a real recipe edit would be
    misfiled as baseline lag — the failure this whole scope exists to prevent, in the other direction.

    A step is a `.py`, or — since ETL-authored single charts arrived — a YAML-only `<recipe>.config.yml`
    with no script at all. Deriving the universe from scripts alone reported every one of those as a
    recipe the filter had invented.
    """
    from apps.wizard.app_pages.metadata_diff.discovery import _active_export_uris, _export_scope_names
    from etl.paths import STEP_DIR

    active = _active_export_uris()
    derived = set()
    for path in (STEP_DIR / "export").rglob("*"):
        if path.suffix not in (".py", ".yml", ".yaml") or path.name == "__init__.py":
            continue
        rel = path.relative_to(STEP_DIR / "export")
        derived.add("export://" + (rel.parent / rel.name.split(".")[0]).as_posix())
    # Every active step is reachable from a file in the tree, so the filter keeps all of them.
    assert active <= derived
    assert active & derived == active

    # A YAML-only recipe still answers to its own name, so an edit to its config is not filed as lag.
    yaml_only = [
        uri
        for uri in active
        if not (STEP_DIR / "export" / f"{uri.removeprefix('export://')}.py").exists()
        and (STEP_DIR / "export" / f"{uri.removeprefix('export://')}.config.yml").exists()
    ]
    for uri in yaml_only:
        assert uri.rsplit("/", 1)[-1] in _export_scope_names(uri)


# --- Bullet-level diffing -------------------------------------------------------------------------


def _highlighted(html: str) -> list[str]:
    """The text inside <ins>/<del> spans — what a reviewer's eye is drawn to."""
    import re

    return re.findall(r"<(?:ins|del)[^>]*>(.*?)</(?:ins|del)>", html)


def test_an_edited_bullet_highlights_only_the_words_that_changed():
    """A one-sentence insertion must not read as a rewritten paragraph.

    Matching bullets by membership could only answer "is this exact text on the other side?", so an
    edited bullet was diffed against the empty string and every word of it came back highlighted —
    both columns solid colour, with no way to see what actually moved.
    """
    from apps.wizard.app_pages.metadata_diff.render import render_text_html

    old = ["Estimates are extrapolated using growth forecasts. See the documentation for the methodology."]
    new = [
        "Estimates are extrapolated using growth forecasts. The most recent years are therefore "
        "projections. See the documentation for the methodology."
    ]

    new_side = _highlighted(render_text_html(new, old, side="new", changed_only=True))
    assert new_side == ["The most recent years are therefore projections. "]
    # Nothing was removed, so the old column has no highlight at all.
    assert _highlighted(render_text_html(old, new, side="old", changed_only=True)) == []


def test_an_added_bullet_is_highlighted_whole_and_a_removed_one_shows_on_the_old_side():
    """A bullet with no counterpart is an addition or a removal, and reads as one."""
    from apps.wizard.app_pages.metadata_diff.render import render_text_html

    old = ["Kept exactly as it was."]
    new = ["Kept exactly as it was.", "An entirely new point about the data."]

    assert _highlighted(render_text_html(new, old, side="new", changed_only=True)) == [
        "An entirely new point about the data."
    ]
    # The addition has nothing on the old side, and the surviving bullet is unchanged, so nothing shows.
    assert "(no changes here)" in render_text_html(old, new, side="old", changed_only=True)

    # And the reverse: a removal is highlighted on the old side.
    assert _highlighted(render_text_html(old, new[1:], side="old", changed_only=True)) != []


def test_unrelated_bullets_are_not_paired_as_an_edit():
    """Below the similarity threshold, two bullets are a removal and an addition, not one rewrite.

    Otherwise boilerplate shared between unrelated bullets ("see the documentation") would pair them and
    present a wholesale replacement as a small edit.
    """
    from apps.wizard.app_pages.metadata_diff.render import pair_bullets

    pairs = pair_bullets(
        ["Income is measured after taxes. See the documentation."],
        ["Wealth counts non-financial assets held by households, minus debts."],
    )
    assert sorted((o is None, n is None) for o, n in pairs) == [(False, True), (True, False)]


def test_pairing_is_identical_whichever_column_asks():
    """Each column renders in its own call, so both must reach the same pairing or they disagree."""
    from apps.wizard.app_pages.metadata_diff.render import pair_bullets

    old = ["First point.", "Second point, later reworded a bit.", "Third point."]
    new = ["First point.", "Second point, reworded a bit later on.", "A fourth point."]
    assert pair_bullets(old, new) == pair_bullets(old, new)
    # The edited bullet pairs with its own earlier wording, not with the survivor or the newcomer.
    assert (old[1], new[1]) in pair_bullets(old, new)


def test_a_reordered_list_still_shows_every_bullet():
    """Pairing finds no textual change in a reorder, so the positional view has to take over."""
    from apps.wizard.app_pages.metadata_diff.render import render_text_html

    old = ["Alpha point.", "Beta point."]
    new = ["Beta point.", "Alpha point."]
    import re

    html = render_text_html(new, old, side="new", changed_only=True)
    assert "(no changes here)" not in html
    # Tags wrap the words that moved, so compare the text with markup stripped.
    text = re.sub(r"<[^>]+>", "", html)
    assert "Alpha point." in text and "Beta point." in text


# --- Section badges -------------------------------------------------------------------------------


def test_a_section_badge_is_just_its_name():
    """It carried a count, then a review marker; with sign-off parked it carries neither.

    The count was actively misleading — `Charts (2/10)` counted *edits*, and beside the word "Charts" it
    read as ten charts, where one edit can reach eight hundred. Whatever the progress dict says, the label
    is the name, and the bar's remaining job is to grey the sections with nothing in them.
    """
    from apps.wizard.app_pages.metadata_diff.core import section_label as _section_label

    for progress in ({}, {"charts": (0, 10)}, {"charts": (3, 10)}, {"charts": (10, 10)}):
        label = _section_label("charts", progress)
        assert label.endswith("Charts"), label
        assert not any(mark in label for mark in ("(", "🟡", "✅", "10")), label

    assert _section_label("explorers", {"explorers": (0, 0)}).endswith("Explorers")


def test_a_tick_only_counts_while_the_text_it_was_made_against_stands(monkeypatch):
    """The badge counter applies the same content-hash rule as the toggles, or the two would disagree.

    It also has to read the ticks live: they are counted from identities the cached summary carries, but
    the counting query itself is uncached, so pressing a toggle moves the number in the same rerun.
    """
    from apps.wizard.app_pages.metadata_diff.core import mark_identity, surface_key
    from apps.wizard.app_pages.metadata_diff.data import REVIEWED, count_ticked

    group = ChangeGroup(field="descriptionKey", old=["a"], new=["b"], view_dims=[{"d": "x"}])
    edited = ChangeGroup(field="descriptionKey", old=["a"], new=["c"], view_dims=[{"d": "x"}])
    surface = surface_key("mdim", "grapher/ns/latest/ds")
    change_key, content_hash = mark_identity(surface, group)

    from apps.wizard.app_pages.metadata_diff import data as data_module

    monkeypatch.setattr(
        data_module,
        "load_reviews",
        lambda engine, catalog_path: {change_key: {"status": REVIEWED, "contentHash": content_hash}},
    )

    assert count_ticked("engine", [(surface, *mark_identity(surface, group))]) == 1
    # Same slot, text moved on: the tick no longer counts.
    assert count_ticked("engine", [(surface, *mark_identity(surface, edited))]) == 0
    assert count_ticked("engine", []) == 0


def test_charts_are_listed_most_prominent_first():
    """Charts whose page shows the change come before the ones that keep it behind the drawer.

    Both are affected, so both are listed — but the first group is where the change actually meets a
    reader, and grouping them keeps a long list scannable instead of interleaving the two.
    """
    from apps.wizard.app_pages.metadata_diff.core import charts_in_reading_order

    charts = [
        {"chartId": 1, "slug": "zebra", "has_data_page": True},
        {"chartId": 2, "slug": "alpha", "has_data_page": False},
        {"chartId": 3, "slug": "beta", "has_data_page": True},
    ]

    # WYSK: the data-page charts lead, each group alphabetical within itself.
    order = [c["slug"] for c in charts_in_reading_order(charts, {"descriptionKey"})]
    assert order == ["beta", "zebra", "alpha"]

    # A field the chart renders itself is equally prominent everywhere, so slug order is all that is left.
    assert [c["slug"] for c in charts_in_reading_order(charts, {"titlePublic"})] == ["alpha", "beta", "zebra"]

    # Without a field to judge by, fall back to whether the chart has a data page at all.
    assert [c["slug"] for c in charts_in_reading_order(charts)] == ["beta", "zebra", "alpha"]


def test_coerce_section_recovers_a_section_from_its_label():
    """A label must never be mistaken for a section key.

    `st.segmented_control` round-trips its value as the formatted label and returns that label unchanged
    when it matches no option — which happens here every time a tick changes a count. Left uncoerced, the
    label reached the URL and the next page load rejected it against the options.
    """
    from apps.wizard.app_pages.metadata_diff.core import coerce_section, section_label

    assert coerce_section("mdims") == "mdims"

    # Every label this page can render maps back to the section it names, whatever the counts say.
    for section in ("charts", "mdims", "explorers"):
        for progress in ({}, {section: (0, 4)}, {section: (2, 10)}, {section: (10, 10)}):
            assert coerce_section(section_label(section, progress)) == section

    # Anything else is the caller's fallback: a deselected control, a hand-edited URL, junk.
    assert coerce_section(None) == "blast", "the page's landing section is the fallback"
    assert coerce_section(None, "mdims") == "mdims"
    assert coerce_section("", "explorers") == "explorers"
    assert coerce_section("charts-and-mdims", "mdims") == "mdims"
    assert coerce_section(3, "mdims") == "mdims"


def test_section_switcher_keeps_the_url_on_a_section_key():
    """The switcher's contract, driven through Streamlit: a label never reaches the URL.

    Covers the crash reported from staging — the URL had picked up ":material/show_chart: Charts (2/10)"
    and every later page load rejected it — and the recovery path for a link that already carries one.
    """
    from streamlit.testing.v1 import AppTest

    def param(at) -> str | None:
        # AppTest hands query params back as lists where the real app has plain strings.
        value = at.query_params.get("diff-type")
        return value[0] if isinstance(value, list) else value

    def app() -> None:
        import streamlit as st

        from apps.wizard.app_pages.metadata_diff.render import st_section_switcher

        # Stands in for the review counter: whatever it says ends up inside the option labels.
        reviewed = st.session_state.get("reviewed", 0)
        st.text(f"section={st_section_switcher({'mdims': (reviewed, 10)})}")
        if st.button("review one"):
            st.session_state["reviewed"] = reviewed + 1

    # A poisoned link — the shape the crash produced — opens on the section its label names, and is
    # rewritten to the key, so navigating on to Chart Diff (which validates strictly) is safe.
    at = AppTest.from_function(app)
    at.query_params["diff-type"] = ":material/dashboard: MDims (2/10)"
    at.run()
    assert not at.exception
    assert at.text[0].value == "section=mdims"
    assert param(at) == "mdims"

    # Reviewing a change alters every label. The selection holds and the URL stays a key.
    at.button[0].click().run()
    assert not at.exception
    assert at.text[0].value == "section=mdims"
    assert param(at) == "mdims"

    # A non-default section is spelled out in the URL...
    at.segmented_control[0].set_value("charts").run()
    assert at.text[0].value == "section=charts"
    assert param(at) == "charts"

    # ...and the landing section is dropped from it rather than spelled out, as url_persist does.
    at.segmented_control[0].set_value("blast").run()
    assert at.text[0].value == "section=blast"
    assert param(at) is None


def test_a_draft_chart_is_listed_but_never_counted_as_reach():
    """Drafts are shown to the author and kept out of every reader-facing number.

    They used to be dropped at the query, so an edit landing on a draft was invisible here. Keeping them
    in the same list as published charts would have been worse: `affected_charts` feeds the section
    header, the PR brief and the "apply to all — N charts" scope decision, none of which may overstate
    who sees a change.
    """
    from apps.wizard.app_pages.metadata_diff.core import affected_charts, affected_drafts, group_usage

    group = ChangeGroup(field="descriptionKey", old=["a"], new=["b"], indicator_ids={1, 2})
    usage = {
        1: {
            "charts": [{"chartId": 10, "slug": "live-one", "has_data_page": True, "is_published": True}],
            "draft_charts": [{"chartId": 11, "slug": "wip", "has_data_page": True, "is_published": False}],
            "mdims": [],
        },
        # A second indicator of the same shared definition, reaching one of the same charts.
        2: {
            "charts": [{"chartId": 10, "slug": "live-one", "has_data_page": True, "is_published": True}],
            "draft_charts": [{"chartId": 12, "slug": "wip-two", "has_data_page": False, "is_published": False}],
            "mdims": [],
        },
    }

    assert [c["chartId"] for c in affected_charts(group, usage)] == [10]
    assert sorted(c["chartId"] for c in affected_drafts(group, usage)) == [11, 12]
    # Both sides dedupe across the indicators of a shared definition.
    assert len(group_usage(group, usage)["charts"]) == 1

    # A usage dict from before drafts were kept carries no key, and must not raise.
    legacy = {1: {"charts": [{"chartId": 10, "slug": "live-one"}], "mdims": []}}
    assert affected_drafts(group, legacy) == []


def test_chart_list_names_drafts_separately(monkeypatch):
    """The list has three groups, and a draft links to the admin: /grapher/<slug> serves nothing yet."""
    from apps.wizard.app_pages.metadata_diff import render

    written: list[str] = []
    monkeypatch.setattr(render.st, "markdown", lambda text, **kw: written.append(str(text)))
    monkeypatch.setattr(render.st, "caption", lambda text, **kw: written.append(str(text)))

    render.render_chart_list(
        [{"chartId": 1, "slug": "published-one", "has_data_page": True, "is_published": True}],
        fields={"descriptionKey"},
        drafts=[{"chartId": 7, "slug": "draft-one", "has_data_page": True, "is_published": False}],
    )
    out = "\n".join(written)

    assert "1 data page affected" in out
    assert "1 unpublished draft" in out
    assert "published-one" in out and "draft-one" in out
    # The draft's link goes to the editor, not to a public URL that would 404.
    assert "/admin/charts/7/edit" in out
    assert "/grapher/draft-one" not in out

    # Drafts alone still render — the case where an edit only lands on unpublished work.
    written.clear()
    render.render_chart_list([], fields={"descriptionKey"}, drafts=[{"chartId": 7, "slug": "d", "is_published": False}])
    out = "\n".join(written)
    assert "1 unpublished draft" in out
    assert "No chart uses these indicators" not in out


def test_reach_never_counts_a_draft_chart():
    """`charts_reached` is the reach reported in the PR comment, so a draft must not enter it.

    Drafts come back from the same query as published charts, on purpose — they are listed so their author
    sees the edit landed there. This is the boundary: listed, not counted.
    """
    group = ChangeGroup(field="descriptionKey", old=["a"], new=["b"], indicator_ids={1})
    usage = {
        1: [
            {"chartId": 1, "slug": "live", "is_published": True},
            {"chartId": 2, "slug": "wip", "is_published": False},
            # Rows from before the flag existed default to published, as the callers always assumed.
            {"chartId": 3, "slug": "legacy"},
        ]
    }
    assert charts_reached([group], usage) == {1, 3}


def _reach_fixture():
    """One shared WYSK edit across three surfaces, plus a second edit on one chart only."""
    from apps.wizard.app_pages.metadata_diff.discovery import ChangeReach

    shared = ChangeReach(
        field="descriptionKey",
        old=["a"],
        new=["b"],
        charts=[
            {"chartId": 1, "slug": "on-page", "has_data_page": True, "is_published": True},
            {"chartId": 2, "slug": "combined", "has_data_page": False, "is_published": True},
        ],
        draft_charts=[{"chartId": 3, "slug": "wip", "has_data_page": True, "is_published": False}],
        mdims=[
            {"catalogPath": "grapher/ns/latest/live", "n_views": 16, "is_draft": False},
            {"catalogPath": "grapher/ns/latest/hidden", "n_views": 4, "is_draft": True},
        ],
        explorers=[{"slug": "an-explorer", "n_views": 3}],
    )
    subtitle = ChangeReach(
        field="descriptionShort",
        old="x",
        new="y",
        charts=[{"chartId": 1, "slug": "on-page", "has_data_page": True, "is_published": True}],
    )
    return shared, subtitle


def test_reach_separates_who_can_see_it_from_who_cannot():
    """The headline number is reader-facing places; drafts and unpublished views are counted apart."""
    shared, subtitle = _reach_fixture()

    # 2 published charts + 16 views of the published MDim + 3 explorer views.
    assert shared.n_reader_facing == 21
    # 1 draft chart + 4 views of the unpublished MDim.
    assert shared.n_hidden == 5
    assert subtitle.n_reader_facing == 1
    assert subtitle.n_hidden == 0


def test_reach_by_surface_collapses_a_page_carrying_two_changes():
    """The by-surface view exists for exactly this: one page, every edit landing on it."""
    from apps.wizard.app_pages.metadata_diff.discovery import reach_by_surface

    rows = reach_by_surface(list(_reach_fixture()))
    by_name = {r["name"]: r for r in rows}

    # `on-page` carries both edits and appears once, with both fields.
    assert sorted(by_name["on-page"]["fields"]) == ["Description", "WYSK"]
    assert by_name["on-page"]["detail"] == "data page"
    assert by_name["on-page"]["published"] is True

    # A multi-indicator chart says how its readers reach the text.
    assert by_name["combined"]["detail"] == "via Learn more about this data"

    # Unpublished surfaces are marked, not dropped: the draft chart and the unpublished MDim.
    assert by_name["wip"]["published"] is False
    assert by_name["grapher/ns/latest/hidden"]["published"] is False
    assert by_name["grapher/ns/latest/live"]["detail"] == "16 views"
    assert by_name["an-explorer"]["detail"] == "3 views"

    # Published charts lead and drafts come last, so the reader-facing rows are read first.
    kinds = [r["kind"] for r in rows]
    assert kinds.index("chart") < kinds.index("mdim") < kinds.index("draft_chart")


def test_the_same_text_on_two_surfaces_is_one_blast_radius_row():
    """`change_identity` ignores the surface, which is what makes a shared definition one row."""
    from apps.wizard.app_pages.metadata_diff.discovery import _reach_slot, change_identity

    mdim_group = ChangeGroup(field="descriptionKey", old=["a"], new=["b"], view_dims=[{"d": "x"}])
    chart_group = ChangeGroup(field="descriptionKey", old=["a"], new=["b"], indicator_ids={5})
    other_text = ChangeGroup(field="descriptionKey", old=["a"], new=["c"])

    assert change_identity(mdim_group) == change_identity(chart_group)

    reach: dict = {}
    _reach_slot(reach, mdim_group).mdims.append({"catalogPath": "cp", "n_views": 2, "is_draft": False})
    _reach_slot(reach, chart_group).charts.append({"chartId": 1, "slug": "s", "is_published": True})
    _reach_slot(reach, other_text)

    assert len(reach) == 2
    merged = reach[change_identity(mdim_group)]
    assert len(merged.mdims) == 1 and len(merged.charts) == 1


def test_blast_radius_badge_carries_no_review_counter():
    """It reports reach and holds no sign-off, so "(0)" there would say the wrong thing."""
    from apps.wizard.app_pages.metadata_diff.core import COUNTED_SECTIONS, SECTIONS, section_label

    assert list(SECTIONS)[0] == "blast", "Blast radius leads the section bar"
    assert "blast" not in COUNTED_SECTIONS
    label = section_label("blast", {})
    assert label.endswith("Blast radius")
    assert "(" not in label and "🟡" not in label and "✅" not in label
    # And neither does a review section, now that sign-off is out of the UI.
    assert section_label("charts", {"charts": (2, 10)}).endswith("Charts")


def test_diff_preview_windows_on_the_change_not_the_start():
    """A sentence added deep inside a long bullet still shows as an insertion.

    Truncating from the front showed the first 260 identical characters and no highlight at all — which is
    what the blast-radius rows did on real data, leaving two rows both labelled WYSK and neither saying
    what it changed.
    """
    from apps.wizard.app_pages.metadata_diff.core import diff_window_html

    lead = "The World Bank defines extreme poverty as living on less than $3 per day, a threshold set so "
    lead += "that poverty can be compared across countries and over time in a consistent way. " * 3
    old = lead + "For more details refer to the documentation."
    new = lead + "The most recent years are projections. For more details refer to the documentation."

    out = diff_window_html(old, new, max_chars=200)
    assert "<ins" in out, "the inserted sentence has to be visible in the preview"
    assert "most recent years" in out
    # The window opens with an ellipsis because it skipped the identical opening.
    assert out.startswith("… ")
    assert len(out) < len(old)

    # A change at the very start needs no ellipsis, and still highlights.
    out_front = diff_window_html("Old title", "New title", max_chars=200)
    assert not out_front.startswith("… ")
    assert "<ins" in out_front or "<del" in out_front

    # Values that differ without any word-level highlight (a pure reorder) still render something.
    reordered = diff_window_html(["a", "b"], ["b", "a"], max_chars=200)
    assert reordered


def test_by_surface_counts_two_edits_of_the_same_field_on_one_page():
    """Two distinct WYSK edits on one chart is the finding this view exists for.

    Deduping the labels for display while counting them for the sort order hid it, and put those rows at
    the top of the list with nothing to explain why they were there.
    """
    from apps.wizard.app_pages.metadata_diff.discovery import ChangeReach, reach_by_surface

    chart = {"chartId": 1, "slug": "hit-twice", "has_data_page": False, "is_published": True}
    other = {"chartId": 2, "slug": "hit-once", "has_data_page": True, "is_published": True}
    reach = [
        ChangeReach(field="descriptionKey", old=["a"], new=["b"], charts=[chart]),
        ChangeReach(field="descriptionKey", old=["c"], new=["d"], charts=[chart, other]),
    ]

    rows = {r["name"]: r for r in reach_by_surface(reach)}
    assert rows["hit-twice"]["field_counts"] == {"WYSK": 2}
    assert rows["hit-twice"]["n_changes"] == 2
    assert rows["hit-once"]["field_counts"] == {"WYSK": 1}

    # A data page is read before a chart that keeps the text behind the sources drawer, even though the
    # drawer chart carries more edits.
    order = [r["name"] for r in reach_by_surface(reach)]
    assert order == ["hit-once", "hit-twice"]


def test_one_authored_sentence_is_one_edit_across_many_texts():
    """A sentence spliced into several descriptions is one edit, not one per description.

    This is what made the headline read "11 distinct text changes" for a single edit to a shared
    `definitions.*` entry: the texts genuinely differ, because their surrounding wording differs, so
    comparing whole texts cannot see that one thing was written.
    """
    from apps.wizard.app_pages.metadata_diff.core import edit_fingerprint

    added = "The most recent years are therefore projections."
    first_old = "Regional estimates are extrapolated from growth forecasts. See the docs."
    second_old = "Mean income comes from PIP surveys. See the docs."
    first_new = f"Regional estimates are extrapolated from growth forecasts. {added} See the docs."
    second_new = f"Mean income comes from PIP surveys. {added} See the docs."

    assert edit_fingerprint(first_old, first_new) == edit_fingerprint(second_old, second_new)
    assert edit_fingerprint(first_old, first_new)[0] == added
    assert edit_fingerprint(first_old, first_new)[1] == ""

    # A different edit stays a different edit.
    assert edit_fingerprint(first_old, first_new) != edit_fingerprint(first_old, first_old + " Something else.")

    # A deletion is recorded as one, and a rewording carries both halves.
    assert edit_fingerprint("Keep this. Drop that.", "Keep this.")[1] != ""
    inserted, deleted = edit_fingerprint("Share of people below $2.15", "Share of people below $3.00")
    assert inserted and deleted

    # A reordered bullet list reads as a move: the flattened order differs, so both halves are non-empty.
    moved_in, moved_out = edit_fingerprint(["a", "b"], ["b", "a"])
    assert moved_in and moved_out

    # Whitespace-only is the case with nothing to quote, and the renderer has to handle it.
    assert edit_fingerprint("one  two", "one   two") == ("", "")


def test_group_by_edit_counts_pages_once_per_edit():
    """The edit's reach is distinct pages: a chart rendering two of its texts is one page."""
    from apps.wizard.app_pages.metadata_diff.discovery import ChangeReach, group_by_edit

    added = "The most recent years are therefore projections."
    shared = {"chartId": 1, "slug": "both-texts", "has_data_page": True, "is_published": True}
    only_first = {"chartId": 2, "slug": "first-only", "has_data_page": True, "is_published": True}

    reach = [
        ChangeReach(
            field="descriptionKey",
            old="Regional estimates. See the docs.",
            new=f"Regional estimates. {added} See the docs.",
            charts=[shared, only_first],
            mdims=[{"catalogPath": "grapher/a/latest/one", "n_views": 6, "is_draft": False}],
        ),
        ChangeReach(
            field="descriptionKey",
            old="Mean income. See the docs.",
            new=f"Mean income. {added} See the docs.",
            charts=[shared],
            mdims=[{"catalogPath": "grapher/a/latest/one", "n_views": 3, "is_draft": False}],
        ),
        # A separate edit, on a different field.
        ChangeReach(field="titlePublic", old="Old title", new="New title", charts=[only_first]),
    ]

    groups = group_by_edit(reach)
    assert len(groups) == 2, "the two spliced texts are one edit; the title is another"

    spliced = next(g for g in groups if g.field == "descriptionKey")
    assert spliced.n_texts == 2
    assert spliced.inserted == added
    # 2 distinct charts + 1 MDim, not 3 charts and 9 views: the shared chart and the shared MDim count once.
    assert spliced.n_reader_facing == 3
    assert spliced.surfaces()["charts"] == {"1", "2"}
    assert spliced.surfaces()["mdims"] == {"grapher/a/latest/one"}

    # Widest reach leads.
    assert groups[0].field == "descriptionKey"


def test_mdim_tree_link_opens_the_section_the_tree_lives_in():
    """`?diff-type=mdims&mdim=...&mode=tree` was the removed deep page's route.

    The MDims list drops both of those parameters on load, so a link built that way opened the plain list
    — the one place the reader already was. The tree lives in the Blast radius section now, so the link
    has to carry that section's own state, MDim included.

    And every MDim catalogPath carries a `#`, which a browser reads as the start of the fragment: left
    raw, it truncates the path and drops every parameter after it.
    """
    from apps.wizard.app_pages.metadata_diff.blast_section import GROUP_KEY, TREE_MDIM_KEY, _mdim_tree_url

    url = _mdim_tree_url("wb/latest/poverty_pip#poverty_pip")

    assert "%23poverty_pip" in url, "the hash has to be percent-encoded"
    assert "#" not in url, "nothing may start a fragment"
    assert "diff-type=blast" in url
    assert f"{GROUP_KEY}=dimensions" in url
    assert f"{TREE_MDIM_KEY}=wb/latest/poverty_pip%23poverty_pip" in url
    assert "diff-type=mdims" not in url and "mode=tree" not in url
    # No doubled slash: SOURCE.wizard_url already ends in one.
    assert "//metadata-diff" not in url


def test_the_mdim_you_asked_for_is_drawn_even_past_the_grid_cap():
    """Arriving from a card's "Dimension tree" button has to land on *that* MDim's grid.

    The grid stops at MAX_TREE_MDIMS and names the rest, so an MDim sorting past the cap used to be
    exactly the one the button could not show. Ordering, not filtering: nothing is dropped.
    """
    from streamlit.testing.v1 import AppTest

    from apps.wizard.app_pages.metadata_diff.blast_section import MAX_TREE_MDIMS, TREE_MDIM_KEY

    n_affected = MAX_TREE_MDIMS + 4
    requested = f"grapher/ns/latest/mdim_{n_affected - 1:02d}"  # last alphabetically, so past the cap

    def app() -> None:
        import streamlit as st

        from apps.wizard.app_pages.metadata_diff.blast_section import MAX_TREE_MDIMS, _requested_first

        affected = sorted(f"grapher/ns/latest/mdim_{i:02d}" for i in range(MAX_TREE_MDIMS + 4))
        ordered = _requested_first(affected)
        st.text(f"first={ordered[0]}")
        st.text(f"drawn={','.join(ordered[:MAX_TREE_MDIMS])}")
        st.text(f"kept={sorted(ordered) == affected}")

    at = AppTest.from_function(app)
    at.query_params[TREE_MDIM_KEY] = requested
    at.run()

    assert not at.exception
    assert at.text[0].value == f"first={requested}"
    assert requested in at.text[1].value.removeprefix("drawn=").split(",")
    assert at.text[2].value == "kept=True", "reordering may not drop or invent an affected MDim"

    # Nothing asked for: the alphabetical order stands.
    at = AppTest.from_function(app)
    at.run()
    assert at.text[0].value == "first=grapher/ns/latest/mdim_00"


def test_the_pr_brief_fetches_usage_for_every_indicator_of_a_shared_edit(monkeypatch):
    """One edit to a shared definition renders into several indicators, and each reaches its own charts.

    The lookup used to request only each group's first `indicator_id` while `group_usage` reads the whole
    of `indicator_ids` back out, so the brief silently omitted the charts and MDims reached through the
    others — for a shared-definition edit, most of the reach.
    """
    from apps.wizard.app_pages.metadata_diff import brief
    from apps.wizard.app_pages.metadata_diff.core import ChangeGroup, group_usage

    captured: dict[str, tuple] = {}

    def fake_usage(ids, catalog_path, engine, cache_key=""):
        captured["ids"] = ids
        return {i: {"charts": [{"chartId": i, "slug": f"chart-{i}"}], "draft_charts": [], "mdims": []} for i in ids}

    monkeypatch.setattr(brief.cached, "usage_for_indicators", fake_usage)

    shared = ChangeGroup(
        field="descriptionShort",
        old="Old.",
        new="New.",
        affects_indicator=True,
        indicator_id=11,
        indicator_ids={11, 12, 13},
    )
    # A group built without the set at all still contributes its single id.
    single = ChangeGroup(field="titlePublic", old="A", new="B", affects_indicator=True, indicator_id=20)
    # An MDim-only override reaches nothing else, so it asks for nothing.
    override = ChangeGroup(field="titlePublic", old="C", new="D", affects_indicator=False, indicator_id=99)

    usage = brief.usage_for(
        None, [shared, single, override], "grapher/a/latest/incomes#incomes", {"configMd5_source": "abc"}
    )

    assert captured["ids"] == (11, 12, 13, 20)
    assert len(group_usage(shared, usage)["charts"]) == 3, "the brief reads every indicator's charts back out"


def test_chart_config_text_is_compared_because_the_indicator_row_never_carries_it():
    """A `presentation.grapher_config` edit lands in the chart's config, not in the `variables` row.

    Measured on a live staging server: editing two shared definitions moved the subtitle of 15 published
    charts and the footnote of 14, while `variables` matched the new text zero times. The Charts section
    read only `variables`, so those edits were not under-reported — they were invisible.
    """
    from apps.wizard.app_pages.metadata_diff.discovery import compare_chart_texts

    source = {
        "reworded": {"chartId": 1, "slug": "reworded", "title": "T", "subtitle": "New wording", "note": "N"},
        "same-wording": {"chartId": 2, "slug": "same-wording", "title": "T", "subtitle": "New wording", "note": "N"},
        "untouched": {"chartId": 3, "slug": "untouched", "title": "T", "subtitle": "Old wording", "note": "N"},
        "brand-new": {"chartId": 4, "slug": "brand-new", "title": "T", "subtitle": "Whatever", "note": "N"},
    }
    target = {
        "reworded": {"slug": "reworded", "title": "T", "subtitle": "Old wording", "note": "N"},
        "same-wording": {"slug": "same-wording", "title": "T", "subtitle": "Old wording", "note": "N"},
        "untouched": {"slug": "untouched", "title": "T", "subtitle": "Old wording", "note": "N"},
        # "brand-new" is absent from the baseline.
    }

    changes = compare_chart_texts(source, target)

    assert sorted(changes.diffs) == ["reworded", "same-wording"]
    assert "untouched" not in changes.diffs, "an identical chart is not a change"
    # A chart the baseline does not publish is a new chart, which is chart-diff's subject, not this one's.
    assert "brand-new" not in changes.diffs

    diff = changes.diffs["reworded"]
    assert set(diff.fields) == {"chart.subtitle"}
    assert diff.fields["chart.subtitle"] == {"old": "Old wording", "new": "New wording"}

    # Each chart is a view keyed by its slug, so the two charts saying the same thing group into one
    # change — which is what a shared `definitions.*` edit produces.
    groups = group_changes(changes.view_diffs())
    assert len(groups) == 1
    assert sorted(d["chart"] for d in groups[0].view_dims) == ["reworded", "same-wording"]

    # The charts are carried on the change itself: a chart-level edit reaches exactly these charts,
    # rather than everything that renders some indicator.
    assert changes.charts["reworded"]["chartId"] == 1
    assert changes.charts["reworded"]["has_data_page"] is True, "a chart's own text is on its canvas"


def test_chart_text_changes_need_the_dataset_to_be_in_scope_and_rebuilt(monkeypatch):
    """The same two signals the indicator layer uses, or a chart master rebuilt would read as ours."""
    from apps.wizard.app_pages.metadata_diff import discovery

    calls = {}

    def fake_rows(engine, dataset_paths):
        calls["paths"] = list(dataset_paths)
        return {}

    monkeypatch.setattr(discovery, "chart_text_rows", fake_rows)
    monkeypatch.setattr(discovery, "chart_text_rows_by_slug", lambda engine, slugs: {})

    scope = BranchScope(dataset_paths={"grapher/ns/2026-01-01/in_scope", "grapher/ns/2026-01-01/not_built"})
    built = {"ns/2026-01-01/in_scope"}
    discovery.changed_chart_texts("src", "tgt", scope, built)  # type: ignore[arg-type]
    assert calls["paths"] == ["grapher/ns/2026-01-01/in_scope"], "only datasets both in scope and rebuilt here"

    # With no git scope there is nothing to narrow by, and the caller is told so rather than shown a list
    # that would include everything master has moved on since.
    unavailable = BranchScope(available=False)
    out = discovery.changed_chart_texts("src", "tgt", unavailable, built)  # type: ignore[arg-type]
    assert out.diffs == {} and out.narrowed is False


def test_a_view_pointing_at_another_variant_is_marked_not_blamed_on_the_baseline():
    """Two unlike reasons a difference is not this branch's, and they read very differently.

    Found on a live branch: a poverty_pip view renders `…survey_comparability_4` on staging and
    `…survey_comparability_6` in production, so its title differs both because the variant changed and
    because this branch reworded it. The tool cannot attribute that, which is correct — but captioning it
    "its recipe is untouched" said master had done it.
    """
    from apps.wizard.app_pages.metadata_diff.core import build_view_bundle, diff_views, group_changes

    def bundle(path: str, title: str):
        return build_view_bundle(
            view={"dimensions": {"poverty_line": "_100"}},
            config_metadata=None,
            variable_row={"id": 1, "catalogPath": path, "titlePublic": title, "name": "n"},
            chart_config=None,
        )

    base = "grapher/wb/2026-06-26/world_bank_pip/poverty#headcount_ratio__spells"

    # Same indicator, reworded: attributable, and not a replacement.
    edited = diff_views([bundle(f"{base}_4", "New title")], [bundle(f"{base}_4", "Old title")])[0]
    assert edited.changed and edited.indicator_changed_fields == {"titlePublic"}
    assert edited.indicator_replaced is False

    # Different variant: the text differs, but no rewording can be attributed to it.
    swapped = diff_views([bundle(f"{base}_4", "New title")], [bundle(f"{base}_6", "Old title")])[0]
    assert swapped.changed
    assert swapped.indicator_changed_fields == set(), "a replacement is not an indicator-layer edit"
    assert swapped.indicator_replaced is True

    # A version bump alone is still the same indicator, so it must not be marked as replaced.
    other_version = base.replace("2026-06-26", "2026-01-22")
    bumped = diff_views([bundle(f"{base}_4", "New title")], [bundle(f"{other_version}_4", "Old title")])[0]
    assert bumped.indicator_replaced is False
    assert bumped.indicator_changed_fields == {"titlePublic"}

    # The flag reaches the group the card reads.
    group = group_changes([swapped])[0]
    assert group.indicator_replaced is True
    assert group_changes([edited])[0].indicator_replaced is False


def test_edit_detail_counts_distinct_views_per_mdim():
    """An MDim reached by several of an edit's texts is one row, with its views counted once."""
    from apps.wizard.app_pages.metadata_diff.blast_section import _mdim_lines, _mdim_totals
    from apps.wizard.app_pages.metadata_diff.discovery import ChangeReach, group_by_edit

    added = "Values are rounded."
    overlapping = {"indicator": "mean", "period": "day"}
    reach = [
        ChangeReach(
            field="descriptionKey",
            old="Income data.",
            new=f"Income data. {added}",
            mdims=[
                {
                    "catalogPath": "grapher/a/latest/incomes",
                    "title": "Incomes across the distribution",
                    "n_views": 2,
                    "views": [overlapping, {"indicator": "mean", "period": "month"}],
                    "is_draft": False,
                }
            ],
        ),
        ChangeReach(
            field="descriptionKey",
            old="Poverty data.",
            new=f"Poverty data. {added}",
            mdims=[
                {
                    "catalogPath": "grapher/a/latest/incomes",
                    "title": "Incomes across the distribution",
                    "n_views": 1,
                    "views": [overlapping],
                    "is_draft": False,
                },
                {
                    "catalogPath": "grapher/a/latest/poverty",
                    "title": "Poverty indicators",
                    "n_views": 1,
                    "views": [{"indicator": "headcount"}],
                    "is_draft": True,
                },
            ],
        ),
    ]
    group = group_by_edit(reach)[0]
    totals = _mdim_totals(group)

    assert [e["title"] for e in totals] == [
        "Incomes across the distribution",
        "Poverty indicators",
    ], "widest reach first"
    # The view both texts land on is one view, so 2 + 1 overlapping = 2, not 3.
    assert totals[0]["n_views"] == 2
    assert totals[0]["is_draft"] is False
    assert totals[1]["is_draft"] is True, "the draft MDim stays flagged, to be listed apart"
    assert _mdim_lines(totals[:1]) == ["- **2 views** in the MDim *Incomes across the distribution*"]
    assert _mdim_lines([totals[1]]) == ["- **1 view** in the MDim *Poverty indicators*"]


def test_edit_detail_counts_reach_entries_without_dimensions():
    """A reach entry carrying only a count still reports one: the largest any single text saw."""
    from apps.wizard.app_pages.metadata_diff.blast_section import _mdim_totals
    from apps.wizard.app_pages.metadata_diff.discovery import ChangeReach, group_by_edit

    reach = [
        ChangeReach(
            field="subtitle",
            old="A.",
            new="A. B.",
            mdims=[{"catalogPath": "grapher/a/latest/one", "n_views": 6, "is_draft": False}],
        ),
        ChangeReach(
            field="subtitle",
            old="C.",
            new="C. B.",
            mdims=[{"catalogPath": "grapher/a/latest/one", "n_views": 3, "is_draft": False}],
        ),
    ]
    totals = _mdim_totals(group_by_edit(reach)[0])
    assert len(totals) == 1
    assert totals[0]["n_views"] == 6, "no dimensions to union, so the tightest bound is the largest count"
    assert totals[0]["title"] == "grapher/a/latest/one", "no title in the entry falls back to the path"


def test_view_url_sends_unpublished_views_to_the_admin_preview():
    """`/grapher/<slug>` 404s until an MDim is published, so an unpublished view links to the preview."""
    from apps.wizard.app_pages.metadata_diff.core import view_url

    class Env:
        site = "http://staging-site-x"
        admin_site = "http://staging-site-x/admin"

    dims = {"indicator": "headcount", "period": "day"}
    published = view_url(Env(), "wb/latest/poverty_pip#poverty_pip", "poverty-wb", dims)
    assert published == "http://staging-site-x/grapher/poverty-wb?indicator=headcount&period=day"

    unpublished = view_url(Env(), "wb/latest/poverty_pip#poverty_pip", None, dims)
    assert unpublished == (
        "http://staging-site-x/admin/grapher/wb%2Flatest%2Fpoverty_pip%23poverty_pip/?indicator=headcount&period=day"
    )
    # One `/admin`, not two: `admin_site` already ends in it, and the doubled path serves the admin
    # shell with no editor or preview in it.
    assert "/admin/admin/" not in unpublished


def test_empty_sections_are_greyed_not_removed():
    """A section with nothing in it stays on the bar, showing its zero."""
    from apps.wizard.app_pages.metadata_diff.core import empty_sections

    progress = {"charts": (0, 12), "mdims": (0, 0), "explorers": (0, 0)}
    assert empty_sections(progress) == ["mdims", "explorers"], "in bar order, and never Blast radius"
    # Everything populated: nothing greyed.
    assert empty_sections({"charts": (1, 2), "mdims": (0, 3), "explorers": (2, 2)}) == []
    # A section the summary never mentioned counts as empty, same as an explicit zero.
    assert empty_sections({}) == ["charts", "mdims", "explorers"]


def test_a_zero_we_cannot_vouch_for_stays_clickable():
    """Greying a section because its lookup failed would hide the tool's own blind spot."""
    from apps.wizard.app_pages.metadata_diff.core import COUNTED_SECTIONS, empty_sections

    nothing = {"charts": (0, 0), "mdims": (0, 0), "explorers": (0, 0)}
    # A warning anywhere: nothing is greyed, because a failed surface reads exactly like an empty one.
    assert empty_sections(nothing, COUNTED_SECTIONS) == []
    # One unresolved surface stays live; the others are honestly empty.
    assert empty_sections(nothing, {"mdims"}) == ["charts", "explorers"]
    # New indicators are not reviewable changes, so Charts counts zero while holding something to read.
    assert empty_sections(nothing, {"charts"}) == ["mdims", "explorers"]


def test_dead_section_css_targets_the_right_buttons():
    """The CSS selects by position, so the indices must follow the options list it was given."""
    from apps.wizard.app_pages.metadata_diff.core import SECTIONS
    from apps.wizard.app_pages.metadata_diff.render import _dead_section_css

    options = list(SECTIONS)  # blast, charts, mdims, explorers
    css = _dead_section_css(options, {"mdims", "explorers"})
    # nth-child is 1-based: mdims is the 3rd button, explorers the 4th.
    assert "button:nth-child(3)" in css and "button:nth-child(4)" in css
    assert "button:nth-child(1)" not in css and "button:nth-child(2)" not in css
    assert "pointer-events: none" in css

    # Nothing to grey: no style block at all, rather than an empty rule.
    assert _dead_section_css(options, set()) == ""


def test_explorer_reach_is_counted_and_named():
    """An explorer view is a page a reader can open, so an edit landing on one has to say so."""
    from apps.wizard.app_pages.metadata_diff.blast_section import _explorer_branch, _explorer_totals
    from apps.wizard.app_pages.metadata_diff.discovery import ChangeReach, group_by_edit

    added = "Values are rounded."
    reach = [
        ChangeReach(
            field="descriptionKey",
            old="Energy data.",
            new=f"Energy data. {added}",
            explorers=[{"slug": "energy", "n_views": 12}, {"slug": "co2", "n_views": 3}],
        ),
        ChangeReach(
            field="descriptionKey",
            old="Emissions data.",
            new=f"Emissions data. {added}",
            explorers=[{"slug": "energy", "n_views": 5}],
        ),
    ]
    group = group_by_edit(reach)[0]
    # Widest first; the explorer both texts land on is reconciled to one row, not 12 + 5.
    assert _explorer_totals(group) == [("energy", 12), ("co2", 3)]

    branch = _explorer_branch(reach)
    assert branch is not None
    assert branch["id"] == "explorers"
    labels = [leaf["label"] for leaf in branch["groups"][0]["leaves"]]
    assert labels == ["co2 · 3 views", "energy · 12 views"]
    assert branch["groups"][0]["leaves"][0]["href"].endswith("/explorers/co2")
    # The caveat that makes an explorer different from a chart is carried on the group.
    assert "no data page" in branch["groups"][0]["note"]

    # No explorers reached: no branch at all, rather than an empty heading.
    assert _explorer_branch([ChangeReach(field="subtitle", old="a", new="b")]) is None


def test_tree_draws_a_branch_per_flat_surface():
    """Charts and explorers are siblings of the MDim grid — each drawn only when it has leaves."""
    from apps.wizard.app_pages.metadata_diff.tree import render_multi_tree_html

    def branch(bid, label, leaves):
        return {"id": bid, "label": label, "groups": [{"name": label, "note": "", "leaves": leaves}]}

    leaf = {"label": "a-chart", "href": "http://x/grapher/a-chart", "preview": "", "badged": False}
    html_out, _ = render_multi_tree_html(
        [],
        branches=[
            branch("charts", "Charts", [leaf]),
            branch("explorers", "Explorers", [{**leaf, "label": "energy · 3 views"}]),
            branch("nothing", "Nothing", []),
        ],
    )
    assert 'id="mdd-section-charts"' in html_out
    assert 'id="mdd-section-explorers"' in html_out
    assert "energy · 3 views" in html_out
    # An empty branch is not drawn, and does not appear in the jump index either.
    assert 'id="mdd-section-nothing"' not in html_out and "Nothing" not in html_out
    assert html_out.count('data-target="mdd-section-') == 2


def test_a_flat_branch_of_the_grid_opens_expanded_and_is_sized_for_it():
    """A list of chart names answers "is my chart here" only when it is open, so it renders open.

    And the frame it renders in does not scroll: its leaves have to be in the height estimate, or the
    tree is cut off until the component's own resize fires.
    """
    from apps.wizard.app_pages.metadata_diff.tree import render_multi_tree_html

    leaves = [
        {"label": f"chart-{i}", "href": f"http://x/grapher/chart-{i}", "preview": "", "badged": False}
        for i in range(12)
    ]
    branch = {"id": "charts", "label": "Charts", "groups": [{"name": "Data pages", "note": "", "leaves": leaves}]}
    html_out, height = render_multi_tree_html([], branches=[branch])

    group_box = html_out[: html_out.index("Data pages")]
    assert "mdd-collapsed" not in group_box.rsplit('<div class="mdd-node', 1)[-1], "the group renders open"
    assert all(f"chart-{i}" in html_out for i in range(12))

    _empty_html, empty_height = render_multi_tree_html(
        [], branches=[{"id": "charts", "label": "Charts", "groups": [{"name": "Data pages", "leaves": leaves[:1]}]}]
    )
    assert height > empty_height, "every drawn leaf has to raise the frame's initial height"


def test_explorer_grid_columns_are_inferred_narrowest_first():
    """An explorer publishes no dimension list, so the grid's columns come from the views themselves.

    Narrowest dimension first: a leaf is named by the last dimension's value, so leaving a two-choice
    toggle there labels every leaf "true" or "false".

    Also pins the crash this shipped with for one page load — the sort key called `order.index`, and
    `list.sort` empties the list while it runs, so the first comparison raised
    `ValueError: list.index(x): x not in list` and took the whole Blast radius down.
    """
    from apps.wizard.app_pages.metadata_diff.blast_section import _explorer_dimensions
    from apps.wizard.app_pages.metadata_diff.core import ViewDiff

    views = [
        ViewDiff(dimensions={"Equivalized": "true", "Decile": d, "Period": p})
        for d in ("1-poorest", "5", "10-richest")
        for p in ("Day", "Month")
    ] + [ViewDiff(dimensions={"Equivalized": "false", "Decile": "1-poorest", "Period": "Day"})]

    dims = _explorer_dimensions(views)
    assert [d["slug"] for d in dims] == ["Equivalized", "Period", "Decile"], "narrowest first, widest last"
    assert [c["slug"] for c in dims[-1]["choices"]] == ["1-poorest", "5", "10-richest"], "first-seen order"
    assert dims[0]["name"] == "Equivalized"

    # Ties keep the order the views listed them in, rather than an arbitrary one.
    tied = [ViewDiff(dimensions={"B": "1", "A": "1"}), ViewDiff(dimensions={"B": "2", "A": "2"})]
    assert [d["slug"] for d in _explorer_dimensions(tied)] == ["B", "A"]

    # Tidying dashes into spaces must not tidy a label away: "-" means this view has no decile, and it
    # rendered as a single space, leaving four hundred leaves labelled with nothing.
    dashed = _explorer_dimensions([ViewDiff(dimensions={"Decile": "-"}), ViewDiff(dimensions={"Decile": "1-poorest"})])
    assert [c["name"] for c in dashed[0]["choices"]] == ["-", "1 poorest"]


def test_a_chart_link_beats_the_blank_left_by_the_lookup_box():
    """Following a chart link inside an open session used to undo itself.

    `url_persist` seeds a widget from the query string only when its session value is `None`. The lookup
    box leaves an empty string once rendered, so arriving with `?chart=<slug>` found a blank widget, wrote
    the blank back over the URL, and left the reader on the list they had just clicked away from.
    """
    from apps.wizard.app_pages.metadata_diff.core import requested_chart

    # The link wins over the blank the box left behind — the case that was broken.
    assert requested_chart("", "life-expectancy") == "life-expectancy"
    assert requested_chart(None, "life-expectancy") == "life-expectancy"
    assert requested_chart("   ", "life-expectancy") == "life-expectancy"

    # A chart already open wins over the URL: typing in the box is what put it there.
    assert requested_chart("child-mortality", "life-expectancy") == "child-mortality"

    # Nothing anywhere means the list, not a blank per-chart page.
    assert requested_chart(None, None) == ""
    assert requested_chart("", "") == ""


def test_chart_text_matching_stops_at_the_dataset_boundary():
    """`LIKE '<path>%'` also selects a sibling dataset whose name merely starts with this one.

    Both `climate/2026-08-21/surface_temperature` and `surface_temperature_anomalies` are live, so the
    prefix quietly pulled the sibling's charts in — and a difference of the sibling's (baseline lag, say)
    was then reported as this branch's work.
    """
    import re

    from apps.wizard.app_pages.metadata_diff.discovery import catalog_path_like_patterns

    def matches(dataset_path: str, candidate: str) -> bool:
        # SQL LIKE, close enough for these patterns: `%` stands for any run of characters.
        return any(
            re.fullmatch(".*".join(re.escape(part) for part in pattern.split("%")), candidate) is not None
            for pattern in catalog_path_like_patterns(dataset_path)
        )

    dataset = "grapher/climate/2026-08-21/surface_temperature"
    sibling = "grapher/climate/2026-08-21/surface_temperature_anomalies"

    assert matches(dataset, f"{dataset}/annual#temperature_anomaly")
    assert matches(dataset, f"{dataset}#temperature_anomaly")
    assert not matches(dataset, f"{sibling}/annual#anomaly")
    assert not matches(dataset, sibling)


def test_a_draft_only_branch_can_still_open_its_mdims_section():
    """An unpublished MDim's card carries Reviewed toggles, and the MDims badge counts `review_keys`.

    Recording a draft's reach but not its marks left a branch whose only change is an unpublished MDim
    with a greyed-out MDims section it could never open — while the page said, correctly, that this
    branch had changed text.
    """
    import pandas as pd

    from apps.wizard.app_pages.metadata_diff.core import ChangeGroup, empty_sections
    from apps.wizard.app_pages.metadata_diff.discovery import Summary, _record_mdim_groups

    groups = [ChangeGroup(field="descriptionShort", old="Old.", new="New.", view_dims=[{"metric": "mean"}])]

    draft = Summary()
    reach: dict = {}
    _record_mdim_groups(draft, reach, set(), "grapher/a/latest/incomes#incomes", groups, pd.DataFrame(), is_draft=True)

    assert draft.has_changes
    assert (draft.n_draft_mdims, draft.n_mdims) == (1, 0), "a draft is not reader-facing, and is not counted as one"
    assert len(draft.review_keys["mdims"]) == 1
    assert next(iter(reach.values())).mdims[0]["is_draft"] is True
    # The symptom: with its marks recorded, the section is reachable instead of greyed out.
    assert "mdims" not in empty_sections({"mdims": (0, len(draft.review_keys["mdims"]))})

    # A published MDim goes through the same recording and is counted as reader-facing.
    published = Summary()
    _record_mdim_groups(
        published, {}, set(), "grapher/a/latest/poverty#poverty", groups, pd.DataFrame(), is_draft=False
    )
    assert (published.n_mdims, published.n_draft_mdims) == (1, 0)
    assert published.n_mdim_changes == 1
    assert len(published.review_keys["mdims"]) == 1


def test_an_edit_landing_only_on_an_unpublished_mdim_still_gets_a_card():
    """The drafts used to sit in a paginated expander of their own, and its fifth card had no route to it.

    An unpublished MDim is in the same picker as the published ones now, and its edits are cards like any
    other — badged unpublished, never filtered: the PR that publishes an MDim is the one whose reviewer has
    to read it.
    """
    from apps.wizard.app_pages.metadata_diff.discovery import ChangeReach, edits_for
    from apps.wizard.app_pages.metadata_diff.edits_view import _reach_line

    draft_only = ChangeReach(
        field="descriptionShort",
        old="Old.",
        new="New.",
        mdims=[
            {
                "catalogPath": "grapher/a/latest/draft#draft",
                "title": "Draft",
                "n_views": 1,
                "is_draft": True,
                "views": [{"metric": "mean"}],
            }
        ],
    )
    (edit,) = edits_for(Summary(reach=[draft_only]), "mdims")
    assert "1 view in Draft (unpublished)" in _reach_line(edit, "mdims")


def test_a_value_that_cannot_be_dumped_says_so_instead_of_passing_off_a_repr(monkeypatch):
    """The brief's snippets are for pasting under a variable, so a fallback has to be unpastable.

    Substituting `repr(value)` and labelling it YAML hands over something that may be invalid, or valid
    and wrong, with nothing to say the dump failed.
    """
    from apps.wizard.app_pages.metadata_diff import core

    def boom(_value):
        raise ValueError("cannot represent this")

    monkeypatch.setattr(core, "ruamel_dump", boom)
    snippet = core.yaml_field_snippet("descriptionShort", object())

    assert "cannot represent this" in snippet
    assert all(line.startswith("#") for line in snippet.splitlines()), "nothing here may be pasted as a value"


def test_an_mdim_overflow_leaves_the_section_reachable_either_way():
    """Both MDim counts can overflow the view-by-view budget, and either leaves no review keys behind.

    The published overflow was allowed for; the unpublished one was not, so a branch changing more than
    the budget's worth of draft MDims met a greyed-out section holding all of their cards.
    """
    from apps.wizard.app_pages.metadata_diff.core import empty_sections
    from apps.wizard.app_pages.metadata_diff.discovery import Summary, keep_sections

    drafts_overflowed = Summary(n_draft_mdims=30, draft_mdims_resolved=False)
    assert "mdims" in keep_sections(drafts_overflowed)
    assert "mdims" not in empty_sections({"mdims": (0, 0)}, keep_sections(drafts_overflowed))

    published_overflowed = Summary(n_mdims=30, mdims_resolved=False)
    assert "mdims" in keep_sections(published_overflowed)

    # A summary that resolved everything still greys a section with nothing in it.
    assert "mdims" in empty_sections({"mdims": (0, 0)}, keep_sections(Summary()))
    # And a warning anywhere keeps every counted section reachable, because its zero means "we could not look".
    assert keep_sections(Summary(warnings=["Chart discovery failed"])) == {"charts", "mdims", "explorers"}


def test_the_bot_comment_reports_a_chart_config_only_change():
    """A garden `grapher_config` edit changes chart text without touching any indicator row.

    `has_changes` is true for it, so the comment posts — and it used to post with a field name and no
    mention of a chart, because the Charts line was keyed off the indicator-layer counts alone.
    """
    from apps.owidbot.metadata_diff import format_metadata_diff, status_icon
    from apps.wizard.app_pages.metadata_diff.discovery import Summary

    config_only = Summary(n_chart_text_changes=2, n_charts_own_text=15, fields={"Chart subtitle": 2})

    assert config_only.has_changes
    body = format_metadata_diff(config_only)
    assert "Charts whose own config text changed: 15 (2 changes)" in body
    assert status_icon(config_only) == "✏️"

    # The indicator-layer line is unchanged, and both appear when both happened.
    both = Summary(n_charts=4, n_indicators=1, n_chart_text_changes=1, n_charts_own_text=3)
    body = format_metadata_diff(both)
    assert "Charts: 4 (from 1 indicator)" in body
    assert "Charts whose own config text changed: 3 (1 change)" in body


def test_chart_text_looks_for_variables_in_the_channel_variables_live_in(monkeypatch):
    """A `shared.meta.yml` edit resolves to garden steps, and a garden path matches no variable.

    Chart text is the only comparison that can see a shared `presentation.grapher_config` edit — it never
    touches the indicator row — so a channel mismatch here loses the change altogether rather than
    reporting it from somewhere else.
    """
    from apps.wizard.app_pages.metadata_diff import discovery

    captured: dict[str, list[str]] = {}

    def fake_rows(engine, dataset_paths):
        captured["paths"] = list(dataset_paths)
        return {}

    monkeypatch.setattr(discovery, "chart_text_rows", fake_rows)

    scope = discovery.BranchScope(
        dataset_paths={
            # What a shared-file edit leaves behind: the garden steps that own the file, and no grapher one.
            "garden/ihme_gbd/2026-02-07/gbd_cause",
            # And the ordinary case, where both channels are in scope for one dataset.
            "garden/wb/2026-06-26/world_bank_pip",
            "grapher/wb/2026-06-26/world_bank_pip",
        },
        available=True,
    )
    built = {"ihme_gbd/2026-02-07/gbd_cause", "wb/2026-06-26/world_bank_pip"}

    discovery.changed_chart_texts(None, None, scope, built)

    assert captured["paths"] == [
        "grapher/ihme_gbd/2026-02-07/gbd_cause",
        "grapher/wb/2026-06-26/world_bank_pip",
    ], "every dataset in scope has to be asked for in the grapher channel, exactly once"


def test_by_edit_is_one_card_per_authored_edit_however_many_texts_word_it():
    """One reworded subtitle reached 348 explorer views, each wording it a little differently: 348 cards.

    Cards keyed on the exact text repeat the same decision once per wording. Grouping by the words that
    moved makes it one card — and scoping to the section first means an edit landing on both a chart and
    an MDim is one card in each section, describing only that surface.
    """
    from apps.wizard.app_pages.metadata_diff.discovery import ChangeReach, edit_key, edits_for

    wordings = [
        ChangeReach(
            field="descriptionShort",
            old=f"Mean income per {unit}.",
            new=f"Mean income per {unit}. Measured in 2021 prices.",
            explorers=[{"slug": "lis", "n_views": 1, "views": [{"unit": unit}]}],
            catalog_paths={"grapher/lis/latest/lis#mean"},
        )
        for unit in ("day", "month", "year")
    ]
    both = ChangeReach(
        field="titlePublic",
        old="GDP",
        new="GDP per capita",
        charts=[{"chartId": 1, "slug": "gdp", "has_data_page": True}],
        mdims=[{"catalogPath": "grapher/a/latest/x#x", "title": "X", "n_views": 2, "is_draft": False}],
    )
    summary = Summary(reach=wordings + [both])

    explorers = edits_for(summary, "explorers")
    assert [e.n_texts for e in explorers] == [3], "three wordings of one edit are one card"
    assert len({edit_key(e) for e in explorers}) == 1
    assert [e.field for e in edits_for(summary, "charts")] == ["titlePublic"]
    assert [e.field for e in edits_for(summary, "mdims")] == ["titlePublic"]


def test_an_edit_tick_outlives_a_new_text_but_not_a_rewording():
    """The slot is anchored on what the baseline carried; the content is what was written.

    A new text picking the edit up (another indicator now shares the definition) must not reopen a tick
    that was made against the very same words. Rewording the insertion must: the slot stays and the hash
    moves, so the tick reads as stale — exactly what a view's tick does when its text is edited again.
    """
    from apps.wizard.app_pages.metadata_diff.discovery import ChangeReach, edit_fields, edit_key, group_by_edit

    where = {"grapher/a/latest/x#x"}
    first = ChangeReach(
        field="descriptionShort", old="Mean income.", new="Mean income. In 2021 prices.", catalog_paths=where
    )
    second = ChangeReach(
        field="descriptionShort", old="Median income.", new="Median income. In 2021 prices.", catalog_paths=where
    )
    reworded = ChangeReach(
        field="descriptionShort", old="Mean income.", new="Mean income. In 2017 prices.", catalog_paths=where
    )

    (one,) = group_by_edit([first])
    (two,) = group_by_edit([first, second])
    assert two.n_texts == 2
    assert edit_key(one) == edit_key(two) and edit_fields(one) == edit_fields(two)

    (again,) = group_by_edit([reworded])
    assert edit_key(again) == edit_key(one), "same field, same words taken out, same dataset: the same slot"
    assert edit_fields(again) != edit_fields(one), "different words put in: the tick goes stale"


def test_a_view_link_from_an_edit_card_opens_the_view_by_view_page_on_that_view():
    """The list under an edit is for getting to one place it lands, so each link has to route exactly.

    The keys are the ones the browsers read, and the catalogPath's `#` is encoded — a bare one would be a
    fragment, dropping every parameter after it and truncating the path.
    """
    from apps.wizard.app_pages.metadata_diff import explorers_section, mdims_section, view_nav

    link = view_nav.mdim_view_link("grapher/wb/latest/incomes_pip#incomes_pip", {"metric": "mean", "period": "day"})
    assert link.startswith("?diff-type=mdims&")
    assert "#" not in link and "%23incomes_pip" in link
    assert f"&{mdims_section.DIM_PARAM_PREFIX}metric=mean&{mdims_section.DIM_PARAM_PREFIX}period=day" in link
    assert f"{mdims_section.VIEWS_KEY}=" in link

    link = view_nav.explorer_view_link("incomes-across-distribution-lis", {"Indicator": "Mean income"})
    assert f"{explorers_section.EXPLORER_KEY}=incomes-across-distribution-lis" in link
    assert f"{explorers_section.DIM_PARAM_PREFIX}Indicator=Mean+income" in link


def test_the_coverage_line_says_how_much_of_the_mdim_an_edit_reaches():
    """Fifty-one links do not say whether every metric is covered; the coverage line does, in choice names."""
    from apps.wizard.app_pages.metadata_diff.edits_view import coverage_line

    dimensions = [
        {
            "slug": "metric",
            "name": "Metric",
            "choices": [{"slug": m, "name": m.title()} for m in ("mean", "median", "gini")],
        },
        {
            "slug": "period",
            "name": "Period",
            "choices": [{"slug": "day", "name": "Day"}, {"slug": "month", "name": "Month"}],
        },
        {"slug": "welfare", "name": "Welfare", "choices": [{"slug": "income", "name": "Income"}]},
    ]
    views = [
        {"metric": "mean", "period": "day", "welfare": "income"},
        {"metric": "median", "period": "month", "welfare": "income"},
        {"metric": "mean", "period": "month", "welfare": "income"},
    ]
    assert coverage_line(views, dimensions) == "Metric: Mean, Median (2 of 3) · Period: all 2 · Welfare: Income"
    # An explorer's dimensions are inferred from the affected views, so a total would always equal the count.
    inferred = [{"slug": "metric", "name": "metric", "choices": [{"slug": "mean", "name": "mean"}]}]
    assert coverage_line(views[:1], inferred, known_universe=False) == "metric: mean (1)"


def test_a_blast_radius_link_focuses_one_edit_and_greys_the_rest():
    """A card's grid link opens Blast radius on that edit alone: its views changed, the others still drawn.

    Greyed, not dropped — what sits beside an edit on the grid is half of what the grid is for. The handle
    in the link is short and stable, so the link can be pasted and still resolve on the next rerun.
    """
    from apps.wizard.app_pages.metadata_diff import view_nav
    from apps.wizard.app_pages.metadata_diff.blast_section import _only_these_views, _view_keys
    from apps.wizard.app_pages.metadata_diff.discovery import ChangeReach, edit_slot, group_by_edit

    views = [
        ViewDiff(dimensions={"m": "mean"}, fields={"titlePublic": {"old": "a", "new": "b"}}),
        ViewDiff(dimensions={"m": "median"}, fields={"titlePublic": {"old": "a", "new": "c"}}, is_new=True),
    ]
    reach = ChangeReach(
        field="titlePublic",
        old="a",
        new="b",
        mdims=[{"catalogPath": "grapher/a/latest/x#x", "n_views": 1, "is_draft": False, "views": [{"m": "mean"}]}],
    )
    keep = _view_keys([reach], "mdims", "catalogPath")["grapher/a/latest/x#x"]
    kept = _only_these_views(views, keep)
    assert [v.changed for v in kept] == [True, False]
    assert kept[1].dimensions == {"m": "median"}, "still on the grid, just not highlighted"
    assert views[1].changed, "the cached diffs themselves are untouched"

    (edit,) = group_by_edit([reach])
    slot = edit_slot(edit)
    assert len(slot) == 12 and slot == edit_slot(edit)
    assert view_nav.blast_edit_link(slot) == f"?diff-type=blast&blast-group=dimensions&blast-edit={slot}"

    # The MDims card holds the edit scoped to MDims; Blast radius holds it whole, with the chart text too.
    # The link from one has to find the other, so the handle cannot depend on which texts came along.
    from apps.wizard.app_pages.metadata_diff.discovery import edits_for

    on_a_chart = ChangeReach(
        field="titlePublic",
        old="x a",
        new="x b",
        charts=[{"chartId": 1, "slug": "gdp", "has_data_page": True}],
        catalog_paths={"grapher/b/latest/y#y"},
    )
    whole = group_by_edit([reach, on_a_chart])
    (scoped,) = edits_for(Summary(reach=[reach, on_a_chart]), "mdims")
    assert len(whole) == 1 and scoped.n_texts == 1 and whole[0].n_texts == 2
    assert edit_slot(scoped) == edit_slot(whole[0])


def test_the_bar_says_a_section_can_be_read_either_way():
    """The two ways through a section are what the badges count, so the bar has to name them.

    A reviewer who never notices the layout switcher reads the tool as view-by-view only, and then the
    By-edit cards' ticks look like a second, unfinished job rather than the same one.
    """
    from streamlit.testing.v1 import AppTest

    def app() -> None:
        from apps.wizard.app_pages.metadata_diff.render import st_section_switcher

        st_section_switcher({"charts": (0, 3)}, empty=(), marks={"charts": "todo"})

    at = AppTest.from_function(app, default_timeout=60).run()
    assert not at.exception
    said = " ".join(str(getattr(el, "value", "") or "") for el in at.caption)
    assert "view by view" in said and "by edit" in said
    assert "finishes the section" in said


def test_a_section_is_done_along_whichever_layout_the_reviewer_took():
    """Ticking every view, or every edit, finishes a section; the two are never added up.

    The bar's emoji and the Review tab's "unfinished" warning both read this, so a reviewer who went view
    by view is not told the edit cards are still waiting for them.
    """
    from apps.wizard.app_pages.metadata_diff.core import section_progress

    totals = {
        surface_key("item", "mdim:grapher/a/latest/x#x"): 3,
        surface_key("item", "edit:mdims"): 1,
        surface_key("item", "chart"): 2,
        surface_key("item", "edit:charts"): 1,
        surface_key("item", "explorer:lis"): 400,
        surface_key("item", "edit:explorers"): 1,
    }
    ticked = {
        surface_key("item", "mdim:grapher/a/latest/x#x"): 3,  # view by view, complete
        surface_key("item", "edit:charts"): 1,  # by edit, complete
        surface_key("item", "explorer:lis"): 10,  # view by view, barely started
    }
    progress = section_progress(ticked, totals)
    assert progress["mdims"] == (3, 3)
    assert progress["charts"] == (1, 1)
    assert progress["explorers"] == (10, 400), "the layout with more progress stands for the section"
    assert section_progress({}, totals)["explorers"] == (0, 400), "untouched, the longer list is the one to do"


def test_a_layout_label_never_survives_as_a_value():
    """A section's own wording for the layout must never reach `?layout=` as a value.

    `st.segmented_control` sends and receives the *formatted* label, and hands the raw label back when it
    matches no current option. The three sections word the item option differently — "🔍 View by view" on
    MDims and Explorers, "🔍 Chart by chart" on Charts — under one shared key, so moving between sections
    produced exactly that, `url_persist` wrote the label into the URL, and the next load raised

        ValueError: Please review the URL query. Value 🔍 Chart by chart not in options ['items', 'changes'].

    Second time this behaviour broke this page: the section switcher was hand-rolled for the same reason.
    """
    from apps.wizard.app_pages.metadata_diff.core import DEFAULT_LAYOUT, LAYOUTS, coerce_layout

    # Every label any section can render maps back to the option it denotes.
    for label in ("🔍 View by view", "🔍 Chart by chart", "🔍 Anything by anything"):
        assert coerce_layout(label) == "items", label
    assert coerce_layout("🧬 By edit") == "changes"
    # The name before the rename to match Blast radius, still honoured so old links resolve.
    assert coerce_layout("🧬 By change") == "changes"

    # The options themselves pass through untouched, and junk lands on the default.
    for option in LAYOUTS:
        assert coerce_layout(option) == option
    for junk in (None, "", "views", 3, "🧬", "layout"):
        assert coerce_layout(junk) == DEFAULT_LAYOUT, junk


def test_the_layout_switcher_sanitizes_a_label_left_in_the_url():
    """The guard has to run before the widget exists, or `url_persist` raises on the way in.

    Reproduced through the real widget: a label in `?layout=` used to reach `_check_options_params` and
    kill the page. It must resolve to the option it denotes instead — and the URL must be left holding a
    value, not a label.
    """
    from streamlit.testing.v1 import AppTest

    def app() -> None:
        import streamlit as st

        from apps.wizard.app_pages.metadata_diff.render import st_layout_switcher

        # What the previous section's widget left behind.
        st.query_params["layout"] = "🔍 Chart by chart"
        layout = st_layout_switcher("🔍 View by view", "help")
        st.text(f"layout={layout}")
        st.text(f"url={st.query_params.get('layout')}")

    at = AppTest.from_function(app, default_timeout=30).run()
    assert not at.exception, at.exception
    assert at.text[0].value == "layout=items"
    assert at.text[1].value in ("url=items", "url=None"), at.text[1].value


def test_a_multiline_note_survives_the_report():
    """Markdown carries no newline inside a blockquote or a list item on its own.

    `> one\ntwo` quotes the first line and drops the second out of the quote; `  - one\ntwo` ends the list
    item and leaves the rest as a stray paragraph between bullets. Both showed up the moment a note had
    two lines in it — which the note box now invites, since Shift+Enter starts a new line.
    """
    from apps.wizard.app_pages.metadata_diff.review_section import _bullet_lines, _quoted

    quoted = _quoted("Line one.\n\nLine three.")
    assert quoted.splitlines() == ["> Line one.", ">", "> Line three."], quoted
    # Every line carries the marker, so the quote is one block rather than a quote and a paragraph.
    assert all(line.startswith(">") for line in quoted.splitlines())

    bullets = _bullet_lines("Line one.\nLine two.\n\nLine four.")
    assert bullets == ["  - Line one.", "    Line two.", "", "    Line four."], bullets
    # Continuations are indented past the bullet marker, which is what keeps them inside the item.
    assert all(line.startswith("    ") or not line for line in bullets[1:])

    # A single-line note is unchanged in both.
    assert _quoted("Just one line.") == "> Just one line."
    assert _bullet_lines("Just one line.") == ["  - Just one line."]

    # An empty note produces something renderable rather than an IndexError.
    assert _quoted("") == ">"
    assert _bullet_lines("") == ["  - "]


def test_a_section_badge_says_finished_or_not_and_nothing_else():
    """Two marks: ⏳ not yet, ✅ every item ticked — and nothing at all when unknown.

    A third mark for started-but-unfinished answered a question nobody asks of a nav bar, and made the two
    that matter harder to separate at a glance. Anything that is not finished reads the same.
    """
    from apps.wizard.app_pages.metadata_diff.core import REVIEW_MARKS, section_label

    assert set(REVIEW_MARKS) == {"todo", "done"}
    assert section_label("charts", {}, {"charts": "todo"}).endswith("⏳")
    assert section_label("charts", {}, {"charts": "done"}).endswith("✅")

    # No mark for this section, an unknown state, or no marks at all: the name alone. "partial" is one of
    # those unknowns now, so a stale caller cannot put a third emoji back on the bar.
    assert section_label("charts", {}, {"mdims": "done"}).endswith("Charts")
    assert section_label("charts", {}, {"charts": "partial"}).endswith("Charts")
    assert section_label("charts", {}, {"charts": "elsewhere"}).endswith("Charts")
    assert section_label("charts", {}).endswith("Charts")

    # Blast radius and Review hold no items, so they never carry one.
    assert section_label("blast", {}, {"blast": "done"}).endswith("Blast radius")
    assert section_label("review", {}, {"review": "todo"}).endswith("Review")


def test_the_shared_digest_trims_around_the_change():
    """A before/after pair has to contain the difference, wherever in the sentence it is.

    Trimming from the front produced two identical openings for an edit whose words moved later on — the
    digest showed "before: X…" and "after: X…" for a change that was real. Second time this branch made
    that mistake: the blast-radius preview had it too.
    """
    from apps.wizard.app_pages.metadata_diff.review_section import _around_change

    lead = "This data is expressed in international dollars at 2021 prices. " * 4
    before, after = _around_change(
        lead + "Compared with earlier editions.", lead + "Compared with earlier editions of this data."
    )
    assert before != after, (before, after)
    assert "earlier editions" in before and "of this data" in after
    assert before.startswith("…") and after.startswith("…"), "the identical opening is trimmed away"

    # A change at the very start needs no leading ellipsis and must still show both sides.
    before, after = _around_change("Alpha " + lead, "Omega " + lead)
    assert before.startswith("Alpha") and after.startswith("Omega")

    # WYSK is a list of bullets; it is flattened rather than printed as a Python list.
    before, after = _around_change(["One.", "Two."], ["One.", "Two.", "Three."])
    assert "[" not in before and "[" not in after
    assert "Three." in after and "Three." not in before

    # Identical values are not a diff, and must not be dressed up as one.
    same_before, same_after = _around_change("Unchanged text.", "Unchanged text.")
    assert same_before == same_after == "Unchanged text."
