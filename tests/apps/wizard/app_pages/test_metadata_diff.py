"""Tests for the Metadata Diff blast-radius logic (pure, no DB).

These cover the key distinction the tool makes: a changed field that comes from the shared
indicator metadata (propagates to charts / other MDIMs) vs. one that comes from an MDIM-level
override (contained to the MDIM).
"""

from apps.owidbot.metadata_diff import format_metadata_diff, status_icon
from apps.wizard.app_pages.metadata_diff.brief import decision
from apps.wizard.app_pages.metadata_diff.core import (
    ChangeGroup,
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
from apps.wizard.app_pages.metadata_diff.datapage import ordered_slots
from apps.wizard.app_pages.metadata_diff.discovery import (
    BranchScope,
    ExplorerChanges,
    Summary,
    _count_fields,
    _dataset_of,
    compare_explorer_views,
    compare_indicator_texts,
    mdim_short_name,
    narrow_to_branch,
    split_mdim_groups,
)
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


def test_brief_decision_routes_from_resolved_rows():
    """The brief builders are pure now: the decision comes in on the row, not out of session state."""
    assert decision({"stale": False, "label": "✅ Approve", "seed_label": "⏳ Pending"}) == "approved"
    assert decision({"stale": False, "label": "🚩 Flag", "seed_label": "⏳ Pending"}) == "flagged"
    assert decision({"stale": False, "label": None, "seed_label": "⏳ Pending"}) == "pending"
    # A change edited since it was reviewed is never treated as approved.
    assert decision({"stale": True, "label": "✅ Approve", "seed_label": "✅ Approve"}) == "stale"


# --- Attribution: this branch's change, or the baseline moving on? --------------------------------


def test_branch_scope_separates_data_steps_from_export_recipes():
    """`export://` URIs identify a changed MDim/explorer recipe; everything else is a dataset path."""
    scope = BranchScope(
        dataset_paths={"garden/wid/2026-06-18/world_inequality_database"},
        export_shorts={"incomes_wid"},
    )
    assert scope.covers_indicator("grapher/wid/2026-06-18/world_inequality_database/tb#share_top_1")
    assert not scope.covers_indicator("grapher/wb/2026-06-26/world_bank_pip/tb#gini")
    assert scope.covers_export("incomes_wid")
    assert not scope.covers_export("poverty_pip")


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

    scope = BranchScope(dataset_paths=set(), export_shorts=set())
    ours, other = split_mdim_groups("grapher/ns/latest/mine#mine", [indicator_change, config_change], scope)
    assert [g.field for g in ours] == ["descriptionKey"]
    assert [g.field for g in other] == ["chart.title"]

    # When the branch edits the MDim's own recipe, its config-level edits are exactly the point.
    scope_with_recipe = BranchScope(dataset_paths=set(), export_shorts={"mine"})
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
        export_shorts=set(),
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
