"""Unit tests for the Metadata Review core (apps/metadata_review)."""

import textwrap

import pytest

import etl.grapher.model as gm
from apps.metadata_review.resolution import (
    Staleness,
    check_staleness,
    dimensions_to_view_id,
    resolve_field,
    slugify,
    suggestions_by_source_key,
)
from apps.metadata_review.targets import ReviewableField
from apps.metadata_review.trace import (
    EditCandidate,
    _trace_in_meta_yaml,
    _trace_mdim_page_field,
    _trace_mdim_view_field,
)


def _must(candidate: EditCandidate | None) -> EditCandidate:
    assert candidate is not None
    return candidate


# ---------------------------------------------------------------------------
# dimensionsToViewId / slugify parity (fixtures mirror owid-grapher's Util.ts)
# ---------------------------------------------------------------------------


def test_slugify_identity_for_snake_case():
    assert slugify("conflict_type") == "conflict_type"
    assert slugify("no_spells") == "no_spells"


def test_slugify_matches_js_behavior():
    assert slugify("CO₂ emissions") == "co2-emissions"
    assert slugify("Income/Wealth") == "incomewealth"
    assert slugify("  padded  ") == "padded"


def test_dimensions_to_view_id_sorts_and_joins():
    dims = {"indicator": "deaths", "conflict_type": "all", "people": "all", "estimate": "best"}
    assert dimensions_to_view_id(dims) == "conflict_type=all__estimate=best__indicator=deaths__people=all"


def test_dimensions_to_view_id_lowercases():
    assert dimensions_to_view_id({"Sex": "Female"}) == "sex=female"


# ---------------------------------------------------------------------------
# resolve_field (ported from faust-metadata-audit)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "override,inherited,expected",
    [
        ("A", "B", ("override", "A")),
        ("", "B", ("override", "")),  # explicit empty string suppresses inherited text
        (None, "B", ("inherited", "B")),
        (None, None, ("missing", None)),
        (None, "", ("inherited", "")),
    ],
)
def test_resolve_field(override, inherited, expected):
    assert resolve_field(override, inherited) == expected


# ---------------------------------------------------------------------------
# Source-keyed suggestions
# ---------------------------------------------------------------------------


def _field(**kwargs) -> ReviewableField:
    defaults = dict(
        target_type="mdim",
        target_path="ns/latest/thing#thing",
        view_id="metric=total",
        field_path="config.subtitle",
        label="Subtitle",
        provenance="inherited",
        current_value="Some text.",
        inherited_from="grapher/ns/latest/ds/table#col",
    )
    defaults.update(kwargs)
    return ReviewableField(**defaults)


def test_source_key_inherited_view_field_routes_to_indicator():
    field = _field(provenance="inherited")
    assert field.source_key() == ("indicator", "grapher/ns/latest/ds/table#col", None, "grapher_config.subtitle")


def test_source_key_missing_view_field_routes_to_indicator():
    field = _field(provenance="missing", current_value=None)
    assert field.source_key()[0] == "indicator"


def test_source_key_override_stays_on_view():
    field = _field(provenance="override")
    assert field.source_key() == ("mdim", "ns/latest/thing#thing", "metric=total", "config.subtitle")


def test_source_key_page_level_stays_on_mdim():
    field = _field(view_id=None, field_path="title.title", provenance="override", inherited_from=None)
    assert field.source_key() == ("mdim", "ns/latest/thing#thing", None, "title.title")


def test_suggestions_by_source_key_groups():
    s1 = gm.MetadataReviewSuggestion(
        targetType="indicator", targetPath="grapher/a/b/c/t#x", fieldPath="description_short",
        provenance="inherited", createdBy=1,
    )  # fmt: skip
    s2 = gm.MetadataReviewSuggestion(
        targetType="indicator", targetPath="grapher/a/b/c/t#x", fieldPath="description_short",
        provenance="inherited", createdBy=2,
    )  # fmt: skip
    grouped = suggestions_by_source_key([s1, s2])
    assert list(grouped) == [("indicator", "grapher/a/b/c/t#x", None, "description_short")]
    assert len(grouped[("indicator", "grapher/a/b/c/t#x", None, "description_short")]) == 2


# ---------------------------------------------------------------------------
# Shared text across views (different expanded indicators, same rendered value)
# ---------------------------------------------------------------------------


def _mdim_with_shared_text():
    from apps.metadata_review.targets import MdimReview, ViewReview

    def view_field(view_id, indicator, value):
        return _field(
            view_id=view_id,
            provenance="inherited",
            current_value=value,
            inherited_from=indicator,
            field_path="config.subtitle",
        )

    review = MdimReview(
        target_path="ns/latest/thing#thing", slug="thing", title="T", title_variant=None, page_checksum="x"
    )
    # Views a and b render the SAME text via DIFFERENT expanded indicators; c differs.
    for view_id, indicator, value in [
        ("metric=a", "grapher/ns/latest/ds/t#col__a", "Shared subtitle."),
        ("metric=b", "grapher/ns/latest/ds/t#col__b", "Shared subtitle."),
        ("metric=c", "grapher/ns/latest/ds/t#col__c", "Different subtitle."),
    ]:
        review.views.append(
            ViewReview(
                view_id=view_id,
                dimensions={"metric": view_id.split("=")[1]},
                indicator_path=indicator,
                fields=[view_field(view_id, indicator, value)],
            )
        )
    return review


def test_shared_view_ids_matches_by_value_across_indicators():
    from apps.metadata_review.resolution import shared_view_ids

    review = _mdim_with_shared_text()
    field_a = review.views[0].fields[0]
    assert shared_view_ids(review, field_a) == ["metric=b"]
    field_c = review.views[2].fields[0]
    assert shared_view_ids(review, field_c) == []


def test_threads_for_field_borrows_across_indicators():
    from apps.metadata_review.resolution import suggestions_by_source_key, threads_for_field

    review = _mdim_with_shared_text()
    # A suggestion filed on view a's indicator must show on view b's identical field.
    suggestion = gm.MetadataReviewSuggestion(
        targetType="indicator",
        targetPath="grapher/ns/latest/ds/t#col__a",
        fieldPath="grapher_config.subtitle",
        provenance="inherited",
        createdBy=1,
        currentValue="Shared subtitle.",
    )
    suggestion.id = 1
    grouped = suggestions_by_source_key([suggestion])
    field_b = review.views[1].fields[0]
    assert [s.id for s in threads_for_field(review, field_b, grouped)] == [1]
    field_c = review.views[2].fields[0]
    assert threads_for_field(review, field_c, grouped) == []


# ---------------------------------------------------------------------------
# Pattern sharing: same template, different dimension words per view
# ---------------------------------------------------------------------------


def _mdim_with_patterned_titles():
    from apps.metadata_review.targets import DimensionChoice, DimensionInfo, MdimReview, ViewReview

    review = MdimReview(
        target_path="wid/latest/incomes#incomes", slug="incomes", title="T", title_variant=None, page_checksum="x"
    )
    review.dimensions = [
        DimensionInfo(
            slug="quantile",
            name="Group",
            choices=[
                DimensionChoice(slug="richest_1pct", name="Richest 1%"),
                DimensionChoice(slug="richest_0_1pct", name="Richest 0.1%"),
            ],
        ),
        DimensionInfo(
            slug="welfare_type",
            name="Income measure",
            choices=[DimensionChoice(slug="before_tax", name="before tax")],
        ),
    ]
    titles = {
        "richest_1pct": "Income share of the richest 1% (before tax)",
        "richest_0_1pct": "Income share of the richest 0.1% (before tax)",
    }
    for quantile, title in titles.items():
        view_id = f"quantile={quantile}__welfare_type=before_tax"
        review.views.append(
            ViewReview(
                view_id=view_id,
                dimensions={"quantile": quantile, "welfare_type": "before_tax"},
                indicator_path=f"grapher/wid/latest/incomes/t#share__{quantile}",
                fields=[
                    _field(
                        view_id=view_id,
                        field_path="config.title",
                        provenance="inherited",
                        current_value=title,
                        inherited_from=f"grapher/wid/latest/incomes/t#share__{quantile}",
                    )
                ],
            )
        )
    return review


def test_parametrize_value_uses_choice_names():
    from apps.metadata_review.resolution import parametrize_value

    review = _mdim_with_patterned_titles()
    out = parametrize_value(
        review,
        {"quantile": "richest_1pct", "welfare_type": "before_tax"},
        "Income share of the richest 1% (before tax)",
    )
    assert out is not None
    pattern, matches = out
    assert pattern == "Income share of the {quantile} ({welfare_type})"
    assert matches == {"quantile": "richest 1%", "welfare_type": "before tax"}


def test_pattern_connects_titles_across_views():
    from apps.metadata_review.resolution import shared_view_ids, suggestions_by_source_key, threads_for_field

    review = _mdim_with_patterned_titles()
    field_1pct = review.views[0].fields[0]
    assert shared_view_ids(review, field_1pct) == ["quantile=richest_0_1pct__welfare_type=before_tax"]
    # A title suggestion filed on the 1% view shows on the 0.1% view.
    suggestion = gm.MetadataReviewSuggestion(
        targetType="indicator",
        targetPath="grapher/wid/latest/incomes/t#share__richest_1pct",
        fieldPath="grapher_config.title",
        provenance="inherited",
        createdBy=1,
        currentValue="Income share of the richest 1% (before tax)",
        suggestedValue="Income share received by the richest 1% (before tax)",
        filedFromViewId="quantile=richest_1pct__welfare_type=before_tax",
    )
    suggestion.id = 1
    grouped = suggestions_by_source_key([suggestion])
    field_01 = review.views[1].fields[0]
    # NOTE: view fields map config.title -> indicator key grapher_config.title.
    assert [s.id for s in threads_for_field(review, field_01, grouped)] == [1]


def test_threads_borrow_across_pages_by_snapshot_text():
    """A thread filed on another MDim's indicator (same garden metadata, identical
    text) surfaces on this page's matching field."""
    from apps.metadata_review.resolution import check_staleness, suggestions_by_source_key, threads_for_field

    review = _mdim_with_shared_text()
    foreign = gm.MetadataReviewSuggestion(
        targetType="indicator",
        # An indicator NOT used by any view of this MDim (sibling MDim's source).
        targetPath="grapher/ns/latest/ds/other_table#palma",
        fieldPath="grapher_config.subtitle",
        provenance="inherited",
        createdBy=1,
        currentValue="Shared subtitle.",
        filedFromPath="ns/latest/other#other",
    )
    foreign.id = 99
    grouped = suggestions_by_source_key([foreign])
    field_a = review.views[0].fields[0]
    assert [s.id for s in threads_for_field(review, field_a, grouped)] == [99]
    # Different text on view c -> not borrowed there.
    assert threads_for_field(review, review.views[2].fields[0], grouped) == []
    # Staleness judged against the displaying field, not flagged target_gone.
    fields_by_key = {field_a.source_key(): field_a}
    staleness = check_staleness(foreign, fields_by_key, display_field=field_a)
    assert not staleness.target_gone and not staleness.field_changed


def test_transfer_proposal_rerenders_dimension_words():
    from apps.metadata_review.resolution import transfer_proposal

    review = _mdim_with_patterned_titles()
    suggestion = gm.MetadataReviewSuggestion(
        targetType="indicator",
        targetPath="grapher/wid/latest/incomes/t#share__richest_1pct",
        fieldPath="grapher_config.title",
        provenance="inherited",
        createdBy=1,
        currentValue="Income share of the richest 1% (before tax)",
        suggestedValue="Income share received by the richest 1% (before tax)",
        filedFromViewId="quantile=richest_1pct__welfare_type=before_tax",
    )
    field_01 = review.views[1].fields[0]
    assert transfer_proposal(review, suggestion, field_01) == "Income share received by the richest 0.1% (before tax)"
    # Editing the dimension word itself blocks the transfer.
    suggestion.suggestedValue = "Income share of the wealthiest people (before tax)"
    assert transfer_proposal(review, suggestion, field_01) is None


# ---------------------------------------------------------------------------
# Bullet diffs (description_key consolidation)
# ---------------------------------------------------------------------------


def test_split_bullets_prose_and_list():
    from apps.metadata_review.diffs import split_bullets

    assert split_bullets(None) == []
    assert split_bullets("Single prose item.") == ["Single prose item."]
    assert split_bullets("- One\n- Two\n  continued\n- Three") == ["One", "Two continued", "Three"]


def test_bullet_diff_only_changed_bullets():
    from apps.metadata_review.diffs import bullet_diff, diff_summary, tracked_changes_html

    current = "- Keep me\n- Old wording\n- Also keep"
    proposed = "- Keep me\n- New wording\n- Also keep\n- Brand new bullet"
    ops = bullet_diff(current, proposed)
    assert [(o.op, o.text) for o in ops] == [
        ("keep", "Keep me"),
        ("remove", "Old wording"),
        ("add", "New wording"),
        ("keep", "Also keep"),
        ("add", "Brand new bullet"),
    ]
    assert diff_summary(ops) == "2 added, 1 removed; 2 unchanged"
    # The tracked rendering shows the ENTIRE list: unchanged bullets in full,
    # the changed one with inline word tracking, additions tinted.
    out = tracked_changes_html(current, proposed, is_bullet_list=True)
    assert "Keep me" in out and "Also keep" in out
    assert "unchanged" not in out  # no collapsed placeholders
    assert "<del" in out and "Old" in out
    # The changed bullet tracks word-by-word, so markup splits the phrase.
    assert "<ins" in out and "New" in out and "wording" in out and "Brand new bullet" in out


def test_word_diff_html_tracks_changes_inline():
    from apps.metadata_review.diffs import word_diff_html

    out = word_diff_html("The mean income per person.", "The median income per capita.")
    assert "<del" in out and "mean" in out
    assert "<ins" in out and "median" in out
    assert "income" in out  # unchanged words kept as plain text
    # HTML in field text must be escaped, not rendered.
    assert "<b>" not in word_diff_html("a <b>bold</b> word", "a <b>bold</b> word changed")


def test_tracked_changes_html_bullets():
    from apps.metadata_review.diffs import tracked_changes_html

    out = tracked_changes_html(
        "- Keep\n- Old wording here\n- Tail", "- Keep\n- New wording here\n- Tail", is_bullet_list=True
    )
    # Every bullet shows (full text always); the changed one is a single bullet
    # with inline word tracking, not a remove+add pair.
    assert out.count("<li>") == 3
    assert "Keep" in out and "Tail" in out
    assert "<del" in out and "Old" in out and "<ins" in out and "New" in out
    assert "unchanged" not in out


def test_apply_bullet_edits_transfers_shared_bullet():
    from apps.metadata_review.diffs import apply_bullet_edits

    # The proposal edits a shared (anchored) bullet inside a DIFFERENT list.
    out = apply_bullet_edits(
        "- Own bullet A\n- Shared warning\n- Own bullet B",
        "- Own bullet A\n- Shared warning, reworded\n- Own bullet B",
        "- Other page intro\n- Shared warning\n- Other page outro",
    )
    assert out == ("- Other page intro\n- Shared warning, reworded\n- Other page outro", 1, 1)
    # Editing a bullet the target list doesn't have -> no transfer.
    assert apply_bullet_edits("- Not shared", "- Not shared, changed", "- Something else entirely") is None
    # A no-op proposal never transfers.
    assert apply_bullet_edits("- A", "- A", "- A\n- B") is None


def test_apply_bullet_edits_partial_transfer():
    from apps.metadata_review.diffs import apply_bullet_edits

    # One edited bullet is shared, the other is page-specific -> partial carry.
    out = apply_bullet_edits(
        "- Page-specific intro\n- Shared source note",
        "- Page-specific intro, reworded\n- Shared source note, reworded",
        "- DIFFERENT intro\n- Shared source note\n- Extra bullet",
    )
    assert out == ("- DIFFERENT intro\n- Shared source note, reworded\n- Extra bullet", 1, 2)


def test_apply_bullet_edits_additions_need_full_rewrite_match():
    from apps.metadata_review.diffs import apply_bullet_edits

    # A pure addition rides along when ALL rewrites apply...
    out = apply_bullet_edits(
        "- Shared note",
        "- Shared note, reworded\n- Brand new bullet",
        "- Shared note\n- Other page tail",
    )
    assert out == ("- Shared note, reworded\n- Brand new bullet\n- Other page tail", 2, 2)
    # ...but NOT when a page-specific rewrite was skipped (different list variant —
    # the addition may carry that variant's context, e.g. after-tax-only content).
    out = apply_bullet_edits(
        "- Dimension-specific bullet\n- Shared note",
        "- Dimension-specific bullet, reworded\n- Shared note, reworded\n- Dimension-specific addition",
        "- OTHER dimension bullet\n- Shared note",
    )
    assert out == ("- OTHER dimension bullet\n- Shared note, reworded", 1, 3)


def test_threads_bullet_borrow_across_sibling_views_only_for_shared_bullets():
    """Across views of one MDim (e.g. before vs after tax), a bullet-list thread
    carries only when it edits bullets shared VERBATIM by both lists; edits to
    dimension-specific bullets stay on their own views."""
    from apps.metadata_review.resolution import suggestions_by_source_key, threads_for_field
    from apps.metadata_review.targets import MdimReview, ViewReview

    review = MdimReview(
        target_path="wid/latest/incomes#incomes", slug="incomes", title="T", title_variant=None, page_checksum="x"
    )
    lists = {
        "before_tax": "- Shared generic bullet\n- Income is measured before taxes.",
        "after_tax": "- Shared generic bullet\n- Income is measured after taxes.",
    }
    for welfare, bullets in lists.items():
        view_id = f"welfare_type={welfare}"
        indicator = f"grapher/wid/latest/inc/t#share__{welfare}"
        review.views.append(
            ViewReview(
                view_id=view_id,
                dimensions={"welfare_type": welfare},
                indicator_path=indicator,
                fields=[
                    _field(
                        view_id=view_id,
                        field_path="metadata.description_key",
                        provenance="inherited",
                        current_value=bullets,
                        inherited_from=indicator,
                    )
                ],
            )
        )
    # A thread on the after-tax indicator editing the SHARED bullet carries to the
    # before-tax view (the edit genuinely applies there).
    shared_edit = gm.MetadataReviewSuggestion(
        targetType="indicator",
        targetPath="grapher/wid/latest/inc/t#share__after_tax",
        fieldPath="description_key",
        provenance="inherited",
        createdBy=1,
        currentValue=lists["after_tax"],
        suggestedValue="- Shared generic bullet, reworded\n- Income is measured after taxes.",
        filedFromViewId="welfare_type=after_tax",
    )
    shared_edit.id = 7
    # A thread editing only the DIMENSION-SPECIFIC bullet stays on its own views.
    variant_edit = gm.MetadataReviewSuggestion(
        targetType="indicator",
        targetPath="grapher/wid/latest/inc/t#share__after_tax",
        fieldPath="description_key",
        provenance="inherited",
        createdBy=1,
        currentValue=lists["after_tax"],
        suggestedValue="- Shared generic bullet\n- Income is measured after taxes, reworded.",
        filedFromViewId="welfare_type=after_tax",
    )
    variant_edit.id = 8
    grouped = suggestions_by_source_key([shared_edit, variant_edit])
    before_field = review.views[0].fields[0]
    assert [s.id for s in threads_for_field(review, before_field, grouped)] == [7]
    # Its own view shows both.
    after_field = review.views[1].fields[0]
    assert [s.id for s in threads_for_field(review, after_field, grouped)] == [7, 8]


def test_bullet_diff_no_changes():
    from apps.metadata_review.diffs import bullet_diff, diff_summary

    ops = bullet_diff("- A\n- B", "- A\n- B")
    assert all(o.op == "keep" for o in ops)
    assert diff_summary(ops) == "no bullet changes; 2 unchanged"


# ---------------------------------------------------------------------------
# Staleness
# ---------------------------------------------------------------------------


def _suggestion(**kwargs) -> gm.MetadataReviewSuggestion:
    defaults = dict(
        targetType="indicator",
        targetPath="grapher/ns/latest/ds/table#col",
        viewId=None,
        fieldPath="grapher_config.subtitle",
        provenance="inherited",
        createdBy=1,
        currentValue="Old text.",
        pageChecksum="abc",
    )
    defaults.update(kwargs)
    return gm.MetadataReviewSuggestion(**defaults)


def _fields_by_key(field: ReviewableField) -> dict:
    return {field.source_key(): field}


def test_staleness_unchanged():
    field = _field(current_value="Old text.", page_checksum="abc")
    staleness = check_staleness(_suggestion(), _fields_by_key(field))
    assert not staleness.is_stale and not staleness.page_changed


def test_staleness_value_changed():
    field = _field(current_value="New text.", page_checksum="abc")
    staleness = check_staleness(_suggestion(), _fields_by_key(field))
    assert staleness.field_changed and staleness.current_value == "New text."


def test_staleness_provenance_flipped():
    # The view gained an override replacing the inherited text -> stale.
    field = _field(provenance="override", current_value="Old text.")
    # An override field keys to the view, so the indicator key disappears -> target_gone.
    staleness = check_staleness(_suggestion(), _fields_by_key(field))
    assert staleness.target_gone


def test_staleness_page_changed_only():
    field = _field(current_value="Old text.", page_checksum="different")
    staleness = check_staleness(_suggestion(), _fields_by_key(field))
    assert not staleness.is_stale and staleness.page_changed


def test_staleness_target_gone():
    staleness = check_staleness(_suggestion(), {})
    assert staleness.target_gone and staleness.is_stale
    assert Staleness(target_gone=True).is_stale


# ---------------------------------------------------------------------------
# Tracer: garden .meta.yml (Phase A/B/C replay)
# ---------------------------------------------------------------------------

GARDEN_META = """\
definitions:
  attention: &attention |-
    This is a shared warning.
  common:
    presentation:
      grapher_config:
        note: A note for every variable in every table.

tables:
  main:
    common:
      description_short: Common description.
    variables:
      deaths:
        title: Deaths
        description_short: |-
          <% if cause == "all" %>Deaths from all causes.<% else %>Deaths from << cause >>.<% endif %>
        presentation:
          grapher_config:
            subtitle: *attention
      births:
        title: Births
        presentation:
          grapher_config:
            subtitle: *attention
"""


@pytest.fixture
def garden_meta(tmp_path):
    path = tmp_path / "ds.meta.yml"
    path.write_text(textwrap.dedent(GARDEN_META))
    return path


def test_trace_jinja_variable_field(garden_meta):
    candidate = _trace_in_meta_yaml(
        garden_meta,
        table_name="main",
        var_name="deaths",
        field_path="description_short",
        dim_dict={"cause": "malaria"},
        current_value="Deaths from malaria.",
    )
    assert candidate is not None
    assert candidate.kind == "jinja"
    assert candidate.supplied_by == "tables.main.variables.deaths"
    assert candidate.yaml_path == "tables.main.variables.deaths.description_short"
    assert candidate.render_verified is True
    assert candidate.render_context == {"cause": "malaria"}


def test_trace_jinja_other_branch(garden_meta):
    candidate = _must(
        _trace_in_meta_yaml(
            garden_meta,
            table_name="main",
            var_name="deaths",
            field_path="description_short",
            dim_dict={"cause": "all"},
            current_value="Deaths from all causes.",
        )
    )
    assert candidate.render_verified is True


def test_trace_common_block(garden_meta):
    # `births` has no description_short of its own -> supplied by tables.main.common.
    candidate = _must(
        _trace_in_meta_yaml(
            garden_meta,
            table_name="main",
            var_name="births",
            field_path="description_short",
            dim_dict={},
            current_value="Common description.",
        )
    )
    assert candidate.kind == "common-block"
    assert candidate.supplied_by == "tables.main.common"
    assert candidate.render_verified is True
    # deaths overrides the key itself, so it is NOT affected by editing the common block.
    assert candidate.shared_with == []


def test_trace_definitions_common(garden_meta):
    candidate = _must(
        _trace_in_meta_yaml(
            garden_meta,
            table_name="main",
            var_name="deaths",
            field_path="grapher_config.note",
            dim_dict={},
            current_value="A note for every variable in every table.",
        )
    )
    assert candidate.supplied_by == "definitions.common"
    assert candidate.render_verified is True
    # births doesn't define note itself -> affected by editing definitions.common.
    assert candidate.shared_with == ["births"]


def test_trace_anchor_detection(garden_meta):
    candidate = _must(
        _trace_in_meta_yaml(
            garden_meta,
            table_name="main",
            var_name="deaths",
            field_path="grapher_config.subtitle",
            dim_dict={},
            current_value="This is a shared warning.",
        )
    )
    assert candidate.supplied_by == "tables.main.variables.deaths"
    assert candidate.render_verified is True
    # The anchor is shared with births.
    assert candidate.shared_with == ["births"]
    assert any("anchor" in note for note in candidate.notes)


def test_trace_absent_field_returns_none(garden_meta):
    candidate = _trace_in_meta_yaml(
        garden_meta,
        table_name="main",
        var_name="deaths",
        field_path="grapher_config.title",
        dim_dict={},
        current_value="whatever",
    )
    assert candidate is None


def test_trace_render_mismatch_flagged(garden_meta):
    candidate = _must(
        _trace_in_meta_yaml(
            garden_meta,
            table_name="main",
            var_name="deaths",
            field_path="description_short",
            dim_dict={"cause": "malaria"},
            current_value="Completely different live text.",
        )
    )
    assert candidate.render_verified is False
    assert any("did NOT reproduce" in note for note in candidate.notes)


# ---------------------------------------------------------------------------
# Tracer: MDim config .yml
# ---------------------------------------------------------------------------

MDIM_CONFIG = """\
definitions:
  common_views:
    - config:
        note: A footnote for all views.
    - dimensions:
        metric: total
      config:
        subtitle: Total subtitle from common_views.

title:
  title: My MDim
  title_variant: by metric

dimensions:
  - slug: metric
    name: Metric
    choices:
      - slug: total
        name: Total number
      - slug: rate
        name: Rate
        description: Per 100,000 people.

views:
  - dimensions:
      metric: rate
    config:
      title: Rate view title
"""


@pytest.fixture
def mdim_config(tmp_path):
    path = tmp_path / "thing.config.yml"
    path.write_text(textwrap.dedent(MDIM_CONFIG))
    return path


def test_trace_mdim_page_title(mdim_config):
    config = _load_mdim(mdim_config)
    candidate = _must(_trace_mdim_page_field(config, mdim_config, "title.title"))
    assert candidate.yaml_path == "title.title"
    assert candidate.template == "My MDim"


def test_trace_mdim_choice_description(mdim_config):
    config = _load_mdim(mdim_config)
    candidate = _must(_trace_mdim_page_field(config, mdim_config, "dimensions.metric.choices.rate.description"))
    assert candidate.yaml_path == "dimensions[0].choices[1].description"
    assert candidate.template == "Per 100,000 people."


def test_trace_mdim_missing_choice_returns_none(mdim_config):
    config = _load_mdim(mdim_config)
    assert _trace_mdim_page_field(config, mdim_config, "dimensions.metric.choices.total.description") is None


def test_trace_mdim_literal_view(mdim_config):
    config = _load_mdim(mdim_config)
    candidate = _must(_trace_mdim_view_field(config, mdim_config, "metric=rate", "config.title"))
    assert candidate.kind == "mdim-view"
    assert candidate.yaml_path == "views[0].config.title"


def test_trace_mdim_common_views(mdim_config):
    config = _load_mdim(mdim_config)
    # subtitle comes from the metric=total common_views entry (more specific).
    candidate = _must(_trace_mdim_view_field(config, mdim_config, "metric=total", "config.subtitle"))
    assert candidate.kind == "mdim-common-views"
    assert candidate.yaml_path == "definitions.common_views[1].config.subtitle"
    # note comes from the catch-all entry.
    candidate = _must(_trace_mdim_view_field(config, mdim_config, "metric=total", "config.note"))
    assert candidate.yaml_path == "definitions.common_views[0].config.note"


def test_trace_mdim_generated_view_returns_none(mdim_config):
    config = _load_mdim(mdim_config)
    assert _trace_mdim_view_field(config, mdim_config, "metric=total", "config.title") is None


def _load_mdim(path):
    from owid.catalog.core.utils import dynamic_yaml_load, dynamic_yaml_to_dict

    return dynamic_yaml_to_dict(dynamic_yaml_load(path))
