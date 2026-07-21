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
