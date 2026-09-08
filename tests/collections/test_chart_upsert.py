"""Tests for `etl.collection.chart_upsert._build_chart_config`.

`_build_chart_config` translates a zero-dimension collection's single `View`
(its `config` + `indicators`) into the grapher chart-config dict that gets
written to `chart_configs.etlConfig`. The only external dependency is
`map_indicator_path_to_id` (a DB lookup), which is mocked here so the tests
stay pure.
"""

from unittest.mock import patch

import pytest

from etl.collection.chart_upsert import _build_chart_config
from etl.collection.model.core import Collection, Definitions
from etl.collection.model.dimension import Dimension, DimensionChoice
from etl.collection.model.view import View, ViewIndicators
from etl.config import DEFAULT_GRAPHER_SCHEMA

# Catalog-path -> variable-id map used to stub the DB lookup.
_PATH_TO_ID = {
    "table#ind1": 111,
    "table#ind2": 222,
    "table#x": 333,
    "table#size": 444,
    "table#color": 555,
}


def _make_view(indicators: dict, config: dict | None = None) -> View:
    return View(
        dimensions={},
        indicators=ViewIndicators.from_dict(indicators),
        config=config,
    )


def _build(view: View, slug: str = "my-chart") -> dict:
    with patch(
        "etl.collection.chart_upsert.map_indicator_path_to_id",
        side_effect=lambda path: _PATH_TO_ID[path],
    ):
        return _build_chart_config(view, slug)


def test_single_y_indicator():
    config = _build(_make_view({"y": "table#ind1"}))
    assert config["slug"] == "my-chart"
    assert config["$schema"] == DEFAULT_GRAPHER_SCHEMA
    assert config["dimensions"] == [{"property": "y", "variableId": 111}]


def test_multiple_y_indicators_preserve_order():
    config = _build(_make_view({"y": ["table#ind1", "table#ind2"]}))
    assert config["dimensions"] == [
        {"property": "y", "variableId": 111},
        {"property": "y", "variableId": 222},
    ]


def test_axis_order_is_y_x_size_color():
    # Indicators declared out of order; output must follow _AXIS_ORDER.
    view = _make_view(
        {
            "color": "table#color",
            "size": "table#size",
            "x": "table#x",
            "y": "table#ind1",
        }
    )
    config = _build(view)
    assert [(d["property"], d["variableId"]) for d in config["dimensions"]] == [
        ("y", 111),
        ("x", 333),
        ("size", 444),
        ("color", 555),
    ]


def test_indicator_display_is_attached():
    view = _make_view({"y": [{"catalogPath": "table#ind1", "display": {"name": "Label A"}}]})
    config = _build(view)
    assert config["dimensions"] == [{"property": "y", "variableId": 111, "display": {"name": "Label A"}}]


def test_sort_column_slug_rewritten_to_id_string():
    view = _make_view({"y": "table#ind1"}, config={"sortColumnSlug": "table#ind2"})
    config = _build(view)
    assert config["sortColumnSlug"] == "222"


def test_map_column_slug_rewritten_to_id_string():
    view = _make_view({"y": "table#ind1"}, config={"map": {"columnSlug": "table#ind2"}})
    config = _build(view)
    assert config["map"]["columnSlug"] == "222"


def test_existing_schema_is_preserved():
    custom_schema = "https://files.ourworldindata.org/schemas/grapher-schema.001.json"
    view = _make_view({"y": "table#ind1"}, config={"$schema": custom_schema})
    config = _build(view)
    assert config["$schema"] == custom_schema


def test_no_indicators_raises():
    view = _make_view({})
    with pytest.raises(ValueError, match="no indicators"):
        _build(view)


def test_chart_config_id_required_for_single_charts():
    collection = Collection(
        catalog_path="animal_welfare/latest/my_chart#my_chart",
        dimensions=[],
        views=[_make_view({"y": "table#ind1"})],
        _definitions=Definitions(),
    )
    with pytest.raises(ValueError, match="missing a top-level `chart_config_id`"):
        collection.validate_chart_config_id()


def _chart_collection(chart_config_id) -> Collection:
    return Collection(
        catalog_path="animal_welfare/latest/my_chart#my_chart",
        dimensions=[],
        views=[_make_view({"y": "table#ind1"})],
        chart_config_id=chart_config_id,
        _definitions=Definitions(),
    )


def test_chart_config_id_must_be_a_uuid():
    with pytest.raises(ValueError, match="invalid `chart_config_id`"):
        _chart_collection("7118").validate_chart_config_id()


def test_chart_config_id_must_be_a_string():
    # An unquoted numeric-looking value in the YAML would arrive as an int.
    with pytest.raises(ValueError, match="expected a UUID string, got int"):
        _chart_collection(7118).validate_chart_config_id()  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "value",
    [
        # All of these parse as UUIDs but wouldn't match the CHAR(36) lookup key in grapher.
        "0191b6c7559570b28d30fa03fccd7add",
        "{0191b6c7-5595-70b2-8d30-fa03fccd7add}",
        "urn:uuid:0191b6c7-5595-70b2-8d30-fa03fccd7add",
        "0191B6C7-5595-70B2-8D30-FA03FCCD7ADD",
    ],
)
def test_chart_config_id_must_be_canonical(value):
    with pytest.raises(ValueError, match="non-canonical `chart_config_id`"):
        _chart_collection(value).validate_chart_config_id()


def test_chart_config_id_warns_when_not_uuid7(capsys):
    # A canonical UUIDv4 is accepted but flagged — grapher only ever mints v7.
    _chart_collection("6e8bc430-9c3a-41d9-a52e-b0a9b8b64f3d").validate_chart_config_id()
    assert "collection.chart_config_id.not_uuid7" in capsys.readouterr().out


def test_chart_config_id_accepted_for_single_charts():
    collection = Collection(
        catalog_path="animal_welfare/latest/my_chart#my_chart",
        dimensions=[],
        views=[_make_view({"y": "table#ind1"})],
        chart_config_id="0191b6c7-5595-70b2-8d30-fa03fccd7add",
        _definitions=Definitions(),
    )
    # Should not raise.
    collection.validate_chart_config_id()


def test_chart_config_id_rejected_on_mdims():
    collection = Collection(
        catalog_path="animal_welfare/latest/my_mdim#my_mdim",
        dimensions=[Dimension(slug="sex", name="Sex", choices=[DimensionChoice(slug="female", name="Female")])],
        views=[View(dimensions={"sex": "female"}, indicators=ViewIndicators.from_dict({"y": "table#ind1"}))],
        chart_config_id="0191b6c7-5595-70b2-8d30-fa03fccd7add",
        _definitions=Definitions(),
    )
    with pytest.raises(ValueError, match="declares `chart_config_id` but has dimensions"):
        collection.validate_chart_config_id()


def test_new_chart_config_id_is_valid_uuid7_and_time_ordered():
    import time
    import uuid as uuid_module

    from etl.collection.chart_upsert import new_chart_config_id

    before_ms = time.time_ns() // 1_000_000
    generated = uuid_module.UUID(new_chart_config_id())
    after_ms = time.time_ns() // 1_000_000

    assert generated.version == 7
    assert generated.variant == uuid_module.RFC_4122
    # The top 48 bits are the unix-ms timestamp.
    assert before_ms <= generated.int >> 80 <= after_ms


def _single_choice_mdim() -> Collection:
    # A genuine mdim whose only dimension has a single choice in use — after
    # `prune_dimensions()` its dimension list is empty, but it must NOT be
    # reclassified as a single chart (it declared no `chart_config_id`).
    return Collection(
        catalog_path="animal_welfare/latest/my_mdim#my_mdim",
        dimensions=[Dimension(slug="sex", name="Sex", choices=[DimensionChoice(slug="female", name="Female")])],
        views=[View(dimensions={"sex": "female"}, indicators=ViewIndicators.from_dict({"y": "table#ind1"}))],
        _definitions=Definitions(),
    )


def test_pruned_mdim_is_not_reclassified_as_chart():
    collection = _single_choice_mdim()
    collection.validate_chart_config_id()  # passes as an mdim
    collection.prune_dimensions()
    assert collection.dimensions == []
    with pytest.raises(ValueError, match="pass `prune_dimensions=False`"):
        collection.upsert_to_db(owid_env=object())  # type: ignore[arg-type]


def test_declared_single_chart_routes_to_chart_upsert():
    collection = _chart_collection("0191b6c7-5595-70b2-8d30-fa03fccd7add")
    with patch("etl.collection.chart_upsert.upsert_collection_as_chart") as mock_upsert:
        collection.upsert_to_db(owid_env=object())  # type: ignore[arg-type]
    mock_upsert.assert_called_once()
