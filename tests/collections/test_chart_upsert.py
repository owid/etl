"""Tests for `etl.collection.chart_upsert._build_chart_config`.

`_build_chart_config` translates a zero-dimension collection's single `View`
(its `config` + `indicators`) into the grapher chart-config dict that gets
written to `chart_configs.etlConfig`. The only external dependency is
`map_indicator_path_to_id` (a DB lookup), which is mocked here so the tests
stay pure.
"""

from unittest.mock import patch

import pytest

from etl.collection.chart_upsert import _build_chart_config, _validate_chart_config
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


def test_validate_accepts_well_formed_config():
    config = {
        "$schema": DEFAULT_GRAPHER_SCHEMA,
        "slug": "my-chart",
        "title": "A chart",
        "chartTypes": ["LineChart"],
        "dimensions": [{"property": "y", "variableId": 111}],
    }
    # Should not raise.
    _validate_chart_config(config, "my-chart")


def test_validate_rejects_unknown_field():
    config = {
        "$schema": DEFAULT_GRAPHER_SCHEMA,
        "slug": "my-chart",
        "dimensions": [{"property": "y", "variableId": 111}],
        "titel": "typo'd field",  # not in the grapher schema (root additionalProperties: false)
    }
    with pytest.raises(ValueError, match="Invalid chart config for slug 'my-chart'"):
        _validate_chart_config(config, "my-chart")


def test_validate_skips_when_schema_not_vendored_locally():
    config = {
        # A version we don't vendor locally — validation is skipped, not an error.
        "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.001.json",
        "dimensions": [{"property": "y", "variableId": 111}],
        "titel": "typo would fail if validated",
    }
    # Should not raise (skipped because the local schema file is absent).
    _validate_chart_config(config, "my-chart")
