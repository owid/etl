from etl.indicator_upgrade.indicator_update import update_chart_config
from etl.indicator_upgrade.schema import validate_chart_config_and_set_defaults


def test_validate_chart_config_ignores_chart_table_fields():
    config = {
        "title": "A chart",
        "isInheritanceEnabled": True,
        "forceDatapage": True,
    }
    schema = {"type": "object", "properties": {"title": {"type": "string"}}}

    config_new = validate_chart_config_and_set_defaults(config, schema=schema)

    assert config_new == {"title": "A chart"}


def test_map_column_slug_is_updated_when_stored_as_string():
    """Regression test: map.columnSlug is stored as a string in the config, unlike
    dimensions[*].variableId (an int). update_chart_config_map used to compare it
    directly against indicator_mapping's int keys, so the `in` check always failed and
    the map tab silently kept pointing at the old variable even though dimensions (and
    therefore every other view of the chart) were upgraded correctly.
    """
    schema = {
        "type": "object",
        "properties": {
            "$schema": {"type": "string"},
            "id": {"type": "integer"},
            "hasMapTab": {"type": "boolean", "default": False},
            "dimensions": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {"variableId": {"type": "integer"}, "property": {"type": "string"}},
                },
            },
            "map": {"type": "object", "properties": {"columnSlug": {"type": "string"}}},
        },
    }

    config = {
        "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.010.json",
        "id": 755,
        "hasMapTab": True,
        "map": {"columnSlug": "100"},  # stored as a string, as it is in real configs
        "dimensions": [{"variableId": 100, "property": "y"}],
    }

    config_new = update_chart_config(config, {100: 200}, schema)

    assert config_new["map"]["columnSlug"] == "200"
    assert config_new["dimensions"][0]["variableId"] == 200


def test_dimension_display_stripped_against_variable_baseline():
    """dimensions[*].display inherits from the variable's own `display` metadata -- a
    separate path from grapherConfigETL-based chart inheritance. A display field the
    chart never explicitly set (only present because validate_chart_config_and_set_defaults
    filled it in) should be stripped when it matches the variable's own display; a genuine
    override should survive even if it also happens to equal a schema default.
    """
    schema = {
        "type": "object",
        "properties": {
            "$schema": {"type": "string"},
            "id": {"type": "integer"},
            "hasMapTab": {"type": "boolean", "default": False},
            "dimensions": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "variableId": {"type": "integer"},
                        "property": {"type": "string"},
                        "display": {
                            "type": "object",
                            "properties": {
                                "tolerance": {"type": "integer", "default": 0},
                                "numDecimalPlaces": {"type": "integer", "default": 0},
                            },
                        },
                    },
                },
            },
        },
    }

    config = {
        "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.010.json",
        "id": 755,
        "dimensions": [
            {
                "variableId": 100,
                "property": "y",
                "display": {"tolerance": 3},  # genuine override: variable's own tolerance is 5
            }
        ],
    }

    config_new = update_chart_config(
        config,
        {100: 200},
        schema,
        dimension_display_baselines={200: {"tolerance": 5, "numDecimalPlaces": 1}},
    )

    display = config_new["dimensions"][0]["display"]
    # tolerance=3 was explicitly set and differs from the variable's own tolerance (5) -> kept.
    assert display["tolerance"] == 3
    # numDecimalPlaces was never set on the chart, only filled in as a schema default (0),
    # and doesn't match the variable's own numDecimalPlaces (1) either -- but since it was
    # never explicit, it must not appear at all.
    assert "numDecimalPlaces" not in display


def test_inheritance_pruning_uses_patch_not_full_config():
    """Regression test (Codex-flagged): `config` passed to update_chart_config is normally
    `chart.config`, which is built from `chart_configs.full` -- the fully *resolved* config,
    with every value the chart inherits from its old indicator already merged in. If
    explicit-field tracking were based on that (rather than the actual stored patch), an
    inherited title the chart never touched would look "explicit" and get compared against
    the new indicator's (possibly different) title -- pinning the stale value as a fake
    chart-level override instead of letting the chart keep inheriting.
    """
    schema = _inheritance_schema()
    schema["properties"]["title"] = {"type": "string"}

    # `full`: the resolved config as chart.config would present it -- title is present only
    # because it's inherited from the *old* indicator, never set by the chart itself.
    full_config = {
        "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.010.json",
        "id": 755,
        "isInheritanceEnabled": True,
        "title": "Old indicator title",
        "dimensions": [{"variableId": 100, "property": "y"}],
    }
    # The chart's *actual* stored patch never touched title at all.
    original_patch = {
        "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.010.json",
        "id": 755,
        "dimensions": [{"variableId": 100, "property": "y"}],
    }
    # The new indicator has since renamed its title.
    indicator_config = {
        "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.010.json",
        "title": "New indicator title",
    }

    config_new = update_chart_config(
        full_config, {100: 200}, schema, indicator_config=indicator_config, original_patch=original_patch
    )

    # title was never explicitly set by the chart -- it must not be pinned as an override,
    # so the chart keeps inheriting the indicator's (now-updated) title.
    assert "title" not in config_new


def test_inheritance_preserves_schema_default_overrides():
    """When inheritance is enabled, properties that match schema defaults but override
    indicator defaults should be preserved (not stripped).

    Regression test: a chart with inheritance enabled that sets hasMapTab=false
    (schema default) to override the indicator's hasMapTab=true was getting the
    map re-enabled after the indicator upgrader ran.
    """
    schema = {
        "type": "object",
        "properties": {
            "$schema": {"type": "string"},
            "id": {"type": "integer"},
            "version": {"type": "integer", "default": 1},
            "hasMapTab": {"type": "boolean", "default": False},
            "dimensions": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "variableId": {"type": "integer"},
                        "property": {"type": "string"},
                    },
                },
            },
            "map": {
                "type": "object",
                "properties": {
                    "columnSlug": {"type": "string"},
                },
            },
        },
    }

    # Chart has inheritance enabled; user disabled map (hasMapTab=false)
    # even though the indicator's ETL config has hasMapTab=true.
    config = {
        "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.010.json",
        "id": 7742,
        "version": 1,
        "isInheritanceEnabled": True,
        "hasMapTab": False,  # user's explicit override
        "dimensions": [{"variableId": 100, "property": "y"}],
    }

    indicator_mapping = {100: 200}

    config_new = update_chart_config(config, indicator_mapping, schema)

    # hasMapTab=false MUST be preserved so the admin API can compute
    # the correct patch against the indicator's hasMapTab=true.
    assert config_new.get("hasMapTab") is False, (
        "hasMapTab=false was stripped from an inheritance-enabled chart; "
        "this would cause the map to be re-enabled via inheritance"
    )
    # Variable ID should be updated
    assert config_new["dimensions"][0]["variableId"] == 200


def _inheritance_schema():
    return {
        "type": "object",
        "properties": {
            "$schema": {"type": "string"},
            "id": {"type": "integer"},
            "version": {"type": "integer", "default": 1},
            "hasMapTab": {"type": "boolean", "default": False},
            "hideLogo": {"type": "boolean", "default": False},
            "dimensions": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "variableId": {"type": "integer"},
                        "property": {"type": "string"},
                    },
                },
            },
            "map": {
                "type": "object",
                "properties": {
                    "colorScale": {
                        "type": "object",
                        "properties": {
                            "midpoint": {"type": "integer", "default": 0},
                        },
                    },
                },
            },
        },
    }


def test_inheritance_with_indicator_config_preserves_real_overrides():
    """With an indicator_config available, a genuine override that happens to equal the
    schema default (hasMapTab=false) but differs from the indicator's own value
    (hasMapTab=true) must still be preserved.
    """
    schema = _inheritance_schema()

    config = {
        "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.010.json",
        "id": 7742,
        "version": 1,
        "isInheritanceEnabled": True,
        "hasMapTab": False,  # user's explicit override
        "dimensions": [{"variableId": 100, "property": "y"}],
    }
    indicator_config = {
        "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.010.json",
        "hasMapTab": True,
    }

    config_new = update_chart_config(config, {100: 200}, schema, indicator_config=indicator_config)

    assert config_new.get("hasMapTab") is False
    assert config_new["dimensions"][0]["variableId"] == 200


def test_inheritance_with_indicator_config_strips_untouched_fields():
    """With an indicator_config available, fields neither the chart nor the indicator ever
    touched should be stripped -- not kept just because inheritance is enabled (the #5911
    fix's blanket "keep everything" fallback shouldn't apply once a real baseline exists).
    """
    schema = _inheritance_schema()

    config = {
        "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.010.json",
        "id": 7742,
        "version": 1,
        "isInheritanceEnabled": True,
        "dimensions": [{"variableId": 100, "property": "y"}],
        "map": {"colorScale": {}},
    }
    # Indicator never sets hideLogo or map.colorScale.midpoint either -- both should
    # resolve to their schema defaults on both sides and therefore get stripped.
    indicator_config = {
        "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.010.json",
        "hasMapTab": True,
    }

    config_new = update_chart_config(config, {100: 200}, schema, indicator_config=indicator_config)

    assert "hideLogo" not in config_new
    assert "midpoint" not in config_new.get("map", {}).get("colorScale", {})
    assert config_new["dimensions"][0]["variableId"] == 200
    # hasMapTab was never set on the chart itself -- only the indicator set it -- so it
    # must not appear at all; the chart should keep inheriting it via the admin API.
    assert "hasMapTab" not in config_new


def test_inheritance_with_indicator_config_strips_redundant_explicit_match():
    """A field the chart explicitly set, but whose value coincidentally matches what the
    indicator itself already provides (not just the generic schema default), carries no
    information and should still be stripped -- it resolves to the same thing either way.
    """
    schema = _inheritance_schema()

    config = {
        "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.010.json",
        "id": 7742,
        "version": 1,
        "isInheritanceEnabled": True,
        "hasMapTab": True,  # explicit, but happens to equal the indicator's own value
        "dimensions": [{"variableId": 100, "property": "y"}],
    }
    indicator_config = {
        "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.010.json",
        "hasMapTab": True,
    }

    config_new = update_chart_config(config, {100: 200}, schema, indicator_config=indicator_config)

    assert "hasMapTab" not in config_new
    assert config_new["dimensions"][0]["variableId"] == 200


def test_inheritance_with_empty_indicator_config_does_not_crash():
    """Regression test (caught by Codex review): an empty indicator_config (`{}`, meaning
    "the indicator has no grapherConfigETL at all -- inherits nothing") used to crash
    `compute_inheritance_patch` on any real grapher schema, which declares top-level
    `required` fields (`$schema`, `dimensions`) with no "default" to fall back on --
    validating a bare `{}` against it raises, and the exception handler crashes a second
    time trying to read `config["id"]` off that same empty dict, masking the real error.
    """
    schema = _inheritance_schema()
    schema["required"] = ["$schema", "dimensions"]

    config = {
        "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.010.json",
        "id": 7742,
        "version": 1,
        "isInheritanceEnabled": True,
        "hasMapTab": True,  # explicit override -- empty baseline means schema default (False)
        "dimensions": [{"variableId": 100, "property": "y"}],
    }

    config_new = update_chart_config(config, {100: 200}, schema, indicator_config={})

    # hasMapTab differs from the schema default (False) -- an empty baseline means "plain
    # schema defaults", so this is a genuine override and must be kept.
    assert config_new.get("hasMapTab") is True
    assert config_new["dimensions"][0]["variableId"] == 200
    assert config_new["$schema"] == config["$schema"]


def test_no_inheritance_strips_schema_defaults():
    """Without inheritance, schema-default values should still be stripped
    to keep configs lean (existing behavior)."""
    schema = {
        "type": "object",
        "properties": {
            "$schema": {"type": "string"},
            "id": {"type": "integer"},
            "version": {"type": "integer", "default": 1},
            "hasMapTab": {"type": "boolean", "default": False},
            "dimensions": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "variableId": {"type": "integer"},
                        "property": {"type": "string"},
                    },
                },
            },
        },
    }

    config = {
        "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.010.json",
        "id": 7742,
        "version": 1,
        "hasMapTab": False,
        "dimensions": [{"variableId": 100, "property": "y"}],
    }

    indicator_mapping = {100: 200}

    config_new = update_chart_config(config, indicator_mapping, schema)

    # Without inheritance, hasMapTab=false should be stripped (matches schema default)
    assert "hasMapTab" not in config_new
    assert config_new["dimensions"][0]["variableId"] == 200


def test_fetch_single_indicator_config_uses_y_dimension_and_empty_baseline(monkeypatch):
    """Regression test: `_fetch_single_indicator_config` used to give up (return None,
    triggering the conservative "keep everything" fallback) for any chart with more than
    one dimension -- e.g. a `color` dimension besides `y` -- even though title/subtitle/map
    inheritance is really driven by the `y` indicator alone. It also returned None when the
    indicator simply has no `grapherConfigETL` row, which is indistinguishable downstream
    from "we don't know the baseline at all" -- but "no ETL config" really means "inherits
    nothing", i.e. the baseline *is* the plain schema defaults, which should return `{}` so
    `compute_inheritance_patch` can safely strip schema-default bloat.

    Found via a real chart (gdp-per-capita-worldbank, id 225) that has a `color` dimension
    and was getting its entire resolved config (map/yAxis/etc. schema defaults included)
    pinned as if every field were a deliberate override.
    """
    import pandas as pd

    from apps.indicator_upgrade import upgrade as upgrade_module

    calls = []

    def fake_read_sql(sql, engine, params: dict):
        calls.append(params)
        if params["vid"] == 900801:
            raise AssertionError("should query the y-dimension's variable, not the color one")
        if params["vid"] == 1292289:
            return pd.DataFrame([{"full": '{"title": "GDP per capita"}'}])
        return pd.DataFrame()  # no grapherConfigETL for this variable

    monkeypatch.setattr(upgrade_module, "read_sql", fake_read_sql)
    monkeypatch.setattr(upgrade_module, "get_engine", lambda: None)

    chart_config = {
        "dimensions": [
            {"property": "y", "variableId": 1204826},
            {"property": "color", "variableId": 900801},
        ]
    }
    result = upgrade_module._fetch_single_indicator_config(chart_config, {1204826: 1292289})
    assert result == {"title": "GDP per capita"}
    assert len(calls) == 1

    # Single dimension, but its variable has no grapherConfigETL at all.
    chart_config_no_config = {"dimensions": [{"property": "y", "variableId": 1204331}]}
    result_no_config = upgrade_module._fetch_single_indicator_config(chart_config_no_config, {1204331: 1291785})
    assert result_no_config == {}
