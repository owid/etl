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
