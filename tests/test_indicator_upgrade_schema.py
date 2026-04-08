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
