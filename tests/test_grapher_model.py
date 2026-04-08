from etl.grapher import model as gm


def test_source_description():
    """Make sure description as a TypedDict works correctly"""
    description: gm.SourceDescription = {"link": "ABC"}
    d = {"description": description}
    s = gm.Source(**d)  # type: ignore
    assert "link" in s.description
    assert s.description["link"] == "ABC"


def test_remap_variable_ids():
    config = {
        "dimensions": [
            {
                "variableId": 988133,
                "color": "#3182bd",
            }
        ],
        "sortColumnSlug": "988133",
        "columnSlug": "988133",
        "unknownStrColumnSlug": "988133",
        "unknownIntColumnSlug": 988133,
    }

    remap_ids = {988133: 123456}

    new_config = gm._remap_variable_ids(config, remap_ids)

    assert new_config["dimensions"][0]["variableId"] == 123456
    assert new_config["sortColumnSlug"] == "123456"
    assert new_config["columnSlug"] == "123456"
    assert new_config["unknownStrColumnSlug"] == "123456"
    assert new_config["unknownIntColumnSlug"] == 123456


def test_chart_config_includes_chart_level_flags():
    class ChartConfigStub:
        full = {"slug": "test-chart"}

    class ChartStub:
        chart_config = ChartConfigStub()
        isInheritanceEnabled = 1
        forceDatapage = 1

    config = gm.Chart.config.fget(ChartStub())

    assert config["slug"] == "test-chart"
    assert config["isInheritanceEnabled"] is True
    assert config["forceDatapage"] is True
