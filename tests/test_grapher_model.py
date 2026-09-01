from etl.grapher import model as gm


def test_source_description():
    """Make sure description as a TypedDict works correctly"""
    description: gm.SourceDescription = {"link": "ABC"}
    d = {"description": description}
    s = gm.Source(**d)  # ty: ignore
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


def test_extract_variable_ids_from_config():
    config = {
        "dimensions": [
            {"variableId": 988133, "property": "y"},
            {"variableId": "988134", "property": "x"},
        ],
        "sortColumnSlug": "988135",
        "map": {"columnSlug": "988136"},
        "minTime": 1950,
    }

    assert gm._extract_variable_ids_from_config(config) == {988133, 988134, 988135, 988136}


def test_chart_config_includes_chart_level_flags():
    class ChartConfigStub:
        config = {"slug": "test-chart"}

    class ChartStub:
        chart_config = ChartConfigStub()
        isInheritanceEnabled = 1
        forceDatapage = 1

    config = gm.Chart.config.fget(ChartStub())

    assert config["slug"] == "test-chart"
    assert config["isInheritanceEnabled"] is True
    assert config["forceDatapage"] is True


def test_chart_load_patch_config_reads_the_authored_layer():
    """`load_patch_config` must read `charts.patchConfigId`, not `configId`.

    The two are different rows: `configId`'s is the merged config that renders, `patchConfigId`'s
    is what someone authored in the chart editor. Reading the wrong one would make every chart
    look edited by hand.
    """
    from sqlalchemy import select

    from etl.grapher.model import Chart, ChartConfig

    class SessionStub:
        def __init__(self):
            self.statement = None

        def scalar(self, statement):
            self.statement = statement
            return {"slug": "whales-caught", "note": "Authored in the admin."}

    class ChartStub:
        id = 7118

    session = SessionStub()
    patch = Chart.load_patch_config(ChartStub(), session)  # ty: ignore[invalid-argument-type]

    assert patch == {"slug": "whales-caught", "note": "Authored in the admin."}
    compiled = str(session.statement)
    assert "patchConfigId" in compiled, compiled
    assert '"configId"' not in compiled, compiled
    expected = str(
        select(ChartConfig.config).join(Chart, ChartConfig.id == Chart.patchConfigId).where(Chart.id == 7118)
    )
    assert compiled == expected
