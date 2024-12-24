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
