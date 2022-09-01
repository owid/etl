from etl import grapher_model as gm


def test_source_description():
    """Make sure description as a TypedDict works correctly"""
    description: gm.SourceDescription = {"link": "ABC"}
    d = {"description": description}
    gm.Source.validate(d)
    s = gm.Source(**d)
    assert "link" in s.description
    assert s.description["link"] == "ABC"
