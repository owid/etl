from owid.catalog import yaml_metadata as ym
from owid.catalog.meta import Source
from owid.catalog.tables import Table


def test_update_metadata_from_yaml_description_key(tmp_path):
    yaml_text = """
description_key_common: &description_key_common
- &text_1 Text 1
- &text_2 Text 2

tables:
  test:
    variables:
      a:
        description_key:
          - *description_key_common
          - *text_2
          - Text 3
""".strip()

    path = tmp_path / "test.yaml"
    with open(path, "w") as f:
        f.write(yaml_text)

    t = Table({"a": [1, 2, 3]})
    ym.update_metadata_from_yaml(t, path, "test")
    assert t.a.metadata.description_key == ["Text 1", "Text 2", "Text 2", "Text 3"]


def test_update_metadata_from_yaml_common(tmp_path):
    # delete sources and add origins for all variables
    yaml_text = """
definitions:
  common:
    sources: []
    origins:
      - producer: Origin1
        title: Title1
    description_processing:
      - A
      - B
    description_key:
      - D
    description_short: Default desc short
    display:
      numDecimalPlaces: 0
      conversionFactor: 2
    presentation:
      grapher_config:
        selectedEntityNames:
          - France

tables:
  test:
    variables:
      a:
        origins:
          - producer: Origin2
            title: Title2
        description_key:
          - C
        description_short: A desc short
        display:
          numDecimalPlaces: 1
        presentation:
          attribution: A presentation attribution
          grapher_config:
            subtitle: A subtitle
""".strip()

    path = tmp_path / "test.yaml"
    with open(path, "w") as f:
        f.write(yaml_text)

    t = Table({"a": [1, 2, 3], "b": [1, 2, 3]})
    t.a.metadata.sources = [Source()]

    ym.update_metadata_from_yaml(t, path, "test")

    assert t.a.m.sources == []
    assert t.b.m.sources == []
    assert len(t.a.m.origins) == 2
    assert len(t.b.m.origins) == 1
    assert t.a.m.description_key == ["C", "D"]
    assert t.b.m.description_key == ["D"]
    assert t.a.m.description_processing == ["A", "B"]
    assert t.b.m.description_processing == ["A", "B"]
    assert t.a.m.description_short == "A desc short"
    assert t.b.m.description_short == "Default desc short"
    assert t.a.m.display["numDecimalPlaces"] == 1
    assert t.b.m.display["numDecimalPlaces"] == 0

    # display is completely overwritten, not merged
    assert "conversionFactor" not in t.a.m.display
    assert t.b.m.display["conversionFactor"] == 2

    # merging presentation
    assert t.a.m.presentation.attribution == "A presentation attribution"
    assert t.b.m.presentation.attribution is None

    # merging grapher_config
    assert t.a.m.presentation.grapher_config["selectedEntityNames"] == ["France"]
    assert t.b.m.presentation.grapher_config["selectedEntityNames"] == ["France"]
    assert t.a.m.presentation.grapher_config["subtitle"] == "A subtitle"
    assert "subtitle" not in t.b.m.presentation.grapher_config
