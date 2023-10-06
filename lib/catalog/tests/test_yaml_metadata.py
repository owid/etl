from owid.catalog import yaml_metadata as ym
from owid.catalog.meta import Origin, Source
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
    description_processing: Processed
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
    t.a.metadata.description_short = "Will be overwritten"
    t.a.metadata.sources = [Source()]
    t.b.metadata.origins = [Origin(producer="Producer", title="Title")]

    ym.update_metadata_from_yaml(t, path, "test")

    assert t.a.m.to_dict() == {
        "description_short": "A desc short",
        "description_key": ["C"],
        "origins": [{"producer": "Origin2", "title": "Title2"}],
        "display": {"numDecimalPlaces": 1},
        "presentation": {
            "grapher_config": {"selectedEntityNames": ["France"], "subtitle": "A subtitle"},
            "attribution": "A presentation attribution",
        },
        "description_processing": "Processed",
    }

    assert t.b.m.to_dict() == {
        "description_short": "Default desc short",
        "description_key": ["D"],
        "origins": [{"producer": "Origin1", "title": "Title1"}],
        "display": {"conversionFactor": 2, "numDecimalPlaces": 0},
        "presentation": {"grapher_config": {"selectedEntityNames": ["France"]}},
        "description_processing": "Processed",
    }
