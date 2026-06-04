import pytest

from owid.catalog.core import yaml_metadata as ym
from owid.catalog.core.meta import Origin
from owid.catalog.core.tables import Table


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
    yaml_text = """
definitions:
  common:
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


def test_update_metadata_from_yaml_extra_variables_ignore_all_miss(tmp_path):
    """When extra_variables='ignore' (e.g. after long_to_wide pivots), a YAML
    section whose variable keys ALL miss the table should still raise — that's
    almost certainly a key typo, not an intentional partial override.
    """
    yaml_text = """
tables:
  test:
    variables:
      stunting_prev_model_estimates:
        title: Wrong key — actual column is `…__sex_both_sexes`
""".strip()

    path = tmp_path / "test.yaml"
    with open(path, "w") as f:
        f.write(yaml_text)

    # Table has the pivoted variable name, not the bare one in the YAML.
    t = Table({"stunting_prev_model_estimates__sex_both_sexes": [1, 2, 3]})

    with pytest.raises(ValueError, match="none match any column"):
        ym.update_metadata_from_yaml(t, path, "test", extra_variables="ignore")


def test_update_metadata_from_yaml_extra_variables_ignore_partial_miss(tmp_path):
    """A partial miss (some YAML variables match, some don't) is still tolerated
    when extra_variables='ignore' — the user is selectively documenting.
    """
    yaml_text = """
tables:
  test:
    variables:
      a:
        title: A
      not_a_real_column:
        title: This one is missing — but `a` matches, so don't raise
""".strip()

    path = tmp_path / "test.yaml"
    with open(path, "w") as f:
        f.write(yaml_text)

    t = Table({"a": [1, 2, 3], "b": [4, 5, 6]})
    # Should not raise; `a` matches, the unknown `not_a_real_column` is silently ignored.
    ym.update_metadata_from_yaml(t, path, "test", extra_variables="ignore")
    assert t.a.metadata.title == "A"
