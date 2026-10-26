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
    # A YAML list (with anchors flattened) is kept as a list until Jinja
    # rendering; it's converted to a markdown string in `update_variable_metadata`.
    assert t.a.metadata.description_key == ["Text 1", "Text 2", "Text 2", "Text 3"]


def test_update_metadata_from_yaml_description_key_item_parsed_as_mapping(tmp_path):
    """An unquoted item containing ': ' parses as a mapping and must fail, not be cut at the colon."""
    yaml_text = """
tables:
  test:
    variables:
      a:
        description_key:
          - Text 1
          - They are not a random sample: results may not be representative.
""".strip()

    path = tmp_path / "test.yaml"
    with open(path, "w") as f:
        f.write(yaml_text)

    t = Table({"a": [1, 2, 3]})
    with pytest.raises(ValueError, match="not strings"):
        ym.update_metadata_from_yaml(t, path, "test")


def test_update_metadata_from_yaml_description_key_string(tmp_path):
    """A bare (multiline) string is used as-is and rendered as free-form markdown."""
    yaml_text = """
tables:
  test:
    variables:
      a:
        description_key: |-
          First paragraph.

          Second paragraph with a markdown list:
          - item 1
          - item 2
""".strip()

    path = tmp_path / "test.yaml"
    with open(path, "w") as f:
        f.write(yaml_text)

    t = Table({"a": [1, 2, 3]})
    ym.update_metadata_from_yaml(t, path, "test")
    assert t.a.metadata.description_key == (
        "First paragraph.\n\nSecond paragraph with a markdown list:\n- item 1\n- item 2"
    )


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
      stunting_prev_model_estimatse:
        title: Typo'd key — neither a column nor a long-format prefix of one
""".strip()

    path = tmp_path / "test.yaml"
    with open(path, "w") as f:
        f.write(yaml_text)

    t = Table({"stunting_prev_model_estimates__sex_both_sexes": [1, 2, 3]})

    with pytest.raises(ValueError, match="none match any column"):
        ym.update_metadata_from_yaml(t, path, "test", extra_variables="ignore")


def test_update_metadata_from_yaml_extra_variables_ignore_long_format_prefix(tmp_path):
    """A YAML key that is the long-format base name of a pivoted column must NOT
    raise: such metadata was already applied to the long table before
    long_to_wide, so missing the wide columns in the second pass is expected.
    """
    yaml_text = """
tables:
  test:
    variables:
      n_animals_killed:
        title: Long-format key, applied pre-pivot
""".strip()

    path = tmp_path / "test.yaml"
    with open(path, "w") as f:
        f.write(yaml_text)

    # Table has only pivoted variable names with `__{dim}_{value}` suffixes.
    t = Table(
        {
            "n_animals_killed__animal_cattle": [1, 2, 3],
            "n_animals_killed__animal_chickens": [4, 5, 6],
        }
    )

    # Should not raise.
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


def _write_annotations_yaml(tmp_path, annotations_yaml: str):
    path = tmp_path / "test.yaml"
    path.write_text(f"tables:\n  test:\n    variables:\n      a:\n        annotations:\n{annotations_yaml}")
    return path


def test_update_metadata_from_yaml_annotations(tmp_path):
    path = _write_annotations_yaml(
        tmp_path,
        """          - entities: [United States]
            time: 2003
            time_end: 2017
            text: Values are affected by a change in measurement.
          - text: Whole indicator.
""",
    )
    t = Table({"a": [1, 2, 3]})
    ym.update_metadata_from_yaml(t, path, "test")
    first, second = t.a.m.annotations
    assert (first.entities, first.time, first.time_end) == (["United States"], 2003, 2017)
    assert (second.text, second.entities, second.time) == ("Whole indicator.", [], None)


def test_update_metadata_from_yaml_annotations_year_from_params(tmp_path):
    path = _write_annotations_yaml(
        tmp_path, '          - {time: 2020, time_end: "{LATEST_YEAR}", text: Preliminary.}\n'
    )
    t = Table({"a": [1, 2, 3]})
    ym.update_metadata_from_yaml(t, path, "test", yaml_params={"LATEST_YEAR": 2024})
    assert t.a.m.annotations[0].time_end == 2024


def test_update_metadata_from_yaml_annotations_entities_must_be_a_list(tmp_path):
    path = _write_annotations_yaml(tmp_path, "          - {entities: United States, text: Note.}\n")
    with pytest.raises(TypeError, match="must list its entities"):
        ym.update_metadata_from_yaml(Table({"a": [1, 2, 3]}), path, "test")


def test_update_metadata_from_yaml_annotations_dates(tmp_path):
    path = _write_annotations_yaml(tmp_path, "          - {time: 2020-03-01, time_end: 2020-06-30, text: Note.}\n")
    t = Table({"a": [1, 2, 3]})
    ym.update_metadata_from_yaml(t, path, "test")
    assert (t.a.m.annotations[0].time, t.a.m.annotations[0].time_end) == ("2020-03-01", "2020-06-30")


def test_update_metadata_from_yaml_annotations_time_must_be_a_year_or_date(tmp_path):
    path = _write_annotations_yaml(tmp_path, "          - {time: March 2020, text: Note.}\n")
    with pytest.raises(ValueError, match="must be a year or a YYYY-MM-DD date"):
        ym.update_metadata_from_yaml(Table({"a": [1, 2, 3]}), path, "test")
