"""Tests for `etl chart-config-id` (apps.chart_config_id.cli).

The DB lookup itself isn't covered here (it needs a grapher DB); what's tested is the part that
touches the user's files: which configs are accepted, and how `chart_config_id` is written.
"""

from pathlib import Path

import pytest
from click.exceptions import ClickException
from click.testing import CliRunner

from apps.chart_config_id.cli import _load_config, _write_field, lookup

UUID_A = "0191b6c7-5595-70b2-8d30-fa03fccd7add"
UUID_B = "019fa94c-42ae-75ee-89d9-1c1d0d754792"

SINGLE_CHART_YAML = """\
# A comment that must survive the rewrite.
topic_tags:
  - "Animal Welfare"
dimensions: []
views:
  - dimensions: {}
    indicators:
      y:
        - "chick_culling_laws#status"
    config:
      title: "Which countries have banned chick culling?"
"""


def _write(tmp_path: Path, content: str, name: str = "my_chart.config.yml") -> Path:
    path = tmp_path / name
    path.write_text(content)
    return path


def test_writes_field_first_and_preserves_comments(tmp_path):
    path = _write(tmp_path, SINGLE_CHART_YAML)
    _write_field(path, _load_config(path), UUID_A, force=False)

    text = path.read_text()
    # The leading document comment stays on top; the field becomes the first key.
    assert text.splitlines()[:2] == [
        "# A comment that must survive the rewrite.",
        f"chart_config_id: {UUID_A}",
    ]
    # The rest of the config is untouched.
    config = _load_config(path)
    assert list(config)[0] == "chart_config_id"
    assert config["topic_tags"] == ["Animal Welfare"]
    assert config["views"][0]["config"]["title"] == "Which countries have banned chick culling?"


def test_refuses_to_replace_existing_id(tmp_path):
    path = _write(tmp_path, f"chart_config_id: {UUID_A}\n{SINGLE_CHART_YAML}")
    with pytest.raises(ClickException, match="already declares"):
        _write_field(path, _load_config(path), UUID_B, force=False)
    # File left untouched.
    assert _load_config(path)["chart_config_id"] == UUID_A


def test_force_replaces_existing_id(tmp_path):
    path = _write(tmp_path, f"chart_config_id: {UUID_A}\n{SINGLE_CHART_YAML}")
    _write_field(path, _load_config(path), UUID_B, force=True)
    assert _load_config(path)["chart_config_id"] == UUID_B


def test_same_id_in_different_case_is_rewritten_canonical_without_force(tmp_path):
    # An upper-case UUID names the same chart, so this is not an identity change —
    # the file is healed to the canonical lower-case form validation requires.
    path = _write(tmp_path, f"chart_config_id: {UUID_A.upper()}\n{SINGLE_CHART_YAML}")
    _write_field(path, _load_config(path), UUID_A, force=False)
    assert _load_config(path)["chart_config_id"] == UUID_A


def test_rewriting_the_same_id_is_a_noop(tmp_path):
    path = _write(tmp_path, f"chart_config_id: {UUID_A}\n{SINGLE_CHART_YAML}")
    before = path.read_text()
    _write_field(path, _load_config(path), UUID_A, force=False)
    assert path.read_text() == before


def test_rejects_mdim_with_dimensions(tmp_path):
    path = _write(
        tmp_path,
        "dimensions:\n  - slug: sex\n    name: Sex\n    choices:\n      - slug: female\n        name: Female\n"
        "views:\n  - dimensions: {sex: female}\n    indicators: {y: 'table#ind'}\n",
    )
    with pytest.raises(ClickException, match="not a single-chart config"):
        _load_config(path)


def test_rejects_programmatic_mdim_with_no_views(tmp_path):
    # `dimensions: []` alone isn't enough — mdims that fill dimensions/views in code look like this.
    path = _write(tmp_path, "title:\n  title: Air Pollution\ndimensions: []\nviews: []\n")
    with pytest.raises(ClickException, match="not a single-chart config"):
        _load_config(path)


@pytest.mark.parametrize(
    "args",
    [
        [],  # neither
        ["--slug", "banning-of-chick-culling", "--chart-id", "7118"],  # both
    ],
)
def test_lookup_requires_exactly_one_identifier(tmp_path, args):
    """The chart must be named explicitly, and unambiguously — no DB is touched otherwise."""
    path = _write(tmp_path, SINGLE_CHART_YAML)
    result = CliRunner().invoke(lookup, [str(path), *args])
    assert result.exit_code != 0
    assert "exactly one of --slug or --chart-id" in result.output


def test_rejects_non_mapping_yaml(tmp_path):
    path = _write(tmp_path, "- just\n- a\n- list\n")
    with pytest.raises(ClickException, match="does not contain a YAML mapping"):
        _load_config(path)
