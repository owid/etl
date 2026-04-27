import tempfile
from pathlib import Path

import pytest

from etl.dag_helpers import (
    _parse_dag_yaml,
    build_consumer_graph,
    compact_dag_file,
    flatten_dag_file,
    get_comments_above_step_in_dag,
    load_dag,
    load_single_dag_file,
    remove_steps_from_dag_file,
    write_to_dag_file,
)


def test_get_comments_above_step_in_dag():
    yaml_content = """\
steps:
  # Comment for meadow_a.
  meadow_a:
    # Comment for snapshot_a.
    - snapshot_a
  # Comment for meadow_b.
  # And another comment.
  meadow_b:
    # Comment for snapshot_b.
    - snapshot_b

  meadow_c:
    - snapshot_a
    - snapshot_b
  #
  meadow_d:

  # Comment for meadow_e.

  meadow_e:
    - snapshot_e

include:
  - path/to/another/dag.yml
"""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_file = Path(temp_dir) / "temp.yml"
        # Create a dag file inside a temporary folder.
        temp_file.write_text(yaml_content)
        assert get_comments_above_step_in_dag(step="meadow_a", dag_file=temp_file) == "# Comment for meadow_a.\n"
        assert (
            get_comments_above_step_in_dag(step="meadow_b", dag_file=temp_file)
            == "# Comment for meadow_b.\n# And another comment.\n"
        )
        assert get_comments_above_step_in_dag(step="meadow_c", dag_file=temp_file) == ""
        assert get_comments_above_step_in_dag(step="meadow_d", dag_file=temp_file) == "#\n"
        assert get_comments_above_step_in_dag(step="meadow_e", dag_file=temp_file) == "# Comment for meadow_e.\n"
        assert get_comments_above_step_in_dag(step="non_existing_step", dag_file=temp_file) == ""


def _assert_remove_steps_from_dag_file(old_content, expected_content, steps_to_remove):
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_file = Path(temp_dir) / "temp.yml"
        # Create a dag file inside a temporary folder.
        temp_file.write_text(old_content)
        # Update dag file with the given dag part.
        remove_steps_from_dag_file(
            dag_file=temp_file,
            steps_to_remove=steps_to_remove,
        )
        # Assert that the file content is the same as before.
        with open(temp_file) as updated_file:
            new_content = updated_file.read()
    assert new_content == expected_content


def test_remove_steps_from_dag_file_empty_list_of_steps():
    old_content = """\
steps:
    meadow_a:
    - snapshot_a
    meadow_b:
    - snapshot_b
"""
    expected_content = old_content
    _assert_remove_steps_from_dag_file(old_content, expected_content, steps_to_remove=[])


def test_remove_steps_from_dag_file_various_examples():
    old_content = """\
steps:
  meadow_a:
    - snapshot_a
  meadow_b:
    - snapshot_b
"""
    expected_content = """\
steps:
  meadow_b:
    - snapshot_b
"""
    _assert_remove_steps_from_dag_file(old_content, expected_content, steps_to_remove=["meadow_a"])

    old_content = """\
steps:
  # Comment for meadow_a.
  meadow_a:
    - snapshot_a
  meadow_b:
    - snapshot_b
"""
    expected_content = """\
steps:
  meadow_b:
    - snapshot_b
"""
    _assert_remove_steps_from_dag_file(old_content, expected_content, steps_to_remove=["meadow_a"])

    old_content = """\
steps:
  # Comment for meadow_a.
  meadow_a:
    - snapshot_a
  # Comment for meadow_b.
  meadow_b:
    - snapshot_b
"""
    expected_content = """\
steps:
  # Comment for meadow_b.
  meadow_b:
    - snapshot_b
"""
    _assert_remove_steps_from_dag_file(old_content, expected_content, steps_to_remove=["meadow_a"])

    old_content = """\
steps:
  # Comment for meadow_a.

  meadow_a:
    - snapshot_a
  # Comment for meadow_b.
  meadow_b:
    - snapshot_b
"""
    expected_content = """\
steps:
  # Comment for meadow_b.
  meadow_b:
    - snapshot_b
"""
    _assert_remove_steps_from_dag_file(old_content, expected_content, steps_to_remove=["meadow_a"])

    old_content = """\
steps:
  # Comment for meadow_a.

  meadow_a:
    - snapshot_a

  # Comment for meadow_b.
  meadow_b:
    - snapshot_b
"""
    expected_content = """\
steps:
  # Comment for meadow_b.
  meadow_b:
    - snapshot_b
"""
    # NOTE: This is not necessarily desired behavior, but it is the one to be expected.
    _assert_remove_steps_from_dag_file(old_content, expected_content, steps_to_remove=["meadow_a"])

    old_content = """\
steps:

  # Comment for meadow_a.

  meadow_a:
    - snapshot_a
  # Comment for meadow_b.
  meadow_b:
    - snapshot_b
"""
    expected_content = """\
steps:
  # Comment for meadow_b.
  meadow_b:
    - snapshot_b
"""
    _assert_remove_steps_from_dag_file(old_content, expected_content, steps_to_remove=["meadow_a"])

    old_content = """\
steps:

  # Comment for meadow_a.

  meadow_a:
    - snapshot_a
  # Comment for meadow_b.
  meadow_b:
    - snapshot_b
  # Comment for meadow_c.
  meadow_c:
    - snapshot_a
    - snapshot_b
    - snapshot_c

include:
  - some_file.yml
"""
    expected_content = """\
steps:

  # Comment for meadow_a.

  meadow_a:
    - snapshot_a
  # Comment for meadow_c.
  meadow_c:
    - snapshot_a
    - snapshot_b
    - snapshot_c

include:
  - some_file.yml
"""
    _assert_remove_steps_from_dag_file(old_content, expected_content, steps_to_remove=["meadow_b"])

    old_content = """\
steps:

  # Comment for meadow_a.

  meadow_a:
    - snapshot_a
  # Comment for meadow_b.
  meadow_b:
    # Comment for snapshot_b.
    - snapshot_b
  # Comment for meadow_c.
  meadow_c:
    - snapshot_a
    - snapshot_b
    - snapshot_c

include:
  - some_file.yml
"""
    expected_content = """\
steps:

  # Comment for meadow_a.

  meadow_a:
    - snapshot_a
  # Comment for meadow_c.
  meadow_c:
    - snapshot_a
    - snapshot_b
    - snapshot_c

include:
  - some_file.yml
"""
    _assert_remove_steps_from_dag_file(old_content, expected_content, steps_to_remove=["meadow_b"])

    old_content = """\
steps:

  # Comment for meadow_a.

  meadow_a:
    - snapshot_a
  # Comment for meadow_b.
  meadow_b:

    # Comment for snapshot_b.

    - snapshot_b
    # Comment for snapshot_c.
    - snapshot_c

  # Comment for meadow_c.
  meadow_c:
    - snapshot_a
    # Comment for snapshot_b.
    - snapshot_b
    - snapshot_c

include:
  - some_file.yml
"""
    expected_content = """\
steps:

  # Comment for meadow_a.

  meadow_a:
    - snapshot_a
  # Comment for meadow_c.
  meadow_c:
    - snapshot_a
    # Comment for snapshot_b.
    - snapshot_b
    - snapshot_c

include:
  - some_file.yml
"""
    _assert_remove_steps_from_dag_file(old_content, expected_content, steps_to_remove=["meadow_b"])

    # Test unusual (and undesired) case where the comment of a dependency is not properly indented.
    # NOTE: In this case, the function will assign the dependencies of the removed step to the previous step.
    # This test exists to document the behavior. It is not desired (but it's an edge case that should not happen).
    old_content = """\
steps:
  # Comment for meadow_a.
  meadow_a:
    - snapshot_a
  # Comment for meadow_b.
  meadow_b:
  # Comment for snapshot_b.
    - snapshot_b
    # Comment for snapshot_c.
    - snapshot_c
  # Comment for meadow_c.
  meadow_c:
    - snapshot_a
    # Comment for snapshot_b.
    - snapshot_b
    - snapshot_c

include:
  - some_file.yml
"""
    expected_content = """\
steps:
  # Comment for meadow_a.
  meadow_a:
    - snapshot_a
  # Comment for snapshot_b.
    - snapshot_b
    # Comment for snapshot_c.
    - snapshot_c
  # Comment for meadow_c.
  meadow_c:
    - snapshot_a
    # Comment for snapshot_b.
    - snapshot_b
    - snapshot_c

include:
  - some_file.yml
"""
    _assert_remove_steps_from_dag_file(old_content, expected_content, steps_to_remove=["meadow_b"])

    old_content = """\
steps:

  # Comment for meadow_a.

  meadow_a:
    - snapshot_a
  # Comment for meadow_b.
  meadow_b:

    # Comment for snapshot_b.

    - snapshot_b
    # Comment for snapshot_c.
    - snapshot_c

  # Comment for meadow_c.
  meadow_c:
    - snapshot_a
    # Comment for snapshot_b.
    - snapshot_b
    - snapshot_c

include:
  - some_file.yml
"""
    expected_content = """\
steps:
  # Comment for meadow_c.
  meadow_c:
    - snapshot_a
    # Comment for snapshot_b.
    - snapshot_b
    - snapshot_c

include:
  - some_file.yml
"""
    _assert_remove_steps_from_dag_file(old_content, expected_content, steps_to_remove=["meadow_a", "meadow_b"])

    # Case where all steps are removed.
    old_content = """\
steps:

  # Comment for meadow_a.

  meadow_a:
    - snapshot_a
  # Comment for meadow_b.
  meadow_b:

    # Comment for snapshot_b.

    - snapshot_b
    # Comment for snapshot_c.
    - snapshot_c

  # Comment for meadow_c.
  meadow_c:
    - snapshot_a
    # Comment for snapshot_b.
    - snapshot_b
    - snapshot_c

include:
  - some_file.yml
"""
    expected_content = """\
steps:

include:
  - some_file.yml
"""
    _assert_remove_steps_from_dag_file(
        old_content, expected_content, steps_to_remove=["meadow_c", "meadow_a", "meadow_b"]
    )

    # Case where step has no dependencies.
    old_content = """\
steps:
  # Comment for meadow_a.
  meadow_a:
  # Comment for meadow_b.
  meadow_b:
    # Comment for snapshot_b.
    - snapshot_b
    # Comment for snapshot_c.
    - snapshot_c
  # Comment for meadow_c.
  meadow_c:
    - snapshot_a
    # Comment for snapshot_b.
    - snapshot_b
    - snapshot_c

include:
  - some_file.yml
"""
    expected_content = """\
steps:
  # Comment for meadow_b.
  meadow_b:
    # Comment for snapshot_b.
    - snapshot_b
    # Comment for snapshot_c.
    - snapshot_c
  # Comment for meadow_c.
  meadow_c:
    - snapshot_a
    # Comment for snapshot_b.
    - snapshot_b
    - snapshot_c

include:
  - some_file.yml
"""
    _assert_remove_steps_from_dag_file(old_content, expected_content, steps_to_remove=["meadow_a"])

    # Another case where step has no dependencies.
    old_content = """\
steps:
  # Comment for meadow_a.
  meadow_a:
  # Comment for meadow_b.
  meadow_b:
    # Comment for snapshot_b.
    - snapshot_b
    # Comment for snapshot_c.
    - snapshot_c
  # Comment for meadow_c.
  meadow_c:
    - snapshot_a
    # Comment for snapshot_b.
    - snapshot_b
    - snapshot_c

include:
  - some_file.yml
"""
    expected_content = """\
steps:
  # Comment for meadow_a.
  meadow_a:
  # Comment for meadow_c.
  meadow_c:
    - snapshot_a
    # Comment for snapshot_b.
    - snapshot_b
    - snapshot_c

include:
  - some_file.yml
"""
    _assert_remove_steps_from_dag_file(old_content, expected_content, steps_to_remove=["meadow_b"])

    # Ensure comments at the beginning of the file are kept.
    old_content = """\
# Random comments before steps section starts.
# More random comments.
steps:
  # Comment for meadow_a.
  meadow_a:
    - snapshot_a
  # Comment for meadow_b.
  meadow_b:
    # Comment for snapshot_b.
    - snapshot_b
    # Comment for snapshot_c.
    - snapshot_c

include:
  - some_file.yml
"""
    expected_content = """\
# Random comments before steps section starts.
# More random comments.
steps:
  # Comment for meadow_b.
  meadow_b:
    # Comment for snapshot_b.
    - snapshot_b
    # Comment for snapshot_c.
    - snapshot_c

include:
  - some_file.yml
"""
    _assert_remove_steps_from_dag_file(old_content, expected_content, steps_to_remove=["meadow_a"])

    # Ensure there is a space before the include section.
    old_content = """\
steps:
  # Comment for meadow_a.
  meadow_a:
    - snapshot_a
  # Comment for meadow_b.
  meadow_b:
    # Comment for snapshot_b.
    - snapshot_b
    # Comment for snapshot_c.
    - snapshot_c

include:
  - some_file.yml
"""
    expected_content = """\
steps:
  # Comment for meadow_a.
  meadow_a:
    - snapshot_a

include:
  - some_file.yml
"""
    _assert_remove_steps_from_dag_file(old_content, expected_content, steps_to_remove=["meadow_b"])


def _assert_write_to_dag_file(
    old_content, expected_content, dag_part, comments=None, indent_step=2, indent_dependency=4
):
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_file = Path(temp_dir) / "temp.yml"
        # Create a dag file inside a temporary folder.
        temp_file.write_text(old_content)
        # Update dag file with the given dag part.
        write_to_dag_file(
            dag_file=temp_file,
            dag_part=dag_part,
            comments=comments,
            indent_step=indent_step,
            indent_dependency=indent_dependency,
        )
        # Assert that the file content is the same as before.
        with open(temp_file) as updated_file:
            new_content = updated_file.read()
    assert new_content == expected_content


def test_write_to_dag_file_empty_dag_part():
    old_content = """\
steps:
    meadow_a:
    - snapshot_a
    meadow_b:
    - snapshot_b
"""
    expected_content = old_content
    _assert_write_to_dag_file(old_content, expected_content, dag_part={})


def test_write_to_dag_file_add_new_step():
    old_content = """\
steps:
  meadow_a:
    - snapshot_a
  meadow_b:
    - snapshot_b
"""
    expected_content = """\
steps:
  meadow_a:
    - snapshot_a
  meadow_b:
    - snapshot_b
  meadow_c:
    - snapshot_a
    - snapshot_b
"""
    _assert_write_to_dag_file(old_content, expected_content, dag_part={"meadow_c": ["snapshot_a", "snapshot_b"]})


def test_write_to_dag_file_update_existing_step():
    old_content = """\
steps:
  meadow_a:
    - snapshot_a
  meadow_b:
    - snapshot_b
"""
    expected_content = """\
steps:
  meadow_a:
    - snapshot_a
  meadow_b:
    - snapshot_b
    - snapshot_c
"""
    _assert_write_to_dag_file(old_content, expected_content, dag_part={"meadow_b": ["snapshot_b", "snapshot_c"]})


def test_write_to_dag_file_change_indent():
    old_content = """\
steps:
  meadow_a:
    - snapshot_a
  meadow_b:
    - snapshot_b
  meadow_c:
    - snapshot_a
    - snapshot_b
"""
    expected_content = """\
steps:
  meadow_a:
    - snapshot_a
  meadow_b:
    - snapshot_b
  meadow_c:
    - snapshot_a
    - snapshot_b
   meadow_d:
     - snapshot_d
"""
    _assert_write_to_dag_file(
        old_content, expected_content, dag_part={"meadow_d": ["snapshot_d"]}, indent_step=3, indent_dependency=5
    )


def test_write_to_dag_file_respect_comments():
    old_content = """\
steps:
  # Comment for meadow_a.
  meadow_a:
    # Comment for snapshot_a.
    - snapshot_a
  # Comment for meadow_b.
  # And another comment.
  meadow_b:
    - snapshot_b
"""
    expected_content = """\
steps:
  # Comment for meadow_a.
  meadow_a:
    # Comment for snapshot_a.
    - snapshot_a
  # Comment for meadow_b.
  # And another comment.
  meadow_b:
    - snapshot_b
  meadow_c:
    - snapshot_a
    - snapshot_b
"""
    _assert_write_to_dag_file(old_content, expected_content, dag_part={"meadow_c": ["snapshot_a", "snapshot_b"]})


def test_write_to_dag_file_respect_line_breaks():
    old_content = """\
steps:
  # Comment for meadow_a.
  meadow_a:
    # Comment for snapshot_a.
    - snapshot_a

  # Comment for meadow_b.

  # And another comment.
  meadow_b:

    - snapshot_b
"""
    expected_content = """\
steps:
  # Comment for meadow_a.
  meadow_a:
    # Comment for snapshot_a.
    - snapshot_a

  # Comment for meadow_b.

  # And another comment.
  meadow_b:

    - snapshot_b
  meadow_c:
    - snapshot_a
    - snapshot_b
"""
    _assert_write_to_dag_file(old_content, expected_content, dag_part={"meadow_c": ["snapshot_a", "snapshot_b"]})


def test_write_to_dag_file_remove_comments_within_updated_dependencies():
    old_content = """\
steps:
  # Comment for meadow_a.
  meadow_a:
    # Comment for snapshot_a.
    - snapshot_a
  # Comment for meadow_b.
  # And another comment.
  meadow_b:
    # Comment for snapshot_b.
    - snapshot_b
"""
    # NOTE: This is not necessarily desired behavior, but it is the one to be expected.
    # Keeping track of comments among dependencies may be a bit trickier.
    expected_content = """\
steps:
  # Comment for meadow_a.
  meadow_a:
    # Comment for snapshot_a.
    - snapshot_a
  # Comment for meadow_b.
  # And another comment.
  meadow_b:
    - snapshot_b
"""
    _assert_write_to_dag_file(old_content, expected_content, dag_part={"meadow_b": ["snapshot_b"]})


def test_write_to_dag_file_add_comments():
    old_content = """\
steps:
  # Comment for meadow_a.
  meadow_a:
    # Comment for snapshot_a.
    - snapshot_a
  # Comment for meadow_b.
  # And another comment.
  meadow_b:
    # Comment for snapshot_b.
    - snapshot_b
"""
    expected_content = """\
steps:
  # Comment for meadow_a.
  meadow_a:
    # Comment for snapshot_a.
    - snapshot_a
  # Comment for meadow_b.
  # And another comment.
  meadow_b:
    # Comment for snapshot_b.
    - snapshot_b
  # Comment for meadow_c.
  meadow_c:
    - snapshot_a
    - snapshot_b
"""
    _assert_write_to_dag_file(
        old_content,
        expected_content,
        dag_part={"meadow_c": ["snapshot_a", "snapshot_b"]},
        comments={"meadow_c": "# Comment for meadow_c."},
    )


def test_write_to_dag_file_with_include_section():
    old_content = """\
steps:
  # Comment for meadow_a.
  meadow_a:
    # Comment for snapshot_a.
    - snapshot_a
  # Comment for meadow_b.
  # And another comment.
  meadow_b:
    # Comment for snapshot_b.
    - snapshot_b
include:
  - path/to/another/dag.yml
"""
    # NOTE: By construction, we impose that there must be an empty space between steps and include sections.
    expected_content = """\
steps:
  # Comment for meadow_a.
  meadow_a:
    # Comment for snapshot_a.
    - snapshot_a
  # Comment for meadow_b.
  # And another comment.
  meadow_b:
    # Comment for snapshot_b.
    - snapshot_b
  # Comment for meadow_c.
  meadow_c:
    - snapshot_a
    - snapshot_b

include:
  - path/to/another/dag.yml
"""
    _assert_write_to_dag_file(
        old_content,
        expected_content,
        dag_part={"meadow_c": ["snapshot_a", "snapshot_b"]},
        comments={"meadow_c": "# Comment for meadow_c."},
    )


def test_write_to_dag_file_keeping_comments_below_updated_steps():
    old_content = """\
steps:
  # Comment of step to be updated.
  meadow_a:
    - snapshot_a
  # Comment of the step following the updated step.
  meadow_b:
    - snapshot_b
"""
    expected_content = """\
steps:
  # Comment of step to be updated.
  meadow_a:
    - snapshot_a
  # Comment of the step following the updated step.
  meadow_b:
    - snapshot_b
"""
    _assert_write_to_dag_file(old_content, expected_content, dag_part={"meadow_a": ["snapshot_a"]})


def test_write_to_dag_file_keeping_comments_below_updated_steps_with_line_breaks():
    old_content = """\
steps:
  # Comment of step to be updated.
  meadow_a:
    - snapshot_a
  # Comment of the step following the updated step.


  meadow_b:
    - snapshot_b
"""
    expected_content = """\
steps:
  # Comment of step to be updated.
  meadow_a:
    - snapshot_a


  # Comment of the step following the updated step.
  meadow_b:
    - snapshot_b
"""
    # NOTE: This is not necessarily desired behavior, but it is the one to be expected.
    # Comments that come after a step definition are considered belonging to the next step.
    # Therefore, they will appear right above the next step (and the line breaks will be below the previous step).
    _assert_write_to_dag_file(old_content, expected_content, dag_part={"meadow_a": ["snapshot_a"]})


def test_write_to_dag_file_keeping_comments_on_two_consecutive_updated_steps():
    old_content = """\
steps:
  # Comment of step to be updated.
  meadow_a:
    - snapshot_a
  # Comment of the step following the updated step.
  meadow_b:
    - snapshot_b
"""
    expected_content = """\
steps:
  # Comment of step to be updated.
  meadow_a:
    - snapshot_a
  # Comment of the step following the updated step.
  meadow_b:
    - snapshot_b
    - snapshot_c
"""
    # NOTE: This is not necessarily desired behavior, but it is the one to be expected.
    # Comments that come after a step definition are considered belonging to the next step.
    # Therefore, they will appear right above the next step (and the line breaks will be below the previous step).
    _assert_write_to_dag_file(
        old_content, expected_content, dag_part={"meadow_a": ["snapshot_a"], "meadow_b": ["snapshot_b", "snapshot_c"]}
    )


# ---------------------------------------------------------------------------
# Nested syntax / compact / flatten
# ---------------------------------------------------------------------------


def test_parse_dag_yaml_nested_syntax_equals_flat():
    flat = {
        "steps": {
            "data://meadow/un/2022-07-11/un_wpp": ["snapshot://un/2022-07-11/un_wpp.zip"],
            "data://garden/un/2022-07-11/un_wpp": ["data://meadow/un/2022-07-11/un_wpp"],
            "data://grapher/un/2022-07-11/un_wpp": ["data://garden/un/2022-07-11/un_wpp"],
        }
    }
    nested = {
        "steps": {
            "data://grapher/un/2022-07-11/un_wpp": [
                {
                    "data://garden/un/2022-07-11/un_wpp": [
                        {"data://meadow/un/2022-07-11/un_wpp": ["snapshot://un/2022-07-11/un_wpp.zip"]}
                    ]
                }
            ]
        }
    }
    assert _parse_dag_yaml(flat) == _parse_dag_yaml(nested)


def test_parse_dag_yaml_nested_shared_dep_stays_flat():
    # A step that is consumed by two parents can only be declared once.
    # Nest it under the first consumer and reference it as a plain string
    # from the second — the flattened graph must still assign both
    # consumers as its parents.
    dag = {
        "steps": {
            "data://grapher/foo/a": [
                {"data://garden/foo/a": ["data://meadow/foo/shared"]},
            ],
            "data://grapher/foo/b": ["data://garden/foo/a"],
        }
    }
    result = _parse_dag_yaml(dag)
    assert result["data://garden/foo/a"] == {"data://meadow/foo/shared"}
    assert result["data://grapher/foo/a"] == {"data://garden/foo/a"}
    assert result["data://grapher/foo/b"] == {"data://garden/foo/a"}


def test_parse_dag_yaml_duplicate_nested_step_raises():
    dag = {
        "steps": {
            "data://grapher/foo/a": [{"data://garden/foo/a": ["snap"]}],
            "data://grapher/foo/b": [{"data://garden/foo/a": ["snap"]}],
        }
    }
    with pytest.raises(ValueError, match="Duplicate step"):
        _parse_dag_yaml(dag)


def test_parse_dag_yaml_multi_key_nested_mapping_raises():
    dag = {
        "steps": {
            "data://grapher/foo/a": [{"data://a": [], "data://b": []}],
        }
    }
    with pytest.raises(ValueError, match="single-key mapping"):
        _parse_dag_yaml(dag)


def test_flatten_dag_file_flat_is_noop():
    flat_content = """\
steps:
  data://meadow/a:
    - snap_a
  data://garden/a:
    - data://meadow/a
"""
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "dag.yml"
        p.write_text(flat_content)
        assert flatten_dag_file(p) is False
        assert p.read_text() == flat_content


def test_flatten_dag_file_promotes_nested_entries_and_preserves_comments():
    nested_content = """\
steps:
  # UN WPP (2022)
  data://grapher/un/2022-07-11/un_wpp:
    - data://garden/un/2022-07-11/un_wpp:
      - data://meadow/un/2022-07-11/un_wpp:
        - snapshot://un/2022-07-11/un_wpp.zip

  # Separate chain
  data://grapher/foo/2024/a:
    - snapshot://foo/2024/a.zip
"""
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "dag.yml"
        p.write_text(nested_content)
        assert flatten_dag_file(p) is True
        flat = p.read_text()
        assert "# UN WPP (2022)" in flat
        assert "# Separate chain" in flat
        # Every chain member is now declared at the top level.
        for step in (
            "data://meadow/un/2022-07-11/un_wpp",
            "data://garden/un/2022-07-11/un_wpp",
            "data://grapher/un/2022-07-11/un_wpp",
        ):
            assert f"\n  {step}:" in flat
        # Idempotent.
        assert flatten_dag_file(p) is False


def _compact_roundtrip(content: str) -> tuple[str, dict]:
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "dag.yml"
        p.write_text(content)
        consumers = build_consumer_graph(p)
        before = load_dag(p)
        compact_dag_file(p, consumers=consumers)
        after = p.read_text()
        # Idempotence: second run must not change the file.
        compact_dag_file(p, consumers=consumers)
        assert p.read_text() == after, "compact is not idempotent"
        # Semantic preservation via flatten.
        flatten_dag_file(p)
        assert load_dag(p) == before
        return after, before


def test_compact_dag_file_folds_linear_chain():
    flat = """\
steps:
  # UN WPP (2022)
  data://meadow/un/2022-07-11/un_wpp:
    - snapshot://un/2022-07-11/un_wpp.zip
  data://garden/un/2022-07-11/un_wpp:
    - data://meadow/un/2022-07-11/un_wpp
  data://grapher/un/2022-07-11/un_wpp:
    - data://garden/un/2022-07-11/un_wpp
"""
    compacted, _ = _compact_roundtrip(flat)
    assert (
        compacted
        == """\
steps:
  # UN WPP (2022)
  data://grapher/un/2022-07-11/un_wpp:
    - data://garden/un/2022-07-11/un_wpp:
      - data://meadow/un/2022-07-11/un_wpp:
        - snapshot://un/2022-07-11/un_wpp.zip
"""
    )


def test_compact_dag_file_keeps_shared_dep_flat():
    flat = """\
steps:
  data://meadow/foo/a:
    - snap_a
  data://garden/foo/a:
    - data://meadow/foo/a
  data://garden/foo/b:
    - data://meadow/foo/a
"""
    compacted, _ = _compact_roundtrip(flat)
    # meadow is used by both gardens so it must stay at the top level.
    assert "  data://meadow/foo/a:\n" in compacted
    assert "    - data://meadow/foo/a" in compacted


def test_write_to_dag_file_handles_nested_input():
    # Writers operate on a flat file; if the target file has nested entries,
    # they should be flattened first so that the line-based logic finds the
    # step it is meant to update.
    old_content = """\
steps:
  data://grapher/un/2022-07-11/un_wpp:
    - data://garden/un/2022-07-11/un_wpp:
      - data://meadow/un/2022-07-11/un_wpp:
        - snapshot://un/2022-07-11/un_wpp.zip
"""
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "dag.yml"
        p.write_text(old_content)
        # Update the garden step to add a new dep — this must apply whether
        # the file is stored flat or nested.
        write_to_dag_file(
            p,
            dag_part={
                "data://garden/un/2022-07-11/un_wpp": [
                    "data://meadow/un/2022-07-11/un_wpp",
                    "data://garden/regions/latest",
                ]
            },
        )
        after = p.read_text()
        assert "    - data://garden/regions/latest" in after
        # The update must not silently reintroduce the old single-dep form.
        assert load_dag(p)["data://garden/un/2022-07-11/un_wpp"] == {
            "data://meadow/un/2022-07-11/un_wpp",
            "data://garden/regions/latest",
        }


def test_remove_steps_from_dag_file_handles_nested_input():
    old_content = """\
steps:
  data://grapher/un/2022-07-11/un_wpp:
    - data://garden/un/2022-07-11/un_wpp:
      - data://meadow/un/2022-07-11/un_wpp:
        - snapshot://un/2022-07-11/un_wpp.zip
"""
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "dag.yml"
        p.write_text(old_content)
        remove_steps_from_dag_file(p, ["data://meadow/un/2022-07-11/un_wpp"])
        graph = load_dag(p)
        assert "data://meadow/un/2022-07-11/un_wpp" not in graph


def test_load_single_dag_file_returns_nested_steps_as_top_level_keys():
    # ``load_single_dag_file`` is the entry point used by
    # ``etl.version_tracker.load_steps_for_each_dag_file`` to attribute steps
    # to a file; it must treat nested and flat declarations identically.
    content = """\
steps:
  data://grapher/un/2022-07-11/un_wpp:
    - data://garden/un/2022-07-11/un_wpp:
      - data://meadow/un/2022-07-11/un_wpp:
        - snapshot://un/2022-07-11/un_wpp.zip
"""
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "dag.yml"
        p.write_text(content)
        graph = load_single_dag_file(p)
        assert "data://grapher/un/2022-07-11/un_wpp" in graph
        assert "data://garden/un/2022-07-11/un_wpp" in graph
        assert "data://meadow/un/2022-07-11/un_wpp" in graph
        assert graph["data://meadow/un/2022-07-11/un_wpp"] == {"snapshot://un/2022-07-11/un_wpp.zip"}
