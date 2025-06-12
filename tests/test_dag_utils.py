import tempfile
from pathlib import Path

from etl.dag_helpers import (
    get_comments_above_step_in_dag,
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
        with open(temp_file, "r") as updated_file:
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
        with open(temp_file, "r") as updated_file:
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
