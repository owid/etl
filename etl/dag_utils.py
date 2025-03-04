from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Union

import yaml
from structlog import get_logger

from etl import paths

log = get_logger()
Graph = Dict[str, Set[str]]


def get_comments_above_step_in_dag(step: str, dag_file: Path) -> str:
    """Get the comment lines right above a step in the dag file."""

    # Read the content of the dag file.
    with open(dag_file, "r") as _dag_file:
        lines = _dag_file.readlines()

    # Initialize a list to store the header lines.
    header_lines = []
    for line in lines:
        if line.strip().startswith("-") or (
            line.strip().endswith(":") and (not line.strip().startswith("#")) and (step not in line)
        ):
            # Restart the header if the current line:
            # * Is a dependency.
            # * Is a step that is not the current step.
            # * Is a special line like "steps:" or "include:".
            header_lines = []
        elif step in line and line.strip().endswith(":"):
            # If the current line is the step, stop reading the rest of the file.
            return "\n".join([line.strip() for line in header_lines]) + "\n" if len(header_lines) > 0 else ""
        elif line.strip() == "":
            # If the current line is empty, ignore it.
            continue
        else:
            # Any line that is not a dependency,
            header_lines.append(line)

    # If the step has not been found, raise an error and return nothing.
    log.error(f"Step {step} not found in dag file {dag_file}.")

    return ""


def write_to_dag_file(
    dag_file: Path,
    dag_part: Dict[str, Any],
    comments: Optional[Dict[str, str]] = None,
    indent_step=2,
    indent_dependency=4,
):
    """Update the content of a dag file, respecting the comments above the steps.

    NOTE: A simpler implementation of function may be possible using ruamel. However, I couldn't find out how to respect
    comments that are above steps.

    Parameters
    ----------
    dag_file : Path
        Path to dag file.
    dag_part : Dict[str, Any]
        Partial dag, containing the steps that need to be updated.
        This partial dag is a dictionary with steps as keys and the set of dependencies as values.
    comments : Optional[Dict[str, str]], optional
        Comments to add above the steps in the partial dag. The keys are the steps, and the values are the comments.
    indent_step : int, optional
        Number of spaces to use as indentation for steps in the dag.
    indent_dependency : int, optional
        Number of spaces to use as indentation for dependencies in the dag.

    """

    # If comments is not defined, assume an empty dictionary.
    if comments is None:
        comments = {}

    for step in comments:
        if len(comments[step]) > 0 and comments[step][-1] != "\n":
            # Ensure all comments end in a line break, otherwise add it.
            comments[step] = comments[step] + "\n"

    # Read the lines in the original dag file.
    with open(dag_file, "r") as file:
        lines = file.readlines()

    # Separate that content into the "steps" section (always given) and the "include" section (sometimes given).
    section_steps = []
    section_include = []
    inside_section_steps = True
    for line in lines:
        if line.strip().startswith("include"):
            inside_section_steps = False
        if inside_section_steps:
            section_steps.append(line)
        else:
            section_include.append(line)

    # Now the "steps" section will be updated, and at the end the "include" section will be appended.

    # Initialize a list with the new lines that will be written to the dag file.
    updated_lines = []
    # Initialize a list of comments preceding the next step after a given step.
    comments_next_step = []
    # Initialize a flag to skip lines until the next step.
    skip_until_next_step = False
    # Initialize a set to keep track of the steps that were found in the original dag file.
    steps_found = set()
    for line in section_steps:
        # Remove leading and trailing whitespace from the line.
        stripped_line = line.strip()

        # Identify the start of a step, e.g. "  data://meadow/temp/latest/step:".
        if stripped_line.endswith(":") and not stripped_line.startswith("-") and not stripped_line.startswith("steps:"):
            if comments_next_step:
                updated_lines.extend(comments_next_step)
                comments_next_step = []
            # Extract the name of the step (without the ":" at the end).
            current_step = ":".join(stripped_line.split(":")[:-1])
            if current_step in dag_part:
                # This step was in dag_part, which means it needs to be updated.
                # First add the step itself.
                updated_lines.append(line)
                # Now add each of its dependencies.
                for dep in dag_part[current_step]:
                    updated_lines.append(" " * indent_dependency + f"- {dep}\n")
                # Skip the following lines until the next step is found.
                skip_until_next_step = True
                # Start tracking possible comments of the next step.
                comments_next_step = []
                # Add the current step to the set of steps found in the dag file.
                steps_found.add(current_step)
                continue
            else:
                # This step was not in dag_part, so it will be copied as is.
                skip_until_next_step = False

        # Skip dependencies and comments among dependencies of the step being updated.
        if skip_until_next_step:
            if stripped_line.startswith("-"):
                # Remove comments among dependencies.
                comments_next_step = []
                continue
            elif stripped_line.startswith("#"):
                # Add comments that may potentially be related to the next step.
                comments_next_step.append(line)
                continue

        # Add lines that should not be skipped.
        updated_lines.append(line)

    # Append new steps that weren't found in the original content.
    for step, dependencies in dag_part.items():
        if step not in steps_found:
            # Add the comment for this step, if any was given.
            if step in comments:
                updated_lines.append(
                    " " * indent_step + ("\n" + " " * indent_step).join(comments[step].split("\n")[:-1]) + "\n"
                    if len(comments[step]) > 0
                    else ""
                )
            # Add the step itself.
            updated_lines.append(" " * indent_step + f"{step}:\n")
            # Add each of its dependencies.
            for dep in dependencies:
                updated_lines.append(" " * indent_dependency + f"- {dep}\n")

    if len(section_include) > 0:
        # Append the include section, ensuring there is only one line break in between.
        for i in range(len(updated_lines) - 1, -1, -1):
            if updated_lines[i] != "\n":
                # Slice the list to remove trailing line breaks
                updated_lines = updated_lines[: i + 1]
                break
        # Add a single line break before the include section, and then add the include section.
        updated_lines.extend(["\n"] + section_include)

    # Write the updated content back to the dag file.
    with open(dag_file, "w") as file:
        file.writelines(updated_lines)


def _remove_step_from_dag_file(dag_file: Path, step: str) -> None:
    with open(dag_file, "r") as file:
        lines = file.readlines()

    new_lines = []
    _number_of_comment_lines = 0
    _step_detected = False
    _continue_until_the_end = False
    num_spaces_indent = 0
    for line in lines:
        if line.startswith("include"):
            # Nothing should be removed from here onwards, so, skip until the end of the file.
            _continue_until_the_end = True

            # Ensure there is a space before the include section starts.
            if new_lines[-1].strip() != "":
                new_lines.append("\n")

        if line.startswith("steps:"):
            # Store this special line and move on.
            new_lines.append(line)
            # If there were comments above "steps", keep them.
            _number_of_comment_lines = 0
            continue

        if _continue_until_the_end:
            new_lines.append(line)
            continue

        if not _step_detected:
            if line.strip().startswith("#") or line.strip() == "":
                _number_of_comment_lines += 1
                new_lines.append(line)
                continue
            elif line.strip().startswith(step):
                if _number_of_comment_lines > 0:
                    # Remove the previous comment lines and ignore the current line.
                    new_lines = new_lines[:-_number_of_comment_lines]
                # Find the number of spaces on the left of the step name.
                # We need this to know if the next comments are indented (as comments within dependencies).
                num_spaces_indent = len(line) - len(line.lstrip())
                _step_detected = True
                continue
            else:
                # This line corresponds to any other step or step dependency.
                new_lines.append(line)
                _number_of_comment_lines = 0
                continue
        else:
            if line.strip().startswith("- "):
                # Ignore the dependencies of the step.
                continue
            elif (line.strip().startswith("#")) and (len(line) - len(line.lstrip()) > num_spaces_indent):
                # Ignore comments that are indented (as comments within dependencies).
                continue
            elif line.strip() == "":
                # Ignore empty lines.
                continue
            else:
                # The step dependencies have ended. Append current line and continue until the end of the dag file.
                new_lines.append(line)
                _continue_until_the_end = True
                continue

    # Write the new content to the active dag file.
    with open(dag_file, "w") as file:
        file.writelines(new_lines)


def remove_steps_from_dag_file(dag_file: Path, steps_to_remove: List[str]) -> None:
    """Remove specific steps from a dag file, including their comments.

    Parameters
    ----------
    dag_file : Path
        Path to dag file.
    steps_to_remove : List[str]
        List of steps to be removed from the DAG file.
        Their dependencies do not need to be specified (they will also be removed).

    """
    for step in steps_to_remove:
        _remove_step_from_dag_file(dag_file=dag_file, step=step)


def create_dag_archive_file(dag_file_archive: Path) -> None:
    """Create an empty dag archive file, and add it to the main dag archive file.

    Parameters
    ----------
    dag_file_archive : Path
        Path to a specific dag archive file that does not exist yet.

    """
    # Create a new archive dag file.
    dag_file_archive.write_text("steps:\n")
    # Find the number of spaces in the indentation of the main dag archive file.
    n_spaces_include_section = 2
    with open(paths.DAG_ARCHIVE_FILE, "r") as file:
        lines = file.readlines()
    for i, line in enumerate(lines):
        if line.strip().startswith("include"):
            n_spaces_include_section = [
                len(_line) - len(_line.lstrip()) for _line in lines[i + 1 :] if _line.strip().startswith("- ")
            ][0]
    # Add this archive dag file to the main dag archive file.
    dag_file_archive_relative = dag_file_archive.relative_to(Path(paths.DAG_DIR).parent)
    with open(paths.DAG_ARCHIVE_FILE, "a") as file:
        file.write(f"{' ' * n_spaces_include_section}- {dag_file_archive_relative}\n")


def load_dag(filename: Union[str, Path] = paths.DEFAULT_DAG_FILE) -> Graph:
    return _load_dag(filename, {})


def _load_dag(filename: Union[str, Path], prev_dag: Dict[str, Any]):
    """
    Recursive helper to 1) load a dag itself, and 2) load any sub-dags
    included in the dag via 'include' statements
    """
    dag_yml = _load_dag_yaml(str(filename))
    curr_dag = _parse_dag_yaml(dag_yml)

    # make sure there are no fast-track steps in the DAG
    if "fasttrack.yml" not in str(filename):
        fast_track_steps = {step for step in curr_dag if "/fasttrack/" in step}
        if fast_track_steps:
            raise ValueError(f"Fast-track steps detected in DAG {filename}: {fast_track_steps}")

    duplicate_steps = prev_dag.keys() & curr_dag.keys()
    if duplicate_steps:
        raise ValueError(f"Duplicate steps detected in DAG {filename}: {duplicate_steps}")

    curr_dag.update(prev_dag)

    for sub_dag_filename in dag_yml.get("include", []):
        sub_dag = _load_dag(paths.BASE_DIR / sub_dag_filename, curr_dag)
        curr_dag.update(sub_dag)

    return curr_dag


def _load_dag_yaml(filename: str) -> Dict[str, Any]:
    with open(filename) as istream:
        return yaml.safe_load(istream)


def _parse_dag_yaml(dag: Dict[str, Any]) -> Dict[str, Any]:
    steps = dag["steps"] or {}

    return {node: set(deps) if deps else set() for node, deps in steps.items()}
