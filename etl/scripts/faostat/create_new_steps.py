"""Create new version of FAOSTAT steps in meadow or garden.

Workflow to create a new version of the FAOSTAT dataset (with version YYYY-MM-DD):
1. Execute this script for the meadow channel.
  > python etl/scripts/faostat/create_new_steps.py -w meadow
2. Run the new etl meadow steps.
  > etl meadow/faostat/YYYY-MM-DD
3. Run this script again for the garden channel.
4. Run the new etl garden steps.
  > etl garden/faostat/YYYY-MM-DD
# TODO: Generalise for grapher steps.

This script will, for a given channel:
* Create a new folder in the channel (named after today's date) and copy step files (for datasets that were updated in
  walden) from their latest code version into that folder.
* Update the dag file with the new steps and their dependencies.

"""

import argparse
import datetime
import re
from typing import Dict, List, Optional

import pandas as pd
from owid.walden import Catalog

from etl.paths import DAG_FILE, DATA_DIR, STEP_DIR
from etl.steps import load_dag

# Current namespace.
NAMESPACE = "faostat"
# Base name of additional metadata file.
ADDITIONAL_METADATA_FILE_NAME = f"{NAMESPACE}_metadata"
RUN_FILE_NAME = "shared"
# Glob pattern to match version folders like "YYYY-MM-DD".
# Note: This is not a regular expression (glob does not accept them), but it works both for glob and for re.
GLOB_VERSION_PATTERN = "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]"
# New version of steps to create.
NEW_VERSION = datetime.datetime.today().strftime("%Y-%m-%d")
# List custom datasets that will be created in garden.
ADDITIONAL_GARDEN_DATASETS = ["faostat_fbsc"]


def list_all_steps() -> List[str]:
    """List all steps (e.g. 'faostat_qcl') to be created in a channel, based on the list of domains considered in the
    latest walden ingest.

    Note: The metadata step is always added as a step.

    Returns
    -------
    step_names : List[str]
        Names of steps.

    """
    # Load walden dataset.
    walden_ds = Catalog().find_latest(
        namespace=NAMESPACE, short_name=ADDITIONAL_METADATA_FILE_NAME
    )
    # List all domains.
    domains = pd.read_json(walden_ds.ensure_downloaded()).columns.tolist()
    step_names = [f"{NAMESPACE}_{domain}" for domain in domains]
    # Add metadata step to the list.
    step_names += [ADDITIONAL_METADATA_FILE_NAME]
    step_names = sorted(step_names)

    return step_names


def get_channel_from_dag_line(dag_line: str) -> str:
    """TODO"""
    if dag_line.startswith("data://garden"):
        channel = "garden"
    elif dag_line.startswith("data://meadow"):
        channel = "meadow"
    elif dag_line.startswith("walden://"):
        channel = "walden"
    elif dag_line.startswith("grapher://"):
        channel = "grapher"
    else:
        raise ValueError("dag line not understood")

    return channel


def create_dag_line_name(channel: str, step_name: str) -> str:
    """TODO"""
    if channel in ["meadow", "garden"]:
        dag_line_name = f"data://{channel}/{NAMESPACE}/{NEW_VERSION}/{step_name}"
    elif channel in ["walden", "grapher"]:
        dag_line_name = f"{channel}://{NAMESPACE}/{NEW_VERSION}/{step_name}"
    else:
        raise ValueError("wrong channel name")

    return dag_line_name


def get_version_from_dag_line(dag_line: str) -> str:
    """TODO"""
    matches = re.findall(GLOB_VERSION_PATTERN, dag_line)
    if len(matches) == 1:
        version = matches[0]
    else:
        raise ValueError("dag line not understood")

    return version


def get_dataset_name_from_dag_line(dag_line: str) -> str:
    """TODO"""
    dataset_name = dag_line.split("/")[-1]

    return dataset_name


def list_updated_steps() -> List[str]:
    """TODO"""
    # Find latest walden folder.
    all_walden_datasets = Catalog().find(namespace=NAMESPACE)
    latest_walden_version = sorted([walden_ds.version for walden_ds in all_walden_datasets])[-1]

    # Now find what steps have that version.
    step_names = [walden_ds.short_name for walden_ds in all_walden_datasets
                  if walden_ds.version == latest_walden_version]

    return step_names


def create_step_file(channel: str, step_name: str) -> None:
    """TODO"""
    # Path to folder containing steps in this channel.
    versions_dir = STEP_DIR / "data" / channel / NAMESPACE
    # Path to folder to be created with new steps.
    new_step_file = versions_dir / NEW_VERSION / f"{step_name}.py"
    # Find latest version of this dataset in channel.
    step_latest_version = find_latest_version_for_step(channel=channel, step_name=step_name)
    if step_latest_version is None:
        print(f"WARNING: No version found for dataset {step_name} in {channel}. Creating file from scratch.")
        # Create the file from scratch, assuming that it will use the shared run.
        file_content = f"from .{RUN_FILE_NAME} import run\n"
        new_step_file.write_text(file_content)
    else:
        # TODO: Compare md5 of shared module in step_latest_version with md5 of latest shared module.
        #  If they differ, raise warning.
        # Copy the file from its latest version.
        latest_step_file = versions_dir / step_latest_version / f"{step_name}.py"
        new_step_file.write_text(latest_step_file.read_text())


def create_steps(channel: str, step_names: List[str]) -> None:
    """TODO"""
    # Path to folder containing steps in this channel.
    versions_dir = STEP_DIR / "data" / channel / NAMESPACE
    # Latest version (taken from the name of the most recent folder).
    latest_version = find_latest_version_for_channel(channel=channel)
    # Path to folder containing code for steps in the latest version.
    latest_version_dir = versions_dir / latest_version
    # Path to folder to be created with new steps.
    new_version_dir = versions_dir / NEW_VERSION

    # Create new folder.
    new_version_dir.mkdir()

    # Copy run file from latest step to new step.
    latest_run_file = latest_version_dir / (RUN_FILE_NAME + ".py")
    new_run_file = new_version_dir / (RUN_FILE_NAME + ".py")
    new_run_file.write_text(latest_run_file.read_text())

    for step_name in step_names:
        create_step_file(channel=channel, step_name=step_name)


def create_dependency_graph(channel: str, step_names: List[str]) -> Dict[str, set]:
    """TODO"""
    dag = load_dag()
    new_steps = {}
    for step_name in step_names:
        # Get the latest occurrence of this dataset in the dag.
        candidates = {step: dag[step] for step in dag
                      if step_name == get_dataset_name_from_dag_line(step) if get_channel_from_dag_line(step) == channel}

        if len(candidates) == 0:
            # Create new step.
            new_step_name = create_dag_line_name(channel=channel, step_name=step_name)
            new_dependencies = []
            # Find its natural dependency.
            natural_dependency = find_natural_dependency_for_step(channel=channel, step_name=step_name)
            if natural_dependency is not None:
                new_dependencies.append(natural_dependency)
        else:
            latest_version = sorted([get_version_from_dag_line(candidate) for candidate in candidates])[-1]

            step_to_update = {step: dag[step] for step in candidates
                              if get_version_from_dag_line(step) == latest_version}
            assert len(list(step_to_update)) == 1
            step_name = list(step_to_update)[0]
            dependencies = step_to_update[step_name]

            # Change the name of the step to be the latest version of the dataset.
            new_step_name = step_name.replace(latest_version, NEW_VERSION)

            # Update the version of each of the dataset dependencies.
            new_dependencies = []
            for dependency in dependencies:
                # Get old dependency version from the old dag line.
                dependency_old_version = get_version_from_dag_line(dependency)
                # Find the latest version of that dependency.
                dependency_new_version = find_latest_version_for_step(
                    channel=get_channel_from_dag_line(dependency), step_name=get_dataset_name_from_dag_line(dependency))
                # Rename the dag line appropriately.
                new_dependency = dependency.replace(dependency_old_version, dependency_new_version)
                new_dependencies.append(new_dependency)

        if len(new_dependencies) > 0:
            # Add the new dag line and its dependencies to the new dag.
            new_steps[new_step_name] = set(new_dependencies)

    return new_steps


def write_steps_to_dag_file(
    dag_steps: Dict[str, set], header_line: Optional[str]
) -> None:
    """TODO"""
    # Indentation of steps in the dag file.
    step_indent = "  "
    dependency_indent = "    - "
    new_step_lines = ""
    if header_line:
        new_step_lines += f"{step_indent}#\n"
        new_step_lines += f"{step_indent}{header_line}\n"
        new_step_lines += f"{step_indent}#\n"

    # Load dag from file.
    dag = load_dag()

    # Initialise a flag that is True only if at least one step has to be written to dag.
    any_step_updated = False
    for dag_step in dag_steps:
        # Add new step to dag if not already there.
        if dag_step in dag:
            print(f"Dag step {dag_step} already in dag. Skipping.")
        else:
            any_step_updated = True
            new_step_lines += f"{step_indent}{dag_step}:\n"
            for dependency in dag_steps[dag_step]:
                new_step_lines += f"{dependency_indent}{dependency}\n"

    if any_step_updated:
        print("Writing new steps to dag file.")
        # Add new lines to dag file.
        with open(DAG_FILE, "a") as _dag_file:
            _dag_file.write(new_step_lines)


def find_latest_version_for_channel(channel: str) -> str:
    """TODO"""
    # Path to folder containing steps in this channel.
    versions_dir = STEP_DIR / "data" / channel / NAMESPACE
    # Latest version (taken from the name of the most recent folder).
    latest_version = sorted(list(versions_dir.glob(GLOB_VERSION_PATTERN)))[-1].name

    return latest_version


def find_latest_version_for_step(channel: str, step_name: str) -> Optional[str]:
    """TODO"""
    latest_version = None
    if channel == "walden":
        # Find latest walden version for current step.
        try:
            latest_version = (
                Catalog().find_latest(namespace=NAMESPACE, short_name=step_name).version
            )
        except ValueError:
            pass
    elif channel in ["meadow", "garden"]:
        dataset_versions = sorted(list((DATA_DIR / channel / NAMESPACE).glob(f"*/{step_name}")))

        if len(dataset_versions) > 0:
            latest_version = dataset_versions[-1].parent.name

    return latest_version


def find_natural_dependency_for_step(channel: str, step_name: str) -> Optional[str]:
    """TODO"""
    if channel == "meadow":
        try:
            # Find latest walden version for current step.
            walden_version = (
                Catalog().find_latest(namespace=NAMESPACE, short_name=step_name).version
            )
            # Define dependencies for new step.
            dependency_step = f"walden://{NAMESPACE}/{walden_version}/{step_name}"
        except ValueError:
            print(f"WARNING: Dataset {step_name} not found in walden.")
            dependency_step = None
    elif channel == "garden":
        meadow_version = find_latest_version_for_step(channel="meadow", step_name=step_name)
        dependency_step = f"data://meadow/{NAMESPACE}/{meadow_version}/{step_name}"
    else:
        raise ValueError("channel must be meadow or garden")

    return dependency_step


def main(channel: str, include_all_datasets: bool = False) -> None:
    # Latest version (taken from the name of the most recent folder).
    latest_version = find_latest_version_for_channel(channel=channel)

    if latest_version == NEW_VERSION:
        print(f"Dataset is already up-to-date with version: {NEW_VERSION}")
    else:
        if include_all_datasets:
            # List all datasets, even if their source data was not updated.
            step_names = list_all_steps()
        else:
            # List steps for which source data was updated.
            step_names = list_updated_steps()

        # Create folder for new version and add a step file for each dataset.
        create_steps(channel=channel, step_names=step_names)

        # Generate dictionary of step dependencies.
        dag_steps = create_dependency_graph(channel=channel, step_names=step_names)

        # Update dag file.
        header_line = f"# FAOSTAT {channel} steps for version {NEW_VERSION}"
        write_steps_to_dag_file(dag_steps=dag_steps, header_line=header_line)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "-c",
        "--channel",
        help=f"Name of channel where new step will be created (either meadow or garden).",
        required=True,
    )
    parser.add_argument(
        "-a",
        "--include_all_datasets",
        default=False,
        action="store_true",
        help="If given, create step files for all datasets, even if the source data was not updated. Otherwise create "
             "step files only for datasets that were updated.",
    )
    args = parser.parse_args()
    main(
        channel=args.channel,
        include_all_datasets=args.include_all_datasets,
    )
