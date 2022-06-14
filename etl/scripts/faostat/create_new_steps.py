"""Create a new version of FAOSTAT steps in a channel (meadow or garden) and update the dag.

This script will, for a given channel:
* Create a new folder in the channel (named after today's date).
* For every dataset that had an update in the latest walden ingest, copy their latest step files onto the new folder.
  * Optionally (with the -a argument), this can be done for all datasets, not just the ones that had an update.
* Update the dag file with the new steps in the channel and and their dependencies.

Workflow to create a new version of the FAOSTAT datasets (with version YYYY-MM-DD):
0. Execute the walden ingest script, to fetch data for any dataset that may have been updated.
  > python vendor/walden/ingests/faostat.py
1. Execute this script for the meadow channel.
  > python etl/scripts/faostat/create_new_steps.py -c meadow
2. Run the new etl meadow steps, to generate the meadow datasets.
  > etl meadow/faostat/YYYY-MM-DD
3. Run this script again for the garden channel.
  > python etl/scripts/faostat/create_new_steps.py -c garden
4. Run the new etl garden steps, to generate the garden datasets.
  > etl garden/faostat/YYYY-MM-DD
5. Run this script again for the grapher channel.
  > python etl/scripts/faostat/create_new_steps.py -c grapher
6. Run the new etl grapher steps, to generate the grapher charts.
  > etl faostat/YYYY-MM-DD --grapher

"""

import argparse
import datetime
import re
from pathlib import Path
from typing import cast, Dict, List, Optional, Set, Tuple

import pandas as pd
from owid.walden import Catalog

from etl.paths import BASE_DIR, STEP_DIR
from etl.steps import load_dag
from etl.files import checksum_file

# Current namespace.
NAMESPACE = "faostat"
# Name of additional metadata file (without extension).
ADDITIONAL_METADATA_FILE_NAME = f"{NAMESPACE}_metadata"
# Path to dag file for FAOSTAT steps.
DAG_FILE = BASE_DIR / "dag_files" / "dag_faostat.yml"
# Name of shared module containing the run function (without extension).
RUN_FILE_NAME = "shared"
# Glob pattern to match version folders like "YYYY-MM-DD".
# Note: This is not a regular expression (glob does not accept them), but it works both for glob and for re.
GLOB_VERSION_PATTERN = "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]"
# New version tag to be created.
NEW_VERSION = datetime.datetime.today().strftime("%Y-%m-%d")
# Additional dependencies to add to each dag line of a specific channel, for new datasets (ones not in the dag).
# Give each dependency as a tuple of (channel, step_name). The latest version of that step will be assumed.
ADDITIONAL_DEPENDENCIES: Dict[str, List[Tuple[str, str]]] = {
    "meadow": [],
    "garden": [("meadow", "faostat_metadata"), ("garden", "owid/latest/key_indicators")],
    "grapher": [],
}
# List of additional files (with extension) that, if existing, should be copied over from the latest version to the new
# (besides the files of each of the steps).
ADDITIONAL_FILES_TO_COPY = [
    RUN_FILE_NAME + ".py",
    f"{NAMESPACE}.countries.json",
    "custom_datasets.csv",
    "custom_elements_and_units.csv",
    "custom_food_explorer_products.csv",
    "custom_items.csv",
]
# Note: Further custom rules are applied to the list of steps to run.
# These rules are defined in apply_custom_rules_to_list_of_steps_to_run.


def get_channel_from_dag_line(dag_line: str) -> str:
    """Get channel name from an entry in the dag (e.g. for a dag line 'data://garden/...', return 'garden').

    Parameters
    ----------
    dag_line : str
        Entry in the dag.

    Returns
    -------
    channel : str
        Channel name.

    """
    if dag_line.startswith("data://garden/"):
        channel = "garden"
    elif dag_line.startswith("data://meadow/"):
        channel = "meadow"
    elif dag_line.startswith("walden://"):
        channel = "walden"
    elif dag_line.startswith("grapher://"):
        channel = "grapher"
    else:
        raise ValueError("dag line not understood")

    return channel


def create_dag_line_name(
    channel: str, step_name: str, namespace: str = NAMESPACE, version: str = NEW_VERSION
) -> str:
    """Create the name of a dag line given its content (e.g. return 'data://garden/faostat/2022-05-05/faostat_qcl').

    Parameters
    ----------
    channel : str
        Channel name.
    step_name : str
        Step name.
    namespace : str
        Namespace.
    version : str
        Version.

    Returns
    -------
    dag_line : str
        Name of the dag line.

    """
    if channel in ["meadow", "garden"]:
        dag_line = f"data://{channel}/{namespace}/{version}/{step_name}"
    elif channel in ["walden", "grapher"]:
        dag_line = f"{channel}://{namespace}/{version}/{step_name}"
    else:
        raise ValueError("wrong channel name")

    return dag_line


def get_version_from_dag_line(
    dag_line: str, regex_version_pattern: str = GLOB_VERSION_PATTERN
) -> str:
    """Get the version of a certain step from the name of a dag line (e.g. given
    'data://garden/faostat/2022-05-05/faostat_qcl', return '2022-05-05').

    Parameters
    ----------
    dag_line : str
        Name of dag line.
    regex_version_pattern : str
        Regular expression to match version format (usually a date).

    Returns
    -------
    version : str
        Version.

    """
    # Match all instances of a date pattern in the name of the dag line.
    matches = re.findall(regex_version_pattern, dag_line)
    if len(matches) == 1:
        version = matches[0]
    else:
        raise ValueError("dag line not understood")

    return cast(str, version)


def get_dataset_name_from_dag_line(dag_line: str) -> str:
    """Get the name of a dataset from the name of a dag line (e.g. given 'data://garden/faostat/2022-05-05/faostat_qcl',
    return 'faostat_qcl').

    Parameters
    ----------
    dag_line : str
        Name of dag line.

    Returns
    -------
    dataset_name : str
        Name of dataset.

    """
    dataset_name = dag_line.split("/")[-1]

    return dataset_name


def list_updated_steps(channel: str, namespace: str = NAMESPACE) -> List[str]:
    """List all datasets in a namespace that were updated in the latest walden ingest.

    Parameters
    ----------
    channel : str
        Channel name (required to check the latest version for the considered namespace).
    namespace : str
        Namespace.

    Returns
    -------
    step_names : List[str]
        Names of datasets that were updated.

    """
    # Find latest walden folder.
    all_walden_datasets = Catalog().find(namespace=namespace)
    latest_walden_version = sorted(
        [walden_ds.version for walden_ds in all_walden_datasets]
    )[-1]

    # Find latest version in current channel for the considered namespace.
    latest_version_in_channel = find_latest_version_for_namespace_in_channel(
        channel=channel
    )

    if latest_walden_version > latest_version_in_channel:
        # Now find what steps have the latest version in walden.
        step_names = [
            walden_ds.short_name
            for walden_ds in all_walden_datasets
            if walden_ds.version == latest_walden_version
        ]
    else:
        # There is already a version for this namespace and channel that is posterior to the latest additions to walden.
        step_names = []
        print(
            f"There was no new additions to walden since the latest {channel} version, {latest_version_in_channel}."
        )

    return step_names


def list_all_steps() -> List[str]:
    """List all datasets (e.g. 'faostat_qcl') that are considered in the walden ingest (even if they were not updated).

    Note: The 'faostat_metadata' step is always added as a step.

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


def find_latest_version_for_namespace_in_channel(
    channel: str,
    namespace: str = NAMESPACE,
    regex_version_pattern: str = GLOB_VERSION_PATTERN,
) -> str:
    """Find latest version for any dataset of a given namespace in a channel.

    Parameters
    ----------
    channel : str
        Channel name.
    namespace : str
        Namespace.
    regex_version_pattern : str
        Regular expression to match version format (usually a date).

    Returns
    -------
    latest_version : str
        Latest version found for namespace in channel.

    """
    # Path to folder containing steps in this channel.
    versions_dir = get_path_to_step_files(channel=channel, namespace=namespace)

    # Latest version (taken from the name of the most recent folder).
    latest_version = sorted(list(versions_dir.glob(regex_version_pattern)))[-1].name

    return latest_version


def find_latest_version_for_step(
    channel: str, step_name: str, namespace: str = NAMESPACE
) -> Optional[str]:
    """Find the latest version of a certain step of a namespace in a channel.

    Parameters
    ----------
    channel : str
        Channel name.
    step_name : str
        Step name (dataset name).
    namespace : str
        Namespace.

    Returns
    -------
    latest_version : str or None
        Latest version found for the given step; if dataset was not found, return None and raise a warning.

    """
    latest_version = None
    warning_message = f"WARNING: Dataset {step_name} not found in {channel}."
    if channel == "walden":
        # Find latest version for current step in walden.
        try:
            latest_version = (
                Catalog().find_latest(namespace=namespace, short_name=step_name).version
            )
        except ValueError:
            print(warning_message)
    elif channel in ["meadow", "garden", "grapher"]:
        versions_dir = get_path_to_step_files(channel=channel, namespace=namespace)
        dataset_versions = sorted(list(versions_dir.glob(f"*/{step_name}.py")))
        if len(dataset_versions) > 0:
            latest_version = dataset_versions[-1].parent.name
        else:
            print(warning_message)

    return latest_version


def generate_content_for_new_step_file(channel: str) -> str:
    """Generate the content of a new step file when creating it from scratch.

    Parameters
    ----------
    channel : str
        Channel name.

    Returns
    -------
    file_content : str
        Content of the new step file.

    """
    if channel in ["meadow", "garden"]:
        file_content = f"from .{RUN_FILE_NAME} import run  # noqa:F401\n"
    elif channel == "grapher":
        file_content = (
            f"from .{RUN_FILE_NAME} import catalog, get_grapher_dataset_from_file_name\n"
            f"from .{RUN_FILE_NAME} import get_grapher_tables  # noqa:F401\n\n\n"
            f"def get_grapher_dataset() -> catalog.Dataset:\n"
            f"    return get_grapher_dataset_from_file_name(__file__)\n"
        )
    else:
        raise ValueError("channel name not understood")

    return file_content


def create_step_file(channel: str, step_name: str) -> None:
    """Create a new step file for a certain dataset, by copying its latest version onto the folder of the new version
    (which is assumed to already exist), or, if none found, by creating a file that imports run from a shared module.

    Parameters
    ----------
    channel : str
        Channel name.
    step_name : str
        Name of dataset.

    """
    # Path to folder containing steps in this channel.
    versions_dir = get_path_to_step_files(channel=channel)
    # Path to folder to be created with new steps.
    new_step_dir = versions_dir / NEW_VERSION
    # Path to new step file.
    new_step_file = new_step_dir / f"{step_name}.py"

    # Find latest version of this dataset in channel.
    step_latest_version = find_latest_version_for_step(
        channel=channel, step_name=step_name
    )
    if step_latest_version is None:
        print(f"Creating file from scratch for dataset {step_name}.")
        # Content of the file to be created.
        file_content = generate_content_for_new_step_file(channel=channel)
        new_step_file.write_text(file_content)
    else:
        # Copy the file from its latest version.
        latest_step_file = versions_dir / step_latest_version / f"{step_name}.py"
        new_step_file.write_text(latest_step_file.read_text())

        # Check if shared module in the latest version of the step is identical to the new shared module.
        latest_shared_file = versions_dir / step_latest_version / f"{RUN_FILE_NAME}.py"
        new_shared_file = new_step_dir / f"{RUN_FILE_NAME}.py"
        if checksum_file(latest_shared_file) != checksum_file(new_shared_file):
            print(
                f"WARNING: Shared module in version {step_latest_version} differs from new shared module."
            )


def get_path_to_step_files(channel: str, namespace: str = NAMESPACE) -> Path:
    """Get path to folder containing different versions of step files, for a given channel and namespace.

    Parameters
    ----------
    channel : str
        Channel name.
    namespace : str
        Namespace.

    Returns
    -------
    versions_dir : Path
        Path to folder of versions of step files.

    """
    if channel in ["meadow", "garden"]:
        versions_dir = STEP_DIR / "data" / channel / namespace
    elif channel == "grapher":
        versions_dir = STEP_DIR / channel / namespace
    else:
        raise ValueError("channel name not understood")

    return versions_dir


def create_steps(channel: str, step_names: List[str]) -> None:
    """Create all steps in a new version folder for a namespace in a channel.

    Parameters
    ----------
    channel : str
        Channel name.
    step_names : List[str]
        Names of steps to be created (e.g. 'faostat_qcl').

    """
    # Path to folder containing steps in this channel.
    versions_dir = get_path_to_step_files(channel=channel)
    # Latest version (taken from the name of the most recent folder).
    latest_version = find_latest_version_for_namespace_in_channel(channel=channel)
    # Path to folder containing code for steps in the latest version.
    latest_version_dir = versions_dir / latest_version
    # Path to folder to be created with new steps.
    new_version_dir = versions_dir / NEW_VERSION

    # Create folder.
    new_version_dir.mkdir()
    # Copy additional files from the latest to the new folder (if the files exist in the latest folder).
    for file in ADDITIONAL_FILES_TO_COPY:
        if list(latest_version_dir.glob(file)):
            latest_file = latest_version_dir / file
            new_file = new_version_dir / file
            new_file.write_text(latest_file.read_text())

    for step_name in step_names:
        create_step_file(channel=channel, step_name=step_name)


def create_dag_line_for_latest_natural_dependency(
    channel: str, step_name: str, namespace: str = NAMESPACE
) -> Optional[str]:
    """Create dag line for latest version of the natural dependency of a given step.

    Natural dependency refers to the analogous step from the previous channel. For example, for a dag line
    "data://garden/faostat/2022-05-05/faostat_qcl",
    the dag line of the latest natural dependency would be
    "data://meadow/faostat/2022-05-04/faostat_qcl".

    Parameters
    ----------
    channel : str
        Channel name.
    step_name : str
        Step name.
    namespace : str
        Namespace.

    Returns
    -------
    dependency_step : str or None
        Dag line of the natural dependency of the given step; or None if no natural dependency was found.

    """
    # Define the channel of the natural dependency.
    if channel == "meadow":
        dependency_channel = "walden"
    elif channel == "garden":
        dependency_channel = "meadow"
    elif channel == "grapher":
        dependency_channel = "garden"
    else:
        raise ValueError("wrong channel name")

    # Find latest version of the natural dependency for current step.
    dependency_version = find_latest_version_for_step(
        channel=dependency_channel, step_name=step_name, namespace=namespace
    )
    dependency_step: Optional[str] = None
    if dependency_version is not None:
        # Create dag line for the dependency.
        dependency_step = create_dag_line_name(
            channel=dependency_channel,
            step_name=step_name,
            namespace=namespace,
            version=dependency_version,
        )

    return dependency_step


def create_updated_dependency_graph(
    channel: str,
    step_names: List[str],
    namespace: str = NAMESPACE,
    new_version: str = NEW_VERSION,
    additional_dependencies: Optional[Dict[str, List[Tuple[str, str]]]] = None,
) -> Dict[str, Set[str]]:
    """Create additional part of the graph that will need be added to the dag to update it.

    Note: This function simply returns that part of the graph, without actually modifying the dag.

    Parameters
    ----------
    channel : str
        Channel name. Only steps from this channel will be considered for update (although dependencies can belong to
        other channels).
    step_names : List[str]
        Names of datasets that will be considered for an update.
    namespace : str
        Namespace.
    new_version : str
        New version of the considered steps.
    additional_dependencies : dict or None
        Additional dependencies to add to any dag line of a particular channel.

    Returns
    -------
    new_steps : Dict[str, set]
        Additional part of the dependency graph (that will be added to the dag).

    """
    # Load dag from file.
    dag = load_dag()

    # Initialise the additional part of the graph that will have to be added to the dag (in another function).
    new_steps = {}
    for step_name in step_names:
        # Find all occurrence in the dag of this dataset for the considered channel.
        candidates = {
            step: dag[step]
            for step in dag
            if step_name == get_dataset_name_from_dag_line(step)
            if get_channel_from_dag_line(step) == channel
        }

        if len(candidates) == 0:
            # For this dataset there is no dag line yet. Create a new one.
            new_step_name = create_dag_line_name(
                channel=channel,
                step_name=step_name,
                namespace=namespace,
                version=new_version,
            )
            new_dependencies = []
            # Create dag lines for its natural dependency, if any is found.
            natural_dependency = create_dag_line_for_latest_natural_dependency(
                channel=channel, step_name=step_name
            )
            if natural_dependency is not None:
                new_dependencies.append(natural_dependency)
            if additional_dependencies is not None:
                # Optionally include additional dependencies.
                for additional_dependency in additional_dependencies[channel]:
                    dependency_channel, dependency_step = additional_dependency
                    dependency_version = find_latest_version_for_step(
                        channel=dependency_channel,
                        step_name=dependency_step,
                        namespace=namespace,
                    )
                    if dependency_version is not None:
                        new_dependencies.append(
                            create_dag_line_name(
                                channel=dependency_channel,
                                step_name=dependency_step,
                                namespace=namespace,
                                version=dependency_version,
                            )
                        )
        else:
            # Identify the latest version of the dataset in the dag. That will be the step to be updated.
            latest_version = sorted(
                [get_version_from_dag_line(candidate) for candidate in candidates]
            )[-1]
            step_to_update = {
                step: dag[step]
                for step in candidates
                if get_version_from_dag_line(step) == latest_version
            }
            assert len(list(step_to_update)) == 1
            step_name = list(step_to_update)[0]
            # Gather the dependencies for that dataset.
            dependencies = step_to_update[step_name]
            # Change the name of the step to refer to the new version of the dataset.
            new_step_name = step_name.replace(latest_version, new_version)
            # Update the version of each of the dataset dependencies.
            new_dependencies = []
            for dependency in dependencies:
                if Path(dependency).parent.name == "latest":
                    # This step that is always up-to-date, so the new dependency will be the same as the current one.
                    new_dependency = dependency
                else:
                    # Get old dependency version from the old dag line.
                    dependency_old_version = get_version_from_dag_line(dependency)
                    # Find the latest existing version of that dependency.
                    dependency_new_version = find_latest_version_for_step(
                        channel=get_channel_from_dag_line(dependency),
                        step_name=get_dataset_name_from_dag_line(dependency),
                    )
                    # Rename the dag line of the dependency appropriately (if its version changed).
                    new_dependency = dependency.replace(dependency_old_version, dependency_new_version)
                new_dependencies.append(new_dependency)

        if len(new_dependencies) > 0:
            # Collect the new dag line and its dependencies, that will later be added to the updated dag.
            new_steps[new_step_name] = set(new_dependencies)

    return new_steps


def update_food_explorer_dependency_version() -> None:
    """Ensure the dependency of the food explorer corresponds to the latest version of the garden dataset of the food
    explorer.

    """
    # Load the full content of the dag.
    with open(DAG_FILE, "r") as _dag_file:
        dag_lines = _dag_file.read()

    # Exact dag line for the OWID food explorer.
    dag_line_explorer = "data://explorer/owid/latest/food_explorer"
    # Find the latest version of the FAOSTAT food explorer dataset in garden.
    new_version = find_latest_version_for_step(
        channel="garden", step_name="faostat_food_explorer"
    )
    if new_version is None:
        raise FileNotFoundError("Food explorer step file not found.")
    # To begin with, assume old version is identical to new.
    old_version = new_version
    # Find dag line for the food explorer, and replace the following line with the new version of the garden dataset.
    replace_line = False
    new_lines = ""
    for line in dag_lines.split("\n"):
        if replace_line:
            # Current line corresponds to the dependency of the food explorer.
            # Get version of the dependency of the food explorer that is written currently in the dag.
            old_version = get_version_from_dag_line(line)
            # Replace that version with the latest one.
            new_lines += line.replace(old_version, new_version) + "\n"
            replace_line = False
        else:
            new_lines += line + "\n"
        if dag_line_explorer in line:
            # The next line is the one corresponding to the dependency of the food explorer.
            replace_line = True

    if old_version != new_version:
        print("Updating version of the dependency dataset of the food explorer.")
        # Write new lines to dag file.
        with open(DAG_FILE, "w") as _dag_file:
            _dag_file.write(new_lines[:-1])


def write_steps_to_dag_file(
    dag_steps: Dict[str, Set[str]], header_line: Optional[str]
) -> None:
    """Add new lines to the dag, given a graph of additional dependencies.

    Parameters
    ----------
    dag_steps : Dict[str, set]
        Additional dependencies to be added to the graph.
    header_line : Optional[str]
        Line to be written in the dag as a comment before the new part of the graph.

    """
    # Indentation of steps in the dag file.
    step_indent = "  "
    dependency_indent = "    - "

    # Initialise string that will be added at the end of the dag file.
    new_step_lines = ""
    if header_line:
        new_step_lines += f"{step_indent}#\n"
        new_step_lines += f"{step_indent}{header_line}\n"
        new_step_lines += f"{step_indent}#\n"

    # Load dag from file.
    dag = load_dag()

    # Initialise a flag that is True only if at least one step has to be written to the dag.
    any_step_updated = False
    for dag_step in dag_steps:
        # Add new step to the dag if the step is not already there.
        if dag_step in dag:
            print(f"Dag step {dag_step} already in dag. Skipping.")
        else:
            # Add lines for this step and its dependencies.
            any_step_updated = True
            new_step_lines += f"{step_indent}{dag_step}:\n"
            for dependency in dag_steps[dag_step]:
                new_step_lines += f"{dependency_indent}{dependency}\n"

    if any_step_updated:
        print("Writing new steps to dag file.")
        # Add new lines to dag file.
        with open(DAG_FILE, "a") as _dag_file:
            _dag_file.write(new_step_lines)

        # Ensure the dependency of the food explorer corresponds is the latest version.
        update_food_explorer_dependency_version()


def apply_custom_rules_to_list_of_steps_to_run(step_names, channel):
    # In garden or grapher, if fbs or fbsh were updated, update fbsc (but omit steps for fbs and fbsh).
    if (channel in ["garden", "grapher"]) and (any({f"{NAMESPACE}_fbs", f"{NAMESPACE}_fbsh"} & set(step_names))):
        step_names += [f"{NAMESPACE}_fbsc"]
        step_names = [step for step in step_names if step not in [f"{NAMESPACE}_fbs", f"{NAMESPACE}_fbsh"]]

    # In garden, if either fbsc or qcl were updated, update food explorer.
    if (channel == "garden") and (any({f"{NAMESPACE}_fbsc", f"{NAMESPACE}_qcl"} & set(step_names))):
        step_names += [f"{NAMESPACE}_food_explorer"]

    # In grapher there is never a step for metadata.
    if channel == "grapher":
        step_names = [step for step in step_names if step not in [f"{NAMESPACE}_metadata"]]

    return step_names


def main(channel: str, include_all_datasets: bool = False) -> None:
    if include_all_datasets:
        # List all datasets, even if their source data was not updated.
        step_names = list_all_steps()
    else:
        # List steps for which source data was updated.
        step_names = list_updated_steps(channel=channel)

        # Apply custom rules to list of steps to run.
        step_names = apply_custom_rules_to_list_of_steps_to_run(step_names=step_names, channel=channel)

    if len(step_names) > 0:
        # Create folder for new version and add a step file for each dataset.
        create_steps(channel=channel, step_names=step_names)

        # Generate dictionary of new step dependencies.
        dag_steps = create_updated_dependency_graph(
            channel=channel,
            step_names=step_names,
            additional_dependencies=ADDITIONAL_DEPENDENCIES,
        )

        # Update dag file with new dependencies.
        header_line = f"# FAOSTAT {channel} steps for version {NEW_VERSION}"
        write_steps_to_dag_file(dag_steps=dag_steps, header_line=header_line)
    else:
        print("Nothing to update.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "-c",
        "--channel",
        help="Name of channel where new step will be created (either meadow or garden).",
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
