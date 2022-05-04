"""Create new version of FAOSTAT steps in meadow or garden.

Workflow to create a new version of the FAOSTAT dataset (with version YYYY-MM-DD):
1. Execute this script for the meadow channel.
  > python etl/scripts/faostat/create_new_steps.py -w meadow
2. Run the new etl meadow steps.
  > etl meadow/faostat/YYYY-MM-DD
3. Run this script again for the garden channel.
4. Run the new etl garden steps.
  > etl garden/faostat/YYYY-MM-DD

This script will, for a given channel:
* Create a new folder in the channel (with today's date) and copy step files from the latest version into that folder.
* Add all new steps to the dag file.

"""

# TODO:
#  * Decide what to do with grapher steps.
#  * Implement the same logic for garden steps.
#  * Every time this script is executed, it will create a new step file for each domain, even if the original data
#    (in walden) did not change. We could consider improving this, so that step files are created only if there was an
#    update in the corresponding source data. Over time we may end up with different steps relying on different
#    versions of shared modules, which can be hard to maintain. I think it would still be better than creating all
#    steps every time (because this implies eventually updating all charts every time). A possible solution would be:
#    * list_dataset_codes() list only dataset codes that were updated.
#    * Some refactor of other functions so that files are not copied from latest version, but each step file is copied
#      from its corresponding latest version. To do this, first copy the latest shared module. Then, list step that
#      were updated. For each of them, find their latest version, and copy it to the new folder. And raise warning if
#      the md5 of new shared module differs from the md5 of the shared module of that latest version.


import argparse
import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import yaml
from owid.walden import Catalog

from etl.paths import DAG_FILE, STEP_DIR

NAMESPACE = "faostat"
# Base name of additional metadata file.
ADDITIONAL_METADATA_FILE_NAME = f"{NAMESPACE}_metadata"
RUN_FILE_NAME = "shared"
# Glob pattern to match version folders like "YYYY-MM-DD". Note: This is not a regular expression.
GLOB_VERSION_PATTERN = "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]"


def list_dataset_codes() -> List[str]:
    """List dataset codes (e.g. domains) that were considered for update in the latest walden ingest.

    Note: This will list all domains, even if not all of them were updated in the latest ingest.

    Returns
    -------
    domains : List[str]
        Domains.

    """
    # Load walden dataset.
    walden_ds = Catalog().find_latest(
        namespace=NAMESPACE, short_name=ADDITIONAL_METADATA_FILE_NAME
    )
    domains = pd.read_json(walden_ds.ensure_downloaded()).columns.tolist()

    return domains


def list_all_steps() -> List[str]:
    """List all steps (e.g. 'faostat_qcl') to be created in a channel, based on the list of domains considered in the
    latest walden ingest.

    Note: The metadata step is always added as a step.

    Returns
    -------
    step_names : List[str]
        Names of steps.

    """
    # List all steps based on the dataset codes that were considered in the latest walden ingest.
    domains = list_dataset_codes()
    step_names = [f"{NAMESPACE}_{domain}" for domain in domains]
    # Add metadata step to the list.
    step_names += [ADDITIONAL_METADATA_FILE_NAME]
    step_names = sorted(step_names)

    return step_names


def create_steps_from_scratch(new_version_dir: Path, latest_version_dir: Path, domains: List[str]) -> None:
    """Generate step files inside a given folder, from scratch, i.e. using minimal code that imports a common 'run'
    function from a shared module.

    Notes:
    * This function should be used only once for each channel, unless possibly a new refactor takes place.
    * Shared module and metadata step will be copied from latest version.

    Parameters
    ----------
    new_version_dir : Path
        Path to new version folder, where files will be created.
    latest_version_dir : Path
        Path to latest version folder. This is necessary to copy the latest shared module.
    domains : List[str]
        Domains to consider

    """
    # Copy run file from latest step to new step.
    latest_run_file = latest_version_dir / (RUN_FILE_NAME + ".py")
    new_run_file = new_version_dir / (RUN_FILE_NAME + ".py")
    new_run_file.write_text(latest_run_file.read_text())

    # Copy additional metadata file from latest step to new step.
    latest_metadata_file = latest_version_dir / (
            ADDITIONAL_METADATA_FILE_NAME + ".py"
    )
    new_metadata_file = new_version_dir / (
            ADDITIONAL_METADATA_FILE_NAME + ".py"
    )
    new_metadata_file.write_text(latest_metadata_file.read_text())

    # Write the same import line in each of the step files.
    file_content = "from .shared import run\n"
    for domain in domains:
        new_step_file = new_version_dir / f"{NAMESPACE}_{domain}.py"
        new_step_file.write_text(file_content)


def create_steps_from_latest_version(new_version_dir: Path, latest_version_dir: Path) -> None:
    # Copy all code files from latest to new version.
    for old_file_path in latest_version_dir.glob("*.py"):
        new_file_path = new_version_dir / old_file_path.name
        # Copy all files from latest step to new step.
        new_file_path.write_text(old_file_path.read_text())


def create_new_namespace_version(
    new_version_dir: Path,
    latest_version_dir: Path,
    domains: List[str],
    create_from_scratch: bool = False,
) -> None:
    if new_version_dir.is_dir():
        print(
            f"Steps for new version already exist (remove folder {new_version_dir} and re-run). Skipping."
        )
    else:
        print(f"Creating steps for new version: {new_version_dir}")
        # Create new folder.
        new_version_dir.mkdir()

        if create_from_scratch:
            create_steps_from_scratch(
                new_version_dir=new_version_dir, latest_version_dir=latest_version_dir, domains=domains)
        else:
            create_steps_from_latest_version(new_version_dir=new_version_dir, latest_version_dir=latest_version_dir)


def create_dependency_graph(channel: str, new_version: str) -> Dict[str, str]:
    # Gather all step names.
    step_names = list_all_steps()

    dag_steps = {}
    for step_name in step_names:
        # Define new step in dag.
        dag_step = f"data://{channel}/{NAMESPACE}/{new_version}/{step_name}"

        if channel == "meadow":
            # Find latest walden version for current step.
            walden_version = (
                Catalog().find_latest(namespace=NAMESPACE, short_name=step_name).version
            )
            # Define dependencies for new step.
            dependency_step = f"walden://{NAMESPACE}/{walden_version}/{step_name}"
        elif channel == "garden":
            # TODO: add dependencies from meadow. Also, create a dictionary for manual dependencies (like fbsc).
            raise NotImplemented
        else:
            raise ValueError("channel must be meadow or garden")

        dag_steps[dag_step] = dependency_step

    return dag_steps


def write_steps_to_dag_file(
    dag_steps: Dict[str, str], header_line: Optional[str]
) -> None:
    # Indentation of steps in the dag file.
    step_indent = "  "
    dependency_indent = "    - "
    new_step_lines = ""
    if header_line:
        new_step_lines += f"{step_indent}#\n"
        new_step_lines += f"{step_indent}{header_line}\n"
        new_step_lines += f"{step_indent}#\n"

    # Load dag.
    with open(DAG_FILE) as _dag_file:
        dag = yaml.safe_load(_dag_file)

    # Initialise a flag that is True only if at least one step has to be written to dag.
    any_step_updated = False
    for dag_step in dag_steps:
        # Add new step to dag if not already there.
        if dag_step in dag["steps"]:
            print(f"Dag step {dag_step} already in dag. Skipping.")
        else:
            any_step_updated = True
            new_step_lines += f"{step_indent}{dag_step}:\n"
            new_step_lines += f"{dependency_indent}{dag_steps[dag_step]}\n"

    if any_step_updated:
        print("Writing new steps to dag file.")
        # Add new lines to dag file.
        with open(DAG_FILE, "a") as _dag_file:
            _dag_file.write(new_step_lines)


def main(channel: str, create_from_scratch: bool = False) -> None:
    # Path to folder containing steps in this channel.
    versions_dir = STEP_DIR / "data" / channel / NAMESPACE
    # Latest version (taken from the name of the most recent folder).
    latest_version = sorted(list(versions_dir.glob(GLOB_VERSION_PATTERN)))[-1].name
    # New version of steps to create.
    new_version = datetime.datetime.today().strftime("%Y-%m-%d")
    # Path to folder containing code for steps in the latest version.
    latest_version_dir = versions_dir / latest_version
    # Path to folder to be created with new steps.
    new_version_dir = versions_dir / new_version

    if latest_version == new_version:
        print(f"Dataset is already up-to-date with version: {new_version}")
    else:
        # List all FAOSTAT dataset codes (domains) that are relevant.
        domains = list_dataset_codes()

        # Create folder for new version and add a file for each step.
        create_new_namespace_version(
            new_version_dir=new_version_dir,
            latest_version_dir=latest_version_dir,
            domains=domains,
            create_from_scratch=create_from_scratch,
        )

        # Generate dictionary of step dependencies.
        dag_steps = create_dependency_graph(channel=channel, new_version=new_version)

        # Update dag file.
        header_line = f"# FAOSTAT {channel} steps for version {new_version}"
        write_steps_to_dag_file(dag_steps=dag_steps, header_line=header_line)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "-w",
        "--channel",
        help=f"Name of channel where new step will be created (either meadow or garden).",
        required=True,
    )
    parser.add_argument(
        "-s",
        "--create_from_scratch",
        default=False,
        action="store_true",
        help="If given, create all files in new step from scratch. Otherwise copy them from latest step.",
    )
    args = parser.parse_args()
    main(
        channel=args.channel,
        create_from_scratch=args.create_from_scratch,
    )
