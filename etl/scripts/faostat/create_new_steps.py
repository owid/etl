"""Create new version of FAOSTAT steps in meadow or garden.

Workflow to create a new version of the FAOSTAT dataset (with version YYYY-MM-DD):
1. Execute this script for the meadow workspace.
  > python etl/scripts/faostat/create_new_steps.py -w meadow
2. Run the new etl meadow steps.
  > etl meadow/faostat/YYYY-MM-DD
3. Run this script again for the garden workspace.
4. Run the new etl garden steps.
  > etl garden/faostat/YYYY-MM-DD
5. TODO: Decide what to do with grapher steps.

This script will, for a given workspace:
* Create a new folder in the workspace (with today's date) and copy all step files from the latest version into the folder.
  * This means that every time it is executed, it will create a new step file for each domain, even if the
    original data (in walden) did not change.
    TODO: Consider improving this, so that step files are created only if there was an update.
* Add all new steps to the dag file.

TODO: Implement the same logic for garden steps.

"""

import argparse
import datetime
import shutil
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import yaml
from owid import catalog
from owid.walden import Catalog

from etl.paths import DAG_FILE, DATA_DIR, STEP_DIR

NAMESPACE = "faostat"
# Base name of additional metadata file.
ADDITIONAL_METADATA_FILE_NAME = f"{NAMESPACE}_metadata"
RUN_FILE_NAME = "shared"


def list_dataset_codes(data_files_dir: Path) -> List[Path]:
    # Path to additional metadata file name (without extension).
    metadata_file_name = data_files_dir / ADDITIONAL_METADATA_FILE_NAME
    # Get list of dataset codes (or domains) from additional metadata.
    additional_metadata = catalog.Dataset(metadata_file_name)
    domains = pd.unique(
        [table_name.split("_")[1] for table_name in additional_metadata.table_names]
    ).tolist()

    return domains


def create_steps_for_new_version(
    new_version_dir: Path,
    latest_version_dir: Path,
    domains: List[Path],
    create_all_files_from_scratch: bool = False,
) -> None:
    if new_version_dir.is_dir():
        print(
            f"Steps for new version already exist (remove folder {new_version_dir} and re-run). Skipping."
        )
    else:
        print(f"Creating steps for new version: {new_version_dir}")
        new_version_dir.mkdir()

        if create_all_files_from_scratch:
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
        else:
            # Copy all code files from latest to new version.
            for old_file_path in latest_version_dir.glob("*.py"):
                new_file_path = new_version_dir / old_file_path.name
                # Copy all files from latest step to new step.
                shutil.copy(old_file_path, new_file_path)


def list_all_steps(data_files_dir: Path) -> List[str]:
    domains = list_dataset_codes(data_files_dir=data_files_dir)
    step_names = [f"{NAMESPACE}_{domain}" for domain in domains]
    # Add metadata step to the list.
    step_names += [ADDITIONAL_METADATA_FILE_NAME]
    step_names = sorted(step_names)

    return step_names


def create_dependency_graph(
    workspace: str, new_version: str, domains: List[Path]
) -> Dict[str, str]:
    # Gather all step names.
    step_names = [f"{NAMESPACE}_{domain}" for domain in domains]
    # Add metadata step to the list.
    step_names += [ADDITIONAL_METADATA_FILE_NAME]
    step_names = sorted(step_names)

    dag_steps = {}
    for step_name in step_names:
        # Define new step in dag.
        dag_step = f"data://{workspace}/{NAMESPACE}/{new_version}/{step_name}"

        if workspace == "meadow":
            # Find latest walden version for current step.
            walden_version = (
                Catalog().find_latest(namespace=NAMESPACE, short_name=step_name).version
            )
            # Define dependencies for new step.
            dependency_step = f"walden://{NAMESPACE}/{walden_version}/{step_name}"
        elif workspace == "garden":
            # TODO: add dependencies from meadow. Also, create a dictionary for manual dependencies (like fbsc).
            dependency_step = ""
            raise NotImplemented
        else:
            raise ValueError("workspace must be meadow or garden")

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


def main(workspace: str, create_all_files_from_scratch: bool = False) -> None:
    # Path to folder containing steps in this workspace.
    versions_dir = STEP_DIR / "data" / workspace / NAMESPACE
    # Latest version (taken from the name of the most recent folder).
    latest_version = sorted(list(versions_dir.glob("2*")))[-1].name
    # New version of steps to create.
    new_version = datetime.datetime.today().strftime("%Y-%m-%d")
    # Path to folder containing data files produced in the latest version.
    latest_files_dir = DATA_DIR / workspace / NAMESPACE / latest_version
    # Path to folder containing code for steps in the latest version.
    latest_version_dir = versions_dir / latest_version
    # Path to folder to be created with new steps.
    new_version_dir = versions_dir / new_version

    if latest_version == new_version:
        print(f"Dataset is already up-to-date with version: {new_version}")
    else:
        # List all FAOSTAT dataset codes (domains) that are relevant.
        domains = list_dataset_codes(data_files_dir=latest_files_dir)

        # Create folder for new version and add a file for each step.
        create_steps_for_new_version(
            new_version_dir=new_version_dir,
            latest_version_dir=latest_version_dir,
            domains=domains,
            create_all_files_from_scratch=create_all_files_from_scratch,
        )

        # Generate dictionary of step dependencies.
        dag_steps = create_dependency_graph(
            workspace=workspace, new_version=new_version, domains=domains
        )

        # Update dag file.
        header_line = f"# FAOSTAT {workspace} steps for version {new_version}"
        write_steps_to_dag_file(dag_steps=dag_steps, header_line=header_line)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "-w",
        "--workspace",
        help=f"Name of workspace where new step will be created (either meadow or garden).",
        required=True,
    )
    parser.add_argument(
        "-s",
        "--create_all_files_from_scratch",
        default=False,
        action="store_true",
        help="If given, create all files in new step from scratch. Otherwise copy them from latest step.",
    )
    args = parser.parse_args()
    main(
        workspace=args.workspace,
        create_all_files_from_scratch=args.create_all_files_from_scratch,
    )
