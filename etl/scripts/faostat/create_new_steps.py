"""Create new version of FAOSTAT steps in meadow or garden.

"""

import argparse
import datetime
import shutil
from pathlib import Path

import pandas as pd
from owid import catalog

from etl.paths import DATA_DIR, STEP_DIR

NAMESPACE = "faostat"
# Base name of additional metadata file.
ADDITIONAL_METADATA_FILE_NAME = f"{NAMESPACE}_metadata"
RUN_FILE_NAME = "shared"


def create_steps_for_new_version(
    new_version_dir: Path,
    latest_version_dir: Path,
    latest_metadata_file: Path,
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
            # Get list of domains from additional metadata generated in latest version.
            additional_metadata = catalog.Dataset(latest_metadata_file)
            domains = pd.unique(
                [
                    table_name.split("_")[1]
                    for table_name in additional_metadata.table_names
                ]
            ).tolist()

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


def main(workspace: str, create_all_files_from_scratch: bool = False) -> None:
    # Path to folder containing steps in this workspace.
    versions_dir = STEP_DIR / "data" / workspace / NAMESPACE
    # Latest version (taken from the name of the most recent folder).
    latest_version = sorted(list(versions_dir.glob("2*")))[-1].name
    # New version of steps to create.
    new_version = datetime.datetime.today().strftime("%Y-%m-%d")
    # Path to folder containing data files produced in the latest version.
    files_dir = DATA_DIR / workspace / NAMESPACE / latest_version
    # Path to additional metadata file from latest version.
    latest_metadata_file = files_dir / ADDITIONAL_METADATA_FILE_NAME
    # Path to folder containing code for steps in the latest version.
    latest_version_dir = versions_dir / latest_version
    # Path to folder to be created with new steps.
    new_version_dir = versions_dir / new_version

    create_steps_for_new_version(
        new_version_dir=new_version_dir,
        latest_version_dir=latest_version_dir,
        latest_metadata_file=latest_metadata_file,
        create_all_files_from_scratch=create_all_files_from_scratch,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "-w",
        "--workspace",
        help=f"Name of workspace where new step will be created (either meadow or garden).",
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
