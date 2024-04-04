from pathlib import Path
from typing import List, Optional, Union

import click
from rich_click.rich_command import RichCommand
from structlog import get_logger
from tqdm.auto import tqdm

from etl.db import get_variables_data
from etl.paths import BASE_DIR
from etl.version_tracker import VersionTracker

# Initialize logger.
log = get_logger()

# Default path to the explorers folder.
EXPLORERS_DIR = BASE_DIR.parent / "owid-content/explorers"

# URL of the data catalog.
CATALOG_URL = "https://catalog.ourworldindata.org/"


def extract_variable_ids_from_explorer_content(explorer: str) -> List[int]:
    variable_ids_all = []
    for line in explorer.split("\n"):
        # Select the lines that correspond to subtables.
        if line.startswith("\t"):
            # Skip lines of subtable headers (that contain strings "yVariableIds" and "variableId").
            if "ariableId" in line:
                continue
            else:
                # Assume the first column contains variable ids (one or more), separated by tabs.
                variable_ids_all.append(line.lstrip().split("\t")[0])

    # Extract all substrings of digits that should be variable ids.
    variable_ids = sorted(
        set(
            [
                int(variable_id)
                for line in variable_ids_all
                for variable_id in line.split()
                if variable_id.isdigit() and len(variable_id) >= 5
            ]
        )
    )

    return variable_ids


def update_file_based_explorer(explorer: str) -> Optional[str]:
    # Gather all data catalog URLs from the explorer content.
    steps = []
    for line in explorer.split("\n"):
        if CATALOG_URL in line:
            urls = [substring for substring in line.split() if CATALOG_URL in substring]
            steps.extend(["/".join(url.replace(CATALOG_URL, "data://").split("/")[:-1]) for url in urls])
    steps = sorted(set(steps))

    # Filter out steps with version "latest", as they are not updateable.
    updateable_steps = [step for step in steps if step.split("/")[-2] != "latest"]

    if len(updateable_steps) == 0:
        log.info("No URLs to update.")
        return None

    # Load dataframe of all steps in ETL.
    steps_df = VersionTracker().steps_df

    # Select steps whose version is not the latest possible.
    updateable_df = steps_df[
        steps_df["step"].isin(updateable_steps) & (steps_df["step"] != steps_df["same_steps_latest"])
    ].reset_index(drop=True)

    if updateable_df.empty:
        log.info("No URLs to update.")
        return None

    # Create columns for the old and new urls.
    updateable_df["url_old"] = [step.replace("data://", CATALOG_URL) for step in updateable_df["step"]]
    updateable_df["url_new"] = [step.replace("data://", CATALOG_URL) for step in updateable_df["same_steps_latest"]]

    # Dictionary mapping old to new urls.
    url_mapping = updateable_df[["url_old", "url_new"]].set_index("url_old")["url_new"].to_dict()

    # Replace old urls with new ones in the explorer content.
    explorer_new = explorer
    # Create an informative log message.
    message = "URLs to be replaced in explorer file:\n"
    for url_old, url_new in url_mapping.items():
        assert url_new.split("/")[-2] > url_old.split("/")[-2], "New version should be greater than old version."  # type: ignore
        message += f"{url_old} ->\n{url_new})\n"
        explorer_new = explorer_new.replace(url_old, url_new)  # type: ignore
    log.info(message)

    return explorer_new


def update_indicator_based_explorer(explorer: str) -> Optional[str]:
    # Extract all possible variable ids from the explorer file.
    variable_ids = extract_variable_ids_from_explorer_content(explorer=explorer)

    if len(variable_ids) == 0:
        log.info("No variable ids found in the explorer file. This may not be an indicator-based explorer.")
        return None

    # Fetch data for these variables from the database, if possible.
    variable_data = get_variables_data(filter={"id": variable_ids})

    # Warn about those variable ids that don't have a catalogPath (and therefore may not be created by ETL).
    variables_wihout_path = set(variable_data[variable_data["catalogPath"].isnull()]["id"])
    if len(variables_wihout_path) > 0:
        log.warning(f"Variable ids without catalogPath: {variables_wihout_path}\nThey may need to be updated manually.")
        variable_data = variable_data[variable_data["catalogPath"].notnull()].reset_index(drop=True)

    if len(variable_data) == 0:
        log.info("No variable ids can be automatically updated.")
        return None

    # Select relevant columns.
    variables_df = variable_data[["id", "catalogPath", "name"]].copy()

    # Create a column with the step name of each variable's grapher dataset.
    variables_df["step"] = [
        "data://" + "/".join(etl_path.split("#")[0].split("/")[:-1]) for etl_path in variables_df["catalogPath"]
    ]

    # Create a column with the table name and variable name.
    variables_df["table_and_variable_short_name"] = [path.split("/")[4] for path in variables_df["catalogPath"]]

    # Load dataframe of all steps in ETL.
    steps_df = VersionTracker().steps_df

    # Steps in the variables table are stored as, e.g. "grapher/faostat/2023-06-12/faostat_qcl/...",
    # so we don't know if they are all "data://..." or they could also be "data-private://...".
    # For now, assume all steps are public, hence using the prefix "data://".
    missing_steps = set(variables_df["step"]) - set(steps_df["step"])
    if len(missing_steps) > 0:
        private_steps = [
            step for step in missing_steps if step.replace("data://", "data-private://") in set(steps_df["step"])
        ]
        other_missing_steps = missing_steps - set(private_steps)
        if len(private_steps) > 0:
            log.warning(
                f"Unexpected private steps found in explorer: {private_steps}\nUnclear if this should be allowed."
            )
        if len(other_missing_steps) > 0:
            log.warning(f"Steps not found in ETL: {missing_steps}\nThey may have been accidentally archived.")
        variables_df = variables_df[~variables_df["step"].isin(missing_steps)].reset_index(drop=True)

    # Combine variable data with steps data.
    variables_df = variables_df.merge(steps_df[["step", "same_steps_latest", "version"]], on="step", how="left")

    # Select variables that can be updated.
    updateable = variables_df[variables_df["step"] != variables_df["same_steps_latest"]].reset_index(drop=True)[
        ["id", "name", "version", "step", "table_and_variable_short_name", "same_steps_latest"]
    ]

    # Create a column for the "catalogPath" of the latest steps.
    updateable["catalogPath_new"] = [
        variable["same_steps_latest"].replace("data://", "") + "/" + variable["table_and_variable_short_name"]
        for _, variable in updateable.iterrows()
    ]

    # Create a column for the new version of the latest steps.
    updateable["version_new"] = [variable["catalogPath_new"].split("/")[2] for _, variable in updateable.iterrows()]

    if len(updateable) == 0:
        log.info("No variables to update.")
        return None

    # Fetch variable data for the latest steps from the database.
    variable_data_new = get_variables_data(filter={"catalogPath": updateable["catalogPath_new"].tolist()})

    # Select and rename columns.
    variable_data_new = variable_data_new.rename(
        columns={"id": "id_new", "catalogPath": "catalogPath_new", "name": "name_new"}, errors="raise"
    )[["id_new", "catalogPath_new", "name_new"]]

    # Combine old and new variable data.
    combined = updateable.merge(variable_data_new, on="catalogPath_new", how="left")

    # Create a dictionary mapping old to new variables.
    variable_mapping = combined.set_index("id")["id_new"].to_dict()

    # Ensure variable ids are only replaced in the first column of subtables.
    explorer_new = ""
    for line in explorer.split("\n"):
        new_line = line
        for variable_id_old, variable_id_new in variable_mapping.items():
            # Replace variable ids only if they are in the first column (i.e. preceded and followed by tabs).
            if (
                (str(variable_id_old) in new_line)
                and new_line.startswith("\t")
                and ("\t" in new_line.split(str(variable_id_old))[-1])
            ):
                new_line = new_line.replace(str(variable_id_old), str(variable_id_new))
        explorer_new += new_line + "\n"

    # Warn if old variable ids are still found in the new explorer content.
    variable_ids_in_new_explorer = extract_variable_ids_from_explorer_content(explorer=explorer_new)

    old_variables_remaining = [
        old_variable_id for old_variable_id in set(variable_mapping) if old_variable_id in variable_ids_in_new_explorer
    ]

    if len(old_variables_remaining) > 0:
        log.warning(f"Old variable ids cannot be replaced in the new explorer content: {old_variables_remaining}")

    message = "Variable ids (and their ETL version) to be replaced in explorer file:\n"
    for _, row in combined.iterrows():
        assert int(row["id_new"]) > int(row["id"]), "New variable id should be greater than old variable id."
        assert row["version_new"] > row["version"], "New version should be greater than old version."
        message += f"{row['id']} ({row['version']}) -> {row['id_new']} ({row['version_new']})\n"
        if row["name"] != row["name_new"]:
            message += "  Name change:\n"
            message += f"  {row['name']} ->\n  {row['name_new']}\n"
    log.info(message)

    return explorer_new


def update_explorer(explorer_file: Path, dry_run: bool = False) -> None:
    """Update a single explorer file."""

    if not explorer_file.is_file():
        raise FileNotFoundError(f"Explorer file not found: {explorer_file}")

    # Load explorer file as a string.
    with open(explorer_file, "r") as f:
        explorer = f.read()

    # Initialize variable for the updated explorer content.
    explorer_new = None

    # TODO: Future improvements based on Sophia's inputs:
    #  * To follow the same order as grapher, first check for indicator-based, then file-based, then other explorers
    #    (although this will probably make no difference).
    #  * The correct way to know if an explorer is file-based is to check if there is a `table` keyword in the very
    #    first column of the explorer config.
    #  * Explorers don't care about the order of columns, so technically, it's not safe to assume that variable IDs are
    #    always in the first column.
    if ("yVariableIds" not in explorer) and (CATALOG_URL not in explorer):
        log.info("Nothing to update (it may be a grapher-based explorer).")

        return None

    elif (CATALOG_URL in explorer) and ("yVariableIds" not in explorer):
        log.info(f"Inspecting file-based explorer: {explorer_file.stem}")

        explorer_new = update_file_based_explorer(explorer=explorer)

    elif ("yVariableIds" in explorer) and (CATALOG_URL not in explorer):
        log.info(f"Inspecting indicator-based explorer: {explorer_file.stem}")

        explorer_new = update_indicator_based_explorer(explorer=explorer)

    else:
        # Explorer file may be both file-based and indicator-based.
        # If this is possible, the code could easily be adapted to update first urls and then variable ids.
        log.warning(f"Unexpected content in explorer file: {explorer_file.stem}")

    if (explorer_new is not None) and (not dry_run):
        # Write to explorer file, to update its content.
        with open(explorer_file, "w") as f:
            f.write(explorer_new)


@click.command(name="explorer-update", cls=RichCommand, help=__doc__)
@click.argument("explorer-names", type=str or List[str], nargs=-1)
@click.option(
    "--explorers-dir", type=str, default=EXPLORERS_DIR, help=f"Path to explorer files. Default: {EXPLORERS_DIR}"
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    type=bool,
    help="Do not write to explorer files, simply print potential changes. Default: False.",
)
def cli(
    explorer_names: Optional[Union[List[str], str]] = None,
    explorers_dir: Union[Path, str] = EXPLORERS_DIR,
    dry_run: bool = False,
) -> None:
    """Update one or more explorer (tsv) files.

    This command will update the content of one or more explorer (tsv) files, with the following logic:
    - If it is a file-based explorer, ensure URLs to data catalog point to the latest version of the data.
    - If it is an indicator-based explorer, ensure variable ids correspond to the latest versions of the variables.

    """

    if isinstance(explorers_dir, str):
        # Ensure explorer folder is a Path object.
        explorers_dir = Path(explorers_dir)

    if not explorers_dir.is_dir():
        raise NotADirectoryError(f"Explorer directory not found: {explorers_dir}")

    if explorer_names is None or len(explorer_names) == 0:
        # If no explorer name is provided, update all explorers in the folder.
        explorer_names = [explorer_file.stem for explorer_file in sorted(explorers_dir.glob("*.tsv"))]
    if isinstance(explorer_names, str):
        # If only one explorer name is provided, convert it into a list.
        explorer_names = [explorer_names]

    for explorer_name in tqdm(explorer_names):
        # Define path to explorer file.
        explorer_file = (explorers_dir / explorer_name).with_suffix(".explorer.tsv")

        # Update variable ids in the explorer file.
        update_explorer(explorer_file=explorer_file, dry_run=dry_run)
