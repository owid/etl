import re
from pathlib import Path
from typing import List, Optional, Union

import click
from rich_click.rich_command import RichCommand
from structlog import get_logger

from etl.db import get_variables_data
from etl.paths import BASE_DIR
from etl.version_tracker import VersionTracker

# Initialize logger.
log = get_logger()

# Default path to the explorers folder.
EXPLORERS_DIR = BASE_DIR.parent / "owid-content/explorers"


def extract_variable_ids_from_explorer_content(explorer: str) -> List[int]:
    variable_ids_all = ""
    for line in explorer.split("\n"):
        # Select the lines that correspond to subtables.
        if line.startswith("\t"):
            # Skip lines of subtable headers (that contain strings "yVariableIds" and "variableId").
            if "ariableId" in line:
                continue
            else:
                # Assume the first column contains variable ids (one or more), separated by tabs.
                variable_ids_all += line.split("\t")[1]

    # Extract all substrings of digits that should be variable ids.
    variable_ids = sorted(set([int(variable_id) for variable_id in re.findall(r"\d{5,6}", variable_ids_all)]))

    return variable_ids


@click.command(name="explorer-update", cls=RichCommand, help=__doc__)
# @click.argument("explorer_name", type=str or List[str], nargs=-1)
@click.argument("explorer_name", type=str)
@click.option(
    "--explorers-dir", type=str, default=EXPLORERS_DIR, help=f"Path to explorer files. Default: {EXPLORERS_DIR}"
)
def cli(explorer_name: str, explorers_dir: Optional[Union[Path, str]] = None) -> None:
    """Update variable ids in indicator-based explorer files."""

    if explorers_dir is None:
        # If explorer folder is not provided, assume owid-content is in the same folder as ETL.
        explorers_dir = EXPLORERS_DIR
    elif isinstance(explorers_dir, str):
        explorers_dir = Path(explorers_dir)

    # Define path to explorer file.
    explorer_file = (explorers_dir / explorer_name).with_suffix(".explorer.tsv")

    if not explorer_file.is_file():
        raise FileNotFoundError(f"Explorer file not found: {explorer_file}")

    # Load explorer file as a string.
    with open(explorer_file, "r") as f:
        explorer = f.read()

    if "yVariableIds" not in explorer:
        log.info("This may not be an indicator-based explorer ('yVariableIds' not found).")
        return None

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
        variable_data = variable_data[variable_data["catalogPath"].isnull()].reset_index(drop=True)

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
        log.warning(
            f"Steps not found in ETL: {missing_steps}\nTry using prefix 'data-private://' instead of 'data://'."
        )

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

    # The following works, but it may replace variable ids in the wrong places.
    # explorer_new = explorer
    # for variable_id_old, variable_id_new in variable_mapping.items():
    #     explorer_new = explorer_new.replace(str(variable_id_old), str(variable_id_new))

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

    message = "Variables to be replaced in explorer file (showing ids, versions and names):\n"
    for i, row in combined.iterrows():
        assert int(row["id_new"]) > int(row["id"]), "New variable id should be greater than old variable id."
        assert row["version_new"] > row["version"], "New version should be greater than old version."
        message += f"{row['id']} ({row['version']}) -> {row['id_new']} ({row['version_new']})\n"
        if row["name"] != row["name_new"]:
            message += "  Name change:\n"
            message += f"  {row['name']} ->\n  {row['name_new']}\n"
    log.info(message)
    input("Press Enter to rewrite the explorer file.")

    # Write to explorer file, to replace old variable ids with new ones.
    with open(explorer_file, "w") as f:
        f.write(explorer_new)


# # Apply the function to all explorer files.
# explorers_dir = EXPLORERS_DIR

# for explorer_file in list(explorers_dir.glob("*.tsv")):
#     explorer_name = explorer_file.stem
#     print(explorer_name)
#     cli(explorer_name, explorers_dir=explorers_dir)


if __name__ == "__main__":
    cli()
