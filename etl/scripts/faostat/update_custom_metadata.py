"""Update FAO names and descriptions

After an update, FAO may have changed dataset, item, element, or unit names and descriptions.
This will cause the garden faostat_metadata step to raise warnings, and may cause further issues on charts and the food
explorer.

This script updates all fao fields in custom_datasets.csv, custom_elements_and_units.csv, and custom_items.csv files.
NOTE: It's recommended to run this script in interactive mode (using the -i flag) to check if any name change implies a
significant change in the data.
"""

import argparse

import pandas as pd
from owid.catalog import Dataset
from shared import INCLUDED_DATASETS_CODES, VERSION
from tqdm.auto import tqdm

from etl.paths import DATA_DIR, STEP_DIR


def _display_differences_and_wait(old: str, new: str, message: str) -> None:
    tqdm.write("\n" + "------------" * 10)
    tqdm.write(message)
    tqdm.write("------------" * 10)
    tqdm.write(f"\n{old}")
    tqdm.write("\n->")
    tqdm.write(f"\n{new}")
    input("\nEnter to move on.")


def update_custom_datasets_file(interactive=False, version=VERSION, read_only=False):
    """Update custom_datasets.csv file of a specific version of the garden steps.

    Parameters
    ----------
    interactive : bool, optional
        True to print changes one by one (and to confirm before overwriting file). False to do all changes silently.
    version : _type_, optional
        Version of the garden steps to consider (where the custom_*.csv file to be updated is).
    read_only : bool, optional
        True to find changes without actually overwriting existing file.

    Returns
    -------
    custom_datasets_updated : pd.DataFrame
        Updated dataframe of custom datasets.

    """
    if interactive:
        tqdm.write("\nChecking for changes in FAO dataset titles and descriptions.")

    # Path to custom datasets file in garden.
    custom_datasets_file = STEP_DIR / "data/garden/faostat" / version / "custom_datasets.csv"

    error = f"File custom_datasets.csv not found. Ensure garden steps for version {version} exist."
    assert custom_datasets_file.is_file(), error

    # Load custom datasets file.
    custom_datasets = pd.read_csv(custom_datasets_file).set_index("dataset")

    # Initialize boolean that is True if there were changes in any field.
    CHANGES_FOUND = False

    # Initialize a new custom datasets dataframe.
    custom_datasets_updated = custom_datasets.copy()

    for domain in tqdm(INCLUDED_DATASETS_CODES):
        dataset_short_name = f"faostat_{domain}"

        # Load metadata from new meadow dataset.
        fao_new_dataset_metadata = Dataset(DATA_DIR / "meadow/faostat" / version / dataset_short_name).metadata

        for field in ["title", "description"]:
            new = getattr(fao_new_dataset_metadata, field)
            try:
                # Load custom dataset metadata for current domain.
                old = custom_datasets.loc[dataset_short_name].fillna("")[f"fao_dataset_{field}"]
            except KeyError:
                # This may be a new dataset that didn't exist in the previous version.
                old = ""

            # For faostat_fa, the new description is empty (while the old wasn't).
            # For consistency, we will change it too, while keeping the old custom dataset description.

            # If old and new are not identical (or if they are not both nan) update custom_datasets.
            if (old != new) and not (pd.isna(new) and pd.isna(old)):
                if interactive:
                    _display_differences_and_wait(
                        old=old, new=new, message=f"Old and new FAO dataset {field} for {dataset_short_name}:"
                    )

                # Update FAO field.
                custom_datasets_updated.loc[dataset_short_name, f"fao_dataset_{field}"] = new

                # There was at least one change.
                CHANGES_FOUND = True

    # Sort custom datasets conveniently.
    custom_datasets_updated = custom_datasets_updated.sort_index()

    if interactive and not CHANGES_FOUND:
        tqdm.write("\nNo changes found.")

    if CHANGES_FOUND and not read_only:
        if interactive:
            input(f"Press enter to overwrite file: {custom_datasets_file}")

        # Update custom datasets file.
        custom_datasets_updated.sort_index().to_csv(custom_datasets_file)

    return custom_datasets_updated


def update_custom_elements_and_units_file(interactive=False, version=VERSION, read_only=False):
    """Update custom_elements_and_units.csv file of a specific version of the garden steps.

    Parameters
    ----------
    interactive : bool, optional
        True to print changes one by one (and to confirm before overwriting file). False to do all changes silently.
    version : _type_, optional
        Version of the garden steps to consider (where the custom_*.csv file to be updated is).
    read_only : bool, optional
        True to find changes without actually overwriting existing file.

    Returns
    -------
    custom_elements_updated : pd.DataFrame
        Updated dataframe of custom elements and units.

    """
    if interactive:
        tqdm.write(
            "\n*** Checking for changes in FAO element names and descriptions, as well as units and short units. ***"
        )

    # Path to custom elements and units file in garden.
    custom_elements_file = STEP_DIR / "data/garden/faostat" / version / "custom_elements_and_units.csv"

    error = f"File custom_elements_and_units.csv not found. Ensure garden steps for version {version} exist."
    assert custom_elements_file.is_file(), error

    # Load custom elements and units file.
    custom_elements = pd.read_csv(custom_elements_file, dtype=str).set_index(["dataset", "element_code"])

    # Initialize boolean that is True if there were changes in any field.
    CHANGES_FOUND = False

    # Initialize a new custom elements and units dataframe.
    custom_elements_updated = custom_elements.copy()

    # Load metadata from new garden dataset.
    fao_new_metadata = Dataset(DATA_DIR / "garden/faostat" / version / "faostat_metadata")

    # Go one by one on the datasets for which at least one custom element or unit was defined.
    for dataset_short_name in tqdm(custom_elements.index.get_level_values(0).unique()):
        for element_code in tqdm(custom_elements.loc[dataset_short_name].index.get_level_values(0).unique()):
            new_metadata = fao_new_metadata["elements"].loc[dataset_short_name, element_code].fillna("")
            old_metadata = custom_elements.loc[dataset_short_name, element_code].fillna("")
            for field in ["fao_element", "fao_element_description", "fao_unit", "fao_unit_short_name"]:
                new = new_metadata[field]
                old = old_metadata[field]

                # If old and new are not identical (or if they are not both nan) update custom_*.
                if (old != new) and not (pd.isna(new) and pd.isna(old)):
                    if interactive:
                        _display_differences_and_wait(
                            old=old,
                            new=new,
                            message=f"Old and new {field} for {dataset_short_name} with element code {element_code}:",
                        )

                    # Update FAO field.
                    custom_elements_updated.loc[dataset_short_name, element_code][field] = new

                    # There was at least one change.
                    CHANGES_FOUND = True

    # Sort custom element conveniently.
    custom_elements_updated = custom_elements_updated.sort_values(["fao_element"])

    if interactive and not CHANGES_FOUND:
        tqdm.write("\nNo changes found.")

    if CHANGES_FOUND and not read_only:
        if interactive:
            input(f"Press enter to overwrite file: {custom_elements_file}")

        # Update custom elements file.
        custom_elements_updated.to_csv(custom_elements_file)

    return custom_elements_updated


def update_custom_items_file(interactive=False, version=VERSION, read_only=False):
    """Update custom_items.csv file of a specific version of the garden steps.

    NOTE: This function is structuraly very similar to update_custom_elements_and_units_file.
    They could be merged into one to avoid redundant code.

    Parameters
    ----------
    interactive : bool, optional
        True to print changes one by one (and to confirm before overwriting file). False to do all changes silently.
    version : _type_, optional
        Version of the garden steps to consider (where the custom_*.csv file to be updated is).
    read_only : bool, optional
        True to find changes without actually overwriting existing file.

    Returns
    -------
    custom_items_updated : pd.DataFrame
        Updated dataframe of custom items.

    """
    if interactive:
        tqdm.write("\n*** Checking for changes in FAO item names and descriptions. ***")

    # Path to custom items file in garden.
    custom_items_file = STEP_DIR / "data/garden/faostat" / version / "custom_items.csv"

    error = f"File custom_items.csv not found. Ensure garden steps for version {version} exist."
    assert custom_items_file.is_file(), error

    # Load custom items file.
    custom_items = pd.read_csv(custom_items_file, dtype=str).set_index(["dataset", "item_code"])

    # Initialize boolean that is True if there were changes in any field.
    CHANGES_FOUND = False

    # Initialize a new custom items dataframe.
    custom_items_updated = custom_items.copy()

    # Load metadata from new garden dataset.
    fao_new_metadata = Dataset(DATA_DIR / "garden/faostat" / version / "faostat_metadata")

    # Fields to compare.
    fields_to_compare = ["fao_item", "fao_item_description"]

    # Dataframe of fields to compare.
    compared = pd.merge(
        custom_items,
        fao_new_metadata["items"],
        left_index=True,
        right_index=True,
        how="left",
        suffixes=("_old", "_new"),
    )

    if interactive:
        for field in fields_to_compare:
            n_changes = len(compared[compared[f"{field}_old"].fillna("") != compared[f"{field}_new"].fillna("")])
            tqdm.write(f"\nNumber of changes in {field} to review: {n_changes}")

    # Go one by one on the datasets for which at least one custom item was defined.
    for field in tqdm(fields_to_compare):
        _compared = compared[compared[f"{field}_old"].fillna("") != compared[f"{field}_new"].fillna("")].reset_index()
        for i, row in tqdm(_compared.iterrows(), total=len(_compared)):
            dataset_short_name = row["dataset"]
            item_code = row["item_code"]
            old = row[f"{field}_old"]
            new = row[f"{field}_new"]

            # If old and new are not identical (or if they are not both nan) update custom_*.
            if (old != new) and not (pd.isna(new) and pd.isna(old)):
                if interactive:
                    _display_differences_and_wait(
                        old=old,
                        new=new,
                        message=f"Old and new {field} for {dataset_short_name} with code {item_code}:",
                    )

                # Update FAO field.
                custom_items_updated.loc[dataset_short_name, item_code][field] = new

                # There was at least one change.
                CHANGES_FOUND = True

    # Sort custom item conveniently.
    custom_items_updated = custom_items_updated.sort_values(["fao_item"])

    if interactive and not CHANGES_FOUND:
        tqdm.write("\nNo changes found.")

    if CHANGES_FOUND and not read_only:
        if interactive:
            input(f"Press enter to overwrite file: {custom_items_file}")

        # Update custom items file.
        custom_items_updated.to_csv(custom_items_file)

    return custom_items_updated


if __name__ == "__main__":
    argument_parser = argparse.ArgumentParser(description=__doc__)
    argument_parser.add_argument(
        "-i",
        "--interactive",
        default=False,
        action="store_true",
        help="If given, changes will be printed one by one, and confirmation will be required to save file.",
    )
    argument_parser.add_argument(
        "-r",
        "--read_only",
        default=False,
        action="store_true",
        help="If given, existing custom_datasets.csv file will not be overwritten.",
    )
    argument_parser.add_argument(
        "-v",
        "--version",
        default=VERSION,
        help="Version of the latest garden steps (where custom_datasets.csv file to be updated is).",
    )
    args = argument_parser.parse_args()
    _ = update_custom_datasets_file(interactive=args.interactive, version=args.version, read_only=args.read_only)
    _ = update_custom_elements_and_units_file(
        interactive=args.interactive, version=args.version, read_only=args.read_only
    )
    _ = update_custom_items_file(interactive=args.interactive, version=args.version, read_only=args.read_only)
