"""After an update, when running the garden faostat_metadata step, it warns that domain titles or descriptions
have changed, so we have to update the custom_datasets.csv file.

This script will update all fao fields.
"""

import argparse

import pandas as pd
from owid.catalog import Dataset
from shared import INCLUDED_DATASETS_CODES, VERSION

from etl.paths import DATA_DIR, STEP_DIR


def _display_differences_and_wait(old: str, new: str, message: str) -> None:
    print("\n" + "------------" * 10)
    print(message)
    print("------------" * 10)
    print(f"\n{old}")
    print("\n->")
    print(f"\n{new}")
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
        print("\nChecking for changes in FAO dataset titles and descriptions.")

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

    for domain in INCLUDED_DATASETS_CODES:
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
        print("\nNo changes found.")

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
        print("\nChecking for changes in FAO element names and descriptions, as well as units and short units.")

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
    for dataset_short_name in custom_elements.index.get_level_values(0).unique():
        for element_code in custom_elements.loc[dataset_short_name].index.get_level_values(0).unique():
            new_metadata = fao_new_metadata["elements"].loc[dataset_short_name, element_code].fillna("")
            old_metadata = custom_elements.loc[dataset_short_name, element_code].fillna("")
            for field in ["fao_element", "fao_element_description", "fao_unit", "fao_unit_short_name"]:
                new = new_metadata[field]
                old = old_metadata[field]

                # If old and new are not identical (or if they are not both nan) update custom_datasets.
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
        print("\nNo changes found.")

    if CHANGES_FOUND and not read_only:
        if interactive:
            input(f"Press enter to overwrite file: {custom_elements_file}")

        # Update custom elements file.
        custom_elements_updated.to_csv(custom_elements_file)

    return custom_elements_updated


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
