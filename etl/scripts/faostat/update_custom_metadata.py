"""After an update, when running the garden faostat_metadata step, it warns that domain titles or descriptions
have changed, so we have to update the custom_datasets.csv file.

This script will update all fao fields.
"""

import argparse

import numpy as np
import pandas as pd
from owid.catalog import Dataset
from shared import INCLUDED_DATASETS_CODES, VERSION

from etl.paths import DATA_DIR, STEP_DIR


def update_custom_datasets_file(interactive=False, version=VERSION, read_only=False):
    """Update custom_datasets.csv file of a specific version of the garden steps.

    Parameters
    ----------
    interactive : bool, optional
        True to print changes one by one (and to confirm before overwriting file). False to do all changes silently.
    version : _type_, optional
        Version of the garden steps to consider (where the custom_datasets.csv file to be updated is).
    read_only : bool, optional
        True to find changes without actually overwriting existing file.

    Returns
    -------
    custom_datasets_updated : pd.DataFrame
        Updated dataframe of custom datasets.

    """
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
                    print("\n" + "------------" * 10)
                    print(f"Old and new FAO dataset {field} for {dataset_short_name}:")
                    print("------------" * 10)
                    print(f"\n{old}")
                    print("\n->")
                    print(f"\n{new}")
                    input("\nEnter to move on.")

                # Update FAO field.
                custom_datasets_updated.loc[dataset_short_name, f"fao_dataset_{field}"] = new

                # There was at least one change.
                CHANGES_FOUND = True

    # Sort custom datasets conveniently.
    custom_datasets_updated = custom_datasets_updated.sort_index()

    if interactive and not CHANGES_FOUND:
        print("No changes found.")

    if CHANGES_FOUND and not read_only:
        if interactive:
            input(f"Press enter to overwrite file: {custom_datasets_file}")

        # Update custom datasets file.
        custom_datasets_updated.sort_index().to_csv(custom_datasets_file)

    return custom_datasets_updated


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
