"""Update FAO names and descriptions

After an update, FAO may have changed dataset, item, element, or unit names and descriptions.
This will cause the garden faostat_metadata step to raise warnings, and may cause further issues on charts and the food
explorer.

This script updates all fao fields in custom_datasets.csv, custom_elements_and_units.csv, and custom_items.csv files, as follows:
* Compare the old FAO dataset names and descriptions with the new FAO ones. They will be updated automatically.
* Compare the old OWID dataset names and descriptions with the new FAO ones. You will be prompted for confirmation and will be able to edit the changes.
* Compare the old FAO element names, element descriptions, and units, with the new FAO ones.
* Compare the old OWID element names, element descriptions, and units, with the new FAO ones. You will be prompted for confirmation and will be able to edit the changes.
* Compare the old FAO item names and descriptions with the new FAO ones.
* Compare the old OWID item names and descriptions with the new FAO ones. You will be prompted for confirmation and will be able to edit the changes.

"""

import argparse
import difflib
import os
import re
import tempfile

import pandas as pd
from owid.catalog import Dataset
from shared import INCLUDED_DATASETS_CODES, VERSION  # type: ignore[reportMissingImports]
from structlog import get_logger
from tqdm.auto import tqdm

from etl.paths import DATA_DIR, STEP_DIR

# Initialize logger.
log = get_logger()

# ANSI color codes.
RED = "\033[91m"
GREEN = "\033[92m"
RESET = "\033[0m"


def _display_differences(old: str, new: str, message: str) -> None:
    # NOTE: This function was quickly created by chatGPT and it does the job, but it probably doesn't handle all edge cases. So, always compare old and new before overwriting.
    tqdm.write("\n" + "------------" * 10)
    tqdm.write(message)
    tqdm.write("------------" * 10)

    # Ensure sentences are split while keeping punctuation intact
    old_sentences = re.split(r"(\.|\n)", old)
    new_sentences = re.split(r"(\.|\n)", new)

    def reconstruct_sentences(segments):
        """Reconstructs sentences from split segments while preserving structure."""
        sentences = []
        buffer = ""
        for segment in segments:
            buffer += segment
            if segment in {".", "\n"}:
                sentences.append(buffer.strip())
                buffer = ""
        if buffer:
            sentences.append(buffer.strip())
        return sentences

    old_sentences = reconstruct_sentences(old_sentences)
    new_sentences = reconstruct_sentences(new_sentences)

    matcher = difflib.SequenceMatcher(None, old_sentences, new_sentences)
    first_change = True

    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == "replace":
            max_len = max(i2 - i1, j2 - j1)
            for i in range(max_len):
                old_sentence = old_sentences[i1 + i] if i1 + i < i2 else ""
                new_sentence = new_sentences[j1 + i] if j1 + i < j2 else ""

                if not first_change:
                    tqdm.write("")
                first_change = False

                word_matcher = difflib.SequenceMatcher(None, old_sentence, new_sentence)
                highlighted_old = []
                highlighted_new = []

                for word_tag, w1, w2, w3, w4 in word_matcher.get_opcodes():
                    old_segment = old_sentence[w1:w2]
                    new_segment = new_sentence[w3:w4]

                    if word_tag == "replace":
                        highlighted_old.append(RED + old_segment + RESET)
                        highlighted_new.append(GREEN + new_segment + RESET)
                    elif word_tag == "delete":
                        highlighted_old.append(RED + old_segment + RESET)
                    elif word_tag == "insert":
                        highlighted_new.append(GREEN + new_segment + RESET)
                    elif word_tag == "equal":
                        highlighted_old.append(old_segment)
                        highlighted_new.append(new_segment)

                tqdm.write("- " + "".join(highlighted_old))
                tqdm.write("+ " + "".join(highlighted_new) + "\n")
        elif tag == "delete":
            if not first_change:
                tqdm.write("")
            first_change = False
            for old_sentence in old_sentences[i1:i2]:
                tqdm.write(RED + "- " + old_sentence + RESET + "\n")
        elif tag == "insert":
            if not first_change:
                tqdm.write("")
            first_change = False
            for new_sentence in new_sentences[j1:j2]:
                tqdm.write(GREEN + "+ " + new_sentence + RESET + "\n")
        elif tag == "equal":
            for sentence in old_sentences[i1:i2]:
                tqdm.write("  " + sentence)


def _display_confirmed_changes(old, new, old_label="Old", new_label="New"):
    # Print full old and new paragraphs after accepting
    tqdm.write("\n" + RED + f"{old_label}:" + RESET)
    tqdm.write(RED + old + RESET)
    tqdm.write("\n" + GREEN + f"{new_label}:" + RESET)
    tqdm.write(GREEN + new + RESET + "\n")
    input("\nPress enter to continue...")


def _confirm_edit_or_skip(old, new):
    choice = (
        input("Type 'y' and enter to accept these changes, 'e' to edit them, or just enter to skip: ").strip().lower()
    )
    if choice == "y":
        _display_confirmed_changes(old, new)
    elif choice == "e":
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            with open(f.name, "w") as f:
                f.write(new)
            os.system(f"open -a TextEdit {f.name}")
            input("Press enter to save changes.")
            with open(f.name) as f:
                new = f.read()
        _display_confirmed_changes(old, new)

    return new


def _confirm_and_write_data_to_file(custom_data, custom_data_file):
    while True:
        choice = (
            input(f"Type 'y' and enter to overwrite file {custom_data_file} or type 'n' to ignore changes: ")
            .strip()
            .lower()
        )
        if choice == "y":
            # Update custom elements file.
            custom_data.to_csv(custom_data_file)
            break
        elif choice == "n":
            tqdm.write("File not updated.")
            break


def update_custom_datasets_file(
    interactive=False, version=VERSION, read_only=False, confirmation=False, compare_with="fao"
):
    """Update custom_datasets.csv file of a specific version of the garden steps.

    Parameters
    ----------
    interactive : bool, optional
        True to print changes one by one (and to confirm before overwriting file). False to do all changes silently.
    version : _type_, optional
        Version of the garden steps to consider (where the custom_*.csv file to be updated is).
    read_only : bool, optional
        True to find changes without actually overwriting existing file.
    confirmation : bool, optional
        True to prompt for confirmation before accepting changes.
    compare_with : str, optional
        The original source to compare with. Can be 'fao' or 'owid'.

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
                old = custom_datasets.loc[dataset_short_name].fillna("")[f"{compare_with}_dataset_{field}"]
            except KeyError:
                # This may be a new dataset that didn't exist in the previous version.
                old = ""

            # Normalize whitespace for comparison.
            _old = old.replace("\n", " ").replace("  ", " ")
            _new = new.replace("\n", " ").replace("  ", " ")
            if (_old != _new) and not (pd.isna(new) and pd.isna(old)):
                if interactive:
                    _display_differences(
                        old=_old,
                        new=_new,
                        message=f"Old {compare_with.upper()} and new FAO dataset {field} for {dataset_short_name}:",
                    )
                    if confirmation:
                        new = _confirm_edit_or_skip(old, new)
                    else:
                        input("These changes will be saved after going through all datasets. Press enter to continue.")

                # Update FAO field.
                custom_datasets_updated.loc[dataset_short_name, f"{compare_with}_dataset_{field}"] = new

                # There was at least one change.
                CHANGES_FOUND = True

    # Sort custom datasets conveniently.
    custom_datasets_updated = custom_datasets_updated.sort_index()

    if interactive and not CHANGES_FOUND:
        tqdm.write("\nNo changes found or accepted.")

    if CHANGES_FOUND and not read_only:
        _confirm_and_write_data_to_file(custom_data=custom_datasets_updated, custom_data_file=custom_datasets_file)

    return custom_datasets_updated


def update_custom_elements_and_units_file(
    interactive=False, version=VERSION, read_only=False, confirmation=False, compare_with="fao"
):
    """Update custom_elements_and_units.csv file of a specific version of the garden steps.

    Parameters
    ----------
    interactive : bool, optional
        True to print changes one by one (and to confirm before overwriting file). False to do all changes silently.
    version : _type_, optional
        Version of the garden steps to consider (where the custom_*.csv file to be updated is).
    read_only : bool, optional
        True to find changes without actually overwriting existing file.
    confirmation : bool, optional
        True to prompt for confirmation before accepting changes.
    compare_with : str, optional
        The original source to compare with. Can be 'fao' or 'owid'.

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
            try:
                new_metadata = fao_new_metadata["elements"].loc[dataset_short_name, element_code].fillna("")
            except KeyError:
                log.error(
                    f"Element code {element_code} (for dataset {dataset_short_name}) in custom elements and units file was not found in new faostat_metadata. Remove it from the custom file or replace it with another element code."
                )
                continue
            old_metadata = custom_elements.loc[dataset_short_name, element_code].fillna("")
            for field in ["element", "element_description", "unit", "unit_short_name"]:
                new = new_metadata[f"fao_{field}"]
                old = old_metadata[f"{compare_with}_{field}"]

                # If old and new are not identical (or if they are not both nan) update custom_*.
                if (old != new) and not (pd.isna(new) and pd.isna(old)):
                    if interactive:
                        _display_differences(
                            old=old,
                            new=new,
                            message=f"Old {compare_with.upper()} and new FAO {field} for {dataset_short_name} with element code {element_code}:",
                        )
                        if confirmation:
                            new = _confirm_edit_or_skip(old, new)
                        else:
                            input("These changes will be saved after going through all elements. Enter to continue.")

                    # Update FAO field.
                    custom_elements_updated.loc[(dataset_short_name, element_code), f"{compare_with}_{field}"] = new

                    # There was at least one change.
                    CHANGES_FOUND = True

    # Sort custom element conveniently.
    custom_elements_updated = custom_elements_updated.sort_values(["fao_element"])

    if interactive and not CHANGES_FOUND:
        tqdm.write("\nNo changes found or accepted.")

    if CHANGES_FOUND and not read_only:
        _confirm_and_write_data_to_file(custom_data=custom_elements_updated, custom_data_file=custom_elements_file)
        tqdm.write("Consider updating the 'owid_unit_factor' (and possibly other fields) if units have changed.")

    return custom_elements_updated


def update_custom_items_file(
    interactive=False, version=VERSION, read_only=False, confirmation=False, compare_with="fao"
):
    """Update custom_items.csv file of a specific version of the garden steps.

    NOTE: This function is structurally very similar to update_custom_elements_and_units_file.
    They could be merged into one to avoid redundant code.

    Parameters
    ----------
    interactive : bool, optional
        True to print changes one by one (and to confirm before overwriting file). False to do all changes silently.
    version : _type_, optional
        Version of the garden steps to consider (where the custom_*.csv file to be updated is).
    read_only : bool, optional
        True to find changes without actually overwriting existing file.
    confirmation : bool, optional
        True to prompt for confirmation before accepting changes.
    compare_with : str, optional
        The original source to compare with. Can be 'fao' or 'owid'.

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

    # Ensure no column is of categorical type.
    compared = compared.astype(object)

    # Go one by one on the datasets for which at least one custom item was defined.
    for field in tqdm(fields_to_compare):
        _compared = compared.copy()
        _compared[f"{field}_old"] = _compared[f"{field}_old"].fillna("")
        _compared[f"{field}_new"] = _compared[f"{field}_new"].fillna("")
        _compared = _compared[_compared[f"{field}_old"] != _compared[f"{field}_new"]].reset_index()
        if interactive:
            n_changes = len(_compared)
            tqdm.write(f"\nNumber of changes in {field} to review: {n_changes}")

        for _, row in tqdm(_compared.iterrows(), total=len(_compared)):
            dataset_short_name = row["dataset"]
            item_code = row["item_code"]
            old = row[f"{field}_old"]
            new = row[f"{field}_new"]

            # If old and new are not identical (or if they are not both nan) update custom_*.
            if (old != new) and not (pd.isna(new) and pd.isna(old)):
                if interactive:
                    _display_differences(
                        old=old,
                        new=new,
                        message=f"Old {compare_with.upper()} and new FAO {field} for {dataset_short_name} with item code {item_code}:",
                    )
                    if confirmation:
                        new = _confirm_edit_or_skip(old, new)
                    else:
                        input("These changes will be saved after going through all items. Enter to continue.")

                # Update FAO field.
                custom_items_updated.loc[(dataset_short_name, item_code), f"{compare_with}_{field}"] = new

                # There was at least one change.
                CHANGES_FOUND = True

    # Sort custom items conveniently.
    custom_items_updated = custom_items_updated.sort_values(["fao_item"])

    if interactive and not CHANGES_FOUND:
        tqdm.write("\nNo changes found or accepted.")

    if CHANGES_FOUND and not read_only:
        _confirm_and_write_data_to_file(custom_data=custom_items_updated, custom_data_file=custom_items_file)

    return custom_items_updated


if __name__ == "__main__":
    argument_parser = argparse.ArgumentParser(description=__doc__)
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
    _ = update_custom_datasets_file(
        interactive=True, version=args.version, read_only=args.read_only, confirmation=False, compare_with="fao"
    )
    _ = update_custom_datasets_file(
        interactive=True, version=args.version, read_only=args.read_only, confirmation=True, compare_with="owid"
    )
    _ = update_custom_elements_and_units_file(
        interactive=True, version=args.version, read_only=args.read_only, confirmation=False, compare_with="fao"
    )
    _ = update_custom_elements_and_units_file(
        interactive=True, version=args.version, read_only=args.read_only, confirmation=True, compare_with="owid"
    )
    _ = update_custom_items_file(
        interactive=True, version=args.version, read_only=args.read_only, confirmation=False, compare_with="fao"
    )
    _ = update_custom_items_file(
        interactive=True, version=args.version, read_only=args.read_only, confirmation=True, compare_with="owid"
    )
