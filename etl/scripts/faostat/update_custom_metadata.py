"""Update FAO names and descriptions

After an update, FAO may have changed dataset, item, element, or unit names and descriptions.
This will cause the garden faostat_metadata step to raise warnings, and may cause further issues on charts and the food
explorer.

This script updates all fao fields in custom_datasets.csv, custom_elements_and_units.csv, and custom_items.csv files.
It runs 6 phases:
  1. FAO dataset fields   (auto-accepted)
  2. OWID dataset fields  (interactive -- you review each change)
  3. FAO element fields    (auto-accepted)
  4. OWID element fields   (interactive)
  5. FAO item fields       (auto-accepted)
  6. OWID item fields      (interactive)

Usage:
  cd etl/scripts/faostat
  python update_custom_metadata.py                  # normal run (all 6 phases)
  python update_custom_metadata.py --field dataset  # only dataset phases
  python update_custom_metadata.py --field element  # only element phases
  python update_custom_metadata.py --field item     # only item phases
  python update_custom_metadata.py --dry-run        # preview changes without writing files

"""

import argparse
import difflib
import os
import sys
import tempfile
import termios
import tty

import pandas as pd
from owid.catalog import Dataset
from shared import INCLUDED_DATASETS_CODES, VERSION  # type: ignore[reportMissingImports]
from structlog import get_logger
from tqdm.auto import tqdm

from etl.paths import DATA_DIR, STEP_DIR

# Initialize logger.
log = get_logger()

# ANSI color codes (only when outputting to a real terminal).
if sys.stdout.isatty():
    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    CYAN = "\033[96m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    RESET = "\033[0m"
else:
    RED = GREEN = YELLOW = CYAN = BOLD = DIM = RESET = ""


def _print(msg=""):
    """Print a message, bypassing tqdm safely."""
    tqdm.write(msg)


def _truncate(text: str, max_len: int = 120) -> str:
    """Truncate text for compact display, collapsing whitespace."""
    text = " ".join(text.split())
    if len(text) <= max_len:
        return text
    return text[:max_len] + f"{DIM}...{RESET}"


def _display_differences(old: str, new: str, message: str, old_label: str = "OLD", new_label: str = "NEW") -> None:
    """Show differences between old and new text, with word-level highlighting."""
    _print()
    _print(f"  {CYAN}{BOLD}{'─' * 76}{RESET}")
    _print(f"  {CYAN}{BOLD}{message}{RESET}")
    _print(f"  {CYAN}{BOLD}{'─' * 76}{RESET}")

    # Normalize whitespace for comparison to detect whitespace-only changes.
    old_normalized = " ".join(old.split())
    new_normalized = " ".join(new.split())

    if old_normalized == new_normalized:
        _print(f"  {YELLOW}Only whitespace/formatting changes (content is identical).{RESET}")
        return

    # Word-level diff for actual content changes.
    old_words = old_normalized.split()
    new_words = new_normalized.split()

    sm = difflib.SequenceMatcher(None, old_words, new_words)

    old_parts = []
    new_parts = []

    for tag, i1, i2, j1, j2 in sm.get_opcodes():
        if tag == "equal":
            chunk = " ".join(old_words[i1:i2])
            old_parts.append(chunk)
            new_parts.append(chunk)
        elif tag == "replace":
            old_parts.append(f"{RED}{BOLD}" + " ".join(old_words[i1:i2]) + RESET)
            new_parts.append(f"{GREEN}{BOLD}" + " ".join(new_words[j1:j2]) + RESET)
        elif tag == "delete":
            old_parts.append(f"{RED}{BOLD}" + " ".join(old_words[i1:i2]) + RESET)
        elif tag == "insert":
            new_parts.append(f"{GREEN}{BOLD}" + " ".join(new_words[j1:j2]) + RESET)

    _print(f"  {RED}{old_label}:{RESET} {' '.join(old_parts)}")
    _print()
    _print(f"  {GREEN}{new_label}:{RESET} {' '.join(new_parts)}")
    _print()


def _getch():
    """Read a single keypress without waiting for enter. Ctrl+C still works."""
    fd = sys.stdin.fileno()
    old_settings = termios.tcgetattr(fd)
    try:
        tty.setcbreak(fd)  # setcbreak (not setraw) so Ctrl+C still sends SIGINT
        ch = sys.stdin.read(1)
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
    return ch


def _confirm_edit_or_skip(current, proposed):
    """Ask the user whether to keep the current value, adopt the proposed one, or edit.

    Parameters
    ----------
    current : str
        The current OWID value.
    proposed : str
        The new FAO value being proposed.

    """
    _print(f"  {BOLD}[c]{RESET} keep current  {BOLD}[n]{RESET} adopt new  {BOLD}[e]{RESET} edit")
    while True:
        ch = _getch().lower()
        if ch == "c":
            _print(f"  {DIM}Kept current value.{RESET}")
            return current
        elif ch == "n":
            _print(f"  {GREEN}Adopted new value.{RESET}")
            return proposed
        elif ch == "e":
            # Use $VISUAL or $EDITOR or fall back to vi.
            editor = os.environ.get("VISUAL") or os.environ.get("EDITOR") or "vi"
            tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False)
            try:
                tmp.write(proposed)
                tmp.close()
                os.system(f'{editor} "{tmp.name}"')
                with open(tmp.name) as f:
                    chosen = f.read()
            finally:
                os.unlink(tmp.name)
            _print(f"  {GREEN}Edited value:{RESET} {_truncate(chosen)}")
            return chosen


def _confirm_and_write_data_to_file(custom_data, custom_data_file, n_changes=0):
    changes_str = f" ({n_changes} change{'s' if n_changes != 1 else ''})" if n_changes else ""
    while True:
        choice = (
            input(f"\n  {BOLD}Save{changes_str} to {custom_data_file.name}?{RESET} [{BOLD}y{RESET}/{BOLD}n{RESET}]: ")
            .strip()
            .lower()
        )
        if choice == "y":
            custom_data.to_csv(custom_data_file)
            _print(f"  {GREEN}Saved {custom_data_file.name}.{RESET}")
            break
        elif choice == "n":
            _print(f"  {YELLOW}File not updated.{RESET}")
            break


def update_custom_datasets_file(version=VERSION, read_only=False, confirmation=False, compare_with="fao"):
    """Update custom_datasets.csv file of a specific version of the garden steps.

    Parameters
    ----------
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
    # Path to custom datasets file in garden.
    custom_datasets_file = STEP_DIR / "data/garden/faostat" / version / "custom_datasets.csv"

    error = f"File custom_datasets.csv not found. Ensure garden steps for version {version} exist."
    assert custom_datasets_file.is_file(), error

    # Load custom datasets file.
    custom_datasets = pd.read_csv(custom_datasets_file).set_index("dataset")

    # Initialize a new custom datasets dataframe.
    custom_datasets_updated = custom_datasets.copy()

    # Collect changes (for summary in auto-accept mode).
    changes = []

    for domain in tqdm(INCLUDED_DATASETS_CODES):
        dataset_short_name = f"faostat_{domain}"

        # Load metadata from new meadow dataset.
        fao_new_dataset_metadata = Dataset(DATA_DIR / "meadow/faostat" / version / dataset_short_name).metadata

        for field in ["title", "description"]:
            new = getattr(fao_new_dataset_metadata, field) or ""
            try:
                # Load custom dataset metadata for current domain.
                old = custom_datasets.loc[dataset_short_name].fillna("")[f"{compare_with}_dataset_{field}"]
            except KeyError:
                # This may be a new dataset that didn't exist in the previous version.
                old = ""

            _old = old
            _new = new
            if (_old != _new) and not (pd.isna(new) and pd.isna(old)):
                if confirmation:
                    # Interactive mode: show word-level diff for long text fields.
                    row = custom_datasets.loc[dataset_short_name].fillna("")
                    owid_current = row.get(f"owid_dataset_{field}", "")
                    fao_new = new

                    _display_differences(
                        old=owid_current,
                        new=fao_new,
                        message=f"{dataset_short_name}: {field}",
                        old_label="OWID (current)",
                        new_label="FAO (new)",
                    )

                    chosen = _confirm_edit_or_skip(owid_current, fao_new)
                else:
                    # Auto-accept mode: silently collect changes.
                    chosen = new
                    changes.append((dataset_short_name, field, old, new))

                # Update field.
                custom_datasets_updated.loc[dataset_short_name, f"{compare_with}_dataset_{field}"] = chosen

    # Sort custom datasets conveniently.
    custom_datasets_updated = custom_datasets_updated.sort_index()

    n_changes = len(changes)

    if not confirmation and n_changes > 0:
        # Print summary for auto-accepted changes.
        _print(f"\n  {GREEN}Auto-accepted {n_changes} change{'s' if n_changes != 1 else ''}:{RESET}")
        for ds, field, _old, _new in changes:
            _print(f"    {BOLD}{ds}{RESET}: {field}")
        choice = input(f"\n  Type {BOLD}d{RESET} to review diffs, or {BOLD}enter{RESET} to continue: ").strip().lower()
        if choice == "d":
            for ds, field, _old, _new in changes:
                _display_differences(
                    old=_old,
                    new=_new,
                    message=f"{ds}: {field}",
                    old_label="FAO (old)",
                    new_label="FAO (new)",
                )
    elif n_changes == 0 and not confirmation:
        _print(f"  {DIM}No changes found.{RESET}")

    # In interactive mode, count changes by comparing with original (before sorting).
    if confirmation:
        n_interactive_changes = (
            (custom_datasets_updated.reindex_like(custom_datasets).fillna("") != custom_datasets.fillna("")).sum().sum()
        )
        has_changes = n_interactive_changes > 0
        n_total = n_interactive_changes
    else:
        has_changes = n_changes > 0
        n_total = n_changes

    if not has_changes:
        _print(f"  {DIM}No changes found or accepted.{RESET}")

    if has_changes and not read_only:
        _confirm_and_write_data_to_file(
            custom_data=custom_datasets_updated, custom_data_file=custom_datasets_file, n_changes=n_total
        )

    return custom_datasets_updated


def update_custom_items_or_elements_file(
    item_or_element, version=VERSION, read_only=False, confirmation=False, compare_with="fao"
):
    """Update custom_elements_and_units.csv file of a specific version of the garden steps.

    Parameters
    ----------
    item_or_element: str
        Either "item" or "element".
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
    if item_or_element == "element":
        # Path to custom elements and units file in garden.
        custom_file = STEP_DIR / "data/garden/faostat" / version / "custom_elements_and_units.csv"
        fields = ["element", "element_description", "unit", "unit_short_name"]
    else:
        custom_file = STEP_DIR / "data/garden/faostat" / version / "custom_items.csv"
        fields = ["item", "item_description"]

    error = f"File of custom definitions not found. Ensure garden steps for version {version} exist."
    assert custom_file.is_file(), error

    # Load custom definitions file.
    custom_definitions = pd.read_csv(custom_file, dtype=str).set_index(["dataset", f"{item_or_element}_code"])

    # Initialize a new custom definitions dataframe.
    custom_definitions_updated = custom_definitions.copy()

    # Collect changes (for summary in auto-accept mode).
    changes = []

    # Load metadata from new garden dataset.
    fao_new_metadata = Dataset(DATA_DIR / "garden/faostat" / version / "faostat_metadata")

    # Go one by one on the datasets for which there is at least one custom definition.
    for dataset_short_name in tqdm(custom_definitions.index.get_level_values(0).unique()):
        for code in tqdm(custom_definitions.loc[dataset_short_name].index.get_level_values(0).unique()):
            try:
                new_metadata = fao_new_metadata[f"{item_or_element}s"].loc[dataset_short_name, code].fillna("")
            except KeyError:
                log.error(
                    f"{item_or_element.capitalize()} code {code} (for dataset {dataset_short_name}) in custom definitions file was not found in new faostat_metadata. Remove it from the custom file or replace it with another code."
                )
                continue
            old_metadata = custom_definitions.loc[dataset_short_name, code].fillna("")
            for field in fields:
                new = new_metadata[f"fao_{field}"]
                old = old_metadata[f"{compare_with}_{field}"]

                # If old and new are not identical (or if they are not both nan) update custom_*.
                if (old != new) and not (pd.isna(new) and pd.isna(old)):
                    if confirmation:
                        # Interactive mode: show full context (all 4 values).
                        fao_old = old_metadata.get(f"fao_{field}", "")
                        owid_current = old_metadata.get(f"owid_{field}", "")
                        fao_new = new
                        owid_new = fao_new  # proposed new OWID value

                        _print()
                        _print(f"  {CYAN}{BOLD}{'─' * 76}{RESET}")
                        _print(f"  {CYAN}{BOLD}{dataset_short_name} ({item_or_element} {code}): {field}{RESET}")
                        _print(f"  {CYAN}{BOLD}{'─' * 76}{RESET}")
                        _print(f"  {DIM}FAO (previous):{RESET}  {fao_old}")
                        _print(f"  FAO (current):   {fao_new}")
                        _print(f"  {YELLOW}OWID (current):{RESET}  {owid_current}")
                        _print(f"  {GREEN}OWID (new):{RESET}      {owid_new}")

                        # For unit fields on elements, show the custom factor as context.
                        if item_or_element == "element" and field in ("unit", "unit_short_name"):
                            factor = old_metadata.get("owid_unit_factor", "")
                            if factor:
                                _print(f"  {YELLOW}Note: OWID applies a custom factor of {BOLD}{factor}{RESET}")
                        _print()

                        chosen = _confirm_edit_or_skip(owid_current, owid_new)
                    else:
                        # Auto-accept mode: silently collect changes.
                        chosen = new
                        changes.append((dataset_short_name, code, field, old, new))

                    # Update FAO field.
                    custom_definitions_updated.loc[(dataset_short_name, code), f"{compare_with}_{field}"] = chosen

    # Sort custom definitions file conveniently.
    custom_definitions_updated = custom_definitions_updated.sort_values([f"fao_{item_or_element}"])

    n_changes = len(changes)

    if not confirmation and n_changes > 0:
        # Print summary for auto-accepted changes.
        _print(f"\n  {GREEN}Auto-accepted {n_changes} change{'s' if n_changes != 1 else ''}:{RESET}")
        for ds, code, field, _old, _new in changes:
            _print(f"    {BOLD}{ds}{RESET} ({item_or_element} {code}): {field}")
        choice = input(f"\n  Type {BOLD}d{RESET} to review diffs, or {BOLD}enter{RESET} to continue: ").strip().lower()
        if choice == "d":
            for ds, code, field, _old, _new in changes:
                _display_differences(
                    old=_old,
                    new=_new,
                    message=f"{ds} ({item_or_element} {code}): {field}",
                    old_label="FAO (old)",
                    new_label="FAO (new)",
                )
    elif n_changes == 0 and not confirmation:
        _print(f"  {DIM}No changes found.{RESET}")

    # In interactive mode, count changes by comparing with original (before sorting).
    if confirmation:
        n_interactive_changes = (
            (custom_definitions_updated.reindex_like(custom_definitions).fillna("") != custom_definitions.fillna(""))
            .sum()
            .sum()
        )
        has_changes = n_interactive_changes > 0
        n_total = n_interactive_changes
    else:
        has_changes = n_changes > 0
        n_total = n_changes

    if not has_changes:
        _print(f"  {DIM}No changes found or accepted.{RESET}")

    if has_changes and not read_only:
        _confirm_and_write_data_to_file(
            custom_data=custom_definitions_updated, custom_data_file=custom_file, n_changes=n_total
        )

    return custom_definitions_updated


# Phase definitions for the main block.
PHASES = [
    {
        "label": "FAO dataset fields",
        "field": "dataset",
        "mode": "auto-accept",
        "func": "update_custom_datasets_file",
        "kwargs": {"confirmation": False, "compare_with": "fao"},
    },
    {
        "label": "OWID dataset fields",
        "field": "dataset",
        "mode": "interactive",
        "func": "update_custom_datasets_file",
        "kwargs": {"confirmation": True, "compare_with": "owid"},
    },
    {
        "label": "FAO element fields",
        "field": "element",
        "mode": "auto-accept",
        "func": "update_custom_items_or_elements_file",
        "kwargs": {"item_or_element": "element", "confirmation": False, "compare_with": "fao"},
    },
    {
        "label": "OWID element fields",
        "field": "element",
        "mode": "interactive",
        "func": "update_custom_items_or_elements_file",
        "kwargs": {"item_or_element": "element", "confirmation": True, "compare_with": "owid"},
    },
    {
        "label": "FAO item fields",
        "field": "item",
        "mode": "auto-accept",
        "func": "update_custom_items_or_elements_file",
        "kwargs": {"item_or_element": "item", "confirmation": False, "compare_with": "fao"},
    },
    {
        "label": "OWID item fields",
        "field": "item",
        "mode": "interactive",
        "func": "update_custom_items_or_elements_file",
        "kwargs": {"item_or_element": "item", "confirmation": True, "compare_with": "owid"},
    },
]


if __name__ == "__main__":
    argument_parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    argument_parser.add_argument(
        "-r",
        "--read-only",
        "--dry-run",
        default=False,
        action="store_true",
        dest="read_only",
        help="Preview changes without writing files.",
    )
    argument_parser.add_argument(
        "-v",
        "--version",
        default=VERSION,
        help="Version of the latest garden steps (where custom_datasets.csv file to be updated is).",
    )
    argument_parser.add_argument(
        "-f",
        "--field",
        choices=["dataset", "element", "item"],
        default=None,
        help="Only run phases for a specific field type (dataset, element, or item). Runs all if omitted.",
    )
    args = argument_parser.parse_args()

    if args.read_only:
        _print(f"\n  {CYAN}{BOLD}DRY RUN -- no files will be modified.{RESET}\n")

    # Filter phases if --field is given.
    phases_to_run = [p for p in PHASES if args.field is None or p["field"] == args.field]

    func_map = {
        "update_custom_datasets_file": update_custom_datasets_file,
        "update_custom_items_or_elements_file": update_custom_items_or_elements_file,
    }

    for i, phase in enumerate(phases_to_run, 1):
        mode_color = GREEN if phase["mode"] == "auto-accept" else YELLOW
        _print(f"\n  {BOLD}{'=' * 76}{RESET}")
        _print(f"  {BOLD}Phase {i}/{len(phases_to_run)}: {phase['label']}  {mode_color}[{phase['mode']}]{RESET}")
        _print(f"  {BOLD}{'=' * 76}{RESET}")

        func = func_map[phase["func"]]
        _ = func(version=args.version, read_only=args.read_only, **phase["kwargs"])

    _print(f"\n  {BOLD}Done.{RESET}\n")
