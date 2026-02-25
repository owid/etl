"""Update FAO names and descriptions

After an update, FAO may have changed dataset, item, element, or unit names and descriptions.
This will cause the garden faostat_metadata step to raise warnings, and may cause further issues on charts and the food
explorer.

This script updates all fao fields in custom_datasets.csv, custom_elements_and_units.csv, and custom_items.csv files.

For each file, it:
  1. Auto-accepts FAO field changes (aligning our records with new FAO data).
  2. Interactively reviews OWID fields, but ONLY where the corresponding FAO field changed.
  3. Saves once at the end.

Usage:
  cd etl/scripts/faostat
  python update_custom_metadata.py                  # normal run (all 3 file types)
  python update_custom_metadata.py --field dataset  # only dataset file
  python update_custom_metadata.py --field element  # only element file
  python update_custom_metadata.py --field item     # only item file
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


def _display_differences(old: str, new: str, message: str) -> None:
    """Show a single merged paragraph with inline red (removed) and green (added) highlights."""
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

    # Word-level diff: build a single merged paragraph.
    old_words = old_normalized.split()
    new_words = new_normalized.split()

    sm = difflib.SequenceMatcher(None, old_words, new_words)
    parts = []

    for tag, i1, i2, j1, j2 in sm.get_opcodes():
        if tag == "equal":
            parts.append(" ".join(old_words[i1:i2]))
        elif tag == "replace":
            parts.append(f"{RED}[-" + " ".join(old_words[i1:i2]) + f"-]{RESET}")
            parts.append(f"{GREEN}[+" + " ".join(new_words[j1:j2]) + f"+]{RESET}")
        elif tag == "delete":
            parts.append(f"{RED}[-" + " ".join(old_words[i1:i2]) + f"-]{RESET}")
        elif tag == "insert":
            parts.append(f"{GREEN}[+" + " ".join(new_words[j1:j2]) + f"+]{RESET}")

    _print(f"  {' '.join(parts)}")
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


def update_custom_datasets_file(version=VERSION, read_only=False):
    """Update custom_datasets.csv: auto-accept FAO changes, then review OWID fields only where FAO changed."""
    custom_datasets_file = STEP_DIR / "data/garden/faostat" / version / "custom_datasets.csv"

    error = f"File custom_datasets.csv not found. Ensure garden steps for version {version} exist."
    assert custom_datasets_file.is_file(), error

    # Load custom datasets file.
    custom_datasets = pd.read_csv(custom_datasets_file).set_index("dataset")
    custom_datasets_updated = custom_datasets.copy()

    # --- Step 1: Detect and auto-accept FAO field changes ---
    _print(f"\n  {BOLD}Aligning FAO fields...{RESET}")
    fao_changes = []  # (dataset_short_name, field, old_fao, new_fao)

    for domain in tqdm(INCLUDED_DATASETS_CODES):
        dataset_short_name = f"faostat_{domain}"
        fao_new_metadata = Dataset(DATA_DIR / "meadow/faostat" / version / dataset_short_name).metadata

        for field in ["title", "description"]:
            new_fao = getattr(fao_new_metadata, field) or ""
            try:
                old_fao = custom_datasets.loc[dataset_short_name].fillna("")[f"fao_dataset_{field}"]
            except KeyError:
                old_fao = ""

            if old_fao != new_fao and not (pd.isna(new_fao) and pd.isna(old_fao)):
                fao_changes.append((dataset_short_name, field, old_fao, new_fao))
                custom_datasets_updated.loc[dataset_short_name, f"fao_dataset_{field}"] = new_fao

    if fao_changes:
        _print(f"\n  {GREEN}Auto-accepted {len(fao_changes)} FAO change{'s' if len(fao_changes) != 1 else ''}:{RESET}")
        for ds, field, _, _ in fao_changes:
            _print(f"    {BOLD}{ds}{RESET}: {field}")
        choice = input(f"\n  Type {BOLD}d{RESET} to review diffs, or {BOLD}enter{RESET} to continue: ").strip().lower()
        if choice == "d":
            for ds, field, old_fao, new_fao in fao_changes:
                _display_differences(old=old_fao, new=new_fao, message=f"{ds}: {field}")
    else:
        _print(f"  {DIM}No FAO changes.{RESET}")

    # --- Step 2: Review OWID fields where FAO changed ---
    if fao_changes:
        _print(f"\n  {BOLD}Reviewing OWID fields ({len(fao_changes)} to review)...{RESET}")

        n_prompts = 0
        for ds, field, old_fao, new_fao in fao_changes:
            try:
                row = custom_datasets.loc[ds].fillna("")
                owid_current = row[f"owid_dataset_{field}"]
            except KeyError:
                owid_current = ""

            # Skip if effective value already matches new FAO (no change).
            effective_current = owid_current if owid_current else old_fao
            if effective_current == new_fao:
                continue

            if not owid_current:
                _print(f"\n  {DIM}OWID is empty (inherited from FAO).{RESET}")

            _display_differences(
                old=effective_current,
                new=new_fao,
                message=f"{ds}: {field}",
            )

            chosen = _confirm_edit_or_skip(owid_current, new_fao)
            # If chosen matches FAO, leave OWID empty so it inherits naturally.
            custom_datasets_updated.loc[ds, f"owid_dataset_{field}"] = "" if chosen == new_fao else chosen
            n_prompts += 1

        if n_prompts == 0:
            _print(f"  {DIM}All OWID fields already match — nothing to review.{RESET}")

    # --- Save ---
    n_total = int(
        (custom_datasets_updated.reindex_like(custom_datasets).fillna("") != custom_datasets.fillna("")).sum().sum()
    )

    if n_total == 0:
        _print(f"  {DIM}No changes to save.{RESET}")
    elif read_only:
        _print(f"  {YELLOW}{n_total} change{'s' if n_total != 1 else ''} detected (dry run, not saving).{RESET}")
    else:
        _confirm_and_write_data_to_file(custom_datasets_updated, custom_datasets_file, n_changes=n_total)

    return custom_datasets_updated


def update_custom_items_or_elements_file(item_or_element, version=VERSION, read_only=False):
    """Update custom file: auto-accept FAO changes, then review OWID fields only where FAO changed."""
    if item_or_element == "element":
        custom_file = STEP_DIR / "data/garden/faostat" / version / "custom_elements_and_units.csv"
        fields = ["element", "element_description", "unit", "unit_short_name"]
    else:
        custom_file = STEP_DIR / "data/garden/faostat" / version / "custom_items.csv"
        fields = ["item", "item_description"]

    error = f"File of custom definitions not found. Ensure garden steps for version {version} exist."
    assert custom_file.is_file(), error

    # Load custom definitions file.
    custom_definitions = pd.read_csv(custom_file, dtype=str).set_index(["dataset", f"{item_or_element}_code"])
    custom_definitions_updated = custom_definitions.copy()

    # Load metadata from new garden dataset.
    fao_new_metadata = Dataset(DATA_DIR / "garden/faostat" / version / "faostat_metadata")

    # --- Step 1: Detect and auto-accept FAO field changes ---
    _print(f"\n  {BOLD}Aligning FAO fields...{RESET}")
    fao_changes = []  # (dataset_short_name, code, field, old_fao, new_fao, old_row)

    for dataset_short_name in tqdm(custom_definitions.index.get_level_values(0).unique()):
        for code in custom_definitions.loc[dataset_short_name].index.get_level_values(0).unique():
            try:
                new_metadata = fao_new_metadata[f"{item_or_element}s"].loc[dataset_short_name, code].fillna("")
            except KeyError:
                log.error(
                    f"{item_or_element.capitalize()} code {code} (dataset {dataset_short_name}) not found in new "
                    f"metadata. Remove it from the custom file or replace it with another code."
                )
                continue
            old_row = custom_definitions.loc[dataset_short_name, code].fillna("")

            for field in fields:
                new_fao = new_metadata[f"fao_{field}"]
                old_fao = old_row[f"fao_{field}"]

                if old_fao != new_fao and not (pd.isna(new_fao) and pd.isna(old_fao)):
                    fao_changes.append((dataset_short_name, code, field, old_fao, new_fao, old_row))
                    custom_definitions_updated.loc[(dataset_short_name, code), f"fao_{field}"] = new_fao

    if fao_changes:
        _print(f"\n  {GREEN}Auto-accepted {len(fao_changes)} FAO change{'s' if len(fao_changes) != 1 else ''}:{RESET}")
        for ds, code, field, _, _, _ in fao_changes:
            _print(f"    {BOLD}{ds}{RESET} ({item_or_element} {code}): {field}")
        choice = input(f"\n  Type {BOLD}d{RESET} to review diffs, or {BOLD}enter{RESET} to continue: ").strip().lower()
        if choice == "d":
            for ds, code, field, old_fao, new_fao, _ in fao_changes:
                _display_differences(old=old_fao, new=new_fao, message=f"{ds} ({item_or_element} {code}): {field}")
    else:
        _print(f"  {DIM}No FAO changes.{RESET}")

    # --- Step 2: Review OWID fields where FAO changed ---
    if fao_changes:
        _print(f"\n  {BOLD}Reviewing OWID fields ({len(fao_changes)} to review)...{RESET}")

        n_prompts = 0
        for ds, code, field, old_fao, new_fao, old_row in fao_changes:
            owid_current = old_row.get(f"owid_{field}", "")
            effective_current = owid_current or old_fao

            # Skip if effective value already matches new FAO (no change).
            if effective_current == new_fao:
                continue

            inherited = ""
            if not owid_current:
                inherited = f"  {DIM}(inherited from FAO){RESET}"

            if "description" in field:
                # Word-level diff for long text.
                _display_differences(
                    old=effective_current,
                    new=new_fao,
                    message=f"{ds} ({item_or_element} {code}): {field}",
                )
                if not owid_current:
                    _print(f"  {DIM}OWID is empty (inherited from FAO).{RESET}")
            else:
                # Compact display for short fields.
                _print()
                _print(f"  {CYAN}{BOLD}{'─' * 76}{RESET}")
                _print(f"  {CYAN}{BOLD}{ds} ({item_or_element} {code}): {field}{RESET}")
                _print(f"  {CYAN}{BOLD}{'─' * 76}{RESET}")
                _print(f"  {YELLOW}OWID (current):{RESET}  {effective_current}{inherited}")
                _print(f"  {GREEN}FAO (new):{RESET}       {new_fao}")

            # For unit fields on elements, show the custom factor as context.
            if item_or_element == "element" and field in ("unit", "unit_short_name"):
                factor = old_row.get("owid_unit_factor", "")
                if factor:
                    _print(f"  {YELLOW}Note: OWID applies a custom factor of {BOLD}{factor}{RESET}")
            _print()

            chosen = _confirm_edit_or_skip(owid_current, new_fao)
            # If chosen matches FAO, leave OWID empty so it inherits naturally.
            custom_definitions_updated.loc[(ds, code), f"owid_{field}"] = "" if chosen == new_fao else chosen
            n_prompts += 1

        if n_prompts == 0:
            _print(f"  {DIM}All OWID fields already match — nothing to review.{RESET}")

    # --- Save ---
    n_total = int(
        (custom_definitions_updated.reindex_like(custom_definitions).fillna("") != custom_definitions.fillna(""))
        .sum()
        .sum()
    )

    if n_total == 0:
        _print(f"  {DIM}No changes to save.{RESET}")
    elif read_only:
        _print(f"  {YELLOW}{n_total} change{'s' if n_total != 1 else ''} detected (dry run, not saving).{RESET}")
    else:
        _confirm_and_write_data_to_file(custom_definitions_updated, custom_file, n_changes=n_total)

    return custom_definitions_updated


# Phase definitions for the main block.
PHASES = [
    {"label": "Dataset fields", "field": "dataset", "func": update_custom_datasets_file, "kwargs": {}},
    {
        "label": "Element & unit fields",
        "field": "element",
        "func": update_custom_items_or_elements_file,
        "kwargs": {"item_or_element": "element"},
    },
    {
        "label": "Item fields",
        "field": "item",
        "func": update_custom_items_or_elements_file,
        "kwargs": {"item_or_element": "item"},
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
        help="Only run for a specific file type (dataset, element, or item). Runs all if omitted.",
    )
    args = argument_parser.parse_args()

    if args.read_only:
        _print(f"\n  {CYAN}{BOLD}DRY RUN -- no files will be modified.{RESET}\n")

    # Filter phases if --field is given.
    phases_to_run = [p for p in PHASES if args.field is None or p["field"] == args.field]

    for i, phase in enumerate(phases_to_run, 1):
        _print(f"\n  {BOLD}{'=' * 76}{RESET}")
        _print(f"  {BOLD}Phase {i}/{len(phases_to_run)}: {phase['label']}{RESET}")
        _print(f"  {BOLD}{'=' * 76}{RESET}")

        phase["func"](version=args.version, read_only=args.read_only, **phase["kwargs"])

    _print(f"\n  {BOLD}Done.{RESET}\n")
