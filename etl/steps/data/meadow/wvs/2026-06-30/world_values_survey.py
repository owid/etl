"""Load a snapshot and create a meadow dataset (World Values Survey time series)."""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Mapping of WVS question codes to readable labels, used to rename loop-group columns produced by
# wvs_create_file.do (columns ending in the code, e.g. "confidence_E069_61"). Single-question blocks in
# the .do already emit final, readable column names, so they need no entry here. Add codes as questions grow.
VARS_DICT: dict[str, str] = {}


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("world_values_survey.csv")

    # Load data from snapshot.
    tb = snap.read(safe_types=False)

    #
    # Process data.
    #
    # Rename loop-group variables (trailing question code -> readable label) and snake-case all columns.
    tb = rename_vars(tb)

    # Remove trailing spaces from country names (the WVS value labels carry them).
    tb["country"] = tb["country"].str.rstrip()

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def rename_vars(tb: Table) -> Table:
    """Rename loop-group columns by replacing the trailing question code with its readable label, then snake-case."""
    # Rename columns, replacing var with name when the original name ends with var.
    for var, name in VARS_DICT.items():
        tb = tb.rename(
            columns={column: column.replace(var, name) for column in tb.columns if column.endswith(var)}, errors="raise"
        )

    # Generate snake case names.
    tb.columns = tb.columns.str.lower().str.replace(" ", "_")

    return tb
