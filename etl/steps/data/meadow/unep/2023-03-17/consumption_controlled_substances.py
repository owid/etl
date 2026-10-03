"""Load snapshots and create a meadow dataset."""

from owid.catalog import Table
from owid.catalog import processing as pr
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
CHEMICAL_NAMES = {
    "bromochloromethane": "Bromochloromethane (BCM)",
    "carbon_tetrachloride": "Carbon Tetrachloride (CTC)",
    "chlorofluorocarbons": "Chlorofluorocarbons (CFCs)",
    "halons": "Halons",
    "hydrobromofluorocarbons": "Hydrobromofluorocarbons (HBFCs)",
    "hydrochlorofluorocarbons": "Hydrochlorofluorocarbons (HCFCs)",
    "hydrofluorocarbons": "Hydrofluorocarbons (HFCs)",
    "methyl_bromide": "Methyl Bromide (MB)",
    "methyl_chloroform": "Methyl Chloroform (TCA)",
    "other_fully_halogenated": "Other Fully Halogenated CFCs",
}


def run(dest_dir: str) -> None:
    log.info("consumption_controlled_substances.start")

    #
    # Load inputs.
    #
    tables = []
    last_snap: Snapshot | None = None
    for name, name_pretty in CHEMICAL_NAMES.items():
        log.info(f"consumption_controlled_substances: loading snapshot `{name}`.")
        # Retrieve snapshot.
        snap: Snapshot = paths.load_dependency(f"consumption_controlled_substances_{name}.xlsx")
        last_snap = snap
        # Load data from snapshot and format it.
        tb = format_frame(snap.read_excel(skiprows=1), name=name_pretty)
        tables.append(tb)
    log.info("consumption_controlled_substances: concatenating tables.")
    tb = pr.concat(tables, ignore_index=True, short_name=paths.short_name)

    #
    # Process data.
    #
    # Ensure all columns are snake-case.
    tb = tb.underscore()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    assert last_snap is not None
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=last_snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("consumption_controlled_substances.end")


def format_frame(tb: Table, name: str) -> Table:
    # Ensure no unexpected columns appear. We expect "Country", "Baseline", and year columns.
    YEAR_MIN = 1986
    YEAR_MAX = 2022
    COLUMN_NAMES_ACCEPTED = {"Country", "Baseline"} | set(range(YEAR_MIN, YEAR_MAX + 1))
    assert not (columns_new := set(tb.columns).difference(COLUMN_NAMES_ACCEPTED)), f"Unexpected columns {columns_new}"
    # Ensure country column is there
    assert "Country" in tb.columns, "Missing country column!"
    # Remove 'Baseline' column if exists
    if "Baseline" in tb.columns:
        tb = tb.drop(columns=["Baseline"])
    # Format tb (unpivot)
    tb = (
        tb.melt(id_vars="Country", var_name="year", value_name="consumption")
        .astype({"year": int})
        .assign(chemical=name)
    )
    return tb
