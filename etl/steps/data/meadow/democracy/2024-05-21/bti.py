"""Load a snapshot and create a meadow dataset."""

from owid.catalog.tables import Table, concat

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# YEAR COVERAGE
YEAR_MIN = 2006
YEAR_MAX = 2024

# COLUMNS (relevant, renamings)
COLUMNS = {
    "Regions:\n1 | East-Central and Southeast Europe\n2 | Latin America and the Caribbean\n3 | West and Central Africa\n4 | Middle East and North Africa\n5 | Southern and Eastern Africa\n6 | Eastern Europe, Caucasus and Central Asia\n7 | Asia and Oceania": "country",
    "  SI | Democracy Status": "democracy_bti",
    "  Q1 | Stateness": "state_bti",
    "  Q2 | Political Participation": "political_participation_bti",
    "  Q3 | Rule of Law": "rule_of_law_bti",
    "  Q4 | Stability of Democratic Institutions": "stability_dem_inst_bti",
    "  Q5 | Political and Social Integration": "pol_soc_integr_bti",
    "  Category.1": "regime_bti",
    "  Q2_1 | Free and fair elections": "electfreefair_bti",
    "  Q2_2 | Effective power to govern": "effective_power_bti",
    "  Q2_3 | Association / assembly rights": "freeassoc_bti",
    "  Q2_4 | Freedom of expression": "freeexpr_bti",
    "  Q3_1 | Separation of powers": "sep_power_bti",
    "  Q3_4 | Civil rights": "civ_rights_bti",
    "  Failed State": "state_basic_bti",
    "  Democracy/Autocracy": "pol_sys",
}
COLUMNS_INDEX = ["country", "year"]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("bti.xlsx")

    # Load data from snapshot.
    tb = load_data(snap)

    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def load_data(snap: Snapshot) -> Table:
    """Load data from snapshot and return a Table."""
    tbs = []
    for year in range(2006, YEAR_MAX + 1, 2):
        # Read
        tb_ = snap.read(sheet_name=f"BTI {year}")
        # Column check
        columns_missing = set(COLUMNS) - set(tb_.columns)
        if columns_missing:
            raise ValueError(f"Columns missing in snapshot: {columns_missing}")
        # Column selection & renaming
        tb_ = tb_.rename(columns=COLUMNS)[COLUMNS.values()]
        # Add year (year of observation = year of report - 1)
        tb_["year"] = year - 1
        tbs.append(tb_)

    # Concatenate
    tb = concat(tbs)

    # Replace '-' -> NA
    columns = [col for col in COLUMNS.values() if col not in COLUMNS_INDEX]
    tb[columns] = tb[columns].replace("-", float("nan"))

    # Map
    tb["pol_sys"] = tb["pol_sys"].replace(
        {
            "Aut.": 0,
            "Dem.": 1,
        }
    )

    # Set dtypes
    tb[columns] = tb[columns].astype(float)

    return tb
