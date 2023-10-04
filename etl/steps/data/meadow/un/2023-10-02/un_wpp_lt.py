"""Load a snapshot and create a meadow dataset."""

from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Logger
log = get_logger()
# Column rename
COLUMNS_RENAME = {
    "mx": "central_death_rate",
    "qx": "probability_of_death",
    "px": "probability_of_survival",
    "lx": "number_survivors",
    "dx": "number_deaths",
    "Lx": "number_person_years_lived",
    "Sx": "survivorship_ratio",
    "Tx": "number_person_years_remaining",
    "ex": "life_expectancy",
    "ax": "average_survival_length",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    snap_short_names = [
        "un_wpp_lt_all",  # ALL
        "un_wpp_lt_f",  # FEMALE
        "un_wpp_lt_m",  # MALE
    ]

    tables = []
    for snap_short_name in snap_short_names:
        # Load data from snapshot.
        log.info(f"un_wpp_lt: reading {snap_short_name}")
        snap = paths.load_snapshot(f"{snap_short_name}.zip")
        tb = snap.read_csv(
            dtype={
                "Notes": str,
                "ISO3_code": "category",
                "ISO2_code": "category",
            }
        )
        # Rename columns
        tb = tb.rename(columns=COLUMNS_RENAME)
        # Filter only relevant location types
        tb = tb[
            tb["LocTypeName"].isin(["Geographic region", "Income group", "Country/Area", "World", "Development group"])
        ]
        # Set index
        tb = tb.set_index(["Location", "Time", "Sex", "AgeGrp", "LocTypeName"], verify_integrity=True).sort_index()
        # Add to tables list
        tables.append(tb)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=tables, check_variables_metadata=True, default_metadata=snap.metadata)  # type: ignore

    # Save changes in the new meadow dataset.
    ds_meadow.save()
