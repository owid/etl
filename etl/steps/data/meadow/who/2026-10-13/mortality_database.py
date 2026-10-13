"""Load the WHO Mortality Database snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    # Step 1: Load the snapshot.
    snap = paths.load_snapshot("mortality_database.feather")
    tb = snap.read(safe_types=False)

    # Step 2: Drop the code columns that duplicate the labels, and WHO's regional grouping, which
    # nothing downstream uses. `icd10_codes` stays as a column: it describes the cause, not the row.
    tb = tb.drop(columns=["Region Code", "Country Code", "Age group code", "Region Name"])
    tb = tb.rename(columns={"Country Name": "country"}, errors="raise")
    tb = tb.format(["country", "year", "sex", "age_group", "cause"])

    # Step 3: Save.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()
