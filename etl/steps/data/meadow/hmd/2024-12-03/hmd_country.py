"""Load a snapshot and create a meadow dataset."""

from pathlib import Path

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("hmd_country.zip")

    # Load data from snapshot.
    paths.log.info("Loading data from snapshot.")
    tbs = []
    with snap.extract_to_tempdir() as tmp_dir:
        p = Path(tmp_dir)
        files = p.glob("**/InputDB/*month.txt")
        for f in files:
            tb_ = pr.read_csv(
                f,
                na_values=["."],
                metadata=snap.to_table_metadata(),
                origin=snap.m.origin,
            )
            tb_.columns = tb_.columns.str.strip()
            tb_ = tb_.rename(
                columns={
                    "NoteCode1": "Note1",
                    "NoteCode2": "Note2",
                    "NoteCode3": "Note3",
                }
            )
            tbs.append(tb_)

    # Concatenate
    paths.log.info("Concatenating tables.")
    tb = pr.concat(tbs, ignore_index=True)
    tb = tb.rename(columns={"PopName": "country"})

    #
    # Process data.
    #
    paths.log.info("Processing data.")
    tb = tb.groupby(["country", "Year", "Month"], as_index=False)["Births"].mean()
    tb = tb.astype(
        {
            "country": "string",
            "Month": "string",
        }
    )
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year", "month"], short_name="monthly")

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
