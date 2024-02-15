"""Load a snapshot and create a meadow dataset."""

import zipfile

from owid.catalog import processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("nutrients.zip")
    snap_regions = paths.load_snapshot("nutrients_europe.csv")

    # Load europe aggregate data from snapshot.
    tb_reg = snap_regions.read()
    tb_reg["countryName"] = "Europe (EEA)"
    # Load data from snapshot.
    with zipfile.ZipFile(snap.path) as z:
        # open the csv file in the dataset
        with z.open("aggregateddata_country.csv") as f:
            # read the dataset
            tb = pr.read_csv(f, metadata=snap.to_table_metadata(), origin=snap.m.origin, delimiter=";")
    tb = pr.concat([tb, tb_reg])
    tb = tb.rename(columns={"countryName": "country", "phenomenonTimeReferenceYear": "year"})
    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.set_index(["country", "year", "waterBodyCategory", "eeaIndicator"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
