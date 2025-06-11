"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("nuclear_weapons_inventories.csv")

    # Load data from snapshot.
    tb = snap.read(sep="\t", encoding="utf-16")

    #
    # Process data.
    #
    tb = tb.rename(columns={"Unnamed: 0": "country"}, errors="raise")
    tb = tb.melt(id_vars=["country"], var_name="year", value_name="number_of_warheads")

    # Improve table format.
    tb = tb.format(["country", "year"]).astype({"number_of_warheads": "string"})

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb])
    ds_meadow.save()
