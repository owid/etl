"""Load snapshot and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load data from snapshot.
    #
    ds_meadow = paths.load_dataset("endemic_invertebrates")
    tb = ds_meadow.read("endemic_invertebrates")
    # tb = pr.concat([tb, tb_fish], ignore_index=True, axis=0)
    tb = paths.regions.harmonize_names(tb)

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the snapshot.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
