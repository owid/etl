"""Load snapshot and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load data from snapshot.
    #
    ds_meadow = paths.load_dataset("endemic_vertebrates")
    ds_fish = paths.load_dataset("endemic_fish")
    tb = ds_meadow.read("endemic_vertebrates")
    tb_fish = ds_fish.read("endemic_fish")
    # tb = pr.concat([tb, tb_fish], ignore_index=True, axis=0)
    tb = paths.regions.harmonize_names(tb)
    tb_fish = paths.regions.harmonize_names(tb_fish, warn_on_unused_countries=False)
    tb = tb.merge(tb_fish, how="outer")
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the snapshot.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
