"""Load snapshot and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load data from snapshot.
    #
    ds_meadow = paths.load_dataset("threatened_and_evaluated_species")
    tb = ds_meadow.read("threatened_and_evaluated_species")
    tb["share_evaluated"] = (tb["evaluated_species"] / tb["described_species"]) * 100

    tb = tb.format(["taxonomic_group", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the snapshot.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
