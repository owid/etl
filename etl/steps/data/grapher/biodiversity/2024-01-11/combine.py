"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("combine")

    # Read table from garden dataset.
    tb = ds_garden["combine"].reset_index()
    tb = tb.drop(columns=["order", "family", "genus", "species", "phylacine_binomial"])
    tb = tb.rename(columns={"iucn2020_binomial": "country"})
    tb["year"] = 2021
    tb = tb[~tb["country"].duplicated(keep=False)]
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()
    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
