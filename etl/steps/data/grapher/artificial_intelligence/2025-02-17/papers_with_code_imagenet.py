"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("papers_with_code_imagenet")

    # Read table from garden dataset.
    tb = ds_garden.read("papers_with_code_imagenet")

    #
    # Process data.
    #

    tb["country"] = "Top-1 accuracy"
    tb = tb.drop("name", axis=1)
    tb = tb.format(["country", "date"])

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
