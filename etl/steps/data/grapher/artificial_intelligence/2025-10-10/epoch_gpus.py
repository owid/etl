"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, grapher_checks

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("epoch_gpus")

    # Read table from garden dataset.
    tb = ds_garden["epoch_gpus"]
    #
    # Process data.
    #
    # Rename for plotting model name as country in grapher
    tb = tb.rename_index_names({"hardware_name": "country", "days_since_2000": "year"})
    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)

    #
    # Checks.
    #
    grapher_checks(ds_grapher)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
