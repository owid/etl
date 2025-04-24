"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("yougov_job_automation")

    # Read table from garden dataset.
    tb = ds_garden.read("yougov_job_automation", reset_index=False)

    #
    # Process data.
    #
    # Rename the 'question' to 'country' for visualising in the grapher
    tb = tb.rename_index_names({"group": "country"})

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata, check_variables_metadata=True)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
