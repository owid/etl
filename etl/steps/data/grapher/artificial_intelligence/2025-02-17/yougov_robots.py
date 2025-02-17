"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset, grapher_checks

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("yougov_robots")

    # Read table from garden dataset.
    tb = ds_garden.read("yougov_robots", reset_index=False)
    #
    # Process data.
    #
    # Rename the 'question' to 'country' for visualising in the grapher
    tb = tb.rename_index_names({"group": "country"})

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb], default_metadata=ds_garden.metadata, check_variables_metadata=True
    )

    #
    # Checks.
    #
    grapher_checks(ds_grapher)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
