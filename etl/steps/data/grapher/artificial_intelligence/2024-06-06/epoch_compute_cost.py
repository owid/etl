"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("epoch_compute_cost")

    # Read table from garden dataset.
    tb = ds_garden["epoch_compute_cost"]

    #
    # Process data.
    #
    tb = tb.reset_index()
    # Rename for plotting model name as country in grapher
    tb = tb.rename(columns={"system": "country", "days_since_1949": "year"})
    tb = tb.set_index(["country", "year"], verify_integrity=True)

    #
    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
