"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("burden_disaggregated")

    # Read table from garden dataset.
    tb = ds_garden["burden_disaggregated"]
    tb_rate = ds_garden["burden_disaggregated_rate"]

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb, tb_rate], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
